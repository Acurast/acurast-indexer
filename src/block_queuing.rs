//! Block queuing module.
//!
//! This module handles queuing blocks for processing:
//! - Gap detection and backfilling
//! - Backwards indexing from latest block
//! - Finalized block subscription

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_channel::Sender;
use backoff::{future::retry_notify, Error as BackoffError, ExponentialBackoff as Backoff};
use sqlx::{query_as, Pool, Postgres};
use subxt::blocks::BlockRef;
use subxt::utils::H256;
use subxt::{OnlineClient, PolkadotConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::entities::ExtrinsicRow;
use crate::task_monitor::TASK_REGISTRY;
use crate::utils::connect_node;
use crate::AppError;

#[derive(Debug, sqlx::FromRow)]
struct Gap {
    start: Option<i64>,
    next_low_hash: Option<String>,
}

/// Queue blocks to fill gaps in the indexed data.
pub async fn queue_gaps(
    tx: Sender<BlockRef<H256>>,
    backpressure: Sender<ExtrinsicRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    lowest_done: u32,
    first_finalized: tokio::sync::oneshot::Receiver<(u32, H256)>,
) {
    let task_id = TASK_REGISTRY.start("Queue gaps", None);

    // Wait for the first finalized block to be captured (with timeout)
    let initial_page = tokio::select! {
        biased;

        _ = cancel_token.cancelled() => {
            TASK_REGISTRY.end(task_id);
            return;
        }
        result = first_finalized => {
            match result {
                Ok((block_num, block_ref)) => {
                    info!("Using first finalized block {}, {} as starting point for gap detection", block_num, block_ref);
                    block_num
                }
                Err(_) => {
                    warn!("Failed to receive first finalized block, falling back to u32::MAX");
                    u32::MAX
                }
            }
        }
    };

    let result = retry_notify(
        Backoff::default(),
        || {
            let tx = tx.clone();
            let backpressure = backpressure.clone();
            let db_pool = db_pool.clone();
            let cancel_token = cancel_token.clone();
            async move {
                if cancel_token.is_cancelled() {
                    return Err(BackoffError::permanent(anyhow!("Cancelled")));
                }

                match queue_gaps_(
                    task_id,
                    tx,
                    backpressure,
                    db_pool,
                    cancel_token,
                    lowest_done,
                    initial_page,
                )
                .await
                {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("{:?}", e);
                        Err(BackoffError::transient(e))
                    }
                }
            }
        },
        |err, dur| {
            error!(
                "[{:?}] Backing off for queue_gaps due to error: {}",
                dur, err
            );
        },
    )
    .await;

    TASK_REGISTRY.end(task_id);

    if let Err(e) = result {
        error!("queue_gaps stopped permanently: {}", e);
    }
}

async fn queue_gaps_(
    task_id: u64,
    tx: Sender<BlockRef<H256>>,
    backpressure: Sender<ExtrinsicRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    lowest_done: u32,
    first_finalized: u32,
) -> Result<(), anyhow::Error> {
    let settings = &crate::config::settings().indexer;
    let (client, _) = connect_node(settings.archive_nodes[0].clone()).await?;

    let mut iteration: u64 = 0;
    let mut total_gaps_queued: u64 = 0;

    'outer: loop {
        iteration += 1;
        let mut gaps_this_iteration: u64 = 0;

        // First iteration: use first_finalized, subsequent: query highest block from DB
        let mut page = if iteration == 1 {
            first_finalized
        } else {
            let highest: Option<(i64,)> = sqlx::query_as("SELECT MAX(block_number) FROM blocks")
                .fetch_optional(&db_pool)
                .await
                .map_err(|e| AppError::InternalError(e.into()))?;
            highest.map(|h| h.0 as u32).unwrap_or(first_finalized)
        };

        TASK_REGISTRY.set_detail(
            task_id,
            format!(
                "Iteration {}, scanning from block {}, {} total gaps queued",
                iteration, page, total_gaps_queued
            ),
        );

        'inner: loop {
            let gaps = query_as!(
                Gap,
                r#"
            WITH ordered_blocks AS (
            SELECT block_number, "hash", LEAD(block_number) OVER (ORDER BY block_number) AS next_block_number,
                    LEAD("hash") OVER (ORDER BY block_number) AS next_low_hash
            FROM blocks
            WHERE block_number > ($1) AND block_number < ($2)
            ORDER BY block_number desc
            )
            SELECT
                block_number + 1 AS start,
                next_low_hash
            FROM ordered_blocks
            WHERE next_block_number IS NOT NULL AND next_block_number - block_number > 1
            LIMIT 1000;
            "#,
                lowest_done as i64,
                page as i64
            )
            .fetch_all(&db_pool)
            .await
            .map_err(|e| AppError::InternalError(e.into()))?;

            if gaps.is_empty() {
                break 'inner; // No more gaps in this iteration, wait and restart
            }

            for gap in gaps.iter() {
                if cancel_token.is_cancelled() {
                    break 'outer;
                }
                debug!("iterating gap {:?}", gap.start);
                page = gap.start.unwrap() as u32;
                if let Some(next_low_hash) = &gap.next_low_hash {
                    let mut todo: subxt::blocks::Block<
                        PolkadotConfig,
                        OnlineClient<PolkadotConfig>,
                    > = client
                        .blocks()
                        .at(H256::from_slice(&hex::decode(next_low_hash).unwrap()))
                        .await?;
                    while gap.start.map(|s| todo.number() > s as u32).unwrap_or(false) {
                        // Wait for extrinsic queue to have capacity before queuing more blocks, but at lower backpressure sensitivity than backwards queuer, to indirectly prioritize gaps (will still queue while backwards will not)
                        while backpressure.len() > 1000 && !cancel_token.is_cancelled() {
                            debug!("Queue gaps waiting for extrinsic queue capacity");
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }

                        // set to parent (correct because in first iteration it's the lowest already processed)
                        todo = client.blocks().at(todo.header().parent_hash).await?;
                        let block_number = todo.number();
                        tokio::select! {
                            biased;

                            _ = cancel_token.cancelled() => break 'outer,
                            result = tx.send(todo.reference()) => {
                                result?
                            }
                        }
                        gaps_this_iteration += 1;
                        total_gaps_queued += 1;
                        TASK_REGISTRY.set_block(task_id, block_number);
                        TASK_REGISTRY.set_detail(
                            task_id,
                            format!(
                                "Iteration {}, {} gaps this iter, {} total",
                                iteration, gaps_this_iteration, total_gaps_queued
                            ),
                        );
                        debug!("Queued gap {:?} {:?}", block_number, todo.reference());
                    }
                }
            }
        }

        // Finished scanning, wait 10 seconds before next iteration
        info!(
            "Gap scan iteration {} complete: {} gaps queued this iteration, {} total",
            iteration, gaps_this_iteration, total_gaps_queued
        );
        TASK_REGISTRY.set_detail(
            task_id,
            format!(
                "Iteration {} done, {} total gaps. Waiting 5min...",
                iteration, total_gaps_queued
            ),
        );

        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break 'outer,
            _ = tokio::time::sleep(Duration::from_secs(300)) => {},
        }
    }

    Ok(())
}

/// Queue parents of specific blocks (for manual backfilling).
pub async fn queue_parents_of(
    tx: Sender<BlockRef<H256>>,
    parents: Vec<String>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Queue parents", None);
    let settings = &crate::config::settings().indexer;

    let (client, _) = connect_node(settings.archive_nodes[0].clone())
        .await
        .expect("Failed to connect to node");

    for parent in parents {
        let parent_block = client
            .blocks()
            .at(H256::from_slice(&hex::decode(parent).unwrap()))
            .await?;
        let todo = client
            .blocks()
            .at(parent_block.header().parent_hash)
            .await?;
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break,
            result = tx.send(todo.reference()) => {
                result?
            }
        }
        debug!(
            "Queued individual {:?} {:?}",
            todo.number(),
            todo.reference()
        );
    }
    if !cancel_token.is_cancelled() {
        info!("Queued all individual");
    }

    TASK_REGISTRY.end(task_id);
    Ok(())
}

/// Queue specific blocks by hash for reprocessing.
/// Block hashes should be hex strings (with or without 0x prefix).
pub async fn queue_reprocess_blocks(
    tx: Sender<BlockRef<H256>>,
    block_hashes: Vec<String>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    if block_hashes.is_empty() {
        return Ok(());
    }

    let task_id = TASK_REGISTRY.start("Queue reprocess blocks", None);

    info!("Queueing {} blocks for reprocessing", block_hashes.len());

    for hash_str in block_hashes {
        // Strip 0x prefix if present
        let hash_hex = hash_str.strip_prefix("0x").unwrap_or(&hash_str);

        let block_hash = match hex::decode(hash_hex) {
            Ok(bytes) if bytes.len() == 32 => H256::from_slice(&bytes),
            Ok(bytes) => {
                error!(
                    "Invalid hash length {} for '{}', expected 32 bytes",
                    bytes.len(),
                    hash_str
                );
                continue;
            }
            Err(e) => {
                error!("Failed to decode hash '{}': {:?}", hash_str, e);
                continue;
            }
        };

        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break,
            result = tx.send(BlockRef::from_hash(block_hash)) => {
                if let Err(e) = result {
                    error!("Failed to queue block {:?}: {:?}", block_hash, e);
                }
            }
        }

        info!("Queued block {:?} for reprocessing", block_hash);
    }

    if !cancel_token.is_cancelled() {
        info!("Queued all reprocess blocks");
    }

    TASK_REGISTRY.end(task_id);
    Ok(())
}

/// Queue blocks backwards from the lowest indexed block.
pub async fn queue_backwards(
    tx: Sender<BlockRef<H256>>,
    backpressure: Sender<ExtrinsicRow>,
    _db_pool: Pool<Postgres>,
    lowest_done: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    cancel_token: CancellationToken,
) {
    let task_id = TASK_REGISTRY.start("Queue backwards", None);

    let result = retry_notify(
        Backoff::default(),
        || {
            let tx = tx.clone();
            let backpressure = backpressure.clone();
            let lowest_done = lowest_done.clone();
            let cancel_token = cancel_token.clone();
            async move {
                if cancel_token.is_cancelled() {
                    return Err(BackoffError::permanent(anyhow!("Cancelled")));
                }

                match queue_backwards_(task_id, tx, backpressure, lowest_done, cancel_token).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(BackoffError::transient(e)),
                }
            }
        },
        |err, dur| {
            error!(
                "[{:?}] Backing off for queue_backwards due to error: {}",
                dur, err
            );
        },
    )
    .await;

    TASK_REGISTRY.end(task_id);

    if let Err(e) = result {
        error!("queue_backwards stopped permanently: {}", e);
    }
}

async fn queue_backwards_(
    task_id: u64,
    tx: Sender<BlockRef<H256>>,
    backpressure: Sender<ExtrinsicRow>,
    lowest_done: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    let settings = &crate::config::settings().indexer;
    let (client, _) = connect_node(settings.archive_nodes[0].clone()).await?;
    let mut todo: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>> = lowest_done;
    while todo.number() > settings.index_from_block {
        // Wait for extrinsic queue to have capacity before queuing more blocks
        while backpressure.len() > 100 && !cancel_token.is_cancelled() {
            debug!("Queue backwards waiting for extrinsic queue capacity");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        // set to parent (correct because in first iteration it's the lowest already processed)
        todo = client.blocks().at(todo.header().parent_hash).await?;
        let block_number = todo.number();
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break,
            result = tx.send(todo.reference()) => {
                result?
            }
        }
        TASK_REGISTRY.set_block(task_id, block_number);
        debug!("Queued backwards {:?} {:?}", block_number, todo.reference());
    }
    if !cancel_token.is_cancelled() {
        info!("Queued all backwards");
    }
    Ok(())
}

/// Subscribe to finalized blocks and queue them for processing.
pub async fn on_finalized(
    tx: Sender<BlockRef<H256>>,
    cancel_token: CancellationToken,
    first_finalized_senders: Vec<tokio::sync::oneshot::Sender<(u32, H256)>>,
    latest_finalized: Arc<AtomicU32>,
) {
    let task_id = TASK_REGISTRY.start("Queue finalized", None);

    // Wrap senders in Option so we can move them into queue_finalized_
    let first_finalized_senders: Vec<_> = first_finalized_senders
        .into_iter()
        .map(|s| std::sync::Mutex::new(Some(s)))
        .collect();

    let result = retry_notify(
        Backoff::default(),
        || {
            let t = tx.clone();
            let c = cancel_token.clone();
            let first_finalized_senders = &first_finalized_senders;
            let latest_finalized = &latest_finalized;
            async move {
                if c.is_cancelled() {
                    return Err(BackoffError::permanent(anyhow!("Cancelled")));
                }

                match on_finalized_(task_id, t, c, first_finalized_senders, latest_finalized).await
                {
                    Ok(_) => Ok(()),
                    Err(e) => Err(BackoffError::transient(e)),
                }
            }
        },
        |err, dur| {
            error!(
                "[{:?}] Backing off for queue_finalized due to error: {}",
                dur, err
            );
        },
    )
    .await;

    TASK_REGISTRY.end(task_id);

    if let Err(e) = result {
        error!("queue_finalized stopped permanently: {}", e);
    }
}

async fn on_finalized_(
    task_id: u64,
    tx: Sender<BlockRef<H256>>,
    cancel_token: CancellationToken,
    first_finalized_senders: &[std::sync::Mutex<
        Option<tokio::sync::oneshot::Sender<(u32, H256)>>,
    >],
    latest_finalized: &Arc<AtomicU32>,
) -> Result<(), anyhow::Error> {
    let settings = &crate::config::settings().indexer;
    let (client, _) = connect_node(settings.archive_nodes[0].clone()).await?;

    // here we subscribe to newly finalized blocks
    let mut blocks_sub = client.blocks().subscribe_finalized().await?;

    loop {
        let block = tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break,
            block = blocks_sub.next() => block,
        };

        let Some(block) = block else { break };

        match block {
            Ok(b) => {
                let block_number = b.number();

                TASK_REGISTRY.set_block(task_id, block_number);

                let old_value = latest_finalized.swap(block_number, Ordering::Relaxed);
                if old_value != block_number {
                    info!(
                        "Updated finalized block cache: {} -> {}",
                        old_value, block_number
                    );
                }

                // Send the first block number to all waiting receivers
                for sender_mutex in first_finalized_senders {
                    if let Ok(mut guard) = sender_mutex.lock() {
                        if let Some(sender) = guard.take() {
                            info!(
                                "Captured first finalized block for listener: {}",
                                block_number
                            );
                            let _ = sender.send((block_number, b.hash()));
                        }
                    }
                }

                if settings.index_finalized {
                    tokio::select! {
                        biased;

                        _ = cancel_token.cancelled() => break,
                        result = tx.send(b.reference()) => {
                            result?
                        }
                    }
                }
            }
            Err(e) => {
                error!(
                    queuing = "finalized",
                    "Read finalized block failed: {:?}", e
                );
                blocks_sub = client.blocks().subscribe_finalized().await?;
            }
        }
    }

    Ok(())
}
