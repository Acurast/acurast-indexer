//! Block queuing module.
//!
//! This module handles queuing blocks for processing:
//! - Gap detection and backfilling
//! - Backwards indexing from latest block
//! - Finalized block subscription

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use async_channel::Sender;
use backoff::{future::retry_notify, Error as BackoffError, ExponentialBackoff as Backoff};
use parity_scale_codec::Decode;
use sqlx::{query_as, Pool, Postgres};
use subxt::blocks::BlockRef;
use subxt::utils::H256;
use subxt::{OnlineClient, PolkadotConfig};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

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
    // Each queuer takes a distinct slot so that when multiple archive nodes
    // are configured, load is distributed instead of all queuers hitting node 0.
    let node_url = settings.archive_nodes[1 % settings.archive_nodes.len()].clone();
    crate::utils::with_node_url(node_url.clone(), async move {
        queue_gaps_inner(
            task_id,
            tx,
            backpressure,
            db_pool,
            cancel_token,
            lowest_done,
            first_finalized,
            node_url,
        )
        .await
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn queue_gaps_inner(
    task_id: u64,
    tx: Sender<BlockRef<H256>>,
    backpressure: Sender<ExtrinsicRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    lowest_done: u32,
    first_finalized: u32,
    node_url: String,
) -> Result<(), anyhow::Error> {
    let settings = &crate::config::settings().indexer;
    let (client, _) = connect_node(&node_url).await?;

    let mut total_gaps_queued: u64 = 0;

    // Track every block number we've sent into the channel during this run.
    // An in-flight block (queued but not yet inserted into `blocks` by the
    // worker) would otherwise look like an unstarted gap when the inner SQL
    // re-runs and would be re-queued. With this set, we skip the duplicate
    // RPC parent-hash walk and the duplicate channel send.
    //
    // The set lives only for the duration of this single pass. Recovery from
    // a worker silently dropping a queued block is operator-driven (process
    // restart re-runs this pass with an empty set).
    let mut queued_blocks: HashSet<u32> = HashSet::new();

    // Single one-shot pass: walk all gaps from `first_finalized` down to
    // `lowest_done`. The inner `'inner: loop` exists because the SQL is
    // `LIMIT 1000` — for large gap counts we need multiple SELECTs, each
    // narrowing `page` to the lowest gap_start we've processed so far.
    // No outer re-iteration: when this function returns Ok, the surrounding
    // `retry_notify` lets the task end. Errors are retried with backoff;
    // permanent failure surfaces via the existing "stopped permanently" log.
    let mut page = first_finalized;
    TASK_REGISTRY.set_detail(
        task_id,
        format!("Scanning from block {}, 0 gaps queued", page),
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
            break 'inner;
        }

        for gap in gaps.iter() {
            if cancel_token.is_cancelled() {
                return Ok(());
            }
            debug!("iterating gap {:?}", gap.start);
            let gap_start = gap.start.unwrap() as u32;
            page = gap_start;
            if queued_blocks.contains(&gap_start) {
                debug!(
                    "Skipping gap starting at {} (already queued this run)",
                    gap_start
                );
                continue;
            }
            if let Some(next_low_hash) = &gap.next_low_hash {
                crate::utils::record_node_rpc_call();
                let mut todo: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>> =
                    client
                        .blocks()
                        .at(H256::from_slice(&hex::decode(next_low_hash).unwrap()))
                        .await?;
                while todo.number() > gap_start {
                    // Wait for extrinsic queue to have capacity before queuing more blocks
                    while backpressure.len() > settings.backpressure_threshold
                        && !cancel_token.is_cancelled()
                    {
                        debug!("Queue gaps waiting for extrinsic queue capacity");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }

                    // set to parent (correct because in first iteration it's the lowest already processed)
                    crate::utils::record_node_rpc_call();
                    todo = client.blocks().at(todo.header().parent_hash).await?;
                    let block_number = todo.number();
                    if queued_blocks.contains(&block_number) {
                        continue;
                    }
                    tokio::select! {
                        biased;

                        _ = cancel_token.cancelled() => return Ok(()),
                        result = tx.send(todo.reference()) => {
                            result?
                        }
                    }
                    queued_blocks.insert(block_number);
                    total_gaps_queued += 1;
                    TASK_REGISTRY.set_block(task_id, block_number);
                    TASK_REGISTRY.set_detail(task_id, format!("{} gaps queued", total_gaps_queued));
                    debug!("Queued gap {:?} {:?}", block_number, todo.reference());
                }
            }
        }
    }

    info!(
        "Gap scan complete: {} blocks queued, exiting",
        total_gaps_queued
    );
    TASK_REGISTRY.set_detail(
        task_id,
        format!("Done: {} gaps queued, exiting", total_gaps_queued),
    );
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
    let node_url = settings.archive_nodes[0].clone();
    let (client, _) = connect_node(&node_url).await?;
    crate::utils::with_node_url(node_url, async move {
        let settings = &crate::config::settings().indexer;
        let mut todo: subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>> =
            lowest_done;
        while todo.number() > settings.index_from_block {
            // Wait for extrinsic queue to have capacity before queuing more blocks
            while backpressure.len() > settings.backpressure_threshold
                && !cancel_token.is_cancelled()
            {
                debug!("Queue backwards waiting for extrinsic queue capacity");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            // set to parent (correct because in first iteration it's the lowest already processed)
            crate::utils::record_node_rpc_call();
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
    })
    .await
}

/// Hard-coded pallet index for AcurastCompute
const EPOCH_PALLET: &str = "AcurastCompute";
/// Hard-coded storage location for current cycle
const EPOCH_STORAGE: &str = "CurrentCycle";

/// The Cycle struct from AcurastCompute pallet
#[derive(Debug, Decode)]
struct Cycle {
    pub epoch: u32,
    pub epoch_start: u32,
}

/// Subscribe to finalized blocks and queue them for processing.
/// Also detects epoch_start blocks and notifies via epoch_tx for commitment rescans.
pub async fn on_finalized(
    tx: Sender<BlockRef<H256>>,
    cancel_token: CancellationToken,
    first_finalized_senders: Vec<tokio::sync::oneshot::Sender<(u32, H256)>>,
    latest_finalized: Arc<AtomicU32>,
    epoch_tx: tokio::sync::mpsc::Sender<(u32, u32, String)>,
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
            let epoch_tx = &epoch_tx;
            async move {
                if c.is_cancelled() {
                    return Err(BackoffError::permanent(anyhow!("Cancelled")));
                }

                match on_finalized_(
                    task_id,
                    t,
                    c,
                    first_finalized_senders,
                    latest_finalized,
                    epoch_tx,
                )
                .await
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
    epoch_tx: &tokio::sync::mpsc::Sender<(u32, u32, String)>,
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

                // Check if this is an epoch_start block and notify for commitment rescan
                if let Err(e) = check_and_notify_epoch_start(&b, epoch_tx).await {
                    error!(
                        "Failed to check epoch_start at block {}: {:?}",
                        block_number, e
                    );
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

/// Check if the given block is an epoch_start block and notify via epoch_tx.
async fn check_and_notify_epoch_start(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    epoch_tx: &tokio::sync::mpsc::Sender<(u32, u32, String)>,
) -> Result<(), anyhow::Error> {
    let block_number = block.number();

    // Fetch CurrentCycle from AcurastCompute pallet
    let storage_query = subxt::dynamic::storage(
        EPOCH_PALLET,
        EPOCH_STORAGE,
        Vec::<subxt::dynamic::Value>::new(),
    );

    let cycle = match block.storage().fetch(&storage_query).await {
        Ok(Some(value)) => Cycle::decode(&mut value.encoded())
            .map_err(|e| anyhow!("Failed to decode Cycle: {}", e))?,
        Ok(None) => {
            // Storage empty, skip
            return Ok(());
        }
        Err(subxt::Error::Metadata(subxt::error::MetadataError::StorageEntryNotFound(_))) => {
            // Storage doesn't exist at this block
            return Ok(());
        }
        Err(e) => return Err(e.into()),
    };

    // Check if this block is the epoch_start block
    if block_number == cycle.epoch_start {
        let block_hash = hex::encode(block.hash().0);
        if let Err(e) = epoch_tx.try_send((cycle.epoch, cycle.epoch_start, block_hash.clone())) {
            debug!(
                "Failed to send epoch notification for epoch {} at block {}: {:?}",
                cycle.epoch, block_number, e
            );
        } else {
            trace!(
                "Sent epoch {} notification at epoch_start block {}",
                cycle.epoch,
                cycle.epoch_start
            );
        }
    }

    Ok(())
}
