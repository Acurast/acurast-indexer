//! Epoch indexing module
//!
//! Indexes the current epoch/cycle from pallet 48 (AcurastCompute) at every block.
//! This is a hard-coded indexer that tracks epoch transitions.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use crate::entities::{Block, EpochIndexPhase, EpochRow, EventsIndexPhase};
use crate::task_monitor::{QueueType, TASK_REGISTRY};
use crate::AppError;
use anyhow::anyhow;
use async_channel::Sender;
use parity_scale_codec::Decode;
use sqlx::{Pool, Postgres};
use subxt::config::PolkadotConfig;
use subxt::utils::H256;
use subxt::OnlineClient;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

/// This eventually is the first block when the current cycle storage was introduced. Before that it was calculated by block_number modulo Epoch length (900 for canary).
static LOWEST_BLOCK_WITH_CURRENT_CYCLE: AtomicU32 = AtomicU32::new(u32::MAX);

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

/// Index the current epoch at the given block.
/// Inserts into the epochs table, ignoring if the epoch already exists.
/// If we're at an epoch_start block and `epoch_tx` is provided, sends (epoch, block_hash) on the channel.
pub async fn index_epoch_at_block(
    block_data: &Block,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    epoch_tx: Option<&tokio::sync::mpsc::Sender<(u32, String)>>,
) -> Result<(), anyhow::Error> {
    let stored_cycle = if (block_data.block_number as u32)
        < LOWEST_BLOCK_WITH_CURRENT_CYCLE.load(Ordering::Relaxed)
    {
        // Parse block hash from hex string
        let hash_bytes = hex::decode(&block_data.hash)
            .map_err(|e| anyhow!("Failed to decode block hash: {}", e))?;
        let block_hash = H256::from_slice(&hash_bytes);

        // Get block for storage queries
        let block = client.blocks().at(block_hash).await?;

        // Fetch CurrentCycle from pallet 48
        let storage_query = subxt::dynamic::storage(
            EPOCH_PALLET,
            EPOCH_STORAGE,
            Vec::<subxt::dynamic::Value>::new(),
        );

        match block.storage().fetch(&storage_query).await {
            Ok(Some(value)) => {
                // Decode the raw bytes into our Cycle struct
                Ok(Some(
                    Cycle::decode(&mut value.encoded())
                        .map_err(|e| anyhow!("Failed to decode Cycle: {}", e))?,
                ))
            }
            Ok(None) => Err(anyhow!(
                "CurrentCycle storage is empty at block {} - skipping epoch indexing",
                block_data.block_number
            )),
            Err(subxt::Error::Metadata(subxt::error::MetadataError::StorageEntryNotFound(_))) => {
                // Storage doesn't exist yet - fall back to modulo calculation
                LOWEST_BLOCK_WITH_CURRENT_CYCLE
                    .fetch_min(block_data.block_number as u32 + 1, Ordering::Relaxed);

                Ok(None)
            }
            Err(e) => return Err(e.into()),
        }
    } else {
        Ok(None)
    }?;

    let cycle = stored_cycle.unwrap_or_else(|| {
        let e = (block_data.block_number / 900) as u32;
        Cycle {
            epoch: e,
            epoch_start: 900 * e,
        }
    });

    // Insert epoch with epoch_start from storage, ignore if already exists
    let result = sqlx::query(
        r#"
                INSERT INTO epochs (epoch, epoch_start, epoch_start_time)
                VALUES ($1, $2, $3)
                ON CONFLICT (epoch) DO NOTHING
                "#,
    )
    .bind(cycle.epoch as i64)
    .bind(cycle.epoch_start as i64)
    .bind(block_data.block_time)
    .execute(db_pool)
    .await?;

    // If we're at the epoch_start block, notify via channel with the block hash
    let is_epoch_start_block = block_data.block_number == cycle.epoch_start as i64;
    if is_epoch_start_block {
        if let Some(tx) = epoch_tx {
            // Send notification on channel (non-blocking)
            if let Err(e) = tx.try_send((cycle.epoch, block_data.hash.clone())) {
                debug!(
                    "Failed to send epoch notification for epoch {} at block {}: {:?}",
                    cycle.epoch, block_data.block_number, e
                );
            } else {
                trace!(
                    "Sent epoch {} notification at epoch_start block {}",
                    cycle.epoch,
                    cycle.epoch_start
                );
            }
        }
    }

    if result.rows_affected() > 0 {
        trace!(
            "New epoch {} inserted (epoch_start block: {})",
            cycle.epoch,
            cycle.epoch_start
        );
    }

    Ok(())
}

/// Queue epochs that need phase processing on startup.
///
/// This finds epochs that are past Raw phase but not yet at the final phase
/// and queues them for phase workers. This handles the case where the indexer was
/// restarted mid-processing.
///
/// Queries for all epochs where: phase >= 1 (EventsReady) AND phase < max_phase_for(Epoch)
pub async fn queue_epochs_phase(
    tx: Sender<EpochRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    index_from_block: u32,
) -> Result<(), anyhow::Error> {
    use crate::storage_indexing::TriggerKind;

    let max_epoch_phase = crate::config::storage_rules().max_phase_for(TriggerKind::Epoch);
    let task_id = TASK_REGISTRY.start(
        format!("Queue epochs (phases 1-{})", max_epoch_phase - 1),
        None,
    );

    // Track exact (epoch, phase) tuples that have been queued to prevent requeuing
    let mut queued: HashSet<(i64, i32)> = HashSet::new();
    let mut wait = 0u64;

    'outer: loop {
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break 'outer,
            _ = tokio::time::sleep(Duration::from_secs(wait)) => {},
        }
        wait = 10;
        info!(
            "Search for epochs in phases 1 to {} (exclusive)",
            max_epoch_phase
        );

        // Query epochs in any intermediate phase (>= 1, < max_phase)
        let epochs: Vec<EpochRow> = sqlx::query_as(
            r#"WITH ranked AS (
                SELECT epoch, epoch_start, epoch_start_time, phase,
                       LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end
                FROM epochs
            )
            SELECT epoch, epoch_start, epoch_start_time, phase, epoch_end
            FROM ranked
            WHERE phase >= 1 AND phase < $1 AND epoch_start >= $2 AND epoch_end IS NOT NULL
            ORDER BY epoch ASC"#,
        )
        .bind(max_epoch_phase as i32)
        .bind(index_from_block as i64)
        .fetch_all(&db_pool)
        .await
        .map_err(|e| AppError::InternalError(e.into()))?;

        info!("Found {} epochs needing processing", epochs.len());

        // Publish queue range to task registry for monitoring (only when we have epochs)
        if !epochs.is_empty() {
            let min_epoch = epochs.iter().map(|e| e.epoch).min().unwrap();
            let max_epoch = epochs.iter().map(|e| e.epoch).max().unwrap();
            TASK_REGISTRY.set_queue_range(
                QueueType::Epoch,
                min_epoch.to_string(),
                max_epoch.to_string(),
            );
        }

        for epoch in epochs {
            if cancel_token.is_cancelled() {
                break;
            }

            // Skip if this exact (epoch, phase) tuple was already queued
            let key = (epoch.epoch, epoch.phase as i32);
            if queued.contains(&key) {
                debug!(
                    "Skipping epoch {} (phase {}) - already queued",
                    epoch.epoch, epoch.phase as i32
                );
                continue;
            }

            TASK_REGISTRY.set_epoch(task_id, epoch.epoch);
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                result = tx.send(epoch.clone()) => {
                    result?;
                    queued.insert(key);
                    info!("Queued epoch {} (phase {}) for processing", epoch.epoch, epoch.phase as i32);
                }
            }
        }
    }

    TASK_REGISTRY.end(task_id);
    Ok(())
}

/// Detect epochs that are ready for phase processing.
///
/// This queuer selects the highest epoch with phase 0 (Raw), then waits for all events
/// in that epoch's block range to be fully indexed (phase >= StorageIndexing) before
/// moving the epoch into next phase EventsReady and queuing it for processing.
pub async fn wait_epoch_events_ready(
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    index_from_block: u32,
) -> Result<(), anyhow::Error> {
    let task_id = TASK_REGISTRY.start("Wait epoch blocks & events", None);
    const POLL_INTERVAL_SECONDS: u64 = 5; // Poll every 5 seconds

    info!("Starting wait_epoch_events_ready task");
    'outer: loop {
        // Check for cancellation
        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => break 'outer,
            _ = tokio::time::sleep(Duration::from_secs(POLL_INTERVAL_SECONDS)) => {},
        }

        // Step 1: Find the HIGHEST epoch with phase 0 (Raw), using LEAD() to get next epoch's start
        // Process newest epoch first; events should be prioritized by recency to match
        // excluding epochs that can never be completed because their start block lies before index_from_block
        let epoch: Option<EpochRow> = sqlx::query_as(
            r#"WITH ranked AS (
                SELECT epoch, epoch_start, epoch_start_time, phase,
                       LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end
                FROM epochs
            )
            SELECT epoch, epoch_start, epoch_start_time, phase, epoch_end
            FROM ranked
            WHERE phase = $1 AND epoch_end IS NOT NULL AND epoch_start >= $2
            ORDER BY epoch DESC
            LIMIT 1"#,
        )
        .bind(EpochIndexPhase::Raw as i32)
        .bind(index_from_block as i64)
        .fetch_optional(&db_pool)
        .await
        .map_err(|e| AppError::InternalError(e.into()))?;

        let Some(epoch) = epoch else {
            debug!(
                "No epochs in Raw phase, wait and recheck in {} seconds",
                POLL_INTERVAL_SECONDS
            );
            continue;
        };

        debug!(
            "Found epoch {} (blocks {}-{}) in Raw phase, waiting for events to be indexed",
            epoch.epoch,
            epoch.epoch_start,
            epoch.epoch_end.unwrap()
        );

        TASK_REGISTRY.set_epoch(task_id, epoch.epoch);

        // Step 2: Check:
        // - All blocks in epoch range are indexed (no more events will be added)
        // - All events are >= StorageIndexing phase (all events fully indexed)
        let expected_blocks = epoch.epoch_end.unwrap() - epoch.epoch_start;
        let indexed_blocks: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM blocks
                    WHERE block_number >= $1 AND block_number < $2"#,
        )
        .bind(epoch.epoch_start)
        .bind(epoch.epoch_end.unwrap())
        .fetch_one(&db_pool)
        .await
        .map_err(|e| AppError::InternalError(e.into()))?;
        let indexed_blocks_count = indexed_blocks.0;

        if indexed_blocks_count < expected_blocks {
            let missing_count = expected_blocks - indexed_blocks_count;
            let msg = if missing_count < 5 {
                // Query for the exact missing block numbers
                let indexed: Vec<(i64,)> = sqlx::query_as(
                    r#"SELECT block_number FROM blocks
                            WHERE block_number >= $1 AND block_number < $2
                            ORDER BY block_number DESC"#,
                )
                .bind(epoch.epoch_start)
                .bind(epoch.epoch_end.unwrap())
                .fetch_all(&db_pool)
                .await
                .map_err(|e| AppError::InternalError(e.into()))?;

                let indexed_set: std::collections::HashSet<i64> =
                    indexed.into_iter().map(|(n,)| n).collect();
                let missing: Vec<i64> = (epoch.epoch_start..epoch.epoch_end.unwrap())
                    .filter(|n| !indexed_set.contains(n))
                    .collect();

                format!(
                    "Epoch {} has {}/{} blocks indexed, missing: {:?}",
                    epoch.epoch, indexed_blocks_count, expected_blocks, missing
                )
            } else {
                format!(
                    "Epoch {} has {}/{} blocks indexed, waiting",
                    epoch.epoch, indexed_blocks_count, expected_blocks
                )
            };
            info!("{}", msg);
            TASK_REGISTRY.set_detail(task_id, msg);
            continue;
        } else {
            let msg = format!(
                "Epoch {} has all {}/{} blocks indexed -> check events phase",
                epoch.epoch, indexed_blocks_count, expected_blocks
            );
            info!("{}", msg);
            TASK_REGISTRY.set_detail(task_id, msg);
        }

        // Check how many events are below StorageIndexing
        let unfinished_events: (i64,) = sqlx::query_as(
            r#"SELECT COUNT(*) FROM events
                    WHERE block_number >= $1 AND block_number < $2
                    AND phase < $3"#,
        )
        .bind(epoch.epoch_start)
        .bind(epoch.epoch_end.unwrap())
        .bind(EventsIndexPhase::MAX as i32)
        .fetch_one(&db_pool)
        .await
        .map_err(|e| AppError::InternalError(e.into()))?;
        let unfinished_events_count = unfinished_events.0;

        if unfinished_events_count == 0 {
            let msg = format!(
                    "Epoch {} all blocks indexed and all events >= StorageIndexing, marking events_ready",
                    epoch.epoch
                );
            info!("{}", msg);
            TASK_REGISTRY.set_detail(task_id, msg);

            // Step 3: Update the epoch phase to EventsReady
            sqlx::query("UPDATE epochs SET phase = $1 WHERE epoch = $2")
                .bind(EpochIndexPhase::EventsReady as i32)
                .bind(epoch.epoch)
                .execute(&db_pool)
                .await
                .map_err(|e| AppError::InternalError(e.into()))?;
        }

        let msg = format!(
            "Epoch {} still has {} events below StorageIndexing phase, waiting",
            epoch.epoch, unfinished_events_count
        );
        info!("{}", msg);
        TASK_REGISTRY.set_detail(task_id, msg);
    }

    TASK_REGISTRY.end(task_id);
    Ok(())
}
