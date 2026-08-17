//! Event indexing phase processing.
//!
//! This module handles the phase-based processing of events:
//! - Queuing events that need processing
//! - Extracting jobs from events

use std::collections::HashMap;
use std::time::Duration;

use async_channel::Sender;
use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use tracing_log::log::trace;

use crate::config::JobFromEventTransformation;
use crate::entities::{EventRow, EventsIndexPhase};
use crate::task_monitor::{QueueType, TaskGuard, TASK_REGISTRY};
use crate::AppError;

/// Queue events that need phase processing.
/// Queues all events with phase < max_phase (includes phase 0 for restart recovery).
/// Events without matching rules at a phase will simply advance to the next phase.
///
/// Parameters:
/// - `queuer_id`: Identifier for this queuer (for task naming)
/// - `from_block`: Start of block range (inclusive)
/// - `to_block`: End of block range (exclusive). If None, queuer runs indefinitely (follows finalized)
pub async fn queue_events_phase(
    tx: Sender<EventRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    queuer_id: u32,
    from_block: u32,
    to_block: Option<u32>,
) -> Result<(), anyhow::Error> {
    let task_name = if to_block.is_some() {
        format!(
            "Queue events #{} ({}-{})",
            queuer_id,
            from_block,
            to_block.unwrap()
        )
    } else {
        format!("Queue events #{} ({}+)", queuer_id, from_block)
    };
    let mut task = TaskGuard::new(task_name, None);

    // Track the min and max composite keys (block_number, index) that have been queued
    // Note: index is unique per block, so extrinsic_index is not needed for key comparison
    let mut min_queued_key: Option<(i64, i32)> = None;
    let mut max_queued_key: Option<(i64, i32)> = None;
    let mut wait = 0u64;

    let max_event_phase = EventsIndexPhase::MAX;

    debug!(
        "Queuing events with phase < {}, block range [{}, {:?})",
        max_event_phase, from_block, to_block
    );

    'outer: loop {
        trace!("Search for events with phase < {}", max_event_phase);

        // Backpressure: if the worker channel is saturated, idle for 500ms
        // before re-checking. Use the same `select!` as the normal pacing so
        // cancellation is honored immediately in either case.
        let backpressured = tx.len() > 5_000;
        let sleep_ms = if backpressured { 500 } else { wait };

        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => break 'outer,
            _ = tokio::time::sleep(Duration::from_millis(sleep_ms)) => {},
        }

        if backpressured {
            continue;
        }

        // Query all events with phases < max_event_phase (includes phase 0 for restart recovery)
        // Build query based on whether we have an upper block limit and exclusion range
        // Note: Primary key is now (block_number, index) since index is unique per block
        let events: Vec<EventRow> = match (to_block, &min_queued_key, &max_queued_key) {
            // Has upper limit and exclusion range - use UNION ALL for better index usage
            (Some(upper), Some(ref min_key), Some(ref max_key)) => {
                sqlx::query_as(
                    "(SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                      WHERE phase < $1 AND error IS NULL
                        AND (block_number, index) < ($2, $3)
                        AND block_number >= $6 AND block_number < $7
                      ORDER BY block_number DESC, index DESC
                      LIMIT 2000)
                     UNION ALL
                     (SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                      WHERE phase < $1 AND error IS NULL
                        AND (block_number, index) > ($4, $5)
                        AND block_number >= $6 AND block_number < $7
                      ORDER BY block_number DESC, index DESC
                      LIMIT 2000)
                     ORDER BY block_number DESC, index DESC
                     LIMIT 2000"
                )
                .bind(max_event_phase as i32)
                .bind(min_key.0)
                .bind(min_key.1)
                .bind(max_key.0)
                .bind(max_key.1)
                .bind(from_block as i64)
                .bind(upper as i64)
                .fetch_all(&db_pool)
                .await
                .map_err(|e| {
                    let err = AppError::InternalError(e.into());
                    task.record_error(&err);
                    err
                })?
            }
            // Has upper limit, no exclusion range (first iteration)
            (Some(upper), None, None) => {
                sqlx::query_as(
                    "SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                     WHERE phase < $1 AND block_number >= $2 AND block_number < $3
                     ORDER BY block_number DESC, index DESC
                     LIMIT 2000"
                )
                .bind(max_event_phase as i32)
                .bind(from_block as i64)
                .bind(upper as i64)
                .fetch_all(&db_pool)
                .await
                .map_err(|e| {
                    let err = AppError::InternalError(e.into());
                    task.record_error(&err);
                    err
                })?
            }
            // No upper limit, has exclusion range - use UNION ALL for better index usage
            (None, Some(ref min_key), Some(ref max_key)) => {
                sqlx::query_as(
                    "(SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                      WHERE phase < $1 AND error IS NULL
                        AND (block_number, index) < ($2, $3)
                        AND block_number >= $6
                      ORDER BY block_number DESC, index DESC
                      LIMIT 2000)
                     UNION ALL
                     (SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                      WHERE phase < $1 AND error IS NULL
                        AND (block_number, index) > ($4, $5)
                        AND block_number >= $6
                      ORDER BY block_number DESC, index DESC
                      LIMIT 2000)
                     ORDER BY block_number DESC, index DESC
                     LIMIT 2000"
                )
                .bind(max_event_phase as i32)
                .bind(min_key.0)
                .bind(min_key.1)
                .bind(max_key.0)
                .bind(max_key.1)
                .bind(from_block as i64)
                .fetch_all(&db_pool)
                .await
                .map_err(|e| {
                    let err = AppError::InternalError(e.into());
                    task.record_error(&err);
                    err
                })?
            }
            // No upper limit, no exclusion range (first iteration)
            (None, None, None) => {
                sqlx::query_as(
                    "SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events
                     WHERE phase < $1 AND block_number >= $2
                     ORDER BY block_number DESC, index DESC
                     LIMIT 2000"
                )
                .bind(max_event_phase as i32)
                .bind(from_block as i64)
                .fetch_all(&db_pool)
                .await
                .map_err(|e| {
                    let err = AppError::InternalError(e.into());
                    task.record_error(&err);
                    err
                })?
            }
            // Partial exclusion keys shouldn't happen, but handle gracefully
            _ => vec![],
        };

        // Update the queued range with this batch
        // Key is (block_number, index) since index is unique per block
        let first_key = events.first().map(|e| (e.block_number, e.index));
        let last_key = events.last().map(|e| (e.block_number, e.index));

        if let (Some(first), Some(last)) = (first_key, last_key) {
            // Update min and max: since we're ordering DESC, first is max, last is min
            max_queued_key = Some(match max_queued_key {
                Some(current_max) => {
                    if first > current_max {
                        first
                    } else {
                        current_max
                    }
                }
                None => first,
            });
            min_queued_key = Some(match min_queued_key {
                Some(current_min) => {
                    if last < current_min {
                        last
                    } else {
                        current_min
                    }
                }
                None => last,
            });
            debug!(
                "Found events in phase < {:?} outside done range [{}.{}, {}.{}]",
                max_event_phase,
                min_queued_key.clone().unwrap().0,
                min_queued_key.clone().unwrap().1,
                max_queued_key.clone().unwrap().0,
                max_queued_key.clone().unwrap().1
            );

            // Publish queue range to task registry for monitoring (only when we have values)
            let min_key = min_queued_key.unwrap();
            let max_key = max_queued_key.unwrap();
            TASK_REGISTRY.set_queue_range(
                QueueType::Event,
                format!("{}.{}", min_key.0, min_key.1),
                format!("{}.{}", max_key.0, max_key.1),
            );
        }

        if events.is_empty() {
            wait = 1_000; // Short wait when caught up
        } else {
            wait = 1_000;
        }

        // queue each row
        for row in events {
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() => break 'outer,
                result = tx.send(row) => {
                    result?
                }
            }
        }
    }

    task.complete();
    Ok(())
}

#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("event-phase-{:?}", worker_id),
        block_number = event.block_number,
        extrinsic = event.extrinsic_index,
        event = event.index
    )
)]
/// Process event phase: extract jobs from event data.
/// Returns true if jobs were extracted successfully, false if an error was recorded.
pub async fn process_event_phase(
    worker_id: u32,
    event: EventRow,
    event_transformations: &HashMap<u32, Vec<JobFromEventTransformation>>,
    db_pool: &Pool<Postgres>,
) -> Result<bool, anyhow::Error> {
    trace!("Process phase",);

    // Build transformation map for this phase
    let mut t_map: HashMap<(u32, u32), Vec<String>> = HashMap::new();
    if let Some(transformations) = event_transformations.get(&1) {
        for t in transformations {
            t_map
                .entry((t.pallet, t.variant))
                .or_default()
                .push(t.data_path.clone());
        }
    }

    // Skip job extraction for system events (no extrinsic_index)
    // Jobs are always associated with extrinsics, so this is safe
    let Some(extrinsic_index) = event.extrinsic_index else {
        return Ok(true); // System events don't have jobs to extract
    };

    if let Some(data_paths) = t_map.get(&(event.pallet as u32, event.variant as u32)) {
        let mut col_extrinsic_block_number: Vec<i64> = vec![];
        let mut col_extrinsic_index: Vec<i32> = vec![];
        let mut col_event_index: Vec<i32> = vec![];
        let mut col_data_path: Vec<String> = vec![];
        let mut col_chain: Vec<String> = vec![];
        let mut col_address: Vec<String> = vec![];
        let mut col_seq_id: Vec<i32> = vec![];
        let mut col_block_time: Vec<DateTime<Utc>> = vec![];

        for data_path in data_paths.clone().into_iter() {
            if let Some(e) = &event.data {
                match crate::data_extraction::extract(e, &data_path) {
                    Ok((chain, address, seq_id)) => {
                        col_chain.push(chain);
                        col_address.push(address);
                        col_seq_id.push(seq_id);
                        col_extrinsic_block_number.push(event.block_number);
                        col_extrinsic_index.push(extrinsic_index);
                        col_event_index.push(event.index);
                        col_data_path.push(data_path.to_owned());
                        col_block_time.push(event.block_time);
                    }
                    Err(err) => {
                        let msg = format!(
                            "could not extract path {:?} in event.data {:?}, got: {:?}",
                            data_path, e, err
                        );
                        error!("Extraction error: {:?}", msg);
                        sqlx::query(
                            "
                                UPDATE events
                                SET error = $1
                                WHERE block_number = $2 AND index = $3;
                            ",
                        )
                        .bind(&msg)
                        .bind(event.block_number)
                        .bind(event.index)
                        .execute(db_pool)
                        .await?;
                        return Ok(false); // Error recorded, don't advance phase
                    }
                }
            }
        }

        let result = sqlx::query(
            "
                INSERT INTO jobs(block_number, extrinsic_index, event_index, data_path, chain, address, seq_id, block_time)
                SELECT
                    block_number,
                    extrinsic_index,
                    event_index,
                    data_path,
                    chain_text::target_chain,
                    address,
                    seq_id,
                    block_time
                FROM UNNEST($1::bigint[], $2::integer[], $3::integer[], $4::text[], $5::text[], $6::text[], $7::integer[], $8::timestamptz[])
                AS t(block_number, extrinsic_index, event_index, data_path, chain_text, address, seq_id, block_time)
                ON CONFLICT (block_number, extrinsic_index, event_index, data_path) DO NOTHING;
            ",
        )
        .bind(&col_extrinsic_block_number[..])
        .bind(&col_extrinsic_index[..])
        .bind(&col_event_index[..])
        .bind(&col_data_path[..])
        .bind(&col_chain[..])
        .bind(&col_address[..])
        .bind(&col_seq_id[..])
        .bind(&col_block_time[..])
        .execute(db_pool)
        .await?;
        crate::task_monitor::TASK_REGISTRY
            .record_db_insert(crate::task_monitor::DbEntity::Job, result.rows_affected());

        debug!("Wrote {:?} jobs", col_extrinsic_block_number.len());
    }

    Ok(true)
}

/// A pending phase advance for a single event, produced by phase workers and
/// drained by [`events_phase_update_flusher`].
#[derive(Debug, Clone, Copy)]
pub struct EventPhaseUpdate {
    pub block_number: i64,
    pub index: i32,
    pub new_phase: i32,
}

pub type EventPhaseUpdateSender = async_channel::Sender<EventPhaseUpdate>;
pub type EventPhaseUpdateReceiver = async_channel::Receiver<EventPhaseUpdate>;

/// Channel for shipping per-event phase advances from workers to the
/// batching flusher. Bounded so a slow flusher applies backpressure to the
/// workers instead of letting the queue grow without bound.
pub fn events_phase_update_channel() -> (EventPhaseUpdateSender, EventPhaseUpdateReceiver) {
    async_channel::bounded(10_000)
}

/// Drain `EventPhaseUpdate`s and apply them as batched
/// `UPDATE events FROM UNNEST(...)` writes. Flushes whichever comes first:
/// `BATCH_SIZE` distinct `(block_number, index)` keys buffered, or
/// `FLUSH_INTERVAL` elapsed since the first item of the current batch.
///
/// Conflict resolution: if the same `(block_number, index)` appears multiple
/// times in a batch — typical when a single worker advances an event through
/// several phases before the flusher fires — only the highest `new_phase` is
/// written. The SQL `WHERE e.phase < u.new_phase` provides the same guarantee
/// across batches.
pub async fn events_phase_update_flusher(
    rx: EventPhaseUpdateReceiver,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
) -> Result<(), anyhow::Error> {
    const BATCH_SIZE: usize = 1000;
    const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
    // Sentinel deadline used when the buffer is empty: setting it far in the
    // future keeps the timer arm of the `select!` armed but inert until the
    // first item arrives and a real flush deadline is set.
    const IDLE: Duration = Duration::from_secs(3600);

    let task_id = TASK_REGISTRY.start("Events phase update flusher", None);

    // Key: (block_number, index). Value: highest `new_phase` observed.
    let mut buffer: HashMap<(i64, i32), i32> = HashMap::new();
    let mut deadline = tokio::time::Instant::now() + IDLE;

    loop {
        tokio::select! {
            biased;

            _ = cancel_token.cancelled() => {
                if !buffer.is_empty() {
                    flush_events_phase_updates(&db_pool, &mut buffer).await?;
                }
                break;
            }

            _ = tokio::time::sleep_until(deadline) => {
                if !buffer.is_empty() {
                    flush_events_phase_updates(&db_pool, &mut buffer).await?;
                }
                deadline = tokio::time::Instant::now() + IDLE;
            }

            result = rx.recv() => {
                let update = match result {
                    Ok(u) => u,
                    Err(_) => {
                        // All senders dropped — flush any remaining work and exit.
                        if !buffer.is_empty() {
                            flush_events_phase_updates(&db_pool, &mut buffer).await?;
                        }
                        break;
                    }
                };

                let was_empty = buffer.is_empty();
                buffer
                    .entry((update.block_number, update.index))
                    .and_modify(|p| *p = (*p).max(update.new_phase))
                    .or_insert(update.new_phase);

                // Start the flush window when transitioning from empty to non-empty.
                if was_empty {
                    deadline = tokio::time::Instant::now() + FLUSH_INTERVAL;
                }

                if buffer.len() >= BATCH_SIZE {
                    flush_events_phase_updates(&db_pool, &mut buffer).await?;
                    deadline = tokio::time::Instant::now() + IDLE;
                }
            }
        }
    }

    TASK_REGISTRY.end(task_id);
    Ok(())
}

async fn flush_events_phase_updates(
    db_pool: &Pool<Postgres>,
    buffer: &mut HashMap<(i64, i32), i32>,
) -> Result<(), anyhow::Error> {
    if buffer.is_empty() {
        return Ok(());
    }

    let n = buffer.len();
    let mut block_numbers = Vec::with_capacity(n);
    let mut indices = Vec::with_capacity(n);
    let mut phases = Vec::with_capacity(n);

    for ((block, idx), phase) in buffer.drain() {
        block_numbers.push(block);
        indices.push(idx);
        phases.push(phase);
    }

    sqlx::query(
        "UPDATE events e
         SET phase = u.new_phase, error = NULL
         FROM UNNEST($1::bigint[], $2::int[], $3::int[]) AS u(block_number, index, new_phase)
         WHERE e.block_number = u.block_number
           AND e.index = u.index
           AND e.phase < u.new_phase",
    )
    .bind(&block_numbers[..])
    .bind(&indices[..])
    .bind(&phases[..])
    .execute(db_pool)
    .await?;

    debug!("Flushed {} events phase updates", n);
    Ok(())
}
