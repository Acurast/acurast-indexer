//! Extrinsic indexing phase processing.
//!
//! This module handles the phase-based processing of extrinsics:
//! - Queuing extrinsics that need processing
//! - Extracting addresses from extrinsics

use std::collections::HashMap;
use std::time::Duration;

use anyhow::anyhow;
use async_channel::Sender;
use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{Pool, Postgres};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::entities::{ExtrinsicRow, ExtrinsicsIndexPhase};
use crate::task_monitor::{QueueType, TaskGuard, TASK_REGISTRY};
use crate::AppError;

/// Queue extrinsics that need phase processing.
pub async fn queue_extrinsics_phase(
    tx: Sender<ExtrinsicRow>,
    db_pool: Pool<Postgres>,
    cancel_token: CancellationToken,
    index_from_block: u32,
) -> Result<(), anyhow::Error> {
    info!(
        "queue_extrinsics_phase: starting, index_from_block={}",
        index_from_block
    );
    let task = TaskGuard::new("Queue extrinsics", None);

    // Track the min and max (block_number, index) that have been queued
    let mut min_queued_key: Option<(i64, i32)> = None;
    let mut max_queued_key: Option<(i64, i32)> = None;
    let mut wait = 0u64;

    'outer: loop {
        debug!(
            "Search for extrinsics in any phase < {:?}",
            ExtrinsicsIndexPhase::MAX
        );

        tokio::select! {
            biased;

            _ = cancel_token.cancelled() =>
                break 'outer
            ,
            _ = tokio::time::sleep(Duration::from_secs(wait)) => {
            },
        }

        trace!(
            "queue_extrinsics_phase: querying DB, min_key={:?}, max_key={:?}",
            min_queued_key,
            max_queued_key
        );
        let query_start = std::time::Instant::now();
        let extrinsics: Vec<ExtrinsicRow> = if let (Some(ref min_key), Some(ref max_key)) =
            (&min_queued_key, &max_queued_key)
        {
            // Exclude items within the already queued range
            sqlx::query_as(
                "SELECT block_number, index, pallet, method, data, tx_hash, account_id, block_time, phase FROM extrinsics
                 WHERE phase < $1 AND NOT ((block_number, index) >= ($2, $3) AND (block_number, index) <= ($4, $5))
                 AND block_number >= $6
                 ORDER BY block_number DESC, index DESC
                 LIMIT 1000",
            )
            .bind(ExtrinsicsIndexPhase::MAX as i32)
            .bind(min_key.0)
            .bind(min_key.1)
            .bind(max_key.0)
            .bind(max_key.1)
            .bind(index_from_block as i32)
            .fetch_all(&db_pool)
            .await
            .map_err(|e| {error!("{:?}", e); AppError::InternalError(e.into())})?
        } else {
            // First iteration, no exclusion needed
            sqlx::query_as(
                "SELECT block_number, index, pallet, method, data, tx_hash, account_id, block_time, phase FROM extrinsics
                 WHERE phase < $1 AND block_number >= $2
                 ORDER BY block_number DESC, index DESC
                 LIMIT 100",
            )
            .bind(ExtrinsicsIndexPhase::MAX as i32)
            .bind(index_from_block as i32)
            .fetch_all(&db_pool)
            .await
            .map_err(|e| AppError::InternalError(e.into()))?
        };
        trace!(
            "queue_extrinsics_phase: query returned {} rows in {:?}",
            extrinsics.len(),
            query_start.elapsed()
        );

        // Update the queued range with this batch
        let first_key = extrinsics.first().map(|e| (e.block_number, e.index));
        let last_key = extrinsics.last().map(|e| (e.block_number, e.index));

        if let (Some(first), Some(last)) = (first_key, last_key) {
            // Update min and max: since we're ordering DESC, first is max, last is min
            let old_max = max_queued_key;
            let old_min = min_queued_key;
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
            trace!(
                "queue_extrinsics_phase: updated range old=[{:?}, {:?}] -> new=[{:?}, {:?}]",
                old_min,
                old_max,
                min_queued_key,
                max_queued_key
            );

            debug!(
                "Found extrinsics in phase < {:?} outside done range [{}-{}, {}-{}]",
                ExtrinsicsIndexPhase::MAX,
                min_queued_key.clone().unwrap().0,
                min_queued_key.clone().unwrap().1,
                max_queued_key.clone().unwrap().0,
                max_queued_key.clone().unwrap().1
            );

            // Publish queue range to task registry for monitoring (only when we have values)
            let min_key = min_queued_key.unwrap();
            let max_key = max_queued_key.unwrap();
            TASK_REGISTRY.set_queue_range(
                QueueType::Extrinsic,
                format!("{}-{}", min_key.0, min_key.1),
                format!("{}-{}", max_key.0, max_key.1),
            );
        }

        if extrinsics.is_empty() {
            wait = 10;
        } else {
            wait = 0;
        }

        // queue each row
        for row in extrinsics {
            if cancel_token.is_cancelled() {
                break 'outer;
            }

            trace!(
                "queue_extrinsics_phase: sending extrinsic {}-{} phase={}",
                row.block_number,
                row.index,
                row.phase as i32
            );
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() =>
                    break 'outer
                ,
                result = tx.send(row) =>
                    result?
            }
        }
    }

    task.complete();
    Ok(())
}

/// Process extrinsic phase: extract addresses from extrinsic data.
pub async fn process_extrinsic_extract_addresses(
    worker_id: u32,
    extrinsic: ExtrinsicRow,
    extrinsic_transformations: &HashMap<
        u32,
        Vec<crate::config::AddressFromExtrinsicTransformation>,
    >,
    pallet_method_map: &HashMap<(String, String), (u32, u32)>,
    db_pool: &Pool<Postgres>,
    tx: &Sender<ExtrinsicRow>,
    cancel_token: &CancellationToken,
) -> Result<(), anyhow::Error> {
    const PHASE: ExtrinsicsIndexPhase = ExtrinsicsIndexPhase::AddressExtracted;
    debug!(
        "Process phase {:?} for extrinsic {:?}",
        PHASE,
        extrinsic.id()
    );

    let mut col_extrinsic_block_number: Vec<i64> = vec![];
    let mut col_extrinsic_index: Vec<i32> = vec![];
    let mut col_batch_index: Vec<Option<i32>> = vec![];
    let mut col_data_path: Vec<String> = vec![];
    let mut col_resolved_data_path: Vec<String> = vec![];
    let mut col_account_id: Vec<String> = vec![];
    let mut col_pallet: Vec<i32> = vec![];
    let mut col_method: Vec<i32> = vec![];
    let mut col_block_time: Vec<DateTime<Utc>> = vec![];

    // Extract calls, unwrapping batch calls if present
    let empty_value = Value::Null;
    let data_ref = extrinsic.data.as_ref().unwrap_or(&empty_value);
    let (calls, is_batch) = crate::data_extraction::extract_calls(
        extrinsic.pallet as u32,
        extrinsic.method as u32,
        data_ref,
        pallet_method_map,
    );

    // Build transformation map
    let mut t_map: HashMap<(u32, u32), Vec<String>> = HashMap::new();
    if let Some(transformations) = extrinsic_transformations.get(&1) {
        for t in transformations {
            t_map
                .entry((t.pallet, t.method))
                .or_default()
                .push(t.data_path.clone());
        }
    }

    // Process each call (will be multiple if it was a batch)
    for (batch_idx, call_info) in calls.iter().enumerate() {
        if let Some(data_paths) = t_map.get(&(call_info.pallet, call_info.method)) {
            for data_path in data_paths.clone().into_iter() {
                let address_values_with_paths =
                    match crate::data_extraction::resolve_json_path_with_resolved_paths(
                        &call_info.data,
                        &data_path,
                    ) {
                        Ok(values) => values,
                        Err(e) => {
                            warn!(
                                worker = format!("phase-{:?}", worker_id),
                                "Extraction error for path {:?} in call data (pallet={}, method={}): {}",
                                data_path, call_info.pallet, call_info.method, e
                            );
                            continue;
                        }
                    };
                debug!(
                    "resolved_json_path {:?} to {:?} results",
                    &data_path,
                    address_values_with_paths.len()
                );
                // Empty result from empty arrays is valid, just skip
                if address_values_with_paths.is_empty() {
                    continue;
                }

                for (address_value, resolved_path) in address_values_with_paths {
                    let account_id = crate::data_extraction::extract_account_address(address_value);
                    col_account_id.push(crate::utils::strip_hex_prefix(&account_id));
                    col_extrinsic_block_number.push(extrinsic.block_number);
                    col_extrinsic_index.push(extrinsic.index);
                    col_data_path.push(data_path.to_owned());
                    col_resolved_data_path.push(resolved_path);
                    col_pallet.push(call_info.pallet as i32);
                    col_method.push(call_info.method as i32);
                    col_batch_index.push(if is_batch {
                        Some(batch_idx as i32)
                    } else {
                        None
                    });
                    col_block_time.push(extrinsic.block_time);
                }
            }
        }
    }

    if !col_extrinsic_block_number.is_empty() {
        sqlx::query(
            "
                INSERT INTO extrinsic_address(block_number, extrinsic_index, batch_index, data_path, resolved_data_path, account_id, pallet, method, block_time)
                SELECT * FROM UNNEST($1::bigint[], $2::int[], $3::int[], $4::text[], $5::text[], $6::text[], $7::int[], $8::int[], $9::timestamptz[])
                ON CONFLICT (block_number, extrinsic_index, batch_index, resolved_data_path) DO NOTHING
            ",
        )
        .bind(&col_extrinsic_block_number[..])
        .bind(&col_extrinsic_index[..])
        .bind(&col_batch_index[..] as &[Option<i32>])
        .bind(&col_data_path[..])
        .bind(&col_resolved_data_path[..])
        .bind(&col_account_id[..])
        .bind(&col_pallet[..])
        .bind(&col_method[..])
        .bind(&col_block_time[..])
        .execute(db_pool)
        .await?;

        debug!(
            worker = format!("phase-{:?}", worker_id),
            "Extracted {:?} addresses for extrinsic {:?}",
            col_extrinsic_block_number.len(),
            extrinsic.id()
        );
    }

    sqlx::query(
        "
            UPDATE extrinsics
            SET phase = $1
            WHERE block_number = $2 AND index = $3;
        ",
    )
    .bind(PHASE as i32)
    .bind(extrinsic.block_number)
    .bind(extrinsic.index)
    .execute(db_pool)
    .await?;

    // Re-queue for next phase if not at max phase
    let next_phase = PHASE as u32;
    if next_phase < ExtrinsicsIndexPhase::MAX {
        // Create updated extrinsic with new phase
        let mut updated_extrinsic = extrinsic.clone();
        updated_extrinsic.phase = PHASE;

        tokio::select! {
            biased;
            _ = cancel_token.cancelled() => {},
            result = tx.send(updated_extrinsic) => {
                result.map_err(|e| anyhow!("Failed to re-queue extrinsic: {:?}", e))?;
            }
        }
    }

    Ok(())
}
