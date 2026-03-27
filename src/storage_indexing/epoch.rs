//! Epoch storage indexing module
//!
//! Custom indexing logic for epoch-based snapshots of manager data.

use crate::entities::{EpochIndexPhase, EpochRow};
use crate::storage_indexing::{
    is_block_within_pruning_threshold, process_storage_rules, StorageIndexingContext, TriggerKind,
};
use crate::utils::ensure_hex_prefix;
use anyhow::anyhow;
use scale_value::At;
use serde_json::{json, Value as JsonValue};
use sqlx::{Pool, Postgres};
use subxt::config::PolkadotConfig;
use subxt::utils::{AccountId32, H256};
use subxt::OnlineClient;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

/// Collection ID for managers in the Uniques pallet
const MANAGERS_COLLECTION_ID: u128 = 0;

/// Hard-coded pallet name for ProcessorManager
const PROCESSOR_TO_MANAGER_PALLET: &str = "AcurastProcessorManager";
/// Hard-coded storage location for ProcessorToManagerIdIndex
const PROCESSOR_TO_MANAGER_STORAGE: &str = "ProcessorToManagerIdIndex";

/// Hard-coded pallet name for Uniques
const UNIQUES_PALLET: &str = "Uniques";
/// Hard-coded storage location for Asset
const UNIQUES_ASSET_STORAGE: &str = "Asset";

/// Hard-coded pallet name for AcurastCompute
const ACURAST_COMPUTE_PALLET: &str = "AcurastCompute";
/// Hard-coded storage location for Backings
const BACKINGS_STORAGE: &str = "Backings";

pub async fn process_epoch_storage_indexing(
    worker_id: u32,
    epoch: EpochRow,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    cancel_token: &CancellationToken,
) -> Result<(), anyhow::Error> {
    const PHASE: EpochIndexPhase = EpochIndexPhase::StorageIndexed2;
    debug!(
        worker = format!("epoch-storage-indexing-{:?}", worker_id),
        "Process phase {:?} for epoch {:?} (current phase: {:?})", PHASE, epoch.epoch, epoch.phase
    );

    // Get the block at epoch_start (beginning of epoch)
    let snapshot_block_number = epoch.epoch_start;

    // Get block hash from database
    let block_row = sqlx::query!(
        "SELECT hash, block_time FROM blocks WHERE block_number = $1",
        snapshot_block_number
    )
    .fetch_optional(db_pool)
    .await?;

    let (block_hash, block_time) = match block_row {
        Some(b) => {
            let hash = H256::from_slice(
                &hex::decode(&b.hash).map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
            );
            (hash, b.block_time)
        }
        None => {
            warn!(
                worker = format!("epoch-storage-indexing-{:?}", worker_id),
                "Block {} not found for epoch {} snapshot, skipping manager indexing",
                snapshot_block_number,
                epoch.epoch
            );
            update_epoch_phase(db_pool, epoch.epoch, PHASE).await?;
            return Ok(());
        }
    };

    // Get block for storage queries
    let block = client.blocks().at(block_hash).await?;

    info!(
        worker = format!("epoch-storage-indexing-{:?}", worker_id),
        "Indexing managers for epoch {} at block {}", epoch.epoch, snapshot_block_number
    );

    // Step 1: Get all processors that had at least one heartbeat in this epoch's block range
    // ProcessorHeartbeatWithVersion is pallet 41, variant 6
    // Join with extrinsics to get the account_id (processor address) directly
    let active_processors: Vec<(String,)> = sqlx::query_as(
        r#"SELECT DISTINCT e.account_id as processor_address
           FROM events ev
           JOIN extrinsics e ON e.block_number = ev.block_number
                            AND e.index = ev.extrinsic_index
           WHERE ev.block_number >= $1 AND ev.block_number < $2
           AND ev.pallet = 41 AND ev.variant = 6"#,
    )
    .bind(epoch.epoch_start)
    .bind(epoch.epoch_end.unwrap_or(epoch.epoch_start + 900)) // Default to 900 blocks if no end
    .fetch_all(db_pool)
    .await?;

    info!(
        worker = format!("epoch-storage-indexing-{:?}", worker_id),
        "Found {} active processors with heartbeats in epoch {} (block range {}-{:?})",
        active_processors.len(),
        epoch.epoch,
        epoch.epoch_start,
        epoch.epoch_end
    );

    // Step 2: For each processor, look up their manager_id from storage (in parallel batches)
    let total_processors = active_processors.len();
    const BATCH_SIZE: usize = 100;

    let mut manager_processors: std::collections::HashMap<u128, Vec<(AccountId32, String)>> =
        std::collections::HashMap::new();

    // Process processors in chunks of BATCH_SIZE
    for (chunk_idx, chunk) in active_processors.chunks(BATCH_SIZE).enumerate() {
        // Check for cancellation at the start of each chunk
        if cancel_token.is_cancelled() {
            info!(
                worker = format!("epoch-storage-indexing-{:?}", worker_id),
                "Cancelled during processor indexing for epoch {}", epoch.epoch
            );
            return Ok(());
        }

        let chunk_start = chunk_idx * BATCH_SIZE;
        if chunk_start % 1000 == 0 {
            debug!(
                worker = format!("epoch-storage-indexing-{:?}", worker_id),
                "Processing processors {}-{}/{} for epoch {}",
                chunk_start,
                chunk_start + chunk.len(),
                total_processors,
                epoch.epoch
            );
        }

        // Spawn all tasks in this chunk concurrently
        let mut tasks = tokio::task::JoinSet::new();

        for (processor_address,) in chunk {
            let processor_address = processor_address.clone();
            let block_storage = block.storage();
            let block_hash_copy = block_hash;

            tasks.spawn(async move {
                // Decode the processor address (hex string without 0x prefix, always 64 chars)
                let processor_bytes: [u8; 32] = match hex::decode(&processor_address) {
                    Ok(bytes) => match bytes.try_into() {
                        Ok(b) => b,
                        Err(_) => {
                            error!("Skipped processor-manager indexing because processor address has incorrect length: {:?}", &processor_address);
                            return None;
                        }
                    },
                    _ => {
                        error!("Skipped processor-manager indexing because processor address contains incorrect bytes: {:?}", &processor_address);
                        return None;
                    }
                };
                let account_id = AccountId32::from(processor_bytes);

                // Query ProcessorToManagerIdIndex for this processor using dynamic API
                let storage_query = subxt::dynamic::storage(
                    PROCESSOR_TO_MANAGER_PALLET,
                    PROCESSOR_TO_MANAGER_STORAGE,
                    vec![subxt::dynamic::Value::from_bytes(&account_id.0)],
                );

                match block_storage.fetch(&storage_query).await {
                    Ok(Some(manager_id_thunk)) => {
                        // Decode the manager_id from the dynamic value
                        let manager_id: u128 = match manager_id_thunk.to_value() {
                            Ok(value) => match value.as_u128() {
                                Some(id) => id,
                                None => {
                                    warn!(
                                        "Failed to decode manager_id as u128 for processor {}",
                                        processor_address
                                    );
                                    return None;
                                }
                            },
                            Err(e) => {
                                warn!(
                                    "Failed to decode manager_id thunk for processor {}: {:?}",
                                    processor_address, e
                                );
                                return None;
                            }
                        };
                        trace!(
                            "✅ Processor {} [0x{}] found in ProcessorToManagerIdIndex at block 0x{} -> manager_id={}",
                            account_id,
                            processor_address,
                            hex::encode(block_hash_copy),
                            manager_id
                        );
                        Some((manager_id, account_id, ensure_hex_prefix(&processor_address)))
                    }
                    Ok(None) => {
                        // Processor not found in index (which happens if the processor got only onboarded in the epoch)
                        trace!(
                            "❌ Processor {} [0x{}] NOT found in ProcessorToManagerIdIndex at block 0x{}",
                            account_id,
                            processor_address,
                            hex::encode(block_hash_copy)
                        );
                        None
                    }
                    Err(e) => {
                        warn!(
                            "Failed to fetch manager_id for processor {}: {:?}",
                            processor_address, e
                        );
                        None
                    }
                }
            });
        }

        // Wait for all tasks in this chunk to complete, with cancellation support
        loop {
            tokio::select! {
                biased;

                _ = cancel_token.cancelled() => {
                    info!(
                        worker = format!("epoch-storage-indexing-{:?}", worker_id),
                        "Cancelled while waiting for tasks in epoch {}", epoch.epoch
                    );
                    tasks.abort_all();
                    return Ok(());
                }

                result = tasks.join_next() => {
                    match result {
                        Some(Ok(Some((manager_id, account_id, address)))) => {
                            manager_processors
                                .entry(manager_id)
                                .or_default()
                                .push((account_id, address));
                        }
                        Some(_) => {} // Task returned None or error
                        None => break, // All tasks completed
                    }
                }
            }
        }
    }

    debug!(
        worker = format!("epoch-storage-indexing-{:?}", worker_id),
        "Found {} unique managers",
        manager_processors.len()
    );

    // Step 3: For each manager, get additional data and insert
    for (manager_id, processors) in manager_processors {
        // Get manager address from Uniques pallet (collection 0, item = manager_id)
        let manager_address =
            get_collection_owner(&block, MANAGERS_COLLECTION_ID, manager_id).await;

        // Get commitment info from Backings storage
        let commitment_id = get_backing_info(&block, manager_id).await;

        // Get metrics for all processors of this manager in one query
        let processor_addresses: Vec<String> =
            processors.iter().map(|(_, addr)| addr.clone()).collect();
        let metrics_map = get_all_processor_metrics_from_db(
            db_pool,
            &processor_addresses,
            epoch.epoch_start,
            epoch.epoch_end.unwrap_or(epoch.epoch_start + 900),
            epoch.epoch,
        )
        .await;

        // Insert into managers table
        let result = sqlx::query(
            r#"
            INSERT INTO managers (epoch, block_number, block_time, manager_id, manager_address, commitment_id, processors)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (epoch, manager_id) DO NOTHING
            "#,
        )
        .bind(epoch.epoch)
        .bind(snapshot_block_number)
        .bind(block_time)
        .bind(manager_id as i64)
        .bind(manager_address.unwrap_or_default())
        .bind(commitment_id.map(|c| c as i64))
        .bind(json!(metrics_map))
        .execute(db_pool)
        .await;

        match result {
            Ok(_) => {
                debug!("Inserted manager {} for epoch {}", manager_id, epoch.epoch);
            }
            Err(e) => {
                warn!(
                    "Failed to insert manager {} for epoch {}: {:?}",
                    manager_id, epoch.epoch, e
                );
            }
        }
    }

    info!(
        worker = format!("epoch-storage-indexing-{:?}", worker_id),
        "Completed manager indexing for epoch {}", epoch.epoch
    );

    // Update epoch phase to StorageIndexed
    update_epoch_phase(db_pool, epoch.epoch, PHASE).await?;

    Ok(())
}

/// Generic epoch storage indexing using configured epoch rules.
/// Similar to process_extrinsic_storage_indexing but triggered by epoch transitions.
/// The `target_phase` indicates which phase rules to process and what phase to advance to.
/// If `finalized_block` is provided, rules with pruning configs are skipped for old epochs.
///
/// Rules can be configured with `epoch_snapshot_at` to snapshot at:
/// - `start`: epoch start block (default)
/// - `end`: epoch end block (next epoch's start - 1)
/// - `both`: both start and end blocks
pub async fn process_epoch_storage_rules_indexing(
    worker_id: u32,
    epoch: EpochRow,
    target_phase: EpochIndexPhase,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    finalized_block: Option<u32>,
) -> Result<(), anyhow::Error> {
    use crate::config::EpochSnapshotTiming;

    let phase_num = target_phase as u32;
    trace!(
        worker = format!("epoch-storage-rules-{:?}", worker_id),
        "Processing epoch storage rules phase {} for epoch {} (current phase: {:?})",
        phase_num,
        epoch.epoch,
        epoch.phase
    );

    let all_epoch_rules =
        crate::config::storage_rules().by_trigger_and_phase(TriggerKind::Epoch, phase_num);

    // Filter rules based on pruning threshold (skip old epochs for rules with pruning)
    let epoch_rules: Vec<_> = all_epoch_rules
        .iter()
        .filter(|rule| {
            if let Some(finalized) = finalized_block {
                let within_threshold = is_block_within_pruning_threshold(
                    epoch.epoch_start as u32,
                    finalized,
                    &rule.pruning,
                );
                if !within_threshold {
                    trace!(
                        "Skipping epoch rule '{}' for epoch {} (block {}) - outside pruning threshold",
                        rule.name, epoch.epoch, epoch.epoch_start
                    );
                }
                within_threshold
            } else {
                true // No finalized block info, process anyway
            }
        })
        .cloned()
        .collect();

    if epoch_rules.is_empty() {
        trace!(
            worker = format!("epoch-storage-rules-{:?}", worker_id),
            "No phase {} epoch rules for epoch {} (after pruning filter)",
            phase_num,
            epoch.epoch
        );
        update_epoch_phase(db_pool, epoch.epoch, target_phase).await?;
        return Ok(());
    }

    // Group rules by snapshot timing
    let start_rules: Vec<_> = epoch_rules
        .iter()
        .filter(|r| {
            r.epoch_snapshot_at == EpochSnapshotTiming::Start
                || r.epoch_snapshot_at == EpochSnapshotTiming::Both
        })
        .cloned()
        .collect();

    let end_rules: Vec<_> = epoch_rules
        .iter()
        .filter(|r| {
            r.epoch_snapshot_at == EpochSnapshotTiming::End
                || r.epoch_snapshot_at == EpochSnapshotTiming::Both
        })
        .cloned()
        .collect();

    // Process rules for epoch START
    if !start_rules.is_empty() {
        let snapshot_block_number = epoch.epoch_start;

        let block_row = sqlx::query!(
            "SELECT hash FROM blocks WHERE block_number = $1",
            snapshot_block_number
        )
        .fetch_optional(db_pool)
        .await?;

        if let Some(b) = block_row {
            let block_hash = H256::from_slice(
                &hex::decode(&b.hash).map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
            );
            let block = client.blocks().at(block_hash).await?;

            let ctx = StorageIndexingContext {
                block_number: snapshot_block_number,
                extrinsic_index: epoch.epoch as i32,
                event_index: None,
                data: None,
                account_id: String::new(),
                block_time: epoch.epoch_start_time,
            };

            process_storage_rules(worker_id, &ctx, &start_rules, db_pool, client, &block).await?;

            debug!(
                worker = format!("epoch-storage-rules-{:?}", worker_id),
                "Completed epoch START snapshot phase {} for epoch {} at block {} ({} rules)",
                phase_num,
                epoch.epoch,
                snapshot_block_number,
                start_rules.len()
            );
        } else {
            warn!(
                "Block {} not found in database for epoch {} START snapshot, skipping",
                snapshot_block_number, epoch.epoch
            );
        }
    }

    // Process rules for epoch END (requires epoch_end to be known)
    if !end_rules.is_empty() {
        if let Some(epoch_end) = epoch.epoch_end {
            // End block is the last block of this epoch (next epoch's start - 1)
            let snapshot_block_number = epoch_end - 1;

            let block_row = sqlx::query!(
                "SELECT hash, block_time FROM blocks WHERE block_number = $1",
                snapshot_block_number
            )
            .fetch_optional(db_pool)
            .await?;

            if let Some(b) = block_row {
                let block_hash = H256::from_slice(
                    &hex::decode(&b.hash)
                        .map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
                );
                let block = client.blocks().at(block_hash).await?;

                let ctx = StorageIndexingContext {
                    block_number: snapshot_block_number,
                    extrinsic_index: epoch.epoch as i32,
                    event_index: Some(-1), // Use -1 to distinguish end snapshots from start
                    data: None,
                    account_id: String::new(),
                    block_time: b.block_time,
                };

                process_storage_rules(worker_id, &ctx, &end_rules, db_pool, client, &block).await?;

                debug!(
                    worker = format!("epoch-storage-rules-{:?}", worker_id),
                    "Completed epoch END snapshot phase {} for epoch {} at block {} ({} rules)",
                    phase_num,
                    epoch.epoch,
                    snapshot_block_number,
                    end_rules.len()
                );
            } else {
                warn!(
                    "Block {} not found in database for epoch {} END snapshot, skipping",
                    snapshot_block_number, epoch.epoch
                );
            }
        } else {
            warn!(
                "Epoch {} has no epoch_end set, cannot process END snapshot rules",
                epoch.epoch
            );
        }
    }

    debug!(
        worker = format!("epoch-storage-rules-{:?}", worker_id),
        "Completed epoch storage rules phase {} for epoch {} ({} rules)",
        phase_num,
        epoch.epoch,
        epoch_rules.len()
    );

    // Update to target phase
    update_epoch_phase(db_pool, epoch.epoch, target_phase).await?;

    Ok(())
}

async fn update_epoch_phase(
    db_pool: &Pool<Postgres>,
    epoch: i64,
    phase: EpochIndexPhase,
) -> Result<(), anyhow::Error> {
    sqlx::query("UPDATE epochs SET phase = $1 WHERE epoch = $2;")
        .bind(phase as i32)
        .bind(epoch)
        .execute(db_pool)
        .await?;

    debug!("Epoch {} phase updated to {:?}", epoch, phase);
    Ok(())
}

/// Get the owner of a Uniques collection item using dynamic query
async fn get_collection_owner(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    collection_id: u128,
    item_id: u128,
) -> Option<String> {
    let storage_query = subxt::dynamic::storage(
        UNIQUES_PALLET,
        UNIQUES_ASSET_STORAGE,
        vec![
            subxt::dynamic::Value::u128(collection_id),
            subxt::dynamic::Value::u128(item_id),
        ],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(item_details_thunk)) => {
            // ItemDetails has an "owner" field of type AccountId32
            match item_details_thunk.to_value() {
                Ok(value) => {
                    // Navigate to the "owner" field and extract bytes
                    if let Some(owner_value) = value.at("owner") {
                        // Try different representations of AccountId32
                        match &owner_value.value {
                            scale_value::ValueDef::Composite(composite) => {
                                // Try to extract bytes from composite - check for nested structure
                                // AccountId32 might be wrapped in a newtype struct with [u8; 32] inside
                                for field_value in composite.values() {
                                    if let scale_value::ValueDef::Composite(inner) =
                                        &field_value.value
                                    {
                                        let bytes: Vec<u8> = inner
                                            .values()
                                            .filter_map(|v| v.as_u128().map(|n| n as u8))
                                            .collect();
                                        if bytes.len() == 32 {
                                            return Some(hex::encode(&bytes));
                                        }
                                    }
                                }
                                // Fallback: try direct values
                                let bytes: Vec<u8> = composite
                                    .values()
                                    .filter_map(|v| v.as_u128().map(|n| n as u8))
                                    .collect();
                                if bytes.len() == 32 {
                                    return Some(hex::encode(&bytes));
                                }
                                debug!(
                                    "Owner composite structure for item {}: {:?}",
                                    item_id, composite
                                );
                            }
                            scale_value::ValueDef::Primitive(scale_value::Primitive::U256(
                                bytes,
                            )) => {
                                // AccountId32 might be stored as U256
                                return Some(hex::encode(&bytes[..32]));
                            }
                            other => {
                                debug!(
                                    "Owner value for item {} is not Composite or U256, it's: {:?}",
                                    item_id,
                                    std::mem::discriminant(other)
                                );
                            }
                        }
                    }
                    None
                }
                Err(e) => {
                    debug!(
                        "Failed to decode Asset for collection {}, item {}: {:?}",
                        collection_id, item_id, e
                    );
                    None
                }
            }
        }
        Ok(None) => {
            debug!(
                "No Asset found for collection {}, item {}",
                collection_id, item_id
            );
            None
        }
        Err(e) => {
            debug!(
                "Failed to fetch Asset for collection {}, item {}: {:?}",
                collection_id, item_id, e
            );
            None
        }
    }
}

/// Get backing commitment_id for a manager using dynamic query
async fn get_backing_info(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    manager_id: u128,
) -> Option<u128> {
    let storage_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        BACKINGS_STORAGE,
        vec![subxt::dynamic::Value::u128(manager_id)],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(commitment_id_thunk)) => match commitment_id_thunk.to_value() {
            Ok(value) => value.as_u128(),
            Err(e) => {
                debug!(
                    "Failed to decode Backings commitment_id for manager {}: {:?}",
                    manager_id, e
                );
                None
            }
        },
        Ok(None) => None,
        Err(e) => {
            debug!(
                "Failed to fetch Backings for manager {}: {:?}",
                manager_id, e
            );
            None
        }
    }
}

/// Get metrics for all processors of a manager from the database in one query.
/// Returns a HashMap of processor address -> { pool_id: metric_value, ... }
/// Queries the last metric snapshot within the epoch block range for each processor,
/// filtering to only include entries matching the target epoch.
async fn get_all_processor_metrics_from_db(
    db_pool: &Pool<Postgres>,
    processor_addresses: &[String],
    epoch_start: i64,
    epoch_end: i64,
    target_epoch: i64,
) -> std::collections::HashMap<String, JsonValue> {
    if processor_addresses.is_empty() {
        return std::collections::HashMap::new();
    }

    // Normalize addresses: lowercase, no 0x prefix
    let normalized_addresses: Vec<String> = processor_addresses
        .iter()
        .map(|a| a.to_lowercase().trim_start_matches("0x").to_string())
        .collect();

    // Query last metric snapshot for each processor within the epoch block range
    // storage_keys->>0 extracts the first element (processor address) from the JSONB array
    // We use DISTINCT ON with DESC ordering to get the last (most recent) metric per processor
    #[derive(sqlx::FromRow)]
    struct MetricRow {
        processor_addr: String,
        data: JsonValue,
    }

    let rows: Vec<MetricRow> = match sqlx::query_as(
        r#"SELECT DISTINCT ON (storage_keys) storage_keys->>0 as processor_addr, data
           FROM storage_snapshots
           WHERE pallet = 48
           AND storage_location = 'Metrics'
           AND block_number >= $1 AND block_number < $2
           AND storage_keys->>0 = ANY($3)
           ORDER BY storage_keys, block_number DESC"#,
    )
    .bind(epoch_start)
    .bind(epoch_end)
    .bind(&normalized_addresses)
    .fetch_all(db_pool)
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            debug!("Failed to fetch processor metrics from db: {:?}", e);
            return std::collections::HashMap::new();
        }
    };

    let target_epoch_str = target_epoch.to_string();

    // Transform data to simplified format: { pool_id: metric_value, ... }
    let mut result: std::collections::HashMap<String, JsonValue> = std::collections::HashMap::new();

    for row in rows {
        // Parse data array: [{"key": "pool_id", "value": {"epoch": "...", "metric": ["..."]}}, ...]
        let mut pool_metrics = serde_json::Map::new();

        if let Some(data_array) = row.data.as_array() {
            for entry in data_array {
                // Extract pool_id from "key"
                let pool_id = entry.get("key").and_then(|k| k.as_str());

                // Extract epoch and metric from "value"
                let value = entry.get("value");
                let epoch = value.and_then(|v| v.get("epoch")).and_then(|e| e.as_str());
                let metric = value
                    .and_then(|v| v.get("metric"))
                    .and_then(|m| m.as_array())
                    .and_then(|arr| arr.first())
                    .and_then(|v| v.as_str());

                // Only include if epoch matches target epoch
                if let (Some(pool_id), Some(epoch), Some(metric)) = (pool_id, epoch, metric) {
                    if epoch == target_epoch_str {
                        pool_metrics
                            .insert(pool_id.to_string(), JsonValue::String(metric.to_string()));
                    }
                }
            }
        }

        if !pool_metrics.is_empty() {
            result.insert(
                ensure_hex_prefix(&row.processor_addr),
                JsonValue::Object(pool_metrics),
            );
        }
    }

    result
}
