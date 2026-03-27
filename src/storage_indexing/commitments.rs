//! Commitment processing module
//!
//! Processes storage snapshots to extract commitment data into a denormalized table
//! for efficient querying via RPC.

use anyhow::anyhow;
use bigdecimal::BigDecimal;
use parity_scale_codec::Decode;
use scale_value::At;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::str::FromStr;
use subxt::config::PolkadotConfig;
use subxt::utils::H256;
use subxt::OnlineClient;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::task_monitor::TASK_REGISTRY;

/// The Cycle struct from AcurastCompute pallet (for fetching current epoch)
#[derive(Debug, Decode)]
struct Cycle {
    pub epoch: u32,
    #[allow(dead_code)]
    pub epoch_start: u32,
}

/// Collection ID for commitments in the Uniques pallet
const COMMITMENTS_COLLECTION_ID: u128 = 1;
/// Collection ID for managers in the Uniques pallet
const MANAGERS_COLLECTION_ID: u128 = 0;

/// Hard-coded pallet name for Uniques
const UNIQUES_PALLET: &str = "Uniques";
/// Hard-coded storage location for Asset
const UNIQUES_ASSET_STORAGE: &str = "Asset";

/// Hard-coded pallet name for AcurastCompute
const ACURAST_COMPUTE_PALLET: &str = "AcurastCompute";
/// Hard-coded storage location for Backings
const BACKINGS_STORAGE: &str = "Backings";
/// Hard-coded storage location for CurrentCycle (epoch info)
const CURRENT_CYCLE_STORAGE: &str = "CurrentCycle";

/// Commitment snapshot data needed for processing
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct CommitmentSnapshot {
    pub id: i64,
    pub block_number: i64,
    pub block_time: chrono::DateTime<chrono::Utc>,
    pub storage_keys: JsonValue,
    pub data: JsonValue,
}

/// Find unprocessed commitment snapshots that are newer than existing commitments.
/// Returns snapshot IDs for incremental processing (after initial sync).
/// Only considers snapshots at or after `min_block` if provided.
pub async fn find_unprocessed_commitment_snapshots(
    db_pool: &Pool<Postgres>,
    min_block: Option<i64>,
    batch_size: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    // Get distinct commitment snapshots ordered by block_number DESC
    // This query gets the latest snapshot for each commitment_id that hasn't been processed
    // Note: storage_keys may be nested arrays like [["449"]] so we use ->0->>0 to extract
    let snapshot_ids: Vec<(i64,)> = sqlx::query_as(
        r#"
        WITH latest_snapshots AS (
            SELECT DISTINCT ON (storage_keys->0->>0)
                s.id
            FROM storage_snapshots s
            LEFT JOIN commitments c ON c.commitment_id = (s.storage_keys->0->>0)::BIGINT
            WHERE s.pallet = 48
            AND s.storage_location = 'Commitments'
            AND s.data IS NOT NULL
            AND s.data != 'null'::jsonb
            AND (c.id IS NULL OR s.block_number > c.block_number)
            AND ($1::BIGINT IS NULL OR s.block_number >= $1)
            ORDER BY storage_keys->0->>0, s.block_number DESC
        )
        SELECT id FROM latest_snapshots
        LIMIT $2
        "#,
    )
    .bind(min_block)
    .bind(batch_size)
    .fetch_all(db_pool)
    .await?;

    Ok(snapshot_ids.into_iter().map(|(id,)| id).collect())
}

/// Find all commitment snapshots at or before a specific block for initial full sync.
/// Returns the latest snapshot for each commitment_id up to that block.
pub async fn find_commitment_snapshots_at_block(
    db_pool: &Pool<Postgres>,
    block_number: i64,
    batch_size: i64,
    offset: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    let snapshot_ids: Vec<(i64,)> = sqlx::query_as(
        r#"
        WITH latest_snapshots AS (
            SELECT DISTINCT ON (storage_keys->0->>0)
                s.id
            FROM storage_snapshots s
            WHERE s.pallet = 48
            AND s.storage_location = 'Commitments'
            AND s.data IS NOT NULL
            AND s.data != 'null'::jsonb
            AND s.block_number <= $1
            ORDER BY storage_keys->0->>0, s.block_number DESC
        )
        SELECT id FROM latest_snapshots
        ORDER BY id
        LIMIT $2 OFFSET $3
        "#,
    )
    .bind(block_number)
    .bind(batch_size)
    .bind(offset)
    .fetch_all(db_pool)
    .await?;

    Ok(snapshot_ids.into_iter().map(|(id,)| id).collect())
}

/// Fetch commitment snapshots by their IDs.
pub async fn fetch_commitment_snapshots(
    db_pool: &Pool<Postgres>,
    snapshot_ids: &[i64],
) -> Result<Vec<CommitmentSnapshot>, anyhow::Error> {
    if snapshot_ids.is_empty() {
        return Ok(vec![]);
    }

    let snapshots: Vec<CommitmentSnapshot> = sqlx::query_as(
        r#"
        SELECT id, block_number, block_time, storage_keys, data
        FROM storage_snapshots
        WHERE id = ANY($1)
        "#,
    )
    .bind(snapshot_ids)
    .fetch_all(db_pool)
    .await?;

    Ok(snapshots)
}

/// Process a list of commitment snapshot IDs.
/// Looks up addresses from chain storage and upserts into commitments table.
pub async fn process_commitment_snapshot_ids(
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    snapshot_ids: &[i64],
) -> Result<u64, anyhow::Error> {
    if snapshot_ids.is_empty() {
        trace!("No commitment snapshots to process");
        return Ok(0);
    }

    // Fetch the actual snapshot data
    let snapshots = fetch_commitment_snapshots(db_pool, snapshot_ids).await?;

    if snapshots.is_empty() {
        trace!("No commitment snapshots found for given IDs");
        return Ok(0);
    }

    info!("Processing {} commitment snapshots", snapshots.len());

    // Get a block hash for RPC lookups (use the highest block number from snapshots)
    let max_block_number = snapshots.iter().map(|s| s.block_number).max().unwrap_or(0);
    let block_row = sqlx::query!(
        "SELECT hash FROM blocks WHERE block_number = $1",
        max_block_number
    )
    .fetch_optional(db_pool)
    .await?;

    let block_hash = match block_row {
        Some(b) => H256::from_slice(
            &hex::decode(&b.hash).map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
        ),
        None => {
            warn!(
                "Block {} not found for commitment processing, skipping batch",
                max_block_number
            );
            return Ok(0);
        }
    };

    let block = client.blocks().at(block_hash).await?;

    process_commitment_snapshots_with_block(db_pool, &block, snapshots).await
}

/// Process commitment snapshots using a specific block for chain lookups.
async fn process_commitment_snapshots_with_block(
    db_pool: &Pool<Postgres>,
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    snapshots: Vec<CommitmentSnapshot>,
) -> Result<u64, anyhow::Error> {
    let mut processed = 0u64;

    for snapshot in snapshots {
        // Extract commitment_id from storage_keys
        // Handle nested arrays like [["449"]] or flat arrays like ["449"]
        // Also handle hex strings like "0x7c" -> 124
        let commitment_id = match &snapshot.storage_keys {
            JsonValue::Array(keys) if !keys.is_empty() => {
                // Check if first element is itself an array (nested structure)
                match &keys[0] {
                    JsonValue::Array(nested) if !nested.is_empty() => {
                        parse_commitment_id(&nested[0])
                    }
                    JsonValue::Array(_) => {
                        // Empty nested array - skip silently
                        None
                    }
                    other => parse_commitment_id(other),
                }
            }
            _ => None,
        };

        let commitment_id = match commitment_id {
            Some(id) => id,
            None => {
                // Only warn for non-empty storage_keys - empty arrays are expected for cleared storage
                if !matches!(&snapshot.storage_keys, JsonValue::Array(arr) if arr.is_empty() || matches!(arr.first(), Some(JsonValue::Array(inner)) if inner.is_empty()))
                {
                    warn!(
                        "Could not extract commitment_id from snapshot {}: {:?}",
                        snapshot.id, snapshot.storage_keys
                    );
                }
                continue;
            }
        };

        // Get epoch for this snapshot's block
        let epoch: Option<(i64,)> = sqlx::query_as(
            "SELECT epoch FROM epochs WHERE epoch_start <= $1 ORDER BY epoch DESC LIMIT 1",
        )
        .bind(snapshot.block_number)
        .fetch_optional(db_pool)
        .await?;

        let epoch = match epoch {
            Some((e,)) => e,
            None => {
                warn!(
                    "Could not find epoch for snapshot {} at block {}, skipping",
                    snapshot.id, snapshot.block_number
                );
                continue;
            }
        };

        // Process the commitment using shared logic
        match process_single_commitment(
            db_pool,
            block,
            commitment_id,
            Some(snapshot.id),
            &snapshot.data,
            snapshot.block_number,
            snapshot.block_time,
            epoch,
        )
        .await
        {
            Ok(true) => processed += 1,
            Ok(false) => {
                // Skipped (e.g., no committer found) - warn for snapshot processing
                warn!(
                    "Could not find committer for commitment {}, skipping",
                    commitment_id
                );
            }
            Err(e) => {
                warn!("Failed to process commitment {}: {:?}", commitment_id, e);
            }
        }
    }

    info!("Processed {} commitments", processed);
    Ok(processed)
}

/// Scan all commitments directly from chain storage at a specific block.
/// Used for initial full sync to capture all existing commitments.
/// If task_id is provided, updates the task monitor with progress.
/// Returns true if scan completed, false if cancelled or skipped.
pub async fn scan_all_commitments_at_block(
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    block_hash: H256,
    task_id: Option<u64>,
    cancel_token: &CancellationToken,
) -> Result<bool, anyhow::Error> {
    // Get block from chain
    let block = client.blocks().at(block_hash).await?;
    let block_number = block.number() as i64;

    info!(
        "Scanning all commitments from chain storage at block {} ({})",
        block_number, block_hash
    );

    // Get block time from chain (Timestamp::Now storage)
    let block_time_millis: u64 = block
        .storage()
        .fetch(&subxt::dynamic::storage("Timestamp", "Now", vec![]))
        .await?
        .ok_or_else(|| anyhow!("Could not fetch timestamp for block {}", block_number))?
        .as_type()?;
    let block_time = chrono::DateTime::from_timestamp_millis(block_time_millis as i64)
        .unwrap_or_else(chrono::Utc::now);

    // Get epoch directly from chain storage (CurrentCycle)
    let cycle_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        CURRENT_CYCLE_STORAGE,
        Vec::<subxt::dynamic::Value>::new(),
    );

    let epoch = match block.storage().fetch(&cycle_query).await {
        Ok(Some(value)) => {
            let cycle = Cycle::decode(&mut value.encoded())
                .map_err(|e| anyhow!("Failed to decode CurrentCycle: {}", e))?;
            cycle.epoch as i64
        }
        Ok(None) => {
            warn!("CurrentCycle storage is empty at block {}", block_number);
            return Ok(false);
        }
        Err(e) => {
            warn!(
                "Failed to fetch CurrentCycle at block {}: {:?}",
                block_number, e
            );
            return Ok(false);
        }
    };

    // Iterate over all Commitments in storage
    let storage_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        "Commitments",
        Vec::<subxt::dynamic::Value>::new(),
    );

    let mut processed = 0u64;
    let mut iter = block.storage().iter(storage_query).await?;

    while let Some(Ok(kv)) = iter.next().await {
        if cancel_token.is_cancelled() {
            info!("Commitment scan cancelled after {} commitments", processed);
            return Ok(false);
        }
        // Extract commitment_id from key (first key in the storage map)
        let commitment_id = match kv.keys.first() {
            Some(key) => match key.as_u128() {
                Some(id) => id as i64,
                None => {
                    warn!("Could not parse commitment_id from key: {:?}", kv.keys);
                    continue;
                }
            },
            None => {
                warn!("No key found in Commitments storage entry");
                continue;
            }
        };

        // Convert value to JSON for processing
        let data = match kv.value.to_value() {
            Ok(scale_val) => {
                match serde_json::to_value(crate::transformation::ValueWrapper::from(scale_val)) {
                    Ok(json) => json,
                    Err(e) => {
                        warn!(
                            "Failed to convert commitment {} to JSON: {:?}",
                            commitment_id, e
                        );
                        continue;
                    }
                }
            }
            Err(e) => {
                warn!("Failed to decode commitment {}: {:?}", commitment_id, e);
                continue;
            }
        };

        // Process this commitment using the same logic as snapshot processing
        match process_single_commitment(
            db_pool,
            &block,
            commitment_id,
            None, // No snapshot for chain-scanned commitments
            &data,
            block_number,
            block_time,
            epoch,
        )
        .await
        {
            Ok(true) => processed += 1,
            Ok(false) => {} // Skipped (e.g., no committer found)
            Err(e) => {
                warn!("Failed to process commitment {}: {:?}", commitment_id, e);
                continue;
            }
        }

        if processed % 10 == 0 {
            info!("Scanned {} commitments from chain storage...", processed);
            if let Some(tid) = task_id {
                TASK_REGISTRY.set_detail(tid, format!("scanned {} commitments", processed));
            }
        }
    }

    // Update task monitor with final count
    if let Some(tid) = task_id {
        TASK_REGISTRY.set_detail(tid, format!("scan complete: {} commitments", processed));
    }

    info!(
        "Initial commitment scan complete: {} commitments from chain storage",
        processed
    );
    Ok(true)
}

/// Process a single commitment with its data.
/// Shared logic used by both snapshot processing and direct chain scan.
/// Returns true if the commitment was processed successfully.
async fn process_single_commitment(
    db_pool: &Pool<Postgres>,
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    commitment_id: i64,
    snapshot_id: Option<i64>,
    data: &JsonValue,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
    epoch: i64,
) -> Result<bool, anyhow::Error> {
    // Look up committer address from Uniques (as bytes)
    let committer_bytes =
        get_asset_owner_bytes(block, COMMITMENTS_COLLECTION_ID, commitment_id as u128).await;

    let committer_bytes = match committer_bytes {
        Some(bytes) => bytes,
        None => {
            trace!(
                "Could not find committer for commitment {}, skipping",
                commitment_id
            );
            return Ok(false);
        }
    };

    // Look up manager_id by finding which manager NFT the committer owns
    let manager_id = get_first_owned_asset(block, &committer_bytes, MANAGERS_COLLECTION_ID).await;

    // Manager address is the same as committer address (they own the manager NFT)
    let committer_address = format!("0x{}", hex::encode(committer_bytes));
    let manager_address = manager_id.map(|_| committer_address.clone());

    // Extract commitment data fields (raw values, no decimal shifting)
    let commission = extract_numeric_field(data, &["commission", "0"]);
    let stake_amount = extract_numeric_field(data, &["stake", "amount"]);
    let stake_rewardable_amount = extract_numeric_field(data, &["stake", "rewardable_amount"]);
    let stake_accrued_reward = extract_numeric_field(data, &["stake", "accrued_reward"]);
    let stake_paid = extract_numeric_field(data, &["stake", "paid"]);
    let delegations_total_amount = extract_numeric_field(data, &["delegations_total_amount"]);
    let delegations_total_rewardable_amount =
        extract_numeric_field(data, &["delegations_total_rewardable_amount"]);
    let last_scoring_epoch = extract_i64_field(data, &["last_scoring_epoch"]);
    let last_slashing_epoch = extract_i64_field(data, &["last_slashing_epoch"]);
    let stake_created_epoch = extract_i64_field(data, &["stake", "created"]);
    let cooldown_started = extract_optional_i64_field(data, &["stake", "cooldown_started"]);
    let stake_cooldown = extract_optional_i64_field(data, &["stake", "cooldown_period"]);

    // Check if stake is active (not null/ended)
    let is_active = data.get("stake").map(|s| !s.is_null()).unwrap_or(false);

    // Extract weights (raw values, no decimal shifting)
    let weights = resolve_weights(data, epoch);
    let delegations_reward_weight = weights.0;
    let delegations_slash_weight = weights.1;
    let self_reward_weight = weights.2;
    let self_slash_weight = weights.3;

    // Extract pool rewards (raw values, no decimal shifting)
    let pool_rewards = extract_pool_rewards(data, stake_created_epoch);
    let reward_per_weight = pool_rewards.0;
    let slash_per_weight = pool_rewards.1;

    // Fetch ComputeCommitments data for this commitment
    let committed_metrics = fetch_committed_metrics(block, commitment_id).await;

    // Calculate utilization metrics (uses pre-fetched committed_metrics)
    let utilization = calculate_utilization(
        block,
        commitment_id,
        epoch,
        &self_slash_weight,
        &delegations_reward_weight,
        &self_reward_weight,
        committed_metrics,
    )
    .await;

    // Convert committed_metrics HashMap to JSON for storage
    let committed_metrics_json: JsonValue = utilization
        .committed_metrics
        .iter()
        .map(|(k, v)| (k.clone(), JsonValue::String(v.to_string())))
        .collect();

    // Upsert into commitments table (snapshot_id = 0 for chain-scanned commitments)
    sqlx::query(
        r#"
        INSERT INTO commitments (
            commitment_id, snapshot_id, block_number, block_time, epoch,
            committer_address, manager_id, manager_address,
            commission, stake_amount, stake_rewardable_amount,
            stake_accrued_reward, stake_paid,
            delegations_total_amount, delegations_total_rewardable_amount,
            last_scoring_epoch, last_slashing_epoch, stake_created_epoch,
            cooldown_started, cooldown_period, is_active,
            delegations_reward_weight, delegations_slash_weight,
            self_reward_weight, self_slash_weight,
            reward_per_weight, slash_per_weight,
            delegation_utilization, target_weight_per_compute_utilization, combined_utilization,
            max_delegation_capacity, min_max_weight_per_compute, remaining_capacity,
            committed_metrics
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34)
        ON CONFLICT (commitment_id) DO UPDATE SET
            snapshot_id = EXCLUDED.snapshot_id,
            block_number = EXCLUDED.block_number,
            block_time = EXCLUDED.block_time,
            epoch = EXCLUDED.epoch,
            committer_address = EXCLUDED.committer_address,
            manager_id = EXCLUDED.manager_id,
            manager_address = EXCLUDED.manager_address,
            commission = EXCLUDED.commission,
            stake_amount = EXCLUDED.stake_amount,
            stake_rewardable_amount = EXCLUDED.stake_rewardable_amount,
            stake_accrued_reward = EXCLUDED.stake_accrued_reward,
            stake_paid = EXCLUDED.stake_paid,
            delegations_total_amount = EXCLUDED.delegations_total_amount,
            delegations_total_rewardable_amount = EXCLUDED.delegations_total_rewardable_amount,
            last_scoring_epoch = EXCLUDED.last_scoring_epoch,
            last_slashing_epoch = EXCLUDED.last_slashing_epoch,
            stake_created_epoch = EXCLUDED.stake_created_epoch,
            cooldown_started = EXCLUDED.cooldown_started,
            cooldown_period = EXCLUDED.cooldown_period,
            is_active = EXCLUDED.is_active,
            delegations_reward_weight = EXCLUDED.delegations_reward_weight,
            delegations_slash_weight = EXCLUDED.delegations_slash_weight,
            self_reward_weight = EXCLUDED.self_reward_weight,
            self_slash_weight = EXCLUDED.self_slash_weight,
            reward_per_weight = EXCLUDED.reward_per_weight,
            slash_per_weight = EXCLUDED.slash_per_weight,
            delegation_utilization = EXCLUDED.delegation_utilization,
            target_weight_per_compute_utilization = EXCLUDED.target_weight_per_compute_utilization,
            combined_utilization = EXCLUDED.combined_utilization,
            max_delegation_capacity = EXCLUDED.max_delegation_capacity,
            min_max_weight_per_compute = EXCLUDED.min_max_weight_per_compute,
            remaining_capacity = EXCLUDED.remaining_capacity,
            committed_metrics = EXCLUDED.committed_metrics
        "#,
    )
    .bind(commitment_id)
    .bind(snapshot_id)
    .bind(block_number)
    .bind(block_time)
    .bind(epoch)
    .bind(&committer_address)
    .bind(manager_id.map(|m| m as i64))
    .bind(&manager_address)
    .bind(&commission)
    .bind(&stake_amount)
    .bind(&stake_rewardable_amount)
    .bind(&stake_accrued_reward)
    .bind(&stake_paid)
    .bind(&delegations_total_amount)
    .bind(&delegations_total_rewardable_amount)
    .bind(last_scoring_epoch)
    .bind(last_slashing_epoch)
    .bind(stake_created_epoch)
    .bind(cooldown_started)
    .bind(stake_cooldown)
    .bind(is_active)
    .bind(&delegations_reward_weight)
    .bind(&delegations_slash_weight)
    .bind(&self_reward_weight)
    .bind(&self_slash_weight)
    .bind(&reward_per_weight)
    .bind(&slash_per_weight)
    .bind(&utilization.delegation_utilization)
    .bind(&utilization.target_weight_per_compute_utilization)
    .bind(&utilization.combined_utilization)
    .bind(&utilization.max_delegation_capacity)
    .bind(&utilization.min_max_weight_per_compute)
    .bind(&utilization.remaining_capacity)
    .bind(&committed_metrics_json)
    .execute(db_pool)
    .await?;

    trace!(
        "Upserted commitment {} (committer: {}, manager: {:?})",
        commitment_id,
        committer_address,
        manager_id
    );

    Ok(true)
}

/// Get the owner of a Uniques collection item using dynamic query
/// Extract owner bytes from Uniques Asset storage value
fn extract_owner_bytes(value: &scale_value::Value<u32>) -> Option<[u8; 32]> {
    if let Some(owner_value) = value.at("owner") {
        match &owner_value.value {
            scale_value::ValueDef::Composite(composite) => {
                // Try to extract bytes from composite
                for field_value in composite.values() {
                    if let scale_value::ValueDef::Composite(inner) = &field_value.value {
                        let bytes: Vec<u8> = inner
                            .values()
                            .filter_map(|v| v.as_u128().map(|n| n as u8))
                            .collect();
                        if bytes.len() == 32 {
                            let mut arr = [0u8; 32];
                            arr.copy_from_slice(&bytes);
                            return Some(arr);
                        }
                    }
                }
                // Fallback: try direct values
                let bytes: Vec<u8> = composite
                    .values()
                    .filter_map(|v| v.as_u128().map(|n| n as u8))
                    .collect();
                if bytes.len() == 32 {
                    let mut arr = [0u8; 32];
                    arr.copy_from_slice(&bytes);
                    return Some(arr);
                }
            }
            scale_value::ValueDef::Primitive(scale_value::Primitive::U256(bytes)) => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes[..32]);
                return Some(arr);
            }
            _ => {}
        }
    }
    None
}

/// Get asset owner as bytes (AccountId32)
async fn get_asset_owner_bytes(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    collection_id: u128,
    item_id: u128,
) -> Option<[u8; 32]> {
    let storage_query = subxt::dynamic::storage(
        UNIQUES_PALLET,
        UNIQUES_ASSET_STORAGE,
        vec![
            subxt::dynamic::Value::u128(collection_id),
            subxt::dynamic::Value::u128(item_id),
        ],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(item_details_thunk)) => match item_details_thunk.to_value() {
            Ok(value) => extract_owner_bytes(&value),
            Err(e) => {
                warn!(
                    "Failed to decode Asset for collection {}, item {}: {:?}",
                    collection_id, item_id, e
                );
                None
            }
        },
        Ok(None) => None,
        Err(e) => {
            warn!(
                "Failed to fetch Asset for collection {}, item {}: {:?}",
                collection_id, item_id, e
            );
            None
        }
    }
}

/// Get the first asset ID owned by an account in a collection
/// Queries Uniques.Account(account, collection_id) to find which items the account owns
async fn get_first_owned_asset(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    account_bytes: &[u8; 32],
    collection_id: u128,
) -> Option<u128> {
    let storage_query = subxt::dynamic::storage(
        UNIQUES_PALLET,
        "Account",
        vec![
            subxt::dynamic::Value::from_bytes(account_bytes),
            subxt::dynamic::Value::u128(collection_id),
        ],
    );

    let mut iter = match block.storage().iter(storage_query).await {
        Ok(iter) => iter,
        Err(e) => {
            warn!(
                "Failed to iterate Uniques.Account for collection {}: {:?}",
                collection_id, e
            );
            return None;
        }
    };

    // Take the first item_id owned by this account
    match iter.next().await {
        Some(Ok(kv)) => {
            debug!(
                "Uniques.Account iteration in collection {}: {} keys",
                collection_id,
                kv.keys.len()
            );

            // The remaining key after (account, collection_id) should be item_id
            if kv.keys.is_empty() {
                warn!(
                    "No keys returned from Uniques.Account in collection {}",
                    collection_id
                );
                None
            } else {
                // Take the last key as item_id (storage structure might include prefix keys)
                let item_id = kv.keys.last().and_then(|k| k.as_u128());
                if item_id.is_none() {
                    warn!(
                        "Failed to parse item_id from Uniques.Account keys ({} total) in collection {}: checking all keys",
                        kv.keys.len(),
                        collection_id
                    );
                }
                item_id
            }
        }
        Some(Err(e)) => {
            warn!(
                "Failed to read Uniques.Account entry in collection {}: {:?}",
                collection_id, e
            );
            None
        }
        None => {
            trace!("No assets found in collection {}", collection_id);
            None
        }
    }
}

/// Get backing manager_id for a commitment using dynamic query
/// Backings is StorageDoubleMap<CommitmentId, ManagerId, ()>
/// We query with commitment_id as partial key and take the first manager_id found
async fn get_backing_manager_id(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    commitment_id: u128,
) -> Result<Option<u128>, anyhow::Error> {
    let storage_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        BACKINGS_STORAGE,
        vec![subxt::dynamic::Value::u128(commitment_id)],
    );

    let mut iter = block.storage().iter(storage_query).await?;

    // Take the first entry - keys[1] is the manager_id
    match iter.next().await {
        Some(Ok(kv)) => {
            if kv.keys.len() == 1 {
                kv.keys[0]
                    .as_u128()
                    .ok_or_else(|| {
                        anyhow!(
                            "Failed to parse manager_id as u128 for commitment {}",
                            commitment_id
                        )
                    })
                    .map(Some)
            } else {
                Err(anyhow!(
                    "Backings entry for commitment {}  keys (expected 1, got {})",
                    commitment_id,
                    kv.keys.len()
                ))
            }
        }
        Some(Err(e)) => Err(anyhow!(
            "Failed to read Backings entry for commitment {}: {:?}",
            commitment_id,
            e
        )),
        None => Ok(None), // No backing exists for this commitment
    }
}

/// Extract a numeric field from nested JSON path as BigDecimal
/// Path elements can be object keys or array indices (numeric strings like "0")
fn extract_numeric_field(data: &JsonValue, path: &[&str]) -> BigDecimal {
    let mut current = data;
    for &key in path {
        current = match get_json_path_element(current, key) {
            Some(v) => v,
            None => return BigDecimal::from(0),
        };
    }

    match current {
        JsonValue::String(s) => BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                BigDecimal::from(i)
            } else if let Some(u) = n.as_u64() {
                BigDecimal::from(u as i64)
            } else {
                BigDecimal::from(0)
            }
        }
        _ => BigDecimal::from(0),
    }
}

/// Extract a U256 weight field from JSON as raw units (no decimal adjustment).
/// Weight values are stored as raw units and should remain that way in the database.
/// The field is accessed as an array where [0] contains the U256 value.
fn extract_weight_field(data: &JsonValue, field_name: &str) -> BigDecimal {
    // Navigate to the field and get the first array element
    let weight_value = match data.get(field_name) {
        Some(JsonValue::Array(arr)) if !arr.is_empty() => &arr[0],
        _ => return BigDecimal::from(0),
    };

    // Parse as U256 (can be array of 4 u64 limbs or other formats)
    // Return raw value without decimal adjustment
    parse_u256_value(weight_value)
}

/// Parse a U256 value from JSON (handles both array of limbs and string formats)
fn parse_u256_value(value: &JsonValue) -> BigDecimal {
    match value {
        // Array of 4 u64 limbs (little-endian)
        JsonValue::Array(limbs) if limbs.len() == 4 => {
            let mut result = BigDecimal::from(0);
            let base: BigDecimal = BigDecimal::from(1u128 << 64); // 2^64

            for (i, limb) in limbs.iter().enumerate() {
                let limb_val: BigDecimal = match limb {
                    JsonValue::String(s) => {
                        BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0))
                    }
                    JsonValue::Number(n) => {
                        if let Some(v) = n.as_u64() {
                            BigDecimal::from(v)
                        } else {
                            BigDecimal::from(0)
                        }
                    }
                    _ => BigDecimal::from(0),
                };

                // Multiply by 2^(64*i)
                let multiplier = (0..i).fold(BigDecimal::from(1), |acc, _| acc * &base);
                result = result + limb_val * multiplier;
            }
            result
        }
        // String (could be hex or decimal)
        JsonValue::String(s) => BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0)),
        // Direct number
        JsonValue::Number(n) => {
            if let Some(v) = n.as_u64() {
                BigDecimal::from(v)
            } else {
                BigDecimal::from(0)
            }
        }
        _ => BigDecimal::from(0),
    }
}

/// Get a JSON element by key (for objects) or index (for arrays)
fn get_json_path_element<'a>(value: &'a JsonValue, key: &str) -> Option<&'a JsonValue> {
    match value {
        JsonValue::Object(_) => value.get(key),
        JsonValue::Array(arr) => {
            // Try parsing key as array index
            if let Ok(idx) = key.parse::<usize>() {
                arr.get(idx)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Extract an i64 field from nested JSON path
/// Path elements can be object keys or array indices (numeric strings like "0")
fn extract_i64_field(data: &JsonValue, path: &[&str]) -> i64 {
    let mut current = data;
    for &key in path {
        current = match get_json_path_element(current, key) {
            Some(v) => v,
            None => return 0,
        };
    }

    match current {
        JsonValue::String(s) => s.parse().unwrap_or(0),
        JsonValue::Number(n) => n.as_i64().unwrap_or(0),
        _ => 0,
    }
}

/// Extract an optional i64 field from nested JSON path (for nullable fields like cooldown_started)
/// Path elements can be object keys or array indices (numeric strings like "0")
fn extract_optional_i64_field(data: &JsonValue, path: &[&str]) -> Option<i64> {
    let mut current = data;
    for &key in path {
        current = match get_json_path_element(current, key) {
            Some(v) => v,
            None => return None,
        };
    }

    match current {
        JsonValue::Null => None,
        JsonValue::String(s) => s.parse().ok(),
        JsonValue::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// Parse a commitment ID from a JSON value
/// Handles: decimal strings ("449"), hex strings ("0x7c"), and integers
fn parse_commitment_id(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::String(s) => {
            // Try hex format first (0x...)
            if let Some(hex_str) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                i64::from_str_radix(hex_str, 16).ok()
            } else {
                // Try decimal
                s.parse::<i64>().ok()
            }
        }
        JsonValue::Number(n) => n.as_i64(),
        _ => None,
    }
}

/// Resolve weights from the current/past structure based on epoch.
/// Returns (delegations_reward_weight, delegations_slash_weight, self_reward_weight, self_slash_weight)
///
/// Logic:
/// - If epoch matches weights.current[0], use weights.current[1]
/// - Else if epoch matches weights.past[0], use weights.past[1]
/// - Else return default (0, 0, 0, 0)
fn resolve_weights(
    data: &JsonValue,
    epoch: i64,
) -> (BigDecimal, BigDecimal, BigDecimal, BigDecimal) {
    let default = (
        BigDecimal::from(0),
        BigDecimal::from(0),
        BigDecimal::from(0),
        BigDecimal::from(0),
    );

    let weights = match data.get("weights") {
        Some(w) => w,
        None => return default,
    };

    // Try current first: weights.current is [epoch, {weight_values}]
    if let Some(current) = weights.get("current") {
        if let JsonValue::Array(arr) = current {
            if arr.len() >= 2 {
                // arr[0] is the epoch when this was set
                let current_epoch = match &arr[0] {
                    JsonValue::String(s) => s.parse::<i64>().unwrap_or(-1),
                    JsonValue::Number(n) => n.as_i64().unwrap_or(-1),
                    _ => -1,
                };
                if current_epoch <= epoch {
                    return extract_weight_values(&arr[1]);
                }
            }
        }
    }

    // Try past: weights.past is [epoch, {weight_values}]
    if let Some(past) = weights.get("past") {
        if let JsonValue::Array(arr) = past {
            if arr.len() >= 2 {
                let past_epoch = match &arr[0] {
                    JsonValue::String(s) => s.parse::<i64>().unwrap_or(-1),
                    JsonValue::Number(n) => n.as_i64().unwrap_or(-1),
                    _ => -1,
                };
                if past_epoch <= epoch {
                    return extract_weight_values(&arr[1]);
                }
            }
        }
    }

    default
}

/// Extract weight values from the weight object as raw units.
/// Each weight field is an array where we take the first element [0].
/// Weights are U256 values stored in raw units (no decimal adjustment).
fn extract_weight_values(
    weight_obj: &JsonValue,
) -> (BigDecimal, BigDecimal, BigDecimal, BigDecimal) {
    let delegations_reward = extract_weight_field(weight_obj, "delegations_reward_weight");
    let delegations_slash = extract_weight_field(weight_obj, "delegations_slash_weight");
    let self_reward = extract_weight_field(weight_obj, "self_reward_weight");
    let self_slash = extract_weight_field(weight_obj, "self_slash_weight");

    (
        delegations_reward,
        delegations_slash,
        self_reward,
        self_slash,
    )
}

/// Extract pool rewards from a MemoryBuffer in pool_rewards.
/// MemoryBuffer structure: { past: Option<(Timestamp, Value)>, current: (Timestamp, Value) }
/// Matches the stake's created timestamp against the buffer timestamps.
/// Logic:
/// - If created == current.0 -> return current.1
/// - If created == past.0 -> return past.1
/// - Otherwise -> return (0, 0)
/// Returns (reward_per_weight, slash_per_weight) as BigDecimals.
fn extract_pool_rewards(data: &JsonValue, created_timestamp: i64) -> (BigDecimal, BigDecimal) {
    let default = (BigDecimal::from(0), BigDecimal::from(0));

    // If created timestamp is 0 (missing), return default
    if created_timestamp == 0 {
        return default;
    }

    let pool_rewards = match data.get("pool_rewards") {
        Some(pr) => pr,
        None => return default,
    };

    // Extract current tuple: (Timestamp, Value)
    // In JSON this is an array: [timestamp, value]
    let current = match pool_rewards.get("current") {
        Some(c) => c,
        None => return default,
    };

    // Get current timestamp (index 0)
    let current_timestamp = match current.get(0) {
        Some(JsonValue::Number(n)) => n.as_i64().unwrap_or(-1),
        Some(JsonValue::String(s)) => s.parse::<i64>().unwrap_or(-1),
        _ => -1,
    };

    // If created matches current timestamp, use current value
    if created_timestamp == current_timestamp {
        let rewards_value = match current.get(1) {
            Some(v) => v,
            None => return default,
        };
        let reward_per_weight = extract_u256_field(rewards_value, "reward_per_weight");
        let slash_per_weight = extract_u256_field(rewards_value, "slash_per_weight");
        return (reward_per_weight, slash_per_weight);
    }

    // Check past value if it exists
    if let Some(past) = pool_rewards.get("past") {
        if !past.is_null() {
            // past is also a tuple: [timestamp, value]
            let past_timestamp = match past.get(0) {
                Some(JsonValue::Number(n)) => n.as_i64().unwrap_or(-1),
                Some(JsonValue::String(s)) => s.parse::<i64>().unwrap_or(-1),
                _ => -1,
            };

            // If created matches past timestamp, use past value
            if created_timestamp == past_timestamp {
                let rewards_value = match past.get(1) {
                    Some(v) => v,
                    None => return default,
                };
                let reward_per_weight = extract_u256_field(rewards_value, "reward_per_weight");
                let slash_per_weight = extract_u256_field(rewards_value, "slash_per_weight");
                return (reward_per_weight, slash_per_weight);
            }
        }
    }

    // No match found, return default
    default
}

/// Extract a U256 field that can be either:
/// - An array of 4 u64 limbs (little-endian): ["123", "456", "0", "0"]
/// - A hex string: "0x00000000"
fn extract_u256_field(obj: &JsonValue, field: &str) -> BigDecimal {
    let value = match obj.get(field) {
        Some(v) => v,
        None => return BigDecimal::from(0),
    };

    match value {
        // Array of 4 u64 limbs (little-endian): value = limb[0] + limb[1]*2^64 + limb[2]*2^128 + limb[3]*2^192
        JsonValue::Array(limbs) if limbs.len() == 4 => {
            let mut result = BigDecimal::from(0);
            let base: BigDecimal = BigDecimal::from(1u128 << 64); // 2^64

            for (i, limb) in limbs.iter().enumerate() {
                let limb_val: BigDecimal = match limb {
                    JsonValue::String(s) => {
                        BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0))
                    }
                    JsonValue::Number(n) => {
                        if let Some(v) = n.as_u64() {
                            BigDecimal::from(v)
                        } else {
                            BigDecimal::from(0)
                        }
                    }
                    _ => BigDecimal::from(0),
                };

                // Multiply by 2^(64*i)
                let multiplier = (0..i).fold(BigDecimal::from(1), |acc, _| acc * &base);
                result = result + limb_val * multiplier;
            }
            result
        }
        // Hex string: "0x..."
        JsonValue::String(s) => {
            if let Some(hex_str) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                // Parse hex bytes and convert to BigDecimal
                if let Ok(bytes) = hex::decode(hex_str) {
                    // Bytes are big-endian, convert to BigDecimal
                    let mut result = BigDecimal::from(0);
                    for byte in bytes {
                        result = result * BigDecimal::from(256) + BigDecimal::from(byte);
                    }
                    return result;
                }
            }
            // Try parsing as decimal string
            BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0))
        }
        _ => BigDecimal::from(0),
    }
}

/// Perbill multiplier (10^9) - used to convert ratios to parts per billion
const PERBILL: i64 = 1_000_000_000;

/// Utilization calculation results (utilization values stored as Perbill)
struct UtilizationResult {
    delegation_utilization: Option<BigDecimal>,
    target_weight_per_compute_utilization: Option<BigDecimal>,
    combined_utilization: Option<BigDecimal>,
    max_delegation_capacity: Option<BigDecimal>,
    min_max_weight_per_compute: Option<BigDecimal>,
    remaining_capacity: Option<BigDecimal>,
    /// ComputeCommitments data: { pool_id: metric_value }
    committed_metrics: HashMap<String, BigDecimal>,
}

/// Convert a ratio (0-1 or higher) to Perbill format (multiply by 10^9)
fn to_perbill(ratio: BigDecimal) -> BigDecimal {
    (ratio * BigDecimal::from(PERBILL)).with_scale(0)
}

/// Fetch ComputeCommitments for a commitment from chain storage.
/// Returns a HashMap of pool_id -> metric value.
async fn fetch_committed_metrics(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    commitment_id: i64,
) -> HashMap<String, BigDecimal> {
    let mut committed_metrics: HashMap<String, BigDecimal> = HashMap::new();

    debug!(
        "Querying ComputeCommitments from chain for commitment {}",
        commitment_id
    );
    let storage_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        "ComputeCommitments",
        vec![subxt::dynamic::Value::u128(commitment_id as u128)],
    );

    match block.storage().iter(storage_query).await {
        Ok(mut iter) => {
            while let Some(Ok(kv)) = iter.next().await {
                // kv.keys[0] is commitment_id (the key we queried with), kv.keys[1] is pool_id
                if let Some(pool_key) = kv.keys.get(1) {
                    let pool_id = pool_key.as_u128().map(|n| n.to_string());
                    if let Some(pool_id) = pool_id {
                        if let Ok(value) = kv.value.to_value() {
                            // Store raw FixedU128 value (as in runtime storage)
                            let metric = scale_value_to_bigdecimal(&value);
                            debug!(
                                "ComputeCommitments[{}][pool={}] = {} (raw)",
                                commitment_id, pool_id, metric
                            );
                            committed_metrics.insert(pool_id, metric);
                        }
                    }
                }
            }
        }
        Err(e) => {
            warn!("Failed to iterate ComputeCommitments from chain: {:?}", e);
        }
    }

    debug!(
        "Commitment {}: found {} ComputeCommitments entries",
        commitment_id,
        committed_metrics.len()
    );

    committed_metrics
}

/// Calculate utilization metrics for a commitment.
/// Accepts pre-fetched committed_metrics from fetch_committed_metrics.
/// Returns UtilizationResult with:
/// - delegation_utilization = delegations_reward_weight / (self_slash_weight * 9), clamped to 0-1
/// - target_weight_per_compute_utilization = total_reward_weight / max_weight
///   where max_weight = min(targetWeightPerCompute[pool] × committedMetric[pool]) for all pools
/// - combined_utilization = min(1, max(delegation_utilization, target_weight_per_compute_utilization))
/// - max_delegation_capacity = self_slash_weight * 9
/// - min_max_weight_per_compute = min(targetWeightPerCompute[pool] × committedMetric[pool]) for all pools
/// - committed_metrics = the pre-fetched ComputeCommitments data
async fn calculate_utilization(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    commitment_id: i64,
    epoch: i64,
    self_slash_weight: &BigDecimal,
    delegations_reward_weight: &BigDecimal,
    self_reward_weight: &BigDecimal,
    committed_metrics: HashMap<String, BigDecimal>,
) -> UtilizationResult {
    // Calculate delegation_utilization
    // maxDelegationCapacity = self_slash_weight * 9
    // delegation_utilization = delegations_reward_weight / maxDelegationCapacity
    let nine = BigDecimal::from(9);
    let zero = BigDecimal::from(0);
    let one = BigDecimal::from(1);
    let max_delegation_capacity = self_slash_weight * &nine;

    let (delegation_utilization, max_delegation_capacity) = if max_delegation_capacity > zero {
        let util = delegations_reward_weight / &max_delegation_capacity;
        // Clamp to 0-1
        let clamped = if util < zero.clone() {
            zero.clone()
        } else if util > one {
            one.clone()
        } else {
            util
        };
        (Some(clamped), Some(max_delegation_capacity))
    } else {
        (None, None)
    };

    // Calculate target_weight_per_compute_utilization
    // Uses pre-fetched committed_metrics and fetches StakeBasedRewards
    let (target_weight_per_compute_utilization, target_weight_per_compute) =
        calculate_target_weight_utilization(
            block,
            commitment_id,
            epoch,
            self_reward_weight,
            delegations_reward_weight,
            &committed_metrics,
        )
        .await;

    // Calculate combined_utilization = min(1, max(delegation_util, target_util))
    let one = BigDecimal::from(1);
    let combined_utilization = match (
        &delegation_utilization,
        &target_weight_per_compute_utilization,
    ) {
        (Some(del), Some(target)) => {
            let max_util = if del > target {
                del.clone()
            } else {
                target.clone()
            };
            Some(if max_util > one {
                one.clone()
            } else {
                max_util
            })
        }
        (Some(del), None) => Some(if del > &one { one.clone() } else { del.clone() }),
        (None, Some(target)) => Some(if target > &one {
            one.clone()
        } else {
            target.clone()
        }),
        (None, None) => None,
    };

    // Calculate remaining_capacity as absolute weight
    // remaining_capacity = min(max_delegation_capacity - delegations_reward_weight, min_max_weight_per_compute - total_reward_weight)
    let total_reward_weight = self_reward_weight + delegations_reward_weight;
    let zero = BigDecimal::from(0);

    let remaining_delegation = max_delegation_capacity.as_ref().map(|cap| {
        let remaining = cap - delegations_reward_weight;
        if remaining < zero {
            zero.clone()
        } else {
            remaining
        }
    });

    let remaining_target = target_weight_per_compute.as_ref().map(|cap| {
        let remaining = cap - &total_reward_weight;
        if remaining < zero {
            zero.clone()
        } else {
            remaining
        }
    });

    let remaining_capacity = match (remaining_delegation, remaining_target) {
        (Some(del), Some(target)) => Some(if del < target { del } else { target }),
        (Some(del), None) => Some(del),
        (None, Some(target)) => Some(target),
        (None, None) => None,
    };

    // Convert utilization ratios to Perbill format (multiply by 10^9)
    UtilizationResult {
        delegation_utilization: delegation_utilization.map(to_perbill),
        target_weight_per_compute_utilization: target_weight_per_compute_utilization
            .map(to_perbill),
        combined_utilization: combined_utilization.map(to_perbill),
        max_delegation_capacity,
        min_max_weight_per_compute: target_weight_per_compute,
        remaining_capacity,
        committed_metrics,
    }
}

/// Calculate target_weight_per_compute_utilization using the frontend's algorithm.
/// Matches calculateMaxWeightByMetrics from frontend.
/// Returns (utilization, max_weight) where:
/// - utilization = total_reward_weight / maxWeightByMetrics
/// - maxWeightByMetrics = min(targetWeightPerCompute × committedMetric) for all pools
/// - targetWeightPerCompute = 0.8 × totalSupply / totalBenchmarkedMetric × 5.0
/// All values are in raw form (not decimal-corrected):
/// - totalSupply: 12 decimals, totalBenchmarkedMetric: 18 decimals, committedMetric: 18 decimals
/// - Result maxWeightByMetrics has 12 decimals (same as total_reward_weight)
async fn calculate_target_weight_utilization(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
    commitment_id: i64,
    _epoch: i64,
    self_reward_weight: &BigDecimal,
    delegations_reward_weight: &BigDecimal,
    committed_metrics: &HashMap<String, BigDecimal>,
) -> (Option<BigDecimal>, Option<BigDecimal>) {
    let total_reward_weight = self_reward_weight + delegations_reward_weight;

    if total_reward_weight <= BigDecimal::from(0) {
        return (Some(BigDecimal::from(0)), None);
    }

    if committed_metrics.is_empty() {
        return (None, None);
    }

    // Get total supply from chain (balance with 12 decimals)
    let total_supply = match get_total_supply(block).await {
        Ok(supply) => supply,
        Err(e) => {
            warn!(
                "Failed to get total supply for commitment {}: {:?}",
                commitment_id, e
            );
            return (None, None);
        }
    };

    if total_supply <= BigDecimal::from(0) {
        return (None, None);
    }

    // Query MetricPools to get totalBenchmarkedMetric for each pool
    let metric_pools = match get_metric_pools(block).await {
        Ok(pools) => pools,
        Err(e) => {
            warn!(
                "Failed to get metric pools for commitment {}: {:?}",
                commitment_id, e
            );
            return (None, None);
        }
    };

    // Calculate maxWeightByMetrics = min(targetWeightPerCompute × committedMetric) for all pools
    let mut min_max_weight: Option<BigDecimal> = None;

    let target_weight_multiplier = BigDecimal::from_str("4.0").unwrap(); // 0.8 × 5.0

    for (pool_id, committed_metric) in committed_metrics {
        if committed_metric <= &BigDecimal::from(0) {
            continue;
        }

        let total_benchmarked_metric = metric_pools
            .get(pool_id)
            .cloned()
            .unwrap_or_else(|| BigDecimal::from(0));

        if total_benchmarked_metric <= BigDecimal::from(0) {
            continue;
        }

        // targetWeightPerCompute = 0.8 × totalSupply / totalBenchmarkedMetric × 5.0 = 4.0 × totalSupply / totalBenchmarkedMetric
        // All raw values: totalSupply (12 decimals), totalBenchmarkedMetric (18 decimals)
        let target_weight_per_compute =
            &target_weight_multiplier * &total_supply / &total_benchmarked_metric;

        // maxScoreByCompute = targetWeightPerCompute × committedMetric
        // Result has 12 decimals: (12 - 18) + 18 = 12
        let max_score_by_compute = &target_weight_per_compute * committed_metric;

        trace!(
            "Commitment {}: pool {} -> targetWeight={} × metric={} = maxScore={}",
            commitment_id,
            pool_id,
            target_weight_per_compute,
            committed_metric,
            max_score_by_compute
        );

        min_max_weight = Some(match min_max_weight {
            Some(current) if max_score_by_compute < current => max_score_by_compute,
            Some(current) => current,
            None => max_score_by_compute,
        });
    }

    match min_max_weight {
        Some(max_weight) if max_weight > BigDecimal::from(0) => {
            // Both total_reward_weight and max_weight have 12 decimals
            let utilization = &total_reward_weight / &max_weight;
            debug!(
                "Commitment {}: UTILIZATION = total_reward_weight={} / maxWeight={} = {}",
                commitment_id, total_reward_weight, max_weight, utilization
            );
            (Some(utilization), Some(max_weight))
        }
        _ => (None, None),
    }
}

/// Convert a scale_value::Value to BigDecimal.
/// Handles u128/u64 primitives and composite types.
fn scale_value_to_bigdecimal(value: &scale_value::Value<u32>) -> BigDecimal {
    match &value.value {
        scale_value::ValueDef::Primitive(prim) => {
            match prim {
                scale_value::Primitive::U128(n) => BigDecimal::from(*n),
                scale_value::Primitive::U256(bytes) => {
                    // U256 as 32 bytes big-endian
                    let mut result = BigDecimal::from(0);
                    for byte in bytes.iter() {
                        result = result * BigDecimal::from(256) + BigDecimal::from(*byte);
                    }
                    result
                }
                scale_value::Primitive::I128(n) => BigDecimal::from(*n),
                _ => BigDecimal::from(0),
            }
        }
        scale_value::ValueDef::Composite(composite) => {
            // Could be a newtype wrapper around a primitive
            if let Some(inner) = composite.values().next() {
                scale_value_to_bigdecimal(inner)
            } else {
                BigDecimal::from(0)
            }
        }
        _ => BigDecimal::from(0),
    }
}

/// Get total supply from chain (Balances.TotalIssuance)
async fn get_total_supply(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
) -> Result<BigDecimal, anyhow::Error> {
    let storage_query = subxt::dynamic::storage(
        "Balances",
        "TotalIssuance",
        Vec::<subxt::dynamic::Value>::new(),
    );

    let value = block
        .storage()
        .fetch(&storage_query)
        .await?
        .ok_or_else(|| anyhow!("TotalIssuance not found"))?
        .to_value()?;

    Ok(scale_value_to_bigdecimal(&value))
}

/// Get metric pools from chain (AcurastCompute.MetricPools)
/// Returns HashMap<pool_id, total_benchmarked_metric>
async fn get_metric_pools(
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
) -> Result<HashMap<String, BigDecimal>, anyhow::Error> {
    let storage_query = subxt::dynamic::storage(
        ACURAST_COMPUTE_PALLET,
        "MetricPools",
        Vec::<subxt::dynamic::Value>::new(),
    );

    let mut metric_pools = HashMap::new();

    let mut iter = block.storage().iter(storage_query).await?;
    while let Some(Ok(kv)) = iter.next().await {
        if let Some(pool_key) = kv.keys.first() {
            if let Some(pool_id) = pool_key.as_u128().map(|n| n.to_string()) {
                if let Ok(value) = kv.value.to_value() {
                    // Extract total from MetricPool structure
                    // MetricPool has a 'total' field which is a SlidingBuffer<Epoch, Perquintill>
                    // We need to get the latest value from the buffer
                    if let Some(total_buffer) = value.at("total") {
                        if let Some(prev) = total_buffer.at("prev") {
                            // prev is Perquintill (FixedU128), extract raw value
                            let total = scale_value_to_bigdecimal(prev);
                            metric_pools.insert(pool_id, total);
                        }
                    }
                }
            }
        }
    }

    Ok(metric_pools)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_numeric_field() {
        let data = json!({
            "stake": {
                "amount": "7323299000000000",
                "rewardable_amount": "7323299000000000"
            },
            "commission": ["15000000"]
        });

        assert_eq!(
            extract_numeric_field(&data, &["stake", "amount"]),
            BigDecimal::from_str("7323299000000000").unwrap()
        );
        assert_eq!(
            extract_numeric_field(&data, &["commission", "0"]),
            BigDecimal::from_str("15000000").unwrap()
        );
    }

    #[test]
    fn test_extract_i64_field() {
        let data = json!({
            "last_scoring_epoch": "1801",
            "last_slashing_epoch": "0"
        });

        assert_eq!(extract_i64_field(&data, &["last_scoring_epoch"]), 1801);
        assert_eq!(extract_i64_field(&data, &["last_slashing_epoch"]), 0);
    }

    #[test]
    fn test_extract_u256_field_array_limbs() {
        // U256 as array of 4 u64 limbs (little-endian)
        // Value = limb[0] + limb[1]*2^64 + limb[2]*2^128 + limb[3]*2^192
        // Simple case: [1, 1, 0, 0] = 1 + 2^64 = 18446744073709551617
        let obj = json!({
            "value": ["1", "1", "0", "0"]
        });

        let result = extract_u256_field(&obj, "value");
        let expected = BigDecimal::from_str("18446744073709551617").unwrap();
        assert_eq!(result, expected);

        // More complex case with the actual data format
        let obj2 = json!({
            "reward_per_weight": ["2875112474845210211", "800536500", "0", "0"]
        });
        let result2 = extract_u256_field(&obj2, "reward_per_weight");
        // Verify it's non-zero and larger than limb[0]
        assert!(result2 > BigDecimal::from_str("2875112474845210211").unwrap());
    }

    #[test]
    fn test_extract_u256_field_hex_zero() {
        // U256 as hex string (all zeros)
        let obj = json!({
            "slash_per_weight": "0x00000000"
        });

        let result = extract_u256_field(&obj, "slash_per_weight");
        assert_eq!(result, BigDecimal::from(0));
    }

    #[test]
    fn test_extract_u256_field_hex_nonzero() {
        // U256 as hex string
        let obj = json!({
            "value": "0x0100"  // 256 in decimal
        });

        let result = extract_u256_field(&obj, "value");
        assert_eq!(result, BigDecimal::from(256));
    }

    #[test]
    fn test_extract_pool_rewards() {
        // pool_rewards structure: { current: [timestamp, value], past: [timestamp, value] }
        let data = json!({
            "pool_rewards": {
                "current": [1350698, {
                    "reward_per_weight": ["2875112474845210211", "800536500", "0", "0"],
                    "slash_per_weight": "0x00000000"
                }],
                "past": [1350697, {
                    "reward_per_weight": ["1000000000000000000", "0", "0", "0"],
                    "slash_per_weight": "0x00000000"
                }]
            }
        });

        // Test accessing current (when created_timestamp == current_timestamp)
        let (reward, slash) = extract_pool_rewards(&data, 1350698);
        // Verify reward is larger than just the first limb (proves multiplication happened)
        assert!(reward > BigDecimal::from_str("2875112474845210211").unwrap());
        assert_eq!(slash, BigDecimal::from(0));

        // Test accessing past (when created_timestamp == past_timestamp)
        let (reward_prev, _) = extract_pool_rewards(&data, 1350697);
        assert_eq!(
            reward_prev,
            BigDecimal::from_str("1000000000000000000").unwrap()
        );

        // Test default (timestamp doesn't match)
        let (reward_none, slash_none) = extract_pool_rewards(&data, 1350600);
        assert_eq!(reward_none, BigDecimal::from(0));
        assert_eq!(slash_none, BigDecimal::from(0));
    }
}
