mod commitments;
mod custom_transforms;
mod epoch;

pub use commitments::{
    find_commitment_snapshots_at_block, find_unprocessed_commitment_snapshots,
    process_commitment_snapshot_ids, scan_all_commitments_at_block, CommitmentSnapshot,
};
pub use custom_transforms::apply_transform;
pub use epoch::{process_epoch_storage_indexing, process_epoch_storage_rules_indexing};

use crate::config::{StorageIndexingRule, StorageIndexingTrigger, StoragePruning};
use crate::entities::{EventRow, ExtrinsicRow, ExtrinsicsIndexPhase};
use crate::transformation::ValueWrapper;
use anyhow::anyhow;
use serde_json::json;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;
use subxt::config::PolkadotConfig;
use subxt::metadata::types::StorageEntryType;
use subxt::utils::H256;
use subxt::OnlineClient;
use tracing::{debug, trace, warn};

/// Simplified trigger kind for filtering rules (without the inner data).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerKind {
    Extrinsic,
    Event,
    Epoch,
    Init,
}

impl TriggerKind {
    /// Check if a trigger matches this kind.
    pub fn matches(&self, trigger: &StorageIndexingTrigger) -> bool {
        match (self, trigger) {
            (TriggerKind::Extrinsic, StorageIndexingTrigger::Extrinsic { .. }) => true,
            (TriggerKind::Event, StorageIndexingTrigger::Event { .. }) => true,
            (TriggerKind::Epoch, StorageIndexingTrigger::Epoch) => true,
            (TriggerKind::Init, StorageIndexingTrigger::Init { .. }) => true,
            _ => false,
        }
    }
}

/// Pre-filtered storage rules by trigger type.
/// Filters rules once on first access using OnceLock for each category.
pub struct FilteredStorageRules {
    all_rules: Vec<StorageIndexingRule>,
    rules_by_kind: OnceLock<HashMap<TriggerKind, Vec<StorageIndexingRule>>>,
    rules_by_kind_and_phase: OnceLock<HashMap<(TriggerKind, u32), Vec<StorageIndexingRule>>>,
    max_phase_by_kind: OnceLock<HashMap<TriggerKind, u32>>,
    event_triggers: OnceLock<HashSet<(i32, i32)>>,
    event_triggers_by_phase: OnceLock<HashMap<u32, HashSet<(i32, i32)>>>,
}

impl FilteredStorageRules {
    pub fn new(rules: Vec<StorageIndexingRule>) -> Self {
        Self {
            all_rules: rules,
            rules_by_kind: OnceLock::new(),
            rules_by_kind_and_phase: OnceLock::new(),
            max_phase_by_kind: OnceLock::new(),
            event_triggers: OnceLock::new(),
            event_triggers_by_phase: OnceLock::new(),
        }
    }

    /// Get all rules (for pruning which needs all rules)
    pub fn all(&self) -> &[StorageIndexingRule] {
        &self.all_rules
    }

    /// Get the trigger kind for a rule.
    fn trigger_kind(trigger: &StorageIndexingTrigger) -> TriggerKind {
        match trigger {
            StorageIndexingTrigger::Extrinsic { .. } => TriggerKind::Extrinsic,
            StorageIndexingTrigger::Event { .. } => TriggerKind::Event,
            StorageIndexingTrigger::Epoch => TriggerKind::Epoch,
            StorageIndexingTrigger::Init { .. } => TriggerKind::Init,
        }
    }

    /// Get rules filtered by trigger kind.
    pub fn by_trigger(&self, kind: TriggerKind) -> &[StorageIndexingRule] {
        let map = self.rules_by_kind.get_or_init(|| {
            let mut by_kind: HashMap<TriggerKind, Vec<StorageIndexingRule>> = HashMap::new();
            for rule in &self.all_rules {
                let k = Self::trigger_kind(&rule.trigger);
                by_kind.entry(k).or_default().push(rule.clone());
            }
            by_kind
        });
        map.get(&kind).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get rules filtered by trigger kind and phase.
    pub fn by_trigger_and_phase(&self, kind: TriggerKind, phase: u32) -> &[StorageIndexingRule] {
        let map = self.rules_by_kind_and_phase.get_or_init(|| {
            let mut by_kind_phase: HashMap<(TriggerKind, u32), Vec<StorageIndexingRule>> =
                HashMap::new();
            for rule in &self.all_rules {
                let k = Self::trigger_kind(&rule.trigger);
                by_kind_phase
                    .entry((k, rule.phase))
                    .or_default()
                    .push(rule.clone());
            }
            by_kind_phase
        });
        map.get(&(kind, phase)).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get the maximum phase value for a trigger kind.
    /// Returns 2 as default if no rules exist for that kind.
    pub fn max_phase_for(&self, kind: TriggerKind) -> u32 {
        let map = self.max_phase_by_kind.get_or_init(|| {
            let mut max_phases: HashMap<TriggerKind, u32> = HashMap::new();
            for rule in &self.all_rules {
                let k = Self::trigger_kind(&rule.trigger);
                let current_max = max_phases.entry(k).or_insert(2);
                if rule.phase > *current_max {
                    *current_max = rule.phase;
                }
            }
            max_phases
        });
        map.get(&kind).copied().unwrap_or(2)
    }

    /// Get all (pallet, variant) pairs from event storage rules.
    pub fn event_triggers(&self) -> &HashSet<(i32, i32)> {
        self.event_triggers.get_or_init(|| {
            let mut triggers = HashSet::new();
            for rule in &self.all_rules {
                if let StorageIndexingTrigger::Event { pallet, variant } = &rule.trigger {
                    triggers.insert((*pallet as i32, *variant as i32));
                }
            }
            triggers
        })
    }

    /// Check if an event (pallet, variant) has a storage rule at ANY phase.
    /// Used to fast-track events without rules directly to max_phase.
    pub fn has_any_event_rule(&self, pallet: i32, variant: i32) -> bool {
        self.event_triggers().contains(&(pallet, variant))
    }

    /// Check if an event (pallet, variant) has a storage rule at a specific phase.
    pub fn has_event_rule_at_phase(&self, pallet: i32, variant: i32, phase: u32) -> bool {
        let map = self.event_triggers_by_phase.get_or_init(|| {
            let mut by_phase: HashMap<u32, HashSet<(i32, i32)>> = HashMap::new();
            for rule in &self.all_rules {
                if let StorageIndexingTrigger::Event {
                    pallet: p,
                    variant: v,
                } = &rule.trigger
                {
                    by_phase
                        .entry(rule.phase)
                        .or_default()
                        .insert((*p as i32, *v as i32));
                }
            }
            by_phase
        });
        map.get(&phase)
            .map(|triggers| triggers.contains(&(pallet, variant)))
            .unwrap_or(false)
    }
}

/// Check if a block is within the pruning threshold for a rule
pub fn is_block_within_pruning_threshold(
    block_number: u32,
    finalized_block: u32,
    pruning: &Option<StoragePruning>,
) -> bool {
    match pruning {
        Some(StoragePruning::KeepBlocks { blocks }) => {
            let threshold = finalized_block.saturating_sub(*blocks);
            block_number >= threshold
        }
        None => true, // No pruning configured, always process
    }
}

/// Context for storage indexing - can represent either an event or extrinsic
pub struct StorageIndexingContext<'a> {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub event_index: Option<i32>,
    pub data: Option<&'a JsonValue>,
    pub account_id: String,
    pub block_time: chrono::DateTime<chrono::Utc>,
}

/// Apply value_path extraction to a JSON value.
/// If value_path is Some, extracts the value at that path.
/// If value_path is None, returns the original value unchanged.
fn apply_value_path(value: JsonValue, value_path: Option<&str>) -> JsonValue {
    match value_path {
        Some(path) if !path.is_empty() => {
            match crate::data_extraction::resolve_json_path(&value, path) {
                Ok(values) => {
                    if let Some(extracted) = values.first() {
                        (*extracted).clone()
                    } else {
                        // Empty result (e.g., from empty arrays) - return original
                        value
                    }
                }
                Err(e) => {
                    warn!(
                        "value_path '{}' extraction failed: {}, storing original",
                        path, e
                    );
                    value
                }
            }
        }
        _ => value,
    }
}

/// Prune old storage snapshots based on rule configurations.
/// Uses batched deletes to avoid long-running transactions that lock the database.
pub async fn prune_storage_snapshots(
    db_pool: &Pool<Postgres>,
    storage_rules: &[StorageIndexingRule],
    finalized_block: u64,
    batch_size: i64,
) -> Result<u64, anyhow::Error> {
    let mut total_deleted = 0u64;

    for rule in storage_rules {
        if let Some(StoragePruning::KeepBlocks { blocks }) = &rule.pruning {
            let threshold_block = finalized_block.saturating_sub(*blocks as u64) as i64;

            debug!(
                "Pruning storage snapshots for rule '{}' older than block {} (keeping last {} blocks)",
                rule.name, threshold_block, blocks
            );

            // Delete in batches to avoid long-running transactions
            loop {
                let result = sqlx::query(
                    r#"
                    DELETE FROM storage_snapshots
                    WHERE id IN (
                        SELECT id FROM storage_snapshots
                        WHERE config_rule = $1 AND block_number < $2
                        LIMIT $3
                    )
                    "#,
                )
                .bind(&rule.name)
                .bind(threshold_block)
                .bind(batch_size)
                .execute(db_pool)
                .await?;

                let deleted = result.rows_affected();
                total_deleted += deleted;

                if deleted == 0 {
                    break;
                }

                debug!(
                    "Pruned {} storage snapshots for rule '{}' (total: {})",
                    deleted, rule.name, total_deleted
                );

                // Small delay between batches to reduce database contention
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    }

    Ok(total_deleted)
}

pub async fn process_extrinsic_storage_indexing(
    worker_id: u32,
    extrinsic: ExtrinsicRow,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
) -> Result<(), anyhow::Error> {
    const PHASE: ExtrinsicsIndexPhase = ExtrinsicsIndexPhase::StorageIndexed;
    trace!(
        "Process phase {:?} for extrinsic {:?}",
        PHASE,
        extrinsic.id()
    );

    let extrinsic_rules = crate::config::storage_rules().by_trigger(TriggerKind::Extrinsic);

    // Find matching rule for this extrinsic (rules are already filtered to Extrinsic trigger type)
    let matching_rules: Vec<_> = extrinsic_rules
        .iter()
        .filter(|rule| {
            // Match by pallet and method indices directly
            match &rule.trigger {
                StorageIndexingTrigger::Extrinsic { pallet, method } => {
                    extrinsic.pallet == *pallet as i32 && extrinsic.method == *method as i32
                }
                _ => false, // Should not happen since rules are pre-filtered
            }
        })
        .cloned()
        .collect();

    // Process each matching rule
    if !matching_rules.is_empty() {
        let block_number = extrinsic.block_number as u64;

        // Get block hash from database
        let block_row = sqlx::query!(
            "SELECT hash FROM blocks WHERE block_number = $1",
            extrinsic.block_number
        )
        .fetch_optional(db_pool)
        .await?;

        let block_hash = if let Some(b) = block_row {
            H256::from_slice(
                &hex::decode(&b.hash).map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
            )
        } else {
            return Err(anyhow!("Block {} not found in database for extrinsic {}, skipping storage indexing (will be requeued when block known)",
                block_number,
                extrinsic.id()));
        };

        // Get block for storage queries
        let block = client.blocks().at(block_hash).await?;

        // Create context for storage indexing
        let ctx = StorageIndexingContext {
            block_number: extrinsic.block_number,
            extrinsic_index: extrinsic.index,
            event_index: None,
            data: extrinsic.data.as_ref(),
            account_id: extrinsic.account_id.clone(),
            block_time: extrinsic.block_time,
        };

        // Process storage rules using shared function
        process_storage_rules(worker_id, &ctx, &matching_rules, db_pool, client, &block).await?;
    }

    // Update phase
    sqlx::query("UPDATE extrinsics SET phase = $1 WHERE block_number = $2 AND index = $3;")
        .bind(PHASE as i32)
        .bind(extrinsic.block_number)
        .bind(extrinsic.index)
        .execute(db_pool)
        .await?;

    Ok(())
}

/// Process storage indexing rules for an event at a specific target phase.
/// `target_phase` is the DB value (2 = StorageIndexed(0), 3 = StorageIndexed(1), etc.)
#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("event-phase-{:?}", worker_id),
        target_phase = target_phase,
        block_number = event.block_number,
        extrinsic = event.extrinsic_index,
        event = event.index
    )
)]
pub async fn process_events_storage_indexing(
    worker_id: u32,
    event: EventRow,
    target_phase: u32,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    finalized_block: Option<u32>,
) -> Result<(), anyhow::Error> {
    trace!("Process phase {} for event", target_phase);

    let event_rules: &[StorageIndexingRule] =
        crate::config::storage_rules().by_trigger_and_phase(TriggerKind::Event, target_phase);

    // Find matching rule for this event (rules are already filtered to Event trigger type)
    // Filter by target phase and pruning threshold
    let matching_rules: Vec<_> = event_rules
        .iter()
        .filter(|rule| {
            // Match by pallet and variant indices directly
            let matches_trigger = match &rule.trigger {
                StorageIndexingTrigger::Event {
                    pallet, variant, ..
                } => event.pallet == *pallet as i32 && event.variant == *variant as i32,
                _ => false,
            };

            // If trigger matches, check if block is within pruning threshold
            if matches_trigger {
                if let Some(finalized) = finalized_block {
                    let within_threshold = is_block_within_pruning_threshold(
                        event.block_number as u32,
                        finalized,
                        &rule.pruning,
                    );
                    if !within_threshold {
                        trace!(
                            "Skipping storage rule '{}' for event {}-{}.{} since outside pruning threshold",
                            rule.name,event.block_number,
                event.extrinsic_index, event.index
                        );
                    }
                    within_threshold
                } else {
                    true // No finalized block info, process anyway
                }
            } else {
                false
            }
        })
        .cloned()
        .collect();

    // Process each matching rule
    if !matching_rules.is_empty() {
        // Get block hash from database
        let block_row = sqlx::query!(
            "SELECT hash FROM blocks WHERE block_number = $1",
            event.block_number
        )
        .fetch_optional(db_pool)
        .await?;

        let block_hash = if let Some(b) = block_row {
            H256::from_slice(
                &hex::decode(&b.hash).map_err(|e| anyhow!("Failed to decode block hash: {}", e))?,
            )
        } else {
            return Err(anyhow!("Block not found in database for event {}-{}.{}, skipping event indexing (will be requeued when block known)",
                event.block_number,
                event.extrinsic_index,
                event.index));
        };

        // Get the account_id from the extrinsic for ORIGIN key_path handling
        let extrinsic_row: Option<(String,)> = sqlx::query_as(
            "SELECT account_id FROM extrinsics WHERE block_number = $1 AND index = $2",
        )
        .bind(event.block_number)
        .bind(event.extrinsic_index)
        .fetch_optional(db_pool)
        .await?;

        let account_id = extrinsic_row.map(|e| e.0).unwrap_or_default();

        // Get block for storage queries
        let block = client.blocks().at(block_hash).await?;

        // Create context for storage indexing
        let ctx = StorageIndexingContext {
            block_number: event.block_number,
            extrinsic_index: event.extrinsic_index,
            event_index: Some(event.index),
            data: event.data.as_ref(),
            account_id,
            block_time: event.block_time,
        };

        // Process storage rules using shared function
        process_storage_rules(worker_id, &ctx, &matching_rules, db_pool, client, &block).await?;
    }

    Ok(())
}

/// Insert or update a storage snapshot (ON CONFLICT DO UPDATE)
async fn insert_storage_snapshot(
    db_pool: &Pool<Postgres>,
    block_number: i64,
    extrinsic_index: i32,
    event_index: Option<i32>,
    pallet: i32,
    storage_location: &str,
    storage_keys: JsonValue,
    data: JsonValue,
    config_rule: &str,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<(), anyhow::Error> {
    sqlx::query(
        r#"
        INSERT INTO storage_snapshots(block_number, extrinsic_index, event_index, pallet, storage_location, storage_keys, data, config_rule, block_time)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (block_number, extrinsic_index, event_index, pallet, storage_location, storage_keys) DO UPDATE
        SET event_index = EXCLUDED.event_index,
            data = EXCLUDED.data,
            config_rule = EXCLUDED.config_rule,
            block_time = EXCLUDED.block_time
        "#,
    )
    .bind(block_number)
    .bind(extrinsic_index)
    .bind(event_index)
    .bind(pallet)
    .bind(storage_location)
    .bind(storage_keys)
    .bind(data)
    .bind(config_rule)
    .bind(block_time)
    .execute(db_pool)
    .await?;

    trace!(
        "Upserted storage snapshot for {}::{} at event {}-{}.{:?}",
        pallet,
        storage_location,
        block_number,
        extrinsic_index,
        event_index.map(|e| e.to_string()).unwrap_or_default()
    );

    Ok(())
}

/// Process storage rules for either an event or extrinsic context.
/// This is the shared logic extracted from process_events_storage_indexing.
#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("storage-indexing-{:?}", worker_id),
        block_number = ctx.block_number,
        extrinsic = ctx.extrinsic_index,
        event = ctx.event_index,
    )
)]
pub async fn process_storage_rules<'a>(
    worker_id: u32,
    ctx: &StorageIndexingContext<'a>,
    matching_rules: &[StorageIndexingRule],
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
) -> Result<(), anyhow::Error> {
    for rule in matching_rules {
        trace!(
            "Processing storage rule '{}' for {}-{}.{:?}",
            rule.name,
            ctx.block_number,
            ctx.extrinsic_index,
            ctx.event_index
        );

        // Process each storage item in the rule
        'snapshot_loop: for storage_item in &rule.storage {
            // Get key_paths from the storage item
            let key_paths = &storage_item.key_paths;
            let metadata = client.metadata();
            let pallet_metadata = metadata.pallet_by_index(storage_item.pallet as u8);

            if let Some(pallet) = pallet_metadata {
                // Log all available storage entries for this pallet
                if let Some(storage) = pallet.storage() {
                    if let Some(storage_entry) =
                        storage.entry_by_name(&storage_item.storage_location)
                    {
                        match storage_entry.entry_type() {
                            StorageEntryType::Plain(_) => {
                                // StorageValue - no keys needed
                                let storage_query = subxt::dynamic::storage(
                                    pallet.name(),
                                    &storage_item.storage_location,
                                    vec![],
                                );

                                match block.storage().fetch(&storage_query).await {
                                    Ok(Some(value)) => {
                                        let scale_value = value.to_value()?;
                                        let value_json =
                                            serde_json::to_value(ValueWrapper::from(scale_value))?;
                                        let final_value = apply_value_path(
                                            value_json,
                                            storage_item.value_path.as_deref(),
                                        );

                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            json!([0]), // 0 for StorageValue
                                            final_value,
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    }
                                    Ok(None) => {
                                        // Store null when storage is empty (e.g., job was removed)
                                        trace!(
                                            "Storage {}.{} returned None, storing null",
                                            pallet.name(),
                                            storage_item.storage_location
                                        );
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            json!([0]),
                                            JsonValue::Null,
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to fetch storage {}.{}: {:?}",
                                            pallet.name(),
                                            storage_item.storage_location,
                                            e
                                        );
                                    }
                                }
                            }
                            StorageEntryType::Map {
                                hashers, key_ty, ..
                            } => {
                                let key_count = hashers.len();
                                let types = metadata.types();

                                // Resolve key types from metadata
                                // If key_ty is a tuple, get individual element types; otherwise use key_ty for single key
                                let key_types: Vec<u32> = if key_count > 1 {
                                    // Multiple keys - key_ty should be a tuple
                                    if let Some(ty) = types.resolve(*key_ty) {
                                        if let scale_info::TypeDef::Tuple(tuple) = &ty.type_def {
                                            tuple.fields.iter().map(|f| f.id).collect()
                                        } else {
                                            vec![*key_ty; key_count] // Fallback: use same type for all
                                        }
                                    } else {
                                        vec![*key_ty; key_count]
                                    }
                                } else {
                                    vec![*key_ty]
                                };

                                // Extract keys from data - store as JSON first
                                let mut key_json_values: Vec<JsonValue> = Vec::new();

                                for key_path in key_paths.iter() {
                                    if key_path == "ORIGIN" {
                                        // Special case: use the extrinsic origin (account_id)
                                        key_json_values.push(json!(ctx.account_id.clone()));
                                    } else if let Some(data) = ctx.data {
                                        trace!(
                                            "Extracting key from data using path '{}': {:?}",
                                            key_path,
                                            data
                                        );
                                        let values = match crate::data_extraction::resolve_json_path(
                                            data, key_path,
                                        ) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                warn!(
                                                    "Could not extract key from path '{}' in data: {}",
                                                    key_path, e
                                                );
                                                break;
                                            }
                                        };
                                        if let Some(key_value) = values.first() {
                                            key_json_values.push((*key_value).clone());
                                        } else {
                                            // Empty result (e.g., from empty arrays) - skip silently
                                            break;
                                        }
                                    } else {
                                        warn!(
                                            "Cannot extract key from path '{}' - no data available",
                                            key_path
                                        );
                                        break;
                                    }
                                }

                                // Apply transform if configured (operates on JSON)
                                if let Some(transform) = &storage_item.transform {
                                    match apply_transform(
                                        worker_id,
                                        transform,
                                        key_json_values.clone(),
                                        block,
                                    )
                                    .await
                                    {
                                        Some(transformed_json) => {
                                            trace!(
                                                "Transform {:?} applied, got {} keys",
                                                transform,
                                                transformed_json.len()
                                            );
                                            key_json_values = transformed_json;
                                        }
                                        None => {
                                            trace!(
                                                "Transform {:?} returned None, skipping storage lookup",
                                                transform
                                            );
                                            continue 'snapshot_loop;
                                        }
                                    }
                                }

                                // Convert JSON keys to dynamic values using type information
                                let dynamic_keys: Vec<subxt::dynamic::Value> = key_json_values
                                    .iter()
                                    .zip(key_types.iter().cycle())
                                    .filter_map(|(json_val, type_id)| {
                                        json_to_typed_dynamic_value(json_val, *type_id, types)
                                            .or_else(|| {
                                                trace!(
                                                    "Type-guided conversion failed for {:?}, using fallback",
                                                    json_val
                                                );
                                                json_to_dynamic_value_fallback(json_val)
                                            })
                                    })
                                    .collect();

                                if dynamic_keys.len() == key_count {
                                    // We have all required keys - fetch single storage entry
                                    trace!(
                                        "Fetching storage {}.{} with {} keys: {:?}",
                                        pallet.name(),
                                        storage_item.storage_location,
                                        dynamic_keys.len(),
                                        key_json_values
                                    );

                                    let storage_query = subxt::dynamic::storage(
                                        pallet.name(),
                                        &storage_item.storage_location,
                                        dynamic_keys,
                                    );

                                    match block.storage().fetch(&storage_query).await {
                                        Ok(Some(value)) => {
                                            let scale_value = value.to_value()?;
                                            let value_json = serde_json::to_value(
                                                ValueWrapper::from(scale_value),
                                            )?;
                                            let final_value = apply_value_path(
                                                value_json,
                                                storage_item.value_path.as_deref(),
                                            );

                                            trace!(
                                                "Successfully fetched storage value for {}.{}",
                                                pallet.name(),
                                                storage_item.storage_location
                                            );

                                            // Pass the keys as a JSON array of actual values (not stringified)
                                            insert_storage_snapshot(
                                                db_pool,
                                                ctx.block_number,
                                                ctx.extrinsic_index,
                                                ctx.event_index,
                                                storage_item.pallet as i32,
                                                &storage_item.storage_location,
                                                JsonValue::Array(key_json_values.clone()),
                                                final_value,
                                                &rule.name,
                                                ctx.block_time,
                                            )
                                            .await?;
                                        }
                                        Ok(None) => {
                                            // Store null when storage is empty
                                            trace!(
                                                "Storage {}.{} returned None for keys: {:?}, storing null",
                                                pallet.name(),
                                                storage_item.storage_location,
                                                key_json_values
                                            );
                                            insert_storage_snapshot(
                                                db_pool,
                                                ctx.block_number,
                                                ctx.extrinsic_index,
                                                ctx.event_index,
                                                storage_item.pallet as i32,
                                                &storage_item.storage_location,
                                                JsonValue::Array(key_json_values.clone()),
                                                JsonValue::Null,
                                                &rule.name,
                                                ctx.block_time,
                                            )
                                            .await?;
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to fetch storage {}.{} with keys {:?}: {:?}",
                                                pallet.name(),
                                                storage_item.storage_location,
                                                key_json_values,
                                                e
                                            );
                                        }
                                    }
                                } else if dynamic_keys.len() > key_count {
                                    warn!(
                                        "Extracted {} keys but storage {} only needs {} keys",
                                        dynamic_keys.len(),
                                        storage_item.storage_location,
                                        key_count
                                    );
                                } else if !dynamic_keys.is_empty() {
                                    // Partial keys provided - iterate over all entries with this prefix
                                    trace!(
                                        "Iterating storage {}.{} with partial keys ({}/{}): {:?}",
                                        pallet.name(),
                                        storage_item.storage_location,
                                        dynamic_keys.len(),
                                        key_count,
                                        key_json_values
                                    );

                                    let storage_query = subxt::dynamic::storage(
                                        pallet.name(),
                                        &storage_item.storage_location,
                                        dynamic_keys,
                                    );

                                    // Collect all entries into an array
                                    let mut entries: Vec<JsonValue> = Vec::new();
                                    let mut iter = block.storage().iter(storage_query).await?;

                                    while let Some(Ok(kv)) = iter.next().await {
                                        let scale_value = kv.value.to_value()?;
                                        let value_json =
                                            serde_json::to_value(ValueWrapper::from(scale_value))?;
                                        let final_value = apply_value_path(
                                            value_json,
                                            storage_item.value_path.as_deref(),
                                        );

                                        // kv.keys is already decoded as Vec<scale_value::Value<()>>
                                        // Extract the remaining key(s) we didn't provide
                                        let provided_count = key_json_values.len();
                                        let remaining_key = if kv.keys.len() > provided_count {
                                            let remaining: Vec<JsonValue> = kv.keys
                                                [provided_count..]
                                                .iter()
                                                .map(|k| {
                                                    // Convert Value<()> to Value<u32> for ValueWrapper
                                                    let key_with_ctx =
                                                        k.clone().map_context(|_| 0u32);
                                                    serde_json::to_value(ValueWrapper::from(
                                                        key_with_ctx,
                                                    ))
                                                    .unwrap_or(JsonValue::Null)
                                                })
                                                .collect();

                                            if remaining.len() == 1 {
                                                // Single remaining key
                                                remaining
                                                    .into_iter()
                                                    .next()
                                                    .unwrap_or(JsonValue::Null)
                                            } else {
                                                // Multiple remaining keys
                                                JsonValue::Array(remaining)
                                            }
                                        } else {
                                            // No remaining keys, use all keys
                                            let all_keys: Vec<JsonValue> = kv
                                                .keys
                                                .iter()
                                                .map(|k| {
                                                    let key_with_ctx =
                                                        k.clone().map_context(|_| 0u32);
                                                    serde_json::to_value(ValueWrapper::from(
                                                        key_with_ctx,
                                                    ))
                                                    .unwrap_or(JsonValue::Null)
                                                })
                                                .collect();
                                            JsonValue::Array(all_keys)
                                        };

                                        entries.push(json!({
                                            "key": remaining_key,
                                            "value": final_value
                                        }));
                                    }

                                    if entries.is_empty() {
                                        trace!(
                                            "Storage {}.{} iteration returned no entries for partial keys: {:?}",
                                            pallet.name(),
                                            storage_item.storage_location,
                                            key_json_values
                                        );
                                        // Store null when no entries found
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            JsonValue::Array(key_json_values.clone()),
                                            JsonValue::Null,
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    } else {
                                        trace!(
                                            "Storage {}.{} iteration returned {} entries",
                                            pallet.name(),
                                            storage_item.storage_location,
                                            entries.len()
                                        );
                                        // Store all entries as JSON array
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            JsonValue::Array(key_json_values.clone()),
                                            JsonValue::Array(entries),
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    }
                                } else if storage_item.group_by_first_key {
                                    // No keys provided - group by first key, one row per unique first key
                                    trace!(
                                        "Iterating ALL entries for storage {}.{} (grouped by first key)",
                                        pallet.name(),
                                        storage_item.storage_location,
                                    );

                                    let storage_query = subxt::dynamic::storage(
                                        pallet.name(),
                                        &storage_item.storage_location,
                                        Vec::<subxt::dynamic::Value>::new(),
                                    );

                                    // Group: first_key_string -> (first_key_json, {remaining_key: value, ...})
                                    let mut grouped: std::collections::HashMap<
                                        String,
                                        (JsonValue, serde_json::Map<String, JsonValue>),
                                    > = std::collections::HashMap::new();
                                    let mut iter = block.storage().iter(storage_query).await?;

                                    while let Some(Ok(kv)) = iter.next().await {
                                        if kv.keys.is_empty() {
                                            continue;
                                        }

                                        let scale_value = kv.value.to_value()?;
                                        let value_json =
                                            serde_json::to_value(ValueWrapper::from(scale_value))?;
                                        let final_value = apply_value_path(
                                            value_json,
                                            storage_item.value_path.as_deref(),
                                        );

                                        // Convert all keys to JSON
                                        let all_keys: Vec<JsonValue> = kv
                                            .keys
                                            .iter()
                                            .map(|k| {
                                                let key_with_ctx = k.clone().map_context(|_| 0u32);
                                                serde_json::to_value(ValueWrapper::from(
                                                    key_with_ctx,
                                                ))
                                                .unwrap_or(JsonValue::Null)
                                            })
                                            .collect();

                                        let first_key = all_keys[0].clone();
                                        let first_key_str = first_key.to_string();

                                        // Remaining key(s) become the map key
                                        let remaining_key = if all_keys.len() == 2 {
                                            match &all_keys[1] {
                                                JsonValue::String(s) => s.clone(),
                                                other => other.to_string(),
                                            }
                                        } else if all_keys.len() > 2 {
                                            serde_json::to_string(&all_keys[1..])
                                                .unwrap_or_default()
                                        } else {
                                            "value".to_string()
                                        };

                                        grouped
                                            .entry(first_key_str)
                                            .or_insert_with(|| (first_key, serde_json::Map::new()))
                                            .1
                                            .insert(remaining_key, final_value);
                                    }

                                    trace!(
                                        "Storage {}.{} grouped: {} unique first keys",
                                        pallet.name(),
                                        storage_item.storage_location,
                                        grouped.len()
                                    );

                                    // Insert one row per unique first key
                                    for (_, (first_key_json, data_map)) in grouped {
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            json!([first_key_json]),
                                            JsonValue::Object(data_map),
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    }
                                } else {
                                    // No keys provided - iterate over ALL entries in the storage map
                                    trace!(
                                        "Iterating ALL entries for storage {}.{} (no keys provided)",
                                        pallet.name(),
                                        storage_item.storage_location,
                                    );

                                    let storage_query = subxt::dynamic::storage(
                                        pallet.name(),
                                        &storage_item.storage_location,
                                        Vec::<subxt::dynamic::Value>::new(),
                                    );

                                    // Collect all entries into an array
                                    let mut entries: Vec<JsonValue> = Vec::new();
                                    let mut iter = block.storage().iter(storage_query).await?;

                                    while let Some(Ok(kv)) = iter.next().await {
                                        let scale_value = kv.value.to_value()?;
                                        let value_json =
                                            serde_json::to_value(ValueWrapper::from(scale_value))?;
                                        let final_value = apply_value_path(
                                            value_json,
                                            storage_item.value_path.as_deref(),
                                        );

                                        // Convert all keys to JSON
                                        let all_keys: Vec<JsonValue> = kv
                                            .keys
                                            .iter()
                                            .map(|k| {
                                                let key_with_ctx = k.clone().map_context(|_| 0u32);
                                                serde_json::to_value(ValueWrapper::from(
                                                    key_with_ctx,
                                                ))
                                                .unwrap_or(JsonValue::Null)
                                            })
                                            .collect();

                                        // Use the key(s) directly - single key as value, multiple as array
                                        let entry_key = if all_keys.len() == 1 {
                                            all_keys.into_iter().next().unwrap_or(JsonValue::Null)
                                        } else {
                                            JsonValue::Array(all_keys)
                                        };

                                        entries.push(json!({
                                            "key": entry_key,
                                            "value": final_value
                                        }));
                                    }

                                    if entries.is_empty() {
                                        trace!(
                                            "Storage {}.{} iteration returned no entries",
                                            pallet.name(),
                                            storage_item.storage_location,
                                        );
                                        // Store empty array when no entries found
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            json!([]),
                                            JsonValue::Array(vec![]),
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    } else {
                                        trace!(
                                            "Storage {}.{} full iteration returned {} entries",
                                            pallet.name(),
                                            storage_item.storage_location,
                                            entries.len()
                                        );
                                        // Store all entries as JSON array
                                        insert_storage_snapshot(
                                            db_pool,
                                            ctx.block_number,
                                            ctx.extrinsic_index,
                                            ctx.event_index,
                                            storage_item.pallet as i32,
                                            &storage_item.storage_location,
                                            json!([]),
                                            JsonValue::Array(entries),
                                            &rule.name,
                                            ctx.block_time,
                                        )
                                        .await?;
                                    }
                                }
                            }
                        }
                    } else {
                        let entry_names: Vec<&str> =
                            storage.entries().iter().map(|e| e.name()).collect();
                        warn!(
                            "Storage location '{}' not found in pallet '{}' (index {}) which has {} storage entries: {:?}",
                            storage_item.storage_location,
                            pallet.name(),
                            storage_item.pallet,
                            entry_names.len(),
                            entry_names
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

/// Convert a JSON value to a subxt dynamic Value using type information from metadata.
/// This is more accurate than guessing from JSON structure alone.
fn json_to_typed_dynamic_value(
    json: &JsonValue,
    type_id: u32,
    types: &scale_info::PortableRegistry,
) -> Option<subxt::dynamic::Value> {
    use scale_info::TypeDef;

    let ty = types.resolve(type_id)?;

    match &ty.type_def {
        TypeDef::Primitive(prim) => json_primitive_to_dynamic(json, prim),

        TypeDef::Composite(composite) => {
            // Composite types (structs) - usually AccountId32 or similar
            let fields = &composite.fields;
            if fields.len() == 1 && fields[0].name.is_none() {
                // Newtype wrapper (e.g., AccountId32 wrapping [u8; 32])
                json_to_typed_dynamic_value(json, fields[0].ty.id, types)
            } else if fields.iter().all(|f| f.name.is_none()) {
                // Tuple-like struct with unnamed fields
                match json {
                    JsonValue::Array(arr) if arr.len() == fields.len() => {
                        let values: Option<Vec<_>> = arr
                            .iter()
                            .zip(fields.iter())
                            .map(|(v, f)| json_to_typed_dynamic_value(v, f.ty.id, types))
                            .collect();
                        values.map(subxt::dynamic::Value::unnamed_composite)
                    }
                    _ => json_to_dynamic_value_fallback(json),
                }
            } else {
                // Named struct
                json_to_dynamic_value_fallback(json)
            }
        }

        TypeDef::Array(arr) => {
            // Fixed-size array (e.g., [u8; 32] for AccountId)
            let len = arr.len as usize;
            let elem_type = arr.type_param.id;

            // For byte arrays, accept hex strings
            if let Some(elem_ty) = types.resolve(elem_type) {
                if matches!(
                    &elem_ty.type_def,
                    TypeDef::Primitive(scale_info::TypeDefPrimitive::U8)
                ) {
                    if let JsonValue::String(s) = json {
                        let hex_str = s.trim_start_matches("0x");
                        if let Ok(bytes) = hex::decode(hex_str) {
                            if bytes.len() == len {
                                return Some(subxt::dynamic::Value::from_bytes(&bytes));
                            }
                        }
                    }
                }
            }

            // Otherwise try as JSON array
            match json {
                JsonValue::Array(arr_json) if arr_json.len() == len => {
                    let values: Option<Vec<_>> = arr_json
                        .iter()
                        .map(|v| json_to_typed_dynamic_value(v, elem_type, types))
                        .collect();
                    values.map(subxt::dynamic::Value::unnamed_composite)
                }
                _ => json_to_dynamic_value_fallback(json),
            }
        }

        TypeDef::Tuple(tuple) => {
            // For single-element tuples, unwrap
            let fields = &tuple.fields;
            if fields.len() == 1 {
                json_to_typed_dynamic_value(json, fields[0].id, types)
            } else {
                match json {
                    JsonValue::Array(arr) if arr.len() == fields.len() => {
                        let values: Option<Vec<_>> = arr
                            .iter()
                            .zip(fields.iter())
                            .map(|(v, f)| json_to_typed_dynamic_value(v, f.id, types))
                            .collect();
                        values.map(subxt::dynamic::Value::unnamed_composite)
                    }
                    _ => json_to_dynamic_value_fallback(json),
                }
            }
        }

        TypeDef::Sequence(seq) => {
            // Variable-length sequence
            let elem_type = seq.type_param.id;
            match json {
                JsonValue::Array(arr) => {
                    let values: Option<Vec<_>> = arr
                        .iter()
                        .map(|v| json_to_typed_dynamic_value(v, elem_type, types))
                        .collect();
                    values.map(subxt::dynamic::Value::unnamed_composite)
                }
                // For byte sequences, accept hex strings
                JsonValue::String(s) => {
                    if let Some(elem_ty) = types.resolve(elem_type) {
                        if matches!(
                            elem_ty.type_def,
                            TypeDef::Primitive(scale_info::TypeDefPrimitive::U8)
                        ) {
                            let hex_str = s.trim_start_matches("0x");
                            if let Ok(bytes) = hex::decode(hex_str) {
                                return Some(subxt::dynamic::Value::from_bytes(&bytes));
                            }
                        }
                    }
                    json_to_dynamic_value_fallback(json)
                }
                _ => json_to_dynamic_value_fallback(json),
            }
        }

        TypeDef::Variant(variant) => {
            // Enum type
            match json {
                JsonValue::Object(map) if map.len() == 1 => {
                    let (variant_name, inner_value) = map.iter().next()?;

                    // Find the variant definition
                    if let Some(var_def) = variant.variants.iter().find(|v| &v.name == variant_name)
                    {
                        let fields = &var_def.fields;
                        if fields.is_empty() {
                            // Unit variant - use unnamed_variant with empty iter
                            Some(subxt::dynamic::Value::unnamed_variant(
                                variant_name.clone(),
                                std::iter::empty(),
                            ))
                        } else if fields.len() == 1 && fields[0].name.is_none() {
                            // Newtype variant
                            let inner =
                                json_to_typed_dynamic_value(inner_value, fields[0].ty.id, types)?;
                            Some(subxt::dynamic::Value::unnamed_variant(
                                variant_name.clone(),
                                [inner],
                            ))
                        } else {
                            // Struct variant - fall back
                            json_to_dynamic_value_fallback(json)
                        }
                    } else {
                        json_to_dynamic_value_fallback(json)
                    }
                }
                JsonValue::String(s) => {
                    // Unit variant by name
                    if variant
                        .variants
                        .iter()
                        .any(|v| &v.name == s && v.fields.is_empty())
                    {
                        Some(subxt::dynamic::Value::unnamed_variant(
                            s.clone(),
                            std::iter::empty(),
                        ))
                    } else {
                        json_to_dynamic_value_fallback(json)
                    }
                }
                _ => json_to_dynamic_value_fallback(json),
            }
        }

        TypeDef::Compact(compact) => {
            // Compact encoding wraps the inner type
            json_to_typed_dynamic_value(json, compact.type_param.id, types)
        }

        TypeDef::BitSequence(_) => {
            // BitVec - not commonly used in storage keys
            json_to_dynamic_value_fallback(json)
        }
    }
}

/// Convert JSON to dynamic value for primitive types.
fn json_primitive_to_dynamic(
    json: &JsonValue,
    prim: &scale_info::TypeDefPrimitive,
) -> Option<subxt::dynamic::Value> {
    use scale_info::TypeDefPrimitive;

    match prim {
        TypeDefPrimitive::Bool => match json {
            JsonValue::Bool(b) => Some(subxt::dynamic::Value::bool(*b)),
            _ => None,
        },
        TypeDefPrimitive::Char => match json {
            JsonValue::String(s) => s.chars().next().map(subxt::dynamic::Value::char),
            _ => None,
        },
        TypeDefPrimitive::Str => match json {
            JsonValue::String(s) => Some(subxt::dynamic::Value::string(s.clone())),
            _ => None,
        },
        TypeDefPrimitive::U8 => {
            json_to_unsigned(json).map(|n| subxt::dynamic::Value::u128(n as u128))
        }
        TypeDefPrimitive::U16 => {
            json_to_unsigned(json).map(|n| subxt::dynamic::Value::u128(n as u128))
        }
        TypeDefPrimitive::U32 => {
            json_to_unsigned(json).map(|n| subxt::dynamic::Value::u128(n as u128))
        }
        TypeDefPrimitive::U64 => {
            json_to_unsigned(json).map(|n| subxt::dynamic::Value::u128(n as u128))
        }
        TypeDefPrimitive::U128 => json_to_u128(json).map(subxt::dynamic::Value::u128),
        TypeDefPrimitive::U256 => {
            // U256 as hex string or byte array
            match json {
                JsonValue::String(s) => {
                    let hex_str = s.trim_start_matches("0x");
                    hex::decode(hex_str)
                        .ok()
                        .map(|b| subxt::dynamic::Value::from_bytes(&b))
                }
                _ => None,
            }
        }
        TypeDefPrimitive::I8 => {
            json_to_signed(json).map(|n| subxt::dynamic::Value::i128(n as i128))
        }
        TypeDefPrimitive::I16 => {
            json_to_signed(json).map(|n| subxt::dynamic::Value::i128(n as i128))
        }
        TypeDefPrimitive::I32 => {
            json_to_signed(json).map(|n| subxt::dynamic::Value::i128(n as i128))
        }
        TypeDefPrimitive::I64 => {
            json_to_signed(json).map(|n| subxt::dynamic::Value::i128(n as i128))
        }
        TypeDefPrimitive::I128 => json_to_i128(json).map(subxt::dynamic::Value::i128),
        TypeDefPrimitive::I256 => match json {
            JsonValue::String(s) => {
                let hex_str = s.trim_start_matches("0x");
                hex::decode(hex_str)
                    .ok()
                    .map(|b| subxt::dynamic::Value::from_bytes(&b))
            }
            _ => None,
        },
    }
}

fn json_to_unsigned(json: &JsonValue) -> Option<u64> {
    match json {
        JsonValue::Number(n) => n.as_u64(),
        JsonValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_to_signed(json: &JsonValue) -> Option<i64> {
    match json {
        JsonValue::Number(n) => n.as_i64(),
        JsonValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

fn json_to_u128(json: &JsonValue) -> Option<u128> {
    match json {
        JsonValue::Number(n) => n.as_u64().map(|n| n as u128),
        JsonValue::String(s) => {
            // Try decimal first, then hex
            s.parse().ok().or_else(|| {
                let hex_str = s.trim_start_matches("0x");
                u128::from_str_radix(hex_str, 16).ok()
            })
        }
        // Array of little-endian u64 limbs: ["437", "0"] => 437 + 0*2^64
        // This happens when subxt serializes u128 values inconsistently
        JsonValue::Array(arr) if !arr.is_empty() && arr.len() <= 2 => {
            let mut result: u128 = 0;
            for (i, limb) in arr.iter().enumerate() {
                let limb_val: u64 = match limb {
                    JsonValue::String(s) => s.parse().ok()?,
                    JsonValue::Number(n) => n.as_u64()?,
                    _ => return None,
                };
                result |= (limb_val as u128) << (i * 64);
            }
            Some(result)
        }
        _ => None,
    }
}

fn json_to_i128(json: &JsonValue) -> Option<i128> {
    match json {
        JsonValue::Number(n) => n.as_i64().map(|n| n as i128),
        JsonValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Fallback conversion when type info doesn't help.
/// Uses heuristics based on JSON structure.
fn json_to_dynamic_value_fallback(json: &JsonValue) -> Option<subxt::dynamic::Value> {
    match json {
        JsonValue::Object(map) if map.len() == 1 => {
            let (variant_name, inner_value) = map.iter().next()?;
            let inner_dynamic = json_to_dynamic_value_fallback(inner_value)?;
            Some(subxt::dynamic::Value::unnamed_variant(
                variant_name.clone(),
                [inner_dynamic],
            ))
        }
        JsonValue::String(s) => {
            if let Ok(num) = s.parse::<u128>() {
                return Some(subxt::dynamic::Value::u128(num));
            }
            let hex_str = s.trim_start_matches("0x");
            if let Ok(bytes) = hex::decode(hex_str) {
                return Some(subxt::dynamic::Value::from_bytes(&bytes));
            }
            Some(subxt::dynamic::Value::string(s.clone()))
        }
        JsonValue::Number(n) => {
            if let Some(u) = n.as_u64() {
                Some(subxt::dynamic::Value::u128(u as u128))
            } else if let Some(i) = n.as_i64() {
                Some(subxt::dynamic::Value::i128(i as i128))
            } else {
                None
            }
        }
        JsonValue::Array(arr) => {
            let values: Option<Vec<_>> = arr.iter().map(json_to_dynamic_value_fallback).collect();
            values.map(subxt::dynamic::Value::unnamed_composite)
        }
        JsonValue::Bool(b) => Some(subxt::dynamic::Value::bool(*b)),
        JsonValue::Null => None,
        JsonValue::Object(_) => None,
    }
}
