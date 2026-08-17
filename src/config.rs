use std::{collections::HashMap, path::Path, sync::OnceLock};

use anyhow::Result;
use config::{Config, Environment, File};
use serde::Deserialize;
use subxt::{OnlineClient, PolkadotConfig};
use tracing::info;

use crate::storage_indexing::FilteredStorageRules;

// ============================================
// Global configuration (initialized once at startup)
// ============================================

static SETTINGS: OnceLock<Settings> = OnceLock::new();
static STORAGE_RULES: OnceLock<FilteredStorageRules> = OnceLock::new();
static PALLET_METHOD_MAP: OnceLock<HashMap<(String, String), (u32, u32)>> = OnceLock::new();

/// Initialize global configuration from environment. Must be called once at startup.
/// Loads settings from yaml files and initializes all config-based globals.
pub async fn init_globals(settings: Settings, client: OnlineClient<PolkadotConfig>) -> Result<()> {
    STORAGE_RULES
        .set(FilteredStorageRules::new(
            settings.indexer.storage_indexing.clone(),
        ))
        .ok()
        .expect("STORAGE_RULES already initialized");

    SETTINGS
        .set(settings)
        .ok()
        .expect("SETTINGS already initialized");

    // Build pallet/method name to index mapping from metadata (needed for address extraction)
    let reverse_map = super::metadata::get_extrinsics_reverse_map(&client).await;
    let pallet_method_map = super::metadata::build_pallet_method_map(&reverse_map);
    info!(
        "Built pallet/method map with {} entries",
        pallet_method_map.len()
    );

    // Initialize pallet/method map (requires connected client)
    super::config::init_pallet_method_map(pallet_method_map);

    Ok(())
}

/// Initialize pallet/method map from chain metadata. Must be called after client is connected.
pub fn init_pallet_method_map(map: HashMap<(String, String), (u32, u32)>) {
    PALLET_METHOD_MAP
        .set(map)
        .ok()
        .expect("PALLET_METHOD_MAP already initialized");
}

pub fn settings() -> &'static Settings {
    SETTINGS
        .get()
        .expect("SETTINGS not initialized - call init_globals first")
}

pub fn storage_rules() -> &'static FilteredStorageRules {
    STORAGE_RULES
        .get()
        .expect("STORAGE_RULES not initialized - call init_globals first")
}

pub fn pallet_method_map() -> &'static HashMap<(String, String), (u32, u32)> {
    PALLET_METHOD_MAP
        .get()
        .expect("PALLET_METHOD_MAP not initialized - call init_pallet_method_map first")
}

pub fn extrinsic_transformations() -> &'static HashMap<u32, Vec<AddressFromExtrinsicTransformation>>
{
    &settings().indexer.extrinsic_transformations
}

pub fn event_transformations() -> &'static HashMap<u32, Vec<JobFromEventTransformation>> {
    &settings().indexer.event_transformations
}

/// Whether phase processing (extrinsic transformations, event job extraction,
/// storage_indexing) should run for a block. Below the configured threshold,
/// rows are still inserted at `Created` but are not queued for the phase
/// pipeline, so a later threshold lowering can pick them up.
pub fn phase_processing_enabled_for_block(block_number: i64) -> bool {
    block_number >= settings().indexer.index_phases_from_block as i64
}

#[derive(Deserialize, Clone)]
pub struct ServerSettings {
    pub host: String,
    pub port: u16,
    pub query_timeout_seconds: u64,
    pub num_db_connections: u32,
}

#[derive(Deserialize, Clone)]
pub struct AuthSettings {
    pub api_key: String,
}

#[derive(Deserialize, Clone)]
pub struct DatabaseSettings {
    pub username: String,
    pub password: String,
    pub host: String,
    pub port: u16,
    pub database: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct JobFromEventTransformation {
    pub pallet: u32,
    pub variant: u32,
    /// The path where to find the job id in event's payload.
    ///
    /// E.g. empty path `""` means job_id is at root level, i.e. the root element has to be a list like:
    ///
    /// ```json
    /// [
    ///   "0x45f6d78c64b153bac9726a51712cb4eb863a4fce932acaa8c89dc5d546165e7b",
    ///   "38666"
    /// ]
    /// ```
    pub data_path: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct AddressFromExtrinsicTransformation {
    pub pallet: u32,
    pub method: u32,
    /// The path where to find the address in extrinsic's signature field.
    ///
    /// E.g. "[0].id.account_id" would extract from signature field like:
    ///
    /// ```json
    /// [
    ///   {
    ///     "id": {
    ///       "account_id": "0x..."
    ///     }
    ///   }
    /// ]
    /// ```
    pub data_path: String,
}

/// Custom key transform to apply before storage lookup.
/// Each variant maps to a function that transforms extracted key values.
#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "PascalCase")]
pub enum StorageKeyTransform {
    /// Look up the owner of a commitment by querying Uniques.Asset(1, commitment_id).owner
    CommitmentIdToCommitter,
}

#[derive(Deserialize, Clone)]
pub struct StorageItemConfig {
    pub pallet: u32,
    pub storage_location: String,
    /// Paths / values used to build the storage keys, one entry per key dimension.
    /// Each entry is interpreted as:
    /// - `"ORIGIN"`: use the extrinsic signer account_id.
    /// - `"0x…"`: hex-encoded literal key value (no data extraction).
    /// - integer-parseable string (e.g. `"42"`): numeric literal key value (no data extraction).
    /// - anything else: JSON path into the triggering event/extrinsic data
    ///   (e.g. `"[0]"` for first element, `"data.free"` for a nested field, `""` for root).
    #[serde(default)]
    pub key_paths: Vec<String>,
    /// Optional path to extract a specific value from the storage data.
    /// If not specified, the entire storage value is stored.
    /// E.g., "data.free" to extract only the free balance from System::Account.
    #[serde(default)]
    pub value_path: Option<String>,
    /// Optional transform to apply to extracted keys before storage lookup.
    /// If the transform returns None, the storage snapshot is skipped.
    #[serde(default)]
    pub transform: Option<StorageKeyTransform>,
    /// When iterating storage maps (no keys provided), store each entry as its own row
    /// instead of accumulating all entries into a single large JSON array.
    /// Each entry's full key set becomes storage_keys and the value becomes data.
    /// This prevents RPC timeouts and reduces memory usage for large storage maps.
    #[serde(default)]
    pub group_by_first_key: bool,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageIndexingTrigger {
    Extrinsic {
        pallet: u32,
        method: u32,
    },
    Event {
        pallet: u32,
        variant: u32,
    },
    Epoch,
    Init {
        // Will be implemented later - triggers on initial sync
    },
}

/// Pruning policy keyed by `(pallet, storage_location)`. Snapshots whose
/// `block_number` is older than `finalized - blocks` are deleted by the
/// periodic prune loop, regardless of which rule produced them.
#[derive(Deserialize, Clone, Debug)]
pub struct StorageLocationPruning {
    /// Pallet index of the storage entry to prune (e.g. `0` for `System`).
    pub pallet: u32,
    /// Storage entry name (e.g. `"Account"`, `"Metrics"`).
    pub storage_location: String,
    /// Optional config-rule filter — when set, only rows produced by rules
    /// with this name are pruned. Useful when the same `(pallet,
    /// storage_location)` is captured by several rules with different
    /// retention needs. Default: prune every row at this storage location.
    #[serde(default)]
    pub config_rule: Option<String>,
    /// Number of blocks to keep from the current finalized block.
    /// e.g. 446_400 ≈ 31 days at 6s blocks.
    pub blocks: u32,
}

/// When to take snapshots for epoch-triggered rules
#[derive(Deserialize, Clone, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EpochSnapshotTiming {
    /// Snapshot at epoch start block (default)
    #[default]
    Start,
    /// Snapshot at epoch end block (= next epoch's start - 1)
    End,
    /// Snapshot at both epoch start and end blocks
    Both,
}

#[derive(Deserialize, Clone)]
pub struct StorageIndexingRule {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub trigger: StorageIndexingTrigger,
    pub storage: Vec<StorageItemConfig>,
    /// Phase at which this rule should be processed (default: 2)
    #[serde(default = "default_rule_phase")]
    pub phase: u32,
    /// When to take snapshots for epoch triggers (default: start)
    /// Only applicable when trigger.type = "epoch"
    #[serde(default)]
    pub epoch_snapshot_at: EpochSnapshotTiming,
}

fn default_rule_phase() -> u32 {
    2
}

#[derive(Deserialize, Clone)]
pub struct IndexerSettings {
    pub archive_nodes: Vec<String>,
    pub index_finalized: bool,
    pub index_backwards: bool,
    pub index_phases: bool,
    pub index_from_block: u32,
    /// Only queue extrinsics/events for phase processing (transformations,
    /// storage_indexing, etc.) when their block_number is at or above this
    /// threshold. Blocks below it are still indexed as raw rows but land
    /// at `phase = MAX` so the phase queuer skips them. Default 0 =
    /// process every block (backwards compatible).
    #[serde(default)]
    pub index_phases_from_block: u32,
    #[serde(default)]
    pub index_from_epoch: i64,
    pub num_workers_backwards: u32,
    pub num_workers_gaps: u32,
    pub num_workers_phases: u32,
    pub num_workers_finalized: u32,
    pub num_conn_phases: u32,
    pub num_db_conn_phases: u32,
    /// Flush the per-worker bulk-insert buffers when the buffered *event*
    /// count crosses this threshold. Events are the dominant write volume
    /// per block (typically 10–100× extrinsics), so gating on events
    /// produces a more predictable per-flush size than gating on extrinsics
    /// or blocks. The 20-second drain timer in `process_blocks_inner`
    /// handles the trickle case where buffered work doesn't reach this
    /// ceiling.
    pub max_events_per_bulk_insert: usize,
    pub event_transformations: HashMap<u32, Vec<JobFromEventTransformation>>,
    pub extrinsic_transformations: HashMap<u32, Vec<AddressFromExtrinsicTransformation>>,
    #[serde(default)]
    pub storage_indexing: Vec<StorageIndexingRule>,
    /// Pruning policies keyed by (pallet, storage_location). Applied
    /// independently of which rule produced the snapshot — useful when
    /// many rules write to the same storage location and you want a
    /// single retention policy for all of them (e.g. `System.Account`).
    #[serde(default)]
    pub storage_pruning: Vec<StorageLocationPruning>,
    /// Number of parallel event queuers. Each queuer handles a portion of the block range.
    pub num_event_queuers: u32,
    /// Backpressure threshold for block queuing. When the extrinsic queue exceeds this length,
    /// block queuers will pause to let workers catch up.
    pub backpressure_threshold: usize,
}

#[derive(Deserialize, Clone)]
pub struct Settings {
    pub indexer: IndexerSettings,
    pub server: ServerSettings,
    pub database: DatabaseSettings,
    pub auth: AuthSettings,
}

pub fn get_config(environment: &str) -> Result<Settings> {
    let base_path = Path::new("configuration");
    let base_file = base_path.join("base.yaml");
    let environment_file = base_path.join(format!("{environment}.yaml"));

    let settings = Config::builder()
        .add_source(File::from(base_file))
        .add_source(File::from(environment_file).required(false))
        .add_source(Environment::with_prefix("ACURAST_INDEXER").separator("__"))
        .build()?
        .try_deserialize::<Settings>()?;
    Ok(settings)
}

pub fn get_config_from_file(file: &str) -> Result<Settings> {
    let settings = Config::builder()
        .add_source(File::with_name(file))
        .build()?
        .try_deserialize::<Settings>()?;
    Ok(settings)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pin down the YAML shape used in `base.yaml` so a refactor of
    /// `StorageLocationPruning` can't silently break config loading.
    #[test]
    fn storage_location_pruning_round_trips() {
        let json = serde_json::json!({
            "pallet": 0,
            "storage_location": "Account",
            "blocks": 446_400,
        });
        let parsed: StorageLocationPruning = serde_json::from_value(json).expect("should parse");
        assert_eq!(parsed.pallet, 0);
        assert_eq!(parsed.storage_location, "Account");
        assert!(parsed.config_rule.is_none());
        assert_eq!(parsed.blocks, 446_400);

        // Optional config_rule filter
        let json_filtered = serde_json::json!({
            "pallet": 48,
            "storage_location": "Metrics",
            "config_rule": "processor_heartbeat_metrics",
            "blocks": 100_800,
        });
        let parsed: StorageLocationPruning =
            serde_json::from_value(json_filtered).expect("should parse");
        assert_eq!(
            parsed.config_rule.as_deref(),
            Some("processor_heartbeat_metrics")
        );
        assert_eq!(parsed.blocks, 100_800);
    }
}
