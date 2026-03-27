use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::OnceLock,
};

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
/// Cached set of (pallet, variant) that have job extraction rules
static JOB_EXTRACTION_EVENTS: OnceLock<HashSet<(i32, i32)>> = OnceLock::new();

/// Initialize global configuration from environment. Must be called once at startup.
/// Loads settings from yaml files and initializes all config-based globals.
pub async fn init_globals(settings: Settings, client: OnlineClient<PolkadotConfig>) -> Result<()> {
    STORAGE_RULES
        .set(FilteredStorageRules::new(
            settings.indexer.storage_indexing.clone(),
        ))
        .ok()
        .expect("STORAGE_RULES already initialized");

    // Build set of events that have job extraction rules (before moving settings)
    let mut job_extraction_events = HashSet::new();
    for transformations in settings.indexer.event_transformations.values() {
        for t in transformations {
            job_extraction_events.insert((t.pallet as i32, t.variant as i32));
        }
    }
    info!(
        "Built job extraction events set with {} entries",
        job_extraction_events.len()
    );
    JOB_EXTRACTION_EVENTS
        .set(job_extraction_events)
        .ok()
        .expect("JOB_EXTRACTION_EVENTS already initialized");

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

/// Check if an event (pallet, variant) has any processing rules (job extraction OR storage rules).
/// Used to determine if an event needs to go through the processing pipeline.
pub fn event_needs_processing(pallet: i32, variant: i32) -> bool {
    // Check job extraction rules
    let has_job_extraction = JOB_EXTRACTION_EVENTS
        .get()
        .map(|set| set.contains(&(pallet, variant)))
        .unwrap_or(false);

    // Check storage rules
    let has_storage_rules = storage_rules().has_any_event_rule(pallet, variant);

    has_job_extraction || has_storage_rules
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
    /// Paths to extract storage keys from event/extrinsic data.
    /// Each path extracts one key (e.g., "[0]" for first element, "ORIGIN" for extrinsic signer).
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

/// Pruning strategy for storage snapshots
#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "strategy", rename_all = "snake_case")]
pub enum StoragePruning {
    /// Keep snapshots for the last N blocks from the current finalized block
    KeepBlocks {
        /// Number of blocks to keep (e.g., 100000 keeps ~2 weeks of data at 6s blocks)
        blocks: u32,
    },
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
    /// Optional pruning strategy for this rule's snapshots
    #[serde(default)]
    pub pruning: Option<StoragePruning>,
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

/// Configuration for reprocessing a specific event at a target phase.
#[derive(Deserialize, Clone, Debug)]
pub struct ReprocessEvent {
    /// Block number of the extrinsic
    pub block_number: i64,
    /// Index of the extrinsic within the block
    pub extrinsic_index: i32,
    /// Event index within the extrinsic
    pub index: i32,
    /// Target phase to set the event to (will be processed from this phase)
    pub phase: u32,
}

#[derive(Deserialize, Clone, Default)]
pub struct ReprocessSettings {
    #[serde(default)]
    pub events: Vec<ReprocessEvent>,
    /// Block hashes to reprocess (queued before backwards/finalized blocks)
    /// Format: hex string without 0x prefix
    #[serde(default)]
    pub blocks: Vec<String>,
}

#[derive(Deserialize, Clone)]
pub struct IndexerSettings {
    pub archive_nodes: Vec<String>,
    pub index_finalized: bool,
    pub index_backwards: bool,
    pub index_phases: bool,
    pub index_from_block: u32,
    #[serde(default)]
    pub index_from_epoch: i64,
    pub num_workers_backwards: u32,
    pub num_workers_gaps: u32,
    pub num_workers_phases: u32,
    pub num_workers_finalized: u32,
    pub num_conn_phases: u32,
    pub num_db_conn_phases: u32,
    pub max_blocks_per_bulk_insert: usize,
    pub event_transformations: HashMap<u32, Vec<JobFromEventTransformation>>,
    pub extrinsic_transformations: HashMap<u32, Vec<AddressFromExtrinsicTransformation>>,
    #[serde(default)]
    pub storage_indexing: Vec<StorageIndexingRule>,
    #[serde(default)]
    pub reprocess: ReprocessSettings,
    /// Number of parallel event queuers. Each queuer handles a portion of the block range.
    /// Default is 4 (single queuer). Set higher to speed up queuing from index_from_block to finalized.
    #[serde(default = "default_num_event_queuers")]
    pub num_event_queuers: u32,
}

fn default_num_event_queuers() -> u32 {
    4
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
