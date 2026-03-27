use crate::db_timeout::with_timeout;
use crate::entities::{Block, EpochRow, EventRow, ExtrinsicRowWithEvents, Page};
use crate::server::AppState;
use crate::utils::*;
use chrono::{DateTime, Utc};
use parity_scale_codec::{Decode as ScaleDecode, Encode as ScaleEncode};
use serde::{Deserialize, Deserializer, Serialize};
use sqlx::{query_as, Postgres, QueryBuilder};
use subxt::{utils::H256, OnlineClient, PolkadotConfig};
use tracing::trace;

/// Custom JSON-RPC error type
#[derive(Debug, Clone, Serialize)]
pub struct RpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<serde_json::Value>,
}

impl RpcError {
    pub fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    pub fn with_data(code: i32, message: impl Into<String>, data: serde_json::Value) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(data),
        }
    }

    pub fn code(&self) -> i32 {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    /// Database error (-32000)
    pub fn database(msg: impl Into<String>) -> Self {
        Self::new(-32000, msg)
    }

    /// Invalid params (-32602)
    pub fn invalid_params(msg: impl Into<String>) -> Self {
        Self::new(-32602, msg)
    }

    /// Method not found (-32601)
    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, format!("Method not found: {}", method))
    }

    /// Internal error (-32603)
    pub fn internal_error(msg: impl Into<String>) -> Self {
        Self::new(-32603, msg)
    }
}

impl From<serde_json::Error> for RpcError {
    fn from(e: serde_json::Error) -> Self {
        Self::invalid_params(e.to_string())
    }
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

pub type RpcResult<T> = Result<T, RpcError>;

// ============================================================================
// Cursor types for pagination
// ============================================================================

/// Cursor for extrinsics - composite key of block_number and index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExtrinsicCursor {
    pub block_number: i64,
    pub index: i32,
}

/// Cursor for events - composite key of block_number, extrinsic_index, and event index
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCursor {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub index: i32,
}

/// A type that can be deserialized from either a string or a number.
/// Used for pallet, method, and variant parameters to support both numeric IDs
/// and string names (which get resolved via metadata).
#[derive(Debug, Clone)]
pub enum StringOrNumber {
    String(String),
    Number(u32),
}

impl<'de> Deserialize<'de> for StringOrNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, Visitor};

        struct StringOrNumberVisitor;

        impl<'de> Visitor<'de> for StringOrNumberVisitor {
            type Value = StringOrNumber;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or a number")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                // Try to parse as number first
                if let Ok(num) = value.parse::<u32>() {
                    Ok(StringOrNumber::Number(num))
                } else {
                    Ok(StringOrNumber::String(value.to_string()))
                }
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                // Try to parse as number first
                if let Ok(num) = value.parse::<u32>() {
                    Ok(StringOrNumber::Number(num))
                } else {
                    Ok(StringOrNumber::String(value))
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StringOrNumber::Number(value as u32))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(StringOrNumber::Number(value as u32))
            }
        }

        deserializer.deserialize_any(StringOrNumberVisitor)
    }
}

/// Resolves pallet and method from StringOrNumber to numeric indices.
/// Returns (pallet_index, method_index) or an error if names can't be resolved.
/// Supports mixed string/numeric arguments.
pub async fn resolve_extrinsic_pallet_method(
    client: &OnlineClient<PolkadotConfig>,
    pallet: Option<&StringOrNumber>,
    method: Option<&StringOrNumber>,
) -> Result<(Option<u32>, Option<u32>), RpcError> {
    // First resolve the pallet
    let pallet_idx = match pallet {
        None => None,
        Some(StringOrNumber::Number(p)) => Some(*p),
        Some(StringOrNumber::String(pallet_name)) => {
            let pallet_index_map = crate::metadata::get_pallet_index_map(client).await;
            let idx = pallet_index_map.get(pallet_name).ok_or_else(|| {
                RpcError::invalid_params(format!("unknown pallet name: {}", pallet_name))
            })?;
            Some(*idx as u32)
        }
    };

    // Then resolve the method
    let method_idx = match method {
        None => None,
        Some(StringOrNumber::Number(m)) => Some(*m),
        Some(StringOrNumber::String(method_name)) => {
            // Need pallet to resolve method by name
            let pallet_name = match pallet {
                None => {
                    return Err(RpcError::invalid_params(
                        "method name requires pallet to be specified",
                    ));
                }
                Some(StringOrNumber::String(name)) => name.clone(),
                Some(StringOrNumber::Number(p)) => {
                    // Resolve numeric pallet to name first
                    let reverse_pallet_map =
                        crate::metadata::get_reverse_pallet_index_map(client).await;
                    reverse_pallet_map
                        .get(&(*p as u8))
                        .ok_or_else(|| {
                            RpcError::invalid_params(format!("unknown pallet index: {}", p))
                        })?
                        .clone()
                }
            };

            let pallet_map = crate::metadata::get_extrinsics_map(client).await;
            let (method_map, _) = pallet_map.get(&pallet_name).ok_or_else(|| {
                RpcError::invalid_params(format!("unknown pallet: {}", pallet_name))
            })?;

            let call_index = method_map.get(method_name).ok_or_else(|| {
                RpcError::invalid_params(format!(
                    "unknown method '{}' in pallet '{}'",
                    method_name, pallet_name
                ))
            })?;
            Some(call_index.method as u32)
        }
    };

    Ok((pallet_idx, method_idx))
}

/// Resolves event pallet and variant from StringOrNumber to numeric indices.
/// Returns (pallet_index, variant_index) or an error if names can't be resolved.
/// Supports mixed string/numeric arguments.
pub async fn resolve_event_pallet_variant(
    client: &OnlineClient<PolkadotConfig>,
    pallet: Option<&StringOrNumber>,
    variant: Option<&StringOrNumber>,
) -> Result<(Option<u32>, Option<u32>), RpcError> {
    // First resolve the pallet
    let pallet_idx = match pallet {
        None => None,
        Some(StringOrNumber::Number(p)) => Some(*p),
        Some(StringOrNumber::String(pallet_name)) => {
            let pallet_index_map = crate::metadata::get_pallet_index_map(client).await;
            let idx = pallet_index_map.get(pallet_name).ok_or_else(|| {
                RpcError::invalid_params(format!("unknown pallet name: {}", pallet_name))
            })?;
            Some(*idx as u32)
        }
    };

    // Then resolve the variant
    let variant_idx = match variant {
        None => None,
        Some(StringOrNumber::Number(v)) => Some(*v),
        Some(StringOrNumber::String(variant_name)) => {
            // Need pallet to resolve variant by name
            let pallet_name = match pallet {
                None => {
                    return Err(RpcError::invalid_params(
                        "variant name requires pallet to be specified",
                    ));
                }
                Some(StringOrNumber::String(name)) => name.clone(),
                Some(StringOrNumber::Number(p)) => {
                    // Resolve numeric pallet to name first
                    let reverse_pallet_map =
                        crate::metadata::get_reverse_pallet_index_map(client).await;
                    reverse_pallet_map
                        .get(&(*p as u8))
                        .ok_or_else(|| {
                            RpcError::invalid_params(format!("unknown pallet index: {}", p))
                        })?
                        .clone()
                }
            };

            let pallet_map = crate::metadata::get_extrinsics_map(client).await;
            let (_, event_map) = pallet_map.get(&pallet_name).ok_or_else(|| {
                RpcError::invalid_params(format!("unknown pallet: {}", pallet_name))
            })?;

            let call_index = event_map.get(variant_name).ok_or_else(|| {
                RpcError::invalid_params(format!(
                    "unknown event variant '{}' in pallet '{}'",
                    variant_name, pallet_name
                ))
            })?;
            Some(call_index.method as u32)
        }
    };

    Ok((pallet_idx, variant_idx))
}

// Parameter structs for RPC methods

#[derive(Debug, Deserialize, Default)]
pub struct GetBlocksParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub time_from: Option<String>,
    #[serde(default)]
    pub time_to: Option<String>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<i64>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetBlocksCountParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct GetExtrinsicParams {
    pub block_number: u32,
    pub index: i32,
    /// If true, include events for the extrinsic (default: false)
    #[serde(default)]
    pub events: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetExtrinsicByHashParams {
    pub tx_hash: String,
    /// If true, include events for the extrinsic (default: false)
    #[serde(default)]
    pub events: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetExtrinsicsParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub method: Option<StringOrNumber>,
    #[serde(default)]
    pub account_id: Option<String>,
    /// Filter by data (JSON that must be contained in data)
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    /// Filter by event properties (pallet, variant)
    /// Only returns extrinsics that emitted at least one matching event
    #[serde(default)]
    pub event: Option<EventFilter>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<ExtrinsicCursor>,
    #[serde(default)]
    pub limit: Option<u32>,
    /// If true, include events for each extrinsic (default: false)
    #[serde(default)]
    pub events: Option<bool>,
    /// If true, expand batch calls into individual items with mapped events
    #[serde(default)]
    pub explode_batch: Option<bool>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetExtrinsicsCountParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub method: Option<StringOrNumber>,
    #[serde(default)]
    pub account_id: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetExtrinsicAddressesParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub method: Option<StringOrNumber>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<ExtrinsicCursor>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetEventsParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub variant: Option<StringOrNumber>,
    #[serde(default)]
    pub account_id: Option<String>,
    /// Filter by data (JSON that must be contained in data)
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    /// Filter by job. Supports multiple formats:
    /// - SS58: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    /// - Hex: "0xd43593..." or "d43593..." (with or without 0x prefix)
    /// - With seq_id: "5GrwvaEF...#123" or "0xd43593...#456"
    #[serde(default)]
    pub job: Option<String>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<EventCursor>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize)]
pub struct GetEventParams {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub index: i32,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetJobsParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    /// Filter by job. Supports multiple formats:
    /// - SS58: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
    /// - Hex: "0xd43593..." or "d43593..." (with or without 0x prefix)
    /// - With seq_id: "5GrwvaEF...#123" or "0xd43593...#456"
    #[serde(default)]
    pub job: Option<String>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<ExtrinsicCursor>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetEpochsParams {
    /// Filter by epoch range
    #[serde(default)]
    pub epoch_from: Option<u64>,
    #[serde(default)]
    pub epoch_to: Option<u64>,
    /// Filter by block range (epoch_start)
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<i64>,
    #[serde(default)]
    pub limit: Option<u32>,
}

/// Filter for extrinsic properties (used in storage snapshots)
#[derive(Debug, Deserialize, Default)]
pub struct ExtrinsicFilter {
    /// Filter by pallet index or name
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    /// Filter by method index or name (requires pallet if using name)
    #[serde(default)]
    pub method: Option<StringOrNumber>,
    /// Filter by account ID (hex or SS58)
    #[serde(default)]
    pub account_id: Option<String>,
}

/// Filter for event properties (used in storage snapshots)
#[derive(Debug, Deserialize, Default)]
pub struct EventFilter {
    /// Filter by pallet index or name
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    /// Filter by variant index or name (requires pallet if using name)
    #[serde(default)]
    pub variant: Option<StringOrNumber>,
}

/// Parameters for getting spec version
#[derive(Debug, Deserialize)]
pub struct GetSpecVersionParams {
    #[serde(default)]
    pub spec_version: Option<i32>,
    #[serde(default)]
    pub block_number: Option<i64>,
}

/// Sampling unit for storage snapshots - groups snapshots by time period
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SampleUnit {
    /// Sample one snapshot per epoch (~3 hours on Acurast)
    PerEpoch,
    /// Sample one snapshot per day (~8 epochs)
    Day,
    /// Sample one snapshot per week (~56 epochs)
    Week,
    /// Sample one snapshot per month (~240 epochs, ~30 days)
    Month,
}

impl SampleUnit {
    /// Convert to approximate number of epochs per sample
    pub fn epochs_per_sample(&self) -> i64 {
        match self {
            SampleUnit::PerEpoch => 1,
            SampleUnit::Day => 8,     // ~24h / 3h per epoch
            SampleUnit::Week => 56,   // 7 * 8
            SampleUnit::Month => 240, // ~30 * 8
        }
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct GetStorageSnapshotsParams {
    /// Filter by block range (inclusive)
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    /// Filter by time range (ISO 8601 format)
    #[serde(default)]
    pub time_from: Option<String>,
    #[serde(default)]
    pub time_to: Option<String>,
    /// Filter by pallet index or name
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    /// Filter by storage location name
    #[serde(default)]
    pub storage_location: Option<String>,
    /// Filter by storage keys (JSON that must be contained in storage_keys)
    #[serde(default)]
    pub storage_keys: Option<serde_json::Value>,
    /// Filter by data (JSON that must be contained in data)
    #[serde(default)]
    pub data: Option<serde_json::Value>,
    /// Filter by config rule name
    #[serde(default)]
    pub config_rule: Option<String>,
    /// Filter by extrinsic properties (pallet, method, account_id)
    #[serde(default)]
    pub extrinsic: Option<ExtrinsicFilter>,
    /// Filter by event properties (pallet, variant)
    #[serde(default)]
    pub event: Option<EventFilter>,
    /// Sort order: asc or desc (default: desc)
    #[serde(default)]
    pub sort_order: Option<String>,
    /// Cursor for pagination (id of last item)
    #[serde(default)]
    pub cursor: Option<i64>,
    /// Number of items to return (default 10, max 1000)
    #[serde(default)]
    pub limit: Option<u32>,
    /// Exclude snapshots that have a subsequent snapshot with null data (default: false)
    #[serde(default)]
    pub exclude_deleted: bool,
    /// Sample snapshots by time unit. Returns first snapshot per time period.
    #[serde(default)]
    pub sample: Option<SampleUnit>,
    /// Fill missing time units with the last known value before that period.
    /// Only applies when sample is set. Default: false
    #[serde(default)]
    pub fill: bool,
    /// Include epoch information in the response.
    /// When true or when sample is set, joins with epochs table.
    #[serde(default)]
    pub include_epochs: bool,
}

// Response structs

#[derive(Debug, Serialize, Clone)]
pub struct ExtrinsicWithMetadata {
    #[serde(flatten)]
    pub extrinsic: ExtrinsicRowWithEvents,
    pub pallet_name: Option<String>,
    pub method_name: Option<String>,
}

#[derive(Debug, Serialize, Clone)]
pub struct ExtrinsicMetadata {
    pub pallets: std::collections::BTreeMap<
        String,
        (
            std::collections::BTreeMap<String, crate::metadata::CallIndex>,
            std::collections::BTreeMap<String, crate::metadata::CallIndex>,
        ),
    >,
}

#[derive(Debug, Serialize, Clone)]
pub struct EventMetadata {
    pub pallets: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::metadata::CallIndex>,
    >,
}

#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct ExtrinsicAddressRow {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub batch_index: Option<i32>,
    pub data_path: String,
    pub resolved_data_path: String,
    pub account_id: String,
    pub pallet: i32,
    pub method: i32,
    pub block_time: DateTime<Utc>,
}

#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct JobRow {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub event_index: i32,
    pub data_path: String,
    pub chain: String,
    pub address: String,
    pub seq_id: i32,
    pub block_time: DateTime<Utc>,
}

/// Nested epoch information for storage snapshot responses
#[derive(Debug, Serialize, Clone)]
pub struct EpochInfo {
    pub epoch: i64,
    pub epoch_start: i64,
    pub epoch_end: Option<i64>,
    pub epoch_start_time: DateTime<Utc>,
}

/// Internal struct for reading from database (flat structure for sqlx)
#[derive(Debug, Clone, sqlx::FromRow)]
struct StorageSnapshotDbRow {
    pub id: i64,
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub event_index: Option<i32>,
    pub block_time: DateTime<Utc>,
    pub pallet: i32,
    pub storage_location: String,
    pub storage_keys: serde_json::Value,
    pub data: serde_json::Value,
    pub config_rule: String,
    // Optional epoch fields (populated when joining with epochs)
    pub epoch: Option<i64>,
    pub epoch_start: Option<i64>,
    pub epoch_end: Option<i64>,
    pub epoch_start_time: Option<DateTime<Utc>>,
    // For sampling: the bucket this snapshot belongs to
    pub epoch_bucket: Option<i64>,
}

/// API response struct with nested epoch info
#[derive(Debug, Serialize, Clone)]
pub struct StorageSnapshotRow {
    pub id: i64,
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub event_index: Option<i32>,
    pub block_time: DateTime<Utc>,
    pub pallet: i32,
    pub storage_location: String,
    pub storage_keys: serde_json::Value,
    pub data: serde_json::Value,
    pub config_rule: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<EpochInfo>,
}

impl From<StorageSnapshotDbRow> for StorageSnapshotRow {
    fn from(row: StorageSnapshotDbRow) -> Self {
        let epoch = row.epoch.map(|e| EpochInfo {
            epoch: e,
            epoch_start: row.epoch_start.unwrap_or(0),
            epoch_end: row.epoch_end,
            epoch_start_time: row.epoch_start_time.unwrap_or(DateTime::UNIX_EPOCH),
        });
        Self {
            id: row.id,
            block_number: row.block_number,
            extrinsic_index: row.extrinsic_index,
            event_index: row.event_index,
            block_time: row.block_time,
            pallet: row.pallet,
            storage_location: row.storage_location,
            storage_keys: row.storage_keys,
            data: row.data,
            config_rule: row.config_rule,
            epoch,
        }
    }
}

/// Event information for batch explosion
#[derive(Debug, Clone, Deserialize, Serialize)]
struct EventInfo {
    index: i32,
    pallet: i32,
    #[serde(rename = "method")]
    variant: i32,
    data: Option<serde_json::Value>,
}

/// Parse job_id filter that supports multiple address formats with optional sequence ID.
/// Accepts:
/// - SS58 address: "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY"
/// - Hex address: "0xd43593c715fdd31c61141abd04a99fd6822c8558854ccde39a0684e1abc76e1" or without 0x prefix
/// - With sequence ID: "5GrwvaEF...#123" or "0xd43593...#456"
///
/// Returns (normalized_hex_address, optional_seq_id)
fn parse_job_id_filter(input: &str) -> (String, Option<i32>) {
    // Check if there's a #<seq_id> suffix
    if let Some(hash_pos) = input.rfind('#') {
        let address_part = &input[..hash_pos];
        let seq_id_part = &input[hash_pos + 1..];

        // Try to parse the seq_id
        if let Ok(seq_id) = seq_id_part.parse::<i32>() {
            return (normalize_address_with_prefix(address_part), Some(seq_id));
        }
    }

    // No seq_id or invalid seq_id format, return just the normalized address
    (normalize_address_with_prefix(input), None)
}

/// Identify which events are "framing" events that should be excluded from batch items
fn identify_framing_events(
    events: &[EventInfo],
    events_reverse_map: &crate::metadata::ReverseMap,
) -> std::collections::HashSet<usize> {
    use std::collections::HashSet;
    let mut framing_indices = HashSet::new();

    for (idx, event) in events.iter().enumerate() {
        let pallet = event.pallet as u8;
        let variant = event.variant as u8;

        if let Some((pallet_name, variant_name)) = events_reverse_map.get(&(pallet, variant)) {
            let is_framing = matches!(
                (pallet_name.as_str(), variant_name.as_str()),
                ("Utility", "ItemCompleted")
                    | ("Utility", "ItemFailed")
                    | ("Utility", "BatchCompleted")
                    | ("Utility", "BatchInterrupted")
                    | ("System", "ExtrinsicSuccess")
                    | ("System", "ExtrinsicFailed")
                    | ("Balances", "Withdraw")
                    | ("Balances", "Deposit")
                    | ("TransactionPayment", "TransactionFeePaid")
            );

            if is_framing {
                framing_indices.insert(idx);
            }
        }
    }

    framing_indices
}

/// Map non-framing events to batch items using sequential distribution
fn map_events_to_batch_items(
    events: &[EventInfo],
    framing_indices: &std::collections::HashSet<usize>,
    num_batch_items: usize,
) -> std::collections::HashMap<usize, Vec<EventInfo>> {
    use std::collections::HashMap;
    let mut result: HashMap<usize, Vec<EventInfo>> = HashMap::new();

    // Filter out framing events
    let non_framing: Vec<(usize, &EventInfo)> = events
        .iter()
        .enumerate()
        .filter(|(idx, _)| !framing_indices.contains(idx))
        .collect();

    if non_framing.is_empty() {
        return result; // No events to map
    }

    // Simple heuristic: distribute events evenly across batch items
    // More sophisticated mapping could use ItemCompleted/ItemFailed boundaries
    let events_per_item = (non_framing.len() + num_batch_items - 1) / num_batch_items;
    let events_per_item = events_per_item.max(1); // At least 1 to avoid division by zero

    for (i, (_, event)) in non_framing.iter().enumerate() {
        let batch_idx = i / events_per_item;
        let batch_idx = batch_idx.min(num_batch_items - 1); // Clamp to valid range

        result
            .entry(batch_idx)
            .or_insert_with(Vec::new)
            .push((*event).clone());
    }

    result
}

// Implementation

impl AppState {
    pub async fn get_block(&self, hash: String) -> RpcResult<Option<Block>> {
        let result = with_timeout(
            self.query_timeout,
            query_as!(
                Block,
                r#"SELECT block_number, '0x' || "hash" as "hash!", block_time FROM blocks WHERE "hash" = ($1) LIMIT 1"#,
                strip_hex_prefix(&hash)
            )
            .fetch_optional(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        Ok(result)
    }

    pub async fn get_blocks(&self, params: GetBlocksParams) -> RpcResult<Page<Block>> {
        // Validate parameters
        let time_from: Option<DateTime<Utc>> = if let Some(t) = &params.time_from {
            Some(
                t.parse()
                    .map_err(|_| RpcError::invalid_params(format!("Invalid time_from: {}", t)))?,
            )
        } else {
            None
        };

        let time_to: Option<DateTime<Utc>> = if let Some(t) = &params.time_to {
            Some(
                t.parse()
                    .map_err(|_| RpcError::invalid_params(format!("Invalid time_to: {}", t)))?,
            )
        } else {
            None
        };

        let mut query_builder = QueryBuilder::<Postgres>::new(
            r#"SELECT block_number, '0x' || "hash" as "hash", block_time FROM blocks"#,
        );

        // Determine sort order first for cursor comparison
        let sort_by = "block_number";
        let sort_order = params.sort_order.as_deref().unwrap_or("asc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || params.time_from.is_some()
            || params.time_to.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");
            if let Some(cursor) = params.cursor {
                conditions
                    .push(format!("block_number {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push("block_number >= ")
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(time_from) = time_from {
                conditions
                    .push("block_time >= ")
                    .push_bind_unseparated(time_from);
            }
            if let Some(time_to) = time_to {
                conditions
                    .push("block_time <= ")
                    .push_bind_unseparated(time_to);
            }
        }

        query_builder.push(format!(
            " ORDER BY {} {}, block_number {}",
            sort_by, sort_order, sort_order
        ));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<Block>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop(); // Remove the extra item
        }

        Ok(Page::<Block> {
            cursor: if has_more {
                items.last().map(|l| serde_json::json!(l.block_number))
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    pub async fn get_blocks_count(&self, params: GetBlocksCountParams) -> RpcResult<i64> {
        let has_filters = params.block_from.is_some() || params.block_to.is_some();

        // Use approximate count from pg_class when no filters (instant, avoids full table scan)
        if !has_filters {
            let result: i64 = with_timeout(
                self.query_timeout,
                sqlx::query_scalar(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'blocks'",
                )
                .fetch_one(&self.db_pool),
            )
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            return Ok(result);
        }

        // Build cache key from filter parameters
        let cache_key = format!(
            "blk_count:{}:{}",
            params.block_from.map_or("_".to_string(), |v| v.to_string()),
            params.block_to.map_or("_".to_string(), |v| v.to_string()),
        );

        // Check cache first
        if let Some(cached) = self.count_cache.get(&cache_key).await {
            trace!("Cache hit for blocks count: {}", cache_key);
            return Ok(cached);
        }

        let mut query_builder = QueryBuilder::<Postgres>::new("SELECT count(*) FROM blocks");

        query_builder.push(" WHERE ");
        let mut conditions = query_builder.separated(" AND ");
        if let Some(block_from) = params.block_from {
            conditions
                .push("block_number >= ")
                .push_bind_unseparated(block_from as i64);
        }
        if let Some(block_to) = params.block_to {
            conditions
                .push("block_number <= ")
                .push_bind_unseparated(block_to as i64);
        }

        let query = query_builder.build_query_scalar::<i64>();

        let result = with_timeout(self.query_timeout, query.fetch_one(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Store in cache (TTL handled by cache config)
        self.count_cache.insert(cache_key, result).await;

        Ok(result)
    }

    pub async fn get_epochs(&self, params: GetEpochsParams) -> RpcResult<Page<EpochRow>> {
        // Determine sort order before cursor so we know comparison direction
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        // Use LEAD() to compute epoch_end from next epoch's start
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "WITH epochs_with_end AS (
                SELECT epoch, epoch_start,
                       LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end,
                       epoch_start_time, phase
                FROM epochs
            )
            SELECT epoch, epoch_start, epoch_end, epoch_start_time, phase FROM epochs_with_end",
        );

        if params.epoch_from.is_some()
            || params.epoch_to.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || params.cursor.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");

            if let Some(epoch_from) = params.epoch_from {
                conditions
                    .push("epoch >= ")
                    .push_bind_unseparated(epoch_from as i64);
            }

            if let Some(epoch_to) = params.epoch_to {
                conditions
                    .push("epoch <= ")
                    .push_bind_unseparated(epoch_to as i64);
            }

            if let Some(block_from) = params.block_from {
                conditions
                    .push("epoch_start >= ")
                    .push_bind_unseparated(block_from as i64);
            }

            if let Some(block_to) = params.block_to {
                conditions
                    .push("epoch_start <= ")
                    .push_bind_unseparated(block_to as i64);
            }

            if let Some(cursor) = params.cursor {
                conditions
                    .push(format!("epoch {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
        }

        query_builder.push(format!(" ORDER BY epoch {}", sort_order));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<EpochRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop();
        }

        Ok(Page::<EpochRow> {
            cursor: if has_more {
                items.last().map(|l| serde_json::json!(l.epoch))
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    /// Get the count of distinct processors that heartbeated at least once per epoch.
    pub async fn get_processors_count_by_epoch(
        &self,
        params: GetProcessorsCountByEpochParams,
    ) -> RpcResult<Page<ProcessorsCountByEpochRow>> {
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(16) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        // Build query to get epochs with their block ranges and count processors
        // Using a CTE to get epoch ranges, then count distinct processors per epoch
        let mut query_builder = QueryBuilder::<Postgres>::new(
            r#"WITH epoch_ranges AS (
                SELECT epoch, epoch_start,
                       COALESCE(LEAD(epoch_start) OVER (ORDER BY epoch), epoch_start + 900) as epoch_end
                FROM epochs
            )
            SELECT er.epoch,
                   COUNT(DISTINCT e.account_id) as count
            FROM epoch_ranges er
            LEFT JOIN events ev ON ev.block_number >= er.epoch_start
                                AND ev.block_number < er.epoch_end
                                AND ev.pallet = 41 AND ev.variant = 6
            LEFT JOIN extrinsics e ON e.block_number = ev.block_number
                                   AND e.index = ev.extrinsic_index"#,
        );

        // Add WHERE conditions
        let has_conditions =
            params.epoch_from.is_some() || params.epoch_to.is_some() || params.cursor.is_some();

        if has_conditions {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");

            if let Some(epoch_from) = params.epoch_from {
                conditions
                    .push("er.epoch >= ")
                    .push_bind_unseparated(epoch_from as i64);
            }
            if let Some(epoch_to) = params.epoch_to {
                conditions
                    .push("er.epoch <= ")
                    .push_bind_unseparated(epoch_to as i64);
            }
            if let Some(cursor) = params.cursor {
                conditions
                    .push(format!("er.epoch {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
        }

        query_builder.push(" GROUP BY er.epoch, er.epoch_start, er.epoch_end");
        query_builder.push(format!(" ORDER BY er.epoch {}", sort_order));
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<ProcessorsCountByEpochRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop();
        }

        Ok(Page::<ProcessorsCountByEpochRow> {
            cursor: if has_more {
                items.last().map(|l| serde_json::json!(l.epoch))
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    pub async fn get_extrinsic(
        &self,
        block_number: u32,
        index: i32,
        include_events: bool,
    ) -> RpcResult<Option<ExtrinsicWithMetadata>> {
        let extrinsic: Option<ExtrinsicRowWithEvents> = if include_events {
            with_timeout(
                self.query_timeout,
                sqlx::query_as(
                    r#"SELECT e.block_number, e.index, e.pallet, e.method, e.data, '0x' || e.tx_hash as tx_hash, '0x' || e.account_id as account_id, e.block_time, e.phase,
                       (
                            SELECT jsonb_agg(jsonb_build_object(
                                    'index', ev.index,
                                    'method', ev.variant,
                                    'pallet', ev.pallet,
                                    'data', ev.data
                                ) ORDER BY ev.index)::jsonb
                            FROM events ev
                            WHERE ev.block_number = e.block_number AND ev.extrinsic_index = e.index
                        ) AS events
                    FROM extrinsics e WHERE e.block_number = $1 AND e.index = $2 LIMIT 1"#,
                )
                .bind(block_number as i64)
                .bind(index)
                .fetch_optional(&self.db_pool),
            )
            .await
        } else {
            with_timeout(
                self.query_timeout,
                sqlx::query_as(
                    r#"SELECT block_number, index, pallet, method, data, '0x' || tx_hash as tx_hash, '0x' || account_id as account_id, block_time, phase, NULL::jsonb AS events FROM extrinsics WHERE block_number = $1 AND index = $2 LIMIT 1"#,
                )
                .bind(block_number as i64)
                .bind(index)
                .fetch_optional(&self.db_pool),
            )
            .await
        }
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        self.extrinsic_with_metadata(extrinsic).await
    }

    pub async fn get_extrinsic_by_hash(
        &self,
        tx_hash: String,
        include_events: bool,
    ) -> RpcResult<Option<ExtrinsicWithMetadata>> {
        let extrinsic: Option<ExtrinsicRowWithEvents> = if include_events {
            with_timeout(
                self.query_timeout,
                sqlx::query_as(
                    r#"SELECT e.block_number, e.index, e.pallet, e.method, e.data, '0x' || e.tx_hash as tx_hash, '0x' || e.account_id as account_id, e.block_time, e.phase,
                       (
                            SELECT jsonb_agg(jsonb_build_object(
                                    'index', ev.index,
                                    'method', ev.variant,
                                    'pallet', ev.pallet,
                                    'data', ev.data
                                ) ORDER BY ev.index)::jsonb
                            FROM events ev
                            WHERE ev.block_number = e.block_number AND ev.extrinsic_index = e.index
                        ) AS events
                    FROM extrinsics e WHERE e.tx_hash = $1 LIMIT 1"#,
                )
                .bind(strip_hex_prefix(&tx_hash))
                .fetch_optional(&self.db_pool),
            )
            .await
        } else {
            with_timeout(
                self.query_timeout,
                sqlx::query_as(
                    r#"SELECT block_number, index, pallet, method, data, '0x' || tx_hash as tx_hash, '0x' || account_id as account_id, block_time, phase, NULL::jsonb AS events FROM extrinsics WHERE tx_hash = $1 LIMIT 1"#,
                )
                .bind(strip_hex_prefix(&tx_hash))
                .fetch_optional(&self.db_pool),
            )
            .await
        }
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        self.extrinsic_with_metadata(extrinsic).await
    }

    async fn extrinsic_with_metadata(
        &self,
        extrinsic: Option<ExtrinsicRowWithEvents>,
    ) -> RpcResult<Option<ExtrinsicWithMetadata>> {
        if let Some(ext) = extrinsic {
            // Get metadata for pallet and method names
            let reverse_map = crate::metadata::get_extrinsics_reverse_map(&self.client).await;
            let (pallet_name, method_name) = reverse_map
                .get(&(ext.pallet as u8, ext.method as u8))
                .map(|(p, m)| (Some(p.clone()), Some(m.clone())))
                .unwrap_or((None, None));

            Ok(Some(ExtrinsicWithMetadata {
                extrinsic: ext,
                pallet_name,
                method_name,
            }))
        } else {
            Ok(None)
        }
    }

    pub async fn get_extrinsics(
        &self,
        params: GetExtrinsicsParams,
    ) -> RpcResult<Page<ExtrinsicRowWithEvents>> {
        // Resolve pallet/method names to numbers if needed
        let (pallet, method) = resolve_extrinsic_pallet_method(
            &self.client,
            params.pallet.as_ref(),
            params.method.as_ref(),
        )
        .await?;

        // Resolve event filter if present
        let (evt_pallet, evt_variant) = if let Some(ref evt_filter) = params.event {
            resolve_event_pallet_variant(
                &self.client,
                evt_filter.pallet.as_ref(),
                evt_filter.variant.as_ref(),
            )
            .await?
        } else {
            (None, None)
        };

        let has_event_filter = evt_pallet.is_some() || evt_variant.is_some();
        let include_events = params.events.unwrap_or(false);

        let mut query_builder = if include_events {
            QueryBuilder::<Postgres>::new(
                "SELECT e.block_number, e.index, e.pallet, e.method, e.data, '0x' || e.tx_hash as tx_hash, '0x' || e.account_id as account_id, e.block_time, e.phase,
                   (
                        SELECT jsonb_agg(jsonb_build_object(
                                'index', ev.index,
                                'method', ev.variant,
                                'pallet', ev.pallet,
                                'data', ev.data
                            ) ORDER BY ev.index)::jsonb
                        FROM events ev
                        WHERE ev.block_number = e.block_number AND ev.extrinsic_index = e.index
                    ) AS events
                FROM extrinsics e"
            )
        } else {
            QueryBuilder::<Postgres>::new(
                "SELECT e.block_number, e.index, e.pallet, e.method, e.data, '0x' || e.tx_hash as tx_hash, '0x' || e.account_id as account_id, e.block_time, e.phase,
                   NULL::jsonb AS events
                FROM extrinsics e"
            )
        };

        // Determine sort order first for cursor comparison
        let sort_by = "block_number";
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || pallet.is_some()
            || method.is_some()
            || params.account_id.is_some()
            || params.data.is_some()
            || has_event_filter
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");
            if let Some(cursor) = &params.cursor {
                conditions.push(format!("(block_number, index) {} (", cursor_op));
                conditions.push_bind_unseparated(cursor.block_number);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.index);
                conditions.push_unseparated(")");
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push("block_number >= ")
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(pallet) = pallet {
                conditions
                    .push("pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(method) = method {
                conditions
                    .push("method = ")
                    .push_bind_unseparated(method as i32);
            }
            if let Some(account_id) = &params.account_id {
                conditions
                    .push("account_id = ")
                    .push_bind_unseparated(normalize_address(account_id));
            }
            if let Some(data) = &params.data {
                // Use @> containment operator for JSONB
                conditions.push("data @> ").push_bind_unseparated(data);
            }
            // Add event filter using WHERE EXISTS subquery
            if has_event_filter {
                conditions.push("EXISTS (SELECT 1 FROM events ev_filter WHERE ev_filter.block_number = e.block_number AND ev_filter.extrinsic_index = e.index");
                if let Some(pallet) = evt_pallet {
                    conditions.push_unseparated(" AND ev_filter.pallet = ");
                    conditions.push_bind_unseparated(pallet as i32);
                }
                if let Some(variant) = evt_variant {
                    conditions.push_unseparated(" AND ev_filter.variant = ");
                    conditions.push_bind_unseparated(variant as i32);
                }
                conditions.push_unseparated(")");
            }
        }

        query_builder.push(format!(
            " ORDER BY {} {}, index {}",
            sort_by, sort_order, sort_order
        ));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<ExtrinsicRowWithEvents>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop(); // Remove the extra item
        }

        // Track last extrinsic cursor before explosion (for stable pagination)
        let last_extrinsic_cursor = if has_more {
            items.last().map(|l| ExtrinsicCursor {
                block_number: l.block_number,
                index: l.index,
            })
        } else {
            None
        };

        // Explode batches if requested (AFTER pagination, BEFORE returning)
        if params.explode_batch.unwrap_or(false) {
            items = self.explode_batches(items).await?;
        }

        Ok(Page::<ExtrinsicRowWithEvents> {
            cursor: last_extrinsic_cursor.map(|c| serde_json::to_value(c).unwrap()),
            items,
            unfiltered_count: None,
        })
    }

    /// Explode batch extrinsics into individual call items with mapped events
    async fn explode_batches(
        &self,
        extrinsics: Vec<ExtrinsicRowWithEvents>,
    ) -> RpcResult<Vec<ExtrinsicRowWithEvents>> {
        use crate::data_extraction::extract_calls;

        let pallet_method_map = crate::config::pallet_method_map();
        let events_reverse_map = crate::metadata::get_events_reverse_map(&self.client).await;
        let mut result = Vec::new();

        for ext in extrinsics {
            // Check if this is a batch extrinsic
            const UTILITY_PALLET: u32 = 8;
            let is_batch = ext.pallet as u32 == UTILITY_PALLET
                && (ext.method == 0 || ext.method == 2 || ext.method == 4);

            if !is_batch {
                result.push(ext);
                continue;
            }

            // Extract batch calls
            let data = match ext.data.as_ref() {
                Some(d) => d,
                None => {
                    // Batch without data, push as-is
                    result.push(ext);
                    continue;
                }
            };

            let (calls, _is_batch) = extract_calls(
                ext.pallet as u32,
                ext.method as u32,
                data,
                pallet_method_map,
            );

            if calls.is_empty() || calls.len() == 1 {
                // Empty batch or single call, not worth exploding
                result.push(ext);
                continue;
            }

            // Parse events array
            let all_events: Vec<EventInfo> = if let Some(ref events_json) = ext.events {
                serde_json::from_value(events_json.clone()).unwrap_or_default()
            } else {
                vec![]
            };

            // Identify framing events
            let framing_event_indices = identify_framing_events(&all_events, &events_reverse_map);

            // Map events to batch items
            let event_groups =
                map_events_to_batch_items(&all_events, &framing_event_indices, calls.len());

            // Create exploded items
            for (batch_idx, call) in calls.iter().enumerate() {
                let item_events = event_groups.get(&batch_idx).cloned().unwrap_or_default();

                let mut exploded = ext.clone();
                exploded.pallet = call.pallet as i32;
                exploded.method = call.method as i32;
                exploded.data = Some(call.data.clone());
                exploded.batch_index = Some(batch_idx as i32);
                exploded.events = if !item_events.is_empty() {
                    Some(serde_json::to_value(&item_events).unwrap())
                } else {
                    Some(serde_json::json!([]))
                };

                result.push(exploded);
            }
        }

        Ok(result)
    }

    pub async fn get_extrinsics_count(&self, params: GetExtrinsicsCountParams) -> RpcResult<i64> {
        // Resolve pallet/method names to numbers if needed
        let (pallet, method) = resolve_extrinsic_pallet_method(
            &self.client,
            params.pallet.as_ref(),
            params.method.as_ref(),
        )
        .await?;

        let has_filters = params.block_from.is_some()
            || params.block_to.is_some()
            || pallet.is_some()
            || method.is_some()
            || params.account_id.is_some();

        // Use approximate count from pg_class when no filters (instant, avoids full table scan)
        if !has_filters {
            let result: i64 = with_timeout(
                self.query_timeout,
                sqlx::query_scalar(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'extrinsics'",
                )
                .fetch_one(&self.db_pool),
            )
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            return Ok(result);
        }

        // Build cache key from filter parameters
        let cache_key = format!(
            "ext_count:{}:{}:{}:{}:{}",
            params.block_from.map_or("_".to_string(), |v| v.to_string()),
            params.block_to.map_or("_".to_string(), |v| v.to_string()),
            pallet.map_or("_".to_string(), |v| v.to_string()),
            method.map_or("_".to_string(), |v| v.to_string()),
            params.account_id.as_deref().unwrap_or("_")
        );

        // Check cache first
        if let Some(cached) = self.count_cache.get(&cache_key).await {
            trace!("Cache hit for extrinsics count: {}", cache_key);
            return Ok(cached);
        }

        let mut query_builder = QueryBuilder::<Postgres>::new("SELECT count(*) FROM extrinsics");

        query_builder.push(" WHERE ");
        let mut conditions = query_builder.separated(" AND ");
        if let Some(block_from) = params.block_from {
            conditions
                .push("block_number >= ")
                .push_bind_unseparated(block_from as i64);
        }
        if let Some(block_to) = params.block_to {
            conditions
                .push("block_number <= ")
                .push_bind_unseparated(block_to as i64);
        }
        if let Some(pallet) = pallet {
            conditions
                .push("pallet = ")
                .push_bind_unseparated(pallet as i32);
        }
        if let Some(method) = method {
            conditions
                .push("method = ")
                .push_bind_unseparated(method as i32);
        }
        if let Some(account_id) = &params.account_id {
            conditions
                .push("account_id = ")
                .push_bind_unseparated(normalize_address(account_id));
        }

        let query = query_builder.build_query_scalar::<i64>();

        let result = with_timeout(self.query_timeout, query.fetch_one(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Store in cache (TTL handled by cache config)
        self.count_cache.insert(cache_key, result).await;

        Ok(result)
    }

    pub async fn get_extrinsic_metadata(&self) -> RpcResult<ExtrinsicMetadata> {
        let pallets = crate::metadata::get_extrinsics_map(&self.client).await;
        Ok(ExtrinsicMetadata { pallets })
    }

    pub async fn get_event_metadata(&self) -> RpcResult<EventMetadata> {
        let pallets = crate::metadata::get_events_map(&self.client).await;
        Ok(EventMetadata { pallets })
    }

    pub async fn get_spec_version(
        &self,
        params: GetSpecVersionParams,
    ) -> RpcResult<serde_json::Value> {
        // Validate that at least one parameter is provided
        if params.spec_version.is_none() && params.block_number.is_none() {
            return Err(RpcError::invalid_params(
                "Either spec_version or block_number must be provided".to_string(),
            ));
        }

        // Fetch spec version info from database
        let (spec_version, block_number, block_hash) =
            if let Some(spec_version) = params.spec_version {
                // Query by exact spec_version
                let row = sqlx::query!(
                    r#"
                SELECT sv.spec_version, sv.block_number, b.hash as block_hash
                FROM spec_versions sv
                JOIN blocks b ON sv.block_number = b.block_number
                WHERE sv.spec_version = $1
                "#,
                    spec_version
                )
                .fetch_optional(&self.db_pool)
                .await
                .map_err(|e| RpcError::database(format!("Database error: {}", e)))?
                .ok_or_else(|| {
                    RpcError::invalid_params(format!("Spec version {} not found", spec_version))
                })?;
                (row.spec_version, row.block_number, row.block_hash)
            } else {
                // Query by block_number - find the closest spec_version <= block_number
                let block_number = params.block_number.unwrap();
                let row = sqlx::query!(
                    r#"
                SELECT sv.spec_version, sv.block_number, b.hash as block_hash
                FROM spec_versions sv
                JOIN blocks b ON sv.block_number = b.block_number
                WHERE sv.block_number <= $1
                ORDER BY sv.block_number DESC
                LIMIT 1
                "#,
                    block_number
                )
                .fetch_optional(&self.db_pool)
                .await
                .map_err(|e| RpcError::database(format!("Database error: {}", e)))?
                .ok_or_else(|| {
                    RpcError::invalid_params(format!(
                        "No spec version found at or below block {}",
                        block_number
                    ))
                })?;
                (row.spec_version, row.block_number, row.block_hash)
            };

        // Parse block hash
        let block_hash_bytes = hex::decode(&block_hash)
            .map_err(|e| RpcError::internal_error(format!("Failed to decode block hash: {}", e)))?;
        let block_hash_h256 = H256::from_slice(&block_hash_bytes);

        // Call the Metadata_metadata_at_version runtime API directly to get raw SCALE bytes
        let version: u32 = 15;
        let version_encoded = ScaleEncode::encode(&version);

        let raw_result = self
            .client
            .backend()
            .call(
                "Metadata_metadata_at_version",
                Some(&version_encoded),
                block_hash_h256,
            )
            .await
            .map_err(|e| RpcError::internal_error(format!("Failed to fetch metadata: {}", e)))?;

        // The result is SCALE-encoded Option<OpaqueMetadata>
        // Decode it to extract the metadata bytes
        match <Option<Vec<u8>> as ScaleDecode>::decode(&mut &raw_result[..]) {
            Ok(Some(metadata_bytes)) => {
                let metadata_hex = format!("0x{}", hex::encode(&metadata_bytes));

                // Return spec version info with metadata as hex string
                Ok(serde_json::json!({
                    "spec_version": spec_version,
                    "block_number": block_number,
                    "block_hash": block_hash,
                    "metadata": metadata_hex
                }))
            }
            Ok(None) => Err(RpcError::internal_error(
                "Metadata v15 not available at this block".to_string(),
            )),
            Err(e) => Err(RpcError::internal_error(format!(
                "Failed to decode metadata result: {}",
                e
            ))),
        }
    }

    pub async fn get_extrinsic_addresses(
        &self,
        params: GetExtrinsicAddressesParams,
    ) -> RpcResult<Page<ExtrinsicAddressRow>> {
        // Resolve pallet/method names to numbers if needed
        let (pallet, method) = resolve_extrinsic_pallet_method(
            &self.client,
            params.pallet.as_ref(),
            params.method.as_ref(),
        )
        .await?;

        let mut query_builder = QueryBuilder::<Postgres>::new("SELECT block_number, extrinsic_index, batch_index, data_path, resolved_data_path, account_id, pallet, method, block_number, block_time FROM extrinsic_address");

        // Determine sort order first for cursor comparison
        let sort_by = "block_number";
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || params.account_id.is_some()
            || pallet.is_some()
            || method.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");
            if let Some(cursor) = &params.cursor {
                conditions.push(format!("(block_number, extrinsic_index) {} (", cursor_op));
                conditions.push_bind_unseparated(cursor.block_number);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.index);
                conditions.push_unseparated(")");
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push("block_number >= ")
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(account_id) = &params.account_id {
                conditions
                    .push("account_id = ")
                    .push_bind_unseparated(normalize_address(account_id));
            }
            if let Some(pallet) = pallet {
                conditions
                    .push("pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(method) = method {
                conditions
                    .push("method = ")
                    .push_bind_unseparated(method as i32);
            }
        }

        query_builder.push(format!(
            " ORDER BY {} {}, extrinsic_index {}",
            sort_by, sort_order, sort_order
        ));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<ExtrinsicAddressRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop(); // Remove the extra item
        }

        Ok(Page::<ExtrinsicAddressRow> {
            cursor: if has_more {
                items.last().and_then(|l| {
                    serde_json::to_value(ExtrinsicCursor {
                        block_number: l.block_number,
                        index: l.extrinsic_index,
                    })
                    .ok()
                })
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    pub async fn get_event(&self, params: GetEventParams) -> RpcResult<Option<EventRow>> {
        let result: Option<EventRow> = with_timeout(
            self.query_timeout,
            sqlx::query_as(
                r#"SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, error, block_number, block_time FROM events WHERE block_number = $1 AND extrinsic_index = $2 AND index = $3 LIMIT 1"#,
            )
            .bind(params.block_number)
            .bind(params.extrinsic_index)
            .bind(params.index)
            .fetch_optional(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        Ok(result)
    }

    pub async fn get_events(&self, params: GetEventsParams) -> RpcResult<Page<EventRow>> {
        // Resolve pallet/variant names to numbers if needed
        let (pallet, variant) = resolve_event_pallet_variant(
            &self.client,
            params.pallet.as_ref(),
            params.variant.as_ref(),
        )
        .await?;

        // Parse job filter if provided
        let job_filter = params.job.as_ref().map(|addr| parse_job_id_filter(addr));

        // Determine if we need JOINs
        let needs_extrinsic_join = params.account_id.is_some();
        let needs_job_join = job_filter.is_some();
        let needs_join = needs_extrinsic_join || needs_job_join;

        // Build query with appropriate JOINs
        // When filtering by job, start from jobs table (smaller) and join to events
        let mut query_builder = if needs_job_join {
            QueryBuilder::<Postgres>::new(
                "SELECT e.block_number, e.extrinsic_index, e.index, e.pallet, e.variant, e.data, e.phase, e.error, e.block_time \
                 FROM jobs j \
                 INNER JOIN events e ON j.block_number = e.block_number AND j.extrinsic_index = e.extrinsic_index AND j.event_index = e.index"
            )
        } else if needs_extrinsic_join {
            QueryBuilder::<Postgres>::new(
                "SELECT e.block_number, e.extrinsic_index, e.index, e.pallet, e.variant, e.data, e.phase, e.error, e.block_time \
                 FROM events e \
                 INNER JOIN extrinsics ext ON e.block_number = ext.block_number AND e.extrinsic_index = ext.index"
            )
        } else {
            QueryBuilder::<Postgres>::new("SELECT * FROM events")
        };

        // Determine sort order first for cursor comparison
        let sort_by = if needs_join {
            "e.block_number"
        } else {
            "block_number"
        };
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || pallet.is_some()
            || variant.is_some()
            || params.account_id.is_some()
            || params.data.is_some()
            || job_filter.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");

            // Determine column prefix based on whether we're using a JOIN
            let col_prefix = if needs_join { "e." } else { "" };

            if let Some(cursor) = &params.cursor {
                conditions.push(format!(
                    "({}block_number, {}extrinsic_index, {}index) {} (",
                    col_prefix, col_prefix, col_prefix, cursor_op
                ));
                conditions.push_bind_unseparated(cursor.block_number);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.extrinsic_index);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.index);
                conditions.push_unseparated(")");
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push(format!("{}block_number >= ", col_prefix))
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push(format!("{}block_number <= ", col_prefix))
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(pallet) = pallet {
                conditions
                    .push(format!("{}pallet = ", col_prefix))
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(variant) = variant {
                conditions
                    .push(format!("{}variant = ", col_prefix))
                    .push_bind_unseparated(variant as i32);
            }
            if let Some(account_id) = &params.account_id {
                conditions
                    .push("ext.account_id = ")
                    .push_bind_unseparated(normalize_address(account_id));
            }
            if let Some(data) = &params.data {
                // Use @> containment operator for JSONB
                conditions
                    .push(format!("{}data @> ", col_prefix))
                    .push_bind_unseparated(data);
            }
            // Add job filter
            if let Some((address, seq_id)) = job_filter {
                // Filter by Acurast chain for better index usage
                conditions.push("j.chain = ");
                conditions.push_bind_unseparated("Acurast");
                conditions.push_unseparated("::target_chain");

                if let Some(seq_id) = seq_id {
                    conditions.push("j.address = ");
                    conditions.push_bind_unseparated(address);
                    conditions.push_unseparated(" AND j.seq_id = ");
                    conditions.push_bind_unseparated(seq_id);
                } else {
                    conditions.push("j.address = ");
                    conditions.push_bind_unseparated(address);
                }
            }
        }

        let order_prefix = if needs_join { "e." } else { "" };
        query_builder.push(format!(
            " ORDER BY {} {}, {}block_number {}, {}extrinsic_index {}, {}index {}",
            sort_by,
            sort_order,
            order_prefix,
            sort_order,
            order_prefix,
            sort_order,
            order_prefix,
            sort_order
        ));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<EventRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop(); // Remove the extra item
        }

        Ok(Page::<EventRow> {
            cursor: if has_more {
                items.last().map(|l| {
                    serde_json::to_value(EventCursor {
                        block_number: l.block_number,
                        extrinsic_index: l.extrinsic_index,
                        index: l.index,
                    })
                    .unwrap()
                })
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    pub async fn get_jobs(&self, params: GetJobsParams) -> RpcResult<Page<JobRow>> {
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT block_number, extrinsic_index, event_index, data_path, chain::text as chain, address, seq_id, block_time FROM jobs"
        );

        // Determine sort order first for cursor comparison
        let sort_by = "block_number";
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || params.job.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");
            if let Some(cursor) = &params.cursor {
                conditions.push(format!("(block_number, extrinsic_index) {} (", cursor_op));
                conditions.push_bind_unseparated(cursor.block_number);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.index);
                conditions.push_unseparated(")");
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push("block_number >= ")
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(job_param) = &params.job {
                // Parse address format: supports SS58, hex, and optional #<seq_id> suffix
                // Examples: "5GrwvaEF...", "0xd43593...", "5GrwvaEF...#123"
                let (address, seq_id) = parse_job_id_filter(job_param);

                // Filter by Acurast chain for better index usage
                conditions.push("chain = ");
                conditions.push_bind_unseparated("Acurast");
                conditions.push_unseparated("::target_chain");

                if let Some(seq_id) = seq_id {
                    // Filter by both address AND seq_id
                    conditions.push("address = ");
                    conditions.push_bind_unseparated(address);
                    conditions.push_unseparated(" AND seq_id = ");
                    conditions.push_bind_unseparated(seq_id);
                } else {
                    // Filter by address only
                    conditions.push("address = ");
                    conditions.push_bind_unseparated(address);
                }
            }
        }

        query_builder.push(format!(
            " ORDER BY {} {}, extrinsic_index {}",
            sort_by, sort_order, sort_order
        ));
        // Fetch one extra to check if there are more items
        query_builder.push(" LIMIT ").push_bind(limit + 1);

        let query = query_builder.build_query_as::<JobRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop(); // Remove the extra item
        }

        Ok(Page::<JobRow> {
            cursor: if has_more {
                items.last().and_then(|l| {
                    serde_json::to_value(ExtrinsicCursor {
                        block_number: l.block_number,
                        index: l.extrinsic_index,
                    })
                    .ok()
                })
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    pub async fn get_storage_snapshots(
        &self,
        params: GetStorageSnapshotsParams,
    ) -> RpcResult<Page<StorageSnapshotRow>> {
        // Parse time filters
        let time_from: Option<DateTime<Utc>> = if let Some(t) = &params.time_from {
            Some(
                t.parse()
                    .map_err(|_| RpcError::invalid_params(format!("Invalid time_from: {}", t)))?,
            )
        } else {
            None
        };

        let time_to: Option<DateTime<Utc>> = if let Some(t) = &params.time_to {
            Some(
                t.parse()
                    .map_err(|_| RpcError::invalid_params(format!("Invalid time_to: {}", t)))?,
            )
        } else {
            None
        };

        // Resolve storage pallet name to index if needed
        let storage_pallet_idx = match &params.pallet {
            None => None,
            Some(StringOrNumber::Number(p)) => Some(*p),
            Some(StringOrNumber::String(pallet_name)) => {
                let pallet_index_map = crate::metadata::get_pallet_index_map(&self.client).await;
                let idx = pallet_index_map.get(pallet_name).ok_or_else(|| {
                    RpcError::invalid_params(format!("unknown pallet name: {}", pallet_name))
                })?;
                Some(*idx as u32)
            }
        };

        // Resolve extrinsic pallet/method filters if present
        let (ext_pallet, ext_method) = if let Some(ref ext_filter) = params.extrinsic {
            resolve_extrinsic_pallet_method(
                &self.client,
                ext_filter.pallet.as_ref(),
                ext_filter.method.as_ref(),
            )
            .await?
        } else {
            (None, None)
        };
        let ext_account_id = params
            .extrinsic
            .as_ref()
            .and_then(|f| f.account_id.as_ref());

        // Check if we need to join with extrinsics table
        let has_extrinsic_filter =
            ext_pallet.is_some() || ext_method.is_some() || ext_account_id.is_some();

        // Resolve event pallet/variant filters if present
        let (evt_pallet, evt_variant) = if let Some(ref evt_filter) = params.event {
            resolve_event_pallet_variant(
                &self.client,
                evt_filter.pallet.as_ref(),
                evt_filter.variant.as_ref(),
            )
            .await?
        } else {
            (None, None)
        };

        // Check if we need to join with events table
        let has_event_filter = evt_pallet.is_some() || evt_variant.is_some();

        // Determine if we need epoch info
        let is_sampling = params.sample.is_some();
        let needs_epochs = params.include_epochs || is_sampling;

        // Determine sort order
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(10) as i64;
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        // Build the query based on whether we need epochs/sampling
        if is_sampling {
            // Use CTE with epochs and DISTINCT ON for sampling
            let sample_unit = params.sample.unwrap();
            let epochs_per_sample = sample_unit.epochs_per_sample();

            // If fill is requested, first get the starting epoch_bucket to constrain the range
            let epoch_range_constraint: Option<(i64, i64)> = if params.fill {
                // Quick query to get the first epoch_bucket using MIN/MAX
                let agg_func = if sort_order.eq_ignore_ascii_case("desc") {
                    "MAX"
                } else {
                    "MIN"
                };

                let mut first_query = QueryBuilder::<Postgres>::new(
                    "WITH epochs_with_end AS (
                        SELECT epoch, epoch_start,
                               LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end,
                               epoch_start_time
                        FROM epochs
                    ),
                    snapshots_with_bucket AS (
                        SELECT (ep.epoch / ",
                );
                first_query.push_bind(epochs_per_sample);
                first_query.push(") * ");
                first_query.push_bind(epochs_per_sample);
                first_query.push(
                    " as epoch_bucket
                        FROM storage_snapshots s
                        LEFT JOIN epochs_with_end ep ON ep.epoch_start <= s.block_number
                            AND (ep.epoch_end IS NULL OR ep.epoch_end > s.block_number)",
                );

                // Join with extrinsics if we have extrinsic filters
                if has_extrinsic_filter {
                    first_query.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
                }

                // Join with events if we have event filters
                if has_event_filter {
                    first_query.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
                }

                // Build WHERE clause
                self.build_storage_snapshot_where_clause(
                    &mut first_query,
                    &params,
                    storage_pallet_idx,
                    time_from,
                    time_to,
                    ext_pallet,
                    ext_method,
                    ext_account_id,
                    evt_pallet,
                    evt_variant,
                    None,
                    cursor_op,
                );

                // Add epoch_bucket NOT NULL filter
                first_query.push(" AND (ep.epoch / ");
                first_query.push_bind(epochs_per_sample);
                first_query.push(") * ");
                first_query.push_bind(epochs_per_sample);
                first_query.push(" IS NOT NULL");

                if let Some(cursor) = params.cursor {
                    first_query.push(format!(" AND (ep.epoch / "));
                    first_query.push_bind(epochs_per_sample);
                    first_query.push(") * ");
                    first_query.push_bind(epochs_per_sample);
                    first_query.push(format!(" {} ", cursor_op));
                    first_query.push_bind(cursor);
                }

                // Close the CTE and select MIN or MAX
                first_query.push(format!(
                    ")
                    SELECT {}(epoch_bucket) as epoch_bucket FROM snapshots_with_bucket",
                    agg_func
                ));

                #[derive(sqlx::FromRow)]
                struct EpochBucketRow {
                    epoch_bucket: Option<i64>,
                }

                let first_result: Option<EpochBucketRow> = first_query
                    .build_query_as()
                    .fetch_optional(&self.db_pool)
                    .await
                    .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

                first_result
                    .and_then(|r| r.epoch_bucket)
                    .map(|first_bucket| {
                        // Calculate the range that gives at most `limit` items after filling
                        let range = (limit - 1) * epochs_per_sample;
                        if sort_order.eq_ignore_ascii_case("desc") {
                            // DESC: from first_bucket down to first_bucket - range
                            (first_bucket - range, first_bucket)
                        } else {
                            // ASC: from first_bucket up to first_bucket + range
                            (first_bucket, first_bucket + range)
                        }
                    })
            } else {
                None
            };

            let mut query_builder = QueryBuilder::<Postgres>::new(
                "WITH epochs_with_end AS (
                    SELECT epoch, epoch_start,
                           LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end,
                           epoch_start_time
                    FROM epochs
                ),
                snapshots_with_epoch AS (
                    SELECT s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                           s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                           ep.epoch, ep.epoch_start, ep.epoch_end, ep.epoch_start_time,
                           (ep.epoch / ",
            );
            query_builder.push_bind(epochs_per_sample);
            query_builder.push(") * ");
            query_builder.push_bind(epochs_per_sample);
            query_builder.push(
                " as epoch_bucket
                    FROM storage_snapshots s
                    LEFT JOIN epochs_with_end ep ON ep.epoch_start <= s.block_number
                        AND (ep.epoch_end IS NULL OR ep.epoch_end > s.block_number)",
            );

            // Join with extrinsics if we have extrinsic filters
            if has_extrinsic_filter {
                query_builder.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
            }

            // Join with events if we have event filters
            if has_event_filter {
                query_builder.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
            }

            // Build WHERE clause for inner query
            self.build_storage_snapshot_where_clause(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                time_from,
                time_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                None, // No cursor on inner query for sampling
                cursor_op,
            );

            query_builder.push(
                ")
                SELECT DISTINCT ON (epoch_bucket)
                    id, block_number, extrinsic_index, event_index, block_time,
                    pallet, storage_location, storage_keys, data, config_rule,
                    epoch, epoch_start, epoch_end, epoch_start_time, epoch_bucket
                FROM snapshots_with_epoch
                WHERE epoch_bucket IS NOT NULL",
            );

            // Apply epoch range constraint if fill is requested
            if let Some((min_bucket, max_bucket)) = epoch_range_constraint {
                query_builder.push(" AND epoch_bucket >= ");
                query_builder.push_bind(min_bucket);
                query_builder.push(" AND epoch_bucket <= ");
                query_builder.push_bind(max_bucket);
            } else if let Some(cursor) = params.cursor {
                // Cursor for sampling is based on epoch_bucket (only if no range constraint)
                query_builder.push(format!(" AND epoch_bucket {} ", cursor_op));
                query_builder.push_bind(cursor);
            }

            query_builder.push(format!(
                " ORDER BY epoch_bucket {}, block_number DESC",
                sort_order
            ));

            // When fill is used with range constraint, we don't need limit+1 check
            if epoch_range_constraint.is_none() {
                query_builder.push(" LIMIT ").push_bind(limit + 1);
            }

            let query = query_builder.build_query_as::<StorageSnapshotDbRow>();
            let db_rows: Vec<StorageSnapshotDbRow> =
                with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
                    .await
                    .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

            // Apply fill if requested
            let filled_rows = if params.fill {
                self.fill_missing_epochs(db_rows, epochs_per_sample)
            } else {
                db_rows
            };

            // Check pagination
            let has_more = if epoch_range_constraint.is_some() {
                // With fill + range constraint, check if we got exactly limit items
                // (meaning there could be more beyond the range)
                filled_rows.len() >= limit as usize
            } else {
                filled_rows.len() > limit as usize
            };

            let mut items: Vec<StorageSnapshotRow> =
                filled_rows.into_iter().map(|r| r.into()).collect();

            if has_more && epoch_range_constraint.is_none() {
                // Only pop if not using range constraint
                items.pop();
            }

            Ok(Page {
                cursor: if has_more {
                    // For sampling, cursor is the last epoch_bucket
                    items
                        .last()
                        .and_then(|l| l.epoch.as_ref().map(|e| serde_json::json!(e.epoch)))
                } else {
                    None
                },
                items,
                unfiltered_count: None,
            })
        } else if needs_epochs {
            // Include epochs but no sampling
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "WITH epochs_with_end AS (
                    SELECT epoch, epoch_start,
                           LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end,
                           epoch_start_time
                    FROM epochs
                )
                SELECT s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                       s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                       ep.epoch, ep.epoch_start, ep.epoch_end, ep.epoch_start_time,
                       NULL::bigint as epoch_bucket
                FROM storage_snapshots s
                LEFT JOIN epochs_with_end ep ON ep.epoch_start <= s.block_number
                    AND (ep.epoch_end IS NULL OR ep.epoch_end > s.block_number)",
            );

            // Join with extrinsics if we have extrinsic filters
            if has_extrinsic_filter {
                query_builder.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
            }

            // Join with events if we have event filters
            if has_event_filter {
                query_builder.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
            }

            // Build WHERE clause
            self.build_storage_snapshot_where_clause(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                time_from,
                time_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                params.cursor,
                cursor_op,
            );

            query_builder.push(format!(
                " ORDER BY s.block_number {}, s.id {}",
                sort_order, sort_order
            ));
            query_builder.push(" LIMIT ").push_bind(limit + 1);

            let query = query_builder.build_query_as::<StorageSnapshotDbRow>();
            let mut db_rows: Vec<StorageSnapshotDbRow> =
                with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
                    .await
                    .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

            let has_more = db_rows.len() > limit as usize;
            if has_more {
                db_rows.pop();
            }

            let items: Vec<StorageSnapshotRow> = db_rows.into_iter().map(|r| r.into()).collect();

            Ok(Page {
                cursor: if has_more {
                    items.last().map(|l| serde_json::json!(l.id))
                } else {
                    None
                },
                items,
                unfiltered_count: None,
            })
        } else {
            // Simple query without epochs
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "SELECT s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                        s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                        NULL::bigint as epoch, NULL::bigint as epoch_start,
                        NULL::bigint as epoch_end, NULL::timestamptz as epoch_start_time,
                        NULL::bigint as epoch_bucket
                 FROM storage_snapshots s",
            );

            // Join with extrinsics if we have extrinsic filters
            if has_extrinsic_filter {
                query_builder.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
            }

            // Join with events if we have event filters
            if has_event_filter {
                query_builder.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
            }

            // Build WHERE clause
            self.build_storage_snapshot_where_clause(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                time_from,
                time_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                params.cursor,
                cursor_op,
            );

            query_builder.push(format!(
                " ORDER BY s.block_number {}, s.id {}",
                sort_order, sort_order
            ));
            query_builder.push(" LIMIT ").push_bind(limit + 1);

            let query = query_builder.build_query_as::<StorageSnapshotDbRow>();
            let mut db_rows: Vec<StorageSnapshotDbRow> =
                with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
                    .await
                    .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

            let has_more = db_rows.len() > limit as usize;
            if has_more {
                db_rows.pop();
            }

            let items: Vec<StorageSnapshotRow> = db_rows.into_iter().map(|r| r.into()).collect();

            Ok(Page {
                cursor: if has_more {
                    items.last().map(|l| serde_json::json!(l.id))
                } else {
                    None
                },
                items,
                unfiltered_count: None,
            })
        }
    }

    /// Helper to build WHERE clause for storage snapshot queries
    #[allow(clippy::too_many_arguments)]
    fn build_storage_snapshot_where_clause<'a>(
        &self,
        query_builder: &mut QueryBuilder<'a, Postgres>,
        params: &'a GetStorageSnapshotsParams,
        storage_pallet_idx: Option<u32>,
        time_from: Option<DateTime<Utc>>,
        time_to: Option<DateTime<Utc>>,
        ext_pallet: Option<u32>,
        ext_method: Option<u32>,
        ext_account_id: Option<&String>,
        evt_pallet: Option<u32>,
        evt_variant: Option<u32>,
        cursor: Option<i64>,
        cursor_op: &str,
    ) {
        let has_extrinsic_filter =
            ext_pallet.is_some() || ext_method.is_some() || ext_account_id.is_some();
        let has_event_filter = evt_pallet.is_some() || evt_variant.is_some();

        let has_conditions = cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || time_from.is_some()
            || time_to.is_some()
            || storage_pallet_idx.is_some()
            || params.storage_location.is_some()
            || params.storage_keys.is_some()
            || params.data.is_some()
            || params.config_rule.is_some()
            || has_extrinsic_filter
            || has_event_filter
            || params.exclude_deleted;

        if has_conditions {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");

            if let Some(cursor) = cursor {
                conditions
                    .push(format!("s.id {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push("s.block_number >= ")
                    .push_bind_unseparated(block_from as i64);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("s.block_number <= ")
                    .push_bind_unseparated(block_to as i64);
            }
            if let Some(time_from) = time_from {
                conditions
                    .push("s.block_time >= ")
                    .push_bind_unseparated(time_from);
            }
            if let Some(time_to) = time_to {
                conditions
                    .push("s.block_time <= ")
                    .push_bind_unseparated(time_to);
            }
            if let Some(pallet) = storage_pallet_idx {
                conditions
                    .push("s.pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(storage_location) = &params.storage_location {
                conditions
                    .push("s.storage_location = ")
                    .push_bind_unseparated(storage_location);
            }
            if let Some(storage_keys) = &params.storage_keys {
                conditions
                    .push("s.storage_keys @> ")
                    .push_bind_unseparated(storage_keys);
            }
            if let Some(data) = &params.data {
                conditions.push("s.data @> ").push_bind_unseparated(data);
            }
            if let Some(config_rule) = &params.config_rule {
                conditions
                    .push("s.config_rule = ")
                    .push_bind_unseparated(config_rule);
            }
            if let Some(pallet) = ext_pallet {
                conditions
                    .push("e.pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(method) = ext_method {
                conditions
                    .push("e.method = ")
                    .push_bind_unseparated(method as i32);
            }
            if let Some(account_id) = ext_account_id {
                conditions
                    .push("e.account_id = ")
                    .push_bind_unseparated(normalize_address(account_id));
            }
            if let Some(pallet) = evt_pallet {
                conditions
                    .push("ev.pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(variant) = evt_variant {
                conditions
                    .push("ev.variant = ")
                    .push_bind_unseparated(variant as i32);
            }
            if params.exclude_deleted {
                conditions.push(
                    "NOT EXISTS (
                        SELECT 1 FROM storage_snapshots s2
                        WHERE s2.pallet = s.pallet
                        AND s2.storage_location = s.storage_location
                        AND s2.storage_keys = s.storage_keys
                        AND s2.block_number > s.block_number
                        AND s2.data = 'null'::jsonb
                    )",
                );
            }
        }
    }

    /// Fill missing epoch buckets with the previous value
    fn fill_missing_epochs(
        &self,
        rows: Vec<StorageSnapshotDbRow>,
        epochs_per_sample: i64,
    ) -> Vec<StorageSnapshotDbRow> {
        if rows.len() < 2 {
            return rows;
        }

        let mut result: Vec<StorageSnapshotDbRow> = Vec::new();
        let mut last_value: Option<StorageSnapshotDbRow> = None;

        // Get min and max buckets
        let min_bucket = rows.iter().filter_map(|r| r.epoch_bucket).min();
        let max_bucket = rows.iter().filter_map(|r| r.epoch_bucket).max();

        if let (Some(min), Some(max)) = (min_bucket, max_bucket) {
            // Build a map of bucket -> row
            let bucket_map: std::collections::HashMap<i64, &StorageSnapshotDbRow> = rows
                .iter()
                .filter_map(|r| r.epoch_bucket.map(|b| (b, r)))
                .collect();

            let mut bucket = min;
            while bucket <= max {
                if let Some(row) = bucket_map.get(&bucket) {
                    result.push((*row).clone());
                    last_value = Some((*row).clone());
                } else if let Some(ref prev) = last_value {
                    // Fill with previous value
                    let mut filled = prev.clone();
                    filled.id = -bucket; // Negative ID indicates filled value
                    filled.epoch_bucket = Some(bucket);
                    // Update epoch to match the bucket
                    filled.epoch = Some(bucket);
                    result.push(filled);
                }
                bucket += epochs_per_sample;
            }
        } else {
            return rows;
        }

        result
    }

    /// Get epoch metrics for a manager.
    /// Returns processor metrics grouped by epoch.
    pub async fn get_metrics_by_manager(
        &self,
        params: GetEpochMetricsParams,
    ) -> RpcResult<Page<EpochMetricsItem>> {
        use crate::entities::EpochIndexPhase;

        let limit = params.limit.unwrap_or(16) as i64;

        trace!(
            "get_metrics_by_manager called with manager={}, epoch_from={:?}, epoch_to={:?}, limit={}, cursor={:?}",
            params.manager,
            params.epoch_from,
            params.epoch_to,
            limit,
            params.cursor
        );

        if params.manager.is_empty() {
            trace!("get_metrics_by_manager: manager is empty, returning error");
            Err(RpcError::invalid_params("Manager cannot be empty"))?;
        }
        let manager_address = normalize_address(&params.manager);
        trace!(
            "get_metrics_by_manager: normalized manager_address={}",
            manager_address
        );

        // Query managers table for the given manager and epoch range
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT m.epoch, m.processors FROM managers m
             INNER JOIN epochs e ON m.epoch = e.epoch
             WHERE m.manager_address = ",
        );
        query_builder.push_bind(&manager_address);
        query_builder.push(" AND e.phase >= ");
        query_builder.push_bind(EpochIndexPhase::StorageIndexed2 as i32);

        if let Some(from) = params.epoch_from {
            query_builder.push(" AND m.epoch >= ");
            query_builder.push_bind(from);
        }
        if let Some(to) = params.epoch_to {
            query_builder.push(" AND m.epoch <= ");
            query_builder.push_bind(to);
        }
        if let Some(cursor) = params.cursor {
            query_builder.push(" AND m.epoch < ");
            query_builder.push_bind(cursor);
        }

        query_builder.push(" ORDER BY m.epoch DESC");
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit + 1);

        #[derive(sqlx::FromRow)]
        struct ManagerRow {
            epoch: i64,
            processors: serde_json::Value,
        }

        let query = query_builder.build_query_as::<ManagerRow>();
        let mut rows = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        trace!(
            "get_metrics_by_manager: fetched {} manager rows from database",
            rows.len()
        );

        // Check if there are more items beyond the limit
        let has_more = rows.len() > limit as usize;
        if has_more {
            rows.pop();
        }

        let items: Vec<EpochMetricsItem> = rows
            .into_iter()
            .map(|r| EpochMetricsItem {
                epoch: r.epoch,
                metrics: r.processors,
            })
            .collect();

        trace!(
            "get_metrics_by_manager: returning {} items, has_more={}",
            items.len(),
            has_more
        );

        Ok(Page {
            cursor: if has_more {
                items.last().map(|i| serde_json::json!(i.epoch))
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    /// Get metrics for a specific processor across epochs.
    /// Returns the processor's metrics grouped by epoch.
    pub async fn get_metrics_by_processor(
        &self,
        params: GetProcessorMetricsParams,
    ) -> RpcResult<Page<EpochMetricsManagerItem>> {
        use crate::entities::EpochIndexPhase;

        let limit = params.limit.unwrap_or(16) as i64;

        let processor = normalize_address_with_prefix(&params.processor);
        trace!(
            "get_metrics_by_processor called with processor={}, epoch_from={:?}, epoch_to={:?}, limit={}, cursor={:?}",
            processor,
            params.epoch_from,
            params.epoch_to,
            limit,
            params.cursor
        );

        if params.processor.is_empty() {
            trace!("get_metrics_by_processor: processor is empty, returning error");
            Err(RpcError::invalid_params("Processor cannot be empty"))?;
        }

        // Query managers table for all managers in epoch range
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT m.epoch, m.manager_address, m.processors FROM managers m
             INNER JOIN epochs e ON m.epoch = e.epoch
             WHERE e.phase >= ",
        );
        query_builder.push_bind(EpochIndexPhase::StorageIndexed2 as i32);

        // Filter for rows containing the processor_address as a key in the processors JSONB
        query_builder.push(" AND m.processors ? ");
        query_builder.push_bind(&processor);

        if let Some(from) = params.epoch_from {
            query_builder.push(" AND m.epoch >= ");
            query_builder.push_bind(from);
        }
        if let Some(to) = params.epoch_to {
            query_builder.push(" AND m.epoch <= ");
            query_builder.push_bind(to);
        }
        if let Some(cursor) = params.cursor {
            query_builder.push(" AND m.epoch < ");
            query_builder.push_bind(cursor);
        }

        query_builder.push(" ORDER BY m.epoch DESC");
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit + 1);

        #[derive(sqlx::FromRow)]
        struct ManagerRow {
            epoch: i64,
            manager_address: String,
            processors: serde_json::Value,
        }

        let rows: Vec<ManagerRow> = with_timeout(
            self.query_timeout,
            query_builder
                .build_query_as::<ManagerRow>()
                .fetch_all(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        trace!(
            "get_metrics_by_processor: fetched {} manager rows from database",
            rows.len()
        );

        // Build result: extract metrics for the specific processor from each epoch
        let mut items: Vec<EpochMetricsManagerItem> = Vec::new();

        for row in rows {
            // processors is stored as an object: { "address": { metrics... }, ... }
            if let Some(processor_data) = row.processors.get(&processor) {
                trace!(
                    "get_metrics_by_processor: found processor {} in epoch {}",
                    processor,
                    row.epoch
                );
                items.push(EpochMetricsManagerItem {
                    epoch: row.epoch,
                    manager_address: ensure_hex_prefix(&row.manager_address),
                    metrics: processor_data.clone(),
                });

                // Stop if we have enough items (limit + 1 for has_more check)
                if items.len() > limit as usize {
                    break;
                }
            }
        }

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop();
        }

        trace!(
            "get_metrics_by_processor: returning {} items, has_more={}",
            items.len(),
            has_more
        );

        Ok(Page {
            cursor: if has_more {
                items.last().map(|i| serde_json::json!(i.epoch))
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    /// Get commitments with optional filtering and sorting
    pub async fn get_commitments(
        &self,
        params: GetCommitmentsParams,
    ) -> RpcResult<Page<CommitmentRow>> {
        let order_by = params.order_by.as_deref().unwrap_or("stake_amount");
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = params.limit.unwrap_or(50) as i64;

        // Validate order_by column
        let valid_columns = [
            "commitment_id",
            "stake_amount",
            "stake_rewardable_amount",
            "delegations_total_amount",
            "commission",
            "epoch",
            "block_number",
            "last_scoring_epoch",
            "cooldown_period",
            "delegation_utilization",
            "target_weight_per_compute_utilization",
            "combined_utilization",
            "max_delegation_capacity",
            "min_max_weight_per_compute",
            "remaining_capacity",
            "combined_stake",
            "combined_weight",
        ];
        if !valid_columns.contains(&order_by) {
            return Err(RpcError::invalid_params(format!(
                "Invalid order_by column: {}. Valid columns: {:?}",
                order_by, valid_columns
            )));
        }

        // Map computed columns to SQL expressions
        let order_by_expr = match order_by {
            "combined_stake" => "(c.stake_amount + c.delegations_total_amount)".to_string(),
            "combined_weight" => "(c.delegations_slash_weight + c.self_slash_weight)".to_string(),
            col => format!("c.{}", col),
        };

        // Build query
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT c.id, c.commitment_id, c.snapshot_id, c.block_number, c.block_time, c.epoch,
                    c.committer_address, c.manager_id, c.manager_address,
                    c.commission, c.stake_amount, c.stake_rewardable_amount,
                    c.stake_accrued_reward, c.stake_paid,
                    c.delegations_total_amount, c.delegations_total_rewardable_amount,
                    c.last_scoring_epoch, c.last_slashing_epoch, c.stake_created_epoch,
                    c.cooldown_started, c.cooldown_period, c.is_active,
                    c.max_delegation_capacity, c.min_max_weight_per_compute,
                    c.delegation_utilization, c.target_weight_per_compute_utilization,
                    c.combined_utilization, c.remaining_capacity,
                    c.delegations_reward_weight, c.delegations_slash_weight,
                    c.self_reward_weight, c.self_slash_weight,
                    c.reward_per_weight, c.slash_per_weight,
                    c.committed_metrics, mes.data as metrics_epoch_sum, c.phase
             FROM commitments c
             LEFT JOIN LATERAL (
                 SELECT s.data
                 FROM storage_snapshots s
                 WHERE s.pallet = 48
                   AND s.storage_location = 'MetricsEpochSum'
                   AND c.manager_id IS NOT NULL
                   AND s.storage_keys->>0 = c.manager_id::TEXT
                 ORDER BY s.block_number DESC
                 LIMIT 1
             ) mes ON true
             WHERE 1=1",
        );

        // Add filters
        if let Some(commitment_id) = params.commitment_id {
            query_builder.push(" AND c.commitment_id = ");
            query_builder.push_bind(commitment_id);
        }
        if let Some(ref committer_address) = params.committer_address {
            let normalized = normalize_address_with_prefix(committer_address);
            query_builder.push(" AND c.committer_address = ");
            query_builder.push_bind(normalized);
        }
        if let Some(manager_id) = params.manager_id {
            query_builder.push(" AND c.manager_id = ");
            query_builder.push_bind(manager_id);
        }
        if let Some(ref manager_address) = params.manager_address {
            let normalized = normalize_address_with_prefix(manager_address);
            query_builder.push(" AND c.manager_address = ");
            query_builder.push_bind(normalized);
        }
        if let Some(is_active) = params.is_active {
            query_builder.push(" AND c.is_active = ");
            query_builder.push_bind(is_active);
        }
        if let Some(in_cooldown) = params.in_cooldown {
            if in_cooldown {
                query_builder.push(" AND c.cooldown_started IS NOT NULL");
            } else {
                query_builder.push(" AND c.cooldown_started IS NULL");
            }
        }

        // Range filters for numeric columns
        // Helper macro to add range filters
        macro_rules! add_range_filter {
            ($col:literal, $min:expr, $max:expr) => {
                if let Some(ref min_val) = $min {
                    if let Ok(v) = min_val.parse::<bigdecimal::BigDecimal>() {
                        query_builder.push(concat!(" AND c.", $col, " >= "));
                        query_builder.push_bind(v);
                    }
                }
                if let Some(ref max_val) = $max {
                    if let Ok(v) = max_val.parse::<bigdecimal::BigDecimal>() {
                        query_builder.push(concat!(" AND c.", $col, " <= "));
                        query_builder.push_bind(v);
                    }
                }
            };
        }

        add_range_filter!(
            "stake_amount",
            params.min_stake_amount,
            params.max_stake_amount
        );
        add_range_filter!(
            "delegations_total_amount",
            params.min_delegations_total_amount,
            params.max_delegations_total_amount
        );
        add_range_filter!("commission", params.min_commission, params.max_commission);
        add_range_filter!(
            "delegation_utilization",
            params.min_delegation_utilization,
            params.max_delegation_utilization
        );
        add_range_filter!(
            "target_weight_per_compute_utilization",
            params.min_target_weight_per_compute_utilization,
            params.max_target_weight_per_compute_utilization
        );
        add_range_filter!(
            "combined_utilization",
            params.min_combined_utilization,
            params.max_combined_utilization
        );
        add_range_filter!(
            "max_delegation_capacity",
            params.min_max_delegation_capacity,
            params.max_max_delegation_capacity
        );
        add_range_filter!(
            "min_max_weight_per_compute",
            params.min_min_max_weight_per_compute,
            params.max_min_max_weight_per_compute
        );
        add_range_filter!(
            "remaining_capacity",
            params.min_remaining_capacity,
            params.max_remaining_capacity
        );
        add_range_filter!(
            "cooldown_period",
            params.min_cooldown_period,
            params.max_cooldown_period
        );

        // Add cursor condition for keyset pagination
        // When ordering by commitment_id, use simple cursor (just the id)
        // When ordering by other columns, use compound cursor with tuple comparison
        let is_desc = sort_order.eq_ignore_ascii_case("desc");
        if let Some(ref cursor) = params.cursor {
            if order_by == "commitment_id" {
                // Simple cursor: just commitment_id
                let cursor_id = cursor
                    .as_i64()
                    .or_else(|| cursor.get("id").and_then(|v| v.as_i64()))
                    .ok_or_else(|| {
                        RpcError::invalid_params(
                            "Invalid cursor: expected number or {\"id\": number}",
                        )
                    })?;
                query_builder.push(if is_desc {
                    " AND c.commitment_id < "
                } else {
                    " AND c.commitment_id > "
                });
                query_builder.push_bind(cursor_id);
            } else {
                // Compound cursor: {id: commitment_id, val: sort_column_value}
                // Use tuple comparison: (sort_col, id) < (cursor_val, cursor_id) for DESC
                let cursor_id = cursor.get("id").and_then(|v| v.as_i64()).ok_or_else(|| {
                    RpcError::invalid_params(
                        "Invalid cursor for non-id ordering: expected {\"id\": number, \"val\": value}",
                    )
                })?;
                let cursor_val = cursor.get("val").ok_or_else(|| {
                    RpcError::invalid_params(
                        "Invalid cursor for non-id ordering: expected {\"id\": number, \"val\": value}",
                    )
                })?;

                // Parse cursor value as BigDecimal for numeric comparisons
                let cursor_val_str = match cursor_val {
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Null => {
                        // NULL cursor value: only compare by id within NULLs
                        query_builder.push(format!(
                            " AND {} IS NULL AND c.commitment_id {} ",
                            order_by_expr,
                            if is_desc { "<" } else { ">" }
                        ));
                        query_builder.push_bind(cursor_id);
                        // Skip the normal tuple comparison below
                        "".to_string()
                    }
                    _ => {
                        return Err(RpcError::invalid_params(
                            "Invalid cursor val: expected number or string",
                        ))
                    }
                };

                if !cursor_val_str.is_empty() {
                    let cursor_decimal =
                        cursor_val_str
                            .parse::<bigdecimal::BigDecimal>()
                            .map_err(|_| {
                                RpcError::invalid_params("Invalid cursor val: not a valid number")
                            })?;

                    // Tuple comparison: (sort_col, id) op (cursor_val, cursor_id)
                    // For DESC: (col < val) OR (col = val AND id < cursor_id) OR (col IS NULL)
                    // For ASC: (col > val) OR (col = val AND id > cursor_id)
                    // Note: NULLS LAST means NULLs come after all values, so in DESC they're last
                    if is_desc {
                        query_builder.push(format!(" AND (({} < ", order_by_expr));
                        query_builder.push_bind(cursor_decimal.clone());
                        query_builder.push(format!(") OR ({} = ", order_by_expr));
                        query_builder.push_bind(cursor_decimal);
                        query_builder.push(" AND c.commitment_id < ");
                        query_builder.push_bind(cursor_id);
                        query_builder.push(format!(") OR ({} IS NULL))", order_by_expr));
                    } else {
                        // ASC: values after cursor_val, or same value with higher id
                        // NULLs come last, so if cursor_val is not NULL, we still see NULLs later
                        query_builder.push(format!(" AND (({} > ", order_by_expr));
                        query_builder.push_bind(cursor_decimal.clone());
                        query_builder.push(format!(") OR ({} = ", order_by_expr));
                        query_builder.push_bind(cursor_decimal);
                        query_builder.push(" AND c.commitment_id > ");
                        query_builder.push_bind(cursor_id);
                        query_builder.push(format!(") OR ({} IS NULL))", order_by_expr));
                    }
                }
            }
        }

        // Add ordering (always secondary sort by commitment_id for stable pagination)
        query_builder.push(format!(
            " ORDER BY {} {} NULLS LAST, c.commitment_id {}",
            order_by_expr,
            if sort_order.eq_ignore_ascii_case("desc") {
                "DESC"
            } else {
                "ASC"
            },
            if sort_order.eq_ignore_ascii_case("desc") {
                "DESC"
            } else {
                "ASC"
            }
        ));

        // Fetch one more than limit to check for more pages
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit + 1);

        let query = query_builder.build_query_as::<CommitmentRow>();

        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Check if there are more items beyond the limit
        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop();
        }

        // Get fast estimate of total commitments using pg_class
        let estimate: Option<(i64,)> = sqlx::query_as(
            "SELECT reltuples::bigint AS estimate FROM pg_class WHERE relname = 'commitments'",
        )
        .fetch_optional(&self.db_pool)
        .await
        .ok()
        .flatten();

        // Build cursor for next page
        let next_cursor = if has_more {
            items.last().map(|l| {
                if order_by == "commitment_id" {
                    // Simple cursor for commitment_id ordering
                    serde_json::json!(l.commitment_id)
                } else {
                    // Compound cursor with sort column value
                    let sort_val: serde_json::Value = match order_by {
                        "stake_amount" => serde_json::json!(l.stake_amount.to_string()),
                        "stake_rewardable_amount" => {
                            serde_json::json!(l.stake_rewardable_amount.to_string())
                        }
                        "delegations_total_amount" => {
                            serde_json::json!(l.delegations_total_amount.to_string())
                        }
                        "commission" => serde_json::json!(l.commission.to_string()),
                        "epoch" => serde_json::json!(l.epoch),
                        "block_number" => serde_json::json!(l.block_number),
                        "last_scoring_epoch" => serde_json::json!(l.last_scoring_epoch),
                        "delegation_utilization" => l
                            .delegation_utilization
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "target_weight_per_compute_utilization" => l
                            .target_weight_per_compute_utilization
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "combined_utilization" => l
                            .combined_utilization
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "max_delegation_capacity" => l
                            .max_delegation_capacity
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "min_max_weight_per_compute" => l
                            .min_max_weight_per_compute
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "remaining_capacity" => l
                            .remaining_capacity
                            .as_ref()
                            .map(|v| serde_json::json!(v.to_string()))
                            .unwrap_or(serde_json::Value::Null),
                        "cooldown_period" => l
                            .cooldown_period
                            .map(|v| serde_json::json!(v))
                            .unwrap_or(serde_json::Value::Null),
                        "combined_stake" => {
                            // Computed: stake_amount + delegations_total_amount
                            let sum = &l.stake_amount + &l.delegations_total_amount;
                            serde_json::json!(sum.to_string())
                        }
                        "combined_weight" => {
                            // Computed: delegations_slash_weight + self_slash_weight
                            let sum = &l.delegations_slash_weight + &l.self_slash_weight;
                            serde_json::json!(sum.to_string())
                        }
                        _ => serde_json::json!(l.commitment_id), // Fallback
                    };
                    serde_json::json!({"id": l.commitment_id, "val": sort_val})
                }
            })
        } else {
            None
        };

        Ok(Page::<CommitmentRow> {
            cursor: next_cursor,
            items,
            unfiltered_count: estimate.map(|(e,)| e as u32),
        })
    }
}

#[derive(Debug, Deserialize, Default)]
pub struct GetProcessorsCountByEpochParams {
    #[serde(default)]
    pub epoch_from: Option<u64>,
    #[serde(default)]
    pub epoch_to: Option<u64>,
    #[serde(default)]
    pub sort_order: Option<String>,
    #[serde(default)]
    pub cursor: Option<i64>,
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct ProcessorsCountByEpochRow {
    pub epoch: i64,
    pub count: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetEpochMetricsParams {
    pub manager: String,
    pub epoch_from: Option<i64>,
    pub epoch_to: Option<i64>,
    pub limit: Option<u32>,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetProcessorMetricsParams {
    pub processor: String,
    pub epoch_from: Option<i64>,
    pub epoch_to: Option<i64>,
    pub limit: Option<u32>,
    pub cursor: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpochMetricsItem {
    pub epoch: i64,
    pub metrics: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct EpochMetricsManagerItem {
    pub epoch: i64,
    pub manager_address: String,
    pub metrics: serde_json::Value,
}

// ============================================
// COMMITMENTS
// ============================================

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetCommitmentsParams {
    /// Filter by commitment_id
    #[serde(default)]
    pub commitment_id: Option<i64>,
    /// Filter by committer address (hex)
    #[serde(default)]
    pub committer_address: Option<String>,
    /// Filter by manager_id
    #[serde(default)]
    pub manager_id: Option<i64>,
    /// Filter by manager address (hex)
    #[serde(default)]
    pub manager_address: Option<String>,
    /// Filter by active status
    #[serde(default)]
    pub is_active: Option<bool>,
    /// Filter by cooldown status: true = in cooldown, false = not in cooldown
    #[serde(default)]
    pub in_cooldown: Option<bool>,

    // Range filters for numeric columns (all optional, can specify min, max, or both)
    /// Minimum stake_amount
    #[serde(default)]
    pub min_stake_amount: Option<String>,
    /// Maximum stake_amount
    #[serde(default)]
    pub max_stake_amount: Option<String>,
    /// Minimum delegations_total_amount
    #[serde(default)]
    pub min_delegations_total_amount: Option<String>,
    /// Maximum delegations_total_amount
    #[serde(default)]
    pub max_delegations_total_amount: Option<String>,
    /// Minimum commission (in basis points)
    #[serde(default)]
    pub min_commission: Option<String>,
    /// Maximum commission (in basis points)
    #[serde(default)]
    pub max_commission: Option<String>,
    /// Minimum delegation_utilization (0.0-1.0)
    #[serde(default)]
    pub min_delegation_utilization: Option<String>,
    /// Maximum delegation_utilization (0.0-1.0)
    #[serde(default)]
    pub max_delegation_utilization: Option<String>,
    /// Minimum target_weight_per_compute_utilization
    #[serde(default)]
    pub min_target_weight_per_compute_utilization: Option<String>,
    /// Maximum target_weight_per_compute_utilization
    #[serde(default)]
    pub max_target_weight_per_compute_utilization: Option<String>,
    /// Minimum combined_utilization (0.0-1.0)
    #[serde(default)]
    pub min_combined_utilization: Option<String>,
    /// Maximum combined_utilization (0.0-1.0)
    #[serde(default)]
    pub max_combined_utilization: Option<String>,
    /// Minimum max_delegation_capacity
    #[serde(default)]
    pub min_max_delegation_capacity: Option<String>,
    /// Maximum max_delegation_capacity
    #[serde(default)]
    pub max_max_delegation_capacity: Option<String>,
    /// Minimum min_max_weight_per_compute
    #[serde(default)]
    pub min_min_max_weight_per_compute: Option<String>,
    /// Maximum min_max_weight_per_compute
    #[serde(default)]
    pub max_min_max_weight_per_compute: Option<String>,
    /// Minimum remaining_capacity
    #[serde(default)]
    pub min_remaining_capacity: Option<String>,
    /// Maximum remaining_capacity
    #[serde(default)]
    pub max_remaining_capacity: Option<String>,
    /// Minimum cooldown_period
    #[serde(default)]
    pub min_cooldown_period: Option<String>,
    /// Maximum cooldown_period
    #[serde(default)]
    pub max_cooldown_period: Option<String>,

    /// Order by column: commitment_id, stake_amount, stake_rewardable_amount,
    /// delegations_total_amount, commission, epoch, block_number, delegation_utilization,
    /// target_weight_per_compute_utilization, combined_utilization, max_delegation_capacity,
    /// min_max_weight_per_compute, remaining_capacity, cooldown_period (default: stake_amount)
    #[serde(default)]
    pub order_by: Option<String>,
    /// Sort order: "asc" or "desc" (default: "desc")
    #[serde(default)]
    pub sort_order: Option<String>,
    /// Maximum results (default: 50)
    #[serde(default)]
    pub limit: Option<u32>,
    /// Cursor for pagination. For simple ordering by commitment_id, pass the commitment_id.
    /// For other orderings, pass a compound cursor: {"id": commitment_id, "val": sort_value}
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct CommitmentRow {
    pub id: i64,
    pub commitment_id: i64,
    pub snapshot_id: Option<i64>,
    pub block_number: i64,
    pub block_time: chrono::DateTime<chrono::Utc>,
    pub epoch: i64,
    pub committer_address: String,
    pub manager_id: Option<i64>,
    pub manager_address: Option<String>,
    pub commission: bigdecimal::BigDecimal,
    pub stake_amount: bigdecimal::BigDecimal,
    pub stake_rewardable_amount: bigdecimal::BigDecimal,
    pub stake_accrued_reward: bigdecimal::BigDecimal,
    pub stake_paid: bigdecimal::BigDecimal,
    pub delegations_total_amount: bigdecimal::BigDecimal,
    pub delegations_total_rewardable_amount: bigdecimal::BigDecimal,
    pub last_scoring_epoch: i64,
    pub last_slashing_epoch: i64,
    pub stake_created_epoch: i64,
    pub cooldown_started: Option<i64>,
    pub cooldown_period: Option<i64>,
    pub is_active: bool,
    pub max_delegation_capacity: Option<bigdecimal::BigDecimal>,
    pub min_max_weight_per_compute: Option<bigdecimal::BigDecimal>,
    pub delegation_utilization: Option<bigdecimal::BigDecimal>,
    pub target_weight_per_compute_utilization: Option<bigdecimal::BigDecimal>,
    pub combined_utilization: Option<bigdecimal::BigDecimal>,
    pub remaining_capacity: Option<bigdecimal::BigDecimal>,
    pub delegations_reward_weight: bigdecimal::BigDecimal,
    pub delegations_slash_weight: bigdecimal::BigDecimal,
    pub self_reward_weight: bigdecimal::BigDecimal,
    pub self_slash_weight: bigdecimal::BigDecimal,
    pub reward_per_weight: bigdecimal::BigDecimal,
    pub slash_per_weight: bigdecimal::BigDecimal,
    pub committed_metrics: Option<serde_json::Value>,
    pub metrics_epoch_sum: Option<serde_json::Value>,
    pub phase: i32,
}
