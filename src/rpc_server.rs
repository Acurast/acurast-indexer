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

    pub fn data(&self) -> Option<&serde_json::Value> {
        self.data.as_ref()
    }

    /// Database error (-32000)
    ///
    /// The real error detail is logged server-side; the client only
    /// receives a static message so schema/query details are not leaked.
    pub fn database(msg: impl Into<String>) -> Self {
        tracing::error!("Database error: {}", msg.into());
        Self::new(-32000, "Database error")
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

/// Deserialize an RPC parameter object and surface a descriptive error
/// when it fails. On success returns `T`. On failure returns an
/// `RpcError(-32602)` whose `data` field is a one-element JSON array of
/// `FieldError` so the response shape stays stable as we add multi-error
/// validation layers later.
pub fn validate_params<T>(value: serde_json::Value) -> RpcResult<T>
where
    T: serde::de::DeserializeOwned,
{
    serde_json::from_value::<T>(value).map_err(|e| {
        RpcError::with_data(-32602, "Invalid params", serde_json::json!(e.to_string()))
    })
}

/// Parse a non-sampling `getStorageSnapshots` cursor.
/// Expects `{ "block_number": <i64>, "id": <i64> }`.
fn parse_snapshot_cursor(c: &serde_json::Value) -> RpcResult<(i64, i64)> {
    let bad = || {
        RpcError::invalid_params(
            "cursor must be an object with shape {\"block_number\": <i64>, \"id\": <i64>}",
        )
    };
    let block = c
        .get("block_number")
        .and_then(|v| v.as_i64())
        .ok_or_else(bad)?;
    let id = c.get("id").and_then(|v| v.as_i64()).ok_or_else(bad)?;
    Ok((block, id))
}

/// Parse a `getAccounts` cursor.
/// Expects `{ "sort_value": <numeric string>, "account_id": <string> }`.
fn parse_accounts_cursor(c: &serde_json::Value) -> RpcResult<(bigdecimal::BigDecimal, String)> {
    let bad = || {
        RpcError::invalid_params(
            "cursor must be an object with shape {\"sort_value\": <numeric string>, \"account_id\": <string>}",
        )
    };
    let sort_value = c
        .get("sort_value")
        .and_then(|v| v.as_str())
        .ok_or_else(bad)?
        .parse::<bigdecimal::BigDecimal>()
        .map_err(|_| RpcError::invalid_params("Invalid cursor sort_value: not a valid number"))?;
    let account_id = c
        .get("account_id")
        .and_then(|v| v.as_str())
        .ok_or_else(bad)?
        .to_string();
    Ok((sort_value, account_id))
}

impl From<serde_json::Error> for RpcError {
    fn from(e: serde_json::Error) -> Self {
        Self::invalid_params(e.to_string())
    }
}

/// Maximum number of items a single paginated RPC call may return.
const MAX_PAGE_LIMIT: u32 = 1000;

/// Resolve a user-supplied page limit: apply `default` when absent and
/// clamp to `[1, MAX_PAGE_LIMIT]` so a huge `limit` cannot force an
/// unbounded `fetch_all` (memory/response-size amplification).
fn page_limit(limit: Option<u32>, default: u32) -> i64 {
    limit.unwrap_or(default).clamp(1, MAX_PAGE_LIMIT) as i64
}

/// Convert a user-supplied integer filter value to a Postgres `bigint`,
/// rejecting values above `i64::MAX` that would silently wrap negative
/// with an `as i64` cast and invert the filter semantics.
fn to_i64_param<T: TryInto<i64>>(name: &str, value: T) -> RpcResult<i64> {
    value.try_into().map_err(|_| {
        RpcError::invalid_params(format!(
            "{} exceeds the maximum supported value ({})",
            name,
            i64::MAX
        ))
    })
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

/// Cursor for events - composite key of block_number and event index
/// Note: Event index is unique per block, so extrinsic_index is not needed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCursor {
    pub block_number: i64,
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
                let value = u32::try_from(value)
                    .map_err(|_| E::custom(format!("number {} exceeds u32 range", value)))?;
                Ok(StringOrNumber::Number(value))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u32::try_from(value)
                    .map_err(|_| E::custom(format!("number {} exceeds u32 range", value)))?;
                Ok(StringOrNumber::Number(value))
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
                    let p_u8 = u8::try_from(*p).map_err(|_| {
                        RpcError::invalid_params(format!("pallet index {} exceeds u8 range", p))
                    })?;
                    reverse_pallet_map
                        .get(&p_u8)
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
                    let p_u8 = u8::try_from(*p).map_err(|_| {
                        RpcError::invalid_params(format!("pallet index {} exceeds u8 range", p))
                    })?;
                    reverse_pallet_map
                        .get(&p_u8)
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

/// One (pallet, method) pair for multi-pair extrinsic filters.
/// `pallet` is required; `method` is optional (count across all methods of a pallet).
#[derive(Debug, Deserialize, Default, Clone)]
pub struct ExtrinsicPair {
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub method: Option<StringOrNumber>,
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
    /// Multiple (pallet, method) pairs, OR'd together. Combined with the single
    /// `pallet`/`method` fields if both are supplied (which becomes one extra pair).
    #[serde(default)]
    pub pairs: Option<Vec<ExtrinsicPair>>,
}

/// One (pallet, variant) pair for multi-pair event filters.
/// `pallet` is required; `variant` is optional (count across all variants of a pallet).
#[derive(Debug, Deserialize, Default, Clone)]
pub struct EventPair {
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub variant: Option<StringOrNumber>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetEventsCountParams {
    #[serde(default)]
    pub block_from: Option<u32>,
    #[serde(default)]
    pub block_to: Option<u32>,
    #[serde(default)]
    pub pallet: Option<StringOrNumber>,
    #[serde(default)]
    pub variant: Option<StringOrNumber>,
    /// Filter by event emission source: "extrinsic" or "system".
    #[serde(default)]
    pub source: Option<EventSourceFilter>,
    /// Multiple (pallet, variant) pairs, OR'd together. Combined with the single
    /// `pallet`/`variant` fields if both are supplied (which becomes one extra pair).
    #[serde(default)]
    pub pairs: Option<Vec<EventPair>>,
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

/// Filter for event emission source
#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EventSourceFilter {
    /// Events emitted during extrinsic execution (ApplyExtrinsic phase)
    Extrinsic,
    /// Events emitted by system (Initialization or Finalization phase)
    System,
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
    /// Filter by event emission source: "extrinsic" (ApplyExtrinsic) or "system" (Initialization/Finalization)
    #[serde(default)]
    pub source: Option<EventSourceFilter>,
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
    /// Cursor for pagination.
    /// - For non-sampling queries: a JSON object {"block_number": N, "id": M} matching
    ///   the last item of the previous page. Required because rows are ordered by
    ///   (block_number, id); a single id wouldn't disambiguate.
    /// - For `sample` queries: a single number (the previous page's `epoch_bucket`).
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
    /// Number of items to return (default 10, max 1000)
    #[serde(default)]
    pub limit: Option<u32>,
    /// Exclude snapshots that have a subsequent snapshot with null data (default: false)
    #[serde(default)]
    pub exclude_deleted: bool,
    /// Sample snapshots by time unit. Returns first snapshot per time period.
    #[serde(default)]
    pub sample: Option<SampleUnit>,
    /// Include epoch information in the response.
    /// When true or when sample is set, joins with epochs table.
    #[serde(default)]
    pub include_epochs: bool,
    /// Filter by epoch index (for epoch-triggered snapshots)
    #[serde(default)]
    pub epoch_index: Option<i64>,
    /// Filter by epoch end flag (true = end of epoch, false = start of epoch)
    #[serde(default)]
    pub epoch_end: Option<bool>,
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
    pub extrinsic_index: Option<i32>,
    pub event_index: Option<i32>,
    pub block_time: DateTime<Utc>,
    pub pallet: i32,
    pub storage_location: String,
    pub storage_keys: serde_json::Value,
    pub data: serde_json::Value,
    pub config_rule: String,
    pub epoch_end: bool,
    // Optional epoch fields (populated when joining with epochs)
    pub epoch: Option<i64>,
    pub epoch_start: Option<i64>,
    // Block number where this epoch ends (LEAD over epoch_start). Aliased to avoid
    // colliding with storage_snapshots.epoch_end (the boolean flag above).
    pub epoch_end_block: Option<i64>,
    pub epoch_start_time: Option<DateTime<Utc>>,
}

/// API response struct with nested epoch info
#[derive(Debug, Serialize, Clone)]
pub struct StorageSnapshotRow {
    pub id: i64,
    pub block_number: i64,
    pub extrinsic_index: Option<i32>,
    pub event_index: Option<i32>,
    pub block_time: DateTime<Utc>,
    pub pallet: i32,
    pub storage_location: String,
    pub storage_keys: serde_json::Value,
    pub data: serde_json::Value,
    pub config_rule: String,
    pub epoch_end: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<EpochInfo>,
}

impl From<StorageSnapshotDbRow> for StorageSnapshotRow {
    fn from(row: StorageSnapshotDbRow) -> Self {
        let epoch = row.epoch.map(|e| EpochInfo {
            epoch: e,
            epoch_start: row.epoch_start.unwrap_or(0),
            epoch_end: row.epoch_end_block,
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
            epoch_end: row.epoch_end,
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

fn primitive_as_text(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Substrate storage location names are PascalCase identifiers (e.g., "Account",
/// "MetricsEpochSum"). We inline these into SQL rather than binding them as
/// parameters so the planner can match partial indexes whose predicate is
/// `WHERE storage_location = '<name>'` — under a generic plan it cannot prove
/// that a bound parameter equals the index's literal.
fn is_valid_storage_location(s: &str) -> bool {
    !s.is_empty() && s.len() <= 64 && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
            sort_by, sort_order_sql, sort_order_sql
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
                .push_bind_unseparated(to_i64_param("block_from", block_from)?);
        }
        if let Some(block_to) = params.block_to {
            conditions
                .push("block_number <= ")
                .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("epoch_from", epoch_from)?);
            }

            if let Some(epoch_to) = params.epoch_to {
                conditions
                    .push("epoch <= ")
                    .push_bind_unseparated(to_i64_param("epoch_to", epoch_to)?);
            }

            if let Some(block_from) = params.block_from {
                conditions
                    .push("epoch_start >= ")
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }

            if let Some(block_to) = params.block_to {
                conditions
                    .push("epoch_start <= ")
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
            }

            if let Some(cursor) = params.cursor {
                conditions
                    .push(format!("epoch {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
        }

        query_builder.push(format!(" ORDER BY epoch {}", sort_order_sql));
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
        let limit = page_limit(params.limit, 16);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("epoch_from", epoch_from)?);
            }
            if let Some(epoch_to) = params.epoch_to {
                conditions
                    .push("er.epoch <= ")
                    .push_bind_unseparated(to_i64_param("epoch_to", epoch_to)?);
            }
            if let Some(cursor) = params.cursor {
                conditions
                    .push(format!("er.epoch {} ", cursor_op))
                    .push_bind_unseparated(cursor);
            }
        }

        query_builder.push(" GROUP BY er.epoch, er.epoch_start, er.epoch_end");
        query_builder.push(format!(" ORDER BY er.epoch {}", sort_order_sql));
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

    /// Distinct active processors (heartbeat signers) per fixed calendar bucket,
    /// for the quarter and year buckets overlapping `[from, to]`, plus per-bucket
    /// new onboards. Active counts come from `processor_active_bucket`, which is
    /// forward-collected per epoch (see `processor_churn::collect_epoch_active_processors`),
    /// so this is a trivial indexed count — no on-demand scan of the ~250M-row
    /// heartbeat index. The bucket containing `to` (typically the tip) is
    /// naturally partial ("active this quarter/year to date").
    pub async fn get_processor_churn(
        &self,
        params: GetProcessorChurnParams,
    ) -> RpcResult<ProcessorChurnResponse> {
        // Key on the REQUESTED range, not the resolved one: resolving requires a
        // query, and the default (both None) is the case worth caching most --
        // it is what the dashboard sends. A defaulted range therefore keys as
        // "_:_" and follows the tip only as fast as the TTL, which is the agreed
        // staleness tolerance.
        let cache_key = format!(
            "churn:{}:{}",
            params.from.map_or("_".to_string(), |v| v.to_rfc3339()),
            params.to.map_or("_".to_string(), |v| v.to_rfc3339()),
        );
        if let Some(cached) = self.churn_cache.get(&cache_key).await {
            trace!("Cache hit for processor churn: {}", cache_key);
            return Ok(cached);
        }
        let response = self.compute_processor_churn(params).await?;
        self.churn_cache.insert(cache_key, response.clone()).await;
        Ok(response)
    }

    async fn compute_processor_churn(
        &self,
        params: GetProcessorChurnParams,
    ) -> RpcResult<ProcessorChurnResponse> {
        // Default the range to the full indexed span.
        let (min_bt, max_bt): (Option<DateTime<Utc>>, Option<DateTime<Utc>>) = with_timeout(
            self.query_timeout,
            sqlx::query_as("SELECT min(block_time), max(block_time) FROM blocks")
                .fetch_one(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        let (from, to) = match (params.from.or(min_bt), params.to.or(max_bt)) {
            (Some(f), Some(t)) if f <= t => (f, t),
            // empty DB, or nothing indexed / inverted range -> nothing to report
            _ => {
                return Ok(ProcessorChurnResponse {
                    quarters: Vec::new(),
                    years: Vec::new(),
                })
            }
        };

        // Onboard extrinsic (pallet, method) for the per-bucket new-onboards count.
        let (onboard_pallet, onboard_method) = resolve_extrinsic_pallet_method(
            &self.client,
            Some(&StringOrNumber::String(
                "AcurastProcessorManager".to_string(),
            )),
            Some(&StringOrNumber::String("onboard".to_string())),
        )
        .await?;
        let onboard_pallet = onboard_pallet.ok_or_else(|| {
            RpcError::internal_error("failed to resolve AcurastProcessorManager pallet")
        })? as i32;
        let onboard_method = onboard_method
            .ok_or_else(|| RpcError::internal_error("failed to resolve onboard method"))?
            as i32;

        // ONE scan for both calendar grains; see `onboarded_by_bucket`.
        let (onboarded_quarters, onboarded_years) = self
            .onboarded_by_bucket(from, to, onboard_pallet, onboard_method)
            .await?;

        let quarters = self
            .churn_buckets(
                crate::processor_churn::BUCKET_QUARTER,
                3,
                from,
                to,
                &onboarded_quarters,
            )
            .await?;
        let years = self
            .churn_buckets(
                crate::processor_churn::BUCKET_YEAR,
                12,
                from,
                to,
                &onboarded_years,
            )
            .await?;

        Ok(ProcessorChurnResponse { quarters, years })
    }

    /// New-onboard counts per calendar quarter AND per calendar year, from a
    /// single scan.
    ///
    /// `count(DISTINCT account_id)` is not summable across buckets -- an account
    /// that onboarded in Q1 and again in Q2 is ONE distinct account for the year
    /// but TWO across the quarters -- so the year figures cannot be derived from
    /// the quarter figures in Rust. They can, however, come out of the same scan
    /// via `GROUPING SETS`, which is why this replaced a per-grain query: the
    /// aggregate used to run twice per request over identical rows.
    ///
    /// Crucially, the inner scan carries **no `block_time` predicate**. The range
    /// is applied to the already-truncated bucket keys in the outer query
    /// instead. That is not a stylistic choice -- it is the difference between
    /// sub-second and 31 s, measured on mainnet 2026-08-05.
    ///
    /// With `block_time` in the inner WHERE, the planner produced a `BitmapAnd`
    /// of `extrinsics_pallet_method_account_idx` (115k rows, 19 ms) and
    /// `extrinsics_block_time_idx` -- and because the default range is all of
    /// history, that second bitmap scan walked **314,118,251** index entries and
    /// read 6.7 GB, for 30.7 s of the 31 s total. Dropping the predicate leaves
    /// the planner only the `(pallet, method)` prefix, which is 114,322 rows.
    /// That is what makes this fix stand on its own: the bad plan is only
    /// reachable *because* of the predicate, so removing it defuses
    /// `extrinsics_block_time_idx` without having to drop the index.
    ///
    /// Correctness is unaffected: the old per-grain ranges were bucket-aligned, so
    /// grouping every row and then discarding whole out-of-range buckets yields
    /// exactly the same counts for the buckets that remain.
    ///
    /// The cost of this is scanning all onboard rows even for a narrow requested
    /// range. At 114,322 rows (measured) that is the right trade; revisit if
    /// onboards ever reach the millions.
    ///
    /// Returns `(by_quarter, by_year)` keyed on bucket start.
    async fn onboarded_by_bucket(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        onboard_pallet: i32,
        onboard_method: i32,
    ) -> RpcResult<(
        std::collections::BTreeMap<DateTime<Utc>, i64>,
        std::collections::BTreeMap<DateTime<Utc>, i64>,
    )> {
        // `date_trunc` units and intervals are compile-time constants here, never
        // user input. Year rows come back with `q IS NULL` and quarter rows with
        // `y IS NULL`, which is how the two grouping sets are told apart.
        const SQL: &str = "\
            SELECT g.y, g.q, g.onboarded FROM ( \
                SELECT date_trunc('year', block_time)    AS y, \
                       date_trunc('quarter', block_time) AS q, \
                       count(DISTINCT account_id)::bigint AS onboarded \
                  FROM extrinsics \
                 WHERE pallet = $1 AND method = $2 \
                 GROUP BY GROUPING SETS ((1), (2)) \
            ) g \
            WHERE (g.q IS NOT NULL \
                   AND g.q >= date_trunc('quarter', $3::timestamptz) \
                   AND g.q <  date_trunc('quarter', $4::timestamptz) + interval '3 months') \
               OR (g.y IS NOT NULL \
                   AND g.y >= date_trunc('year', $3::timestamptz) \
                   AND g.y <  date_trunc('year', $4::timestamptz) + interval '1 year')";

        let rows: Vec<(Option<DateTime<Utc>>, Option<DateTime<Utc>>, i64)> = with_timeout(
            self.query_timeout,
            sqlx::query_as(SQL)
                .bind(onboard_pallet)
                .bind(onboard_method)
                .bind(from)
                .bind(to)
                .fetch_all(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        let mut by_quarter = std::collections::BTreeMap::new();
        let mut by_year = std::collections::BTreeMap::new();
        for (year, quarter, onboarded) in rows {
            match (year, quarter) {
                (_, Some(q)) => {
                    by_quarter.insert(q, onboarded);
                }
                (Some(y), None) => {
                    by_year.insert(y, onboarded);
                }
                (None, None) => {}
            }
        }
        Ok((by_quarter, by_year))
    }

    /// Build the churn buckets of one calendar length overlapping `[from, to]`.
    /// `months` is an internal constant (3 for quarters, 12 for years), never
    /// user input, so deriving the SQL interval from it is safe. Merges the
    /// forward-collected `active` count with the pre-computed per-bucket
    /// `onboarded` counts from [`AppState::onboarded_by_bucket`].
    async fn churn_buckets(
        &self,
        kind: i16,
        months: u32,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        onboarded_by_bucket: &std::collections::BTreeMap<DateTime<Utc>, i64>,
    ) -> RpcResult<Vec<ProcessorChurnBucket>> {
        let interval = if months >= 12 { "1 year" } else { "3 months" };

        // Active: buckets whose [start, start+interval) overlaps [from, to].
        let active_sql = format!(
            "SELECT bucket_start, count(*)::bigint AS active \
             FROM processor_active_bucket \
             WHERE bucket_kind = $1 AND bucket_start <= $3 \
               AND bucket_start + interval '{interval}' > $2 \
             GROUP BY bucket_start"
        );
        let active_rows: Vec<(DateTime<Utc>, i64)> = with_timeout(
            self.query_timeout,
            sqlx::query_as(&active_sql)
                .bind(kind)
                .bind(from)
                .bind(to)
                .fetch_all(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        // Merge by bucket_start (BTreeMap keeps them sorted).
        let mut merged: std::collections::BTreeMap<DateTime<Utc>, (i64, i64)> =
            std::collections::BTreeMap::new();
        for (bs, active) in active_rows {
            merged.entry(bs).or_default().0 = active;
        }
        for (bs, onboarded) in onboarded_by_bucket {
            merged.entry(*bs).or_default().1 = *onboarded;
        }

        let mut out = Vec::with_capacity(merged.len());
        for (bucket_start, (active, onboarded)) in merged {
            let bucket_end = bucket_start
                .checked_add_months(chrono::Months::new(months))
                .ok_or_else(|| RpcError::internal_error("bucket_end overflow"))?;
            out.push(ProcessorChurnBucket {
                bucket_start,
                bucket_end,
                active,
                onboarded,
            });
        }
        Ok(out)
    }

    /// Paged, filterable listing of the `accounts` table, ranked by a balance
    /// dimension (`total`, `total_with_locked`, or `transferable`). Supports
    /// role-flag and attestation-classification filters plus keyset pagination.
    /// With no filters this is the top-N ranking (each dimension has a matching
    /// DESC index); `account_id` is the stable secondary sort.
    pub async fn get_accounts(&self, params: GetAccountsParams) -> RpcResult<Page<TopAccountRow>> {
        let order_expr = params.sort.order_expr();
        let limit = params.limit.unwrap_or(100).clamp(1, 100);

        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT account_id, \
                    free::text AS free, \
                    reserved::text AS reserved, \
                    frozen::text AS frozen, \
                    transferable::text AS transferable, \
                    remaining_vesting::text AS remaining_vesting, \
                    remaining_token_claim::text AS remaining_token_claim, \
                    sort_num::text AS sort_value, \
                    is_processor, is_manager, is_committer, \
                    processor_type, device_type, \
                    block_number, block_time \
             FROM ( \
                SELECT account_id, free, reserved, frozen, transferable, \
                       remaining_vesting, remaining_token_claim, ",
        );
        query_builder.push(order_expr);
        query_builder.push(
            " AS sort_num, \
                       is_processor, is_manager, is_committer, \
                       processor_type, device_type, \
                       block_number, block_time \
                FROM accounts \
                WHERE 1=1",
        );

        if let Some(is_processor) = params.is_processor {
            query_builder.push(" AND is_processor = ");
            query_builder.push_bind(is_processor);
        }
        if let Some(is_manager) = params.is_manager {
            query_builder.push(" AND is_manager = ");
            query_builder.push_bind(is_manager);
        }
        if let Some(is_committer) = params.is_committer {
            query_builder.push(" AND is_committer = ");
            query_builder.push_bind(is_committer);
        }
        if let Some(processor_type) = params.processor_type {
            query_builder.push(" AND processor_type = ");
            query_builder.push_bind(processor_type);
        }
        if let Some(device_type) = params.device_type {
            query_builder.push(" AND device_type = ");
            query_builder.push_bind(device_type);
        }
        if let Some(ref account_id) = params.account_id {
            let normalized = normalize_address_with_prefix(account_id);
            query_builder.push(" AND account_id = ");
            query_builder.push_bind(normalized);
        }
        if let Some(ref exclude_addresses) = params.exclude_addresses {
            let normalized: Vec<String> = exclude_addresses
                .iter()
                .map(|a| normalize_address_with_prefix(a))
                .collect();
            if !normalized.is_empty() {
                query_builder.push(" AND account_id <> ALL(");
                query_builder.push_bind(normalized);
                query_builder.push(")");
            }
        }
        if let Some(ref cursor) = params.cursor {
            let (cursor_val, cursor_account_id) = parse_accounts_cursor(cursor)?;
            // Tuple comparison: (sort_num, account_id) < (cursor_val, cursor_account_id).
            // Always DESC (highest balance first); sort columns are never NULL so
            // no NULLS-LAST branch is needed (unlike get_commitments).
            query_builder.push(" AND ((");
            query_builder.push(order_expr);
            query_builder.push(" < ");
            query_builder.push_bind(cursor_val.clone());
            query_builder.push(") OR (");
            query_builder.push(order_expr);
            query_builder.push(" = ");
            query_builder.push_bind(cursor_val);
            query_builder.push(" AND account_id < ");
            query_builder.push_bind(cursor_account_id);
            query_builder.push("))");
        }

        query_builder.push(" ORDER BY ");
        query_builder.push(order_expr);
        query_builder.push(" DESC, account_id ASC LIMIT ");
        query_builder.push_bind(limit + 1);
        query_builder.push(" ) t ORDER BY sort_num DESC, account_id ASC");

        let query = query_builder.build_query_as::<TopAccountRow>();
        let mut items = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        let has_more = items.len() > limit as usize;
        if has_more {
            items.pop();
        }

        let next_cursor = if has_more {
            items.last().map(
                |l| serde_json::json!({"sort_value": l.sort_value, "account_id": l.account_id}),
            )
        } else {
            None
        };

        Ok(Page::<TopAccountRow> {
            cursor: next_cursor,
            items,
            unfiltered_count: None,
        })
    }

    /// Count accounts matching the same filters as `get_accounts` (sort/cursor/limit
    /// are irrelevant to a count and ignored). Result is cached.
    pub async fn get_accounts_count(&self, params: GetAccountsCountParams) -> RpcResult<i64> {
        let normalized_account_id = params
            .account_id
            .as_ref()
            .map(|a| normalize_address_with_prefix(a));
        let normalized_excludes: Vec<String> = params
            .exclude_addresses
            .as_ref()
            .map(|xs| {
                xs.iter()
                    .map(|a| normalize_address_with_prefix(a))
                    .collect()
            })
            .unwrap_or_default();

        let has_filters = params.is_processor.is_some()
            || params.is_manager.is_some()
            || params.is_committer.is_some()
            || params.processor_type.is_some()
            || params.device_type.is_some()
            || normalized_account_id.is_some()
            || !normalized_excludes.is_empty();

        // Unfiltered count: use the approximate count from pg_class (instant,
        // avoids a full table scan), consistent with the other *_count methods.
        if !has_filters {
            let result: i64 = with_timeout(
                self.query_timeout,
                sqlx::query_scalar(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'accounts'",
                )
                .fetch_one(&self.db_pool),
            )
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            return Ok(result);
        }

        // Deterministic cache key (sort excludes so hits are order-insensitive).
        let mut excludes_key = normalized_excludes.clone();
        excludes_key.sort();
        let cache_key = format!(
            "acc_count:{}:{}:{}:{}:{}:{}:{}",
            params
                .is_processor
                .map_or("_".to_string(), |v| v.to_string()),
            params.is_manager.map_or("_".to_string(), |v| v.to_string()),
            params
                .is_committer
                .map_or("_".to_string(), |v| v.to_string()),
            params.processor_type.as_deref().unwrap_or("_"),
            params.device_type.as_deref().unwrap_or("_"),
            normalized_account_id.as_deref().unwrap_or("_"),
            excludes_key.join(","),
        );

        if let Some(cached) = self.count_cache.get(&cache_key).await {
            trace!("Cache hit for accounts count: {}", cache_key);
            return Ok(cached);
        }

        let mut query_builder =
            QueryBuilder::<Postgres>::new("SELECT count(*) FROM accounts WHERE 1=1");

        if let Some(is_processor) = params.is_processor {
            query_builder.push(" AND is_processor = ");
            query_builder.push_bind(is_processor);
        }
        if let Some(is_manager) = params.is_manager {
            query_builder.push(" AND is_manager = ");
            query_builder.push_bind(is_manager);
        }
        if let Some(is_committer) = params.is_committer {
            query_builder.push(" AND is_committer = ");
            query_builder.push_bind(is_committer);
        }
        if let Some(processor_type) = params.processor_type {
            query_builder.push(" AND processor_type = ");
            query_builder.push_bind(processor_type);
        }
        if let Some(device_type) = params.device_type {
            query_builder.push(" AND device_type = ");
            query_builder.push_bind(device_type);
        }
        if let Some(account_id) = normalized_account_id {
            query_builder.push(" AND account_id = ");
            query_builder.push_bind(account_id);
        }
        if !normalized_excludes.is_empty() {
            query_builder.push(" AND account_id <> ALL(");
            query_builder.push_bind(normalized_excludes);
            query_builder.push(")");
        }

        let query = query_builder.build_query_scalar::<i64>();
        let result = with_timeout(self.query_timeout, query.fetch_one(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
        self.count_cache.insert(cache_key, result).await;
        Ok(result)
    }

    /// Per-epoch network-wide totals time series from the `epoch_totals` rollup:
    /// total remaining vesting (pallet 15), total remaining token-claim
    /// (pallet 55), total committer self-stake and total delegated (pallet 48).
    /// Ordered by epoch descending (most recent first).
    pub async fn get_epoch_totals(
        &self,
        params: GetEpochTotalsParams,
    ) -> RpcResult<Vec<EpochTotalsRow>> {
        let limit = params.limit.unwrap_or(1000).clamp(1, 5000);

        let rows = with_timeout(
            self.query_timeout,
            query_as::<_, EpochTotalsRow>(
                "SELECT epoch, block_number, block_time, \
                        total_vesting::text AS total_vesting, \
                        total_token_claim::text AS total_token_claim, \
                        total_self_staked::text AS total_self_staked, \
                        total_delegated::text AS total_delegated \
                 FROM epoch_totals \
                 WHERE ($1::BIGINT IS NULL OR epoch >= $1) \
                   AND ($2::BIGINT IS NULL OR epoch <= $2) \
                 ORDER BY epoch DESC \
                 LIMIT $3",
            )
            .bind(params.epoch_from)
            .bind(params.epoch_to)
            .bind(limit)
            .fetch_all(&self.db_pool),
        )
        .await
        .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        Ok(rows)
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
                .bind(to_i64_param("block_number", block_number)?)
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
                .bind(to_i64_param("block_number", block_number)?)
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

        // Validate: data filter requires pallet and method for efficient index usage
        if params.data.is_some() && (pallet.is_none() || method.is_none()) {
            return Err(RpcError::invalid_params(
                "data filter requires both pallet and method to be specified",
            ));
        }

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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
            sort_by, sort_order_sql, sort_order_sql
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
        // Resolve legacy single pallet/method and any provided pairs into one
        // unified list of resolved (pallet, method) tuples.
        let mut resolved_pairs: Vec<(Option<u32>, Option<u32>)> = Vec::new();

        let (legacy_pallet, legacy_method) = resolve_extrinsic_pallet_method(
            &self.client,
            params.pallet.as_ref(),
            params.method.as_ref(),
        )
        .await?;
        if legacy_pallet.is_some() || legacy_method.is_some() {
            resolved_pairs.push((legacy_pallet, legacy_method));
        }

        if let Some(pairs) = &params.pairs {
            for pair in pairs {
                let (p, m) = resolve_extrinsic_pallet_method(
                    &self.client,
                    pair.pallet.as_ref(),
                    pair.method.as_ref(),
                )
                .await?;
                if p.is_none() && m.is_none() {
                    continue;
                }
                if p.is_none() {
                    return Err(RpcError::invalid_params(
                        "each pair in `pairs` must specify a pallet",
                    ));
                }
                resolved_pairs.push((p, m));
            }
        }

        let has_pair_filter = !resolved_pairs.is_empty();
        let has_filters = params.block_from.is_some()
            || params.block_to.is_some()
            || has_pair_filter
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

        // Build deterministic cache key (sort pairs so cache hits are order-insensitive)
        let mut pair_key = resolved_pairs.clone();
        pair_key.sort();
        let pairs_str = pair_key
            .iter()
            .map(|(p, m)| {
                format!(
                    "{}.{}",
                    p.map_or("_".to_string(), |v| v.to_string()),
                    m.map_or("_".to_string(), |v| v.to_string())
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let cache_key = format!(
            "ext_count:{}:{}:{}:{}",
            params.block_from.map_or("_".to_string(), |v| v.to_string()),
            params.block_to.map_or("_".to_string(), |v| v.to_string()),
            pairs_str,
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
                .push_bind_unseparated(to_i64_param("block_from", block_from)?);
        }
        if let Some(block_to) = params.block_to {
            conditions
                .push("block_number <= ")
                .push_bind_unseparated(to_i64_param("block_to", block_to)?);
        }
        if resolved_pairs.len() == 1 {
            let (pallet, method) = resolved_pairs[0];
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
        } else if resolved_pairs.len() > 1 {
            // OR-list across pairs; planner uses BitmapOr over the
            // (pallet, method, ...) index.
            conditions.push("(");
            let mut first = true;
            for (pallet, method) in &resolved_pairs {
                if !first {
                    conditions.push_unseparated(" OR ");
                }
                first = false;
                match (pallet, method) {
                    (Some(p), Some(m)) => {
                        conditions.push_unseparated("(pallet = ");
                        conditions.push_bind_unseparated(*p as i32);
                        conditions.push_unseparated(" AND method = ");
                        conditions.push_bind_unseparated(*m as i32);
                        conditions.push_unseparated(")");
                    }
                    (Some(p), None) => {
                        conditions.push_unseparated("pallet = ");
                        conditions.push_bind_unseparated(*p as i32);
                    }
                    _ => unreachable!("pair must have a pallet at this point"),
                }
            }
            conditions.push_unseparated(")");
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

    pub async fn get_events_count(&self, params: GetEventsCountParams) -> RpcResult<i64> {
        // Resolve legacy single pallet/variant and any provided pairs into one
        // unified list of resolved (pallet, variant) tuples.
        let mut resolved_pairs: Vec<(Option<u32>, Option<u32>)> = Vec::new();

        let (legacy_pallet, legacy_variant) = resolve_event_pallet_variant(
            &self.client,
            params.pallet.as_ref(),
            params.variant.as_ref(),
        )
        .await?;
        if legacy_pallet.is_some() || legacy_variant.is_some() {
            resolved_pairs.push((legacy_pallet, legacy_variant));
        }

        if let Some(pairs) = &params.pairs {
            for pair in pairs {
                let (p, v) = resolve_event_pallet_variant(
                    &self.client,
                    pair.pallet.as_ref(),
                    pair.variant.as_ref(),
                )
                .await?;
                if p.is_none() && v.is_none() {
                    continue;
                }
                if p.is_none() {
                    return Err(RpcError::invalid_params(
                        "each pair in `pairs` must specify a pallet",
                    ));
                }
                resolved_pairs.push((p, v));
            }
        }

        let has_pair_filter = !resolved_pairs.is_empty();
        let has_filters = params.block_from.is_some()
            || params.block_to.is_some()
            || has_pair_filter
            || params.source.is_some();

        // Use approximate count from pg_class when no filters (instant, avoids full table scan)
        if !has_filters {
            let result: i64 = with_timeout(
                self.query_timeout,
                sqlx::query_scalar(
                    "SELECT reltuples::bigint FROM pg_class WHERE relname = 'events'",
                )
                .fetch_one(&self.db_pool),
            )
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            return Ok(result);
        }

        let mut pair_key = resolved_pairs.clone();
        pair_key.sort();
        let pairs_str = pair_key
            .iter()
            .map(|(p, v)| {
                format!(
                    "{}.{}",
                    p.map_or("_".to_string(), |v| v.to_string()),
                    v.map_or("_".to_string(), |v| v.to_string())
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let source_str = match params.source {
            Some(EventSourceFilter::Extrinsic) => "ext",
            Some(EventSourceFilter::System) => "sys",
            None => "_",
        };
        let cache_key = format!(
            "evt_count:{}:{}:{}:{}",
            params.block_from.map_or("_".to_string(), |v| v.to_string()),
            params.block_to.map_or("_".to_string(), |v| v.to_string()),
            pairs_str,
            source_str,
        );

        if let Some(cached) = self.count_cache.get(&cache_key).await {
            trace!("Cache hit for events count: {}", cache_key);
            return Ok(cached);
        }

        let mut query_builder = QueryBuilder::<Postgres>::new("SELECT count(*) FROM events");

        query_builder.push(" WHERE ");
        let mut conditions = query_builder.separated(" AND ");
        if let Some(block_from) = params.block_from {
            conditions
                .push("block_number >= ")
                .push_bind_unseparated(to_i64_param("block_from", block_from)?);
        }
        if let Some(block_to) = params.block_to {
            conditions
                .push("block_number <= ")
                .push_bind_unseparated(to_i64_param("block_to", block_to)?);
        }
        if resolved_pairs.len() == 1 {
            let (pallet, variant) = resolved_pairs[0];
            if let Some(pallet) = pallet {
                conditions
                    .push("pallet = ")
                    .push_bind_unseparated(pallet as i32);
            }
            if let Some(variant) = variant {
                conditions
                    .push("variant = ")
                    .push_bind_unseparated(variant as i32);
            }
        } else if resolved_pairs.len() > 1 {
            // OR-list across pairs; planner uses BitmapOr over
            // `events_pallet_variant_idx` (or `events_pallet_idx` for pallet-only pairs).
            conditions.push("(");
            let mut first = true;
            for (pallet, variant) in &resolved_pairs {
                if !first {
                    conditions.push_unseparated(" OR ");
                }
                first = false;
                match (pallet, variant) {
                    (Some(p), Some(v)) => {
                        conditions.push_unseparated("(pallet = ");
                        conditions.push_bind_unseparated(*p as i32);
                        conditions.push_unseparated(" AND variant = ");
                        conditions.push_bind_unseparated(*v as i32);
                        conditions.push_unseparated(")");
                    }
                    (Some(p), None) => {
                        conditions.push_unseparated("pallet = ");
                        conditions.push_bind_unseparated(*p as i32);
                    }
                    _ => unreachable!("pair must have a pallet at this point"),
                }
            }
            conditions.push_unseparated(")");
        }
        if let Some(source) = &params.source {
            match source {
                EventSourceFilter::Extrinsic => {
                    conditions.push("event_phase = 'ApplyExtrinsic'::event_phase_type");
                }
                EventSourceFilter::System => {
                    conditions.push("event_phase IN ('Initialization'::event_phase_type, 'Finalization'::event_phase_type)");
                }
            }
        }

        let query = query_builder.build_query_scalar::<i64>();

        let result = with_timeout(self.query_timeout, query.fetch_one(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

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
                let row = with_timeout(
                    self.query_timeout,
                    sqlx::query!(
                        r#"
                SELECT sv.spec_version, sv.block_number, b.hash as block_hash
                FROM spec_versions sv
                JOIN blocks b ON sv.block_number = b.block_number
                WHERE sv.spec_version = $1
                "#,
                        spec_version
                    )
                    .fetch_optional(&self.db_pool),
                )
                .await
                .map_err(|e| RpcError::database(format!("Database error: {}", e)))?
                .ok_or_else(|| {
                    RpcError::invalid_params(format!("Spec version {} not found", spec_version))
                })?;
                (row.spec_version, row.block_number, row.block_hash)
            } else {
                // Query by block_number - find the closest spec_version <= block_number
                let block_number = params.block_number.unwrap();
                let row = with_timeout(
                    self.query_timeout,
                    sqlx::query!(
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
                    .fetch_optional(&self.db_pool),
                )
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

        // Metadata is immutable per spec_version; serve from cache when possible
        // to avoid a live archive-node call (hundreds of KB-MB) per request.
        if let Some(cached) = self.metadata_cache.get(&spec_version).await {
            return Ok(cached);
        }

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
                let response = serde_json::json!({
                    "spec_version": spec_version,
                    "block_number": block_number,
                    "block_hash": block_hash,
                    "metadata": metadata_hex
                });
                self.metadata_cache
                    .insert(spec_version, response.clone())
                    .await;
                Ok(response)
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

        let mut query_builder = QueryBuilder::<Postgres>::new("SELECT block_number, extrinsic_index, batch_index, data_path, resolved_data_path, account_id, pallet, method, block_time FROM extrinsic_address");

        // Determine sort order first for cursor comparison
        let sort_by = "block_number";
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
            sort_by, sort_order_sql, sort_order_sql
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
                r#"SELECT block_number, extrinsic_index, index, pallet, variant, data, phase, event_phase, error, block_time FROM events WHERE block_number = $1 AND index = $2 LIMIT 1"#,
            )
            .bind(params.block_number)
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

        // Validate: data filter requires pallet and variant for efficient index usage
        if params.data.is_some() && (pallet.is_none() || variant.is_none()) {
            return Err(RpcError::invalid_params(
                "data filter requires both pallet and variant to be specified",
            ));
        }

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
                "SELECT e.block_number, e.extrinsic_index, e.index, e.pallet, e.variant, e.data, e.phase, e.event_phase, e.error, e.block_time \
                 FROM jobs j \
                 INNER JOIN events e ON j.block_number = e.block_number AND j.extrinsic_index = e.extrinsic_index AND j.event_index = e.index"
            )
        } else if needs_extrinsic_join {
            QueryBuilder::<Postgres>::new(
                "SELECT e.block_number, e.extrinsic_index, e.index, e.pallet, e.variant, e.data, e.phase, e.event_phase, e.error, e.block_time \
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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

        if params.cursor.is_some()
            || params.block_from.is_some()
            || params.block_to.is_some()
            || pallet.is_some()
            || variant.is_some()
            || params.account_id.is_some()
            || params.data.is_some()
            || job_filter.is_some()
            || params.source.is_some()
        {
            query_builder.push(" WHERE ");
            let mut conditions = query_builder.separated(" AND ");

            // Determine column prefix based on whether we're using a JOIN
            let col_prefix = if needs_join { "e." } else { "" };

            if let Some(cursor) = &params.cursor {
                conditions.push(format!(
                    "({}block_number, {}index) {} (",
                    col_prefix, col_prefix, cursor_op
                ));
                conditions.push_bind_unseparated(cursor.block_number);
                conditions.push_unseparated(", ");
                conditions.push_bind_unseparated(cursor.index);
                conditions.push_unseparated(")");
            }
            if let Some(block_from) = params.block_from {
                conditions
                    .push(format!("{}block_number >= ", col_prefix))
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push(format!("{}block_number <= ", col_prefix))
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
            // Add source filter (event phase)
            if let Some(source) = &params.source {
                match source {
                    EventSourceFilter::Extrinsic => {
                        // ApplyExtrinsic phase
                        conditions.push(format!(
                            "{}event_phase = 'ApplyExtrinsic'::event_phase_type",
                            col_prefix
                        ));
                    }
                    EventSourceFilter::System => {
                        // Initialization or Finalization phase
                        conditions.push(format!(
                            "{}event_phase IN ('Initialization'::event_phase_type, 'Finalization'::event_phase_type)",
                            col_prefix
                        ));
                    }
                }
            }
        }

        let order_prefix = if needs_join { "e." } else { "" };
        query_builder.push(format!(
            " ORDER BY {} {}, {}block_number {}, {}index {}",
            sort_by, sort_order_sql, order_prefix, sort_order_sql, order_prefix, sort_order_sql
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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };
        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

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
                    .push_bind_unseparated(to_i64_param("block_from", block_from)?);
            }
            if let Some(block_to) = params.block_to {
                conditions
                    .push("block_number <= ")
                    .push_bind_unseparated(to_i64_param("block_to", block_to)?);
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
            sort_by, sort_order_sql, sort_order_sql
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

    /// Resolve a time-range filter (`time_from` / `time_to`) into a
    /// block_number range using two cheap lookups against
    /// `blocks_block_time_idx`. Combined with `params.block_from` / `block_to`
    /// (intersection), this gives the effective block_number bounds for the
    /// snapshot query.
    ///
    /// Filtering snapshots by `block_number` (rather than `block_time`) lets
    /// the planner satisfy the time-window predicate using the partial
    /// expression indexes that already have `block_number DESC` as their
    /// secondary key — a single Index Scan instead of a BitmapAnd that also
    /// pulls in `storage_snapshots_block_time_idx` and pays bitmap-construction
    /// cost proportional to the time window's row count.
    async fn resolve_block_bounds_from_time(
        &self,
        time_from: Option<DateTime<Utc>>,
        time_to: Option<DateTime<Utc>>,
    ) -> RpcResult<(Option<i64>, Option<i64>)> {
        let from = if let Some(tf) = time_from {
            let row = sqlx::query!(
                "SELECT block_number FROM blocks WHERE block_time >= $1 ORDER BY block_time ASC LIMIT 1",
                tf
            )
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            // No block satisfies block_time >= time_from: time_from is past
            // the head. Use i64::MAX so `block_number >= MAX` matches nothing.
            Some(row.map(|r| r.block_number).unwrap_or(i64::MAX))
        } else {
            None
        };

        let to = if let Some(tt) = time_to {
            let row = sqlx::query!(
                "SELECT block_number FROM blocks WHERE block_time <= $1 ORDER BY block_time DESC LIMIT 1",
                tt
            )
            .fetch_optional(&self.db_pool)
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;
            // No block satisfies block_time <= time_to: time_to is before
            // the tail. Use -1 so `block_number <= -1` matches nothing.
            Some(row.map(|r| r.block_number).unwrap_or(-1))
        } else {
            None
        };

        Ok((from, to))
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

        // Convert the time range to a block_number range so the partial
        // expression indexes (keyed on (storage_keys->>0, block_number DESC))
        // can satisfy the window in a single Index Scan; intersect with any
        // explicit block_from / block_to.
        let (time_block_from, time_block_to) = self
            .resolve_block_bounds_from_time(time_from, time_to)
            .await?;
        let block_from = match (
            params
                .block_from
                .map(|x| to_i64_param("block_from", x))
                .transpose()?,
            time_block_from,
        ) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        let block_to = match (
            params
                .block_to
                .map(|x| to_i64_param("block_to", x))
                .transpose()?,
            time_block_to,
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
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

        // Validate: data filter requires pallet and storage_location for efficient index usage
        if params.data.is_some()
            && (storage_pallet_idx.is_none() || params.storage_location.is_none())
        {
            return Err(RpcError::invalid_params(
                "data filter requires both pallet and storage_location to be specified",
            ));
        }

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
        let limit = page_limit(params.limit, 10);
        let cursor_op = if sort_order.eq_ignore_ascii_case("desc") {
            "<"
        } else {
            ">"
        };

        // Canonical ASC/DESC literal for inlining into ORDER BY clauses (the
        // raw `params.sort_order` is unvalidated user input and must not be
        // formatted into SQL directly).
        let sort_order_sql = if cursor_op == "<" { "DESC" } else { "ASC" };

        // Build the query based on whether we need epochs/sampling
        if is_sampling {
            // Sampling: instead of "scan every snapshot in [block_from, block_to]
            // for this filter, then DISTINCT ON (epoch_bucket)" — which has to
            // read O(matching_snapshots) rows just to keep one per bucket — we
            // pre-compute bucket boundaries from the (small) `epochs` table and
            // pull one snapshot per bucket via a `CROSS JOIN LATERAL` that hits
            // the partial index `(storage_keys->>0, block_number DESC)` with a
            // `LIMIT 1`. Total rows fetched from `storage_snapshots` is
            // O(buckets), not O(account_snapshots_in_window).
            let sample_unit = params.sample.unwrap();
            let epochs_per_sample = sample_unit.epochs_per_sample();

            let bucket_cursor = if let Some(cursor_value) = &params.cursor {
                Some(cursor_value.as_i64().ok_or_else(|| {
                    RpcError::invalid_params(
                        "cursor for `sample` queries must be a number (epoch_bucket)",
                    )
                })?)
            } else {
                None
            };

            // Bucket pre-computation: group epochs by `(epoch / N) * N` and
            // record the [bucket_start_block, bucket_end_block) range. Use a
            // sentinel bigint MAX for the open-ended last epoch so the LATERAL
            // upper bound stays well-defined.
            let mut query_builder = QueryBuilder::<Postgres>::new(format!(
                "WITH epochs_with_end AS (
                    SELECT epoch, epoch_start,
                           LEAD(epoch_start) OVER (ORDER BY epoch) AS epoch_end_block,
                           epoch_start_time
                    FROM epochs
                ),
                buckets AS (
                    SELECT (epoch / {n}) * {n} AS epoch_bucket,
                           MIN(epoch_start) AS bucket_start_block,
                           MAX(COALESCE(epoch_end_block, 9223372036854775807)) AS bucket_end_block
                    FROM epochs_with_end",
                n = epochs_per_sample
            ));

            // Restrict the bucket pool to epochs whose block range intersects
            // the user's [block_from, block_to]. Cheap (epochs is small) and
            // avoids GROUP BY on the entire history.
            let mut bucket_where_started = false;
            if let Some(bt) = block_to {
                query_builder.push(" WHERE TRUE");
                bucket_where_started = true;
                query_builder.push(" AND epoch_start <= ").push_bind(bt);
            }
            if let Some(bf) = block_from {
                if !bucket_where_started {
                    query_builder.push(" WHERE TRUE");
                    bucket_where_started = true;
                }
                query_builder
                    .push(" AND (epoch_end_block IS NULL OR epoch_end_block > ")
                    .push_bind(bf)
                    .push(")");
            }
            let _ = bucket_where_started;

            query_builder.push(format!(
                " GROUP BY (epoch / {n}) * {n}",
                n = epochs_per_sample
            ));

            if let Some(cursor) = bucket_cursor {
                query_builder.push(format!(
                    " HAVING (epoch / {n}) * {n} {} ",
                    cursor_op,
                    n = epochs_per_sample
                ));
                query_builder.push_bind(cursor);
            }

            // Limit at the bucket level so the LATERAL only fires for buckets
            // we actually need. limit+1 lets us detect has_more.
            query_builder.push(format!(" ORDER BY epoch_bucket {} LIMIT ", sort_order_sql));
            query_builder.push_bind(limit + 1);

            // Outer SELECT: one row per (kept) bucket, with the LATERAL-picked
            // snapshot inlined. `LEFT JOIN epochs_with_end` re-attaches the
            // epoch info for the chosen snapshot so the response shape matches
            // the non-LATERAL branch.
            query_builder.push(
                ")
                SELECT s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                       s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                       s.epoch_end,
                       ep.epoch, ep.epoch_start, ep.epoch_end_block, ep.epoch_start_time
                FROM buckets b
                CROSS JOIN LATERAL (
                    SELECT s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                           s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                           s.epoch_end
                    FROM storage_snapshots s",
            );

            if has_extrinsic_filter {
                query_builder.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
            }
            if has_event_filter {
                query_builder.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
            }

            // Per-bucket bounds are mandatory; user filters are appended.
            // Passing `block_from`/`block_to` here is fine — they intersect with
            // the per-bucket bounds (the planner uses whichever is tighter).
            query_builder.push(
                " WHERE s.block_number >= b.bucket_start_block AND s.block_number < b.bucket_end_block",
            );
            self.append_storage_snapshot_conditions(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                block_from,
                block_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                None, // bucket-level cursor handled at the buckets CTE above
                cursor_op,
            )?;

            // Top-1 within the bucket. The partial index serves this directly:
            // (storage_keys->>0 = $1, block_number DESC) → first index entry
            // is the row we want.
            query_builder.push(
                " ORDER BY s.block_number DESC LIMIT 1) s
                LEFT JOIN epochs_with_end ep ON ep.epoch_start <= s.block_number
                    AND (ep.epoch_end_block IS NULL OR ep.epoch_end_block > s.block_number)",
            );
            query_builder.push(format!(" ORDER BY b.epoch_bucket {}", sort_order_sql));

            let query = query_builder.build_query_as::<StorageSnapshotDbRow>();
            let db_rows: Vec<StorageSnapshotDbRow> =
                with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
                    .await
                    .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

            // The buckets CTE LIMITs at limit+1, so >limit non-empty rows
            // means more buckets follow. Empty buckets (no snapshot match the
            // filters in that range) are dropped by CROSS JOIN LATERAL — in
            // that edge case has_more is conservatively false, but the cursor
            // still advances past the last returned bucket so pagination
            // remains consistent.
            let has_more = db_rows.len() > limit as usize;
            let mut items: Vec<StorageSnapshotRow> =
                db_rows.into_iter().map(|r| r.into()).collect();
            if has_more {
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
            // Include epochs but no sampling.
            //
            // Same dedup approach as the simple branch below — DISTINCT ON inside a
            // subquery with id direction opposite to user sort_order, then JOIN
            // epochs_with_end on the deduped result.
            let inner_id_dir = if sort_order.eq_ignore_ascii_case("desc") {
                "ASC"
            } else {
                "DESC"
            };
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "WITH epochs_with_end AS (
                    SELECT epoch, epoch_start,
                           LEAD(epoch_start) OVER (ORDER BY epoch) as epoch_end_block,
                           epoch_start_time
                    FROM epochs
                )
                SELECT d.id, d.block_number, d.extrinsic_index, d.event_index, d.block_time,
                       d.pallet, d.storage_location, d.storage_keys, d.data, d.config_rule,
                       d.epoch_end,
                       ep.epoch, ep.epoch_start, ep.epoch_end_block, ep.epoch_start_time
                FROM (
                    SELECT DISTINCT ON (s.block_number, s.pallet, s.storage_location, s.storage_keys)
                           s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                           s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                           s.epoch_end
                    FROM storage_snapshots s",
            );

            // Join with extrinsics if we have extrinsic filters (inside dedup subquery)
            if has_extrinsic_filter {
                query_builder.push(" INNER JOIN extrinsics e ON s.block_number = e.block_number AND s.extrinsic_index = e.index");
            }

            // Join with events if we have event filters (inside dedup subquery)
            if has_event_filter {
                query_builder.push(" INNER JOIN events ev ON s.block_number = ev.block_number AND s.extrinsic_index = ev.extrinsic_index AND s.event_index = ev.index");
            }

            let compound_cursor = match &params.cursor {
                Some(c) => Some(parse_snapshot_cursor(c)?),
                None => None,
            };

            // Build WHERE clause (inside dedup subquery)
            self.build_storage_snapshot_where_clause(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                block_from,
                block_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                compound_cursor,
                cursor_op,
            )?;

            // Close dedup subquery, then LEFT JOIN epoch info on the deduped rows.
            // Inner ORDER BY leads with (pallet, storage_location, storage_keys,
            // block_number DESC, id) to match storage_snapshots_pallet_loc_keys_block_id_idx,
            // letting the planner skip the Sort node in front of the Unique.
            // DISTINCT ON keys are unchanged; only the ordering of equality-grouped
            // columns is permuted, which doesn't affect semantics.
            query_builder.push(format!(
                " ORDER BY s.pallet, s.storage_location, s.storage_keys, s.block_number DESC, s.id {}) d
                 LEFT JOIN epochs_with_end ep ON ep.epoch_start <= d.block_number
                     AND (ep.epoch_end_block IS NULL OR ep.epoch_end_block > d.block_number)
                 ORDER BY d.block_number {}, d.id {}",
                inner_id_dir, sort_order_sql, sort_order_sql
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
                    items
                        .last()
                        .map(|l| serde_json::json!({"block_number": l.block_number, "id": l.id}))
                } else {
                    None
                },
                items,
                unfiltered_count: None,
            })
        } else {
            // Simple query without epochs.
            //
            // Storage_snapshots can hold multiple rows for the same
            // (block_number, pallet, storage_location, storage_keys) when they differ
            // only in epoch_index (the UNIQUE constraint includes epoch_index).
            // We dedup at the query level via DISTINCT ON. The inner ORDER BY's
            // direction on `s.id` is the *opposite* of the user's sort_order so the
            // chosen row's id sits at the page boundary — that lets the existing
            // tuple-compare cursor `(s.block_number, s.id) <op> (...)` correctly
            // exclude same-group rows on subsequent pages.
            let inner_id_dir = if sort_order.eq_ignore_ascii_case("desc") {
                "ASC"
            } else {
                "DESC"
            };
            let mut query_builder = QueryBuilder::<Postgres>::new(
                "SELECT d.id, d.block_number, d.extrinsic_index, d.event_index, d.block_time,
                        d.pallet, d.storage_location, d.storage_keys, d.data, d.config_rule,
                        d.epoch_end,
                        NULL::bigint as epoch, NULL::bigint as epoch_start,
                        NULL::bigint as epoch_end_block, NULL::timestamptz as epoch_start_time
                 FROM (
                     SELECT DISTINCT ON (s.block_number, s.pallet, s.storage_location, s.storage_keys)
                            s.id, s.block_number, s.extrinsic_index, s.event_index, s.block_time,
                            s.pallet, s.storage_location, s.storage_keys, s.data, s.config_rule,
                            s.epoch_end
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

            let compound_cursor = match &params.cursor {
                Some(c) => Some(parse_snapshot_cursor(c)?),
                None => None,
            };

            // Build WHERE clause (applied inside the dedup subquery)
            self.build_storage_snapshot_where_clause(
                &mut query_builder,
                &params,
                storage_pallet_idx,
                block_from,
                block_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                compound_cursor,
                cursor_op,
            )?;

            // Inner ORDER BY leads with (pallet, storage_location, storage_keys,
            // block_number DESC, id) to match storage_snapshots_pallet_loc_keys_block_id_idx,
            // letting the planner skip the Sort node in front of the Unique.
            // DISTINCT ON keys are unchanged; only the ordering of equality-grouped
            // columns is permuted, which doesn't affect semantics. Trailing s.id <dir>
            // chooses MIN id (desc sort) or MAX id (asc sort) per group.
            query_builder.push(format!(
                " ORDER BY s.pallet, s.storage_location, s.storage_keys, s.block_number DESC, s.id {}) AS d
                 ORDER BY d.block_number {}, d.id {}",
                inner_id_dir, sort_order_sql, sort_order_sql
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
                    items
                        .last()
                        .map(|l| serde_json::json!({"block_number": l.block_number, "id": l.id}))
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
        block_from: Option<i64>,
        block_to: Option<i64>,
        ext_pallet: Option<u32>,
        ext_method: Option<u32>,
        ext_account_id: Option<&'a String>,
        evt_pallet: Option<u32>,
        evt_variant: Option<u32>,
        cursor: Option<(i64, i64)>,
        cursor_op: &str,
    ) -> RpcResult<()> {
        let has_extrinsic_filter =
            ext_pallet.is_some() || ext_method.is_some() || ext_account_id.is_some();
        let has_event_filter = evt_pallet.is_some() || evt_variant.is_some();

        let has_conditions = cursor.is_some()
            || block_from.is_some()
            || block_to.is_some()
            || storage_pallet_idx.is_some()
            || params.storage_location.is_some()
            || params.storage_keys.is_some()
            || params.data.is_some()
            || params.config_rule.is_some()
            || params.epoch_index.is_some()
            || params.epoch_end.is_some()
            || has_extrinsic_filter
            || has_event_filter
            || params.exclude_deleted;

        if has_conditions {
            // Emit `WHERE TRUE` then have the helper append each condition
            // prefixed with ` AND `. The trailing `TRUE AND ...` is folded by
            // the planner. Sharing the helper with the LATERAL sampling branch
            // (which prefixes its own `WHERE <bucket bounds>`) avoids
            // duplicating ~150 lines of filter logic.
            query_builder.push(" WHERE TRUE");
            self.append_storage_snapshot_conditions(
                query_builder,
                params,
                storage_pallet_idx,
                block_from,
                block_to,
                ext_pallet,
                ext_method,
                ext_account_id,
                evt_pallet,
                evt_variant,
                cursor,
                cursor_op,
            )?;
        }
        Ok(())
    }

    /// Append every active storage-snapshot filter to `query_builder` as
    /// ` AND <condition>`. The caller is responsible for emitting the leading
    /// `WHERE` (and any caller-specific conditions before this) so the helper
    /// can be used both standalone and inside a LATERAL whose WHERE already
    /// has per-bucket bounds.
    #[allow(clippy::too_many_arguments)]
    fn append_storage_snapshot_conditions<'a>(
        &self,
        qb: &mut QueryBuilder<'a, Postgres>,
        params: &'a GetStorageSnapshotsParams,
        storage_pallet_idx: Option<u32>,
        block_from: Option<i64>,
        block_to: Option<i64>,
        ext_pallet: Option<u32>,
        ext_method: Option<u32>,
        ext_account_id: Option<&'a String>,
        evt_pallet: Option<u32>,
        evt_variant: Option<u32>,
        cursor: Option<(i64, i64)>,
        cursor_op: &str,
    ) -> RpcResult<()> {
        if let Some((cursor_block, cursor_id)) = cursor {
            qb.push(format!(" AND (s.block_number, s.id) {} (", cursor_op))
                .push_bind(cursor_block)
                .push(", ")
                .push_bind(cursor_id)
                .push(")");
        }
        if let Some(block_from) = block_from {
            qb.push(" AND s.block_number >= ").push_bind(block_from);
        }
        if let Some(block_to) = block_to {
            qb.push(" AND s.block_number <= ").push_bind(block_to);
        }
        // Inline `pallet` and `storage_location` as SQL literals (rather than
        // bind parameters) so the planner can match the partial expression
        // indexes such as `storage_snapshots_system_account_key_idx`, whose
        // predicate is `WHERE pallet = N AND storage_location = '<name>'`.
        // Under sqlx's prepared statements PG flips to a generic plan after
        // ~5 executions; a generic plan cannot prove that a bound parameter
        // equals the index's literal, so the partial index is not used and
        // the query falls back to a wide bitmap scan.
        if let Some(pallet) = storage_pallet_idx {
            qb.push(format!(" AND s.pallet = {}", pallet as i32));
        }
        if let Some(storage_location) = &params.storage_location {
            if !is_valid_storage_location(storage_location) {
                return Err(RpcError::invalid_params(format!(
                    "invalid storage_location: {}",
                    storage_location
                )));
            }
            qb.push(format!(" AND s.storage_location = '{}'", storage_location));
        }
        if let Some(storage_keys) = &params.storage_keys {
            // Positional filter: each array position maps to a storage_keys->>N
            // equality (or storage_keys->N->>0 for nested keys). Null positions
            // are skipped. This always matches the expression indexes on
            // storage_keys when pallet+storage_location are specified.
            let arr = storage_keys.as_array().ok_or_else(|| {
                RpcError::invalid_params(
                    "storage_keys must be a JSON array (e.g. [\"x\"] or [null, \"y\"] or [[\"nested\"]])",
                )
            })?;

            for (idx, element) in arr.iter().enumerate() {
                if element.is_null() {
                    continue;
                }

                if let Some(text) = primitive_as_text(element) {
                    qb.push(format!(" AND s.storage_keys->>{} = ", idx))
                        .push_bind(text);
                    continue;
                }

                if let Some(inner) = element.as_array() {
                    if inner.len() == 1 {
                        if let Some(text) = primitive_as_text(&inner[0]) {
                            qb.push(format!(" AND s.storage_keys->{}->>0 = ", idx))
                                .push_bind(text);
                            continue;
                        }
                    }
                    return Err(RpcError::invalid_params(format!(
                        "storage_keys[{}]: nested key must be a single-element array of string|number|bool",
                        idx
                    )));
                }

                return Err(RpcError::invalid_params(format!(
                    "storage_keys[{}]: must be null, a primitive (string|number|bool), or a single-element nested array",
                    idx
                )));
            }
        }
        if let Some(data) = &params.data {
            qb.push(" AND s.data @> ").push_bind(data);
        }
        if let Some(config_rule) = &params.config_rule {
            qb.push(" AND s.config_rule = ").push_bind(config_rule);
        }
        if let Some(epoch_index) = params.epoch_index {
            qb.push(" AND s.epoch_index = ").push_bind(epoch_index);
        }
        if let Some(epoch_end) = params.epoch_end {
            qb.push(" AND s.epoch_end = ").push_bind(epoch_end);
        }
        if let Some(pallet) = ext_pallet {
            qb.push(format!(" AND e.pallet = {}", pallet as i32));
        }
        if let Some(method) = ext_method {
            qb.push(format!(" AND e.method = {}", method as i32));
        }
        if let Some(account_id) = ext_account_id {
            qb.push(" AND e.account_id = ")
                .push_bind(normalize_address(account_id));
        }
        if let Some(pallet) = evt_pallet {
            qb.push(format!(" AND ev.pallet = {}", pallet as i32));
        }
        if let Some(variant) = evt_variant {
            qb.push(format!(" AND ev.variant = {}", variant as i32));
        }
        if params.exclude_deleted {
            qb.push(
                " AND NOT EXISTS (
                    SELECT 1 FROM storage_snapshots s2
                    WHERE s2.pallet = s.pallet
                    AND s2.storage_location = s.storage_location
                    AND s2.storage_keys = s.storage_keys
                    AND s2.block_number > s.block_number
                    AND s2.data = 'null'::jsonb
                )",
            );
        }
        Ok(())
    }

    /// Get epoch metrics for a manager.
    /// Returns processor metrics grouped by epoch.
    pub async fn get_metrics_by_manager(
        &self,
        params: GetEpochMetricsParams,
    ) -> RpcResult<Page<EpochMetricsItem>> {
        use crate::entities::EpochIndexPhase;

        let limit = page_limit(params.limit, 16);

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

        let limit = page_limit(params.limit, 16);

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
        let limit = page_limit(params.limit, 50);

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
                        "cooldown_period" => serde_json::json!(l.cooldown_period),
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

    pub async fn get_base_rewards(
        &self,
        params: GetBaseRewardsParams,
    ) -> RpcResult<Page<BaseRewardItem>> {
        let limit = page_limit(params.limit, 50);

        if params.manager.is_empty() {
            Err(RpcError::invalid_params("Manager cannot be empty"))?;
        }
        // events.data->>0 stores addresses with 0x prefix; extrinsics.account_id without.
        let manager_address = normalize_address_with_prefix(&params.manager);
        let processor_address = params.processor.as_deref().map(normalize_address);
        let cursor_processor = params.cursor_processor.as_deref().map(normalize_address);

        let (heartbeat_pallet, heartbeat_method) = resolve_extrinsic_pallet_method(
            &self.client,
            Some(&StringOrNumber::String(
                "AcurastProcessorManager".to_string(),
            )),
            Some(&StringOrNumber::String(
                "heartbeat_with_metrics".to_string(),
            )),
        )
        .await?;
        let heartbeat_pallet = heartbeat_pallet.unwrap() as i32;
        let heartbeat_method = heartbeat_method.unwrap() as i32;

        let (deposit_pallet, deposit_variant) = resolve_event_pallet_variant(
            &self.client,
            Some(&StringOrNumber::String("Balances".to_string())),
            Some(&StringOrNumber::String("Deposit".to_string())),
        )
        .await?;
        let deposit_pallet = deposit_pallet.unwrap() as i32;
        let deposit_variant = deposit_variant.unwrap() as i32;

        // One row per (epoch, processor): SUM of all heartbeat deposits within that epoch.
        // A processor typically heartbeats 2-3 times per epoch; aggregating gives the true
        // total base reward per processor per epoch.
        // Sorted (epoch DESC, processor ASC) for stable cursor-based pagination.
        let mut query_builder = QueryBuilder::<Postgres>::new(
            "SELECT '0x' || ext.account_id AS processor, ep.epoch, SUM((e.data->>1)::numeric)::text AS amount
            FROM events e
            INNER JOIN extrinsics ext
                ON ext.block_number = e.block_number AND ext.index = e.extrinsic_index
            INNER JOIN LATERAL (
                SELECT epoch FROM epochs
                WHERE epoch_start <= e.block_number
                ORDER BY epoch_start DESC LIMIT 1
            ) ep ON true
            WHERE e.pallet = ",
        );
        query_builder.push_bind(deposit_pallet);
        query_builder.push(" AND e.variant = ");
        query_builder.push_bind(deposit_variant);
        query_builder.push(" AND ext.pallet = ");
        query_builder.push_bind(heartbeat_pallet);
        query_builder.push(" AND ext.method = ");
        query_builder.push_bind(heartbeat_method);
        query_builder.push(" AND e.data->>0 = ");
        query_builder.push_bind(&manager_address);

        if let Some(ref proc) = processor_address {
            query_builder.push(" AND ext.account_id = ");
            query_builder.push_bind(proc);
        }
        if let Some(from) = params.epoch_from {
            query_builder.push(" AND ep.epoch >= ");
            query_builder.push_bind(from);
        }
        if let Some(to) = params.epoch_to {
            query_builder.push(" AND ep.epoch <= ");
            query_builder.push_bind(to);
        }

        // Composite cursor: resume after (cursor_epoch, cursor_processor) in (epoch DESC, processor ASC) order.
        if let Some(c_epoch) = params.cursor_epoch {
            if let Some(c_proc) = cursor_processor {
                query_builder.push(" AND (ep.epoch < ");
                query_builder.push_bind(c_epoch);
                query_builder.push(" OR (ep.epoch = ");
                query_builder.push_bind(c_epoch);
                query_builder.push(" AND ext.account_id > ");
                query_builder.push_bind(c_proc);
                query_builder.push("))");
            } else {
                query_builder.push(" AND ep.epoch <= ");
                query_builder.push_bind(c_epoch);
            }
        }

        query_builder.push(
            " GROUP BY ep.epoch, ext.account_id ORDER BY ep.epoch DESC, ext.account_id ASC LIMIT ",
        );
        query_builder.push_bind(limit + 1);

        #[derive(sqlx::FromRow)]
        struct Row {
            processor: String,
            epoch: i64,
            amount: Option<String>,
        }

        let query = query_builder.build_query_as::<Row>();
        let mut rows = with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
            .await
            .map_err(|e| RpcError::database(format!("Database error: {}", e)))?;

        let has_more = rows.len() > limit as usize;
        if has_more {
            rows.pop();
        }

        let items: Vec<BaseRewardItem> = rows
            .into_iter()
            .map(|r| BaseRewardItem {
                processor: r.processor,
                epoch: r.epoch,
                amount: r.amount.unwrap_or_default(),
            })
            .collect();

        Ok(Page {
            cursor: if has_more {
                items.last().map(|i| {
                    serde_json::json!({
                        "epoch": i.epoch,
                        "processor": i.processor,
                    })
                })
            } else {
                None
            },
            items,
            unfiltered_count: None,
        })
    }

    /// Get deployments with optional filtering and sorting
    pub async fn get_deployments(
        &self,
        params: GetDeploymentsParams,
    ) -> RpcResult<Page<DeploymentRow>> {
        let order_by = params.order_by.as_deref().unwrap_or("block_number");
        let sort_order = params.sort_order.as_deref().unwrap_or("desc");
        let limit = page_limit(params.limit, 50);

        // Validate order_by column
        let valid_columns = ["block_number", "created_block_number", "start_time"];
        if !valid_columns.contains(&order_by) {
            return Err(RpcError::invalid_params(format!(
                "Invalid order_by column: {}. Valid columns: {:?}",
                order_by, valid_columns
            )));
        }

        // Map order_by to actual SQL column
        let order_by_col = match order_by {
            "start_time" => "schedule_start_time",
            col => col,
        };

        // The related_extrinsics subquery is expensive (correlated jsonb_agg per row);
        // only build it when the caller explicitly asks for it.
        let include_related = params.related_extrinsics.unwrap_or(false);
        let related_select = if include_related {
            r#"COALESCE(
                   (SELECT jsonb_agg(jsonb_build_object(
                       'block_number', e.block_number,
                       'index', e.index,
                       'pallet', e.pallet,
                       'method', e.method,
                       'tx_hash', e.tx_hash,
                       'account_id', e.account_id,
                       'block_time', e.block_time
                   ) ORDER BY e.block_number ASC, e.index ASC)
                   FROM jobs j
                   JOIN extrinsics e ON e.block_number = j.block_number AND e.index = j.extrinsic_index
                   WHERE j.chain = d.chain
                     AND j.address = d.address
                     AND j.seq_id = d.seq_id),
                   '[]'::jsonb
               ) as related_extrinsics"#
        } else {
            "NULL::jsonb as related_extrinsics"
        };

        let mut query_builder = QueryBuilder::<Postgres>::new(format!(
            r#"SELECT d.id, d.chain::TEXT, d.address, d.seq_id,
                      d.snapshot_id, d.block_number, d.block_time,
                      d.created_block_number, d.created_block_time,
                      d.schedule_duration, d.schedule_start_time, d.schedule_end_time,
                      d.schedule_interval, d.schedule_max_start_delay,
                      d.allowed_sources, d.allow_only_verified_sources,
                      d.memory, d.network_requests, d.storage_capacity,
                      d.required_modules, d.slots, d.reward,
                      d.assignment_strategy, d.planned_executions,
                      d.script, d.min_reputation, d.processor_version, d.runtime,
                      d.is_active,
                      {related_select}
               FROM deployments d
               WHERE 1=1"#
        ));

        // Add filters
        if let Some(ref account_id) = params.account_id {
            let normalized = normalize_address_with_prefix(account_id);
            query_builder.push(" AND d.address = ");
            query_builder.push_bind(normalized);
        }

        if let Some(seq_id) = params.seq_id {
            query_builder.push(" AND d.seq_id = ");
            query_builder.push_bind(seq_id);
        }

        if let Some(is_active) = params.is_active {
            query_builder.push(" AND d.is_active = ");
            query_builder.push_bind(is_active);
        }

        if let Some(block_from) = params.block_from {
            query_builder.push(" AND d.block_number >= ");
            query_builder.push_bind(to_i64_param("block_from", block_from)?);
        }

        if let Some(block_to) = params.block_to {
            query_builder.push(" AND d.block_number <= ");
            query_builder.push_bind(to_i64_param("block_to", block_to)?);
        }

        if let Some(ref exclude_addresses) = params.exclude_addresses {
            let normalized: Vec<String> = exclude_addresses
                .iter()
                .map(|a| normalize_address_with_prefix(a))
                .collect();
            if !normalized.is_empty() {
                query_builder.push(" AND d.address <> ALL(");
                query_builder.push_bind(normalized);
                query_builder.push(")");
            }
        }

        // Add cursor condition for keyset pagination
        let is_desc = sort_order.eq_ignore_ascii_case("desc");
        if let Some(ref cursor) = params.cursor {
            // Compound cursor: {seq_id: ..., val: sort_column_value}
            let cursor_seq_id = cursor
                .get("seq_id")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| {
                    RpcError::invalid_params(
                        "Invalid cursor: expected {\"seq_id\": number, \"val\": value}",
                    )
                })?;
            let cursor_val = cursor
                .get("val")
                .ok_or_else(|| RpcError::invalid_params("Invalid cursor: missing 'val' field"))?;
            let cursor_val_i64 = cursor_val.as_i64().ok_or_else(|| {
                RpcError::invalid_params("Invalid cursor: 'val' must be a number")
            })?;

            // Use tuple comparison for stable pagination
            if is_desc {
                query_builder.push(format!(" AND (d.{}, d.seq_id) < (", order_by_col));
            } else {
                query_builder.push(format!(" AND (d.{}, d.seq_id) > (", order_by_col));
            }
            query_builder.push_bind(cursor_val_i64);
            query_builder.push(", ");
            query_builder.push_bind(cursor_seq_id);
            query_builder.push(")");
        }

        // ORDER BY and LIMIT
        let direction = if is_desc { "DESC" } else { "ASC" };
        query_builder.push(format!(
            " ORDER BY d.{} {} NULLS LAST, d.seq_id {}",
            order_by_col, direction, direction
        ));
        query_builder.push(" LIMIT ");
        query_builder.push_bind(limit + 1);

        // Execute query
        let query = query_builder.build_query_as::<DeploymentQueryRow>();
        let rows: Vec<DeploymentQueryRow> =
            with_timeout(self.query_timeout, query.fetch_all(&self.db_pool))
                .await
                .map_err(|e| RpcError::database(e.to_string()))?;

        // Determine if there's a next page
        let has_next = rows.len() > limit as usize;
        let rows: Vec<_> = rows.into_iter().take(limit as usize).collect();

        // Build cursor for next page
        let next_cursor = if has_next && !rows.is_empty() {
            let last = rows.last().unwrap();
            let cursor_val = match order_by_col {
                "schedule_start_time" => last.schedule_start_time,
                "created_block_number" => last.created_block_number,
                _ => last.block_number,
            };
            Some(serde_json::json!({
                "seq_id": last.seq_id,
                "val": cursor_val
            }))
        } else {
            None
        };

        let items: Vec<DeploymentRow> = rows
            .into_iter()
            .map(|row| DeploymentRow {
                id: row.id,
                chain: row.chain,
                address: row.address,
                seq_id: row.seq_id,
                snapshot_id: row.snapshot_id,
                block_number: row.block_number,
                block_time: row.block_time,
                created_block_number: row.created_block_number,
                created_block_time: row.created_block_time,
                schedule_duration: row.schedule_duration,
                schedule_start_time: row.schedule_start_time,
                schedule_end_time: row.schedule_end_time,
                schedule_interval: row.schedule_interval,
                schedule_max_start_delay: row.schedule_max_start_delay,
                allowed_sources: row.allowed_sources,
                allow_only_verified_sources: row.allow_only_verified_sources,
                memory: row.memory,
                network_requests: row.network_requests,
                storage_capacity: row.storage_capacity,
                required_modules: row.required_modules,
                slots: row.slots,
                reward: row.reward,
                assignment_strategy: row.assignment_strategy,
                planned_executions: row.planned_executions,
                script: row.script,
                min_reputation: row.min_reputation,
                processor_version: row.processor_version,
                runtime: row.runtime,
                is_active: row.is_active,
                related_extrinsics: row.related_extrinsics,
            })
            .collect();

        Ok(Page::<DeploymentRow> {
            cursor: next_cursor,
            items,
            unfiltered_count: None,
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

#[derive(Debug, Deserialize, Default)]
pub struct GetProcessorChurnParams {
    /// Start of the date range (inclusive, RFC3339). Buckets overlapping the range
    /// are returned. Defaults to the earliest indexed `block_time`.
    #[serde(default)]
    pub from: Option<DateTime<Utc>>,
    /// End of the date range (inclusive, RFC3339). Defaults to the latest indexed
    /// `block_time` (so the last bucket is the current, in-progress one).
    #[serde(default)]
    pub to: Option<DateTime<Utc>>,
}

/// One fixed calendar bucket (a quarter or a year) with its active and onboarded
/// processor counts. `bucket_end` is exclusive (`bucket_start` + 3 months / 1 year).
#[derive(Debug, Serialize, Clone)]
pub struct ProcessorChurnBucket {
    pub bucket_start: DateTime<Utc>,
    pub bucket_end: DateTime<Utc>,
    /// Distinct processors that heartbeated during the bucket.
    pub active: i64,
    /// Distinct processors whose `onboard` extrinsic falls in the bucket.
    pub onboarded: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct ProcessorChurnResponse {
    pub quarters: Vec<ProcessorChurnBucket>,
    pub years: Vec<ProcessorChurnBucket>,
}

/// Ranking dimension for `getAccounts`. Each maps to a dedicated DESC index on
/// `accounts` so the unfiltered top-N is served by an index walk.
#[derive(Debug, Clone, Copy, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TopAccountsType {
    /// Liquid balance: `free + reserved` (index `accounts_total_idx`).
    Total,
    /// Whole balance incl. external locks:
    /// `free + reserved + remaining_vesting + remaining_token_claim`
    /// (index `accounts_total_external_idx`). Default ranking dimension.
    #[default]
    TotalWithLocked,
    /// Spendable balance: generated `transferable` column
    /// (index `accounts_transferable_idx`).
    Transferable,
    /// Free balance only (index `accounts_free_idx`).
    Free,
    /// Reserved balance only (index `accounts_reserved_idx`).
    Reserved,
    /// Frozen balance only (index `accounts_frozen_idx`).
    Frozen,
}

impl TopAccountsType {
    /// SQL expression to rank by. Fixed strings — never interpolate user input.
    /// Each matches its index's expression verbatim so the planner uses it.
    fn order_expr(self) -> &'static str {
        match self {
            TopAccountsType::Total => "(free + reserved)",
            TopAccountsType::TotalWithLocked => {
                "(free + reserved + remaining_vesting + remaining_token_claim)"
            }
            TopAccountsType::Transferable => "transferable",
            TopAccountsType::Free => "free",
            TopAccountsType::Reserved => "reserved",
            TopAccountsType::Frozen => "frozen",
        }
    }
}

/// Filters + keyset cursor for `getAccounts`.
#[derive(Debug, Deserialize, Default)]
pub struct GetAccountsParams {
    /// Ranking/sort dimension. Defaults to `total_with_locked`.
    #[serde(default)]
    pub sort: TopAccountsType,
    #[serde(default)]
    pub is_processor: Option<bool>,
    #[serde(default)]
    pub is_manager: Option<bool>,
    #[serde(default)]
    pub is_committer: Option<bool>,
    /// Exact match against the attestation-derived classification: "Core" | "Lite" | "Unknown".
    #[serde(default)]
    pub processor_type: Option<String>,
    /// Exact match against the attestation-derived classification: "iOS" | "Android" | "Unknown".
    #[serde(default)]
    pub device_type: Option<String>,
    /// Exact match on account_id (hex or SS58, normalized before lookup).
    #[serde(default)]
    pub account_id: Option<String>,
    /// Exclude these accounts (hex and SS58 may be mixed; normalized before compare).
    #[serde(default)]
    pub exclude_addresses: Option<Vec<String>>,
    /// Keyset cursor: `{"sort_value": <numeric string>, "account_id": <string>}`.
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
    /// Number of rows to return. Defaults to 100, clamped to [1, 100].
    #[serde(default)]
    pub limit: Option<i64>,
}

/// Filters for `getAccountsCount`. Mirrors the filter fields of
/// `GetAccountsParams`; sort/cursor/limit are irrelevant to a count.
#[derive(Debug, Deserialize, Default)]
pub struct GetAccountsCountParams {
    #[serde(default)]
    pub is_processor: Option<bool>,
    #[serde(default)]
    pub is_manager: Option<bool>,
    #[serde(default)]
    pub is_committer: Option<bool>,
    /// Exact match against the attestation-derived classification: "Core" | "Lite" | "Unknown".
    #[serde(default)]
    pub processor_type: Option<String>,
    /// Exact match against the attestation-derived classification: "iOS" | "Android" | "Unknown".
    #[serde(default)]
    pub device_type: Option<String>,
    /// Exact match on account_id (hex or SS58, normalized before lookup).
    #[serde(default)]
    pub account_id: Option<String>,
    /// Exclude these accounts (hex and SS58 may be mixed; normalized before compare).
    #[serde(default)]
    pub exclude_addresses: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct GetEpochTotalsParams {
    /// Minimum epoch (inclusive).
    #[serde(default)]
    pub epoch_from: Option<i64>,
    /// Maximum epoch (inclusive).
    #[serde(default)]
    pub epoch_to: Option<i64>,
    /// Max rows to return (most recent first). Defaults to 1000, clamped to [1, 5000].
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct EpochTotalsRow {
    pub epoch: i64,
    pub block_number: i64,
    pub block_time: DateTime<Utc>,
    // NUMERIC(38,0) columns cast to ::text to preserve full precision.
    pub total_vesting: String,
    pub total_token_claim: String,
    pub total_self_staked: String,
    pub total_delegated: String,
}

#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct TopAccountRow {
    pub account_id: String,
    // NUMERIC(38,0) columns cast to ::text in SQL to preserve full precision
    // (a JSON number / f64 would lose it).
    pub free: String,
    pub reserved: String,
    pub frozen: String,
    pub transferable: String,
    pub remaining_vesting: String,
    pub remaining_token_claim: String,
    /// The amount this row was ranked by (matches the requested `type`).
    pub sort_value: String,
    pub is_processor: bool,
    pub is_manager: bool,
    pub is_committer: bool,
    /// Derived from the processor's `StoredAttestation`: "Core" | "Lite" | "Unknown" | null (not yet classified).
    pub processor_type: Option<String>,
    /// Derived from the processor's `StoredAttestation`: "iOS" | "Android" | "Unknown" | null (not yet classified).
    pub device_type: Option<String>,
    pub block_number: i64,
    pub block_time: DateTime<Utc>,
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

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetBaseRewardsParams {
    pub manager: String,
    /// Optional: filter to a single processor (SS58 or hex).
    pub processor: Option<String>,
    pub epoch_from: Option<i64>,
    pub epoch_to: Option<i64>,
    pub limit: Option<u32>,
    /// Cursor epoch from the previous page's `cursor.epoch`.
    pub cursor_epoch: Option<i64>,
    /// Cursor processor from the previous page's `cursor.processor`.
    pub cursor_processor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct BaseRewardItem {
    /// Processor address (0x-prefixed hex).
    pub processor: String,
    pub epoch: i64,
    /// Total base reward for this processor in this epoch, in planck (sum of all heartbeat deposits).
    pub amount: String,
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
    pub cooldown_period: i64,
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

// ============================================
// DEPLOYMENTS
// ============================================

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GetDeploymentsParams {
    /// Filter by deployer address (hex or SS58)
    #[serde(default)]
    pub account_id: Option<String>,
    /// Filter by sequence ID
    #[serde(default)]
    pub seq_id: Option<i64>,
    /// Filter by active status
    #[serde(default)]
    pub is_active: Option<bool>,

    /// Block range filter - minimum
    #[serde(default)]
    pub block_from: Option<u32>,
    /// Block range filter - maximum
    #[serde(default)]
    pub block_to: Option<u32>,

    /// Exclude deployments deployed by any of these addresses (hex or SS58, may be mixed)
    #[serde(default)]
    pub exclude_addresses: Option<Vec<String>>,

    /// Order by column: block_number, created_block_number, start_time (default: block_number)
    #[serde(default)]
    pub order_by: Option<String>,
    /// Sort order: "asc" or "desc" (default: "desc")
    #[serde(default)]
    pub sort_order: Option<String>,
    /// Maximum results (default: 50)
    #[serde(default)]
    pub limit: Option<u32>,
    /// Cursor for pagination: {"seq_id": ..., "val": ...}
    #[serde(default)]
    pub cursor: Option<serde_json::Value>,
    /// If true, include related extrinsics for each deployment (default: false).
    /// Excluded by default because it's expensive for list queries.
    #[serde(default)]
    pub related_extrinsics: Option<bool>,
}

#[derive(Debug, Clone, Serialize, sqlx::FromRow)]
pub struct DeploymentRow {
    pub id: i64,
    pub chain: String,
    pub address: String,
    pub seq_id: i64,
    pub snapshot_id: Option<i64>,
    pub block_number: i64,
    pub block_time: chrono::DateTime<chrono::Utc>,
    pub created_block_number: i64,
    pub created_block_time: chrono::DateTime<chrono::Utc>,

    // Schedule
    pub schedule_duration: i64,
    pub schedule_start_time: i64,
    pub schedule_end_time: i64,
    pub schedule_interval: i64,
    pub schedule_max_start_delay: i64,

    // Specs
    pub allowed_sources: Option<serde_json::Value>,
    pub allow_only_verified_sources: bool,
    pub memory: i64,
    pub network_requests: i32,
    pub storage_capacity: i64,
    pub required_modules: Vec<String>,
    pub slots: i32,
    pub reward: bigdecimal::BigDecimal,
    pub assignment_strategy: String,
    pub planned_executions: Option<serde_json::Value>,
    pub script: String,
    pub min_reputation: Option<bigdecimal::BigDecimal>,
    pub processor_version: Option<serde_json::Value>,
    pub runtime: String,

    // Status
    pub is_active: bool,

    // Related extrinsics (joined from jobs table)
    #[sqlx(skip)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub related_extrinsics: Option<serde_json::Value>,
}

/// Intermediate row for querying deployments with related extrinsics
#[derive(Debug, Clone, sqlx::FromRow)]
struct DeploymentQueryRow {
    pub id: i64,
    pub chain: String,
    pub address: String,
    pub seq_id: i64,
    pub snapshot_id: Option<i64>,
    pub block_number: i64,
    pub block_time: chrono::DateTime<chrono::Utc>,
    pub created_block_number: i64,
    pub created_block_time: chrono::DateTime<chrono::Utc>,
    pub schedule_duration: i64,
    pub schedule_start_time: i64,
    pub schedule_end_time: i64,
    pub schedule_interval: i64,
    pub schedule_max_start_delay: i64,
    pub allowed_sources: Option<serde_json::Value>,
    pub allow_only_verified_sources: bool,
    pub memory: i64,
    pub network_requests: i32,
    pub storage_capacity: i64,
    pub required_modules: Vec<String>,
    pub slots: i32,
    pub reward: bigdecimal::BigDecimal,
    pub assignment_strategy: String,
    pub planned_executions: Option<serde_json::Value>,
    pub script: String,
    pub min_reputation: Option<bigdecimal::BigDecimal>,
    pub processor_version: Option<serde_json::Value>,
    pub runtime: String,
    pub is_active: bool,
    pub related_extrinsics: Option<serde_json::Value>,
}
