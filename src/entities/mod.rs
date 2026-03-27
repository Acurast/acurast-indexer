use chrono::DateTime;
use chrono::Utc;
use serde::{Deserialize, Serialize};
pub use serde_json::Value as JsonValue;
use sqlx::Type;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[repr(i32)]
pub enum ExtrinsicsIndexPhase {
    Raw = 0,
    AddressExtracted = 1,
    StorageIndexed = 2,
}

impl ExtrinsicsIndexPhase {
    /// The maximum phase value (final phase)
    pub const MAX: u32 = Self::StorageIndexed as u32;
}

impl Serialize for ExtrinsicsIndexPhase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for ExtrinsicsIndexPhase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

impl From<u32> for ExtrinsicsIndexPhase {
    fn from(value: u32) -> Self {
        (value as i32).into()
    }
}

impl From<i32> for ExtrinsicsIndexPhase {
    fn from(value: i32) -> Self {
        match value {
            1 => ExtrinsicsIndexPhase::AddressExtracted,
            2 => ExtrinsicsIndexPhase::StorageIndexed,
            _ => ExtrinsicsIndexPhase::Raw,
        }
    }
}

impl From<ExtrinsicsIndexPhase> for i32 {
    fn from(phase: ExtrinsicsIndexPhase) -> Self {
        phase as i32
    }
}

impl From<ExtrinsicsIndexPhase> for u32 {
    fn from(phase: ExtrinsicsIndexPhase) -> Self {
        phase as u32
    }
}

/// Events index phase - flattened enum for simplicity.
/// Serialized as: Created=0, JobsExtracted=1, StorageIndexed2=2, StorageIndexed3=3, StorageIndexed4=4
#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[repr(i32)]
pub enum EventsIndexPhase {
    Created = 0,
    JobsExtracted = 1,
    StorageIndexed2 = 2,
    StorageIndexed3 = 3,
    StorageIndexed4 = 4,
    StorageIndexed5 = 5,
}

impl EventsIndexPhase {
    /// The maximum phase value (final phase)
    pub const MAX: u32 = Self::StorageIndexed5 as u32;
}

impl Serialize for EventsIndexPhase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for EventsIndexPhase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

impl From<i32> for EventsIndexPhase {
    fn from(value: i32) -> Self {
        match value {
            0 => EventsIndexPhase::Created,
            1 => EventsIndexPhase::JobsExtracted,
            2 => EventsIndexPhase::StorageIndexed2,
            3 => EventsIndexPhase::StorageIndexed3,
            4 => EventsIndexPhase::StorageIndexed4,
            5 => EventsIndexPhase::StorageIndexed5,
            _ => EventsIndexPhase::Created,
        }
    }
}

impl From<u32> for EventsIndexPhase {
    fn from(value: u32) -> Self {
        (value as i32).into()
    }
}

impl From<EventsIndexPhase> for i32 {
    fn from(phase: EventsIndexPhase) -> Self {
        phase as i32
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Type)]
#[repr(i32)]
pub enum EpochIndexPhase {
    Raw = 0,
    EventsReady = 1,
    StorageIndexed2 = 2,
    StorageIndexed3 = 3,
    StorageIndexed4 = 4,
}

impl EpochIndexPhase {
    /// The maximum phase value (final phase)
    pub const MAX: u32 = Self::StorageIndexed4 as u32;
}

impl Serialize for EpochIndexPhase {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_i32(*self as i32)
    }
}

impl<'de> Deserialize<'de> for EpochIndexPhase {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = i32::deserialize(deserializer)?;
        Ok(Self::from(value))
    }
}

impl From<u32> for EpochIndexPhase {
    fn from(value: u32) -> Self {
        (value as i32).into()
    }
}

impl From<i32> for EpochIndexPhase {
    fn from(value: i32) -> Self {
        match value {
            1 => EpochIndexPhase::EventsReady,
            2 => EpochIndexPhase::StorageIndexed2,
            3 => EpochIndexPhase::StorageIndexed3,
            4 => EpochIndexPhase::StorageIndexed4,
            _ => EpochIndexPhase::Raw,
        }
    }
}

impl From<EpochIndexPhase> for i32 {
    fn from(phase: EpochIndexPhase) -> Self {
        phase as i32
    }
}

impl From<EpochIndexPhase> for u32 {
    fn from(phase: EpochIndexPhase) -> Self {
        phase as u32
    }
}

#[derive(sqlx::FromRow, Serialize, Clone)]
pub struct Block {
    pub block_number: i64,
    pub hash: String,
    pub block_time: DateTime<Utc>,
}

#[derive(sqlx::FromRow, Serialize, Clone)]
pub struct SpecVersionChange {
    pub spec_version: i32,
    pub block_number: i64,
    pub block_time: DateTime<Utc>,
    pub block_hash: String,
}

#[derive(Serialize, Clone)]
pub struct StorageSnapshot {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub event_index: Option<i32>,
    pub pallet: i32,
    pub storage_location: String,
    pub storage_keys: JsonValue,
    pub data: JsonValue,
    pub config_rule: String,
    pub block_time: DateTime<Utc>,
}

#[derive(sqlx::FromRow, Serialize, Clone, Debug)]
pub struct ExtrinsicRow {
    pub block_number: i64,
    pub index: i32,
    pub pallet: i32,
    pub method: i32,
    pub data: Option<JsonValue>,
    pub tx_hash: String,
    pub account_id: String,
    pub block_time: DateTime<Utc>,
    pub phase: ExtrinsicsIndexPhase,
}

impl ExtrinsicRow {
    /// Returns the extrinsic id as "{block_number}-{index}"
    pub fn id(&self) -> String {
        format!("{}-{}", self.block_number, self.index)
    }
}

/// ExtrinsicRow with events included (for API queries that need events)
#[derive(sqlx::FromRow, Serialize, Clone, Debug)]
pub struct ExtrinsicRowWithEvents {
    pub block_number: i64,
    pub index: i32,
    pub pallet: i32,
    pub method: i32,
    pub data: Option<JsonValue>,
    pub tx_hash: String,
    pub account_id: String,
    pub block_time: DateTime<Utc>,
    pub phase: ExtrinsicsIndexPhase,
    pub events: Option<JsonValue>,

    /// For exploded batch items, the 0-indexed position within the parent batch
    #[serde(skip_serializing_if = "Option::is_none")]
    #[sqlx(default)]
    pub batch_index: Option<i32>,
}

impl ExtrinsicRowWithEvents {
    /// Returns the extrinsic id as "{block_number}-{index}"
    pub fn id(&self) -> String {
        format!("{}-{}", self.block_number, self.index)
    }

    /// Convert to ExtrinsicRow (dropping events)
    pub fn into_row(self) -> ExtrinsicRow {
        ExtrinsicRow {
            block_number: self.block_number,
            index: self.index,
            pallet: self.pallet,
            method: self.method,
            data: self.data,
            tx_hash: self.tx_hash,
            account_id: self.account_id,
            block_time: self.block_time,
            phase: self.phase,
        }
    }
}

#[derive(Serialize, Clone)]
pub struct Extrinsic {
    pub id: String,
    pub block_number: i64,
    pub index: i32,
    pub pallet: i32,
    pub method: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pallet_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method_name: Option<String>,
    pub data: Option<JsonValue>,
    pub tx_hash: String,
    pub account_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<JsonValue>,
    pub block_time: DateTime<Utc>,
    pub phase: ExtrinsicsIndexPhase,
}

impl Extrinsic {
    pub fn from_row(
        row: &ExtrinsicRow,
        events: Option<JsonValue>,
        pallet_name: Option<String>,
        method_name: Option<String>,
    ) -> Self {
        Self {
            id: row.id(),
            block_number: row.block_number,
            index: row.index,
            pallet: row.pallet,
            method: row.method,
            pallet_name,
            method_name,
            data: row.data.clone(),
            tx_hash: row.tx_hash.clone(),
            account_id: row.account_id.clone(),
            events,
            block_time: row.block_time,
            phase: row.phase,
        }
    }

    pub fn from_row_with_events(
        row: &ExtrinsicRowWithEvents,
        events: Option<JsonValue>,
        pallet_name: Option<String>,
        method_name: Option<String>,
    ) -> Self {
        Self {
            id: row.id(),
            block_number: row.block_number,
            index: row.index,
            pallet: row.pallet,
            method: row.method,
            pallet_name,
            method_name,
            data: row.data.clone(),
            tx_hash: row.tx_hash.clone(),
            account_id: row.account_id.clone(),
            events,
            block_time: row.block_time,
            phase: row.phase,
        }
    }
}

// #[derive(Serialize)]
// pub struct Event {
//     pub index: u32,
//     pub pallet: u8,
//     pub method: u8,
//     pub data: JsonValue,
// }

#[derive(sqlx::FromRow, Serialize, Clone)]
pub struct EventRow {
    pub block_number: i64,
    pub extrinsic_index: i32,
    pub index: i32,
    pub pallet: i32,
    pub variant: i32,
    pub data: Option<JsonValue>,
    pub phase: EventsIndexPhase,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub block_time: DateTime<Utc>,

    #[sqlx(skip)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pallet_name: Option<String>,
    #[sqlx(skip)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method_name: Option<String>,
}

impl EventRow {
    pub fn from_row(
        row: EventRow,
        pallet_name: Option<String>,
        method_name: Option<String>,
    ) -> Self {
        Self {
            pallet_name,
            method_name,
            ..row
        }
    }

    /// Returns the extrinsic id as "{block_number}-{index}"
    pub fn id(&self) -> String {
        format!("{}.{}", self.block_number, self.index)
    }
}

#[derive(sqlx::FromRow, Serialize, Clone)]
pub struct Job {
    pub job_id: String,
    pub address: String,
}

/// EpochRow with computed epoch_end from LEAD() window function
#[derive(Debug, Serialize, Clone, sqlx::FromRow)]
pub struct EpochRow {
    pub epoch: i64,
    pub epoch_start: i64,
    pub epoch_end: Option<i64>,
    pub epoch_start_time: DateTime<Utc>,
    pub phase: EpochIndexPhase,
}

#[derive(sqlx::FromRow, Serialize)]
pub struct Transfer {
    pub id: String,
    pub source: String,
    pub dest: String,
    pub amount: i64,
}

/// Paginated response with cursor for continuation.
/// Cursor can be any JSON-serializable type (number, string, or object).
#[derive(Serialize, Clone)]
pub struct Page<T: Serialize + Clone> {
    pub items: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unfiltered_count: Option<u32>,
}
