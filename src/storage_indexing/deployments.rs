//! Deployment processing module
//!
//! Processes events to populate the deployments table:
//! - JobRegistrationStoredV2: Insert/update deployment
//! - JobRegistrationRemoved: Set is_active = false

use anyhow::anyhow;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use subxt::{OnlineClient, PolkadotConfig};
use tracing::{debug, trace, warn};

use super::parse::{extract_numeric_string, extract_optional_numeric_string, extract_u64};
use crate::transformation::ValueWrapper;

/// Process a JobRegistrationStoredV2 event and insert/update deployment
pub async fn process_job_registration_stored(
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
    event_data: &JsonValue,
    block_number: i64,
    block_hash: &str,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<bool, anyhow::Error> {
    // Event structure: JobRegistrationStoredV2(JobId) where JobId = (MultiOrigin, seq_id)
    // The event data IS the job_id, not nested under a key
    let (chain, address, seq_id) = parse_job_id_json(event_data)
        .ok_or_else(|| anyhow!("Failed to parse job_id from event data: {:?}", event_data))?;

    // Fetch registration from storage using the job_id
    let registration = fetch_job_registration(client, &chain, &address, seq_id, block_hash).await?;
    let registration = match registration {
        Some(reg) => reg,
        None => {
            warn!(
                "No registration found in storage for {}:{}#{} at block {}",
                chain, address, seq_id, block_number
            );
            return Ok(false);
        }
    };

    // Check if this deployment already exists (for created_block tracking)
    let existing: Option<(i64, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
        "SELECT created_block_number, created_block_time FROM deployments WHERE chain = $1::target_chain AND address = $2 AND seq_id = $3",
    )
    .bind(&chain)
    .bind(&address)
    .bind(seq_id)
    .fetch_optional(db_pool)
    .await?;

    let (created_block_number, created_block_time) = existing.unwrap_or((block_number, block_time));

    // Extract schedule
    let schedule = registration.get("schedule");
    let schedule_duration = extract_u64(schedule, "duration").unwrap_or(0) as i64;
    let schedule_start_time = extract_u64(schedule, "start_time").unwrap_or(0) as i64;
    let schedule_end_time = extract_u64(schedule, "end_time").unwrap_or(0) as i64;
    let schedule_interval = extract_u64(schedule, "interval").unwrap_or(0) as i64;
    let schedule_max_start_delay = extract_u64(schedule, "max_start_delay").unwrap_or(0) as i64;

    // Extract specs - base JobRegistration fields
    let allowed_sources = registration.get("allowed_sources").cloned();
    let allow_only_verified_sources = registration
        .get("allow_only_verified_sources")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let memory = extract_u64(Some(&registration), "memory").unwrap_or(0) as i64;
    let network_requests = extract_u64(Some(&registration), "network_requests").unwrap_or(0) as i32;
    let storage_capacity = extract_u64(Some(&registration), "storage").unwrap_or(0) as i64;

    // Extract required_modules as string array
    let required_modules = extract_required_modules(&registration);

    // Extract script
    let script = extract_script(&registration);

    // Extract extra.requirements fields
    let extra = registration.get("extra");
    let requirements = extra.and_then(|e| e.get("requirements"));

    let slots = requirements
        .and_then(|r| r.get("slots"))
        .and_then(|v| v.as_u64())
        .unwrap_or(1) as i32;

    let reward = extract_numeric_string(requirements, "reward");

    let (assignment_strategy, planned_executions) = extract_assignment_strategy(requirements);

    let min_reputation = extract_optional_numeric_string(requirements, "min_reputation");

    let processor_version = requirements
        .and_then(|r| r.get("processor_version"))
        .cloned();

    let runtime = extract_runtime(requirements);

    // Upsert into deployments table
    let result = sqlx::query(
        r#"
        INSERT INTO deployments (
            chain, address, seq_id,
            block_number, block_time,
            created_block_number, created_block_time,
            schedule_duration, schedule_start_time, schedule_end_time,
            schedule_interval, schedule_max_start_delay,
            allowed_sources, allow_only_verified_sources,
            memory, network_requests, storage_capacity,
            required_modules, slots, reward,
            assignment_strategy, planned_executions,
            script, min_reputation, processor_version, runtime,
            is_active
        )
        VALUES (
            $1::target_chain, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
            $21, $22, $23, $24, $25, $26, $27
        )
        ON CONFLICT (chain, address, seq_id) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            block_time = EXCLUDED.block_time,
            schedule_duration = EXCLUDED.schedule_duration,
            schedule_start_time = EXCLUDED.schedule_start_time,
            schedule_end_time = EXCLUDED.schedule_end_time,
            schedule_interval = EXCLUDED.schedule_interval,
            schedule_max_start_delay = EXCLUDED.schedule_max_start_delay,
            allowed_sources = EXCLUDED.allowed_sources,
            allow_only_verified_sources = EXCLUDED.allow_only_verified_sources,
            memory = EXCLUDED.memory,
            network_requests = EXCLUDED.network_requests,
            storage_capacity = EXCLUDED.storage_capacity,
            required_modules = EXCLUDED.required_modules,
            slots = EXCLUDED.slots,
            reward = EXCLUDED.reward,
            assignment_strategy = EXCLUDED.assignment_strategy,
            planned_executions = EXCLUDED.planned_executions,
            script = EXCLUDED.script,
            min_reputation = EXCLUDED.min_reputation,
            processor_version = EXCLUDED.processor_version,
            runtime = EXCLUDED.runtime
        "#,
    )
    .bind(&chain)
    .bind(&address)
    .bind(seq_id)
    .bind(block_number)
    .bind(block_time)
    .bind(created_block_number)
    .bind(created_block_time)
    .bind(schedule_duration)
    .bind(schedule_start_time)
    .bind(schedule_end_time)
    .bind(schedule_interval)
    .bind(schedule_max_start_delay)
    .bind(&allowed_sources)
    .bind(allow_only_verified_sources)
    .bind(memory)
    .bind(network_requests)
    .bind(storage_capacity)
    .bind(&required_modules)
    .bind(slots)
    .bind(&reward)
    .bind(&assignment_strategy)
    .bind(&planned_executions)
    .bind(&script)
    .bind(&min_reputation)
    .bind(&processor_version)
    .bind(&runtime)
    .bind(true) // is_active
    .execute(db_pool)
    .await?;
    crate::task_monitor::TASK_REGISTRY.record_db_insert(
        crate::task_monitor::DbEntity::Deployment,
        result.rows_affected(),
    );

    trace!(
        "Upserted deployment {}:{}#{} at block {}",
        chain,
        address,
        seq_id,
        block_number
    );

    Ok(true)
}

/// Process a JobRegistrationRemoved event and set is_active = false
pub async fn process_job_registration_removed(
    db_pool: &Pool<Postgres>,
    event_data: &JsonValue,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<bool, anyhow::Error> {
    // Event structure: JobRegistrationRemoved(JobId) where JobId = (MultiOrigin, seq_id)
    // The event data IS the job_id, not nested under a key
    let (chain, address, seq_id) = parse_job_id_json(event_data)
        .ok_or_else(|| anyhow!("Failed to parse job_id from event data: {:?}", event_data))?;

    // Set is_active = false (blindly, no conflict possible)
    let result = sqlx::query(
        r#"
        UPDATE deployments
        SET is_active = false, block_number = $4, block_time = $5
        WHERE chain = $1::target_chain AND address = $2 AND seq_id = $3
        "#,
    )
    .bind(&chain)
    .bind(&address)
    .bind(seq_id)
    .bind(block_number)
    .bind(block_time)
    .execute(db_pool)
    .await?;

    if result.rows_affected() > 0 {
        trace!(
            "Marked deployment {}:{}#{} as inactive at block {}",
            chain,
            address,
            seq_id,
            block_number
        );
        Ok(true)
    } else {
        debug!(
            "Deployment {}:{}#{} not found for removal at block {}",
            chain, address, seq_id, block_number
        );
        Ok(false)
    }
}

// ============================================================================
// Storage fetching
// ============================================================================

/// Fetch JobRegistration from chain storage at a specific block
async fn fetch_job_registration(
    client: &OnlineClient<PolkadotConfig>,
    chain: &str,
    address: &str,
    seq_id: i64,
    block_hash: &str,
) -> Result<Option<JsonValue>, anyhow::Error> {
    use subxt::utils::H256;

    // Parse block hash
    let hash_bytes = hex::decode(block_hash.strip_prefix("0x").unwrap_or(block_hash))
        .map_err(|e| anyhow!("Invalid block hash: {}", e))?;
    let block_hash = H256::from_slice(&hash_bytes);

    // Get block reference
    let block = client.blocks().at(block_hash).await?;

    // Build the MultiOrigin key
    // Address should be hex bytes (with or without 0x prefix)
    let address_bytes = hex::decode(address.strip_prefix("0x").unwrap_or(address))
        .map_err(|e| anyhow!("Invalid address hex: {}", e))?;

    let multi_origin = subxt::dynamic::Value::named_variant(
        chain,
        [("", subxt::dynamic::Value::from_bytes(&address_bytes))],
    );

    // Query StoredJobRegistration storage
    let storage_query = subxt::dynamic::storage(
        "Acurast",
        "StoredJobRegistration",
        vec![multi_origin, subxt::dynamic::Value::u128(seq_id as u128)],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(value)) => match value.to_value() {
            Ok(scale_val) => {
                let json = serde_json::to_value(ValueWrapper::from(scale_val))
                    .map_err(|e| anyhow!("Failed to convert registration to JSON: {}", e))?;
                Ok(Some(json))
            }
            Err(e) => {
                warn!("Failed to decode registration value: {:?}", e);
                Ok(None)
            }
        },
        Ok(None) => Ok(None),
        Err(e) => {
            warn!("Failed to fetch registration from storage: {:?}", e);
            Err(anyhow!("Storage fetch failed: {}", e))
        }
    }
}

// ============================================================================
// Helper functions for parsing event data
// ============================================================================

/// Parse a JobId JSON structure into (chain, address, seq_id)
fn parse_job_id_json(value: &JsonValue) -> Option<(String, String, i64)> {
    match value {
        JsonValue::Array(arr) if arr.len() >= 2 => {
            // [MultiOrigin, seq_id]
            let (chain, address) = extract_multi_origin(&arr[0])?;
            let seq_id = parse_seq_id(&arr[1])?;
            Some((chain, address, seq_id))
        }
        JsonValue::Object(obj) => {
            // Could be named fields like {"0": MultiOrigin, "1": seq_id}
            let multi_origin = obj.get("0").or_else(|| obj.get("multi_origin"))?;
            let seq_id_val = obj.get("1").or_else(|| obj.get("seq_id"))?;
            let (chain, address) = extract_multi_origin(multi_origin)?;
            let seq_id = parse_seq_id(seq_id_val)?;
            Some((chain, address, seq_id))
        }
        _ => None,
    }
}

/// Extract chain and address from MultiOrigin JSON
fn extract_multi_origin(multi_origin: &JsonValue) -> Option<(String, String)> {
    if let JsonValue::Object(map) = multi_origin {
        for (chain, address_val) in map {
            let address = match address_val {
                JsonValue::String(s) => s.clone(),
                JsonValue::Object(inner) => {
                    // Handle nested structure like {"Id": "0x..."}
                    inner.values().next()?.as_str()?.to_string()
                }
                _ => continue,
            };
            return Some((chain.clone(), address));
        }
    }
    None
}

/// Parse seq_id from JSON value
fn parse_seq_id(value: &JsonValue) -> Option<i64> {
    match value {
        JsonValue::Number(n) => n.as_i64(),
        JsonValue::String(s) => s.parse().ok(),
        _ => None,
    }
}

/// Extract required_modules as string array
fn extract_required_modules(data: &JsonValue) -> Vec<String> {
    data.get("required_modules")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|module| {
                    // Module can be a string like "DataEncryption" or an object like {"DataEncryption": null}
                    match module {
                        JsonValue::String(s) => Some(s.clone()),
                        JsonValue::Object(map) => map.keys().next().cloned(),
                        _ => None,
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Extract script (IPFS URL or raw script)
fn extract_script(data: &JsonValue) -> String {
    data.get("script")
        .and_then(|v| {
            match v {
                JsonValue::String(s) => {
                    // Script might be hex-encoded (0x...) - decode to UTF-8
                    if let Some(hex_str) = s.strip_prefix("0x") {
                        if let Ok(bytes) = hex::decode(hex_str) {
                            if let Ok(decoded) = String::from_utf8(bytes) {
                                return Some(decoded);
                            }
                        }
                    }
                    Some(s.clone())
                }
                JsonValue::Array(arr) => {
                    // Script stored as byte array - convert to string
                    let bytes: Vec<u8> = arr
                        .iter()
                        .filter_map(|b| b.as_u64().map(|n| n as u8))
                        .collect();
                    String::from_utf8(bytes).ok()
                }
                _ => None,
            }
        })
        .unwrap_or_default()
}

/// Extract runtime variant name from runtime field
fn extract_runtime(requirements: Option<&JsonValue>) -> String {
    let runtime = match requirements.and_then(|r| r.get("runtime")) {
        Some(v) => v,
        None => return "NodeJS".to_string(),
    };

    match runtime {
        JsonValue::String(s) => s.clone(),
        JsonValue::Object(map) => {
            // Get the first key which is the variant name
            map.keys()
                .next()
                .cloned()
                .unwrap_or_else(|| "NodeJS".to_string())
        }
        _ => "NodeJS".to_string(),
    }
}

/// Extract assignment strategy and planned_executions
fn extract_assignment_strategy(requirements: Option<&JsonValue>) -> (String, Option<JsonValue>) {
    let strategy = match requirements.and_then(|r| r.get("assignment_strategy")) {
        Some(v) => v,
        None => return ("Single".to_string(), None),
    };

    // AssignmentStrategy is an enum: Single(Option<PlannedExecutions>) or Competing
    match strategy {
        JsonValue::String(s) => {
            if s == "Competing" {
                ("Competing".to_string(), None)
            } else {
                ("Single".to_string(), None)
            }
        }
        JsonValue::Object(map) => {
            if map.contains_key("Competing") {
                ("Competing".to_string(), None)
            } else if let Some(planned) = map.get("Single") {
                let planned_executions = if planned.is_null() {
                    None
                } else {
                    Some(planned.clone())
                };
                ("Single".to_string(), planned_executions)
            } else {
                ("Single".to_string(), None)
            }
        }
        _ => ("Single".to_string(), None),
    }
}
