//! Debug script to retrieve and validate JobRegistration parsing

use acurast_indexer::transformation::ValueWrapper;
use subxt::{OnlineClient, PolkadotConfig};

/// Decode SS58 address to bytes (simplified, Acurast uses prefix 42)
fn ss58_decode(address: &str) -> Option<[u8; 32]> {
    let decoded = bs58::decode(address).into_vec().ok()?;
    if decoded.len() >= 35 {
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&decoded[1..33]);
        Some(bytes)
    } else {
        None
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Known test data
    let job_owner_ss58 = "5Cqsw5UiavToTmrH4dQuYtgKKyY8BF4wthjZzwPQmtnLH2xU";
    let seq_id: u64 = 21441;

    let job_owner_bytes = ss58_decode(job_owner_ss58).expect("Invalid job owner SS58");

    println!("Job owner SS58: {}", job_owner_ss58);
    println!("Job owner bytes: 0x{}", hex::encode(job_owner_bytes));
    println!("Seq ID: {}", seq_id);
    println!();

    // Connect to mainnet
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "wss://archive.mainnet.acurast.com".to_string());
    println!("Connecting to {}...", rpc_url);
    let client = OnlineClient::<PolkadotConfig>::from_url(&rpc_url).await?;
    println!("Connected to Acurast mainnet");

    let block = client.blocks().at_latest().await?;
    println!("At block: {}", block.number());

    // Build the keys - StoredJobRegistration is a DoubleMap with (MultiOrigin, seq_id) as separate keys
    let multi_origin = subxt::dynamic::Value::named_variant(
        "Acurast",
        [("", subxt::dynamic::Value::from_bytes(&job_owner_bytes))],
    );

    println!("\n=== Fetching StoredJobRegistration ===\n");

    // Try as DoubleMap (two separate keys)
    let storage_query = subxt::dynamic::storage(
        "Acurast",
        "StoredJobRegistration",
        vec![multi_origin, subxt::dynamic::Value::u128(seq_id as u128)],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(value)) => {
            println!("Raw encoded bytes length: {}", value.encoded().len());

            match value.to_value() {
                Ok(scale_val) => {
                    println!("\n=== JSON Conversion ===\n");
                    match serde_json::to_value(ValueWrapper::from(scale_val)) {
                        Ok(json) => {
                            println!("{}", serde_json::to_string_pretty(&json)?);

                            println!("\n=== Field Extraction Test ===\n");
                            validate_fields(&json);
                        }
                        Err(e) => {
                            println!("Failed to convert to JSON: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to decode value: {:?}", e);
                }
            }
        }
        Ok(None) => {
            println!("No JobRegistration found for this JobId");
        }
        Err(e) => {
            println!("Failed to fetch: {:?}", e);
        }
    }

    Ok(())
}

fn validate_fields(json: &serde_json::Value) {
    // Test extraction of each field the indexer uses

    // Schedule
    if let Some(schedule) = json.get("schedule") {
        println!("schedule.duration: {:?}", schedule.get("duration"));
        println!("schedule.start_time: {:?}", schedule.get("start_time"));
        println!("schedule.end_time: {:?}", schedule.get("end_time"));
        println!("schedule.interval: {:?}", schedule.get("interval"));
        println!(
            "schedule.max_start_delay: {:?}",
            schedule.get("max_start_delay")
        );
    } else {
        println!("WARNING: No 'schedule' field found!");
    }

    println!();

    // Base fields
    println!("allowed_sources: {:?}", json.get("allowed_sources"));
    println!(
        "allow_only_verified_sources: {:?}",
        json.get("allow_only_verified_sources")
    );
    println!("memory: {:?}", json.get("memory"));
    println!("network_requests: {:?}", json.get("network_requests"));
    println!("storage: {:?}", json.get("storage"));
    println!("required_modules: {:?}", json.get("required_modules"));

    println!();

    // Script
    if let Some(script) = json.get("script") {
        match script {
            serde_json::Value::String(s) => {
                println!("script (string): {} chars", s.len());
                // Try to decode hex to get actual script
                if let Some(hex_str) = s.strip_prefix("0x") {
                    if let Ok(bytes) = hex::decode(hex_str) {
                        if let Ok(decoded) = String::from_utf8(bytes) {
                            println!("  decoded from hex: {}", decoded);
                        } else {
                            println!("  raw hex: {}", s);
                        }
                    }
                } else if s.len() < 200 {
                    println!("  content: {}", s);
                } else {
                    println!("  content (truncated): {}...", &s[..100]);
                }
            }
            serde_json::Value::Array(arr) => {
                println!("script (byte array): {} bytes", arr.len());
                let bytes: Vec<u8> = arr
                    .iter()
                    .filter_map(|v| v.as_u64().map(|n| n as u8))
                    .collect();
                if let Ok(s) = String::from_utf8(bytes.clone()) {
                    if s.len() < 200 {
                        println!("  decoded: {}", s);
                    } else {
                        println!("  decoded (truncated): {}...", &s[..100]);
                    }
                }
            }
            _ => println!("script: {:?}", script),
        }
    } else {
        println!("WARNING: No 'script' field found!");
    }

    // Runtime extraction test
    let runtime = json
        .get("extra")
        .and_then(|e| e.get("requirements"))
        .and_then(|r| r.get("runtime"));
    if let Some(rt) = runtime {
        match rt {
            serde_json::Value::String(s) => println!("\nruntime (string): {}", s),
            serde_json::Value::Object(map) => {
                let variant = map.keys().next().unwrap_or(&"unknown".to_string()).clone();
                println!("\nruntime (object variant): {}", variant);
            }
            _ => println!("\nruntime: {:?}", rt),
        }
    }

    println!();

    // Extra (requirements)
    if let Some(extra) = json.get("extra") {
        println!("extra found: {}", extra.is_object());
        if let Some(requirements) = extra.get("requirements") {
            println!("  requirements.slots: {:?}", requirements.get("slots"));
            println!("  requirements.reward: {:?}", requirements.get("reward"));
            println!(
                "  requirements.min_reputation: {:?}",
                requirements.get("min_reputation")
            );
            println!(
                "  requirements.assignment_strategy: {:?}",
                requirements.get("assignment_strategy")
            );
            println!(
                "  requirements.processor_version: {:?}",
                requirements.get("processor_version")
            );
            println!("  requirements.runtime: {:?}", requirements.get("runtime"));
        } else {
            println!("  WARNING: No 'requirements' field in extra!");
        }
    } else {
        println!("WARNING: No 'extra' field found!");
    }
}
