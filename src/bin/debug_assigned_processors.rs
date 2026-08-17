//! Debug script to test AssignedProcessors storage iteration

use subxt::{OnlineClient, PolkadotConfig};

/// Decode SS58 address to bytes (simplified, Acurast uses prefix 42)
fn ss58_decode(address: &str) -> Option<[u8; 32]> {
    // Use bs58 decode
    let decoded = bs58::decode(address).into_vec().ok()?;
    // SS58 format: [prefix byte(s), 32 bytes of address, 2 bytes checksum]
    // For prefix 42, it's 1 byte prefix
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
    // Known test data from user
    let job_owner_ss58 = "5Cqsw5UiavToTmrH4dQuYtgKKyY8BF4wthjZzwPQmtnLH2xU";
    let seq_id: u64 = 21441;
    let expected_processor_ss58 = "5HeuYsNg953hQNCAkfvt46gpi6KAZPSrru54JZwGM6F9C9E3";

    // Decode SS58 addresses to bytes
    let job_owner_bytes = ss58_decode(job_owner_ss58).expect("Invalid job owner SS58");
    let expected_processor_bytes =
        ss58_decode(expected_processor_ss58).expect("Invalid processor SS58");

    println!("Job owner SS58: {}", job_owner_ss58);
    println!("Job owner bytes: 0x{}", hex::encode(job_owner_bytes));
    println!("Seq ID: {}", seq_id);
    println!("Expected processor SS58: {}", expected_processor_ss58);
    println!(
        "Expected processor bytes: 0x{}",
        hex::encode(expected_processor_bytes)
    );
    println!();

    // Connect to mainnet
    let rpc_url = std::env::var("RPC_URL")
        .unwrap_or_else(|_| "wss://archive.mainnet.acurast.com".to_string());
    println!("Connecting to {}...", rpc_url);
    let client = OnlineClient::<PolkadotConfig>::from_url(&rpc_url).await?;
    println!("Connected to Acurast mainnet");

    // MultiOrigin::Acurast variant
    let multi_origin = subxt::dynamic::Value::named_variant(
        "Acurast",
        [("", subxt::dynamic::Value::from_bytes(&job_owner_bytes))],
    );

    // JobId is (MultiOrigin, u64)
    let job_id = subxt::dynamic::Value::unnamed_composite(vec![
        multi_origin,
        subxt::dynamic::Value::u128(seq_id as u128),
    ]);

    println!("Querying AssignedProcessors with JobId...");

    // Query AssignedProcessors with partial key (just JobId) to iterate all processors
    let storage_query =
        subxt::dynamic::storage("AcurastMarketplace", "AssignedProcessors", vec![job_id]);

    let block = client.blocks().at_latest().await?;
    println!("At block: {}", block.number());

    match block.storage().iter(storage_query).await {
        Ok(mut iter) => {
            println!("Storage iteration started successfully");
            let mut count = 0;
            while let Some(result) = iter.next().await {
                match result {
                    Ok(kv) => {
                        count += 1;
                        println!("\n--- Entry {} ---", count);
                        println!("Keys count: {}", kv.keys.len());
                        for (i, key) in kv.keys.iter().enumerate() {
                            println!("Key[{}] full: {:?}", i, key);
                            // Try to extract bytes from the key
                            if let scale_value::ValueDef::Composite(c) = &key.value {
                                println!("  -> Composite with {} values", c.len());
                                for (j, inner) in c.values().enumerate() {
                                    println!("     [{}]: {:?}", j, inner);
                                    // Try deeper
                                    if let scale_value::ValueDef::Composite(c2) = &inner.value {
                                        println!(
                                            "        -> Inner composite with {} values",
                                            c2.len()
                                        );
                                        let bytes: Vec<u8> = c2
                                            .values()
                                            .filter_map(|v| v.as_u128().map(|n| n as u8))
                                            .collect();
                                        if bytes.len() == 32 {
                                            println!(
                                                "        -> Extracted 32 bytes: 0x{}",
                                                hex::encode(&bytes)
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        println!("Value: {:?}", kv.value.to_value());
                    }
                    Err(e) => {
                        println!("Error iterating: {:?}", e);
                    }
                }
            }
            println!("\nTotal entries found: {}", count);
        }
        Err(e) => {
            println!("Failed to start iteration: {:?}", e);
        }
    }

    Ok(())
}
