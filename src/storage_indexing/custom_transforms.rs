//! Custom key transforms for storage indexing.
//!
//! Each transform function takes extracted key values as JSON Values
//! and transforms them before the storage lookup is performed.

use crate::config::StorageKeyTransform;
use scale_value::At;
use serde_json::Value as JsonValue;
use subxt::config::PolkadotConfig;
use subxt::dynamic::Value;
use subxt::OnlineClient;
use tracing::{debug, info, trace, warn};

/// Collection ID for commitments in the Uniques pallet
const COMMITMENTS_COLLECTION_ID: u128 = 1;

/// Apply a transform to key values.
/// Returns None if the transform fails or determines the storage lookup should be skipped.
/// Returns Some(transformed_keys) if successful.
#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("storage-indexing-{:?}", worker_id),
        block_hash = block.hash().to_string(),
    )
)]
pub async fn apply_transform(
    worker_id: u32,
    transform: &StorageKeyTransform,
    key_values: Vec<JsonValue>,
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
) -> Option<Vec<JsonValue>> {
    match transform {
        StorageKeyTransform::CommitmentIdToCommitter => {
            commitment_id_to_committer(key_values, block).await
        }
    }
}

/// Transform a commitment_id to its owner (committer) by looking up Uniques.Asset.
/// The commitment is stored as Uniques collection 1, item = commitment_id.
/// Returns the owner's account as the new key value.
async fn commitment_id_to_committer(
    key_values: Vec<JsonValue>,
    block: &subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>,
) -> Option<Vec<JsonValue>> {
    info!("CommitmentIdToCommitter: key_values {:?}", key_values);
    if key_values.is_empty() {
        warn!("CommitmentIdToCommitter: no key values provided");
        return None;
    }

    // Extract commitment_id from the first key value (JSON)
    // Handles two variants:
    // 1. String: "0x56" (hex) or "437" (decimal)
    // 2. Array: ["437"] or ["437", "0"] - little-endian u64 limbs as strings
    let commitment_id: u128 = match &key_values[0] {
        JsonValue::String(s) => {
            // Try parsing as hex first, then decimal
            if let Some(hex_str) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
                match u128::from_str_radix(hex_str, 16) {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            "CommitmentIdToCommitter: could not parse hex string '{}': {:?}",
                            s, e
                        );
                        return None;
                    }
                }
            } else {
                match s.parse::<u128>() {
                    Ok(id) => id,
                    Err(e) => {
                        warn!(
                            "CommitmentIdToCommitter: could not parse string '{}' as number: {:?}",
                            s, e
                        );
                        return None;
                    }
                }
            }
        }
        JsonValue::Array(arr) => {
            // Array of little-endian u64 limbs as strings
            // For u128: up to 2 limbs, result = limb[0] + limb[1] * 2^64
            let mut result: u128 = 0;
            for (i, limb_val) in arr.iter().enumerate() {
                if i >= 2 {
                    warn!(
                        "CommitmentIdToCommitter: too many limbs for u128: {}",
                        arr.len()
                    );
                    return None;
                }
                let limb: u64 = match limb_val {
                    JsonValue::String(limb_str) => match limb_str.parse::<u64>() {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(
                                "CommitmentIdToCommitter: could not parse limb '{}' as u64: {:?}",
                                limb_str, e
                            );
                            return None;
                        }
                    },
                    JsonValue::Number(n) => {
                        if let Some(v) = n.as_u64() {
                            v
                        } else {
                            warn!(
                                "CommitmentIdToCommitter: limb number out of u64 range: {:?}",
                                n
                            );
                            return None;
                        }
                    }
                    other => {
                        warn!("CommitmentIdToCommitter: unexpected limb type: {:?}", other);
                        return None;
                    }
                };
                result |= (limb as u128) << (i * 64);
            }
            result
        }
        other => {
            warn!(
                "CommitmentIdToCommitter: unexpected key value type: {:?}",
                other
            );
            return None;
        }
    };

    trace!(
        "CommitmentIdToCommitter: looking up owner for commitment_id {}",
        commitment_id
    );

    // Query Uniques.Asset(collection_id=1, item_id=commitment_id)
    let storage_query = subxt::dynamic::storage(
        "Uniques",
        "Asset",
        vec![
            Value::u128(COMMITMENTS_COLLECTION_ID),
            Value::u128(commitment_id),
        ],
    );

    match block.storage().fetch(&storage_query).await {
        Ok(Some(asset_thunk)) => {
            match asset_thunk.to_value() {
                Ok(value) => {
                    // Navigate to the "owner" field
                    if let Some(owner_value) = value.at("owner") {
                        // Extract the account bytes from the owner
                        match &owner_value.value {
                            scale_value::ValueDef::Composite(composite) => {
                                // Try to extract bytes from composite
                                for field_value in composite.values() {
                                    if let scale_value::ValueDef::Composite(inner) =
                                        &field_value.value
                                    {
                                        let bytes: Vec<u8> = inner
                                            .values()
                                            .filter_map(|v| v.as_u128().map(|n| n as u8))
                                            .collect();
                                        if bytes.len() == 32 {
                                            let hex_account = format!("0x{}", hex::encode(&bytes));
                                            debug!(
                                                "CommitmentIdToCommitter: commitment {} owned by {}",
                                                commitment_id, hex_account
                                            );
                                            return Some(vec![JsonValue::String(hex_account)]);
                                        }
                                    }
                                }
                                // Fallback: try direct values
                                let bytes: Vec<u8> = composite
                                    .values()
                                    .filter_map(|v| v.as_u128().map(|n| n as u8))
                                    .collect();
                                if bytes.len() == 32 {
                                    let hex_account = format!("0x{}", hex::encode(&bytes));
                                    debug!(
                                        "CommitmentIdToCommitter: commitment {} owned by {}",
                                        commitment_id, hex_account
                                    );
                                    return Some(vec![JsonValue::String(hex_account)]);
                                }
                                warn!(
                                    "CommitmentIdToCommitter: could not extract 32-byte account from composite"
                                );
                            }
                            scale_value::ValueDef::Primitive(scale_value::Primitive::U256(
                                bytes,
                            )) => {
                                let hex_account = format!("0x{}", hex::encode(&bytes[..32]));
                                debug!(
                                    "CommitmentIdToCommitter: commitment {} owned by {}",
                                    commitment_id, hex_account
                                );
                                return Some(vec![JsonValue::String(hex_account)]);
                            }
                            other => {
                                warn!(
                                    "CommitmentIdToCommitter: unexpected owner value type: {:?}",
                                    std::mem::discriminant(other)
                                );
                            }
                        }
                    } else {
                        warn!("CommitmentIdToCommitter: no 'owner' field in Asset");
                    }
                }
                Err(e) => {
                    warn!(
                        "CommitmentIdToCommitter: failed to decode Asset value: {:?}",
                        e
                    );
                }
            }
        }
        Ok(None) => {
            debug!(
                "CommitmentIdToCommitter: no Asset found for commitment {}",
                commitment_id
            );
        }
        Err(e) => {
            warn!(
                "CommitmentIdToCommitter: failed to fetch Asset storage: {:?}",
                e
            );
        }
    }

    None
}
