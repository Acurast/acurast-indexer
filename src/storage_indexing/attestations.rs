//! Processor attestation classification.
//!
//! Decodes the generic `Acurast.StoredAttestation` JSON blobs captured by the
//! `attestation_stored` / `attestation_stored_v2` storage-indexing rules
//! (phase 4) into a `processor_type` (Core/Lite) and `device_type`
//! (iOS/Android) classification per account, per
//! `notes/Distinguishing Lite vs Core.md`.
//!
//! Follows the same shape as `commitments.rs`: a pure decode function plus
//! an incremental "find unprocessed snapshots, then process them" pair,
//! driven by a periodic task (`attestation_processing_task` in main.rs)
//! rather than being inlined into `process_storage_rules`.

use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use tracing::trace;

use crate::utils::normalize_address_with_prefix;

const UNKNOWN: &str = "Unknown";
const CORE: &str = "Core";
const LITE: &str = "Lite";
const IOS: &str = "iOS";
const ANDROID: &str = "Android";

const CORE_PACKAGE_PREFIX: &str = "com.acurast.attested.executor.";
const LITE_PACKAGE_INFIX: &str = ".sbs.";

/// A snapshot row from `storage_snapshots` for `pallet = 40, storage_location
/// = 'StoredAttestation'`.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct AttestationSnapshot {
    pub id: i64,
    pub block_number: i64,
    pub block_time: chrono::DateTime<chrono::Utc>,
    pub storage_keys: JsonValue,
    pub data: JsonValue,
}

/// Classify an `Attestation` value (already generically SCALE-decoded to
/// JSON via `ValueWrapper`) into `(processor_type, device_type)`.
///
/// Never fails — any missing/malformed field classifies as `"Unknown"`.
pub fn classify_attestation(data: &JsonValue) -> (&'static str, &'static str) {
    let content = match data.get("content") {
        Some(c) => c,
        None => return (UNKNOWN, UNKNOWN),
    };

    if content.get("DeviceAttestation").is_some() {
        // iOS attestations carry no package name; the barrier hard-codes
        // Core=false, Lite=true for this variant.
        return (LITE, IOS);
    }

    if let Some(key_description) = content.get("KeyDescription") {
        let package_names = key_description
            .get("tee_enforced")
            .and_then(package_names_from_authorization_list)
            .or_else(|| {
                key_description
                    .get("software_enforced")
                    .and_then(package_names_from_authorization_list)
            })
            .unwrap_or_default();

        if package_names.iter().any(|n| n.contains(LITE_PACKAGE_INFIX)) {
            return (LITE, ANDROID);
        }
        if package_names
            .iter()
            .any(|n| n.starts_with(CORE_PACKAGE_PREFIX))
        {
            return (CORE, ANDROID);
        }
        return (UNKNOWN, ANDROID);
    }

    (UNKNOWN, UNKNOWN)
}

/// Extract decoded ASCII package names from a `BoundedAuthorizationList`
/// JSON value's `attestation_application_id.package_infos[].package_name`.
fn package_names_from_authorization_list(authorization_list: &JsonValue) -> Option<Vec<String>> {
    let package_infos = authorization_list
        .get("attestation_application_id")?
        .get("package_infos")?
        .as_array()?;

    Some(
        package_infos
            .iter()
            .filter_map(|info| info.get("package_name")?.as_str())
            .filter_map(decode_hex_ascii)
            .collect(),
    )
}

/// `package_name` is a `BoundedVec<u8>`, hex-encoded by `ValueWrapper`
/// (e.g. `"0x636f6d2e..."`). Decode it back to an ASCII string.
fn decode_hex_ascii(hex_str: &str) -> Option<String> {
    let bytes = hex::decode(hex_str.strip_prefix("0x")?).ok()?;
    String::from_utf8(bytes).ok()
}

/// Find accounts whose latest `StoredAttestation` snapshot is newer than
/// what's already reflected in `accounts.attestation_block_number` (or
/// hasn't been classified at all yet). Mirrors
/// `find_unprocessed_commitment_snapshots`.
pub async fn find_unprocessed_attestation_snapshots(
    db_pool: &Pool<Postgres>,
    batch_size: i64,
) -> Result<Vec<i64>, anyhow::Error> {
    let snapshot_ids: Vec<(i64,)> = sqlx::query_as(
        r#"
        WITH latest_snapshots AS (
            -- storage_keys is a nested array like [["0x..."]] (same shape as
            -- commitments), so the account is at ->0->>0.
            SELECT DISTINCT ON (s.storage_keys->0->>0)
                s.id, s.storage_keys->0->>0 AS account_id, s.block_number
            FROM storage_snapshots s
            WHERE s.pallet = 40
            AND s.storage_location = 'StoredAttestation'
            AND s.data IS NOT NULL
            AND s.data != 'null'::jsonb
            ORDER BY s.storage_keys->0->>0, s.block_number DESC
        )
        SELECT ls.id
        FROM latest_snapshots ls
        LEFT JOIN accounts a ON a.account_id = (
            CASE
                WHEN ls.account_id LIKE '0x%' THEN ls.account_id
                ELSE '0x' || ls.account_id
            END
        )
        WHERE a.attestation_block_number IS NULL OR ls.block_number > a.attestation_block_number
        LIMIT $1
        "#,
    )
    .bind(batch_size)
    .fetch_all(db_pool)
    .await?;

    Ok(snapshot_ids.into_iter().map(|(id,)| id).collect())
}

/// Fetch attestation snapshots by their IDs.
async fn fetch_attestation_snapshots(
    db_pool: &Pool<Postgres>,
    snapshot_ids: &[i64],
) -> Result<Vec<AttestationSnapshot>, anyhow::Error> {
    if snapshot_ids.is_empty() {
        return Ok(vec![]);
    }

    let snapshots: Vec<AttestationSnapshot> = sqlx::query_as(
        r#"
        SELECT id, block_number, block_time, storage_keys, data
        FROM storage_snapshots
        WHERE id = ANY($1)
        "#,
    )
    .bind(snapshot_ids)
    .fetch_all(db_pool)
    .await?;

    Ok(snapshots)
}

/// Classify a batch of attestation snapshots and upsert the results into
/// `accounts`. Returns the number of accounts updated.
pub async fn process_attestation_snapshot_ids(
    db_pool: &Pool<Postgres>,
    snapshot_ids: &[i64],
) -> Result<u64, anyhow::Error> {
    if snapshot_ids.is_empty() {
        trace!("No attestation snapshots to process");
        return Ok(0);
    }

    let snapshots = fetch_attestation_snapshots(db_pool, snapshot_ids).await?;
    let mut updated = 0u64;

    for snapshot in snapshots {
        // storage_keys is nested (e.g. [["0x..."]]): the account is at [0][0].
        let account_id = match snapshot
            .storage_keys
            .get(0)
            .and_then(|v| v.get(0))
            .and_then(|v| v.as_str())
        {
            Some(id) if !id.is_empty() => id,
            _ => {
                trace!(
                    "Attestation snapshot {} has no account key, skipping",
                    snapshot.id
                );
                continue;
            }
        };
        let normalized = normalize_address_with_prefix(account_id);
        let (processor_type, device_type) = classify_attestation(&snapshot.data);

        sqlx::query(
            r#"
            INSERT INTO accounts (account_id, block_number, block_time, processor_type, device_type, attestation_block_number)
            VALUES ($1, $2, $3, $4, $5, $2)
            ON CONFLICT (account_id) DO UPDATE
            SET processor_type = $4, device_type = $5, attestation_block_number = $2
            WHERE accounts.attestation_block_number IS NULL OR accounts.attestation_block_number < $2
            "#,
        )
        .bind(&normalized)
        .bind(snapshot.block_number)
        .bind(snapshot.block_time)
        .bind(processor_type)
        .bind(device_type)
        .execute(db_pool)
        .await?;

        updated += 1;
    }

    Ok(updated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn package_name_hex(name: &str) -> String {
        format!("0x{}", hex::encode(name.as_bytes()))
    }

    fn key_description_with_package(field: &str, package: &str) -> JsonValue {
        json!({
            "content": {
                "KeyDescription": {
                    field: {
                        "attestation_application_id": {
                            "package_infos": [
                                { "package_name": package_name_hex(package) }
                            ],
                            "signature_digests": []
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn lite_android_via_sbs_package() {
        let data = key_description_with_package(
            "tee_enforced",
            "com.acurast.attested.executor.sbs.mainnet",
        );
        assert_eq!(classify_attestation(&data), (LITE, ANDROID));
    }

    #[test]
    fn core_android_via_executor_package() {
        let data =
            key_description_with_package("tee_enforced", "com.acurast.attested.executor.mainnet");
        assert_eq!(classify_attestation(&data), (CORE, ANDROID));
    }

    #[test]
    fn unknown_android_for_unrecognized_package() {
        let data = key_description_with_package("tee_enforced", "com.example.other.app");
        assert_eq!(classify_attestation(&data), (UNKNOWN, ANDROID));
    }

    #[test]
    fn falls_back_to_software_enforced_when_tee_enforced_empty() {
        let data = json!({
            "content": {
                "KeyDescription": {
                    "tee_enforced": { "attestation_application_id": null },
                    "software_enforced": {
                        "attestation_application_id": {
                            "package_infos": [
                                { "package_name": package_name_hex("com.acurast.attested.executor.canary") }
                            ],
                            "signature_digests": []
                        }
                    }
                }
            }
        });
        assert_eq!(classify_attestation(&data), (CORE, ANDROID));
    }

    #[test]
    fn device_attestation_is_lite_ios() {
        let data = json!({
            "content": {
                "DeviceAttestation": {
                    "key_usage_properties": {},
                    "device_os_information": {},
                    "nonce": {}
                }
            }
        });
        assert_eq!(classify_attestation(&data), (LITE, IOS));
    }

    #[test]
    fn missing_or_null_content_is_unknown() {
        assert_eq!(classify_attestation(&JsonValue::Null), (UNKNOWN, UNKNOWN));
        assert_eq!(classify_attestation(&json!({})), (UNKNOWN, UNKNOWN));
        assert_eq!(
            classify_attestation(&json!({ "content": {} })),
            (UNKNOWN, UNKNOWN)
        );
    }

    #[test]
    fn no_application_id_is_unknown_android() {
        let data = json!({
            "content": {
                "KeyDescription": {
                    "tee_enforced": { "attestation_application_id": null },
                    "software_enforced": { "attestation_application_id": null }
                }
            }
        });
        assert_eq!(classify_attestation(&data), (UNKNOWN, ANDROID));
    }

    // === Real mainnet fixtures ==========================================
    // Actual `StoredAttestation` values captured from mainnet and decoded by
    // this repo's `ValueWrapper` (via the live indexer). Note the real data
    // populates `software_enforced` with `tee_enforced.attestation_application_id
    // = null`, so these exercise the tee->software fallback on genuine payloads.

    const REAL_CORE_ANDROID: &str = include_str!("testdata/attestation_core_android.json");
    const REAL_LITE_ANDROID: &str = include_str!("testdata/attestation_lite_android.json");
    const REAL_IOS: &str = include_str!("testdata/attestation_ios.json");

    fn classify_str(s: &str) -> (&'static str, &'static str) {
        classify_attestation(&serde_json::from_str::<JsonValue>(s).unwrap())
    }

    #[test]
    fn real_mainnet_core_android() {
        // package com.acurast.attested.executor.canary (no ".sbs.")
        assert_eq!(classify_str(REAL_CORE_ANDROID), (CORE, ANDROID));
    }

    #[test]
    fn real_mainnet_lite_android() {
        // package com.acurast.attested.executor.sbs.canary (".sbs." => Lite)
        assert_eq!(classify_str(REAL_LITE_ANDROID), (LITE, ANDROID));
    }

    #[test]
    fn real_mainnet_ios_is_lite() {
        // DeviceAttestation => iOS, always Lite
        assert_eq!(classify_str(REAL_IOS), (LITE, IOS));
    }
}
