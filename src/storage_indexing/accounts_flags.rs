//! Role-flag upserts on the `accounts` table. Called from the domain
//! insert sites for processors, managers, and committers. Flags latch to
//! TRUE — v1 has no clearing semantics.
//!
//! A row is inserted if the account has never been seen (balance fields
//! default to 0). A later `SystemAccountToAccounts` snapshot fills in the
//! real balance values; the flag is preserved on that conflict.

use anyhow::Result;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use tracing::trace;

use crate::utils::normalize_address_with_prefix;

/// `AcurastProcessorManager` pallet index (mainnet).
pub const PROCESSOR_MANAGER_PALLET: i32 = 41;

// Event variant indices for the CURRENT mainnet runtime (spec 13). These are
// runtime-specific: the canary/kusama runtime baked into `canary.scale` uses
// *different* indices (e.g. 4/15), so do not copy variant numbers from there.
// Authoritative mapping (from the mainnet metadata / prod indexer):
//   3  = ProcessorPaired(processor, pairing)           [legacy]
//   6  = ProcessorHeartbeatWithVersion(processor, ver)
//   13 = ProcessorPairedV2(processor, manager)
// In all three the processor account is the first event field (`data[0]`).
const PROCESSOR_PAIRED: i32 = 3;
const PROCESSOR_HEARTBEAT_WITH_VERSION: i32 = 6;
const PROCESSOR_PAIRED_V2: i32 = 13;

/// If `(pallet, variant, data)` is a `ProcessorManager` event that identifies
/// an account as a processor (paired or heartbeat), return that processor's
/// account id (`data[0]`); otherwise `None`.
///
/// Pure and DB-free so it can be unit-tested against real event payloads.
pub fn processor_account_from_event(pallet: i32, variant: i32, data: &JsonValue) -> Option<&str> {
    if pallet != PROCESSOR_MANAGER_PALLET {
        return None;
    }
    if !matches!(
        variant,
        PROCESSOR_PAIRED | PROCESSOR_HEARTBEAT_WITH_VERSION | PROCESSOR_PAIRED_V2
    ) {
        return None;
    }
    data.get(0)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
}

/// Mark `account_id` as a processor.
pub async fn flag_processor(
    db_pool: &Pool<Postgres>,
    account_id: &str,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    flag(
        db_pool,
        "is_processor",
        account_id,
        block_number,
        block_time,
    )
    .await
}

/// Mark `account_id` as a manager.
pub async fn flag_manager(
    db_pool: &Pool<Postgres>,
    account_id: &str,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    flag(db_pool, "is_manager", account_id, block_number, block_time).await
}

/// Mark `account_id` as a committer.
pub async fn flag_committer(
    db_pool: &Pool<Postgres>,
    account_id: &str,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    flag(
        db_pool,
        "is_committer",
        account_id,
        block_number,
        block_time,
    )
    .await
}

async fn flag(
    db_pool: &Pool<Postgres>,
    column: &'static str,
    account_id: &str,
    block_number: i64,
    block_time: chrono::DateTime<chrono::Utc>,
) -> Result<()> {
    if account_id.is_empty() {
        return Ok(());
    }
    let normalized = normalize_address_with_prefix(account_id);
    // `column` is a compile-time literal from the three wrappers above, so
    // string interpolation into the SQL is safe.
    let sql = format!(
        r#"
        INSERT INTO accounts (account_id, block_number, block_time, {col})
        VALUES ($1, $2, $3, TRUE)
        ON CONFLICT (account_id) DO UPDATE SET {col} = TRUE
        "#,
        col = column
    );
    sqlx::query(&sql)
        .bind(&normalized)
        .bind(block_number)
        .bind(block_time)
        .execute(db_pool)
        .await?;
    trace!("Flagged {} = TRUE for {}", column, normalized);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // Event payloads below are REAL mainnet events captured from the prod
    // indexer (https://indexer.mainnet.acurast.com), so these tests would have
    // caught the original wrong variant constants (4/15 = canary indices;
    // mainnet uses 3/6/13, where 15 is actually ProcessorAdvertisementV2).

    #[test]
    fn heartbeat_with_version_v6_flags_processor() {
        // ProcessorHeartbeatWithVersion(processor, {build_number, platform})
        let data = json!([
            "0x66274c5904b43ee318f3e0868ccb47e304bc334c80af369855d8265a28bece32",
            { "build_number": "110", "platform": "0" }
        ]);
        assert_eq!(
            processor_account_from_event(41, 6, &data),
            Some("0x66274c5904b43ee318f3e0868ccb47e304bc334c80af369855d8265a28bece32")
        );
    }

    #[test]
    fn paired_v2_v13_flags_processor() {
        // ProcessorPairedV2(processor, manager)
        let data = json!([
            "0x18701399835729d0535fa54361cba89be017723755d3a0b5eb6cee04449bb022",
            "0x9c9f0194e40614a7e34b1ad235b604f9335209e9d37ca5c536dac3d91b37a22f"
        ]);
        assert_eq!(
            processor_account_from_event(41, 13, &data),
            Some("0x18701399835729d0535fa54361cba89be017723755d3a0b5eb6cee04449bb022")
        );
    }

    #[test]
    fn paired_legacy_v3_flags_processor() {
        // ProcessorPaired(processor, {account: manager, proof: ...})
        let data = json!([
            "0x44e90370b85836e3b728588adce467946eeddd5aacd780d9fc2f07b94dcc8e76",
            { "account": "0x10e6412499d5dfcaf76fb0615240357e24bd2c4690b6a2e5ea8a5d40ad601495",
              "proof": { "signature": { "Sr25519": "0xce5d" }, "timestamp": "1783587384000" } }
        ]);
        assert_eq!(
            processor_account_from_event(41, 3, &data),
            Some("0x44e90370b85836e3b728588adce467946eeddd5aacd780d9fc2f07b94dcc8e76")
        );
    }

    #[test]
    fn advertisement_v15_is_not_a_processor_signal() {
        // Variant 15 on mainnet is ProcessorAdvertisementV2, NOT ProcessorPairedV2.
        // The old code matched 15 and missed the real pairing/heartbeat events.
        let data = json!(["0x66274c5904b43ee318f3e0868ccb47e304bc334c80af369855d8265a28bece32"]);
        assert_eq!(processor_account_from_event(41, 15, &data), None);
    }

    #[test]
    fn variant_4_is_not_a_processor_signal() {
        // Old hardcoded PROCESSOR_HEARTBEAT = 4 matches nothing on mainnet.
        let data = json!(["0x66274c5904b43ee318f3e0868ccb47e304bc334c80af369855d8265a28bece32"]);
        assert_eq!(processor_account_from_event(41, 4, &data), None);
    }

    #[test]
    fn wrong_pallet_is_ignored() {
        // pallet 40 (Acurast) AttestationStoredV2 also carries an account at
        // data[0], but it is not a ProcessorManager event.
        let data = json!(["0x44e90370b85836e3b728588adce467946eeddd5aacd780d9fc2f07b94dcc8e76"]);
        assert_eq!(processor_account_from_event(40, 6, &data), None);
    }

    #[test]
    fn missing_or_empty_account_is_none() {
        assert_eq!(processor_account_from_event(41, 6, &json!([])), None);
        assert_eq!(processor_account_from_event(41, 6, &json!([""])), None);
        assert_eq!(processor_account_from_event(41, 6, &json!([123])), None);
    }
}
