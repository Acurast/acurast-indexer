//! Per-epoch network-wide totals rollup, plus complete per-account vesting /
//! token-claim.
//!
//! Runs at the `AccountsMaterialized` → `EpochTotalsComputed` transition of the
//! epoch pipeline, once `epoch.epoch_end` is known (the in-progress last epoch
//! is deferred). Writes one `epoch_totals` row per epoch AND — since it already
//! iterates the (small) vesting / token-claim maps in full — the complete
//! per-account `accounts.remaining_vesting` / `remaining_token_claim` columns,
//! replacing the event-snapshot-derived (eventually-consistent) values the
//! per-account materializer used to compute. Those upserts are guarded by
//! `accounts.vesting_epoch` so out-of-order epochs can't regress them.
//!
//! Each total is computed by **iterating the full storage map directly from the
//! archive node at the epoch's end block** — NOT by aggregating over
//! `storage_snapshots`.
//!
//! Why not `storage_snapshots` (or the `accounts` table)? Those are built from
//! event-triggered, per-key captures and are only *eventually* complete: the
//! "latest snapshot ≤ epoch_end" for a key can live many epochs in the past,
//! and epochs are processed out of order (concurrent workers, backwards/gap
//! catch-up) with only a per-epoch-window readiness gate. So an epoch could
//! finalize totals while an earlier epoch's snapshots are still missing,
//! producing an understated total that then persists (the row is written once
//! at MAX and never recomputed). Reading chain state at the epoch's end block
//! is complete and order-independent — the only dependency is the epoch's own
//! last block being indexed (guaranteed by the existing events-ready gate),
//! never any earlier epoch.
//!
//! Storage entries are resolved by pallet/storage NAME (not index) so subxt
//! resolves them against the live chain metadata — mainnet pallet indices
//! differ from the canary runtime baked into `canary.scale`.
//!
//! Per-total semantics:
//! - `total_vesting` — `Vesting.Vesting` (pallet_vesting): sum over every
//!   account's schedule vec of `max(0, locked - max(0, block - starting_block)
//!   * per_block)` evaluated at the epoch's last block.
//! - `total_token_claim` — `AcurastTokenClaim.Vesting` +
//!   `AcurastTokenClaim.MultiVesting`: sum of each entry's `remaining`.
//! - `total_self_staked` / `total_delegated` — `AcurastCompute.Commitments`:
//!   sum of `stake.amount` and `delegations_total_amount` over every live
//!   commitment (removed commitments are simply absent from storage).

use anyhow::Result;
use bigdecimal::BigDecimal;
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::str::FromStr;
use subxt::config::PolkadotConfig;
use subxt::utils::H256;
use subxt::OnlineClient;
use tracing::{debug, info, warn};

use super::epoch::update_epoch_phase;
use crate::entities::{EpochIndexPhase, EpochRow};
use crate::transformation::ValueWrapper;

type ChainBlock = subxt::blocks::Block<PolkadotConfig, OnlineClient<PolkadotConfig>>;

/// Parse a JSON scalar (string or number, as `ValueWrapper` emits u128s as
/// decimal strings) into a `BigDecimal`, defaulting to 0.
fn as_decimal(v: Option<&JsonValue>) -> BigDecimal {
    match v {
        Some(JsonValue::String(s)) => {
            BigDecimal::from_str(s).unwrap_or_else(|_| BigDecimal::from(0))
        }
        Some(JsonValue::Number(n)) => {
            BigDecimal::from_str(&n.to_string()).unwrap_or_else(|_| BigDecimal::from(0))
        }
        _ => BigDecimal::from(0),
    }
}

/// Decode a storage-map key at position `idx` (an AccountId) to the canonical
/// 0x-prefixed lowercase-hex `accounts.account_id` form.
fn key_account(keys: &[scale_value::Value], idx: usize) -> Option<String> {
    let key = keys.get(idx)?.clone().map_context(|_| 0u32);
    let json = serde_json::to_value(ValueWrapper::from(key)).ok()?;
    json.as_str()
        .map(crate::utils::normalize_address_with_prefix)
}

/// Network-wide totals at a single block, read directly from chain storage,
/// plus the complete per-account `remaining_vesting` / `remaining_token_claim`
/// captured from the same full-map iteration.
pub struct BlockTotals {
    pub total_vesting: BigDecimal,
    pub total_token_claim: BigDecimal,
    pub total_self_staked: BigDecimal,
    pub total_delegated: BigDecimal,
    /// account_id -> remaining_vesting (pallet_vesting), complete at this block.
    pub per_account_vesting: HashMap<String, BigDecimal>,
    /// account_id (destination) -> remaining_token_claim (AcurastTokenClaim
    /// Vesting + MultiVesting), complete at this block.
    pub per_account_token_claim: HashMap<String, BigDecimal>,
}

/// Iterate the four source maps at `block`, summing network totals and, for the
/// vesting / token-claim maps, the complete per-account amounts. `decay_at_block`
/// is the block height used to decay pallet_vesting schedules.
pub async fn compute_totals_at_block(
    block: &ChainBlock,
    decay_at_block: i64,
) -> Result<BlockTotals> {
    let decay_block = BigDecimal::from(decay_at_block);
    let mut per_account_vesting: HashMap<String, BigDecimal> = HashMap::new();
    let mut per_account_token_claim: HashMap<String, BigDecimal> = HashMap::new();

    // total_vesting: pallet_vesting `Vesting` — one entry per account, value is
    // a schedule vec (ValueWrapper may collapse a single-element vec to a bare
    // object, so accept either shape), decayed to the read block.
    let mut total_vesting = BigDecimal::from(0);
    {
        let query =
            subxt::dynamic::storage("Vesting", "Vesting", Vec::<subxt::dynamic::Value>::new());
        let mut iter = block.storage().iter(query).await?;
        while let Some(kv) = iter.next().await {
            let kv = kv?;
            let json = serde_json::to_value(ValueWrapper::from(kv.value.to_value()?))?;
            let schedules: Vec<&JsonValue> = match &json {
                JsonValue::Array(a) => a.iter().collect(),
                JsonValue::Object(_) => vec![&json],
                _ => vec![],
            };
            let mut remaining = BigDecimal::from(0);
            for s in schedules {
                let locked = as_decimal(s.get("locked"));
                let per_block = as_decimal(s.get("per_block"));
                let start = as_decimal(s.get("starting_block"));
                let mut elapsed = &decay_block - &start;
                if elapsed < BigDecimal::from(0) {
                    elapsed = BigDecimal::from(0);
                }
                let r = &locked - &(elapsed * &per_block);
                if r > BigDecimal::from(0) {
                    remaining += r;
                }
            }
            total_vesting += &remaining;
            if let Some(acc) = key_account(&kv.keys, 0) {
                *per_account_vesting
                    .entry(acc)
                    .or_insert_with(|| BigDecimal::from(0)) += remaining;
            }
        }
    }

    // total_token_claim: AcurastTokenClaim `Vesting` (destination = key 0) and
    // `MultiVesting` (destination = key 1), each entry's `remaining`, summed per
    // destination account.
    let mut total_token_claim = BigDecimal::from(0);
    for (storage, dest_key_idx) in [("Vesting", 0usize), ("MultiVesting", 1usize)] {
        let query = subxt::dynamic::storage(
            "AcurastTokenClaim",
            storage,
            Vec::<subxt::dynamic::Value>::new(),
        );
        let mut iter = block.storage().iter(query).await?;
        while let Some(kv) = iter.next().await {
            let kv = kv?;
            let json = serde_json::to_value(ValueWrapper::from(kv.value.to_value()?))?;
            let remaining = as_decimal(json.get("remaining"));
            total_token_claim += &remaining;
            if let Some(acc) = key_account(&kv.keys, dest_key_idx) {
                *per_account_token_claim
                    .entry(acc)
                    .or_insert_with(|| BigDecimal::from(0)) += remaining;
            }
        }
    }

    // total_self_staked / total_delegated: AcurastCompute `Commitments`, summed
    // in a single pass over the map (only live commitments are present). These
    // are network totals only — not projected onto per-account columns.
    let mut total_self_staked = BigDecimal::from(0);
    let mut total_delegated = BigDecimal::from(0);
    {
        let query = subxt::dynamic::storage(
            "AcurastCompute",
            "Commitments",
            Vec::<subxt::dynamic::Value>::new(),
        );
        let mut iter = block.storage().iter(query).await?;
        while let Some(kv) = iter.next().await {
            let kv = kv?;
            let json = serde_json::to_value(ValueWrapper::from(kv.value.to_value()?))?;
            total_self_staked += as_decimal(json.get("stake").and_then(|s| s.get("amount")));
            total_delegated += as_decimal(json.get("delegations_total_amount"));
        }
    }

    Ok(BlockTotals {
        total_vesting,
        total_token_claim,
        total_self_staked,
        total_delegated,
        per_account_vesting,
        per_account_token_claim,
    })
}

/// Compute and upsert the `epoch_totals` row for `epoch`, then advance the
/// epoch to `EpochTotalsComputed`.
#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("epoch-totals-{:?}", worker_id),
        epoch = epoch.epoch,
    )
)]
pub async fn process_epoch_totals(
    worker_id: u32,
    epoch: EpochRow,
    db_pool: &Pool<Postgres>,
    client: &OnlineClient<PolkadotConfig>,
) -> Result<()> {
    let epoch_end = match epoch.epoch_end {
        Some(v) => v,
        None => {
            // The last (in-progress) epoch has no known end — skip; it will be
            // picked up on the next epoch boundary.
            debug!(
                "epoch {} has no epoch_end yet, deferring epoch totals",
                epoch.epoch
            );
            return Ok(());
        }
    };

    // Read chain state at the epoch's last block (epoch_end is the next epoch's
    // start, i.e. an exclusive boundary). The block is in this epoch's own
    // window, so the events-ready gate guarantees it is indexed and its hash is
    // present.
    let last_block = epoch_end - 1;
    let block_meta: Option<(String, chrono::DateTime<chrono::Utc>)> =
        sqlx::query_as("SELECT hash, block_time FROM blocks WHERE block_number = $1")
            .bind(last_block)
            .fetch_optional(db_pool)
            .await?;
    let (block_hash_hex, block_time) = match block_meta {
        Some(v) => v,
        None => {
            // Should not happen once the epoch is events-ready; defer (leave at
            // AccountsMaterialized) so the queuer re-picks it.
            warn!(
                "epoch {} last block {} not in blocks table, deferring epoch totals",
                epoch.epoch, last_block
            );
            return Ok(());
        }
    };
    let hash_bytes = hex::decode(block_hash_hex.trim_start_matches("0x"))
        .map_err(|e| anyhow::anyhow!("bad block hash for {}: {}", last_block, e))?;
    // Guard against a wrong-length hash: H256::from_slice would panic (killing
    // the phase worker) — return an error instead so it's caught and retried.
    if hash_bytes.len() != 32 {
        return Err(anyhow::anyhow!(
            "block {} hash is {} bytes, expected 32",
            last_block,
            hash_bytes.len()
        ));
    }
    let block_hash = H256::from_slice(&hash_bytes);
    let block = client.blocks().at(block_hash).await?;

    let BlockTotals {
        total_vesting,
        total_token_claim,
        total_self_staked,
        total_delegated,
        per_account_vesting,
        per_account_token_claim,
    } = compute_totals_at_block(&block, last_block).await?;

    // Write the complete per-account vesting / token-claim columns. Union the
    // two maps; an account present in only one gets 0 for the other. Guarded by
    // `vesting_epoch` so an out-of-order older epoch can't regress a newer one.
    //
    // These are multi-row writes over the (largely overlapping) vesting cohort
    // that every epoch rewrites, so running them for two epochs concurrently
    // deadlocks. Serialization is guaranteed upstream — the queuer only ever
    // queues the latest epoch for totals and the phase worker holds a
    // process-wide lock across this whole call — so plain autocommit statements
    // are safe here (no DB advisory lock needed).
    let mut rows: Vec<(String, BigDecimal, BigDecimal)> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for acc in per_account_vesting
        .keys()
        .chain(per_account_token_claim.keys())
    {
        if !seen.insert(acc.clone()) {
            continue;
        }
        rows.push((
            acc.clone(),
            per_account_vesting
                .get(acc)
                .cloned()
                .unwrap_or_else(|| BigDecimal::from(0)),
            per_account_token_claim
                .get(acc)
                .cloned()
                .unwrap_or_else(|| BigDecimal::from(0)),
        ));
    }
    let ids: Vec<String> = rows.iter().map(|r| r.0.clone()).collect();
    let vest: Vec<BigDecimal> = rows.iter().map(|r| r.1.clone()).collect();
    let tc: Vec<BigDecimal> = rows.iter().map(|r| r.2.clone()).collect();

    // Zero accounts that dropped out of BOTH maps since a lower epoch (e.g.
    // fully claimed out), unless a >= epoch already refreshed them.
    sqlx::query(
        r#"
        UPDATE accounts
        SET remaining_vesting = 0, remaining_token_claim = 0, vesting_epoch = $1
        WHERE vesting_epoch < $1
          AND (remaining_vesting <> 0 OR remaining_token_claim <> 0)
          AND account_id <> ALL($2)
        "#,
    )
    .bind(epoch.epoch)
    .bind(&ids)
    .execute(db_pool)
    .await?;

    if !ids.is_empty() {
        sqlx::query(
            r#"
            INSERT INTO accounts (
                account_id, block_number, block_time,
                remaining_vesting, remaining_token_claim, vesting_epoch
            )
            SELECT u.account_id, $2, $3, u.rv, u.rtc, $4
            FROM UNNEST($1::text[], $5::numeric[], $6::numeric[])
                 AS u(account_id, rv, rtc)
            ON CONFLICT (account_id) DO UPDATE SET
                remaining_vesting     = EXCLUDED.remaining_vesting,
                remaining_token_claim = EXCLUDED.remaining_token_claim,
                vesting_epoch         = EXCLUDED.vesting_epoch
            WHERE EXCLUDED.vesting_epoch >= accounts.vesting_epoch
            "#,
        )
        .bind(&ids)
        .bind(last_block)
        .bind(block_time)
        .bind(epoch.epoch)
        .bind(&vest)
        .bind(&tc)
        .execute(db_pool)
        .await?;
    }

    sqlx::query(
        r#"
        INSERT INTO epoch_totals (
            epoch, block_number, block_time,
            total_vesting, total_token_claim, total_self_staked, total_delegated
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (epoch) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            block_time = EXCLUDED.block_time,
            total_vesting = EXCLUDED.total_vesting,
            total_token_claim = EXCLUDED.total_token_claim,
            total_self_staked = EXCLUDED.total_self_staked,
            total_delegated = EXCLUDED.total_delegated
        "#,
    )
    .bind(epoch.epoch)
    .bind(last_block)
    .bind(block_time)
    .bind(&total_vesting)
    .bind(&total_token_claim)
    .bind(&total_self_staked)
    .bind(&total_delegated)
    .execute(db_pool)
    .await?;

    update_epoch_phase(db_pool, epoch.epoch, EpochIndexPhase::EpochTotalsComputed).await?;

    info!(
        worker = format!("epoch-totals-{:?}", worker_id),
        "epoch {} totals @block {}: vesting={} token_claim={} self_staked={} delegated={}",
        epoch.epoch,
        last_block,
        total_vesting,
        total_token_claim,
        total_self_staked,
        total_delegated
    );

    Ok(())
}
