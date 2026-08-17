//! Epoch-boundary `accounts` materializer.
//!
//! Runs at the `StorageIndexed3` → `AccountsMaterialized` transition, after
//! every event and every epoch-triggered storage rule for the epoch has
//! landed in `storage_snapshots`. At that point aggregation queries over
//! the snapshot table see a coherent view: no live-race half-populated
//! partial sums, no ordering hazards.
//!
//! This materializer owns ONLY the System.Account balance columns
//! (`free / reserved / frozen / flags`) — the latest `System.Account` snapshot
//! for the account at or before `epoch_end`. The `System.Account` map cannot be
//! fully iterated every epoch (millions of accounts), so it stays event-driven
//! and only accounts touched by a balance snapshot in the epoch's window are
//! recomputed; untouched rows are left alone.
//!
//! `remaining_vesting` / `remaining_token_claim` are NOT written here. Those
//! come from `epoch_totals`, which iterates the (small) vesting / token-claim
//! maps in full at the epoch boundary and writes complete per-account values —
//! avoiding the event-snapshot eventual-consistency problem for those columns.
//!
//! Role flags (`is_processor` / `is_manager` / `is_committer`) are
//! preserved on conflict.

use anyhow::Result;
use bigdecimal::BigDecimal;
use sqlx::{Pool, Postgres};
use tracing::{debug, info, warn};

use super::epoch::update_epoch_phase;
use crate::entities::{EpochIndexPhase, EpochRow};

/// Materialize `accounts` rows for every account touched by a snapshot in
/// `epoch`'s block window. Advances the epoch to `AccountsMaterialized` on
/// success.
#[tracing::instrument(
    skip_all,
    fields(
        worker = format!("accounts-epoch-{:?}", worker_id),
        epoch = epoch.epoch,
    )
)]
pub async fn process_epoch_accounts_materialization(
    worker_id: u32,
    epoch: EpochRow,
    db_pool: &Pool<Postgres>,
) -> Result<()> {
    let epoch_end = match epoch.epoch_end {
        Some(v) => v,
        None => {
            // The last (in-progress) epoch has no known end — skip; the
            // materializer will pick it up on the next epoch boundary.
            debug!(
                "epoch {} has no epoch_end yet, deferring accounts materialization",
                epoch.epoch
            );
            return Ok(());
        }
    };
    let epoch_start = epoch.epoch_start;
    let last_block = epoch_end - 1;

    // 1. Collect accounts whose System.Account balance changed in this epoch.
    // Vesting / token-claim are NOT sourced here anymore: those small maps are
    // iterated in full at the epoch boundary by `epoch_totals`, which writes the
    // complete per-account `remaining_vesting` / `remaining_token_claim` columns
    // directly. This materializer now owns only the System.Account balance
    // columns (free/reserved/frozen/flags), which can't be fully iterated.
    let touched: Vec<(String,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT storage_keys->>0 AS a
        FROM storage_snapshots
        WHERE block_number BETWEEN $1 AND $2
          AND pallet = 0 AND storage_location = 'Account'
          AND storage_keys->>0 IS NOT NULL AND storage_keys->>0 <> ''
        "#,
    )
    .bind(epoch_start)
    .bind(last_block)
    .fetch_all(db_pool)
    .await?;

    if touched.is_empty() {
        update_epoch_phase(db_pool, epoch.epoch, EpochIndexPhase::AccountsMaterialized).await?;
        debug!(
            "epoch {} accounts materialization: no touched accounts",
            epoch.epoch
        );
        return Ok(());
    }

    let mut ok = 0u64;
    let mut errs = 0u64;
    for (raw_key,) in &touched {
        match materialize_one_account(db_pool, raw_key, epoch_end).await {
            Ok(()) => ok += 1,
            Err(e) => {
                errs += 1;
                warn!(
                    "epoch {} accounts materialization: account {} failed: {:?}",
                    epoch.epoch, raw_key, e
                );
            }
        }
    }

    update_epoch_phase(db_pool, epoch.epoch, EpochIndexPhase::AccountsMaterialized).await?;
    info!(
        worker = format!("accounts-epoch-{:?}", worker_id),
        "epoch {} accounts materialized: {} ok, {} failed (of {} touched)",
        epoch.epoch,
        ok,
        errs,
        touched.len()
    );
    Ok(())
}

/// Aggregate every column group for a single account at `epoch_end` and
/// upsert. `raw_key` is whatever ValueWrapper stored in `storage_keys` —
/// hex, possibly missing the `0x` prefix; we normalize to the canonical
/// `accounts.account_id` form before writing.
async fn materialize_one_account(
    db_pool: &Pool<Postgres>,
    raw_key: &str,
    epoch_end: i64,
) -> Result<()> {
    let account_id = crate::utils::normalize_address_with_prefix(raw_key);
    let dest_prefixed = account_id.clone();
    let dest_bare = account_id
        .strip_prefix("0x")
        .unwrap_or(&account_id)
        .to_string();

    // Latest System.Account (pre-`epoch_end`). value_path='data' on the
    // rule means the JSON here is the AccountData subobject.
    let sys: Option<(
        Option<BigDecimal>,
        Option<BigDecimal>,
        Option<BigDecimal>,
        Option<BigDecimal>,
        Option<i64>,
        Option<chrono::DateTime<chrono::Utc>>,
    )> = sqlx::query_as(
        r#"
            SELECT
                (data->>'free')::NUMERIC       AS free,
                (data->>'reserved')::NUMERIC   AS reserved,
                (data->>'frozen')::NUMERIC     AS frozen,
                -- `flags` is the ExtraFlags(u128) newtype, which ValueWrapper
                -- serializes as a single-element JSON array (e.g. ["170141..."]),
                -- not a scalar. Extract element 0; fall back to a scalar in case
                -- a snapshot ever stored it unwrapped.
                CASE jsonb_typeof(data->'flags')
                    WHEN 'array' THEN (data->'flags'->>0)::NUMERIC
                    ELSE (data->>'flags')::NUMERIC
                END                            AS flags,
                block_number,
                block_time
            FROM storage_snapshots
            WHERE pallet = 0 AND storage_location = 'Account'
              AND (storage_keys->>0 = $1 OR storage_keys->>0 = $2)
              AND block_number <= $3
              AND data IS NOT NULL
            ORDER BY block_number DESC
            LIMIT 1
            "#,
    )
    .bind(&dest_prefixed)
    .bind(&dest_bare)
    .bind(epoch_end)
    .fetch_optional(db_pool)
    .await?;

    let (free, reserved, frozen, flags, sys_block, sys_time) = match sys {
        Some((f, r, fr, fl, b, t)) => (
            f.unwrap_or_else(|| BigDecimal::from(0)),
            r.unwrap_or_else(|| BigDecimal::from(0)),
            fr.unwrap_or_else(|| BigDecimal::from(0)),
            fl.unwrap_or_else(|| BigDecimal::from(0)),
            b,
            t,
        ),
        None => (
            BigDecimal::from(0),
            BigDecimal::from(0),
            BigDecimal::from(0),
            BigDecimal::from(0),
            None,
            None,
        ),
    };

    // Whichever source is freshest wins for the row's block_number stamp.
    let block_number = sys_block.unwrap_or(epoch_end);
    let block_time = sys_time.unwrap_or_else(chrono::Utc::now);

    // Only the System.Account balance columns are written here. The
    // `remaining_vesting` / `remaining_token_claim` columns are owned by
    // `epoch_totals` (full-map iteration), so they are intentionally left
    // untouched by this upsert.
    sqlx::query(
        r#"
        INSERT INTO accounts (
            account_id, block_number, block_time,
            free, reserved, frozen, flags
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (account_id) DO UPDATE SET
            block_number          = EXCLUDED.block_number,
            block_time            = EXCLUDED.block_time,
            free                  = EXCLUDED.free,
            reserved              = EXCLUDED.reserved,
            frozen                = EXCLUDED.frozen,
            flags                 = EXCLUDED.flags
        -- Only let a newer snapshot win. Epoch phases run on many workers
        -- and can complete out of order (esp. after an epoch-phase reset);
        -- without this guard an older epoch's balances could overwrite a
        -- newer one's while block_number (previously GREATEST) stayed pinned
        -- at the max, leaving the row internally inconsistent.
        WHERE EXCLUDED.block_number >= accounts.block_number
        "#,
    )
    .bind(&account_id)
    .bind(block_number)
    .bind(block_time)
    .bind(&free)
    .bind(&reserved)
    .bind(&frozen)
    .bind(&flags)
    .execute(db_pool)
    .await?;

    Ok(())
}
