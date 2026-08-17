//! Standalone, eventually-correct collection of processor-churn membership into
//! fixed calendar buckets.
//!
//! `getProcessorChurn` needs "distinct active processors (heartbeat signers) in a
//! calendar quarter / year". Computing that on demand is I/O-bound (~100k random
//! probes into the ~250M-row heartbeat index). Instead a background task
//! (`processor_churn_collect_task` in main.rs) walks indexed blocks and
//! dedup-inserts each heartbeat signer into the quarter and year bucket of its
//! `block_time`, and the RPC answers with a trivial indexed count over
//! `processor_active_bucket`.
//!
//! The collector is idempotent and monotonic (`ON CONFLICT DO NOTHING`, buckets
//! only gain members), so re-scanning is safe and the counts converge to the true
//! value as blocks get indexed — deliberately only *eventually correct*, fully
//! decoupled from the epoch pipeline.
//!
//! Heartbeat = `AcurastProcessorManager` event `pallet 41 / variant 6`
//! (`ProcessorHeartbeatWithVersion`), whose signer is the processor — the same
//! definition as `get_processors_count_by_epoch`. Variant 6 covers both
//! `heartbeat_with_metrics` and `heartbeat_with_version` (the extrinsic was
//! renamed; the event stayed 6). Membership stores the bare-hex
//! `extrinsics.account_id` (no `accounts`/`is_processor` dependency), so counts
//! are complete regardless of flag state.

use anyhow::Result;
use sqlx::PgPool;

/// `bucket_kind` for a calendar quarter.
pub const BUCKET_QUARTER: i16 = 0;
/// `bucket_kind` for a calendar year.
pub const BUCKET_YEAR: i16 = 1;

/// Dedup-insert distinct heartbeat signers whose event block is in
/// `(from_block, to_block]` into the calendar quarter and year bucket of each
/// heartbeat's own `block_time`. Idempotent.
pub async fn collect_active_processors_for_range(
    pool: &PgPool,
    from_block: i64,
    to_block: i64,
) -> Result<()> {
    sqlx::query(COLLECT_RANGE_SQL)
        .bind(from_block)
        .bind(to_block)
        .execute(pool)
        .await?;
    Ok(())
}

/// How far above `sealed` a single [`contiguous_frontier`] call will look.
///
/// The scan costs one index probe per contiguous block, so without a bound a
/// cold start (`sealed = 0` against a ~3.7M-block chain, or a lost
/// `_index_progress` row) spends millions of probes on *every* pass of the 60 s
/// collector loop until it catches up. Capping the lookahead makes each pass
/// O(1) in the size of the chain; the frontier still advances monotonically, just
/// in bounded steps, and the collector runs often enough to close a large gap
/// quickly.
const FRONTIER_LOOKAHEAD_BLOCKS: i64 = 1_000_000;

/// The highest block for which `(sealed, frontier]` is gap-free — i.e. the first
/// block above `sealed` whose successor is missing (or the cap, whichever comes
/// first). Used as the watermark so we never permanently seal past an unfilled
/// hole left by backwards / gap indexing. Returns `sealed` unchanged when no
/// block above it is indexed yet.
///
/// Bounded by [`FRONTIER_LOOKAHEAD_BLOCKS`]: when the range is fully contiguous
/// up to the cap, the cap is returned and the next pass continues from there.
/// That is safe because the result is only ever used as "everything up to here is
/// gap-free", which stays true of a prefix.
pub async fn contiguous_frontier(pool: &PgPool, sealed: i64) -> Result<i64> {
    // The run has to START at `sealed + 1`, otherwise there is nothing to seal.
    //
    // Without this check the query below advances the frontier ACROSS a hole that
    // sits immediately above `sealed`: with `sealed = 100`, blocks 101..199
    // missing and 200..300 present, the first block whose successor is absent is
    // 300, so the frontier jumped to 300 and blocks 101..199 ended up permanently
    // below the watermark. Their heartbeat signers would never be collected --
    // silently under-reporting `active` forever, not "eventually correct" as the
    // module doc intends. Gaps above the frontier are exactly what
    // queue_gaps/queue_backwards produce, so this was reachable.
    let next_present: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM blocks WHERE block_number = $1)")
            .bind(sealed.saturating_add(1))
            .fetch_one(pool)
            .await?;
    if !next_present {
        return Ok(sealed);
    }

    let limit = sealed.saturating_add(FRONTIER_LOOKAHEAD_BLOCKS);

    let frontier: Option<i64> = sqlx::query_scalar(
        "SELECT block_number FROM blocks b \
         WHERE block_number > $1 AND block_number <= $2 \
           AND NOT EXISTS (SELECT 1 FROM blocks WHERE block_number = b.block_number + 1) \
         ORDER BY block_number ASC LIMIT 1",
    )
    .bind(sealed)
    .bind(limit)
    .fetch_optional(pool)
    .await?;

    // `sealed + 1` exists and no block in the window ends a run, so the window is
    // gap-free all the way to the cap.
    Ok(frontier.unwrap_or(limit))
}

const COLLECT_RANGE_SQL: &str = "\
    INSERT INTO processor_active_bucket (bucket_kind, bucket_start, account_id) \
    SELECT DISTINCT b.kind, b.bstart, ex.account_id \
    FROM events ev \
    JOIN extrinsics ex \
      ON ex.block_number = ev.block_number AND ex.index = ev.extrinsic_index \
    CROSS JOIN LATERAL (VALUES (0::smallint, date_trunc('quarter', ex.block_time)), \
                               (1::smallint, date_trunc('year',    ex.block_time))) b(kind, bstart) \
    WHERE ev.pallet = 41 AND ev.variant = 6 \
      AND ev.block_number > $1 AND ev.block_number <= $2 \
      AND ex.account_id <> '' \
    ON CONFLICT (bucket_kind, bucket_start, account_id) DO NOTHING";
