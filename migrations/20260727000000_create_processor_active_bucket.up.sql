-- ============================================
-- PROCESSOR_ACTIVE_BUCKET (create)
-- The table's create statement was missing from
-- 20260716000000_processor_active_bucket.up.sql (that migration only dropped the
-- superseded processor_churn table), so getProcessorChurn failed with
-- "relation processor_active_bucket does not exist". This migration creates it.
-- Idempotent (IF NOT EXISTS) so it is safe on DBs that never lost the table.
--
-- One row per (fixed calendar bucket, distinct heartbeat signer). Filled
-- incrementally by the churn collector (see src/processor_churn.rs): the distinct
-- heartbeat signers (pallet 41 / event variant 6) are dedup-inserted into the
-- calendar quarter and year bucket they fall in. The RPC then answers "distinct
-- active processors in a bucket" with a trivial indexed count instead of ~100k
-- random probes into the ~250M-row heartbeat index.
--
--   bucket_kind:  0 = calendar quarter, 1 = calendar year
--   bucket_start: date_trunc('quarter'|'year', block_time) (UTC, timestamptz)
--   account_id:   bare-hex extrinsics.account_id (no accounts/is_processor join,
--                 so counts are complete regardless of flag backfill state)
--
-- The PK prefix (bucket_kind, bucket_start) already serves the count query, so
-- no separate index is needed.
-- ============================================
CREATE TABLE IF NOT EXISTS public.processor_active_bucket (
    bucket_kind  smallint NOT NULL,
    bucket_start timestamp with time zone NOT NULL,
    account_id   text NOT NULL,
    PRIMARY KEY (bucket_kind, bucket_start, account_id)
);
