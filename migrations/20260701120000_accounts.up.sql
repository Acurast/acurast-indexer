-- ============================================
-- ACCOUNTS
-- Denormalized "current state" table for per-account balances and roles.
--
-- Balance and external-lock columns (free/reserved/frozen/flags,
-- transferable, remaining_vesting, remaining_token_claim) are
-- (re)materialized at each epoch boundary by
-- `storage_indexing::process_epoch_accounts_materialization`, which runs
-- at the `StorageIndexed3` → `AccountsMaterialized` (=4) transition of
-- the epoch pipeline. Historical epochs at earlier phases get naturally
-- advanced to 4 by the epoch queuer, materialising accounts as they go —
-- no per-migration balance backfill is needed.
--
-- Role flags (is_processor / is_manager / is_committer) are set live by
-- `storage_indexing::flag_processor` (main.rs phase-4 event hook for
-- pallet 41 variants 3, 6, 13), `flag_manager` (epoch.rs manager insert +
-- commitment processing), and `flag_committer` (commitments.rs upsert). They
-- latch to TRUE. There is NO migration-time backfill — the live pipeline
-- populates all three; see the ROLE FLAGS note below.
--
-- `account_id` is normalized to 0x-prefixed lowercase hex at every write
-- site via `utils::normalize_address_with_prefix`. Historical source
-- rows are already lowercase hex (`hex::encode` output); only
-- `managers.manager_address` lacks the 0x prefix, which the CASE below
-- adds inline.
-- ============================================

CREATE TABLE public.accounts (
    id                        BIGSERIAL PRIMARY KEY,
    account_id                TEXT NOT NULL,
    UNIQUE(account_id),

    -- Snapshot metadata (latest observed)
    block_number              BIGINT NOT NULL,
    block_time                TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Raw AccountData (from System.Account.data)
    free                      NUMERIC(38,0) NOT NULL DEFAULT 0,
    reserved                  NUMERIC(38,0) NOT NULL DEFAULT 0,
    frozen                    NUMERIC(38,0) NOT NULL DEFAULT 0,
    flags                     NUMERIC(39,0) NOT NULL DEFAULT 0,

    -- Generated: transferable = max(free - max(frozen - reserved, 0), 0)
    -- Formula from acurast-hub balance-history-chart.component.ts:208
    transferable              NUMERIC(38,0) GENERATED ALWAYS AS
        (GREATEST(free - GREATEST(frozen - reserved, 0::NUMERIC), 0::NUMERIC)) STORED,

    -- Locked in external pallets (updated by the epoch materializer)
    remaining_vesting         NUMERIC(38,0) NOT NULL DEFAULT 0,
    remaining_token_claim     NUMERIC(38,0) NOT NULL DEFAULT 0,

    -- Role flags (latch to TRUE)
    is_processor              BOOLEAN NOT NULL DEFAULT FALSE,
    is_manager                BOOLEAN NOT NULL DEFAULT FALSE,
    is_committer              BOOLEAN NOT NULL DEFAULT FALSE
);

-- Ordering indexes
CREATE INDEX accounts_transferable_idx    ON accounts (transferable DESC);
CREATE INDEX accounts_total_idx           ON accounts ((free + reserved) DESC);
CREATE INDEX accounts_total_external_idx  ON accounts
    ((free + reserved + remaining_vesting + remaining_token_claim) DESC);
CREATE INDEX accounts_free_idx            ON accounts (free DESC);
CREATE INDEX accounts_reserved_idx        ON accounts (reserved DESC);
CREATE INDEX accounts_frozen_idx          ON accounts (frozen DESC);

-- Partial indexes for role listings
CREATE INDEX accounts_is_processor_idx    ON accounts (account_id) WHERE is_processor;
CREATE INDEX accounts_is_manager_idx      ON accounts (account_id) WHERE is_manager;
CREATE INDEX accounts_is_committer_idx    ON accounts (account_id) WHERE is_committer;

-- NOTE: the former "materializer support" indexes (events_processor_recipient_idx
-- and the pallet-15/55 storage_snapshots vesting/token-claim indexes) were
-- removed — they have no remaining consumer. The per-account materializer now
-- reads System.Account only, and vesting / token-claim totals are computed by
-- iterating chain state directly (`epoch_totals::compute_totals_at_block`), not
-- by aggregating over `storage_snapshots`.

-- ============================================
-- ROLE FLAGS: no migration-time backfill.
-- ============================================
-- is_processor / is_manager / is_committer are all populated by the live
-- indexing pipeline and require no backfill here:
--   - is_processor: `flag_processor` in the phase-4 event hook (pallet 41
--     variants 3/6/13), which sweeps the whole historical backlog when the
--     event MAX bump advances existing rows through phase 4.
--   - is_committer / is_manager: `flag_committer` / `flag_manager` in
--     `process_single_commitment`, re-fired on every commitment scan — incl.
--     the full `scan_all_commitments_at_block` that `commitment_processing_task`
--     runs at startup and each epoch boundary — plus `flag_manager` in the
--     epoch manager materialization.
-- A previous events-table `is_processor` backfill was removed because it
-- scanned millions of heartbeat rows and timed out; the manager/committer
-- backfills were removed for consistency since the live pipeline supersedes
-- them within seconds of startup.
