-- ============================================
-- EPOCH_TOTALS
-- Per-epoch rollup of network-wide locked/staked amounts, one row per epoch.
--
-- Populated by `storage_indexing::process_epoch_totals` at the
-- `AccountsMaterialized` -> `EpochTotalsComputed` transition of the epoch
-- pipeline (see src/entities/mod.rs EpochIndexPhase). Each total is a fresh
-- full aggregation over `storage_snapshots` evaluated at the epoch's end
-- block, because e.g. vesting decays with block height and cannot be carried
-- forward from the previous epoch.
--
-- Bumping the epoch pipeline MAX phase to EpochTotalsComputed re-queues every
-- historical epoch through the new phase (via queue_epochs_phase, bounded by
-- EpochIndexPhase::MAX), so historical rows are backfilled automatically with
-- no one-shot migration.
--
-- All amounts are raw on-chain integer units (NUMERIC(38,0)), no decimal shift.
-- Aggregation reuses existing storage_snapshots partial indexes
-- (storage_snapshots_pallet_vesting_key_idx, ..._token_claim_vesting_by_dest_idx,
-- ..._token_claim_multi_by_dest_idx, storage_snapshots_commitment_key_idx),
-- so no new index is required here.
-- ============================================

CREATE TABLE public.epoch_totals (
    epoch                 BIGINT PRIMARY KEY,
    -- The block the totals were evaluated at (epoch_end): the last block of
    -- the epoch, i.e. LEAD(epoch_start) OVER (ORDER BY epoch).
    block_number          BIGINT NOT NULL,
    block_time            TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Total remaining vesting locked in pallet_vesting (pallet 15).
    total_vesting         NUMERIC(38,0) NOT NULL DEFAULT 0,
    -- Total remaining token-claim locked in AcurastTokenClaim (pallet 55),
    -- Vesting + MultiVesting combined.
    total_token_claim     NUMERIC(38,0) NOT NULL DEFAULT 0,
    -- Total committer self-stake across all live commitments (pallet 48).
    total_self_staked     NUMERIC(38,0) NOT NULL DEFAULT 0,
    -- Total delegated across all live commitments (runtime-aggregated
    -- delegations_total_amount on the Commitment struct).
    total_delegated       NUMERIC(38,0) NOT NULL DEFAULT 0
);
