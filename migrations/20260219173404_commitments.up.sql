-- ============================================
-- COMMITMENTS
-- Denormalized table for fast commitment queries
-- Updated by post-processing storage_snapshots
-- ============================================
CREATE TABLE public.commitments (
    id BIGSERIAL PRIMARY KEY,
    commitment_id BIGINT NOT NULL UNIQUE,

    -- Snapshot reference (no FK to avoid coupling)
    snapshot_id BIGINT NULL,  -- storage_snapshots.id that last updated this row
    block_number BIGINT NOT NULL,
    block_time TIMESTAMP WITH TIME ZONE NOT NULL,
    epoch BIGINT NOT NULL,

    -- Ownership
    committer_address TEXT NOT NULL,  -- SS58 address (owner of commitment NFT in Uniques collection 1)
    manager_id BIGINT,                -- From Backings storage lookup
    manager_address TEXT,             -- SS58 address (owner of manager NFT in Uniques collection 0)

    -- Core commitment data (extracted from JSON)
    commission NUMERIC(38,0) NOT NULL DEFAULT 0,  -- data.commission[0]
    stake_amount NUMERIC(38,0) NOT NULL DEFAULT 0,  -- data.stake.amount
    stake_rewardable_amount NUMERIC(38,0) NOT NULL DEFAULT 0,  -- data.stake.rewardable_amount
    stake_accrued_reward NUMERIC(38,0) NOT NULL DEFAULT 0,  -- data.stake.accrued_reward
    stake_paid NUMERIC(38,0) NOT NULL DEFAULT 0,  -- data.stake.paid
    delegations_total_amount NUMERIC(38,0) NOT NULL DEFAULT 0,
    delegations_total_rewardable_amount NUMERIC(38,0) NOT NULL DEFAULT 0,

    -- Epoch tracking
    last_scoring_epoch BIGINT NOT NULL DEFAULT 0,
    last_slashing_epoch BIGINT NOT NULL DEFAULT 0,
    stake_created_epoch BIGINT NOT NULL DEFAULT 0,  -- data.stake.created

    -- Status
    cooldown_started BIGINT,  -- data.stake.cooldown_started (null if not in cooldown)
    is_active BOOLEAN NOT NULL DEFAULT TRUE,  -- stake is not null

    -- max_delegation_capacity = self_slash_weight * 9
    -- Used in: delegation_utilization = delegations_reward_weight / max_delegation_capacity
    max_delegation_capacity NUMERIC(38,0),

    -- target_weight_per_compute = min(target_weight_per_compute[pool] * committed_metric[pool])
    -- Used in: target_weight_per_compute_utilization = total_reward_weight / target_weight_per_compute
    target_weight_per_compute NUMERIC(78,0),

    -- Utilization metrics (stored as Perbill: 1_000_000_000 = 100%)
    delegation_utilization NUMERIC(38,0),  -- 0 to 1_000_000_000 (0%-100%)
    target_weight_per_compute_utilization NUMERIC(38,0),  -- 0 to 10_000_000_000 (0%-1000%, can exceed 100%)
    combined_utilization NUMERIC(38,0),  -- 0 to 1_000_000_000 (0%-100%)

    -- Remaining capacity as absolute weight (not relative)
    -- min(max_delegation_capacity - delegations_reward_weight, target_weight_per_compute - total_reward_weight)
    remaining_capacity NUMERIC(78,0),

    -- Weights (resolved from current/past based on epoch match)
    -- Each weight is the first element [0] of the weight array
    delegations_reward_weight NUMERIC(38,0) NOT NULL DEFAULT 0,
    delegations_slash_weight NUMERIC(38,0) NOT NULL DEFAULT 0,
    self_reward_weight NUMERIC(38,0) NOT NULL DEFAULT 0,
    self_slash_weight NUMERIC(38,0) NOT NULL DEFAULT 0,

    -- Pool rewards (from pool_rewards.current, U256 values)
    -- reward_per_weight is [u64;4] limbs, slash_per_weight is hex string - both are U256
    reward_per_weight NUMERIC(78,0) NOT NULL DEFAULT 0,
    slash_per_weight NUMERIC(78,0) NOT NULL DEFAULT 0,

    -- ComputeCommitments data per pool: { "pool_id": metric_value, ... }
    -- Fetched from AcurastCompute::ComputeCommitments storage
    committed_metrics JSONB,

    -- Processing phase
    phase INTEGER NOT NULL DEFAULT 0
);

-- Primary lookup indexes
CREATE INDEX "commitments_commitment_id_idx" ON commitments (commitment_id);
CREATE INDEX "commitments_committer_address_idx" ON commitments (committer_address);
CREATE INDEX "commitments_manager_id_idx" ON commitments (manager_id) WHERE manager_id IS NOT NULL;
CREATE INDEX "commitments_manager_address_idx" ON commitments (manager_address) WHERE manager_address IS NOT NULL;
CREATE INDEX "commitments_epoch_idx" ON commitments (epoch DESC);
CREATE INDEX "commitments_block_number_idx" ON commitments (block_number DESC);

-- Ordering indexes for RPC queries (all numeric columns, includes commitment_id for cursor pagination)
CREATE INDEX "commitments_stake_amount_idx" ON commitments (stake_amount DESC, commitment_id DESC);
CREATE INDEX "commitments_stake_rewardable_amount_idx" ON commitments (stake_rewardable_amount DESC, commitment_id DESC);
CREATE INDEX "commitments_delegations_total_amount_idx" ON commitments (delegations_total_amount DESC, commitment_id DESC);
CREATE INDEX "commitments_commission_idx" ON commitments (commission DESC, commitment_id DESC);
CREATE INDEX "commitments_max_delegation_capacity_idx" ON commitments (max_delegation_capacity DESC NULLS LAST, commitment_id DESC);
CREATE INDEX "commitments_target_weight_per_compute_idx" ON commitments (target_weight_per_compute DESC NULLS LAST, commitment_id DESC);
CREATE INDEX "commitments_delegation_utilization_idx" ON commitments (delegation_utilization DESC NULLS LAST, commitment_id DESC);
CREATE INDEX "commitments_target_weight_per_compute_utilization_idx" ON commitments (target_weight_per_compute_utilization DESC NULLS LAST, commitment_id DESC);
CREATE INDEX "commitments_combined_utilization_idx" ON commitments (combined_utilization DESC NULLS LAST, commitment_id DESC);
CREATE INDEX "commitments_remaining_capacity_idx" ON commitments (remaining_capacity DESC NULLS LAST, commitment_id DESC);

-- Active commitments with various orderings (most common queries)
CREATE INDEX "commitments_active_stake_idx" ON commitments (stake_amount DESC, commitment_id DESC) WHERE is_active = TRUE;
CREATE INDEX "commitments_active_remaining_capacity_idx" ON commitments (remaining_capacity DESC NULLS LAST, commitment_id DESC) WHERE is_active = TRUE;
CREATE INDEX "commitments_active_combined_utilization_idx" ON commitments (combined_utilization DESC NULLS LAST, commitment_id DESC) WHERE is_active = TRUE;
CREATE INDEX "commitments_active_delegation_utilization_idx" ON commitments (delegation_utilization DESC NULLS LAST, commitment_id DESC) WHERE is_active = TRUE;
CREATE INDEX "commitments_active_delegations_total_idx" ON commitments (delegations_total_amount DESC, commitment_id DESC) WHERE is_active = TRUE;

-- Phase processing
CREATE INDEX "commitments_phase_idx" ON commitments (phase);

-- Index on storage_snapshots for efficient commitment processing
-- Supports DISTINCT ON (storage_keys->0->>0) ORDER BY storage_keys->0->>0, block_number DESC
-- Note: storage_keys contains nested arrays like [["449"]] so we use ->0->>0
CREATE INDEX "storage_snapshots_commitment_key_idx"
ON storage_snapshots ((storage_keys->0->>0), block_number DESC)
WHERE pallet = 48 AND storage_location = 'Commitments';
