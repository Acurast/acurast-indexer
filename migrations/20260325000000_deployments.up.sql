-- ============================================
-- DEPLOYMENTS
-- Denormalized table for fast deployment/job queries
-- Populated from JobRegistrationStoredV2 events
-- ============================================
CREATE TABLE public.deployments (
    id BIGSERIAL PRIMARY KEY,

    -- Primary identifiers (reuses existing target_chain enum)
    chain target_chain NOT NULL,
    address TEXT NOT NULL,              -- deployer address (hex with 0x prefix)
    seq_id BIGINT NOT NULL,
    UNIQUE(chain, address, seq_id),

    -- Snapshot reference (no FK to avoid coupling)
    snapshot_id BIGINT NULL,
    block_number BIGINT NOT NULL,
    block_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Creation tracking (first seen)
    created_block_number BIGINT NOT NULL,
    created_block_time TIMESTAMP WITH TIME ZONE NOT NULL,

    -- Schedule columns (all u64 milliseconds)
    schedule_duration BIGINT NOT NULL,
    schedule_start_time BIGINT NOT NULL,
    schedule_end_time BIGINT NOT NULL,
    schedule_interval BIGINT NOT NULL,
    schedule_max_start_delay BIGINT NOT NULL,

    -- Specs - core fields from JobRegistration
    allowed_sources JSONB,                      -- array of addresses (optional)
    allow_only_verified_sources BOOLEAN NOT NULL DEFAULT FALSE,
    memory BIGINT NOT NULL,                     -- bytes
    network_requests INTEGER NOT NULL,
    storage_capacity BIGINT NOT NULL,           -- bytes (renamed from 'storage' to avoid SQL keyword)
    required_modules TEXT[] NOT NULL DEFAULT '{}', -- JobModule enum values as strings
    slots INTEGER NOT NULL,
    reward NUMERIC(38,0) NOT NULL,              -- u128 as decimal
    assignment_strategy TEXT NOT NULL,          -- "Single" or "Competing"
    planned_executions JSONB,                   -- optional, from Single variant
    script TEXT NOT NULL,                       -- ipfs:// or other URI
    min_reputation NUMERIC(38,0),               -- optional u128
    processor_version JSONB,                    -- {"Min": [...]} or null
    runtime TEXT NOT NULL,                      -- "NodeJS", "NodeJSWithBundle", "Shell"

    -- Status tracking
    is_active BOOLEAN NOT NULL DEFAULT TRUE,    -- FALSE when removed

    -- Processing phase
    phase INTEGER NOT NULL DEFAULT 0
);

-- ============================================
-- INDEXES
-- Non-redundant indexes for filtering and sorting
-- ============================================

-- Primary lookup by deployer address
CREATE INDEX deployments_chain_address_idx ON deployments (chain, address, block_number DESC);

CREATE INDEX deployments_chain_address_seq_id_idx ON deployments (chain, address, seq_id);

-- Sorting indexes with seq_id for stable keyset pagination
CREATE INDEX deployments_block_number_idx ON deployments (block_number DESC, seq_id DESC);
CREATE INDEX deployments_created_block_number_idx ON deployments (created_block_number DESC, seq_id DESC);
CREATE INDEX deployments_start_time_idx ON deployments (schedule_start_time DESC, seq_id DESC);

-- Processing phase
CREATE INDEX deployments_phase_idx ON deployments (phase);

-- Partial index for active deployments (common filter)
CREATE INDEX deployments_active_start_time_idx ON deployments (schedule_start_time DESC, seq_id DESC)
    WHERE is_active = TRUE;
