-- Enable btree_gin for composite GIN indexes with scalar types
CREATE EXTENSION IF NOT EXISTS btree_gin;

-- ============================================
-- BLOCKS
-- ============================================
CREATE TABLE public.blocks (
    block_number bigint PRIMARY KEY,
    "hash" text NOT NULL,
    block_time timestamp with time zone NOT NULL
);
CREATE INDEX "blocks_hash_idx" ON public.blocks USING hash ("hash");
CREATE INDEX "blocks_block_time_idx" ON public.blocks USING btree (block_time DESC);

-- ============================================
-- SPEC VERSIONS
-- ============================================
CREATE TABLE public.spec_versions (
    spec_version int PRIMARY KEY,
    block_number bigint NOT NULL,
    block_time timestamp with time zone NOT NULL
);
CREATE INDEX "spec_versions_block_number_idx" ON public.spec_versions USING btree (block_number DESC);

-- ============================================
-- EXTRINSICS
-- Composite primary key: (block_number, index)
-- ============================================
CREATE TABLE public.extrinsics (
    block_number bigint NOT NULL,
    index integer NOT NULL,
    pallet integer NOT NULL,
    method integer NOT NULL,
    data jsonb NULL,
    tx_hash text NOT NULL,
    account_id text NOT NULL,
    block_time timestamp with time zone NOT NULL,
    phase integer NOT NULL DEFAULT 0,
    PRIMARY KEY (block_number, index)
);

-- Indexes: filter by pallet/method first, then order by block_number, index
CREATE INDEX "extrinsics_pallet_idx" ON public.extrinsics USING btree (pallet, block_number DESC, index DESC);
CREATE INDEX "extrinsics_pallet_method_idx" ON public.extrinsics USING btree (pallet, method, block_number DESC, index DESC);
CREATE INDEX "extrinsics_account_id_idx" ON public.extrinsics USING btree (account_id, block_number DESC, index DESC);
CREATE INDEX "extrinsics_tx_hash_idx" ON public.extrinsics USING hash (tx_hash);
CREATE INDEX "extrinsics_phase_idx" ON public.extrinsics USING btree (phase, block_number DESC, index DESC);
CREATE INDEX "extrinsics_block_time_idx" ON public.extrinsics USING btree (block_time DESC);

-- ============================================
-- EVENTS
-- ============================================
CREATE TABLE events (
    block_number bigint NOT NULL,
    extrinsic_index integer NOT NULL,
    index integer NOT NULL,
    pallet integer NOT NULL,
    variant integer NOT NULL,
    data jsonb NULL,
    phase integer NOT NULL DEFAULT 0,
    error text NULL,
    block_time timestamp with time zone NOT NULL,
    PRIMARY KEY (block_number, extrinsic_index, index)
);

-- Indexes: filter by pallet/variant first, then order by block_number, extrinsic_index, index
CREATE INDEX "events_pallet_idx" ON public.events USING btree (pallet, block_number DESC, extrinsic_index DESC, index DESC);
CREATE INDEX "events_pallet_variant_idx" ON public.events USING btree (pallet, variant, block_number DESC, extrinsic_index DESC, index DESC);
CREATE INDEX "events_phase_idx" ON public.events USING btree (phase, block_number DESC, extrinsic_index DESC, index DESC);
CREATE INDEX "events_block_number_idx" ON public.events USING btree (block_number DESC, extrinsic_index DESC, index DESC);

-- ============================================
-- JOBS
-- ============================================
CREATE TYPE target_chain AS ENUM (
    'Acurast',
    'Tezos',
    'Ethereum',
    'AlephZero',
    'Vara',
    'Ethereum20',
    'Solana'
);

CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    block_number bigint NOT NULL,
    extrinsic_index integer NOT NULL,
    event_index integer NOT NULL,
    data_path text NOT NULL,
    chain target_chain NOT NULL,
    address text NOT NULL,
    seq_id integer NOT NULL,
    block_time timestamp with time zone NOT NULL,
    UNIQUE (block_number, extrinsic_index, event_index, data_path)
);

CREATE INDEX "jobs_block_number_idx" ON jobs USING btree (block_number DESC);
CREATE INDEX "jobs_chain_seq_id_idx" ON jobs (chain, seq_id, block_number DESC);
CREATE INDEX "jobs_address_idx" ON jobs (address, block_number DESC);

-- ============================================
-- EXTRINSIC_ADDRESS
-- For address extraction from extrinsics
-- ============================================
CREATE TABLE extrinsic_address (
    id SERIAL PRIMARY KEY,
    block_number bigint NOT NULL,
    extrinsic_index integer NOT NULL,
    batch_index integer NULL,
    data_path text NOT NULL,
    resolved_data_path text NOT NULL,
    account_id text NOT NULL,
    pallet integer NOT NULL,
    method integer NOT NULL,
    block_time timestamp with time zone NOT NULL,
    UNIQUE (block_number, extrinsic_index, batch_index, resolved_data_path)
);

CREATE INDEX "extrinsic_address_block_number_idx" ON extrinsic_address USING btree (block_number DESC);
CREATE INDEX "extrinsic_address_block_time_idx" ON extrinsic_address (block_time DESC);
CREATE INDEX "extrinsic_address_account_id_idx" ON extrinsic_address USING btree (account_id, block_number DESC);
CREATE INDEX "extrinsic_address_account_id_pallet_idx" ON extrinsic_address USING btree (account_id, pallet, block_number DESC);
CREATE INDEX "extrinsic_address_account_id_pallet_method_idx" ON extrinsic_address USING btree (account_id, pallet, method, block_number DESC);

-- ============================================
-- STORAGE_SNAPSHOTS
-- Captures on-chain storage values at specific extrinsic execution points
-- ============================================
CREATE TABLE public.storage_snapshots (
    id BIGSERIAL PRIMARY KEY,
    block_number bigint NOT NULL,
    extrinsic_index integer NOT NULL,
    event_index integer NULL,
    block_time timestamp with time zone NOT NULL,
    pallet integer NOT NULL,
    storage_location text NOT NULL,
    storage_keys jsonb NOT NULL DEFAULT '[]',
    data jsonb NOT NULL,
    config_rule text NOT NULL,
    UNIQUE(block_number, extrinsic_index, event_index, pallet, storage_location, storage_keys)
);

CREATE INDEX "storage_snapshots_block_time_idx" ON storage_snapshots (block_time DESC);
CREATE INDEX "storage_snapshots_event_idx" ON storage_snapshots (block_number DESC, extrinsic_index, event_index);
CREATE INDEX "storage_snapshots_pallet_location_keys_idx" ON storage_snapshots USING gin (pallet, storage_location, storage_keys);
CREATE INDEX "storage_snapshots_pallet_location_data_idx" ON storage_snapshots USING gin (pallet, storage_location, data);
CREATE INDEX "storage_snapshots_data_idx" ON storage_snapshots USING gin (data);
CREATE INDEX "storage_snapshots_pallet_storage_block_idx" ON storage_snapshots (pallet, storage_location, block_number DESC);
CREATE INDEX "storage_snapshots_config_rule_idx" ON storage_snapshots (config_rule, block_number DESC);
-- Partial index for efficient NOT EXISTS queries to find subsequent null data snapshots
CREATE INDEX "storage_snapshots_null_data_idx" ON storage_snapshots (pallet, storage_location, storage_keys, block_number DESC) WHERE data = 'null'::jsonb;

-- ============================================
-- EPOCHS
-- ============================================
CREATE TABLE public.epochs (
    epoch BIGSERIAL PRIMARY KEY,
    epoch_start bigint NOT NULL,
    epoch_start_time timestamp with time zone NOT NULL,
    phase integer NOT NULL DEFAULT 0
);

CREATE INDEX "epochs_epoch_start_idx" ON epochs (epoch_start DESC);
CREATE INDEX "epochs_epoch_start_time_idx" ON epochs (epoch_start_time DESC);
CREATE INDEX "epochs_phase_idx" ON public.epochs (phase);

-- ============================================
-- MANAGERS
-- Epoch-based snapshots of manager processor assignments and metrics
-- ============================================
CREATE TABLE public.managers (
    id BIGSERIAL PRIMARY KEY,
    epoch bigint NOT NULL,
    block_number bigint NOT NULL,
    block_time timestamp with time zone NOT NULL,
    manager_id bigint NOT NULL,
    manager_address text NOT NULL,
    commitment_id bigint NULL,
    processors jsonb NOT NULL DEFAULT '[]',
    UNIQUE(epoch, manager_id)
);

CREATE INDEX "managers_epoch_idx" ON managers (epoch DESC);
CREATE INDEX "managers_block_number_idx" ON managers (block_number DESC);
CREATE INDEX "managers_manager_id_idx" ON managers (manager_id, epoch DESC);
CREATE INDEX "managers_manager_address_idx" ON managers (manager_address, epoch DESC);
CREATE INDEX "managers_commitment_id_idx" ON managers (commitment_id, epoch DESC) WHERE commitment_id IS NOT NULL;
CREATE INDEX "managers_processors_idx" ON managers USING gin (processors);
