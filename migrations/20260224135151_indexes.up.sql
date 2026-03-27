-- Index for MetricsEpochSum LATERAL JOIN in getCommitments
-- Supports: WHERE storage_keys->>0 = manager_id ORDER BY block_number DESC LIMIT 1
-- Note: MetricsEpochSum keys are ["manager_id", "pool_id"] (flat array), so we use ->>0
CREATE INDEX "storage_snapshots_metrics_epoch_sum_key_idx"
ON storage_snapshots ((storage_keys->>0), block_number DESC)
WHERE pallet = 48 AND storage_location = 'MetricsEpochSum';

-- Index for general storage snapshot queries without pallet/location filter
-- Supports: ORDER BY block_number DESC, id DESC with cursor pagination
CREATE INDEX "storage_snapshots_block_id_idx"
ON storage_snapshots (block_number DESC, id DESC);

-- Improved index for pallet+location queries with proper cursor support
-- Replaces the need for secondary sort step when using id-based cursor pagination
CREATE INDEX "storage_snapshots_pallet_storage_block_id_idx"
ON storage_snapshots (pallet, storage_location, block_number DESC, id DESC);

-- Drop obsolete index (the new index above is a superset)
DROP INDEX IF EXISTS "storage_snapshots_pallet_storage_block_idx";