-- Drop the new indexes
DROP INDEX IF EXISTS "storage_snapshots_metrics_epoch_sum_key_idx";
DROP INDEX IF EXISTS "storage_snapshots_block_id_idx";
DROP INDEX IF EXISTS "storage_snapshots_pallet_storage_block_id_idx";

-- Recreate the original index that was dropped
CREATE INDEX "storage_snapshots_pallet_storage_block_idx"
ON storage_snapshots (pallet, storage_location, block_number DESC);
