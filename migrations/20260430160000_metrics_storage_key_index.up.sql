-- no-transaction
-- Partial expression index for AcurastCompute.Metrics storage snapshot lookups.
-- Mirrors storage_snapshots_metrics_epoch_sum_key_idx and
-- storage_snapshots_system_account_key_idx, which cover the same access
-- pattern for other (pallet, storage_location) combinations.
--
-- Supports:
--   WHERE pallet = 48 AND storage_location = 'Metrics'
--     AND storage_keys->>0 = $1 [AND block_number BETWEEN ...]
--   ORDER BY block_number DESC
--
-- Used by getStorageSnapshots when filtering by processor account, and by
-- get_all_processor_metrics_from_db during epoch manager indexing.
CREATE INDEX CONCURRENTLY IF NOT EXISTS "storage_snapshots_metrics_key_idx"
ON storage_snapshots ((storage_keys->>0), block_number DESC)
WHERE pallet = 48 AND storage_location = 'Metrics';
