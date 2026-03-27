-- Revert: Rename the index back
ALTER INDEX commitments_min_max_weight_per_compute_idx
RENAME TO commitments_target_weight_per_compute_idx;

-- Revert: Rename min_max_weight_per_compute back to target_weight_per_compute
ALTER TABLE commitments
RENAME COLUMN min_max_weight_per_compute TO target_weight_per_compute;
