-- Rename target_weight_per_compute to min_max_weight_per_compute in commitments table
ALTER TABLE commitments
RENAME COLUMN target_weight_per_compute TO min_max_weight_per_compute;

-- Rename the index to match the new column name
ALTER INDEX commitments_target_weight_per_compute_idx
RENAME TO commitments_min_max_weight_per_compute_idx;
