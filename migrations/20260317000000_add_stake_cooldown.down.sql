-- Drop the indexes first
DROP INDEX IF EXISTS "commitments_active_cooldown_period_idx";
DROP INDEX IF EXISTS "commitments_cooldown_period_idx";

-- Drop the column
ALTER TABLE commitments
DROP COLUMN IF EXISTS cooldown_period;
