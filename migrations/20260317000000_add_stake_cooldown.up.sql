-- Add cooldown_period column to commitments table
-- Represents the cooldown period duration (in epochs or blocks)
ALTER TABLE commitments
ADD COLUMN cooldown_period BIGINT;

-- Create index for sorting by cooldown_period (DESC NULLS LAST pattern)
CREATE INDEX "commitments_cooldown_period_idx"
ON commitments (cooldown_period DESC NULLS LAST, commitment_id DESC);

-- Create filtered index for active commitments
CREATE INDEX "commitments_active_cooldown_period_idx"
ON commitments (cooldown_period DESC NULLS LAST, commitment_id DESC)
WHERE is_active = TRUE;
