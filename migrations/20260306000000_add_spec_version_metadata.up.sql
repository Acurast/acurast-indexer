-- Add metadata column to spec_versions table
-- This column will store the raw SCALE-encoded metadata bytes

ALTER TABLE spec_versions
ADD COLUMN metadata BYTEA NULL;
