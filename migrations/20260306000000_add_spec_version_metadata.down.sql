-- Remove metadata column from spec_versions table

ALTER TABLE spec_versions
DROP COLUMN IF EXISTS metadata;
