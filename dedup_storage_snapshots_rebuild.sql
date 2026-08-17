-- Faster alternative to dedup_storage_snapshots.sql.
--
-- Builds a deduped copy of storage_snapshots in a side table, then swaps it
-- in atomically. Much faster than chunked DELETE on a 33M+ duplicate set
-- because:
--   - Single seq scan + sort vs. repeated row_number() windows over the table.
--   - Bulk INSERT with no index maintenance during the load.
--   - Indexes built once at the end on the final dataset (parallel where
--     possible) rather than maintained per row.
--   - No dead tuples to vacuum afterward — the old table is dropped.
--
-- REQUIREMENTS:
--   - Indexer (writer) MUST be stopped. API ideally paused too — the swap
--     takes a brief ACCESS EXCLUSIVE lock and reopens with a fresh OID,
--     which can invalidate prepared plans on other connections.
--   - Disk: temporarily needs ~the table's worth of extra space until the
--     old table is dropped.
--   - PostgreSQL 12+.
--
-- This script LEAVES THE LEGACY UNIQUE CONSTRAINT (NULLS DISTINCT) IN PLACE.
-- The follow-up sqlx migration (20260504120000_storage_snapshots_unique_nulls_not_distinct)
-- swaps it to NULLS NOT DISTINCT and adds the new dedup-supporting index.
-- Running this script first then the migration is the intended sequence.
--
-- Usage:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/dedup_storage_snapshots_rebuild.sql

\timing on

SET maintenance_work_mem = '4GB';
SET max_parallel_maintenance_workers = 8;
SET work_mem = '1GB';
SET synchronous_commit = off;

-- Phase 1: side table, columns only (no indexes, no constraints).
\echo '== Phase 1: create side table =='
DROP TABLE IF EXISTS public.storage_snapshots_new;
CREATE TABLE public.storage_snapshots_new (
    LIKE public.storage_snapshots INCLUDING DEFAULTS INCLUDING COMMENTS
);

-- Phase 2: bulk dedup INSERT. DISTINCT ON treats NULL as equal in the
-- partition (same as the future NULLS NOT DISTINCT semantics). Within each
-- group, ORDER BY ... id DESC keeps the row with the highest id (most
-- recent insert; presumed most up-to-date data).
\echo '== Phase 2: bulk INSERT deduped rows (heavy step) =='
INSERT INTO public.storage_snapshots_new
SELECT DISTINCT ON (
    block_number, extrinsic_index, event_index, epoch_index, epoch_end,
    pallet, storage_location, storage_keys
) *
FROM public.storage_snapshots
ORDER BY block_number, extrinsic_index, event_index, epoch_index, epoch_end,
         pallet, storage_location, storage_keys, id DESC;

-- Sanity: row counts.
\echo '== Row counts =='
SELECT (SELECT count(*) FROM public.storage_snapshots)     AS old_rows,
       (SELECT count(*) FROM public.storage_snapshots_new) AS new_rows,
       (SELECT count(*) FROM public.storage_snapshots)
       - (SELECT count(*) FROM public.storage_snapshots_new) AS deleted_rows;

-- Phase 3: build PK, legacy UNIQUE, and every index. Names use a *_new_*
-- pattern so they don't collide with the live table; renamed in Phase 4.
\echo '== Phase 3: build PK + unique + all indexes on side table =='

ALTER TABLE public.storage_snapshots_new
    ADD CONSTRAINT storage_snapshots_new_pkey PRIMARY KEY (id);

ALTER TABLE public.storage_snapshots_new
    ADD CONSTRAINT storage_snapshots_new_legacy_unique UNIQUE (
        block_number, extrinsic_index, event_index, epoch_index,
        epoch_end, pallet, storage_location, storage_keys
    );

CREATE INDEX storage_snapshots_new_block_time_idx
    ON public.storage_snapshots_new (block_time DESC);
CREATE INDEX storage_snapshots_new_event_idx
    ON public.storage_snapshots_new (block_number DESC, extrinsic_index, event_index);
CREATE INDEX storage_snapshots_new_epoch_idx
    ON public.storage_snapshots_new (epoch_index DESC, epoch_end)
    WHERE epoch_index IS NOT NULL;
CREATE INDEX storage_snapshots_new_pallet_location_keys_idx
    ON public.storage_snapshots_new USING gin (pallet, storage_location, storage_keys);
CREATE INDEX storage_snapshots_new_pallet_location_data_idx
    ON public.storage_snapshots_new USING gin (pallet, storage_location, data, config_rule);
CREATE INDEX storage_snapshots_new_data_idx
    ON public.storage_snapshots_new USING gin (data);
CREATE INDEX storage_snapshots_new_config_rule_idx
    ON public.storage_snapshots_new (config_rule, block_number DESC);
CREATE INDEX storage_snapshots_new_null_data_idx
    ON public.storage_snapshots_new (pallet, storage_location, storage_keys, block_number DESC)
    WHERE data = 'null'::jsonb;
CREATE INDEX storage_snapshots_new_metrics_epoch_sum_key_idx
    ON public.storage_snapshots_new ((storage_keys->>0), block_number DESC)
    WHERE pallet = 48 AND storage_location = 'MetricsEpochSum';
CREATE INDEX storage_snapshots_new_system_account_key_idx
    ON public.storage_snapshots_new ((storage_keys->>0), block_number DESC)
    WHERE pallet = 0 AND storage_location = 'Account';
CREATE INDEX storage_snapshots_new_block_id_idx
    ON public.storage_snapshots_new (block_number DESC, id DESC);
CREATE INDEX storage_snapshots_new_pallet_storage_block_id_idx
    ON public.storage_snapshots_new (pallet, storage_location, block_number DESC, id DESC);
CREATE INDEX storage_snapshots_new_metrics_key_idx
    ON public.storage_snapshots_new ((storage_keys->>0), block_number DESC)
    WHERE pallet = 48 AND storage_location = 'Metrics';
CREATE INDEX storage_snapshots_new_commitment_key_idx
    ON public.storage_snapshots_new ((storage_keys->0->>0), block_number DESC)
    WHERE pallet = 48 AND storage_location = 'Commitments';

-- Phase 4: atomic swap inside one short ACCESS EXCLUSIVE window.
-- Steps:
--   a. Detach the BIGSERIAL sequence from the old column so DROP TABLE
--      doesn't take it (the sequence is preserved across the swap so its
--      next-id state stays consistent).
--   b. Drop old table.
--   c. Rename side table + its constraints/indexes to canonical names.
--   d. Re-attach sequence to the new column and bump it past max(id).
\echo '== Phase 4: swap =='
BEGIN;

LOCK TABLE public.storage_snapshots IN ACCESS EXCLUSIVE MODE;

ALTER SEQUENCE public.storage_snapshots_id_seq OWNED BY NONE;

DROP TABLE public.storage_snapshots;

ALTER TABLE public.storage_snapshots_new RENAME TO storage_snapshots;

ALTER INDEX public.storage_snapshots_new_pkey RENAME TO storage_snapshots_pkey;
ALTER TABLE public.storage_snapshots
    RENAME CONSTRAINT storage_snapshots_new_legacy_unique TO storage_snapshots_legacy_unique;

ALTER INDEX public.storage_snapshots_new_block_time_idx                 RENAME TO storage_snapshots_block_time_idx;
ALTER INDEX public.storage_snapshots_new_event_idx                      RENAME TO storage_snapshots_event_idx;
ALTER INDEX public.storage_snapshots_new_epoch_idx                      RENAME TO storage_snapshots_epoch_idx;
ALTER INDEX public.storage_snapshots_new_pallet_location_keys_idx       RENAME TO storage_snapshots_pallet_location_keys_idx;
ALTER INDEX public.storage_snapshots_new_pallet_location_data_idx       RENAME TO storage_snapshots_pallet_location_data_idx;
ALTER INDEX public.storage_snapshots_new_data_idx                       RENAME TO storage_snapshots_data_idx;
ALTER INDEX public.storage_snapshots_new_config_rule_idx                RENAME TO storage_snapshots_config_rule_idx;
ALTER INDEX public.storage_snapshots_new_null_data_idx                  RENAME TO storage_snapshots_null_data_idx;
ALTER INDEX public.storage_snapshots_new_metrics_epoch_sum_key_idx      RENAME TO storage_snapshots_metrics_epoch_sum_key_idx;
ALTER INDEX public.storage_snapshots_new_system_account_key_idx         RENAME TO storage_snapshots_system_account_key_idx;
ALTER INDEX public.storage_snapshots_new_block_id_idx                   RENAME TO storage_snapshots_block_id_idx;
ALTER INDEX public.storage_snapshots_new_pallet_storage_block_id_idx    RENAME TO storage_snapshots_pallet_storage_block_id_idx;
ALTER INDEX public.storage_snapshots_new_metrics_key_idx                RENAME TO storage_snapshots_metrics_key_idx;
ALTER INDEX public.storage_snapshots_new_commitment_key_idx             RENAME TO storage_snapshots_commitment_key_idx;

ALTER SEQUENCE public.storage_snapshots_id_seq OWNED BY public.storage_snapshots.id;

SELECT setval('public.storage_snapshots_id_seq',
              GREATEST((SELECT COALESCE(MAX(id), 0) FROM public.storage_snapshots), 1),
              true);

COMMIT;

-- Phase 5: refresh stats. (No VACUUM needed — table was rebuilt fresh.)
\echo '== Phase 5: ANALYZE =='
ANALYZE public.storage_snapshots;

\echo '== Done =='
