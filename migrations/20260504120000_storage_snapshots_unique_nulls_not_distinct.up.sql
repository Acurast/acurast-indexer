-- no-transaction
-- Rebuild the storage_snapshots UNIQUE constraint with NULLS NOT DISTINCT.
--
-- The original constraint (init migration) included nullable columns
-- (extrinsic_index, event_index, epoch_index). Under PostgreSQL's default
-- NULLS DISTINCT semantics, two rows with NULL in any of these columns
-- never conflict, so the upsert path in insert_storage_snapshot
-- (ON CONFLICT DO UPDATE) silently inserted duplicates for system events,
-- epoch-end snapshots, and any other NULL-bearing rows instead of updating.
--
-- PREREQUISITE: run scripts/dedup_storage_snapshots.sql first to remove
-- existing duplicates in committed chunks. This migration assumes dedup
-- is complete and bails out if duplicates remain.
--
-- Steps:
--   1. Guard: fail loudly if duplicate groups still exist.
--   2. Drop the auto-named legacy UNIQUE constraint.
--   3. Recreate with NULLS NOT DISTINCT (requires PostgreSQL 15+).
--   4. Add a covering index for the rpc_server DISTINCT ON dedup pattern
--      (see getStorageSnapshots in rpc_server.rs).

-- Step 1: guard.
DO $$
DECLARE
    dup_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO dup_count FROM (
        SELECT 1
        FROM storage_snapshots
        GROUP BY block_number, extrinsic_index, event_index, epoch_index,
                 epoch_end, pallet, storage_location, storage_keys
        HAVING COUNT(*) > 1
    ) t;

    IF dup_count > 0 THEN
        RAISE EXCEPTION
            'storage_snapshots still has % duplicate groups. Run scripts/dedup_storage_snapshots.sql before applying this migration.',
            dup_count;
    END IF;
END $$;

-- Step 2: discover and drop the auto-named UNIQUE constraint.
DO $$
DECLARE
    cname text;
BEGIN
    SELECT con.conname INTO cname
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid = con.conrelid
    JOIN pg_namespace ns ON ns.oid = rel.relnamespace
    WHERE ns.nspname = 'public'
      AND rel.relname = 'storage_snapshots'
      AND con.contype = 'u'
      AND (
        SELECT array_agg(attname ORDER BY attname)
        FROM pg_attribute
        WHERE attrelid = con.conrelid AND attnum = ANY(con.conkey)
      ) = ARRAY[
        'block_number', 'epoch_end', 'epoch_index', 'event_index',
        'extrinsic_index', 'pallet', 'storage_keys', 'storage_location'
      ]::name[];

    IF cname IS NULL THEN
        RAISE EXCEPTION 'Original UNIQUE constraint not found on storage_snapshots';
    END IF;

    EXECUTE format('ALTER TABLE storage_snapshots DROP CONSTRAINT %I', cname);
END $$;

-- Step 3: recreate with NULLS NOT DISTINCT.
ALTER TABLE storage_snapshots
    ADD CONSTRAINT storage_snapshots_event_epoch_unique
    UNIQUE NULLS NOT DISTINCT (
        block_number,
        extrinsic_index,
        event_index,
        epoch_index,
        epoch_end,
        pallet,
        storage_location,
        storage_keys
    );

-- Step 4: dedup-supporting index.
-- The getStorageSnapshots dedup uses
--   DISTINCT ON (block_number, pallet, storage_location, storage_keys)
-- with inner ORDER BY (pallet, storage_location, storage_keys, block_number DESC, id <dir>).
-- This index lets the planner skip the Sort step in front of the Unique node,
-- and also serves WHERE filters that lead with pallet (+ optional storage_location,
-- storage_keys), which is the common API access pattern.
--
-- Plain CREATE INDEX (not CONCURRENTLY) because the indexer is stopped during
-- this migration; this is significantly faster (single scan, no lock-wait phases).
-- Switch to CREATE INDEX CONCURRENTLY if the API is still serving writers.
CREATE INDEX IF NOT EXISTS storage_snapshots_pallet_loc_keys_block_id_idx
    ON storage_snapshots (pallet, storage_location, storage_keys, block_number DESC, id DESC);
