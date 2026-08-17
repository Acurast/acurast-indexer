-- Chunked dedup of storage_snapshots.
--
-- Removes duplicate rows created by the buggy ON CONFLICT path when nullable
-- columns (extrinsic_index, event_index, epoch_index) were NULL, before
-- swapping the UNIQUE constraint to NULLS NOT DISTINCT.
--
-- Strategy:
--   - For each group of dup rows (by the 8 logical-key columns), keep the
--     row with the highest id; delete the rest.
--   - PARTITION BY treats NULL as equal so this works without NULLS NOT
--     DISTINCT.
--
-- Run via psql with autocommit (default). The procedure commits after each
-- batch so progress survives interruption and WAL doesn't balloon. You can
-- Ctrl-C between batches; on restart it resumes from wherever it stopped
-- (each iteration recomputes which rows are still dup victims).
--
-- Usage:
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f scripts/dedup_storage_snapshots.sql
--
-- Tune BATCH_SIZE for your DB. 100k is conservative; raise to 500k or 1M
-- if I/O headroom allows.

CREATE OR REPLACE PROCEDURE dedup_storage_snapshots_run(batch_size INT)
LANGUAGE plpgsql AS $$
DECLARE
    deleted_in_batch INT;
    total_deleted    BIGINT := 0;
    batch_no         INT := 0;
    started_at       TIMESTAMPTZ := clock_timestamp();
    batch_started    TIMESTAMPTZ;
BEGIN
    LOOP
        batch_no := batch_no + 1;
        batch_started := clock_timestamp();

        -- Pick up to batch_size victim ids: rows that are not the highest-id
        -- in their (8-key) duplicate group.
        WITH ranked AS (
            SELECT id,
                   ROW_NUMBER() OVER (
                       PARTITION BY block_number, extrinsic_index, event_index,
                                    epoch_index, epoch_end, pallet,
                                    storage_location, storage_keys
                       ORDER BY id DESC
                   ) AS rn
            FROM storage_snapshots
        ),
        victims AS (
            SELECT id FROM ranked WHERE rn > 1 LIMIT batch_size
        )
        DELETE FROM storage_snapshots s
        USING victims v
        WHERE s.id = v.id;

        GET DIAGNOSTICS deleted_in_batch = ROW_COUNT;
        total_deleted := total_deleted + deleted_in_batch;

        RAISE NOTICE 'batch %: deleted % rows in % (total: %, elapsed: %)',
            batch_no,
            deleted_in_batch,
            clock_timestamp() - batch_started,
            total_deleted,
            clock_timestamp() - started_at;

        EXIT WHEN deleted_in_batch = 0;

        COMMIT;  -- release locks, flush WAL, let autovacuum catch up
    END LOOP;

    RAISE NOTICE 'Done. Deleted % rows in % batches over %.',
        total_deleted, batch_no - 1, clock_timestamp() - started_at;
END $$;

-- Run it. Adjust batch_size as needed.
CALL dedup_storage_snapshots_run(100000);

-- Cleanup the helper procedure.
DROP PROCEDURE dedup_storage_snapshots_run(INT);

-- Recommend running an explicit VACUUM (ANALYZE) after this to reclaim
-- space and refresh stats — autovacuum will eventually do it but that
-- can take a while on a table with millions of dead tuples.
-- VACUUM (ANALYZE) storage_snapshots;
