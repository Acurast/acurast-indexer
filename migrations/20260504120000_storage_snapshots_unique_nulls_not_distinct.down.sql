-- Revert to the original NULLS DISTINCT UNIQUE constraint.
-- (Down migration cannot recover dropped duplicate rows.)

DROP INDEX IF EXISTS storage_snapshots_pallet_loc_keys_block_id_idx;

ALTER TABLE storage_snapshots
    DROP CONSTRAINT storage_snapshots_event_epoch_unique;

ALTER TABLE storage_snapshots
    ADD CONSTRAINT storage_snapshots_event_epoch_unique_legacy
    UNIQUE (
        block_number,
        extrinsic_index,
        event_index,
        epoch_index,
        epoch_end,
        pallet,
        storage_location,
        storage_keys
    );
