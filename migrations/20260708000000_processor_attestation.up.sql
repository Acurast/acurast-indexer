-- ============================================
-- PROCESSOR ATTESTATION CLASSIFICATION
-- Denormalized Core/Lite and iOS/Android classification per processor,
-- derived from the Acurast.StoredAttestation storage snapshots captured
-- since the "index attestations" commit (attestation_stored /
-- attestation_stored_v2 rules, phase 4).
--
-- Populated incrementally by `storage_indexing::attestations` (driven by
-- the `attestation_processing_task` in main.rs), which decodes the
-- generic JSON blob in `storage_snapshots` per
-- `notes/Distinguishing Lite vs Core.md` and upserts these columns.
-- `attestation_block_number` tracks which snapshot's block was last
-- applied, so the incremental poller can skip already-processed
-- accounts and naturally catches up on all historical snapshots too —
-- no separate backfill script needed.
-- ============================================

ALTER TABLE public.accounts
    ADD COLUMN processor_type TEXT NULL,
    ADD COLUMN device_type TEXT NULL,
    ADD COLUMN attestation_block_number BIGINT NULL;

CREATE INDEX accounts_processor_type_idx ON accounts (processor_type) WHERE is_processor;
