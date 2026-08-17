-- no-transaction
-- Partial expression index on events.data->>0 (recipient address) for Balances::Deposit events.
-- Supports the getBaseRewards endpoint which filters deposits by manager address.
-- pallet = 10 (Balances), variant = 7 (Deposit)
--
-- CONCURRENTLY so the indexer's INSERT INTO events keeps running while the
-- build proceeds; the -- no-transaction directive disables sqlx's per-file
-- transaction wrapper (required for CONCURRENTLY).
CREATE INDEX CONCURRENTLY IF NOT EXISTS "events_deposit_recipient_idx"
ON events ((data->>0), block_number DESC, extrinsic_index DESC, index DESC)
WHERE pallet = 10 AND variant = 7;
