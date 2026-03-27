-- Remove the chain+address+seq_id composite index
DROP INDEX IF EXISTS "jobs_chain_address_seq_id_idx";

-- Restore old indexes
CREATE INDEX "jobs_address_idx" ON jobs (address, block_number DESC);
CREATE INDEX "jobs_chain_seq_id_idx" ON jobs (chain, seq_id, block_number DESC);
