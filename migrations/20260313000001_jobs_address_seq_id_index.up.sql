-- Drop old redundant indexes
DROP INDEX IF EXISTS "jobs_address_idx";
DROP INDEX IF EXISTS "jobs_chain_seq_id_idx";

-- Add composite index for filtering by chain, address, and seq_id together
-- Supports: WHERE chain = 'Acurast' AND address = '...' AND seq_id = 123
-- Also supports: WHERE chain = 'Acurast' AND address = '...' (uses prefix)
-- Replaces both jobs_address_idx and jobs_chain_seq_id_idx
CREATE INDEX "jobs_chain_address_seq_id_idx"
ON jobs (chain, address, seq_id, block_number DESC);
