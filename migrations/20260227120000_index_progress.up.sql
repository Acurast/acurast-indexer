-- Helper table to track indexing progress for various scan operations
CREATE TABLE IF NOT EXISTS _index_progress (
    id TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
