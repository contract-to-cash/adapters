-- Event store index fixes.
--
-- 001 created idx_snapshots_stream_asof on (stream_id, as_of), but the
-- LoadSnapshotBefore query filters and orders on created_at (matching the
-- core in-memory reference semantics), so that index never served the query.
-- Repoint it to (stream_id, created_at).
--
-- Guarded with IF EXISTS / IF NOT EXISTS so the final state converges both
-- on fresh databases (001 just created the old index) and on databases that
-- were already reconciled by hand before this migration existed.

DROP INDEX IF EXISTS idx_snapshots_stream_asof;

CREATE INDEX IF NOT EXISTS idx_snapshots_stream_created
    ON snapshots (stream_id, created_at DESC);
