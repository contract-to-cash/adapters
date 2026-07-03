-- MySQL 8.0 event store index fixes
-- (mirrors postgres/migrations/005_event_store_index_fixes.up.sql).
--
-- Fixes two indexes created by 001:
--
--   * events: KEY idx_events_global_position duplicated the PRIMARY KEY on
--     (global_position) exactly and is dropped. (postgres has no counterpart
--     to this fix: there global_position is not the primary key, so its
--     index is not redundant.)
--   * snapshots: idx_snapshots_stream_asof indexed (stream_id, as_of), but
--     LoadSnapshotBefore filters and orders on created_at, so the index
--     never served the query. Repoint it to (stream_id, created_at).
--
-- MySQL 8.0 supports neither DROP INDEX IF EXISTS nor CREATE INDEX IF NOT
-- EXISTS, so each change is guarded by an information_schema lookup executed
-- through a prepared statement (a no-op 'SELECT 1' when there is nothing to
-- do). The final state therefore converges both on fresh databases (001 just
-- created the old indexes) and on databases that were already reconciled by
-- hand before this migration existed.

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'events'
          AND index_name = 'idx_events_global_position'),
    'ALTER TABLE events DROP INDEX idx_events_global_position',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'snapshots'
          AND index_name = 'idx_snapshots_stream_asof'),
    'ALTER TABLE snapshots DROP INDEX idx_snapshots_stream_asof',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    NOT EXISTS(
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'snapshots'
          AND index_name = 'idx_snapshots_stream_created'),
    'ALTER TABLE snapshots ADD KEY idx_snapshots_stream_created (stream_id, created_at)',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;
