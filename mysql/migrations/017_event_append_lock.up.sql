-- Issue #60: serialize event appends so commit order matches global_position
-- order, closing the global-position visibility race that permanently loses
-- events in subscriptions/projections.
--
-- global_position (BIGINT AUTO_INCREMENT) is assigned at INSERT time but only
-- becomes visible at COMMIT time, and commits are not ordered by position. Two
-- concurrent appends can therefore commit out of position order, letting a
-- subscriber advance its checkpoint past a higher position while a lower one is
-- still uncommitted — that lower event is then never delivered.
--
-- PostgreSQL closes this with pg_advisory_xact_lock (no schema change). MySQL has
-- no transaction-scoped advisory lock (GET_LOCK is session-scoped and would not
-- release on commit/rollback of an ambient transaction), so instead we serialize
-- appends on a single, dedicated lock row via SELECT ... FOR UPDATE. The row lock
-- is held until the append transaction ends, making the [assign position, commit]
-- windows of concurrent appends non-overlapping, exactly like the advisory lock.
--
-- The table holds exactly one row (id = 1); it stores no data of its own.
--
-- Both statements are idempotent (IF NOT EXISTS + ON DUPLICATE KEY UPDATE) so the
-- whole file is safe to re-run after a manual reconciliation of a half-applied
-- migration (MySQL DDL auto-commits, so a mid-file failure can leave the table
-- created but the seed row missing). This matches schema.sql exactly.

CREATE TABLE IF NOT EXISTS event_append_lock (
    id    TINYINT UNSIGNED NOT NULL,
    notes VARCHAR(255)     NOT NULL DEFAULT '',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

INSERT INTO event_append_lock (id, notes)
VALUES (1, 'serializes event appends; see issue #60')
ON DUPLICATE KEY UPDATE id = id;
