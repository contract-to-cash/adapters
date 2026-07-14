-- MySQL 8.0 schema for the contract-to-cash event store.
--
-- The event store is an append-only log. Optimistic concurrency is enforced
-- structurally by the UNIQUE (stream_id, version) constraint: two concurrent
-- appends targeting the same expected version cannot both succeed.
--
-- DSN: configure the connection with loc=UTC, e.g.
--   user:pass@tcp(host:3306)/db?loc=UTC&parseTime=true
-- All timestamps are stored and returned in UTC. The adapter scans DATETIME
-- under either parseTime setting, but loc=UTC is required so a driver-parsed
-- time.Time carries UTC wall-clock.

CREATE TABLE IF NOT EXISTS events (
    global_position BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    id              VARCHAR(64)     NOT NULL,
    stream_id       VARCHAR(255)    NOT NULL,
    type            VARCHAR(255)    NOT NULL,
    version         INT             NOT NULL,
    schema_version  INT             NOT NULL,
    data            JSON            NOT NULL,
    metadata        JSON            NOT NULL,
    occurred_at     DATETIME(6)     NOT NULL,
    recorded_at     DATETIME(6)     NOT NULL,
    PRIMARY KEY (global_position),
    UNIQUE KEY uq_event_id (id),
    UNIQUE KEY uq_stream_version (stream_id, version),
    KEY idx_stream (stream_id),
    KEY idx_occurred (occurred_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE IF NOT EXISTS snapshots (
    stream_id  VARCHAR(255) NOT NULL,
    version    INT          NOT NULL,
    state      JSON         NOT NULL,
    as_of      DATETIME(6)  NOT NULL,
    created_at DATETIME(6)  NOT NULL,
    PRIMARY KEY (stream_id, version),
    KEY idx_stream_created (stream_id, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

-- Single-row lock table that serializes event appends so commit order matches
-- global_position order (issue #60). Appends take SELECT ... FOR UPDATE on the
-- id = 1 row for the lifetime of the append transaction; see mysql/eventstore.go
-- appendOn and migration 017_event_append_lock.up.sql.
CREATE TABLE IF NOT EXISTS event_append_lock (
    id    TINYINT UNSIGNED NOT NULL,
    notes VARCHAR(255)     NOT NULL DEFAULT '',
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

INSERT INTO event_append_lock (id, notes)
VALUES (1, 'serializes event appends; see issue #60')
ON DUPLICATE KEY UPDATE id = id;
