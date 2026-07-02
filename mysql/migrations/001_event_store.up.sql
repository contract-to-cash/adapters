-- MySQL 8.0 event store DDL (mirrors postgres/migrations/001_event_store.up.sql).
--
-- The event store is an append-only log. Optimistic concurrency is enforced
-- structurally by the UNIQUE (stream_id, version) constraint.
--
-- DSN: configure the connection with loc=UTC, e.g.
--   user:pass@tcp(host:3306)/db?loc=UTC&parseTime=true
-- All timestamps are stored and returned in UTC.
--
-- Translation notes vs. PostgreSQL:
--   * TIMESTAMPTZ      -> DATETIME(6)
--   * BIGSERIAL        -> BIGINT UNSIGNED AUTO_INCREMENT. AUTO_INCREMENT requires
--     a KEY, so global_position is the PRIMARY KEY and id is UNIQUE instead.
--   * TEXT PK/FK/index -> VARCHAR(191) (utf8mb4 index length limit).
--   * JSONB            -> JSON.
--   * The LISTEN/NOTIFY trigger has no MySQL equivalent; the MySQL EventStore
--     tails new events via polling (Subscribe), so no trigger is created here.

CREATE TABLE events (
    global_position BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    id              VARCHAR(191)    NOT NULL,
    stream_id       VARCHAR(191)    NOT NULL,
    type            VARCHAR(191)    NOT NULL,
    version         INT             NOT NULL,
    schema_version  INT             NOT NULL DEFAULT 1,
    data            JSON            NOT NULL,
    metadata        JSON            NOT NULL,
    occurred_at     DATETIME(6)     NOT NULL,
    recorded_at     DATETIME(6)     NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (global_position),
    UNIQUE KEY uq_event_id (id),
    UNIQUE KEY uq_stream_version (stream_id, version),
    KEY idx_events_type (type),
    KEY idx_events_stream_occurred (stream_id, occurred_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE snapshots (
    stream_id  VARCHAR(191) NOT NULL,
    version    INT          NOT NULL,
    state      JSON         NOT NULL,
    as_of      DATETIME(6)  NOT NULL,
    created_at DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (stream_id, version),
    KEY idx_snapshots_stream_created (stream_id, created_at)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
