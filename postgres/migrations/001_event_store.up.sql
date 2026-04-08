-- ============================================================
-- Streams: first-class entity representing an event stream.
-- Every event-sourced aggregate (e.g. Contract) owns one stream.
--
-- Benefits:
--   1. FK target for events, snapshots, and domain tables
--   2. O(1) optimistic locking via current_version (PK lookup)
--      instead of SELECT MAX(version) FROM events (range scan)
--   3. Aggregate discovery and lifecycle tracking
-- ============================================================
CREATE TABLE streams (
    stream_id       TEXT        PRIMARY KEY,
    aggregate_type  TEXT        NOT NULL,
    aggregate_id    TEXT        NOT NULL,
    current_version INTEGER     NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_type, aggregate_id)
);

CREATE INDEX idx_streams_aggregate_type ON streams (aggregate_type);

-- ============================================================
-- Events
-- FK: stream_id → streams (referential integrity)
-- UNIQUE (stream_id, version) enforces per-stream ordering
-- ============================================================
CREATE TABLE events (
    id              TEXT        PRIMARY KEY,
    stream_id       TEXT        NOT NULL REFERENCES streams (stream_id),
    type            TEXT        NOT NULL,
    version         INTEGER     NOT NULL,
    schema_version  INTEGER     NOT NULL DEFAULT 1,
    data            JSONB       NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    global_position BIGSERIAL   NOT NULL,
    UNIQUE (stream_id, version)
);

CREATE INDEX idx_events_stream_id ON events (stream_id);
CREATE INDEX idx_events_type ON events (type);
CREATE INDEX idx_events_global_position ON events (global_position);
-- NOTE: idx for (stream_id, version) is already created by the UNIQUE constraint

-- Notification function for subscriber wakeup
CREATE OR REPLACE FUNCTION notify_events_inserted()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('events_inserted', NEW.global_position::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_inserted
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION notify_events_inserted();
