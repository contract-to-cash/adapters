-- ============================================================
-- Event store: events and snapshots.
--
-- The events table is the append-only source of truth for every
-- event-sourced aggregate (currently only Contract). Optimistic
-- concurrency is enforced by the UNIQUE (stream_id, version)
-- constraint: two concurrent appenders racing to version N will
-- have exactly one winner; the loser receives a unique-violation
-- that the adapter translates to tx.ErrVersionConflict.
--
-- global_position is a BIGSERIAL so Subscribe/LoadAll can order
-- events across streams. 0 is reserved (never used by a real
-- event); callers pass 0 to mean "from the beginning".
-- ============================================================
CREATE TABLE events (
    id              TEXT        PRIMARY KEY,
    stream_id       TEXT        NOT NULL,
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

CREATE INDEX idx_events_global_position ON events (global_position);
CREATE INDEX idx_events_type ON events (type);
CREATE INDEX idx_events_stream_occurred ON events (stream_id, occurred_at);

-- Subscribers use LISTEN events_inserted to wake up and catch-up
-- via LoadAll. FOR EACH STATEMENT (not FOR EACH ROW) so that a
-- multi-row Append fires one notification instead of N.
CREATE OR REPLACE FUNCTION notify_events_inserted()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('events_inserted', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_inserted
    AFTER INSERT ON events
    FOR EACH STATEMENT
    EXECUTE FUNCTION notify_events_inserted();

-- ============================================================
-- Snapshots: one row per (stream_id, version). Aggregates keep
-- multiple snapshots so point-in-time queries can pick an older
-- snapshot via LoadSnapshotBefore(as_of).
-- ============================================================
CREATE TABLE snapshots (
    stream_id  TEXT        NOT NULL,
    version    INTEGER     NOT NULL,
    state      JSONB       NOT NULL,
    as_of      TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

CREATE INDEX idx_snapshots_stream_asof ON snapshots (stream_id, as_of DESC);
