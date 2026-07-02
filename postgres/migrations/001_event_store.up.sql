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

CREATE TABLE snapshots (
    stream_id  TEXT        NOT NULL,
    version    INTEGER     NOT NULL,
    state      JSONB       NOT NULL,
    as_of      TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

CREATE INDEX idx_snapshots_stream_created ON snapshots (stream_id, created_at DESC);
