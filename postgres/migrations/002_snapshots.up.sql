CREATE TABLE snapshots (
    stream_id   TEXT        NOT NULL,
    version     INTEGER     NOT NULL,
    state       JSONB       NOT NULL,
    as_of       TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

CREATE INDEX idx_snapshots_stream_id ON snapshots (stream_id);
