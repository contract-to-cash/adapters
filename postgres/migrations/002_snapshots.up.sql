CREATE TABLE snapshots (
    stream_id   TEXT        NOT NULL REFERENCES streams (stream_id),
    version     INTEGER     NOT NULL,
    state       JSONB       NOT NULL,
    as_of       TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);
