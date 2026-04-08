CREATE TABLE projection_checkpoints (
    projector_name TEXT        PRIMARY KEY,
    last_position  BIGINT      NOT NULL DEFAULT 0,
    last_updated   TIMESTAMPTZ NOT NULL
);
