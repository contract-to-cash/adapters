CREATE TABLE projection_checkpoints (
    projector_name TEXT        PRIMARY KEY,
    last_position  BIGINT      NOT NULL DEFAULT 0,
    last_updated   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE contract_read_models (
    id             TEXT        PRIMARY KEY,
    account_id     TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    start_date     TIMESTAMPTZ,
    end_date       TIMESTAMPTZ,
    renewal_date   TIMESTAMPTZ,
    trial_end_date TIMESTAMPTZ,
    price_id       TEXT,
    data           JSONB       NOT NULL DEFAULT '{}',
    version        INTEGER     NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_contract_rm_account_id ON contract_read_models (account_id);
CREATE INDEX idx_contract_rm_status ON contract_read_models (status);
CREATE INDEX idx_contract_rm_end_date ON contract_read_models (end_date) WHERE end_date IS NOT NULL;
CREATE INDEX idx_contract_rm_renewal_date ON contract_read_models (renewal_date) WHERE renewal_date IS NOT NULL;
CREATE INDEX idx_contract_rm_trial_end ON contract_read_models (trial_end_date) WHERE trial_end_date IS NOT NULL;

CREATE TABLE invoice_read_models (
    id          TEXT        PRIMARY KEY,
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    total       BIGINT      NOT NULL DEFAULT 0,
    currency    TEXT        NOT NULL DEFAULT 'JPY',
    data        JSONB       NOT NULL DEFAULT '{}',
    version     INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- total is a floor-truncated whole-currency-unit approximation kept for
-- query/display convenience only (filtering, ordering, dashboards). It is NOT
-- the source of truth: the exact big.Rat amount lives in the data JSON column
-- on this table and in the event store's event payloads / state JSON. Do not
-- use this column for arithmetic or financial reconciliation. Same lossy
-- BIGINT-from-big.Rat truncation as the domain-table money columns documented
-- in postgres/money.go (parseMoneyPayload).
-- NOTE: this SQL comment is the canonical documentation of that behavior. The
-- migration runner (postgres/migrate.go) tracks applied migrations by
-- filename only and never re-runs an already-applied file, so the
-- COMMENT ON COLUMN below only reaches pg_catalog (and thus \d+ / information
-- schema introspection) on a database migrated from scratch after this
-- change; an existing deployment that already has this table keeps whatever
-- comment (or none) it had before, regardless of what this file now says.
COMMENT ON COLUMN invoice_read_models.total IS
    'Floor-truncated whole-currency-unit approximation for query/display only; exact big.Rat amount lives in event payloads / state JSON (see also the data column on this table); do not use for arithmetic or reconciliation.';

CREATE INDEX idx_invoice_rm_contract_id ON invoice_read_models (contract_id);
CREATE INDEX idx_invoice_rm_account_id ON invoice_read_models (account_id);
CREATE INDEX idx_invoice_rm_status ON invoice_read_models (status);
