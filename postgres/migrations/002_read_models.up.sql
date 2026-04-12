-- ============================================================
-- Projection checkpoints.
--
-- Each async projector persists its cursor (global_position) here
-- so that restarts can resume from the last processed event.
-- ============================================================
CREATE TABLE projection_checkpoints (
    projector_name TEXT        PRIMARY KEY,
    last_position  BIGINT      NOT NULL DEFAULT 0,
    last_updated   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Contract read model.
--
-- This is the query-side projection of the Contract aggregate.
-- Other domain tables (invoices, credit_notes, usage_records)
-- reference this table via DEFERRABLE FK so that projection
-- rebuild (DELETE + replay) can run inside a transaction that
-- defers constraint checking until COMMIT.
-- ============================================================
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

-- ============================================================
-- Invoice read model.
--
-- A pure projection with no foreign keys pointing at it, so
-- rebuild is plain DELETE + replay without DEFERRABLE tricks.
-- ============================================================
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

CREATE INDEX idx_invoice_rm_contract_id ON invoice_read_models (contract_id);
CREATE INDEX idx_invoice_rm_account_id ON invoice_read_models (account_id);
CREATE INDEX idx_invoice_rm_status ON invoice_read_models (status);
