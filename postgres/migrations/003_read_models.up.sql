-- ============================================================
-- Trigger function: validates that a contract stream exists
-- in the streams table. Used as a "soft FK" for tables that
-- reference contract_id, since direct FK to event-sourced
-- aggregates isn't possible (aggregate_id alone isn't unique
-- in streams — it requires aggregate_type to be unique).
-- ============================================================
CREATE OR REPLACE FUNCTION check_contract_stream_exists()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM streams
        WHERE aggregate_type = 'contract' AND aggregate_id = NEW.contract_id
    ) THEN
        RAISE EXCEPTION 'contract stream does not exist: %', NEW.contract_id
            USING ERRCODE = 'foreign_key_violation';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ============================================================
-- Contract read model (projection target)
-- NOTE: No FK/trigger — projections are rebuilt via TRUNCATE + replay.
-- ============================================================
CREATE TABLE contract_read_models (
    id             TEXT        PRIMARY KEY,
    account_id     TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('draft', 'trial', 'active', 'suspended', 'cancelled', 'expired')),
    start_date     TIMESTAMPTZ NOT NULL,
    end_date       TIMESTAMPTZ,
    renewal_date   TIMESTAMPTZ,
    trial_end_date TIMESTAMPTZ,
    data           JSONB       NOT NULL,
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
-- Invoices
-- contract_id validated via trigger → streams table
-- ============================================================
CREATE TABLE invoices (
    id          TEXT        PRIMARY KEY,
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    status      TEXT        NOT NULL CHECK (status IN ('draft', 'issued', 'paid', 'overdue', 'voided', 'cancelled')),
    amount      BIGINT      NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'JPY',
    due_date    TIMESTAMPTZ,
    issued_at   TIMESTAMPTZ,
    paid_at     TIMESTAMPTZ,
    data        JSONB       NOT NULL,
    version     INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_invoices_check_contract
    BEFORE INSERT OR UPDATE OF contract_id ON invoices
    FOR EACH ROW EXECUTE FUNCTION check_contract_stream_exists();

CREATE INDEX idx_invoices_contract_id ON invoices (contract_id);
CREATE INDEX idx_invoices_account_id ON invoices (account_id);
CREATE INDEX idx_invoices_status ON invoices (status);
CREATE INDEX idx_invoices_due_date ON invoices (due_date) WHERE due_date IS NOT NULL;
CREATE INDEX idx_invoices_contract_status ON invoices (contract_id, status);
CREATE INDEX idx_invoices_contract_issued ON invoices (contract_id, issued_at);

-- Invoice history for temporal queries (FindByIDAsOf)
CREATE TABLE invoice_history (
    id         TEXT        NOT NULL REFERENCES invoices (id) ON DELETE CASCADE,
    data       JSONB       NOT NULL,
    version    INTEGER     NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to   TIMESTAMPTZ,
    PRIMARY KEY (id, version)
);

CREATE INDEX idx_invoice_history_temporal ON invoice_history (id, valid_from);

-- Invoice read model (projection target)
-- NOTE: No FK/trigger — projection table, rebuilt via TRUNCATE + replay.
CREATE TABLE invoice_read_models (
    id          TEXT        PRIMARY KEY,
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    status      TEXT        NOT NULL,
    amount      BIGINT      NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'JPY',
    data        JSONB       NOT NULL,
    version     INTEGER     NOT NULL DEFAULT 0,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Credit notes
-- FK: invoice_id → invoices
-- contract_id validated via trigger → streams table
-- ============================================================
CREATE TABLE credit_notes (
    id          TEXT        PRIMARY KEY,
    invoice_id  TEXT        NOT NULL REFERENCES invoices (id),
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    amount      BIGINT      NOT NULL CHECK (amount > 0),
    currency    TEXT        NOT NULL DEFAULT 'JPY',
    reason      TEXT,
    status      TEXT        NOT NULL CHECK (status IN ('draft', 'issued', 'applied', 'voided')),
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_credit_notes_check_contract
    BEFORE INSERT OR UPDATE OF contract_id ON credit_notes
    FOR EACH ROW EXECUTE FUNCTION check_contract_stream_exists();

CREATE INDEX idx_credit_notes_invoice_id ON credit_notes (invoice_id);
CREATE INDEX idx_credit_notes_account_id ON credit_notes (account_id);
CREATE INDEX idx_credit_notes_contract_id ON credit_notes (contract_id);
CREATE INDEX idx_credit_notes_status ON credit_notes (status);

-- ============================================================
-- Payments
-- FK: invoice_id → invoices
-- ============================================================
CREATE TABLE payments (
    id              TEXT    PRIMARY KEY,
    invoice_id      TEXT    NOT NULL REFERENCES invoices (id),
    idempotency_key TEXT    UNIQUE,
    amount          BIGINT  NOT NULL CHECK (amount > 0),
    currency        TEXT    NOT NULL DEFAULT 'JPY',
    status          TEXT    NOT NULL CHECK (status IN ('pending', 'authorized', 'captured', 'failed', 'refunded', 'cancelled')),
    method          TEXT,
    data            JSONB   NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_invoice_id ON payments (invoice_id);

-- ============================================================
-- Balance entries
-- ============================================================
CREATE TABLE balance_entries (
    id         TEXT    PRIMARY KEY,
    account_id TEXT    NOT NULL,
    amount     BIGINT  NOT NULL CHECK (amount > 0),
    currency   TEXT    NOT NULL DEFAULT 'JPY',
    data       JSONB   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_entries_account_id ON balance_entries (account_id);
CREATE INDEX idx_balance_entries_account_currency ON balance_entries (account_id, currency);

-- Balance applications (linking balance entries to invoices)
-- FK: balance_entry_id → balance_entries, invoice_id → invoices
CREATE TABLE balance_applications (
    id               TEXT    PRIMARY KEY,
    balance_entry_id TEXT    NOT NULL REFERENCES balance_entries (id),
    invoice_id       TEXT    NOT NULL REFERENCES invoices (id),
    amount           BIGINT  NOT NULL CHECK (amount > 0),
    data             JSONB   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_apps_entry_id ON balance_applications (balance_entry_id);
CREATE INDEX idx_balance_apps_invoice_id ON balance_applications (invoice_id);

-- Balance refunds
-- FK: balance_entry_id → balance_entries
CREATE TABLE balance_refunds (
    id               TEXT    PRIMARY KEY,
    balance_entry_id TEXT    NOT NULL REFERENCES balance_entries (id),
    amount           BIGINT  NOT NULL CHECK (amount > 0),
    data             JSONB   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_refunds_entry_id ON balance_refunds (balance_entry_id);

-- ============================================================
-- Usage records
-- contract_id validated via trigger → streams table
-- ============================================================
CREATE TABLE usage_records (
    id              TEXT        PRIMARY KEY,
    contract_id     TEXT        NOT NULL,
    metric          TEXT        NOT NULL,
    quantity        BIGINT      NOT NULL CHECK (quantity >= 0),
    timestamp       TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT        UNIQUE,
    data            JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_usage_records_check_contract
    BEFORE INSERT OR UPDATE OF contract_id ON usage_records
    FOR EACH ROW EXECUTE FUNCTION check_contract_stream_exists();

CREATE INDEX idx_usage_records_contract_id ON usage_records (contract_id);
CREATE INDEX idx_usage_records_contract_metric ON usage_records (contract_id, metric);
CREATE INDEX idx_usage_records_timestamp ON usage_records (timestamp);
CREATE INDEX idx_usage_records_contract_metric_time ON usage_records (contract_id, metric, timestamp);

-- ============================================================
-- Products (master data)
-- ============================================================
CREATE TABLE products (
    id         TEXT  PRIMARY KEY,
    name       TEXT  NOT NULL,
    status     TEXT  NOT NULL CHECK (status IN ('active', 'archived')),
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prices (master data)
-- FK: product_id → products
CREATE TABLE prices (
    id             TEXT   PRIMARY KEY,
    product_id     TEXT   NOT NULL REFERENCES products (id),
    amount         BIGINT NOT NULL CHECK (amount >= 0),
    currency       TEXT   NOT NULL DEFAULT 'JPY',
    billing_period TEXT,
    status         TEXT   NOT NULL CHECK (status IN ('active', 'archived')),
    data           JSONB  NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prices_product_id ON prices (product_id);
CREATE INDEX idx_prices_product_status ON prices (product_id, status);
