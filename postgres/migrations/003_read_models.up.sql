-- Contract read model (projection target)
CREATE TABLE contract_read_models (
    id             TEXT        PRIMARY KEY,
    account_id     TEXT        NOT NULL,
    status         TEXT        NOT NULL,
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
CREATE INDEX idx_contract_rm_end_date ON contract_read_models (end_date);
CREATE INDEX idx_contract_rm_renewal_date ON contract_read_models (renewal_date);
CREATE INDEX idx_contract_rm_trial_end ON contract_read_models (trial_end_date) WHERE trial_end_date IS NOT NULL;

-- Invoices
CREATE TABLE invoices (
    id          TEXT        PRIMARY KEY,
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    status      TEXT        NOT NULL,
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

CREATE INDEX idx_invoices_contract_id ON invoices (contract_id);
CREATE INDEX idx_invoices_account_id ON invoices (account_id);
CREATE INDEX idx_invoices_status ON invoices (status);
CREATE INDEX idx_invoices_due_date ON invoices (due_date);
CREATE INDEX idx_invoices_contract_status ON invoices (contract_id, status);
CREATE INDEX idx_invoices_contract_issued ON invoices (contract_id, issued_at);

-- Invoice history for temporal queries (FindByIDAsOf)
CREATE TABLE invoice_history (
    id         TEXT        NOT NULL,
    data       JSONB       NOT NULL,
    version    INTEGER     NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_to   TIMESTAMPTZ,
    PRIMARY KEY (id, version)
);

CREATE INDEX idx_invoice_history_temporal ON invoice_history (id, valid_from);

-- Invoice read model (projection target)
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

-- Credit notes
CREATE TABLE credit_notes (
    id          TEXT        PRIMARY KEY,
    invoice_id  TEXT        NOT NULL,
    contract_id TEXT        NOT NULL,
    account_id  TEXT        NOT NULL,
    amount      BIGINT      NOT NULL,
    currency    TEXT        NOT NULL DEFAULT 'JPY',
    reason      TEXT,
    status      TEXT        NOT NULL,
    data        JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_notes_invoice_id ON credit_notes (invoice_id);
CREATE INDEX idx_credit_notes_account_id ON credit_notes (account_id);
CREATE INDEX idx_credit_notes_contract_id ON credit_notes (contract_id);
CREATE INDEX idx_credit_notes_status ON credit_notes (status);

-- Payments
CREATE TABLE payments (
    id              TEXT    PRIMARY KEY,
    invoice_id      TEXT    NOT NULL,
    idempotency_key TEXT    UNIQUE,
    amount          BIGINT  NOT NULL,
    currency        TEXT    NOT NULL DEFAULT 'JPY',
    status          TEXT    NOT NULL,
    method          TEXT,
    data            JSONB   NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_invoice_id ON payments (invoice_id);

-- Balance entries
CREATE TABLE balance_entries (
    id         TEXT    PRIMARY KEY,
    account_id TEXT    NOT NULL,
    amount     BIGINT  NOT NULL,
    currency   TEXT    NOT NULL DEFAULT 'JPY',
    data       JSONB   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_entries_account_id ON balance_entries (account_id);
CREATE INDEX idx_balance_entries_account_currency ON balance_entries (account_id, currency);

-- Balance applications (linking balance entries to invoices)
CREATE TABLE balance_applications (
    id               TEXT    PRIMARY KEY,
    balance_entry_id TEXT    NOT NULL REFERENCES balance_entries (id),
    invoice_id       TEXT    NOT NULL,
    amount           BIGINT  NOT NULL,
    data             JSONB   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_apps_entry_id ON balance_applications (balance_entry_id);
CREATE INDEX idx_balance_apps_invoice_id ON balance_applications (invoice_id);

-- Balance refunds
CREATE TABLE balance_refunds (
    id               TEXT    PRIMARY KEY,
    balance_entry_id TEXT    NOT NULL REFERENCES balance_entries (id),
    amount           BIGINT  NOT NULL,
    data             JSONB   NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_refunds_entry_id ON balance_refunds (balance_entry_id);

-- Usage records
CREATE TABLE usage_records (
    id              TEXT        PRIMARY KEY,
    contract_id     TEXT        NOT NULL,
    metric          TEXT        NOT NULL,
    quantity        BIGINT      NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT        UNIQUE,
    data            JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_records_contract_id ON usage_records (contract_id);
CREATE INDEX idx_usage_records_contract_metric ON usage_records (contract_id, metric);
CREATE INDEX idx_usage_records_timestamp ON usage_records (timestamp);
CREATE INDEX idx_usage_records_contract_metric_time ON usage_records (contract_id, metric, timestamp);

-- Products (master data)
CREATE TABLE products (
    id         TEXT  PRIMARY KEY,
    name       TEXT  NOT NULL,
    status     TEXT  NOT NULL,
    data       JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prices (master data)
CREATE TABLE prices (
    id             TEXT   PRIMARY KEY,
    product_id     TEXT   NOT NULL REFERENCES products (id),
    amount         BIGINT NOT NULL,
    currency       TEXT   NOT NULL DEFAULT 'JPY',
    billing_period TEXT,
    status         TEXT   NOT NULL,
    data           JSONB  NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prices_product_id ON prices (product_id);
CREATE INDEX idx_prices_product_status ON prices (product_id, status);
