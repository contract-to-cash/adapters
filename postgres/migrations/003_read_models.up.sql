-- Contract read model (projection target)
CREATE TABLE contract_read_models (
    id            TEXT        PRIMARY KEY,
    account_id    TEXT        NOT NULL,
    status        TEXT        NOT NULL,
    start_date    TIMESTAMPTZ NOT NULL,
    end_date      TIMESTAMPTZ,
    renewal_date  TIMESTAMPTZ,
    data          JSONB       NOT NULL,
    version       INTEGER     NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_contract_rm_account_id ON contract_read_models (account_id);
CREATE INDEX idx_contract_rm_status ON contract_read_models (status);
CREATE INDEX idx_contract_rm_end_date ON contract_read_models (end_date);
CREATE INDEX idx_contract_rm_renewal_date ON contract_read_models (renewal_date);

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
    id         TEXT        PRIMARY KEY,
    invoice_id TEXT        NOT NULL,
    amount     BIGINT      NOT NULL,
    currency   TEXT        NOT NULL DEFAULT 'JPY',
    reason     TEXT,
    status     TEXT        NOT NULL,
    data       JSONB       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_credit_notes_invoice_id ON credit_notes (invoice_id);

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

-- Balances (with optimistic locking version)
CREATE TABLE balances (
    id         TEXT    PRIMARY KEY,
    account_id TEXT    NOT NULL,
    amount     BIGINT  NOT NULL DEFAULT 0,
    currency   TEXT    NOT NULL DEFAULT 'JPY',
    version    INTEGER NOT NULL DEFAULT 0,
    data       JSONB   NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balances_account_id ON balances (account_id);

-- Usage records
CREATE TABLE usage_records (
    id              TEXT        PRIMARY KEY,
    subscription_id TEXT        NOT NULL,
    meter_id        TEXT        NOT NULL,
    quantity        NUMERIC     NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT        UNIQUE,
    data            JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_usage_records_subscription_id ON usage_records (subscription_id);
CREATE INDEX idx_usage_records_meter_id ON usage_records (meter_id);
CREATE INDEX idx_usage_records_timestamp ON usage_records (timestamp);

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
CREATE INDEX idx_prices_status ON prices (status);
