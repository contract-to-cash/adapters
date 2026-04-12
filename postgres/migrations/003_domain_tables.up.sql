-- ============================================================
-- Invoices
-- ============================================================
CREATE TABLE invoices (
    id                  TEXT        PRIMARY KEY,
    invoice_number      TEXT        NOT NULL DEFAULT '',
    account_id          TEXT        NOT NULL,
    contract_id         TEXT        NOT NULL,
    status              TEXT        NOT NULL,
    subtotal            BIGINT      NOT NULL,
    tax_amount          BIGINT      NOT NULL,
    discount_amount     BIGINT      NOT NULL,
    total               BIGINT      NOT NULL,
    applied_balance     BIGINT      NOT NULL,
    amount_due          BIGINT      NOT NULL,
    paid_amount         BIGINT      NOT NULL,
    balance             BIGINT      NOT NULL,
    currency            TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    billing_period_from TIMESTAMPTZ,
    billing_period_to   TIMESTAMPTZ,
    issue_date          TIMESTAMPTZ,
    due_date            TIMESTAMPTZ,
    paid_at             TIMESTAMPTZ,
    void_reason         TEXT        NOT NULL DEFAULT '',
    revision_of         TEXT,
    original_invoice_id TEXT,
    payment_method_id   TEXT,
    allow_partial_pay   BOOLEAN     NOT NULL DEFAULT FALSE,
    line_items          JSONB       NOT NULL DEFAULT '[]',
    metadata            JSONB       NOT NULL DEFAULT '{}',
    version             INTEGER     NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_invoices_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE INDEX idx_invoices_contract_id ON invoices (contract_id);
CREATE INDEX idx_invoices_account_id ON invoices (account_id);
CREATE INDEX idx_invoices_status ON invoices (status);
CREATE INDEX idx_invoices_due_date ON invoices (due_date) WHERE due_date IS NOT NULL;
CREATE INDEX idx_invoices_contract_status ON invoices (contract_id, status);
CREATE INDEX idx_invoices_contract_issued ON invoices (contract_id, issue_date);

-- Historical snapshots of invoices for FindByIDAsOf temporal query.
-- Each Save appends a row and closes any previous open row
-- (valid_to = NOW()).
CREATE TABLE invoice_history (
    id         TEXT        NOT NULL REFERENCES invoices (id) ON DELETE CASCADE,
    version    INTEGER     NOT NULL,
    snapshot   JSONB       NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to   TIMESTAMPTZ,
    PRIMARY KEY (id, version)
);

CREATE INDEX idx_invoice_history_temporal ON invoice_history (id, valid_from);

-- ============================================================
-- Credit notes
-- ============================================================
CREATE TABLE credit_notes (
    id             TEXT        PRIMARY KEY,
    number         TEXT        NOT NULL DEFAULT '',
    invoice_id     TEXT        NOT NULL REFERENCES invoices (id),
    contract_id    TEXT        NOT NULL,
    account_id     TEXT        NOT NULL,
    status         TEXT        NOT NULL,
    reason         TEXT        NOT NULL DEFAULT '',
    memo           TEXT        NOT NULL DEFAULT '',
    items          JSONB       NOT NULL DEFAULT '[]',
    subtotal       BIGINT      NOT NULL,
    tax_amount     BIGINT      NOT NULL,
    total          BIGINT      NOT NULL,
    credit_amount  BIGINT      NOT NULL DEFAULT 0,
    refund_amount  BIGINT      NOT NULL DEFAULT 0,
    currency       TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    issued_at      TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_credit_notes_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE INDEX idx_credit_notes_invoice_id ON credit_notes (invoice_id);
CREATE INDEX idx_credit_notes_account_id ON credit_notes (account_id);
CREATE INDEX idx_credit_notes_contract_id ON credit_notes (contract_id);
CREATE INDEX idx_credit_notes_status ON credit_notes (status);

-- ============================================================
-- Payments
-- ============================================================
CREATE TABLE payments (
    id                     TEXT        PRIMARY KEY,
    invoice_id             TEXT        NOT NULL REFERENCES invoices (id),
    idempotency_key        TEXT        UNIQUE,
    amount                 BIGINT      NOT NULL,
    refunded_amount        BIGINT      NOT NULL DEFAULT 0,
    currency               TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    status                 TEXT        NOT NULL,
    method                 TEXT        NOT NULL DEFAULT '',
    gateway_transaction_id TEXT        NOT NULL DEFAULT '',
    failure_reason         TEXT        NOT NULL DEFAULT '',
    processed_at           TIMESTAMPTZ,
    metadata               JSONB       NOT NULL DEFAULT '{}',
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_payments_invoice_id ON payments (invoice_id);

-- ============================================================
-- Balance entries
-- ============================================================
CREATE TABLE balance_entries (
    id               TEXT        PRIMARY KEY,
    account_id       TEXT        NOT NULL,
    original_amount  BIGINT      NOT NULL,
    remaining_amount BIGINT      NOT NULL,
    currency         TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    reason           TEXT        NOT NULL DEFAULT '',
    source_type      TEXT        NOT NULL DEFAULT '',
    source_id        TEXT        NOT NULL DEFAULT '',
    description      TEXT        NOT NULL DEFAULT '',
    expires_at       TIMESTAMPTZ,
    version          INTEGER     NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_entries_account_currency ON balance_entries (account_id, currency);

-- Applications (allocation of a balance entry against an invoice)
CREATE TABLE balance_applications (
    id               TEXT        PRIMARY KEY,
    balance_entry_id TEXT        NOT NULL REFERENCES balance_entries (id),
    invoice_id       TEXT        NOT NULL REFERENCES invoices (id),
    amount           BIGINT      NOT NULL CHECK (amount > 0),
    currency         TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    applied_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_apps_entry_id ON balance_applications (balance_entry_id);
CREATE INDEX idx_balance_apps_invoice_id ON balance_applications (invoice_id);

-- Refunds (returning balance back out of a balance entry)
CREATE TABLE balance_refunds (
    id               TEXT        PRIMARY KEY,
    balance_entry_id TEXT        NOT NULL REFERENCES balance_entries (id),
    account_id       TEXT        NOT NULL,
    amount           BIGINT      NOT NULL CHECK (amount > 0),
    currency         TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    refunded_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_balance_refunds_entry_id ON balance_refunds (balance_entry_id);

-- ============================================================
-- Usage records
-- ============================================================
CREATE TABLE usage_records (
    id              TEXT        PRIMARY KEY,
    contract_id     TEXT        NOT NULL,
    metric          TEXT        NOT NULL,
    quantity        BIGINT      NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT        UNIQUE,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_usage_records_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE INDEX idx_usage_records_contract_metric_time ON usage_records (contract_id, metric, timestamp);
CREATE INDEX idx_usage_records_timestamp ON usage_records (timestamp);

-- ============================================================
-- Products (master data)
-- ============================================================
CREATE TABLE products (
    id          TEXT        PRIMARY KEY,
    name        TEXT        NOT NULL,
    description TEXT        NOT NULL DEFAULT '',
    status      TEXT        NOT NULL,
    features    JSONB       NOT NULL DEFAULT '[]',
    usage_metrics JSONB     NOT NULL DEFAULT '[]',
    metadata    JSONB       NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- Prices (master data)
-- ============================================================
CREATE TABLE prices (
    id             TEXT        PRIMARY KEY,
    product_id     TEXT        NOT NULL REFERENCES products (id),
    amount         BIGINT      NOT NULL,
    currency       TEXT        NOT NULL DEFAULT 'JPY' CHECK (length(currency) = 3),
    billing_cycle  TEXT        NOT NULL DEFAULT '',
    interval_data  JSONB       NOT NULL DEFAULT '{}',
    pricing_model  JSONB       NOT NULL DEFAULT '{}',
    status         TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prices_product_id ON prices (product_id);
CREATE INDEX idx_prices_product_status ON prices (product_id, status);
