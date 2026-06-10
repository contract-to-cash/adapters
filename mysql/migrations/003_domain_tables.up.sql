-- MySQL 8.0 domain-table DDL (mirrors postgres/migrations/003_domain_tables.up.sql).
--
-- Translation notes vs. PostgreSQL:
--   * TIMESTAMPTZ -> DATETIME(6); JSONB -> JSON; TEXT keys/FKs -> VARCHAR(191).
--   * length(currency)=3 -> CHAR_LENGTH(currency)=3.
--   * DEFERRABLE FK constraints have no MySQL equivalent. The FKs are kept as
--     plain (immediate) constraints. A projector that rebuilds read models out
--     of FK order must run inside `SET FOREIGN_KEY_CHECKS=0` (the projector
--     itself is out of scope for this adapter).
--   * Partial indexes (... WHERE ...) are unsupported; full indexes are used.

CREATE TABLE invoices (
    id                  VARCHAR(191) NOT NULL,
    invoice_number      VARCHAR(191) NOT NULL DEFAULT '',
    account_id          VARCHAR(191) NOT NULL,
    contract_id         VARCHAR(191) NOT NULL,
    status              VARCHAR(191) NOT NULL,
    subtotal            BIGINT       NOT NULL,
    tax_amount          BIGINT       NOT NULL,
    discount_amount     BIGINT       NOT NULL,
    total               BIGINT       NOT NULL,
    applied_balance     BIGINT       NOT NULL,
    amount_due          BIGINT       NOT NULL,
    paid_amount         BIGINT       NOT NULL,
    balance             BIGINT       NOT NULL,
    currency            VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    billing_period_from DATETIME(6),
    billing_period_to   DATETIME(6),
    issue_date          DATETIME(6),
    due_date            DATETIME(6),
    paid_at             DATETIME(6),
    void_reason         TEXT         NOT NULL,
    revision_of         VARCHAR(191),
    original_invoice_id VARCHAR(191),
    payment_method_id   VARCHAR(191),
    allow_partial_pay   BOOLEAN      NOT NULL DEFAULT FALSE,
    line_items          JSON         NOT NULL,
    metadata            JSON         NOT NULL,
    version             INT          NOT NULL DEFAULT 0,
    created_at          DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at          DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_invoices_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_invoices_contract_id ON invoices (contract_id);
CREATE INDEX idx_invoices_account_id ON invoices (account_id);
CREATE INDEX idx_invoices_status ON invoices (status);
-- Partial-index WHERE predicate dropped (MySQL): full index.
CREATE INDEX idx_invoices_due_date ON invoices (due_date);
CREATE INDEX idx_invoices_contract_status ON invoices (contract_id, status);
CREATE INDEX idx_invoices_contract_issued ON invoices (contract_id, issue_date);

CREATE TABLE invoice_history (
    id         VARCHAR(191) NOT NULL,
    version    INT          NOT NULL,
    snapshot   JSON         NOT NULL,
    valid_from DATETIME(6)  NOT NULL DEFAULT NOW(6),
    valid_to   DATETIME(6),
    PRIMARY KEY (id, version),
    CONSTRAINT fk_invoice_history_invoice
        FOREIGN KEY (id) REFERENCES invoices (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_invoice_history_temporal ON invoice_history (id, valid_from);

CREATE TABLE credit_notes (
    id            VARCHAR(191) NOT NULL,
    number        VARCHAR(191) NOT NULL DEFAULT '',
    invoice_id    VARCHAR(191) NOT NULL,
    contract_id   VARCHAR(191) NOT NULL,
    account_id    VARCHAR(191) NOT NULL,
    status        VARCHAR(191) NOT NULL,
    reason        TEXT         NOT NULL,
    memo          TEXT         NOT NULL,
    items         JSON         NOT NULL,
    subtotal      BIGINT       NOT NULL,
    tax_amount    BIGINT       NOT NULL,
    total         BIGINT       NOT NULL,
    credit_amount BIGINT       NOT NULL DEFAULT 0,
    refund_amount BIGINT       NOT NULL DEFAULT 0,
    currency      VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    issued_at     DATETIME(6),
    created_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_credit_notes_invoice
        FOREIGN KEY (invoice_id) REFERENCES invoices (id),
    CONSTRAINT fk_credit_notes_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_credit_notes_invoice_id ON credit_notes (invoice_id);
CREATE INDEX idx_credit_notes_account_id ON credit_notes (account_id);
CREATE INDEX idx_credit_notes_contract_id ON credit_notes (contract_id);
CREATE INDEX idx_credit_notes_status ON credit_notes (status);

CREATE TABLE payments (
    id                     VARCHAR(191) NOT NULL,
    invoice_id             VARCHAR(191) NOT NULL,
    idempotency_key        VARCHAR(191) UNIQUE,
    amount                 BIGINT       NOT NULL,
    refunded_amount        BIGINT       NOT NULL DEFAULT 0,
    currency               VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    status                 VARCHAR(191) NOT NULL,
    method                 VARCHAR(191) NOT NULL DEFAULT '',
    gateway_transaction_id VARCHAR(191) NOT NULL DEFAULT '',
    failure_reason         TEXT         NOT NULL,
    processed_at           DATETIME(6),
    metadata               JSON         NOT NULL,
    created_at             DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at             DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_payments_invoice
        FOREIGN KEY (invoice_id) REFERENCES invoices (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_payments_invoice_id ON payments (invoice_id);

CREATE TABLE balance_entries (
    id               VARCHAR(191) NOT NULL,
    account_id       VARCHAR(191) NOT NULL,
    original_amount  BIGINT       NOT NULL,
    remaining_amount BIGINT       NOT NULL,
    currency         VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    reason           VARCHAR(191) NOT NULL DEFAULT '',
    source_type      VARCHAR(191) NOT NULL DEFAULT '',
    source_id        VARCHAR(191) NOT NULL DEFAULT '',
    description      TEXT         NOT NULL,
    expires_at       DATETIME(6),
    version          INT          NOT NULL DEFAULT 0,
    created_at       DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_balance_entries_account_currency ON balance_entries (account_id, currency);

CREATE TABLE balance_applications (
    id               VARCHAR(191) NOT NULL,
    balance_entry_id VARCHAR(191) NOT NULL,
    invoice_id       VARCHAR(191) NOT NULL,
    amount           BIGINT       NOT NULL CHECK (amount > 0),
    currency         VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    applied_at       DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_balance_apps_entry
        FOREIGN KEY (balance_entry_id) REFERENCES balance_entries (id),
    CONSTRAINT fk_balance_apps_invoice
        FOREIGN KEY (invoice_id) REFERENCES invoices (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_balance_apps_entry_id ON balance_applications (balance_entry_id);
CREATE INDEX idx_balance_apps_invoice_id ON balance_applications (invoice_id);

CREATE TABLE balance_refunds (
    id               VARCHAR(191) NOT NULL,
    balance_entry_id VARCHAR(191) NOT NULL,
    account_id       VARCHAR(191) NOT NULL,
    amount           BIGINT       NOT NULL CHECK (amount > 0),
    currency         VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    refunded_at      DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_balance_refunds_entry
        FOREIGN KEY (balance_entry_id) REFERENCES balance_entries (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_balance_refunds_entry_id ON balance_refunds (balance_entry_id);

CREATE TABLE usage_records (
    id              VARCHAR(191) NOT NULL,
    contract_id     VARCHAR(191) NOT NULL,
    metric          VARCHAR(191) NOT NULL,
    quantity        BIGINT       NOT NULL,
    timestamp       DATETIME(6)  NOT NULL,
    idempotency_key VARCHAR(191) UNIQUE,
    metadata        JSON         NOT NULL,
    created_at      DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_usage_records_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_usage_records_contract_metric_time ON usage_records (contract_id, metric, timestamp);
CREATE INDEX idx_usage_records_timestamp ON usage_records (timestamp);

CREATE TABLE products (
    id            VARCHAR(191) NOT NULL,
    name          VARCHAR(191) NOT NULL,
    description   TEXT         NOT NULL,
    status        VARCHAR(191) NOT NULL,
    features      JSON         NOT NULL,
    usage_metrics JSON         NOT NULL,
    metadata      JSON         NOT NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE prices (
    id            VARCHAR(191) NOT NULL,
    product_id    VARCHAR(191) NOT NULL,
    amount        BIGINT       NOT NULL,
    currency      VARCHAR(191) NOT NULL DEFAULT 'JPY' CHECK (CHAR_LENGTH(currency) = 3),
    billing_cycle VARCHAR(191) NOT NULL DEFAULT '',
    interval_data JSON         NOT NULL,
    pricing_model JSON         NOT NULL,
    status        VARCHAR(191) NOT NULL,
    created_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at    DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id),
    CONSTRAINT fk_prices_product
        FOREIGN KEY (product_id) REFERENCES products (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_prices_product_id ON prices (product_id);
CREATE INDEX idx_prices_product_status ON prices (product_id, status);
