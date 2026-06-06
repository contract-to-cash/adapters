-- MySQL 8.0 read-model DDL (mirrors postgres/migrations/002_read_models.up.sql).
--
-- Partial indexes (... WHERE ...) are unsupported in MySQL; the predicate is
-- dropped and a full index is created instead (noted per index below).

CREATE TABLE projection_checkpoints (
    projector_name VARCHAR(191) NOT NULL,
    last_position  BIGINT       NOT NULL DEFAULT 0,
    last_updated   DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (projector_name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE TABLE contract_read_models (
    id             VARCHAR(191) NOT NULL,
    account_id     VARCHAR(191) NOT NULL,
    status         VARCHAR(191) NOT NULL,
    start_date     DATETIME(6),
    end_date       DATETIME(6),
    renewal_date   DATETIME(6),
    trial_end_date DATETIME(6),
    price_id       VARCHAR(191),
    data           JSON         NOT NULL,
    version        INT          NOT NULL DEFAULT 0,
    created_at     DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at     DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_contract_rm_account_id ON contract_read_models (account_id);
CREATE INDEX idx_contract_rm_status ON contract_read_models (status);
-- Partial-index WHERE predicates dropped (MySQL): full indexes below.
CREATE INDEX idx_contract_rm_end_date ON contract_read_models (end_date);
CREATE INDEX idx_contract_rm_renewal_date ON contract_read_models (renewal_date);
CREATE INDEX idx_contract_rm_trial_end ON contract_read_models (trial_end_date);

CREATE TABLE invoice_read_models (
    id          VARCHAR(191) NOT NULL,
    contract_id VARCHAR(191) NOT NULL,
    account_id  VARCHAR(191) NOT NULL,
    status      VARCHAR(191) NOT NULL,
    total       BIGINT       NOT NULL DEFAULT 0,
    currency    VARCHAR(191) NOT NULL DEFAULT 'JPY',
    data        JSON         NOT NULL,
    version     INT          NOT NULL DEFAULT 0,
    created_at  DATETIME(6)  NOT NULL DEFAULT NOW(6),
    updated_at  DATETIME(6)  NOT NULL DEFAULT NOW(6),
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;

CREATE INDEX idx_invoice_rm_contract_id ON invoice_read_models (contract_id);
CREATE INDEX idx_invoice_rm_account_id ON invoice_read_models (account_id);
CREATE INDEX idx_invoice_rm_status ON invoice_read_models (status);
