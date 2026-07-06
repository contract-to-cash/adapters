-- 011: per-(contract_id, billing_period) uniqueness for invoices
-- (issue adapters#45 / core#149). MySQL mirror of
-- postgres/migrations/012_invoice_period_uniqueness.up.sql.
--
-- core#149 added a per-period uniqueness contract to invoice.Repository.Save:
-- two DISTINCT non-voided, non-proration invoices must not exist for the same
-- (contract_id, billing_period). The core re-checks inside the generation tx, but
-- a check-then-insert of a NEW invoice cannot raise an optimistic-lock conflict,
-- so two concurrent GenerateInvoice(contractID, samePeriod) calls can both insert
-- under READ COMMITTED. This unique index is the storage-layer backstop.
--
-- MySQL has no partial (WHERE-filtered) indexes, so the exemption is expressed
-- through a STORED generated column that is NULL for every exempt row. A UNIQUE
-- index permits multiple NULLs, so exempt rows never collide (same technique as
-- migration 009's contract idempotency key). The key is
-- CONCAT(contract_id,'|',billing_period_from,'|',billing_period_to) for a
-- participating row, and NULL otherwise. Exemptions mirror
-- infrastructure/inmemory participatesInPeriodUniqueness:
--   * VOIDED invoices                       -> NULL (status = 'voided')
--   * PRORATION invoices                    -> NULL (metadata.$.invoice_type =
--     'proration'; invoice.MetadataKeyInvoiceType / InvoiceTypeProration). A
--     'regeneration' replacement is a regular period invoice and PARTICIPATES.
--   * ZERO-PERIOD invoices                  -> NULL (billing_period_from/to are
--     NULL when BillingPeriod().IsZero()).
--
-- DATETIME(6) values are concatenated in their canonical UTC string form
-- ('YYYY-MM-DD HH:MM:SS.ffffff'); invoice_repo.go stores billing_period_from/to
-- in UTC, so equal instants produce equal keys. VARCHAR(255) utf8mb4 (<= 1020
-- key bytes) stays within the InnoDB DYNAMIC index-prefix limit given
-- contract_id is VARCHAR(191).
--
-- On violation MySQL raises 1062 for key 'ux_invoice_period'; invoice_repo.go
-- maps that to a shared.ErrCodeConflict DomainError carrying the same message the
-- inmemory reference uses ("invoice already exists for this billing period").
--
-- Forward-only runner (no .down files); a mid-file failure is caught by the
-- 'pending' status marker.
ALTER TABLE invoices
    ADD COLUMN period_uniq_key VARCHAR(255)
        GENERATED ALWAYS AS (
            CASE
                WHEN status <> 'voided'
                     AND billing_period_from IS NOT NULL
                     AND billing_period_to IS NOT NULL
                     AND COALESCE(JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.invoice_type')), '') <> 'proration'
                THEN CONCAT(contract_id, '|', billing_period_from, '|', billing_period_to)
                ELSE NULL
            END
        ) STORED;

CREATE UNIQUE INDEX ux_invoice_period ON invoices (period_uniq_key);
