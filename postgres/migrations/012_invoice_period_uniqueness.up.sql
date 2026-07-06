-- 012: per-(contract_id, billing_period) uniqueness for invoices
-- (issue adapters#45 / core#149).
--
-- core#149 added a per-period uniqueness contract to invoice.Repository.Save:
-- two DISTINCT non-voided, non-proration invoices must not exist for the same
-- (contract_id, billing_period). BillingService.GenerateInvoice / RegenerateInvoice
-- re-check for a duplicate inside their tx.Run closure, but a check-then-insert of
-- a NEW invoice cannot raise an optimistic-lock conflict (there is no prior
-- version to compare), so under READ COMMITTED two concurrent
-- GenerateInvoice(contractID, samePeriod) calls can both pass the re-check and
-- both insert. This partial unique index is the storage-layer backstop that
-- closes that window, exactly as recommended by the Save godoc.
--
-- Exemptions (mirroring infrastructure/inmemory participatesInPeriodUniqueness):
--   * VOIDED invoices are exempt: void-and-recreate leaves the voided original
--     alongside its replacement (status <> 'voided').
--   * PRORATION invoices are exempt: proration adjustments intentionally coexist
--     with the period's regular invoice. The invoice type lives in the metadata
--     JSONB under 'invoice_type' (invoice.MetadataKeyInvoiceType); a value of
--     'proration' (invoice.InvoiceTypeProration) is excluded. A regeneration
--     replacement (invoice_type = 'regeneration') is a regular period invoice and
--     PARTICIPATES.
--   * ZERO-PERIOD invoices are exempt: an invoice without a billing period stores
--     NULL billing_period_from/to (invoice_repo.go leaves them NULL when
--     BillingPeriod().IsZero()), so `billing_period_from IS NOT NULL` drops them.
--
-- On violation the driver raises SQLSTATE 23505 with constraint name
-- ux_invoice_period; invoice_repo.go maps that to a shared.ErrCodeConflict
-- DomainError carrying the same message the inmemory reference uses
-- ("invoice already exists for this billing period").
--
-- Forward-only runner (no .down files); tracked in schema_migrations.
CREATE UNIQUE INDEX ux_invoice_period
    ON invoices (contract_id, billing_period_from, billing_period_to)
    WHERE status <> 'voided'
      AND billing_period_from IS NOT NULL
      AND coalesce(metadata->>'invoice_type', '') <> 'proration';
