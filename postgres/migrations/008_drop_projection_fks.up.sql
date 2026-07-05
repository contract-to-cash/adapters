-- Drop the write-side -> projection-table foreign keys.
--
-- 003 pointed invoices / credit_notes / usage_records at contract_read_models
-- (a PROJECTION table). Under the async projection mode that core officially
-- supports, the projection lags the event log, so a freshly created contract's
-- read model may not exist yet when the first invoice/usage row is written —
-- the FK would then reject a legitimate write purely because the projector had
-- not caught up. Referential integrity for contracts belongs to the event log
-- (the source of truth), not to a derived read model, so these FKs are removed.
--
-- Guarded with IF EXISTS so the migration converges on databases created before
-- 008 (constraint present) and any that already dropped it by hand.

ALTER TABLE invoices      DROP CONSTRAINT IF EXISTS fk_invoices_contract;
ALTER TABLE credit_notes  DROP CONSTRAINT IF EXISTS fk_credit_notes_contract;
ALTER TABLE usage_records DROP CONSTRAINT IF EXISTS fk_usage_records_contract;
