-- 014: balance_refunds invoice/application linkage columns
-- (issue adapters#/ core#184).
--
-- Background. core#184 made void-triggered credit restoration idempotent. When a
-- voided invoice's consumed credit is restored to its BalanceEntry, the
-- BalanceRefund now records WHICH invoice the restoration is for (InvoiceID) and
-- WHICH BalanceApplication it reverses (ApplicationID). The new
-- balance.Repository.FindRefundsByInvoice(ctx, invoiceID) lets the restoration
-- flow skip applications whose credit was already restored, so a double void /
-- transaction retry does not restore the same credit twice.
--
-- Both columns default to '' (empty) for backward compatibility: refunds not
-- tied to an invoice legitimately carry an empty InvoiceID, and rows written
-- before this migration keep an empty ApplicationID. The index on invoice_id
-- backs the FindRefundsByInvoice lookup.
--
-- Forward-only runner (no .down files); applied files are tracked in
-- schema_migrations, so re-running skips this file.
ALTER TABLE balance_refunds
    ADD COLUMN invoice_id     TEXT NOT NULL DEFAULT '',
    ADD COLUMN application_id TEXT NOT NULL DEFAULT '';

CREATE INDEX idx_balance_refunds_invoice_id ON balance_refunds (invoice_id);
