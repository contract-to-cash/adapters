-- 013: balance_refunds invoice/application linkage columns
-- (issue adapters#/ core#184). MySQL mirror of
-- postgres/migrations/014_balance_refund_invoice_application.up.sql.
--
-- Background. core#184 made void-triggered credit restoration idempotent. The
-- BalanceRefund now records WHICH invoice the restoration is for (InvoiceID) and
-- WHICH BalanceApplication it reverses (ApplicationID). The new
-- balance.Repository.FindRefundsByInvoice(ctx, invoiceID) lets the restoration
-- flow skip applications whose credit was already restored, so a double void /
-- transaction retry does not restore the same credit twice.
--
-- Both columns default to '' (empty) for backward compatibility: refunds not
-- tied to an invoice carry an empty InvoiceID, and rows written before this
-- migration keep an empty ApplicationID. The index on invoice_id backs the
-- FindRefundsByInvoice lookup.
--
-- Forward-only runner (no .down files); a mid-file failure is caught by the
-- 'pending' status marker.
ALTER TABLE balance_refunds
    ADD COLUMN invoice_id     VARCHAR(191) NOT NULL DEFAULT '',
    ADD COLUMN application_id VARCHAR(191) NOT NULL DEFAULT '';

CREATE INDEX idx_balance_refunds_invoice_id ON balance_refunds (invoice_id);
