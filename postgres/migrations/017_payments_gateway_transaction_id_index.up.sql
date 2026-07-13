-- 017: index on payments.gateway_transaction_id for webhook settlement
-- lookups (issue adapters#72).
--
-- Background. The platform webhook settlement handler (platform#65) resolves
-- an inbound gateway callback to a payment row with:
--   SELECT id FROM payments WHERE gateway_transaction_id = $1
--     ORDER BY created_at DESC LIMIT 1
-- payments carries only idx_payments_invoice_id (003), so this lookup is a
-- full table scan.
--
-- Partial index. gateway_transaction_id is NOT NULL DEFAULT '' (003):
-- payments that never go through a gateway (e.g. zero-amount settlements)
-- keep the column empty and accumulate as a large slice of the table that the
-- webhook lookup never targets — it short-circuits before issuing the query
-- when the incoming transaction id is empty. A partial index excluding
-- empty-string rows serves every real lookup while keeping the index small.
--
-- Not unique. A single column is sufficient: in practice more than one row
-- shares the same gateway_transaction_id essentially never happens, but this
-- migration does not promote that into a UNIQUE constraint — it only adds the
-- index the lookup needs.
CREATE INDEX IF NOT EXISTS idx_payments_gateway_transaction_id
    ON payments (gateway_transaction_id)
    WHERE gateway_transaction_id <> '';
