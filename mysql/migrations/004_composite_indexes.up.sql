-- MySQL 8.0 composite indexes (mirrors postgres/migrations/004_composite_indexes.up.sql).
--
-- Composite indexes for read-model query patterns identified in code review.
--
-- Partial indexes (... WHERE ...) are unsupported in MySQL; the predicate is
-- dropped and a full index over the leading column(s) is created instead. The
-- query planner still uses these indexes for the equality/range scans noted.
--
-- MySQL has no CREATE INDEX IF NOT EXISTS; migrations are applied once in order.

-- contract_read_models: FindExpiring uses status + end_date.
-- Partial-index WHERE predicate dropped (MySQL): full index on end_date.
CREATE INDEX idx_contract_rm_expiring
    ON contract_read_models (end_date);

-- contract_read_models: FindTrialsEndingSoon uses status + trial_end_date.
-- Partial-index WHERE predicate dropped (MySQL): full index on trial_end_date.
CREATE INDEX idx_contract_rm_trial_expiring
    ON contract_read_models (trial_end_date);

-- contract_read_models: FindDueForRenewal uses status + renewal_date.
-- Partial-index WHERE predicate dropped (MySQL): full index on renewal_date.
CREATE INDEX idx_contract_rm_renewal
    ON contract_read_models (renewal_date);

-- invoices: FindOverdue uses status + due_date.
-- Partial-index WHERE predicate dropped (MySQL): full index on due_date.
CREATE INDEX idx_invoices_overdue
    ON invoices (due_date);

-- balance_entries: FindAvailable uses account_id + currency + remaining_amount > 0.
-- Partial-index WHERE predicate dropped (MySQL): full composite index.
CREATE INDEX idx_balance_entries_available
    ON balance_entries (account_id, currency, created_at);
