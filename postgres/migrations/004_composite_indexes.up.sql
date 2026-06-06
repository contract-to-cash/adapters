-- Composite indexes for read-model query patterns identified in code review.

-- contract_read_models: FindExpiring uses status + end_date
CREATE INDEX IF NOT EXISTS idx_contract_rm_expiring
    ON contract_read_models (end_date)
    WHERE end_date IS NOT NULL AND status = 'active';

-- contract_read_models: FindTrialsEndingSoon uses status + trial_end_date
CREATE INDEX IF NOT EXISTS idx_contract_rm_trial_expiring
    ON contract_read_models (trial_end_date)
    WHERE trial_end_date IS NOT NULL AND status = 'trialing';

-- contract_read_models: FindDueForRenewal uses status + renewal_date
CREATE INDEX IF NOT EXISTS idx_contract_rm_renewal
    ON contract_read_models (renewal_date)
    WHERE renewal_date IS NOT NULL AND status = 'active';

-- invoices: FindOverdue uses status + due_date
CREATE INDEX IF NOT EXISTS idx_invoices_overdue
    ON invoices (due_date)
    WHERE status = 'issued' AND due_date IS NOT NULL;

-- balance_entries: FindAvailable uses account_id + currency + remaining_amount > 0
CREATE INDEX IF NOT EXISTS idx_balance_entries_available
    ON balance_entries (account_id, currency, created_at)
    WHERE remaining_amount > 0;
