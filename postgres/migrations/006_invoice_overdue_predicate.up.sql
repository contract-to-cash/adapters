-- Realign the FindOverdue partial index with the corrected query predicate.
--
-- 004 created idx_invoices_overdue with `WHERE status = 'issued' AND due_date
-- IS NOT NULL`, matching the old (too narrow) FindOverdue query. FindOverdue
-- now mirrors the core in-memory reference:
--
--   status = 'overdue'
--   OR (status IN ('issued', 'finalized') AND due_date < NOW())
--
-- so the partial index predicate must widen to cover 'finalized' as well, and
-- a second partial index serves the 'overdue' branch. Both index (due_date) so
-- the planner can also satisfy the ORDER BY due_date ASC.
--
-- Guarded with IF EXISTS / IF NOT EXISTS so the final state converges both on
-- fresh databases (004 just created the old index) and on databases already
-- reconciled by hand before this migration existed.

DROP INDEX IF EXISTS idx_invoices_overdue;

-- (b) issued/finalized invoices past their due date.
CREATE INDEX IF NOT EXISTS idx_invoices_overdue
    ON invoices (due_date)
    WHERE status IN ('issued', 'finalized') AND due_date IS NOT NULL;

-- (a) invoices already marked overdue, regardless of due_date.
CREATE INDEX IF NOT EXISTS idx_invoices_overdue_status
    ON invoices (due_date)
    WHERE status = 'overdue';
