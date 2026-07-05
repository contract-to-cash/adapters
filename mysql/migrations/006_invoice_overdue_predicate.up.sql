-- MySQL 8.0 FindOverdue index realignment
-- (mirrors postgres/migrations/006_invoice_overdue_predicate.up.sql).
--
-- 004 created idx_invoices_overdue on (due_date) (the partial-index WHERE
-- predicate is unsupported in MySQL and was dropped). FindOverdue now mirrors
-- the core in-memory reference:
--
--   status = 'overdue'
--   OR (status IN ('issued', 'finalized') AND due_date < NOW(6))
--
-- which filters on status as well as due_date, so repoint the index to the
-- composite (status, due_date). The planner can use it for the status
-- equality/IN scans and still order by due_date.
--
-- MySQL 8.0 supports neither DROP INDEX IF EXISTS nor CREATE INDEX IF NOT
-- EXISTS, so each change is guarded by an information_schema lookup executed
-- through a prepared statement (a no-op 'SELECT 1' when there is nothing to
-- do). The final state therefore converges both on fresh databases (004 just
-- created the old index) and on databases already reconciled by hand.

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'invoices'
          AND index_name = 'idx_invoices_overdue'),
    'ALTER TABLE invoices DROP INDEX idx_invoices_overdue',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    NOT EXISTS(
        SELECT 1 FROM information_schema.statistics
        WHERE table_schema = DATABASE()
          AND table_name = 'invoices'
          AND index_name = 'idx_invoices_overdue'),
    'ALTER TABLE invoices ADD KEY idx_invoices_overdue (status, due_date)',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;
