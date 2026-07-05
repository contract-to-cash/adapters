-- MySQL 8.0: drop the write-side -> projection-table foreign keys
-- (mirrors postgres/migrations/006_drop_projection_fks.up.sql).
--
-- 003 pointed invoices / credit_notes / usage_records at contract_read_models
-- (a PROJECTION table). Under the async projection mode that core officially
-- supports, the projection lags the event log, so a freshly created contract's
-- read model may not exist yet when the first invoice/usage row is written and
-- the FK would reject a legitimate write. Referential integrity for contracts
-- belongs to the event log (the source of truth), not to a derived read model,
-- so these FKs are removed.
--
-- MySQL 8.0 has no ALTER TABLE ... DROP FOREIGN KEY IF EXISTS, so each drop is
-- guarded by an information_schema lookup executed through a prepared statement
-- (a no-op 'SELECT 1' when the constraint is already gone). The final state
-- therefore converges on databases created before 006 and any that dropped the
-- constraint by hand.

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'invoices'
          AND constraint_name = 'fk_invoices_contract'
          AND constraint_type = 'FOREIGN KEY'),
    'ALTER TABLE invoices DROP FOREIGN KEY fk_invoices_contract',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'credit_notes'
          AND constraint_name = 'fk_credit_notes_contract'
          AND constraint_type = 'FOREIGN KEY'),
    'ALTER TABLE credit_notes DROP FOREIGN KEY fk_credit_notes_contract',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    EXISTS(
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_schema = DATABASE()
          AND table_name = 'usage_records'
          AND constraint_name = 'fk_usage_records_contract'
          AND constraint_type = 'FOREIGN KEY'),
    'ALTER TABLE usage_records DROP FOREIGN KEY fk_usage_records_contract',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;
