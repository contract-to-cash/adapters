-- Issue #11: balance_entries / payments / prices stored their Money values in
-- BIGINT columns via Money.Int64(), which truncates the fractional part and
-- silently dropped precision (e.g. proration credits like ¥100.75, or the
-- cent portion of USD/EUR amounts).
--
-- This migration adds a precise `state` JSON column to each table, mirroring
-- the approach already used by invoices / credit_notes (Money is serialized as
-- a big.Rat RatString). The repositories now treat `state` as the source of
-- truth on read and keep the BIGINT columns as write-only, query/index-friendly
-- approximations. Existing rows have a NULL `state` and fall back to the BIGINT
-- columns on read, so this change is backward compatible.
--
-- MySQL 8.0 has no ADD COLUMN IF NOT EXISTS, so each add is guarded by an
-- information_schema lookup executed through a prepared statement (a no-op
-- 'SELECT 1' when the column already exists). This mirrors the guard style of
-- migrations 005 and 008 and makes the file idempotent, so re-applying it (e.g.
-- a test harness that reuses a database) converges instead of failing with
-- ER_DUP_FIELDNAME (1060).

SET @stmt := IF(
    NOT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'balance_entries'
          AND column_name = 'state'),
    'ALTER TABLE balance_entries ADD COLUMN state JSON NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    NOT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'payments'
          AND column_name = 'state'),
    'ALTER TABLE payments ADD COLUMN state JSON NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;

SET @stmt := IF(
    NOT EXISTS(
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'prices'
          AND column_name = 'state'),
    'ALTER TABLE prices ADD COLUMN state JSON NULL',
    'SELECT 1');
PREPARE migration_stmt FROM @stmt;
EXECUTE migration_stmt;
DEALLOCATE PREPARE migration_stmt;
