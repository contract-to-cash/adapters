-- Issue #11: balance_entries / payments / prices stored their Money values in
-- BIGINT columns via Money.Int64(), which truncates the fractional part and
-- silently dropped precision (e.g. proration credits like ¥100.75, or the
-- cent portion of USD/EUR amounts).
--
-- This migration adds a precise `state` JSONB column to each table, mirroring
-- the approach already used by invoices / credit_notes (Money is serialized as
-- a big.Rat RatString). The repositories now treat `state` as the source of
-- truth on read and keep the BIGINT columns as write-only, query/index-friendly
-- approximations. Existing rows have a NULL `state` and fall back to the BIGINT
-- columns on read, so this change is backward compatible.
--
-- IF NOT EXISTS keeps the migration idempotent when re-applied against a
-- persistent database (the test harness re-runs migrations).

ALTER TABLE balance_entries ADD COLUMN IF NOT EXISTS state JSONB;
ALTER TABLE payments        ADD COLUMN IF NOT EXISTS state JSONB;
ALTER TABLE prices          ADD COLUMN IF NOT EXISTS state JSONB;
