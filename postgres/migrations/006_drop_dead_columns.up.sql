-- Drop prices.billing_cycle: the interval is fully captured in interval_data
-- (JSONB). billing_cycle was always written as empty string and never read,
-- per the code review finding.
ALTER TABLE prices DROP COLUMN IF EXISTS billing_cycle;

-- Enforce non-negative usage quantities at the DB level. The domain guard
-- can still slip (or a future migration / external write could insert bad
-- data); this gives defence-in-depth.
ALTER TABLE usage_records
    DROP CONSTRAINT IF EXISTS usage_records_quantity_nonneg;
ALTER TABLE usage_records
    ADD CONSTRAINT usage_records_quantity_nonneg CHECK (quantity >= 0);
