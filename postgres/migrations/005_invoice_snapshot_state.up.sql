-- Split the overloaded `line_items` JSONB column into two columns:
--   - line_items:     JSONB ARRAY of LineItemSnapshot (the original intent)
--   - snapshot_state: JSONB OBJECT with monetary subtotals + billing period
--
-- Prior to this migration, `line_items` was a misnomer — it contained the
-- full snapshot payload including subtotals and billing period. This made
-- the schema misleading and prevented SQL queries from inspecting line
-- items alone.
ALTER TABLE invoices
    ADD COLUMN IF NOT EXISTS snapshot_state JSONB NOT NULL DEFAULT '{}';
