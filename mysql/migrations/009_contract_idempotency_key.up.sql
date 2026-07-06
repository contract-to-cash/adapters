-- 009: contract-creation idempotency uniqueness (issue adapters#46 / core#159).
-- MySQL mirror of postgres/migrations/010_contract_idempotency_key.up.sql.
--
-- ContractAggregate.Create requires a non-empty CreateContractCommand.
-- IdempotencyKey and carries it on the contract.created event
-- (data->'$.idempotency_key', SchemaVersion 3). The core validates PRESENCE
-- only; this index closes the at-most-once creation window at the storage layer
-- per contract.Repository.Save's godoc.
--
-- MySQL has no partial (WHERE-filtered) indexes, so the exemption is expressed
-- through a STORED generated column that is NULL for every row that must NOT be
-- constrained. A UNIQUE index permits multiple NULLs, so those rows never
-- collide:
--   * non-contract.created events                     -> NULL (unconstrained)
--   * contract.created with an empty/absent key        -> NULL (historical
--     pre-#159 events; JSON_EXTRACT of an absent key is SQL NULL, and an
--     explicit empty string is filtered by the `<> ''` guard) -> EXEMPT
--   * contract.created with a non-empty key            -> the key (constrained)
--
-- On violation the event store's Append maps the 1062 duplicate-key error on
-- this index name to a shared.ErrCodeConflict DomainError (mysql/eventstore.go +
-- mysql/errors.go), mirroring the payments #35 translation. This adapter's
-- runner is forward-only (no .down files); a mid-file failure is caught by the
-- 'pending' status marker.
ALTER TABLE events
    ADD COLUMN contract_idempotency_key VARCHAR(191)
        GENERATED ALWAYS AS (
            CASE
                WHEN type = 'contract.created'
                     AND JSON_UNQUOTE(JSON_EXTRACT(data, '$.idempotency_key')) <> ''
                THEN JSON_UNQUOTE(JSON_EXTRACT(data, '$.idempotency_key'))
                ELSE NULL
            END
        ) STORED;

CREATE UNIQUE INDEX ux_contract_idempotency_key ON events (contract_idempotency_key);
