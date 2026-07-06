-- 010: contract-creation idempotency uniqueness (issue adapters#46 / core#159).
--
-- ContractAggregate.Create requires a non-empty CreateContractCommand.
-- IdempotencyKey and carries it on the contract.created event
-- (data->>'idempotency_key', SchemaVersion 3). The core can only validate
-- PRESENCE — it has no cross-aggregate view, so two retried Create calls with
-- the same key would otherwise produce two distinct contracts. This partial
-- unique expression index closes that at-most-once window at the storage layer,
-- exactly as recommended by contract.Repository.Save's godoc.
--
-- Scope of the constraint:
--   * type = 'contract.created' only — other event types are never constrained.
--   * coalesce(data->>'idempotency_key','') <> '' — historical pre-#159 events
--     omit the key (it is `json:",omitempty"`, so data->>'idempotency_key' is
--     NULL -> coalesce '' -> excluded). Empty/legacy keys are therefore EXEMPT,
--     matching the payments/usage NULL-exemption pattern.
--
-- On violation the event store's Append maps the 23505 on this index name to a
-- shared.ErrCodeConflict DomainError (postgres/eventstore.go), mirroring the
-- payments #35 idempotency-conflict translation.
--
-- IF NOT EXISTS makes a re-run against an already-migrated database a no-op
-- (this adapter's runner is forward-only; there are no .down files).
CREATE UNIQUE INDEX IF NOT EXISTS ux_contract_idempotency_key
    ON events ((data->>'idempotency_key'))
    WHERE type = 'contract.created'
      AND coalesce(data->>'idempotency_key', '') <> '';
