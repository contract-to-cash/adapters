# PostgreSQL Adapter Design

**Status:** Approved, ready to implement
**Target:** `contract-to-cash/adapters` PostgreSQL adapter under `postgres/`
**Depends on:** `contract-to-cash/core` main (post-PR#91, #92, #95)

## Purpose

Provide a reference PostgreSQL implementation of all core repository interfaces. The adapter serves two roles:

1. **Reference implementation** — demonstrates how to build a C2C adapter, lowering the adoption barrier
2. **Production-ready** — can be used directly by adopters who want Postgres-backed persistence

Both roles prioritize **readability and faithfulness to the in-memory reference** over clever optimization.

## Source of truth

When in doubt, follow `core/infrastructure/inmemory/*`. The Postgres adapter should be a near-mechanical translation of the in-memory implementation to SQL, not a re-invention.

## Scope

### In scope

| Component | Core interface | Reference |
|---|---|---|
| `postgres/eventstore.go` | `eventstore.Store` | `inmemory/event_store.go` |
| `postgres/tx_manager.go` | `tx.TxManager` | `inmemory/` (NoopTxManager) |
| `postgres/contract_repo.go` | `contract.Repository` | `inmemory/contract_repository.go` |
| `postgres/invoice_repo.go` | `invoice.Repository` | `inmemory/invoice_repository.go` |
| `postgres/creditnote_repo.go` | `invoice.CreditNoteRepository` | - |
| `postgres/payment_repo.go` | `payment.Repository` | `inmemory/payment_repository.go` |
| `postgres/balance_repo.go` | `balance.Repository` | `inmemory/balance_repository.go` |
| `postgres/usage_repo.go` | `usage.Repository` | `inmemory/usage_repository.go` |
| `postgres/product_repo.go` | `product.Repository` | `inmemory/product_repository.go` |
| `postgres/price_repo.go` | `pricing.PriceRepository` | `inmemory/price_repository.go` |
| `postgres/contract_projector.go` | `projection.Projector` | - |
| `postgres/invoice_projector.go` | `projection.Projector` | - |
| `postgres/projection_checkpoint.go` | (adapter-internal) | - |
| `postgres/migrations/*.sql` | DDL | - |

### Out of scope

- Snapshot scheduling — core's `SnapshotService` drives when to snapshot. Adapter only implements `SaveSnapshot` / `LoadSnapshot` / `LoadSnapshotBefore`.
- Multi-currency amount scaling — use `Money.Int64()` per core conventions. This truncates; non-JPY correctness is a future follow-up.
- Connection pooling tuning — expose knobs via `Config`, use pgx defaults.

## Core rules (from investigation)

### 1. Error return convention

| Situation | Return |
|---|---|
| Optimistic lock failure | `tx.ErrVersionConflict` (required for `RetryOnConflict`) |
| Record not found | `shared.NewDomainError(shared.ErrCodeNotFound, msg)` |
| Any other domain violation | `shared.NewDomainError(shared.ErrCode*, msg)` |
| Infrastructure error | wrapped via `fmt.Errorf("context: %w", err)` |

### 2. Stream ID convention

**Bare `string(aggregate.ID())` — no prefix.** This matches `inmemory/contract_repository.go`. The earlier PR's `"contract-"` prefix is removed.

### 3. Transaction scoping

- `tx.Repos` contains: `Contracts`, `Invoices`, `Payments`, `Balances`. **Not** `CreditNotes`, `Usage`, `Product`, `Pricing`.
- **Reads happen outside Tx**, mutations applied in memory, writes inside `RunInTx`.
- **Nested `RunInTx` must work** — `CreditNoteService.ReissueInvoice` calls `BillingService.GenerateInvoice` which opens its own `RunInTx`. Implementation: context-embedded `pgx.Tx`.
- Read-only repositories (`product`, `pricing`, `usage`) operate on the pool directly; they never need transaction scoping.

### 4. Event store guarantees

- `Append` must be atomic per-stream, with optimistic concurrency on `(stream_id, version)`.
- `UNIQUE(stream_id, version)` constraint violation must be translated to `tx.ErrVersionConflict`.
- `GlobalPosition` is `BIGSERIAL`, strictly increasing, 0 is reserved (never used by real events).
- `LoadAll(ctx, fromPosition, limit)` — `fromPosition` is exclusive; `limit <= 0` means unlimited.
- `Subscribe` uses `LISTEN / NOTIFY` for wakeup plus `LoadAll` for catch-up. Slow subscribers **block** (no silent drop) — for a billing system, silent data loss is unacceptable.

### 5. Persistence strategy per entity

| Entity | Strategy |
|---|---|
| `ContractAggregate` | Event store + snapshot (via `MarshalSnapshot` / `LoadFromSnapshot`) |
| `Invoice` | `ToSnapshot` → SQL columns + JSONB for line items and metadata |
| `CreditNote` | Same as Invoice |
| `Payment` | `ToSnapshot` → SQL columns |
| `BalanceEntry` | `ToSnapshot` → SQL columns with `version` for optimistic lock |
| `BalanceApplication` / `BalanceRefund` | Public struct fields, direct columns, no snapshot needed |
| `UsageRecord` | `ToSnapshot` → SQL columns |
| `UsageSummary` | Public fields, computed on read, never stored |
| `Product` | `ToSnapshot` → SQL columns |
| `Price` | `ToSnapshot` → SQL columns |

**Rule of thumb:** queryable fields become columns, opaque values (`metadata`, nested objects used only on read) go to JSONB.

### 6. Read model / projection strategy

- Read models (`contract_read_models`, `invoice_read_models`) are async projections driven by `ProjectionService` → `Store.Subscribe`.
- `Projector.Rebuild(ctx, until)` uses `LoadAll` to replay. Delete + re-insert strategy; must run within a Tx with DEFERRABLE FK constraints if the table is referenced by other domain tables.
- `CheckpointStore` is adapter-internal (core does not define it). Stores `last_position` per projector name.

### 7. Repository query patterns

Contract repository methods like `FindByAccountID`, `FindExpiring`, `FindDueForRenewal`:
1. Query `contract_read_models` to get IDs.
2. For each ID, load the aggregate via event store (`FindByID`).
3. Return the aggregate slice.

**Known N+1** — acceptable for reference impl; a future optimization can batch-load events.

## Implementation order

Sequential, because each phase depends on the previous.

### Phase A: Infrastructure (connection, Querier, Tx)

- `postgres/conn.go`: `Config`, `NewPool`, `Querier` interface, context-embedded Tx helpers (`TxFromContext`, `ContextWithTx`, `QuerierFromContext`).
- `postgres/tx_manager.go`: `PostgresTxManager` with nested `RunInTx` via context propagation.

### Phase B: EventStore

- `postgres/migrations/001_event_store.up.sql`: `events` (BIGSERIAL global_position, UNIQUE(stream_id, version)), `snapshots`, `notify_events_inserted` trigger, `events_inserted` channel.
- `postgres/eventstore.go`: all 10 methods of `eventstore.Store`.
- Optimistic lock via `SELECT MAX(version) FOR UPDATE` + UNIQUE constraint catch → `tx.ErrVersionConflict`.
- `Subscribe`: `LISTEN events_inserted` + `LoadAll` for catch-up; blocking channel send.
- `Event.Metadata` unmarshaled from JSONB into `EventMetadata` struct.

### Phase C: Read models + projection checkpoint

- `postgres/migrations/002_read_models.up.sql`: `contract_read_models`, `invoice_read_models`, `projection_checkpoints`.
- DEFERRABLE FK from `invoices.contract_id` → `contract_read_models.id` so rebuild can DELETE + re-insert.
- `postgres/projection_checkpoint.go`: `CheckpointStore` (Load/Save/Reset).

### Phase D: ContractRepository + ContractProjector

- `postgres/contract_repo.go`: Save (append events, update aggregate version), FindByID (snapshot + replay), FindBy* read-model queries.
- `postgres/contract_projector.go`: Project (dispatch on event type, update `contract_read_models`), Rebuild (Tx + DEFERRABLE + DELETE + replay).
- **Requires a `shared.Clock` field** (needed by `NewContractAggregate(id, clock)`).

### Phase E: State-stored repositories

In this order (dependency): Invoice → CreditNote → Payment → Balance → Usage → Product → Price.

Each follows the pattern:

```go
func (r *Repo) Save(ctx context.Context, e *Entity) error {
    s := e.ToSnapshot()
    _, err := r.q(ctx).Exec(ctx, `INSERT ... ON CONFLICT DO UPDATE ...`, s.Field1, s.Field2, ...)
    return err
}

func (r *Repo) FindByID(ctx context.Context, id shared.EntityID) (*Entity, error) {
    var s EntitySnapshot
    err := r.q(ctx).QueryRow(ctx, `SELECT ... WHERE id = $1`, id).Scan(&s.Field1, &s.Field2, ...)
    if errors.Is(err, pgx.ErrNoRows) {
        return nil, shared.NewDomainError(shared.ErrCodeNotFound, fmt.Sprintf("<entity> %s not found", id))
    }
    if err != nil {
        return nil, fmt.Errorf("find <entity>: %w", err)
    }
    return entity.FromSnapshot(s)
}
```

### Phase F: InvoiceProjector

Similar to ContractProjector but for `invoice_read_models`. Read model has no FK targets, so rebuild can use plain DELETE.

### Phase G: Tests

- **Round-trip tests** per entity: build → Save → FindByID → compare via `ToSnapshot`
- **Optimistic lock test**: concurrent Tx on EventStore.Append → exactly one wins with `tx.ErrVersionConflict`
- **Nested RunInTx test**: outer Tx → inner `RunInTx` sees same pgx.Tx
- **Projector test**: Append events → wait for projection → verify read model
- **Rebuild test**: corrupt read model → call Rebuild → verify recovery
- **Subscribe test**: LISTEN/NOTIFY round-trip with catch-up from position 0

### Phase H: Conformance suite

Reusable test harness that runs against any `eventstore.Store` + `tx.TxManager` implementation. Exported so users can run it against their own custom adapter.

## DDL sketch (for reference)

### events / snapshots

```sql
CREATE TABLE events (
    id              TEXT        PRIMARY KEY,
    stream_id       TEXT        NOT NULL,
    type            TEXT        NOT NULL,
    version         INTEGER     NOT NULL,
    schema_version  INTEGER     NOT NULL DEFAULT 1,
    data            JSONB       NOT NULL,
    metadata        JSONB       NOT NULL DEFAULT '{}',
    occurred_at     TIMESTAMPTZ NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    global_position BIGSERIAL   NOT NULL,
    UNIQUE (stream_id, version)
);

CREATE INDEX idx_events_global_position ON events (global_position);
CREATE INDEX idx_events_type ON events (type);

CREATE OR REPLACE FUNCTION notify_events_inserted()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('events_inserted', NEW.global_position::TEXT);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_events_inserted
    AFTER INSERT ON events
    FOR EACH STATEMENT  -- single notification per batch, not per row
    EXECUTE FUNCTION notify_events_inserted();

CREATE TABLE snapshots (
    stream_id  TEXT        NOT NULL,
    version    INTEGER     NOT NULL,
    state      JSONB       NOT NULL,
    as_of      TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);
```

### Invoice table (representative)

Columns mirror `InvoiceSnapshot` fields; opaque values go to JSONB:

```sql
CREATE TABLE invoices (
    id                  TEXT        PRIMARY KEY,
    invoice_number      TEXT,
    account_id          TEXT        NOT NULL,
    contract_id         TEXT        NOT NULL,
    status              TEXT        NOT NULL,
    subtotal            BIGINT      NOT NULL,
    tax_amount          BIGINT      NOT NULL,
    discount_amount     BIGINT      NOT NULL,
    total               BIGINT      NOT NULL,
    applied_balance     BIGINT      NOT NULL,
    amount_due          BIGINT      NOT NULL,
    paid_amount         BIGINT      NOT NULL,
    balance             BIGINT      NOT NULL,
    currency            TEXT        NOT NULL,
    billing_period_from TIMESTAMPTZ,
    billing_period_to   TIMESTAMPTZ,
    issue_date          TIMESTAMPTZ,
    due_date            TIMESTAMPTZ,
    paid_at             TIMESTAMPTZ,
    void_reason         TEXT        NOT NULL DEFAULT '',
    revision_of         TEXT,
    original_invoice_id TEXT,
    payment_method_id   TEXT,
    allow_partial_pay   BOOLEAN     NOT NULL DEFAULT FALSE,
    line_items          JSONB       NOT NULL DEFAULT '[]',
    metadata            JSONB       NOT NULL DEFAULT '{}',
    version             INTEGER     NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_invoices_contract
        FOREIGN KEY (contract_id) REFERENCES contract_read_models (id)
        DEFERRABLE INITIALLY IMMEDIATE
);

CREATE INDEX idx_invoices_contract_id ON invoices (contract_id);
CREATE INDEX idx_invoices_account_id ON invoices (account_id);
CREATE INDEX idx_invoices_status ON invoices (status);
CREATE INDEX idx_invoices_contract_status ON invoices (contract_id, status);
CREATE INDEX idx_invoices_contract_issued ON invoices (contract_id, issue_date);
```

Other entities follow the same pattern.

## Open questions (non-blocking, can be decided during implementation)

1. **`invoice_history` table for `FindByIDAsOf`** — retain previous PR approach (shadow table with valid_from/valid_to), or use event sourcing once Invoice becomes event-sourced? For now, shadow table.
2. **`LineItem` storage** — JSONB in `line_items` column (keeps Save atomic) vs normalized child table (enables SQL queries on line items). Start with JSONB.
3. **`Money` persistence** — `BIGINT` via `Int64()` is lossy for non-JPY. Track as known limitation; follow-up.
4. **Projection sync mode** — core supports both sync and async. Default to async; document how to switch.

## References

- [core#94 / #95](https://github.com/contract-to-cash/core/pull/95): Snapshot/FromSnapshot API
- [core#91](https://github.com/contract-to-cash/core/pull/91): LoadAll / GlobalPosition
- [core#92](https://github.com/contract-to-cash/core/pull/92): RunInTx semantics fix
- [adapters#1](https://github.com/contract-to-cash/adapters/issues/1): Original spec
- [adapters#2](https://github.com/contract-to-cash/adapters/pull/2): First attempt (closed)
- `investigation/docs-findings.md`: core docs research
- `investigation/services-findings.md`: service code trace
- `investigation/github-findings.md`: issue/PR history
- `investigation/reconstruction-patterns.md`: DDD reconstruction research
