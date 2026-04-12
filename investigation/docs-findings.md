# contract-to-cash/core Docs Investigation — Adapter-Focused Findings

**Investigated**: 2026-04-10
**Target**: `contract-to-cash/core` @ `main`
**Scope**: All files under `docs/` + root `README.md`, `README.ja.md`, `CLAUDE.md` (plus cross-checks against `application/tx/tx.go` and `infrastructure/inmemory/*.go` to disambiguate the docs).

---

## TL;DR

- **External adapters are a first-class concept but not a named architectural noun.** The docs call persistence implementations "infrastructure implementations" and expect you to write them. There is a dedicated integration guide showing a skeleton PostgreSQL event-store/repository, but no production-grade reference SQL adapter and no conformance test kit.
- **Core ships exactly one infrastructure implementation: `infrastructure/inmemory/`**, marked explicitly as "for testing/demos". Everything else you bring yourself.
- **Only `Contract` is event-sourced.** `Invoice`, `Payment`, `CreditNote`, `BalanceEntry`, `UsageRecord`, `Price`, `Product` are state-stored entities with plain `Save`/`FindByID` repositories.
- **`TxManager.RunInTx`** is defined in `application/tx/tx.go` and exposes exactly four write repositories (`Contracts`, `Invoices`, `Payments`, `Balances`). Read-only repos (pricing, product, usage) are intentionally excluded from the tx-scope — "they have no writes to transact and reads complete outside the transaction" (verbatim from `tx.go`).
- **Version-conflict signalling has two competing forms.** The docs only define `shared.ErrCodeVersionConflict` (a `DomainError` code), but the actual `application/tx` package defines a separate sentinel `var ErrVersionConflict = errors.New("version conflict")` used by `RetryOnConflict`. The in-memory event store returns the `shared.DomainError` form, which means `RetryOnConflict` will **not** retry it out of the box. This is an inconsistency an adapter author needs to be aware of.
- **No stream-ID prefix convention.** `streamID` is just `string(aggregate.ID())` — no `"contract-"` prefix anywhere in docs or code.
- **No conformance test suite** is published for adapter implementations.

---

## 1. Adapter philosophy — Is it first-class? Is there PG/SQL guidance?

### Verdict
Adapters are treated as first-class in the sense that the whole architecture is Ports & Adapters / Clean Architecture, and the tagline explicitly says "**you bring your own database and payment gateway**" (`README.md:9`). But the word "adapter" is reserved for plugin/payment extensions; persistence implementations are called "infrastructure implementations".

### Evidence

- `README.md:9`:
  > "Provides domain models, billing pipelines, and extension points — **you bring your own database and payment gateway**."

- `docs/architecture.md:67-82` lists PostgreSQL/MySQL/DynamoDB explicitly as examples of the infrastructure layer:
  ```
  Infrastructure Layer: PostgreSQL, MySQL, DynamoDB, EventStore...
  ```
  and the strict rules:
  > "Interfaces are defined in `domain/` or `application/port/`; implementations live in `infrastructure/`"

- `docs/guides/integration.md` is the **only** document that gives concrete adapter-writing guidance. It contains:
  - Step 1: "Implement Repository Interfaces" (`:15-54`)
  - Step 2: "Implement Event Store" with a **full PostgreSQL `Append` code sample** (`:56-90`) — including raw SQL `INSERT INTO events (...)`, manual optimistic-lock check via `SELECT MAX(version)`, and the comment `// Check current version (optimistic locking)`.
  - Step 3: Implement PaymentGateway (`:92-107`)
  - Step 4: Wiring via `NewPostgresContractRepository`, `NewPostgresInvoiceRepository`, `NewPostgresPaymentRepository`, etc. (`:109-152`)
  - A suggested directory layout (`:170-194`) with `internal/infrastructure/postgres/{event_store.go, contract_repo.go, invoice_repo.go, ...}` and `internal/infrastructure/migrations/`

- `docs/internals/event-sourcing.md:903-972` goes further and provides a **full PostgreSQL DDL** for `events` and `snapshots` tables, including indexes and a `UNIQUE (stream_id, version)` constraint as the recommended optimistic-lock mechanism. It also shows a sample `contracts_projection` table.

- `docs/internals/codebase-review-20260327.md:179-183` explicitly addresses the question "why is there no PG implementation?" under "指摘取り下げ（過大評価だったもの）":
  > "**PostgreSQL/MySQL実装が未提供** — ライブラリとしてポートインターフェースを定義するのが責務。DB実装を同梱しないのは適切な設計判断"
  > (Translation: "No PG/MySQL implementation provided — defining port interfaces is the library's responsibility. Not shipping DB implementations is an appropriate design decision.")

### Conclusion
Core has **clearly decided not to ship SQL adapters**, and the guidance to "write your own" is explicit. The PG example in `integration.md` is a documentation skeleton, not production code.

---

## 2. Entity persistence strategy (non-event-sourced entities)

### Verdict
State-stored. There is **no documented reconstruction pattern** for Invoice/Payment/Balance — repositories are expected to load/save the entity's current state directly.

### Evidence

- `docs/concepts/domain-model.md:163-167`:
  > "Contract is the event-sourced aggregate root (`ContractAggregate`)" — Contract is the **only** entity described as event-sourced.

- `docs/api/domain-types.md:207-216` — `contract.Repository` is the only repo whose `Save` semantics are tied to events (via `UncommittedEvents()`).

- `docs/api/domain-types.md:222-281` — Invoice docs describe `NewInvoice(...)` as a normal constructor with functional options, and list state-mutation methods (`Finalize`, `Void`, `RecordPayment`) — no events emitted.

- `docs/internals/domain-model.md:1111-1125` — `invoice.Repository` interface is plain CRUD: `Save`, `FindByID`, `FindByContractID`, `FindOverdue`, `FindByStatus`, etc. Same shape for `payment.Repository` (`:1243-1249`), `balance.Repository` (`:1795-1814`), `usage.Repository`, etc.

- The in-memory reference implementations confirm this. `infrastructure/inmemory/invoice_repository.go`:
  ```go
  func (r *InMemoryInvoiceRepository) Save(_ context.Context, inv *invoice.Invoice) error {
      r.invoices[inv.ID()] = inv  // plain put
      return nil
  }
  ```
  No event log, no snapshot, no rehydration.

- **Surprising wrinkle**: even `InMemoryContractRepository.FindByID` (which you'd expect to replay from the event store) actually returns from an **in-memory cache**:
  ```go
  func (r *InMemoryContractRepository) FindByID(..., id shared.ContractID) (...) {
      agg, ok := r.contracts[id]  // cached Go pointer
      ...
  }
  ```
  Only `FindByIDAsOf` uses `LoadUntil` + `LoadFromHistory`. A real SQL adapter would have to actually replay — the in-memory impl cheats.

### Reconstruction pattern for Invoice/Payment/Balance
**None documented.** The only "reconstruction" discussed anywhere in docs is event-replay for the Contract aggregate (via `LoadFromHistory` or `LoadFromSnapshot` on `ContractAggregate`). For state-stored entities, the implicit pattern is "deserialize a row into the entity struct" and core gives no guidance on how to construct an Invoice from a DB row (no `invoice.Hydrate`, no `invoice.Reconstruct`, no `invoice.FromState` function is documented). Adapter authors must either:
(a) call `invoice.NewInvoice(...)` with all the functional options, or
(b) rely on the domain package exporting enough constructors/setters — which it doesn't uniformly (e.g. `Invoice.SetRevisionOf`, `SetOriginalInvoiceID` exist, but there's no public way to set status directly; you'd have to call `Finalize()` etc. which may fail validation).

This is a real gap the adapters repo will have to solve.

---

## 3. Event sourcing scope

### Verdict
Only `ContractAggregate` is event-sourced. **No plans documented** to event-source anything else.

### Evidence

- `README.md:124`: architecture diagram labels only `contract/` as "Event-sourced contract aggregate".
- `docs/architecture.md:169`: in the "Domain Entities" table, only Contract is tagged as "Event Sourced Aggregate"; Invoice/CreditNote/Payment/Price/Product/BalanceEntry are "Entity" or "Immutable Entity".
- `CLAUDE.md:44-52` (root) has the canonical table:
  | エンティティ | 種別 |
  |---|---|
  | Contract | Event Sourced Aggregate |
  | Invoice | Entity |
  | CreditNote | Entity |
  | Payment | Entity |
  | Price | Immutable Entity |
  | Product | Entity |
  | BalanceEntry | Entity |

- `docs/decisions/design-decisions.md:334-342` "9. 将来の検討事項" (Future considerations) does **not** mention expanding event sourcing to other aggregates. The listed future items are: distributed transactions, event archival, realtime notifications, A/B testing for pricing plans, multi-currency extension.

- `docs/api/event-store.md:131-148` lists Contract event types only. No Invoice/Payment/etc. events exist anywhere.

---

## 4. Stream ID convention

### Verdict
**No prefix convention.** The stream ID is literally the aggregate's ID string.

### Evidence

- `docs/api/event-store.md:14`: `Append(ctx, streamID string, events []Event, expectedVersion int) error` — documents streamID as opaque string.
- `docs/api/event-store.md:43`: `StreamID string // Aggregate ID`
- `docs/concepts/event-sourcing.md:38`: `StreamID string // Aggregate ID (contract ID)`
- `docs/guides/integration.md:35`: the example PG repo does `r.eventStore.Append(ctx, string(agg.ContractID()), events, ...)` — raw ULID, no prefix.
- `infrastructure/inmemory/contract_repository.go`: `r.store.Append(ctx, aggregate.ID(), events, ...)` — again, raw aggregate ID.
- `eventstore/aggregate_base.go` (reproduced in `docs/internals/event-sourcing.md:235`): `StreamID: a.id` — the BaseAggregate passes its ID directly.
- Recommended PG DDL in `docs/internals/event-sourcing.md:911` is `stream_id VARCHAR(255) NOT NULL` — no hint at a prefix.

**Implication**: If the adapters repo wants to multiplex contracts and future aggregates into a single `events` table, it will need to invent its own prefix scheme — core has no opinion. A `"contract-"` prefix is a perfectly reasonable choice but is **not** mandated or mentioned anywhere in core docs.

---

## 5. Version-conflict error — canonical form

### Verdict
**There are two competing forms, and they are not cross-compatible.** This is a real source of bugs for adapter authors.

### Evidence

(a) `domain/shared` form, documented in `docs/api/domain-types.md:91`:
```go
ErrCodeVersionConflict  ErrorCode = "version_conflict"
```
Returned as `shared.NewDomainError(shared.ErrCodeVersionConflict, "...")`. This is what `infrastructure/inmemory/event_store.go` currently returns on `Append` conflict:
```go
if currentVersion != expectedVersion {
    return shared.NewDomainError(shared.ErrCodeVersionConflict,
        fmt.Sprintf("expected version %d but stream %q is at version %d", ...))
}
```

(b) `application/tx` form, defined in `application/tx/tx.go` (not documented in any `docs/*.md` file — found only by reading source):
```go
// ErrVersionConflict is returned when an optimistic lock conflict is detected.
// RetryOnConflict will retry on this error.
var ErrVersionConflict = errors.New("version conflict")
```

`tx.RetryOnConflict` explicitly checks `errors.Is(err, ErrVersionConflict)`:
```go
func RetryOnConflict(maxRetries int, fn func() error) error {
    for i := 0; i < maxRetries; i++ {
        err = fn()
        if err == nil { return nil }
        if !errors.Is(err, ErrVersionConflict) { return err }
    }
    return err
}
```

### Critical finding
A `shared.DomainError{Code: ErrCodeVersionConflict}` does **not** satisfy `errors.Is(err, tx.ErrVersionConflict)` unless `DomainError` implements an `Is(target error) bool` method — and from the docs it doesn't appear to. This means:

- **The in-memory event store's conflict error is never retried by `tx.RetryOnConflict`.**
- An adapter that wants `RetryOnConflict` to work must return `tx.ErrVersionConflict` (or wrap it such that `errors.Is` unwraps to it).

### Recommendation for the adapters repo
Return `tx.ErrVersionConflict` from `Append` (possibly wrapped with `fmt.Errorf("stream %q: %w", streamID, tx.ErrVersionConflict)`). Do **not** rely on `shared.ErrCodeVersionConflict` — that's the older form and is incompatible with `RetryOnConflict`. Consider also filing an issue upstream, because core's own in-memory implementation is inconsistent with its own `RetryOnConflict` helper.

---

## 6. Snapshot policy

### Verdict
Taking snapshots is the **service's responsibility** via `SnapshotService`, configured by an interval (default 100 events). Adapters just need to implement the `SaveSnapshot`/`LoadSnapshot`/`LoadSnapshotBefore` methods on the `Store` interface.

### Evidence

- `docs/api/services.md:196-208`:
  ```go
  snapshotService := service.NewSnapshotService(eventStore, clock, interval)
  snapshotService.CreateSnapshot(ctx, aggregate) error
  ```
- `docs/concepts/event-sourcing.md:68-77`:
  > "For aggregates with many events, snapshots avoid replaying the entire event history... `service.NewSnapshotService(eventStore, clock, 100) // every 100 events`"
- `docs/decisions/design-decisions.md:305-310`: "N件ごとのスナップショット（デフォルト100件）" — N-events-based, default 100, user-adjustable.
- `docs/internals/event-sourcing.md:976-1019` shows `ShouldCreateSnapshot(currentVersion) = currentVersion > 0 && currentVersion % interval == 0` and `CreateSnapshot` as application-service responsibilities.
- `docs/guides/integration.md:166-168` suggests running `SnapshotService` as a periodic job:
  > "Snapshot creation (run periodically for performance): `snapshotService := service.NewSnapshotService(eventStore, clock, 50)`"

### Adapter responsibility
Only implement the three `Snapshot` methods on `eventstore.Store`. The decision of **when** to snapshot lives above the adapter, in application code calling `SnapshotService.CreateSnapshot`.

---

## 7. Projection / Read-model strategy

### Verdict
**Consumer's choice** — sync OR async, configurable via `ProjectionOptions.SyncMode`.

### Evidence

- `docs/architecture.md:115-120` "CQRS":
  > "**Simplified CQRS** — Uses projection tables in the same DB. Sync/async selectable via options"
- `docs/decisions/design-decisions.md:24-39` explicit decision: "Projection更新タイミング = 利用者が選択可能（同期/非同期をオプションで指定可能）". Rationale: immediate consistency → sync, throughput → async.
- `docs/api/services.md:232-253`:
  ```go
  projService := projection.NewProjectionService(eventStore, projection.ProjectionOptions{
      SyncMode:   true,
      BatchSize:  100,
      MaxRetries: 3,
      RetryDelay: time.Second,
      ...
  })
  projService.RegisterProjector(myProjector)
  projService.Start(ctx) // Blocking — subscribes to events
  projService.ProcessEvent(ctx, event) // Manual processing
  ```
- `Projector` interface (`docs/api/services.md:237-241`):
  ```go
  type Projector interface {
      Project(ctx context.Context, event eventstore.Event) error
      Rebuild(ctx context.Context, until time.Time) error
  }
  ```
- `docs/internals/event-sourcing.md:788-840` shows both `Start` (subscribes via `eventStore.Subscribe`) and `RebuildAll` (uses `LoadAll` for cross-stream ordering). Explicitly notes:
  > "呼び出し元は事前に既存の Projection データをクリアすること" (caller must clear existing projection data before rebuild)

### Adapter implications
- The event store must support `Subscribe(ctx, fromPosition) <-chan Event` for async projections.
- Must support `LoadAll(ctx, fromPosition, limit)` for rebuild (this is documented in `internals` but missing from the public `docs/api/event-store.md` — another gap).
- `GlobalPosition int64` on the Event struct is required for cross-stream ordering.

Projections are **not** automatically run by repositories. BillingService/PaymentService write to the event store; async projectors pick events up via Subscribe; sync projectors must be wired to fire after write. There is no "repository saves → projection updated" coupling at the adapter layer.

---

## 8. Transaction boundaries — `TxManager.RunInTx`

### Verdict
TxManager scopes **writes only** for exactly four aggregates. Reads (pricing, product, usage) are explicitly outside the transaction. The docs only reference TxManager in passing via service-option APIs; the canonical definition lives in `application/tx/tx.go`.

### Evidence from docs

- `docs/api/services.md:26, 71, 127`:
  ```go
  service.WithBillingTxManager(txManager),    // tx.TxManager (defaults to NoopTxManager)
  service.WithPaymentTxManager(txManager),    // tx.TxManager (defaults to NoopTxManager)
  service.WithCreditNoteTxManager(txManager), // tx.TxManager (defaults to NoopTxManager)
  ```
- `README.md:133`, `docs/architecture.md:150`: `application/tx/` described as "Transaction management (TxManager, Saga)".
- `docs/api/services.md:191` (about `ReissueInvoice`): "All writes run within a transaction."
- `docs/internals/domain-model.md:1843-1870` (credit application section) explicitly discusses the trade-off: "イベントソーシングの『1トランザクション = 1集約』原則と緊張関係にある" and commits to "アプリケーションサービス層での同一DBトランザクション" (application-service-layer same-DB transaction), with a future-path to Saga if the project ever moves to full CQRS.

### Evidence from source (`application/tx/tx.go`)

Because no `docs/*.md` file actually defines `TxManager`, I fetched the source. The authoritative definition:

```go
// Repos holds transaction-scoped repositories for write operations.
// Read-only repositories (pricing, product, usage) are excluded — they have
// no writes to transact and reads complete outside the transaction.
type Repos struct {
    Contracts contract.Repository
    Invoices  invoice.Repository
    Payments  payment.Repository
    Balances  balance.Repository
}

type TxManager interface {
    RunInTx(ctx context.Context, fn func(ctx context.Context, repos Repos) error) error
}

type NoopTxManager struct { r Repos }
func NewNoopTxManager(r Repos) *NoopTxManager { return &NoopTxManager{r: r} }
func (m *NoopTxManager) RunInTx(ctx context.Context, fn func(context.Context, Repos) error) error {
    return fn(ctx, m.r)
}
```

### Key semantics
1. **The closure receives fresh transaction-scoped repository instances**, not the ambient ones. This means a SQL adapter's `TxManager.RunInTx` must construct a new `Repos{...}` whose underlying `db` is actually a `*sql.Tx` for the duration of the closure.
2. **Only 4 write repositories** inside the tx. Read repositories (`pricing`, `product`, `usage`) are deliberately left outside — they must be callable on the ambient (non-tx) connection.
3. **Nil return = commit, error return = rollback.**
4. **NoopTxManager** is the library-provided default — it just passes through the stored repos, used by in-memory tests.
5. **`RetryOnConflict(maxRetries, fn)`** is a sibling helper, not a method on TxManager. The typical pattern is:
   ```go
   tx.RetryOnConflict(3, func() error {
       return txManager.RunInTx(ctx, func(ctx, repos) error { ... })
   })
   ```

### Read repositories outside transactions?
**Yes, explicitly.** From the `Repos` doc comment: "reads complete outside the transaction". This means in the adapters repo:
- `contract.Repository.FindByID`, `invoice.Repository.FindByID`, etc. called outside `RunInTx` should work on a plain connection pool, not require a transaction handle.
- This forces a design where each adapter implementation carries both "ambient" and "tx-scoped" modes — a common SQL pattern is to abstract over `sql.DB` + `sql.Tx` via a narrow `DBTX` interface (what sqlc calls it).

---

## 9. Reconstruction APIs

### Verdict
**Only Contract has a documented reconstruction API** (`LoadFromHistory`, `LoadFromSnapshot`). No `Reconstruct` / `Hydrate` / `Rehydrate` function exists anywhere in the docs for Invoice/Payment/etc.

### Evidence

- `docs/api/domain-types.md:195-202`:
  ```go
  agg.LoadFromHistory(events []eventstore.Event) error
  agg.LoadFromSnapshot(snapshot eventstore.Snapshot) error
  agg.MarshalSnapshot() ([]byte, error)
  ```
- `docs/concepts/event-sourcing.md:24-29`:
  ```go
  agg := contract.NewContractAggregate(contractID, clock)
  events, _ := eventStore.Load(ctx, string(contractID))
  agg.LoadFromHistory(events)
  ```
- `docs/guides/integration.md:38-54` is the most complete reconstruction example — it shows the **snapshot-first, then-replay-tail** pattern:
  ```go
  snap, _ := r.eventStore.LoadSnapshot(ctx, string(id))
  agg := contract.NewContractAggregate(id, r.clock)
  if snap != nil {
      agg.LoadFromSnapshot(*snap)
      events, _ := r.eventStore.LoadUntilVersion(ctx, string(id), snap.Version)
      // ... replay remaining events
  } else {
      events, _ := r.eventStore.Load(ctx, string(id))
      agg.LoadFromHistory(events)
  }
  ```
  (Note: the sample code has a bug — it loads `LoadUntilVersion(..., snap.Version)` when it should be events **after** the snapshot. This appears to be a doc-level typo; the query signature `LoadUntilVersion` is wrong for this use case.)

- For Invoice/Payment/Balance: **no reconstruction guidance**. The docs assume you'll `NewInvoice(...)` with enough functional options to reproduce the persisted state. The adapter has to figure out for itself how to rebuild an Invoice from a DB row — there's no `invoice.Rehydrate(InvoiceState)` or similar.

---

## 10. Testing strategy & conformance test suite

### Verdict
**No conformance test suite is published for adapter authors.** Core ships its own unit/integration tests for its in-memory impl, but there is no drop-in test kit you can run against a PG/MySQL adapter.

### Evidence

- Searched every doc for "conformance", "test suite", "fake", "test double", "helper" — none appear in the context of adapter testing.
- `docs/guides/integration.md` has **no** "Testing your adapter" section. Steps 1-5 cover implement → wire → batch jobs → directory layout, then the doc ends.
- `CLAUDE.md:102-107` ("テスト" section) gives intra-core testing guidance only:
  > "`shared.FixedClock` で時刻を固定化 / `infrastructure/inmemory/` のリポジトリ実装をテストで使用 / `-race` フラグを常に有効化"
- `docs/internals/codebase-review-20260327.md:137` notes test coverage for `infrastructure/inmemory` is only **35.2%** ("7ファイルにテストなし"), which means **even core's own in-memory reference isn't fully tested** — certainly no polished "here's how to test any impl of these interfaces" kit.
- `docs/guides/performance.md` provides benchmarks for core's own in-memory impls but no benchmark harness targeted at third-party adapters.

### What exists that an adapter author *can* reuse
- `shared.FixedClock{FixedTime: ...}` — deterministic clock for any test.
- `infrastructure/inmemory/*` can be used as a golden reference for expected behaviour on reads/writes.
- `tests/integration/` exists at the repo root (per `CLAUDE.md:107`) but is not documented as reusable.

### Gap
The adapters repo should probably author its own conformance suite (table-driven tests against the `eventstore.Store` + 7 repository interfaces) and could consider upstreaming it.

---

## 11. Reference implementations & planned adapters

### Verdict
Only `infrastructure/inmemory/` exists and is labelled "for testing/demos". No PG/MySQL/DynamoDB adapter is planned or in progress, and the core review committee has explicitly decided **not** to ship one.

### Evidence

- `README.md:54-62`: the quick-start comment is explicit:
  > "// Infrastructure (replace with your own in production)"
  ```go
  es := inmemory.NewInMemoryEventStore(clock)
  contractRepo := inmemory.NewInMemoryContractRepository(es, clock)
  ...
  ```
- `docs/api/event-store.md:150-160`:
  > "## In-Memory Implementation — For testing and demos: ... Thread-safe, supports all Store interface methods including subscriptions and snapshots."
- `docs/architecture.md:156`: `infrastructure/inmemory/     # In-memory implementations (test/demo)`
- `docs/internals/codebase-review-20260327.md` table row:
  > "DB | インメモリ実装のみ (ポート定義でPostgreSQL/MySQL等に対応可)"
- **Explicit non-roadmap** item (`codebase-review-20260327.md:180-181`):
  > "PostgreSQL/MySQL実装が未提供 — ライブラリとしてポートインターフェースを定義するのが責務。DB実装を同梱しないのは適切な設計判断"

No other adapters (DynamoDB, Cassandra, MongoDB, Redis, etc.) are mentioned as work-in-progress anywhere.

---

## 12. Open issues, TODOs, known limitations related to persistence/adapters

From `CLAUDE.md:135-148` and `docs/internals/codebase-review-20260327.md:144-184`:

### High-severity items that will affect adapter authors
| # | Issue | Relevance for adapters |
|---|-------|------------------------|
| 1 | `NewUsageRecord` accepts negative `quantity` (no validation). `domain/usage/entity.go:21-38`. | Adapter can't rely on domain to reject bad data — adapter-level validation may be needed. |
| 2 | `UsagePrice.CalculatePrice` returns negative money for negative usage. | Same. |
| 3 | `NewLineItem` accepts negative `quantity`. | Same. |
| 4 | `domain/payment` state-transition methods untested. | Adapter tests can't assume state transitions are bulletproof upstream. |
| 5 | `application/port/webhook.go` WebhookProcessor untested. | N/A for persistence adapters. |
| 6 | `application/query/TemporalQueryService` untested. | Adapter author should add their own temporal query tests — relies on `LoadUntil`/`LoadSnapshotBefore` correctness. |
| 7 | `application/projection/ProjectionService` untested. | Adapter author implementing async projection pipeline should add their own. |

### Medium
| # | Issue | Relevance |
|---|-------|-----------|
| 8 | `Product.AddFeature/AddUsageMetric` no duplicate check. | Adapter may see duplicate rows. |
| 9 | `BalanceEntry.sourceType` is untyped `string`. | Typos invisible to compiler — schema constraint required. |
| 10 | `UsageRecord.metricName` untyped string. | Same — consider VARCHAR + check constraints. |
| 11 | `infrastructure/inmemory/` 7 files have no tests. | Reference impl isn't a gold standard. |

### Low
- `PaymentService` drops hook errors via `_ = hookErr` in two places (non-critical path, intentional).
- Same-priority plugin execution order is registration-order dependent (not adapter-relevant).

### Separate doc-level gap (found during this investigation)
- **`tx.ErrVersionConflict` is not documented** anywhere in `docs/`. It exists only in `application/tx/tx.go`. And core's own `inmemory.InMemoryEventStore` does **not** return it — it returns `shared.DomainError{Code: ErrCodeVersionConflict}`. This is a bug or at least an inconsistency. The adapters repo needs to pick a lane and stick to it, and should probably use `tx.ErrVersionConflict` so that `tx.RetryOnConflict` works.
- **`LoadAll(ctx, fromPosition, limit)` is documented in `docs/internals/event-sourcing.md:73` but missing from `docs/api/event-store.md`**. Adapters must implement it for projection rebuild.
- **`GlobalPosition int64`** on Event is only mentioned in internals (`:116`), not in the public event-store API doc. Adapters must populate it.

---

## Answers-at-a-glance matrix

| # | Question | Answer |
|---|----------|--------|
| 1 | Adapters first-class? PG guidance? | Yes philosophically; concrete guide exists in `docs/guides/integration.md` with a PG skeleton and a full `events` DDL in `docs/internals/event-sourcing.md`. Users are expected to write their own. |
| 2 | Non-ES entity persistence strategy? | Plain state-stored; `Save(entity)` + `FindByID`. No `Hydrate`/`Reconstruct` pattern documented. |
| 3 | ES scope? | Contract only. No plans to expand. |
| 4 | Stream ID format? | Raw `string(aggregateID)`. No `"contract-"` prefix anywhere. |
| 5 | Version-conflict canonical form? | **Inconsistent.** `tx.ErrVersionConflict` (sentinel, source-only) is the one `RetryOnConflict` checks. `shared.ErrCodeVersionConflict` (DomainError, documented) is what in-memory returns. **Use `tx.ErrVersionConflict` in new adapters.** |
| 6 | Snapshot policy? | N-events (default 100), configurable. Service responsibility via `SnapshotService`, not adapter. Adapter implements the 3 snapshot methods on `Store`. |
| 7 | Projection strategy? | Consumer's choice: sync OR async. Via `ProjectionService` + `Projector` interface. Adapter must support `Subscribe` + `LoadAll` + `GlobalPosition`. |
| 8 | TxManager semantics? | `RunInTx(ctx, fn(ctx, Repos))`. Repos contains only Contracts/Invoices/Payments/Balances. Reads on pricing/product/usage happen outside the tx. `NoopTxManager` provided by library. |
| 9 | Reconstruction APIs? | `LoadFromHistory`, `LoadFromSnapshot`, `MarshalSnapshot` — Contract only. Nothing for other entities. |
| 10 | Conformance test suite? | None. Adapters repo should author its own. |
| 11 | Reference impls? | `infrastructure/inmemory/` only. No PG/MySQL/etc. planned — explicit "out of scope". |
| 12 | Open TODOs affecting adapters? | Negative-quantity validation gap; untested payment state machine; `tx.ErrVersionConflict` vs `shared.ErrCodeVersionConflict` inconsistency; `LoadAll`/`GlobalPosition` undocumented in public API. |

---

## Source files consulted

All under `contract-to-cash/core@main`:

- `/README.md` (fetched: root)
- `/README.ja.md`
- `/CLAUDE.md`
- `/docs/architecture.md`
- `/docs/concepts/domain-model.md`
- `/docs/concepts/event-sourcing.md`
- `/docs/concepts/payment-gateway.md`
- `/docs/concepts/plugin-system.md`
- `/docs/guides/integration.md` *(most important for adapter authors)*
- `/docs/guides/custom-plugin.md`
- `/docs/guides/payment-integration.md`
- `/docs/guides/performance.md`
- `/docs/guides/temporal-queries.md`
- `/docs/api/event-store.md`
- `/docs/api/services.md`
- `/docs/api/domain-types.md`
- `/docs/api/plugin-hooks.md`
- `/docs/decisions/design-decisions.md`
- `/docs/internals/domain-model.md` *(definitive repository interface signatures)*
- `/docs/internals/event-sourcing.md` *(definitive PG DDL, projection semantics)*
- `/docs/internals/codebase-review-20260327.md`

Plus **source files fetched to disambiguate docs**:

- `/application/tx/tx.go` — definitive `TxManager`, `ErrVersionConflict`, `RetryOnConflict` definitions (not in docs).
- `/infrastructure/inmemory/event_store.go` — actual in-memory `Append` behaviour (returns `shared.DomainError`, not `tx.ErrVersionConflict`).
- `/infrastructure/inmemory/contract_repository.go` — shows that the in-memory contract repo uses an in-memory cache for `FindByID`, not event replay.
- `/infrastructure/inmemory/invoice_repository.go` — shows Invoice persistence is plain state storage.
