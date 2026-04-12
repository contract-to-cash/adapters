# GitHub Investigation: contract-to-cash/core and adapters

Investigation date: 2026-04-10

## TL;DR — Direction of core team

1. **Core is deliberately dependency-free** (only `ulid` in `go.mod`). The `contract-to-cash/adapters` repo (this repo) is the *official* home for PostgreSQL, Stripe, Redis, Kafka and other infrastructure implementations. See core#69 close comment.
2. **Repository interfaces are stabilising**: `TxManager` (closure + `Repos` struct + Saga + `RetryOnConflict`) is the canonical transaction model (core#28, merged). Recent activity has been fine-tuning that model — moving state mutations inside `RunInTx` (core#92), adding idempotency lookups, etc.
3. **Event store got the cross-stream capability needed for read-model rebuild**: `LoadAll` + `Event.GlobalPosition` were added specifically because the adapters repo needed them to implement a projection rebuilder (core#83 → core#91, merged 2026-04-10).
4. **Payment gateway contract is being hardened in parallel** as part of the Stripe adapter work: `ThreeDSecureResult` on Charge/Authorize responses (core#81 → core#90, merged), Saga compensation switched `Void` → `Refund` (core#82 → core#84, merged), hardcoded `PaymentMethodCreditCard` fix (core#88 → core#93, merged).
5. **Three open core issues are directly relevant to the Stripe adapter implementation that will follow the PostgreSQL adapter** — see core#86, core#87.
6. **No `ADAPTERS.md` / `CONTRIBUTING.md` exists in core.** The closest guidance is `docs/guides/integration.md` (walks through implementing repositories/event-store/gateway with a PostgreSQL sketch) plus `docs/decisions/design-decisions.md`.
7. **No milestones or roadmap** are configured on core. Direction has to be inferred from issues/PRs.
8. **Org has only three repos**: `core`, `cli`, `adapters`. No separate `adapters-mysql`, `examples`, or `postgres` repo exists — despite core#69's closing comment suggesting one would be created as `contract-to-cash/postgres` (it was not; work landed in `adapters` instead).

---

## 1. Open issues in `contract-to-cash/core` (adapter/persistence relevant)

All three currently-open issues are relevant to adapter work:

| Issue | Title | Relevance |
|---|---|---|
| [contract-to-cash/core#81](https://github.com/contract-to-cash/core/issues/81) | ChargeResponse/AuthorizeResponseに ThreeDSecureResult フィールドを追加 | **CLOSED as of 2026-04-10 via core#90.** Was opened explicitly because "adaptersリポジトリでStripe PaymentGatewayを実装する際、3D Secure認証フローのレスポンスをクライアントに伝達する手段がないことが判明". |
| [contract-to-cash/core#86](https://github.com/contract-to-cash/core/issues/86) | ProcessPayment: 未決済チャージへの即時Refund失敗に対するフォールバック戦略 | Open. Enhancement. Saga compensation currently Refunds after Charge, but some gateways (non-card, bank transfer, conbini, carrier) reject Refund before Settlement. Proposes Void→Refund fallback, Authorize/Capture split, or compensation retry queue. |
| [contract-to-cash/core#87](https://github.com/contract-to-cash/core/issues/87) | ProcessPayment: 補償Refund後のリトライで charge→refund→charge サイクルが発生しうる | Open. Bug. When compensation Refund succeeds but the retry reuses the same IdempotencyKey, gateways (Stripe et al.) return the original Charge result → system records as "completed" despite being refunded. Proposes gateway-state check / marker persistence / retry-with-new-key. |

Note: The three currently-open issues listed above (81/86/87) come from `gh issue list --state open`. Issue #81 was open at the time the task was planned but is now closed/merged via PR #90.

No open issues are labelled with `persistence`, `postgres`, `storage`, `infrastructure`, or `reference implementation` — those concerns all closed out when work moved to this repo (see #69, #83 below).

---

## 2. Closed issues related to adapters / persistence / reference implementation

Extracted from `gh issue list --state closed --search "adapter OR postgres OR persistence OR reference"`.

### The critical one: [contract-to-cash/core#69](https://github.com/contract-to-cash/core/issues/69) — "Provide reference PostgreSQL implementation for repository interfaces"

Closed 2026-04-07. **This is the issue that explains why `contract-to-cash/adapters` exists.**

Key points from the body:
- Core defines 8+ repository interfaces with only `inmemory` implementations. "This is the single biggest barrier to adoption as a framework."
- SQL schema existed in `docs/design/event-sourcing.md` §8 but no `.sql` migration files, no Go impl, no `eventstore.Store` PostgreSQL reference, no transaction guidance.
- Proposed 3 phases: (1) migration files, (2) reference EventStore + TxManager, (3) repository implementations. Explicitly called out "EventStore is the most critical piece — it's the foundation of the event sourcing architecture and the hardest for users to implement correctly (optimistic locking, subscription via LISTEN/NOTIFY, snapshot management)".

**Closing comment from @harakeishi (MEMBER):**
> 方針変更: 別リポジトリで対応
> PostgreSQL参照実装はコアライブラリ（`contract-to-cash/core`）の外部依存ゼロ原則（`go.mod`にulid以外を持ち込まない）を維持するため、同org内の別リポジトリ `contract-to-cash/postgres` で対応します。
> - `database/sql` + PostgreSQLドライバの依存をコアに持ち込まない
> - `go get github.com/contract-to-cash/postgres` で追加利用する形
> - コアの `infrastructure/inmemory/` はテスト・デモ用として残す
> 本issueはクローズし、別リポジトリ側で新issueとして起票します。

Actual outcome: no `contract-to-cash/postgres` repo exists. Work landed in `contract-to-cash/adapters` as issue #1 ("Implement PostgreSQL adapter layer for core repository interfaces") with `adapters/postgres/...` directory layout. Current open PR `adapters#2` (this repo) resolves that issue.

### [contract-to-cash/core#83](https://github.com/contract-to-cash/core/issues/83) — "EventStore.Store インターフェースにストリーム横断の全イベント取得メソッドを追加"

Closed 2026-04-10. Opened because "adaptersリポジトリでProjection Rebuildを実装する際、全ストリームのイベントをglobal position順に取得するメソッドが Store インターフェースにないことが判明". Added `LoadAll(ctx, fromPosition, limit)` to `Store` interface and `GlobalPosition int64` to `Event`. Merged via core#91. **This is directly driven by adapters-repo requirements.**

### [contract-to-cash/core#82](https://github.com/contract-to-cash/core/issues/82) — "PaymentGateway の Void/Cancel メソッドの役割分担を明確化"

Closed. Opened because "adaptersリポジトリでStripe PaymentGatewayを実装する際、VoidとCancelのセマンティクスが曖昧で実装困難であることが判明". Stripe maps both to `PaymentIntents.Cancel`; `Charge`-then-`Void` doesn't work since capture already happened. Resolved by merging core#84 (switched Saga compensation from `Void` to `Refund`).

### [contract-to-cash/core#28](https://github.com/contract-to-cash/core/issues/28) — "Design: Transaction strategy — Closure + Transactional Repository Set with Saga compensation"

Closed 2026-03-29. **This is the definitive design spec for the `application/tx` package** any adapter must satisfy. Essentials:

- `tx.TxManager.RunInTx(ctx, fn func(ctx, Repos) error) error`
- `tx.Repos` is a **struct** (not interface) containing only write-side repositories: `Contracts`, `Invoices`, `Payments`, `Credits` (the body lists these four; CreditNotes were subsequently excluded per adapters#1). Read-only repos (`pricing`, `product`, `usage`) are intentionally outside `Repos`.
- `NoopTxManager` is bundled for in-memory / testing.
- `Saga` with `AddCompensation` (reverse-order) for external gateway calls that cannot be rolled back by DB tx.
- `RetryOnConflict(maxRetries, fn)` — no backoff, default 3, for optimistic lock conflicts.
- `CreditEntry.version` (int) — optimistic locking on concurrent credit consumption.
- `Payment.refundedAmount` + `RecordRefund()` replacing `MarkRefunded` / `MarkPartiallyRefunded`. Single path enforces "total refunded ≤ original amount" invariant.
- `Payment.idempotencyKey` + `Repository.FindByIdempotencyKey`.
- Sample `sqlTxManager` using `database/sql` → `BeginTx` → `repoFactory(sqlTx)` → `fn(ctx, repos)`. **This is the template the PostgreSQL adapter should follow** (adapted to pgx).

### [contract-to-cash/core#27](https://github.com/contract-to-cash/core/issues/27) — "Data consistency risks: missing transaction boundaries and compensation patterns"

Closed 2026-03-29. 11 specific risks (gateway-then-save, credit partial save, refund partial save, double-consume, TOCTOU on duplicate invoice, missing idempotency, no cumulative refund tracking, mutable pointer leakage from inmemory repos, context-thread-safety in plugins, snapshot version drift). Parent of #28. **Adapter implementations must address #1/#2/#5/#6/#7/#9 at the DB level** — unique constraint on `(contractID, period, status)`, `FOR UPDATE` on balance, optimistic version checks, deep copy semantics are not needed when loading from DB.

---

## 3. Recent merged PRs in `contract-to-cash/core` (last 30)

Full list with direction commentary:

### Highly relevant — affect adapter interfaces

| PR | Title | Merged | Notes |
|---|---|---|---|
| [contract-to-cash/core#91](https://github.com/contract-to-cash/core/pull/91) | feat: add LoadAll and GlobalPosition for cross-stream event loading | 2026-04-10 | **Closes #83. Directly required by adapters PR.** Adds `Store.LoadAll(ctx, fromPosition, limit)` — `fromPosition` exclusive, `limit<=0` means unlimited; `Event.GlobalPosition int64` with `omitempty`; `ProjectionService.RebuildAll` uses `LoadAll` with batched pagination and infinite-loop guard. PostgreSQL impl must assign `GlobalPosition` via `BIGSERIAL` in Append. |
| [contract-to-cash/core#92](https://github.com/contract-to-cash/core/pull/92) | fix: move state mutations inside RunInTx in ProcessPayment (#85) | 2026-04-10 | Moves `p.Complete()` and `inv.RecordPayment()` *inside* the `RunInTx` closure so tx failure rolls back in-memory state and Saga compensation actually fires. Idempotency check moved before state mutation. **Important behavioural note for adapter test expectations.** |
| [contract-to-cash/core#90](https://github.com/contract-to-cash/core/pull/90) | feat: add ThreeDSecureResult to ChargeResponse/AuthorizeResponse (#81) | 2026-04-10 | Adds `ThreeDSecure *ThreeDSecureResult` to both response types. Stripe adapter dependency. |
| [contract-to-cash/core#93](https://github.com/contract-to-cash/core/pull/93) | fix: resolve hardcoded PaymentMethodCreditCard in ProcessPayment (#88) | 2026-04-10 | Adds `PaymentMethodType` to `ChargeResponse`; adds `PaymentMethod` to `ProcessPaymentInput`; 3-level fallback chain. Adds `PaymentMethodDebitCard`, `PaymentMethodQRCode`, `PaymentMethodPostpay` constants. Backward-compatible (zero value → credit_card). |
| [contract-to-cash/core#84](https://github.com/contract-to-cash/core/pull/84) | fix: use Refund instead of Void for saga compensation after Charge | 2026-04-09 | Closes #82. Saga `AddCompensation` now calls `gateway.Refund` after `Charge` (since Charge is authorize+capture). Spawned new edge cases tracked in #86/#87. |
| [contract-to-cash/core#80](https://github.com/contract-to-cash/core/pull/80) | docs: consolidate docs/ as single source of truth for Docusaurus | 2026-04-08 | Restructures `docs/` into `concepts/`, `internals/`, `api/`, `examples/`, `guides/`, `decisions/`, `research/`. Net −1,700 lines. **Relevant because it moved the SQL schema location** — it used to be `docs/design/event-sourcing.md` §8 (per core#69), now lives under `docs/internals/` or `docs/concepts/`. |

### Other notable merged PRs (30-day window)

| PR | Title | Merged |
|---|---|---|
| [contract-to-cash/core#78](https://github.com/contract-to-cash/core/pull/78) | test: add benchmark tests for critical paths | 2026-04-07 |
| [contract-to-cash/core#77](https://github.com/contract-to-cash/core/pull/77) | feat: support flexible billing intervals (interval + intervalCount model) | 2026-04-07 |
| [contract-to-cash/core#76](https://github.com/contract-to-cash/core/pull/76) | test: add tests for remaining inmemory repositories | 2026-04-07 |
| [contract-to-cash/core#75](https://github.com/contract-to-cash/core/pull/75) | feat: add validation to BillingConfig and service constructors | 2026-04-07 |
| [contract-to-cash/core#74](https://github.com/contract-to-cash/core/pull/74) | fix: return error from NewInvoice on currency mismatch | 2026-04-07 |
| [contract-to-cash/core#73](https://github.com/contract-to-cash/core/pull/73) | refactor: typed strings for sourceType and metricName | 2026-04-07 |
| [contract-to-cash/core#72](https://github.com/contract-to-cash/core/pull/72) | test: add comprehensive tests for TemporalQueryService | 2026-04-07 |
| [contract-to-cash/core#62](https://github.com/contract-to-cash/core/pull/62) | fix: return DomainError instead of panic for empty items in NewCreditNote | 2026-04-06 |
| [contract-to-cash/core#59](https://github.com/contract-to-cash/core/pull/59) | test: improve coverage for payment, balance, and webhook | 2026-04-06 |
| [contract-to-cash/core#58](https://github.com/contract-to-cash/core/pull/58) | fix: reject negative quantities in NewLineItem with DomainError | 2026-04-06 |
| [contract-to-cash/core#55](https://github.com/contract-to-cash/core/pull/55) | feat: add RegenerateInvoice for void-and-recreate during SuspensionBillingDefer | 2026-04-03 |
| [contract-to-cash/core#54](https://github.com/contract-to-cash/core/pull/54) | feat: add GenerateProrationInvoice to BillingService | 2026-04-03 |
| [contract-to-cash/core#50](https://github.com/contract-to-cash/core/pull/50) | Remove PlanID from codebase | 2026-04-04 |
| [contract-to-cash/core#49](https://github.com/contract-to-cash/core/pull/49) | CLAUDE.mdを追加: アーキテクチャ・設計規約・コーディング規約を集約 | 2026-03-31 |
| [contract-to-cash/core#47](https://github.com/contract-to-cash/core/pull/47) | feat: add FindByAccountID to BalanceRepository | 2026-03-31 |
| [contract-to-cash/core#46](https://github.com/contract-to-cash/core/pull/46) | feat: add Int64() and Float64() convenience methods to Money type | 2026-03-31 |

### Observed direction

- **Write operations are consolidating inside `RunInTx`**: #92 is the clearest example. Adapter implementations must assume all state mutations happen within the tx closure.
- **Event-sourcing surface is expanding** to support read-model rebuild: `LoadAll` + `GlobalPosition` (#91).
- **PaymentGateway contract is being actively extended** for Stripe adapter needs: `ThreeDSecureResult` (#90), `PaymentMethodType` on `ChargeResponse` (#93), Saga compensation semantics (#84).
- **Read-model queries on write-side repositories** (e.g. `FindByAccountID` on BalanceRepository #47, `FindExpiring`/`FindDueForRenewal` on ContractRepository) are growing — adapter impls need dual-access patterns (projection table → aggregate rebuild).

---

## 4. Adapter contribution guides

**There is no `ADAPTERS.md`, `CONTRIBUTING.md`, or `CODE_OF_CONDUCT.md` in `contract-to-cash/core`.** Root files are: `.golangci.yml`, `CLAUDE.md`, `LICENSE`, `Makefile`, `README.md`, `README.ja.md`, `go.mod`, `go.sum`.

Closest guidance:

- **`docs/guides/integration.md`** — End-to-end walkthrough for integrating core into a service. Includes a **sketch of PostgreSQL implementations** for `contract.Repository`, `eventstore.Store` (with optimistic locking via `SELECT MAX(version)`), and the Step 4 wiring. The directory layout it suggests is an application consuming core, not the adapter repo layout — but the patterns are the reference.
- **`docs/decisions/design-decisions.md`** — ADR-style document covering: CQRS choice (simplified CQRS, same DB, sync/async projection opt-in), multitenant handled by consumers (not OSS), UTC-only, projection options, trial status, proration behaviours, suspension behaviours, partial payments, batch processor interface (scheduler is consumer's responsibility), idempotency key TTL (24h default), audit metadata (UserID mandatory, IP/UA optional), plugin calculation order (accounting-compliant: pre-calc → calc → discount → subtotal → tax → total → credit → draft → post-calc), plugins share the same transaction as core, event schema versioning.
- **`docs/guides/custom-plugin.md`** — plugin authoring, not repository authoring.
- **`README.md`** — "Architecture" section describes package layout, including `infrastructure/` ("In-memory implementations (for testing/demos)") — confirms inmemory is *only* for test/demo.

The README explicitly says:
> An event-sourced billing engine with a plugin architecture for SaaS and subscription businesses. Provides domain models, billing pipelines, and extension points — **you bring your own database and payment gateway**.

`CLAUDE.md` in core root (from #49) is another source — it was added to consolidate architecture / design / coding conventions for AI-assisted development.

---

## 5. Milestones / roadmap

- `gh api repos/contract-to-cash/core/milestones` returned `[]` — **no milestones configured**.
- `README.md` does not contain a "Roadmap" section.
- No roadmap file found under root or `docs/`.

Direction must be inferred from issue/PR activity (see TL;DR).

---

## 6. TODO/FIXME code search for persistence

`gh search code` returned `[]` for all of:
- `TODO adapter` in contract-to-cash/core
- `TODO persistence` in contract-to-cash/core
- `TODO postgres` in contract-to-cash/core

No persistence-related TODOs left in the core codebase. All actionable persistence work has been pushed to the adapters repo.

---

## 7. `contract-to-cash` GitHub org repos

`gh api orgs/contract-to-cash/repos`:

| Repo | Description | Updated |
|---|---|---|
| [contract-to-cash/core](https://github.com/contract-to-cash/core) | *(none)* | 2026-04-10 |
| [contract-to-cash/cli](https://github.com/contract-to-cash/cli) | *(none)* | 2026-04-01 |
| [contract-to-cash/adapters](https://github.com/contract-to-cash/adapters) | *(none)* | 2026-04-08 |

**No `postgres`, `adapters-mysql`, `examples`, `stripe`, or similar repo exists.** The closing comment on core#69 proposed `contract-to-cash/postgres`, but the actual decision landed on consolidating under `contract-to-cash/adapters` with subdirectories (`adapters/postgres/`, future `adapters/stripe/`, `adapters/redis/`, `adapters/kafka/`). The directory layout in `adapters#1` confirms this.

`cli` is not yet investigated but is likely the end-user CLI, unrelated to adapter work.

---

## 8. `contract-to-cash/adapters` — existing issues

`gh issue list -R contract-to-cash/adapters --state all`:

### [contract-to-cash/adapters#1](https://github.com/contract-to-cash/adapters/issues/1) — "Implement PostgreSQL adapter layer for core repository interfaces"

OPEN. **This is the issue the current PR resolves.** Complete content summary:

**Target directory structure:**
```
adapters/
├── postgres/
│   ├── conn.go                    # Connection pool + Querier interface
│   ├── eventstore.go              # eventstore.Store implementation, WithTx(pgx.Tx)
│   ├── tx_manager.go              # tx.TxManager, ctx-embedded Tx for nested tx
│   ├── contract_repo.go           # contract.Repository
│   ├── invoice_repo.go            # invoice.Repository
│   ├── creditnote_repo.go         # invoice.CreditNoteRepository (outside Repos)
│   ├── payment_repo.go            # payment.Repository
│   ├── balance_repo.go            # balance.Repository
│   ├── usage_repo.go              # usage.Repository
│   ├── product_repo.go            # product.Repository
│   ├── price_repo.go              # pricing.PriceRepository
│   ├── contract_projector.go      # projection.Projector
│   ├── invoice_projector.go       # projection.Projector
│   ├── projection_checkpoint.go   # Checkpoint mgmt
│   └── migrations/
│       ├── 001_event_store.up.sql
│       ├── 002_snapshots.up.sql
│       ├── 003_read_models.up.sql
│       ├── 004_projection_checkpoints.up.sql
│       └── ...
├── stripe/                        # (future) PaymentGateway, CustomerGateway, WebhookHandler
├── redis/                         # (future) WebhookDeduplicator
└── kafka/                         # (future) event publishing
```

**Interfaces to implement** (from issue):

Domain Repositories:
- `contract.Repository` — Event Sourced, Save via EventStore.Append, FindByID via event replay
- `invoice.Repository` — 10 methods including `FindByIDAsOf` (temporal query)
- `invoice.CreditNoteRepository` — operates outside `TxManager.Repos`
- `payment.Repository` — includes `FindByIdempotencyKey`
- `balance.Repository` — optimistic locking via LoadedVersion pattern
- `usage.Repository` — read-only in billing, idempotent Record
- `pricing.PriceRepository` — master data
- `product.Repository` — master data

Event Store: `eventstore.Store` — 10 methods (Append, Load*, Subscribe, *Snapshot)

Application:
- `tx.TxManager` — must support **nested transactions** (savepoint or ctx propagation). Core comment: "Real DB implementations must support savepoints or reuse the outer transaction." `CreditNoteService.ReissueInvoice` nests RunInTx calls.
- `projection.Projector` — Project + Rebuild

**Key design decisions documented in the issue:**

1. **Querier interface pattern**: shared by `pgxpool.Pool` and `pgx.Tx`:
   ```go
   type Querier interface {
       Query(ctx, sql, args...) (pgx.Rows, error)
       QueryRow(ctx, sql, args...) pgx.Row
       Exec(ctx, sql, args...) (pgconn.CommandTag, error)
   }
   ```

2. **Transaction propagation for nested RunInTx**: embed `pgx.Tx` in context; if `RunInTx` detects existing Tx in ctx, reuse it instead of starting a new one.

3. **EventStore tx sharing**: `ContractRepository.Save` calls `EventStore.Append` — both must share the same DB tx. Solution: `PostgresEventStore.WithTx(pgx.Tx)` returns a tx-scoped EventStore instance.

4. **Optimistic locking**:
   - EventStore: `SELECT MAX(version) FOR UPDATE` + `UNIQUE(stream_id, version)` constraint
   - Balance: `version` column with `WHERE version = $loaded_version` on UPDATE
   - Both return errors compatible with `RetryOnConflict`

5. **Subscribe**: Hybrid LISTEN/NOTIFY + `global_position` polling. `NOTIFY events_inserted` after commit, subscriber catches up from last known position. Slow subscribers dropped (matching InMemory semantics, buffer 100).

6. **Projection checkpoint table** tracks last processed `global_position` per projector.

7. **Repository duality** for read-model-style queries returning write model (`FindExpiring`, `FindDueForRenewal`, `FindByAccountID`): (1) query projection table for IDs, (2) restore full aggregate from EventStore (snapshot + replay).

**Schema excerpts from the issue:**

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

CREATE TABLE snapshots (
    stream_id   TEXT        NOT NULL,
    version     INTEGER     NOT NULL,
    state       JSONB       NOT NULL,
    as_of       TIMESTAMPTZ NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stream_id, version)
);

CREATE TABLE projection_checkpoints (
    projector_name TEXT PRIMARY KEY,
    last_position  BIGINT NOT NULL DEFAULT 0,
    last_updated   TIMESTAMPTZ NOT NULL
);
```

**Implementation order** (from issue):
1. `go.mod` (core + pgx)
2. `postgres/conn.go`
3. `postgres/migrations/`
4. `postgres/eventstore.go` (with WithTx)
5. `postgres/tx_manager.go` (with nested tx)
6. `postgres/contract_repo.go` (most complex)
7. Remaining repositories
8. Projectors + checkpoint

**Dependencies on core (noted in issue body):**
- core#81 — ThreeDSecureResult ✓ merged as core#90
- core#82 — Void/Cancel semantics ✓ merged as core#84
- core#83 — LoadAll method ✓ merged as core#91

All three have been resolved in core since adapters#1 was opened — the PostgreSQL implementation path is unblocked.

**References in issue:**
- `core/infrastructure/inmemory/` — reference patterns
- Core `CLAUDE.md` — design principles (dependency direction, event management, calculation order)
- [ThreeDotsLabs/wild-workouts](https://github.com/ThreeDotsLabs/wild-workouts-go-ddd-example) — Adapters pattern reference
- [AleksK1NG/Go-EventSourcing-CQRS](https://github.com/AleksK1NG/Go-EventSourcing-CQRS) — PostgreSQL EventStore reference

### [contract-to-cash/adapters#2](https://github.com/contract-to-cash/adapters/pull/2) — the current open PR

OPEN. Created 2026-04-08. Title: "Implement PostgreSQL adapter layer for core repository interfaces". Body mirrors the structure in adapters#1 and declares `Resolves #1`. This is the PR under review.

Note: the task brief mentions "the current PR says it resolves #1" — confirmed. There is no adapters#1 PR; issue #1 and PR #2 are paired.

---

## Summary: implications for the current PR

1. **All three core-side blockers have been merged in the last 48 hours** (core#90, core#91, core#93 — 2026-04-10; core#84 — 2026-04-09; core#92 — 2026-04-10 for tx semantics). The PR should be rebased on the latest core tag/main; `go.mod` should reference a recent core SHA/tag post-2026-04-10.
2. **`LoadAll` / `GlobalPosition` are now required** by the `Store` interface — PostgreSQL `eventstore.go` must implement `LoadAll` with BIGSERIAL-backed ordering.
3. **`PaymentMethodType` and `ThreeDSecureResult`** landed on `ChargeResponse` / `AuthorizeResponse` — while this doesn't affect the PostgreSQL adapter directly, if the PR also stubs a Stripe test double or Gateway types, they need the new fields.
4. **Saga tx semantics (core#92)**: adapter tx_manager/test suites should verify that mutations inside the `RunInTx` closure are rolled back on failure — the inmemory tests were recently updated for this.
5. **Nested RunInTx support is non-negotiable** — core's `CreditNoteService.ReissueInvoice` nests, and the inmemory `NoopTxManager` makes this trivially work; pgx tx_manager must support this via ctx propagation or savepoints (adapters#1 prefers ctx propagation).
6. **The `adapters` repo is the single destination for all infrastructure code** — no separate `contract-to-cash/postgres` was created despite core#69's closing comment saying so. Future Stripe/Redis/Kafka adapters will land as siblings of `postgres/`.
7. **There is no formal `CONTRIBUTING.md` or `ADAPTERS.md`** — the review bar is effectively whatever the core maintainers apply at PR time, informed by `docs/guides/integration.md`, `docs/decisions/design-decisions.md`, and core `CLAUDE.md`.
8. **No roadmap or milestones** — priority has been reactive (adapters repo needs drive core PRs).
9. **Next likely adapter work after postgres**: Stripe gateway (three open issues core#81 [now closed]/#86/#87 all relate to Stripe adapter behaviour).

## Direct links

- https://github.com/contract-to-cash/core/issues/27
- https://github.com/contract-to-cash/core/issues/28
- https://github.com/contract-to-cash/core/issues/69
- https://github.com/contract-to-cash/core/issues/82
- https://github.com/contract-to-cash/core/issues/83
- https://github.com/contract-to-cash/core/issues/81
- https://github.com/contract-to-cash/core/issues/86
- https://github.com/contract-to-cash/core/issues/87
- https://github.com/contract-to-cash/core/pull/84
- https://github.com/contract-to-cash/core/pull/90
- https://github.com/contract-to-cash/core/pull/91
- https://github.com/contract-to-cash/core/pull/92
- https://github.com/contract-to-cash/core/pull/93
- https://github.com/contract-to-cash/adapters/issues/1
- https://github.com/contract-to-cash/adapters/pull/2
