# adapters

Concrete infrastructure adapters for
[`contract-to-cash/core`](https://github.com/contract-to-cash/core). The core
library follows a **BYO DB / BYO Gateway** model: it defines domain repository
and port interfaces but ships only in-memory reference implementations. This
module provides production-oriented implementations of those interfaces.

## Layout

```
postgres/   PostgreSQL implementation of the full persistence stack
            (event store, all repositories, read-model projectors, tx manager)
mysql/      MySQL 8.0 implementation of the same full persistence stack
fincode/    fincode payment gateway adapter (port.PaymentGateway / webhooks)
```

## Coverage

`postgres` and `mysql` implement the same set of core interfaces (see the
Testing section for the difference in how each is verified).

| Core interface | postgres | mysql | fincode |
|---|---|---|---|
| `eventstore.Store` | ✅ | ✅ | — |
| `contract.Repository` | ✅ | ✅ | — |
| `invoice.Repository` | ✅ | ✅ | — |
| `invoice.CreditNoteRepository` | ✅ | ✅ | — |
| `payment.Repository` | ✅ | ✅ | — |
| `balance.Repository` | ✅ | ✅ | — |
| `pricing.PriceRepository` | ✅ | ✅ | — |
| `product.Repository` | ✅ | ✅ | — |
| `usage.Repository` | ✅ | ✅ | — |
| `tx.TxManager` | ✅ | ✅ | — |
| `projection.Projector` (contract + invoice) | ✅ | ✅ | — |
| `port.PaymentGateway` | — | — | ✅ (card / JPY only) |
| `port.WebhookHandler` | — | — | ✅ |

Event store semantics follow the core reference implementation
(`infrastructure/inmemory/event_store.go`) in both SQL adapters:
`LoadRange` is half-open (`from <= occurred_at < to`), `LoadSnapshotBefore`
cuts off on snapshot `CreatedAt`, and `Append` derives event versions
server-side from `expectedVersion`.

### fincode scope and conventions

- **Credit cards / JPY only.** `SupportedMethods()` reports `credit_card`;
  other payment method types, non-JPY currencies, and the 3D Secure flow are
  rejected with typed `port.GatewayError`s rather than silently ignored.
- **IDs**: the port-level `TransactionID` / `AuthorizationID` is the fincode
  payment (order) ID; the adapter re-fetches `access_id` internally. Payment
  method IDs are composite `"<customer_id>/<card_id>"`.
- **Idempotency**: when `IdempotencyKey` is set on Charge/Authorize, the
  fincode order ID is derived deterministically from the key
  (`"o" + base32(sha256(key))[:29]`, lowercase), so a retry re-registers the
  same order and fincode rejects the duplicate — permanent deduplication as
  required by the core `PaymentService`, not just the 30-minute
  `idempotent_key` header window (the header is still sent as well).
  Capture/Void/Cancel/Refund forward their `IdempotencyKey` as the
  `idempotent_key` header too.
- **Refunds**: fincode has no refund endpoint; full refunds go through
  `/cancel`, partial refunds through `/change` (amount reduction), and
  `RefundResponse.RefundID` is the payment ID. Partial refunds are a
  read-modify-write without compare-and-set on the fincode side: **serialize
  refund operations against the same payment in your caller** — concurrent
  refunds can silently lose one (last write wins on the new total).
- **Two-step recovery**: register/execute is not atomic at the HTTP layer; a
  failure between the steps returns `*fincode.PartialAuthorizeError` and is
  recovered with `CompleteCharge` / `CompleteAuthorize`. Both check the
  payment's current state first, so a lost execute *response* (payment
  actually captured/authorized) is converted into a success instead of a
  failed re-execute.
- **Webhooks**: `fincode.WebhookHandler` verifies a configurable signature
  header (default `"signature"`, constant-time comparison) in one of two
  **explicitly selected** modes — there is **no default**, because fincode's
  signature scheme could not be confirmed from primary sources and this
  adapter refuses to bake in an unverified assumption:
  1. Check in the fincode dashboard / specification whether the signature
     issued for your webhook subscription is a **fixed string sent verbatim
     on every delivery** or an **HMAC computed over each request body**.
  2. Set `WebhookConfig.Mode` accordingly:
     - `SignatureModeStatic` — `Secret` is the fixed signature string;
       the header is compared for equality (constant time). *Caution:* this
       authenticates the sender only; the body is **not** covered by the
       signature, so payload integrity rests entirely on HTTPS.
     - `SignatureModeHMAC` — `Secret` is an HMAC key; the header must equal
       base64(HMAC-SHA256(secret, raw body)), which authenticates sender
       *and* body.
  `NewWebhookHandler` returns an error when `Mode` is unset or unknown.
  Known fincode card events map to `port.WebhookEventType` constants;
  unknown event names are verified and passed through with their raw fincode
  name as the type.

## Testing

The interface coverage of `postgres` and `mysql` is identical, but the
verification level differs:

- **postgres**: integration tests against a real PostgreSQL (schema,
  constraints, transactions, and concurrency behavior are exercised
  end-to-end). Without a reachable database the suite self-skips; setting
  `ADAPTERS_TEST_DSN` makes an unreachable database a hard failure (used in
  CI, which runs a `postgres:16` service container).
- **mysql**: unit tests with [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock)
  so SQL, argument binding, transaction boundaries, and error mapping are verified
  deterministically without a running database (`go test ./mysql/... -race`) —
  but no real MySQL server is exercised. Integration tests against a real
  MySQL (docker / testcontainers) are a recommended follow-up.
- **fincode**: `httptest`-based unit tests against a fake fincode server
  (no live API calls).

CI (`.github/workflows/ci.yml`) checks out `contract-to-cash/core` best-effort
and injects a local `replace`; building this module standalone requires the
core repository to be available (it is not published to the Go module proxy).

## Migrations

Migration files live in `postgres/migrations/` and `mysql/migrations/`
(001–004) and are applied in filename order.

**Pre-release history rewrite (001):** this adapter collection is unreleased
(no tags; the module did not even build before the current series of fixes),
so instead of stacking corrective 005+ migrations, `001_event_store.up.sql`
was edited in place:

- mysql: dropped `idx_events_global_position`, which duplicated the PRIMARY
  KEY on `global_position` exactly.
- postgres + mysql: the snapshots index was repointed from
  `(stream_id, as_of)` to `(stream_id, created_at)` to match the
  `LoadSnapshotBefore` query (which filters and orders on `created_at`).

If you already applied an older 001 to an environment, reconcile manually:

```sql
-- mysql only: remove the index that duplicates the PRIMARY KEY
ALTER TABLE events DROP INDEX idx_events_global_position;

-- postgres
DROP INDEX IF EXISTS idx_snapshots_stream_asof;
CREATE INDEX idx_snapshots_stream_created ON snapshots (stream_id, created_at DESC);

-- mysql
ALTER TABLE snapshots DROP INDEX idx_snapshots_stream_asof;
ALTER TABLE snapshots ADD KEY idx_snapshots_stream_created (stream_id, created_at);
```

Once this module is tagged, migration files become immutable and schema
changes will only ship as new numbered migrations.

## MySQL schema & connection

Apply the DDL in `mysql/migrations/` (001–004) before use; `mysql/schema.sql`
contains the standalone event-store tables. Configure the MySQL DSN with
`loc=UTC` (and typically `parseTime=true`), e.g.
`user:pass@tcp(host:3306)/db?loc=UTC&parseTime=true`. All timestamps are stored
and returned in UTC; `DATETIME` columns are scanned correctly under either
`parseTime` setting.

### MySQL vs PostgreSQL translation notes

- Optimistic concurrency: `UNIQUE (stream_id, version)` (event store) and a
  version-guarded `UPDATE ... WHERE version = ?` (balance) — same as postgres.
- No deferrable constraints: the contract read-model `Rebuild` wraps its reload
  in `SET SESSION foreign_key_checks = 0/1` (postgres uses `SET CONSTRAINTS
  ALL DEFERRED`).
- No `LISTEN/NOTIFY`: `Subscribe` tails new events by polling `LoadAll`
  (replay-then-tail, honours `fromPosition`, back-pressures slow consumers).
- Partial indexes are emulated as full indexes; `JSONB`→`JSON`,
  `BIGSERIAL`→`AUTO_INCREMENT`, `NOW()`→`NOW(6)`.
