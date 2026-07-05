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
stripe/     Stripe payment gateway adapter (port.PaymentGateway / webhooks)
```

## Coverage

`postgres` and `mysql` implement the same set of core interfaces (see the
Testing section for the difference in how each is verified).

| Core interface | postgres | mysql | fincode | stripe |
|---|---|---|---|---|
| `eventstore.Store` | ✅ | ✅ | — | — |
| `contract.Repository` | ✅ | ✅ | — | — |
| `invoice.Repository` | ✅ | ✅ | — | — |
| `invoice.CreditNoteRepository` | ✅ | ✅ | — | — |
| `payment.Repository` | ✅ | ✅ | — | — |
| `balance.Repository` | ✅ | ✅ | — | — |
| `pricing.PriceRepository` | ✅ | ✅ | — | — |
| `product.Repository` | ✅ | ✅ | — | — |
| `usage.Repository` | ✅ | ✅ | — | — |
| `tx.TxManager` | ✅ | ✅ | — | — |
| `projection.Projector` (contract + invoice) | ✅ | ✅ | — | — |
| `port.PaymentGateway` | — | — | ✅ (card / JPY only) | ✅ (card; JPY/USD/EUR) |
| `port.WebhookHandler` | — | — | ✅ | ✅ |

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
  `idempotent_key` header window (the header is still sent as well). On such
  a duplicate rejection the adapter retrieves the existing order and replays
  its outcome (success if already completed, `*PartialAuthorizeError` if
  registered but not executed) — but only when the request's amount and job
  code match the existing order. Reusing the same `IdempotencyKey` for a
  **different amount or job code** is rejected with a non-retryable
  `port.GatewayError` (`processing_error`) instead of silently replaying the
  original amount's outcome (matching Stripe, which refuses idempotency-key
  reuse with changed parameters). Void/Cancel/Refund forward their
  `IdempotencyKey` as the `idempotent_key` header verbatim; Capture, which
  can issue `/change` + `/capture` from one request, forwards a distinct
  per-operation key derived from it for each call.
- **Refunds**: fincode has no refund endpoint; full refunds go through
  `/cancel`, partial refunds through `/change` (amount reduction), and
  `RefundResponse.RefundID` is the payment ID. Partial refunds are a
  read-modify-write without compare-and-set on the fincode side: **serialize
  refund operations against the same payment in your caller** — concurrent
  refunds can silently lose one (last write wins on the new total). Retrying
  a partial refund with the same `IdempotencyKey` is safe only within
  fincode's 30-minute `idempotent_key` TTL; after that the amount reduction
  is applied again (double refund), so **callers must also prevent duplicate
  refund requests via their own refund records**.
- **Two-step recovery**: register/execute is not atomic at the HTTP layer; a
  failure between the steps returns `*fincode.PartialAuthorizeError` and is
  recovered with `CompleteCharge` / `CompleteAuthorize`. Both check the
  payment's current state first, so a lost execute *response* (payment
  actually captured/authorized) is converted into a success instead of a
  failed re-execute.
- **Capture**: a partial capture below the authorized hold is applied via
  `/change` then `/capture`. A capture amount **above** the authorized hold is
  rejected with a `*ValidationError` before any mutating call — fincode's
  `/change` would otherwise silently raise the hold and over-charge (Stripe's
  API rejects over-capture server-side; fincode does not).
- **Configuration / `BaseURL`**: `Config.BaseURL` must be set explicitly
  (`fincode.ProductionBaseURL` or `fincode.SandboxBaseURL`). An empty `BaseURL`
  **no longer** silently defaults to the sandbox; `NewClient` returns an error
  instead, so a forgotten production endpoint fails at construction rather than
  routing every "successful" payment to the test environment unnoticed. To use
  the sandbox, either set `BaseURL: fincode.SandboxBaseURL` or set
  `Config.Sandbox: true` (an explicit opt-in; ignored when `BaseURL` is set).

  > **Migration (breaking change)**: `NewClient(Config)` now returns
  > `(*Client, error)`. Update call sites to handle the error, and set
  > `BaseURL` (or `Sandbox: true`) explicitly — code that relied on the empty
  > `BaseURL` → sandbox default must now opt in via `Sandbox: true`.
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
  name as the type. Two events are deliberately conservative:
  - `payments.card.exec` maps to `payment.succeeded` **only when the payload
    `status` is `CAPTURED`** (one-step charge). An `AUTHORIZED` exec is a
    funds hold, not a completed payment, and is passed through with its raw
    name — the later capture emits `payments.card.capture`, which always maps
    to `payment.succeeded` (no double signal: one-step charges never emit a
    capture event). A missing/unknown `status` is passed through, not guessed.
  - `payments.card.cancel` is **never** mapped to `refund.succeeded`: this
    adapter implements Void (auth reversal, no funds moved), Cancel, and full
    Refund all through the same `/cancel` endpoint, so a cancel event cannot
    distinguish "authorization released" from "money returned". It is passed
    through with its raw name; consumers that need to know whether funds
    moved must retrieve the payment state (e.g. `GetTransaction` or their own
    payment records) and decide from context.

### stripe scope and conventions

Built on the official `github.com/stripe/stripe-go/v82` SDK. Unlike fincode,
Stripe maps almost one-to-one onto `port.PaymentGateway` — every method is
implemented; nothing is rejected as unsupported except by currency.

- **Cards; JPY / USD / EUR.** `SupportedMethods()` reports `credit_card` and
  `debit_card` (Stripe's single `card` PaymentMethod type). Only the
  currencies defined in `domain/shared` are supported; any other currency is
  rejected with a typed `port.GatewayError` (`currency_not_supported`). Extend
  `currencyExponent` in `stripe/types.go` when core adds more currencies.
- **Amounts**: Stripe amounts are integers in the currency's smallest unit
  (cents for USD/EUR, whole yen for zero-decimal JPY). `shared.Money` is
  converted using each currency's exponent; an amount that is not exactly
  representable in minor units (e.g. `100.5` JPY), non-positive, or beyond
  int64 range is rejected with a `*ValidationError` before any network call.
- **IDs**: the port-level `TransactionID` and `AuthorizationID` are both the
  Stripe **PaymentIntent** ID (`pi_...`) — Charge/Authorize create it,
  Capture/Void/Cancel/GetTransaction operate on it by that single ID (no
  separate access token). `RefundResponse.RefundID` is the Stripe refund ID
  (`re_...`); the refund request's `TransactionID` is the PaymentIntent ID.
  Payment method IDs are the flat Stripe PaymentMethod ID (`pm_...`).
- **Charge / Authorize / Capture**: Charge creates a confirmed PaymentIntent
  with automatic capture; Authorize uses manual capture (status
  `requires_capture`) and Capture (optionally partial via `amount_to_capture`)
  settles it. Void and Cancel both cancel the PaymentIntent (Stripe models an
  auth reversal and a cancel the same way), differing only in the recorded
  cancellation reason.
- **3D Secure**: a required authentication surfaces as a **successful**
  response with `TransactionStatusRequiresAction` and a
  `ThreeDSecureResult.RedirectURL` (from the PaymentIntent's
  `next_action.redirect_to_url`), not an error — the caller redirects the
  customer and completes the flow.
- **Payment methods**: `RegisterPaymentMethod` attaches an existing
  Stripe PaymentMethod (created client-side with Stripe.js/Elements and passed
  as `Token`) to the customer, optionally setting it as the customer's default
  invoice payment method. Card storage/vaulting itself is done client-side;
  the adapter never handles raw PANs.
- **Idempotency**: `IdempotencyKey` on Charge/Authorize/Capture/Refund is
  forwarded verbatim as Stripe's `Idempotency-Key` header. **Stripe expires
  idempotency keys after 24h**, so a retry beyond that window is no longer
  deduplicated by Stripe; permanent deduplication (as the core
  `PaymentService` expects) must also be backed by the caller's own payment
  records.
- **Refunds**: full (`Amount == nil`) or partial (`Amount` set). Only
  Stripe's accepted reasons (`duplicate` / `fraudulent` /
  `requested_by_customer`) are forwarded; `RefundReasonOther` (and any other
  value) is sent with no `reason` rather than a value Stripe would reject.
- **Error mapping**: SDK `*stripe.Error`s are converted to
  `*port.GatewayError` with the original error preserved via `RawError`
  (recoverable with `errors.As`). Stripe error codes map to `port.ErrorCode`
  (e.g. `card_declined`, `expired_card`, `insufficient_funds`); rate limits,
  `api_error`s, and 5xx/timeout responses are marked `Retryable`.
- **Webhooks**: `stripe.WebhookHandler` verifies the `Stripe-Signature` header
  via the SDK's `webhook.ConstructEventWithOptions` (HMAC-SHA256 over
  `"{timestamp}.{body}"`, constant-time). Timestamp/replay validation is
  **delegated to the core `port.WebhookProcessor`** (which checks
  `event.CreatedAt` against an injected `shared.Clock` and dedups on
  `event.ID`), because the SDK's own tolerance check reads the wall clock and
  would break this module's clock-injection convention. Known Stripe event
  types map to `port.WebhookEventType` constants; unknown types pass through
  with their raw Stripe name.

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
- **stripe**: `httptest`-based unit tests against a fake Stripe API, with the
  SDK's backend URL pointed at the test server (`Config.APIBase`); no live API
  calls. Webhook tests sign payloads with the same HMAC scheme the SDK verifies.

CI (`.github/workflows/ci.yml`) builds against the `contract-to-cash/core`
version pinned in `go.mod`, resolved from the Go module proxy like any other
dependency. To develop against a local core checkout, add a `replace`
directive to `go.mod` (`go mod edit -replace github.com/contract-to-cash/core=/path/to/core`).

## Migrations

Migration files live in `postgres/migrations/` and `mysql/migrations/`
(001–005) and are applied in filename order. Applied migration files are
immutable: schema corrections ship as new numbered migrations, never as
in-place edits of already-published files.

**005_event_store_index_fixes** corrects two indexes created by 001:

- mysql: drops `idx_events_global_position`, which duplicated the PRIMARY
  KEY on `global_position` exactly (postgres is unaffected: there
  `global_position` is not the primary key, so its index is not redundant).
- postgres + mysql: repoints the snapshots index from `(stream_id, as_of)`
  to `(stream_id, created_at)` to match the `LoadSnapshotBefore` query
  (which filters and orders on `created_at`).

No manual reconciliation is needed — just apply 005. Both variants are
idempotent, so environments that applied the original 001 and environments
that already fixed the indexes by hand converge on the same final schema:
the postgres file uses `DROP INDEX IF EXISTS` / `CREATE INDEX IF NOT
EXISTS`; MySQL 8.0 supports neither, so the mysql file guards each change
with an `information_schema` lookup executed through a prepared statement.

## MySQL schema & connection

Apply the DDL in `mysql/migrations/` (001–005) before use; `mysql/schema.sql`
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
