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
fincode/    fincode payment gateway adapter
            (port.PaymentGateway / port.CustomerGateway / webhooks)
stripe/     Stripe payment gateway adapter
            (port.PaymentGateway / port.CustomerGateway / webhooks)
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
| `port.PaymentGateway` | — | — | ✅ (card / JPY only) | ✅ (card JPY/USD/EUR; konbini + JP bank transfer + PayPay, JPY / charge-only) |
| `port.CustomerGateway` | — | — | ✅ | ✅ |
| `port.WebhookHandler` | — | — | ✅ | ✅ |

Event store semantics follow the core reference implementation
(`infrastructure/inmemory/event_store.go`) in both SQL adapters:
`LoadRange` is half-open (`from <= occurred_at < to`), `LoadSnapshotBefore`
cuts off on snapshot `CreatedAt`, and `Append` validates that each event's
caller-stamped `Version` is contiguous with `expectedVersion` (i.e. event `i`
must equal `expectedVersion+i+1`), rejecting the whole batch with a
`shared.DomainError` (code `validation_error`) on a gap, stale version, or
out-of-order value instead of silently renumbering it. One divergence remains:
both SQL adapters treat an empty event batch as a no-op and return `nil`
immediately, without performing the `expectedVersion` optimistic-lock check,
whereas the in-memory reference checks `currentVersion != expectedVersion`
before ever looking at the events slice and returns `version_conflict` even
for an empty batch. This is pre-existing behavior (pinned by mysql's
`TestEventStore_Append_EmptyIsNoop`), not something this change alters.

`usage.Repository.Record` also follows the core reference
(`infrastructure/inmemory/usage_repository.go`): recording a `UsageRecord`
whose `idempotency_key` collides with an existing row returns a
`shared.DomainError` with code `duplicate_request` (not a silent success).
Both SQL adapters translate the unique-key violation
(Postgres `usage_records_idempotency_key_key` / MySQL 1062 on the
`idempotency_key` index) into that sentinel, so behaviour is identical to what
integrators observe when testing against the in-memory repository. A record
with an empty `idempotency_key` is never deduplicated (inserted
unconditionally). Metering ingest pipelines that prefer silent at-least-once
dedup can `errors.As` the `*shared.DomainError`, check for
`ErrCodeDuplicateRequest`, and drop it; a caller that needs the duplicate
signal cannot recover it from a silent no-op, which is why the adapters surface
it by default (issue #38). A duplicate PRIMARY KEY (`id`) is a distinct,
non-idempotent fault and always surfaces as a wrapped error.

`balance.Repository.FindExpired` (added for core#159's
`batch.BalanceExpirationProcessor`) also follows the core reference
(`infrastructure/inmemory/balance_repository.go`): it returns entries whose
expiry has passed as of `asOf` — `BalanceEntry.IsExpired(asOf)`, i.e. `asOf`
strictly after `expires_at` — whose remaining amount is still non-zero (expired
credit not yet forfeited by `MarkExpired`), ordered by `created_at` ascending
across all accounts/currencies. Entries without an expiry and fully-consumed
entries are excluded. Consistent with `FindAvailable` (issue #11), the
fully-consumed check runs in Go on the **precise** remaining amount from the
`state` JSON, not the lossy `remaining_amount` BIGINT column, so a sub-unit
remainder still counts as forfeitable credit.

### fincode scope and conventions

- **Credit cards / JPY only.** `SupportedMethods()` reports `credit_card`;
  other payment method types, non-JPY currencies, and the 3D Secure flow are
  rejected with typed `port.GatewayError`s rather than silently ignored.
- **IDs**: the port-level `TransactionID` / `AuthorizationID` is the fincode
  payment (order) ID; the adapter re-fetches `access_id` internally. Payment
  method IDs are composite `"<customer_id>/<card_id>"`.
- **Customers**: the same `Gateway` implements `port.CustomerGateway`. Unlike
  Stripe, fincode lets the caller choose the customer ID, so
  `CreateCustomerRequest.InternalID` is sent as the fincode customer `id`
  directly (no metadata round-trip; the returned `Customer.ID` *is* the
  InternalID). fincode customers have no metadata or description fields:
  non-empty `Metadata` / `Description` is rejected with a `*ValidationError`
  rather than silently dropped. `Address.Country` maps to fincode's
  `addr_country`, which is ISO 3166-1 **numeric** (e.g. `"392"` for Japan) and
  is passed through unmodified. `Phone` maps to `phone_no`; `phone_cc` is never
  set. `Customer.DefaultPaymentMethodID` is resolved via a second `ListCards`
  call when the customer response reports `card_registration == "1"` (the
  composite ID of the card with `default_flag == "1"`, nil if none); missing
  customers surface as `port.GatewayError` with code `customer_not_found`.
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
- **Error mapping**: `*fincode.HTTPError`s are converted to `*port.GatewayError`
  with the original error preserved via `RawError` (recoverable with
  `errors.As`), and the fincode `error_code` is carried on `GatewayError.DeclineCode`.
  Status-derived codes are classified as before: 429 → `rate_limit_exceeded`,
  408/504 → `gateway_timeout`, 5xx → `gateway_unavailable` (all retryable),
  transport failures → retryable `gateway_unavailable`. **Card-company
  authorization declines** — fincode's `E9993*` family (オーソリエラー) — are
  classified as `card_declined`, so `port.ErrorCode`-based policy (dunning
  classification, user-facing messaging) behaves the same as with the Stripe
  adapter for the dominant decline case, instead of collapsing every business
  rejection into `processing_error`. **Asymmetry with Stripe**: unlike Stripe,
  fincode does **not** expose a granular decline reason (insufficient funds vs.
  expired card vs. invalid card vs. CVC) in `error_code` — the whole
  authorization-decline class shares the `E9993` family and the reason lives
  only in the dashboard. This adapter therefore maps that family to the
  `card_declined` umbrella and deliberately does **not** synthesise the finer
  `port.ErrorCode` categories fincode does not report; the raw fincode code
  remains on `DeclineCode` for callers that need it. Any `error_code` that is
  not grounded in fincode's [error documentation](https://docs.fincode.jp/develop_support/error)
  keeps the non-retryable `processing_error` fallback rather than being guessed.
  (The 3D Secure flow is rejected up front — see below — so no
  `authentication_required` mapping is needed.)
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
Stripe maps almost one-to-one onto `port.PaymentGateway` — every interface
method is implemented. The same `Gateway` also implements
`port.CustomerGateway`
(`CreateCustomer` / `GetCustomer` / `UpdateCustomer` / `DeleteCustomer`).

- **Payment methods.** `SupportedMethods()` reports `credit_card` /
  `debit_card` (Stripe's single `card` PaymentMethod type; JPY / USD / EUR),
  two **async-settling, JPY-only** methods — `convenience_store` (Stripe
  `konbini`) and `bank_transfer` (Stripe `customer_balance` configured for
  `jp_bank_transfer` funding) — plus `qr_code` (Stripe `paypay`, a JPY-only
  **redirect-approval** method). Only the currencies defined in
  `domain/shared` are supported; any other currency is rejected with a typed
  `port.GatewayError` (`currency_not_supported`). Extend `currencyExponent`
  in `stripe/types.go` when core adds more currencies.
- **Async settlement (konbini / JP bank transfer)**: `Charge` looks up the
  PaymentMethod's type first (one extra API call per charge — a bare `pm_...`
  ID does not reveal its type), then names it explicitly in
  `payment_method_types` (with
  `payment_method_options[customer_balance][funding_type]=bank_transfer` +
  `[bank_transfer][type]=jp_bank_transfer` for bank transfers) instead of the
  card-pinned `automatic_payment_methods` path. The confirm returns
  `TransactionStatusRequiresAction` with the hosted **konbini voucher URL** /
  **bank-transfer instructions URL** in `ThreeDSecureResult.RedirectURL` — the
  same requires-action channel the 3DS redirect flow uses — and the payment
  settles later via webhook (`payment_intent.succeeded`;
  `payment_intent.processing` while funds are in flight).
  `ChargeResponse.PaymentMethodType` reports the real method type. Caveats:
  both methods are **charge-only** (`Authorize` rejects them with
  `method_not_supported` — Stripe has no manual capture for them, so nothing
  is silently mis-charged); `customer_balance` requires a Stripe customer
  (`ChargeRequest.CustomerID`); konbini requires the PaymentMethod to carry
  billing details (email/name) when it is created client-side; refunds of
  async charges settle asynchronously too (see Webhooks below). Everything
  else — Capture, Void, per-method 3DS — remains card-only.
- **PayPay (`qr_code`)**: a JPY-only redirect-approval method — the customer
  approves the payment in the PayPay app/web and is redirected back. `Charge`
  pins `payment_method_types=["paypay"]` and **requires a return URL**,
  sourced from `ThreeDSecureRequest.ReturnURL` (the port's only return-URL
  carrier, shared with the card 3DS flow); without one the charge is rejected
  client-side with a `ValidationError` rather than creating an unconfirmable
  intent. The approval URL surfaces from `next_action.redirect_to_url`
  through the same `ThreeDSecureResult.RedirectURL` channel, and
  `ChargeResponse.PaymentMethodType` reports `qr_code`. PayPay is
  **charge-only** (`Authorize` rejects it — stripe-go v82 exposes no
  manual-capture surface for it, so the adapter is conservative) and
  **single-use per charge**: `RegisterPaymentMethod` rejects `qr_code`
  (Stripe's off-session / reusable PayPay is out of scope for now — future
  work). Note stripe-go v82.5.1 does not enumerate `paypay`; the adapter
  sends the type as a raw string, which the string-driven Stripe API and the
  SDK's string-typed fields handle fine.
- **Amounts**: Stripe amounts are integers in the currency's smallest unit
  (cents for USD/EUR, whole yen for zero-decimal JPY). `shared.Money` is
  converted using each currency's exponent; an amount that is not exactly
  representable in minor units (e.g. `100.5` JPY), non-positive, or beyond
  int64 range is rejected with a `*ValidationError` before any network call.
  On the response side, a Stripe amount reported in a currency outside
  `currencyExponent` is rejected with a `port.GatewayError`
  (`currency_not_supported`) rather than decoded at a guessed zero exponent —
  guessing whole units would misreport a two-decimal amount by 100x.
- **IDs**: the port-level `TransactionID` and `AuthorizationID` are both the
  Stripe **PaymentIntent** ID (`pi_...`) — Charge/Authorize create it,
  Capture/Void/Cancel/GetTransaction operate on it by that single ID (no
  separate access token). `RefundResponse.RefundID` is the Stripe refund ID
  (`re_...`); the refund request's `TransactionID` is the PaymentIntent ID.
  Payment method IDs are the flat Stripe PaymentMethod ID (`pm_...`).
  `GetTransaction` on a pending (`requires_action`) intent also surfaces the
  outstanding customer-action URL — 3DS redirect, konbini voucher,
  bank-transfer instructions, or PayPay approval — on
  `Transaction.ThreeDSecure.RedirectURL`, the same channel the original
  `ChargeResponse` used, so a pending charge can be read back later without
  losing its action URL.
- **Charge / Authorize / Capture**: Charge creates a confirmed PaymentIntent
  with automatic capture; Authorize uses manual capture (status
  `requires_capture`) and Capture (optionally partial via `amount_to_capture`)
  settles it. Void and Cancel both cancel the PaymentIntent (Stripe models an
  auth reversal and a cancel the same way), differing only in the recorded
  cancellation reason.
- **3D Secure**: `ThreeDSecureRequest.Required = true` forces a challenge by
  sending `payment_method_options[card][request_three_d_secure] = "any"` (so a
  caller demanding strong authentication is never charged without it);
  `ReturnURL` is forwarded as the PaymentIntent's `return_url`. A required
  authentication then surfaces as a **successful** response with
  `TransactionStatusRequiresAction` and a `ThreeDSecureResult.RedirectURL`
  (from the PaymentIntent's `next_action.redirect_to_url`), not an error — the
  caller redirects the customer and completes the flow. 3DS is card-only: on
  the konbini / customer_balance paths the `ThreeDSecure` request is ignored
  (no `return_url` is involved) and the same result field instead carries the
  hosted voucher / instructions URL from
  `next_action.konbini_display_details.hosted_voucher_url` /
  `next_action.display_bank_transfer_instructions.hosted_instructions_url`.
- **Customers**: Stripe only accepts its own customer IDs (`cus_...`), so a
  charge that references an unknown customer fails with `No such customer` —
  internal account IDs (e.g. core `AccountID` ULIDs) must never be passed as
  `ChargeRequest.CustomerID`. The intended wiring is: **create the customer
  before charging** — on account registration call `CreateCustomer` (your own
  account ID goes in `CreateCustomerRequest.InternalID` and is stored in the
  Stripe customer's metadata under `internal_id`), persist the returned
  `cus_...` ID on the account, and pass that ID as
  `ChargeRequest.CustomerID` / `AuthorizeRequest.CustomerID` and to the
  payment-method APIs. A get/update/delete of a customer Stripe does not know
  (or a get of a deleted customer — Stripe returns a deleted stub, not a 404)
  surfaces as a `port.GatewayError` with code `customer_not_found`.
- **Payment method storage**: `RegisterPaymentMethod` attaches an existing
  Stripe PaymentMethod (created client-side with Stripe.js/Elements and passed
  as `Token`) to the customer, optionally setting it as the customer's default
  invoice payment method. Card storage/vaulting itself is done client-side;
  the adapter never handles raw PANs. `RegisterPaymentMethodRequest.Type` is
  honored: cards (or an unspecified `Type`, for backward compatibility)
  attach; `convenience_store` / `bank_transfer` / `qr_code` are rejected with
  `method_not_supported` — konbini, customer_balance and paypay
  PaymentMethods are single-use in this adapter, so a fresh one is created
  client-side per charge — as is any other type this adapter does not
  implement. `ListPaymentMethods`
  returns **all** attached methods (no card filter); each entry's `Type` is
  mapped from the Stripe type (`card` → credit/debit by funding, `konbini` →
  `convenience_store`, `customer_balance` → `bank_transfer`,
  `us_bank_account` → `direct_debit` with `BankAccount` details populated,
  `paypay` → `qr_code` with `QRCode.Provider` set to `paypay` — the SDK
  exposes no further PayPay details), and unrecognized Stripe types degrade
  gracefully to a typed pass-through of their raw name rather than
  masquerading as cards.
- **Idempotency**: `IdempotencyKey` on Charge/Authorize/Capture/Refund is
  forwarded verbatim as Stripe's `Idempotency-Key` header. **Stripe expires
  idempotency keys after ~24h**, so a retry beyond that window is no longer
  deduplicated by Stripe and can double-charge. Unlike the fincode adapter
  (which derives a deterministic order ID and so dedups permanently), this
  adapter only satisfies the core `PaymentService`'s "gateway deduplicates on
  `IdempotencyKey`" contract within that 24h window. For durable
  deduplication — e.g. a payment retried days later by a dunning batch — pair
  the gateway with the core `port.IdempotencyStore` (recommended), so the
  recorded outcome, not Stripe's window, gates the replay.
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
  with their raw Stripe name. The PaymentIntent lifecycle maps to
  `payment.succeeded` / `payment.failed` / `payment.pending`
  (`payment_intent.processing`). `payment_intent.requires_action` is
  classified from the intent's payment method (its `payment_method_types` /
  expanded `payment_method.type`): only konbini / customer_balance become
  `payment_instruction.created` — the "voucher / wire instructions issued"
  signal, with `payment_intent.succeeded` arriving as the settlement webhook
  hours-to-days later — while a card 3DS challenge, a PayPay redirect
  approval (the same nature as 3DS), or an uninspectable payload passes
  through with its raw Stripe name, since an authentication/approval prompt
  is not a payment instruction. Refund-object
  events (`refund.created` / `refund.updated` / `charge.refund.updated`) are
  classified from the refund's `status` (`succeeded` → `refund.succeeded`,
  `failed`/`canceled` → `refund.failed`, anything else passes through
  unclassified), because refunds are asynchronous and can arrive as `pending`
  first. `charge.refunded` gets the **same status inspection** — it is
  classified from the newest refund embedded in the Charge's `refunds` list —
  instead of mapping unconditionally to `refund.succeeded`, so a pending
  refund on an async method is never booked as completed; on API versions
  2022-11-15+ (which no longer embed `charge.refunds` by default) the status
  cannot be inspected and the event passes through with its raw name — rely
  on the Refund-object events for classified outcomes there.

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
  deterministically without a running database (`go test ./mysql/... -race`),
  **plus** integration tests against a real MySQL 8 (migrations, event-store
  append/load/version-conflict + duplicate-event-id semantics, projector rebuild,
  and repository round-trips). Like postgres, the integration suite self-skips
  without a reachable database; setting `ADAPTERS_TEST_MYSQL_DSN` makes an
  unreachable database a hard failure (used in CI, which runs a `mysql:8` service
  container). The integration DSN needs `parseTime=true&loc=UTC`;
  `multiStatements` is **not** required — `mysql.Migrate` splits each migration
  file into single statements itself (`mysql/migrate.go`, `splitStatements`) and
  executes them one at a time, so it never issues a multi-statement query.
- **fincode**: `httptest`-based unit tests against a fake fincode server
  (no live API calls).
- **stripe**: `httptest`-based unit tests against a fake Stripe API, with the
  SDK's backend URL pointed at the test server (`Config.APIBase`); no live API
  calls. Webhook tests sign payloads with the same HMAC scheme the SDK verifies.

CI (`.github/workflows/ci.yml`) builds against the `contract-to-cash/core`
version pinned in `go.mod`, resolved from the Go module proxy like any other
dependency. The pin is now core **v0.7.0**.

Round 4 (core v0.7.0):

- **dependency bump only**: core `v0.4.0` → `v0.7.0`. No adapter code changes
  were required — the intervening core releases are additive (Minor). Notable
  new core surface now available to adapters:
  - `PaymentInstructions` on `ChargeResponse` (core 0.7.0): async / push charges
    (konbini vouchers, bank-transfer virtual accounts) can return customer-facing
    instructions (url / reference / expiry) as a first-class field instead of
    smuggling a URL through `ThreeDSecureResult`.
  - `OnCompensationExecutedHook` (core#257): non-fatal observability hook fired
    after `ProcessPayment` saga charge-reversal.

Round 3 (core v0.4.0):

- **zero-interval `one_time` contracts (core#218)**: a `one_time` contract may
  now `Activate` with an unset (zero-value) `CurrentPeriod`. The event's
  `current_period` field still marshals as
  `{"start":"0001-01-01T00:00:00Z","end":"0001-01-01T00:00:00Z"}` (never
  null/omitted — see core `domain/shared/datetime.go` `DateRange.MarshalJSON`),
  and the projector's `parseTime` previously turned that year-0001 timestamp
  into a non-nil pointer, so `contract_read_models.renewal_date`/`end_date`
  ended up set to `0001-01-01` — which made `FindDueForRenewal` wrongly return
  a one_time contract that has no recurring billing at all. **Fixed** in both
  `postgres/contract_projector.go` and `mysql/contract_projector.go`:
  `parseTime` now returns `nil` when the parsed time `.IsZero()`, so the
  `COALESCE` in the `contract.activated` handler leaves `renewal_date`/
  `end_date` `NULL` for a zero-interval activation (verified by
  `TestContractRepo_FindDueForRenewal_ExcludesZeroIntervalOneTime` /
  `TestContractProjector_Project_Activated_ZeroPeriod_LeavesRenewalDateNull`,
  with regression coverage for the normal-subscription case alongside them).
- **`pricing.NewOneTimePrice` round-trip (core#218)**: the corresponding Price
  constructor produces a zero `Interval()`, empty `BillingCycle()`, and nil
  `PricingModel()`. This already round-trips through the existing `prices`
  schema with no migration needed (`interval_data` JSONB/JSON stores JSON
  `null`, `billing_cycle` keeps its `''` default, `pricing_model`'s
  `{"kind":""}` envelope deserializes back to `nil`) — locked in by
  `TestPriceRepo_SaveAndFindByID_OneTimePrice` (postgres) and
  `TestPriceRepo_Save_OneTimePrice_NullIntervalData` /
  `TestPriceRepo_FindByID_OneTimePrice_ReconstructsZeroInterval` (mysql).
- **transactional outbox writer ports (core#248)** and **new contract
  lifecycle hooks (core#227)** are integrator (platform) concerns: `core`
  added `port.PaymentOutboxWriter`/`port.InvoiceOutboxWriter` and new hook
  interfaces that the *integrator* implements and fires (or, for the outbox
  ports, wires via `WithPaymentOutboxWriter`/`WithInvoiceOutboxWriter` on
  core's own services). Neither touches a repository/port signature this
  adapter implements, so **no adapter code change was required**.

Round 2 (core#196 / core#197, merge `6e621a1`):

- **batch finder `limit` parameter (core#197)**: `contract.Repository.FindDueForRenewal`
  / `FindTrialsEndingBefore` and `balance.Repository.FindExpired` gained a
  trailing `limit int` argument. A positive limit returns at most that many rows,
  **oldest-eligible first** (`ORDER BY renewal_date` / `trial_end_date` /
  `created_at` ASC, with `id` as a stable tiebreaker) so repeated batch runs drain
  the backlog deterministically; `limit <= 0` means unbounded. Both postgres and
  mysql repos translate a positive limit to a `LIMIT ?`/`LIMIT $n` clause and add
  the `ORDER BY` that makes "oldest first" genuine.
- **`pricing.NewPrice`/`NewPriceWithInterval` now return `(*Price, error)`
  (core#196)**: they validate negative amount, amount/declared-currency mismatch,
  zero interval, and unknown billing cycle (strict). **No adapter code changes
  were required**: the price repos reconstruct persisted rows through the
  adapter-sanctioned `pricing.FromSnapshot` (the `ToSnapshot`/`FromSnapshot`
  danger-zone API), not the validating constructor. `FromSnapshot` deliberately
  bypasses business-rule re-validation (it only rejects an empty ID), which is the
  correct load-path behaviour: a persisted Price is immutable and was validated at
  its original construction, so reconstruction should be faithful rather than
  re-run rules against historical rows. `FromSnapshot`'s error is already wrapped
  as a scan/reconstruct error in `scanPriceRow`/`scanPriceRows`.
- **`FindByID` not-found convention (core#197)**: the contract/invoice/payment/
  credit-note/product repos already return a `shared.DomainError` with
  `ErrCodeNotFound` on a missing row (never `(nil, nil)`), so no change was needed;
  verified against the newly documented interface contracts.

Round 1 (`82c7cfb`, `v0.1.1-0.20260711062854-82c7cfb3dd7b`):

- **payment optimistic locking (core#190)**: `payment.Payment` now carries
  `Version()`/`LoadedVersion()`/`SetVersion()` and `payment.Repository.Save` must
  reject a version-conflicting write with `tx.ErrVersionConflict`. Both payment
  repos add a `lock_version` column (postgres migration 013 / mysql migration
  012) and a version-guarded upsert, mirroring the invoice/balance repos.
- **`balance.Repository.FindRefundsByInvoice` (core#184)**: new method plus
  `BalanceRefund.InvoiceID`/`ApplicationID` fields for idempotent void
  restoration. Both balance repos add the two columns (postgres migration 014 /
  mysql migration 013), persist them in `SaveRefund`, and implement the lookup.
- **`projection.CheckpointStore` port (core#192)**: the existing
  `CheckpointStore` adapters now carry a compile-time `var _
  projection.CheckpointStore` assertion.
- **webhook transport-replay protection (core#191)**: `WebhookHandler.ParseAndVerify`
  now owns replay defense. The stripe handler enforces the signed `Stripe-Signature`
  `t=` timestamp against a clock-injected 5-minute tolerance
  (`WebhookConfig.Tolerance`, `WithWebhookClock`). fincode exposes no HMAC-signed
  transport timestamp, so it cannot implement this control — see the gap note in
  `fincode/webhook.go`.

To develop against a local core checkout, add a `replace` directive to
`go.mod` (`go mod edit -replace github.com/contract-to-cash/core=/path/to/core`).

## Concurrency guarantees

core's `BillingService.FinalizeInvoice` documents that the loser of a concurrent
finalize "reads the finalized row and is rejected with `invalid_state_transition`",
so `OnInvoiceIssuedHook` fires at most once per invoice. That guarantee only holds
if the read-then-write is serialized by the storage layer — it does **not** hold
under READ COMMITTED / REPEATABLE READ with an unconditional last-writer-wins
upsert. This adapter supplies the missing serialization: **when `FindByID` runs
inside an ambient transaction (detected via `QuerierFromContext`), it issues
`SELECT ... FOR UPDATE`** (both postgres and mysql). core's `FinalizeInvoice`
does its read → `Finalize()` → `Save` inside one `tx.Run`, so two concurrent
finalizers contend on the same row: the winner finalizes and commits; the loser
blocks on the row lock, then reads the already-finalized row and is rejected by
`Finalize()` with `invalid_state_transition`, never reaching `Save`. The same row
lock also serializes the `invoice_history` upsert, so the losing writer bails out
before it can overwrite the winner's history row. Outside a transaction (pooled
reads) no lock is taken, so ordinary lookups are unaffected. This addresses
adapters issue #12 (and is cross-referenced by core#130, which pins the wording of
the guarantee core relies on).

**Invoice optimistic locking (issue #30 / core#130 / core#147).** core's
`invoice.Repository.Save` godoc lets an adapter satisfy the concurrency contract by
either (1) optimistic locking or (2) read serialization. The `FOR UPDATE` read
above is option 2; the adapter now **also** implements option 1, for parity with
`balance_entries` and to give lock-averse deployments a path that does not hold a
row lock. Both guards are in place at once (belt and suspenders) — the core godoc
permits either or both. Each `invoices` row carries a `lock_version` column
(migration postgres 011 / mysql 010) holding the Invoice's domain optimistic-lock
`Version()`, which core#147 bumps on **every** mutating method. `Save` writes the
new `Version()` only when the stored `lock_version` still equals the entity's
`LoadedVersion()`; a mismatch means a concurrent writer already advanced the row,
and `Save` returns an error matching `errors.Is(err, tx.ErrVersionConflict)`
(so `FinalizeInvoice` retries the loser, which re-reads the finalized row and is
rejected by `Finalize()`). `FindByID` / list finders restore `lock_version` onto
both `Version()` and `LoadedVersion()` (the column is authoritative; the value
persisted on the next `Save` is the entity's `Version()`), and it round-trips
through `InvoiceSnapshot.Version` for `FindByIDAsOf`. A dedicated `lock_version`
column is used rather than overloading the pre-existing `version` column: `version`
is the per-**save** counter that keys the bitemporal `invoice_history` rows, whereas
the optimistic-lock version counts state **mutations** — two saves at the same
loaded version must fork history (new `version`) yet not both win the lock.
Backfill is seamless: existing rows default `lock_version` to 0, the first load
adopts 0 as the baseline, and the first guarded write advances cleanly (a draft
legitimately persisted at version 0 is distinguished from a brand-new insert by row
existence, never by the version value).

**Invoice `Save` is atomic even without an ambient transaction (issue #36).** The
writes it performs — the `invoices` upsert (a `lock_version`-guarded upsert on
postgres; a guarded `UPDATE` then conditional `INSERT` on mysql, which has no
`WHERE` on `ON DUPLICATE KEY UPDATE`), closing the prior `invoice_history` row, and
inserting the new history row — are wrapped in a Save-local transaction when no
ambient transaction is present (mirroring the event store's `Append`). This
prevents a crash or connection loss mid-sequence from leaving `invoice_history`
with a permanently-open stale row or a missing version, which would make
`FindByIDAsOf` (temporal queries) return the wrong state. When core calls `Save`
inside its own `tx.Run` the repository joins that transaction instead of nesting.

**Per-period invoice uniqueness (issue #45 / core#149).** core's
`invoice.Repository.Save` godoc also requires that two DISTINCT non-voided,
non-proration invoices cannot exist for the same `(contract_id, billing_period)`.
core re-checks inside the generation `tx.Run`, but a check-then-insert of a NEW
invoice has no prior version to conflict on, so under READ COMMITTED two concurrent
`GenerateInvoice(contractID, samePeriod)` calls can both insert. Both adapters add
the storage-layer backstop, and translate a violation into a `shared.DomainError`
with code `ErrCodeConflict` and the same message the inmemory reference uses
(`"invoice already exists for this billing period"`):

- **postgres** (migration 012): a partial unique index
  `ux_invoice_period ON invoices (contract_id, billing_period_from, billing_period_to)
  WHERE status <> 'voided' AND billing_period_from IS NOT NULL AND
  coalesce(metadata->>'invoice_type','') <> 'proration'`. `Save` matches the 23505
  on that index name.
- **mysql** (migration 011): a **STORED generated column** `period_uniq_key` that is
  `NULL` for every exempt row (voided, proration, or zero-period) and
  `CONCAT(contract_id,'|',billing_period_from,'|',billing_period_to)` otherwise, with
  a `UNIQUE` index over it (a unique index permits multiple `NULL`s). `Save` matches
  the 1062 on that index name (`dupEntryOnKey`).

VOIDED, PRORATION (`metadata.invoice_type = "proration"`), and ZERO-PERIOD invoices
are EXEMPT, so `RegenerateInvoice` (void-and-recreate the same period), proration
adjustments that coexist with the period invoice, and period-less invoices all keep
working; a `regeneration` replacement is a regular period invoice and participates.

**`Payment` and `CreditNote` `Save` remain last-writer-wins** and take no row
lock. core only pins the concurrency contract for the invoice finalize path (the
`OnInvoiceIssuedHook` at-most-once guarantee above); it makes no equivalent
promise for payments or credit notes, so the adapter deliberately does not add
`SELECT ... FOR UPDATE` there. The practical asymmetry to be aware of: two
concurrent credit-note issues can both win their writes and cause core's
`OnCreditNoteIssuedHook` to fire more than once (see core#147 / #151), and
metrics hooks are documented as at-least-once — dedupe by entity ID in those
hooks rather than relying on the storage layer. If your workload needs
serialization here, apply the same ambient-tx `FOR UPDATE` pattern in a fork.

**Contract creation is idempotent at the storage layer (issue #46 / core#159).**
`ContractAggregate.Create` requires a non-empty `CreateContractCommand.
IdempotencyKey` and carries it on the `contract.created` event (SchemaVersion 3),
but the core can only validate **presence** — it has no cross-aggregate view, so
two retried `Create` calls with the same key would otherwise produce two distinct
contracts. `contract.Repository.Save`'s godoc makes the **uniqueness** the
adapter's contract, so both SQL adapters add a unique constraint over the
created event's idempotency key and translate a violation into a
`shared.DomainError` with code `ErrCodeConflict` (the retried caller looks up the
existing contract instead of failing opaquely — mirroring the payments #35
pattern). The constraint lives on the **event store** (not the read model), so it
is enforced synchronously and atomically with the event append, regardless of
projection mode:

- **postgres** (migration 010): a partial unique **expression** index,
  `ux_contract_idempotency_key ON events ((data->>'idempotency_key')) WHERE type
  = 'contract.created' AND coalesce(data->>'idempotency_key','') <> ''`. `Append`
  matches the 23505 violation on that index name (like `isVersionConflict`).
- **mysql** (migration 009): a **STORED generated column**
  `contract_idempotency_key` that is `NULL` for every row that must not be
  constrained (non-`contract.created` events, and `contract.created` events with
  an empty/absent key), with a `UNIQUE` index over it — a unique index permits
  multiple `NULL`s. `Append` matches the 1062 duplicate-key on that index name
  (`dupEntryOnKey`).

Historical pre-#159 events omit the key (`json:",omitempty"`) and are therefore
**exempt** (NULL / `coalesce(...,'') = ''` is never constrained), so replay of
legacy history never trips the constraint — the same NULL/empty-exemption pattern
as the payments and usage idempotency indexes. This is orthogonal to the
optimistic-concurrency (`stream_id, version`) conflict, which continues to map to
`ErrCodeVersionConflict`.

## Migrations

Migration files live in `postgres/migrations/` and `mysql/migrations/`
and are applied in filename order. Applied migration files are
immutable: schema corrections ship as new numbered migrations, never as
in-place edits of already-published files.

### Running migrations

Each SQL adapter ships a runner that applies the embedded files and tracks
progress in a `schema_migrations` table (filename + status), so a file is
applied exactly once and a re-run is a no-op:

```go
// postgres
if err := postgres.Migrate(ctx, pool); err != nil { ... } // pool is *pgxpool.Pool

// mysql
if err := mysql.Migrate(ctx, db); err != nil { ... }      // db is *sql.DB
```

- **postgres** wraps each file (DDL + bookkeeping row) in one transaction, so a
  failed file rolls back cleanly and is retried on the next run — PostgreSQL DDL
  is transactional.
- **mysql** cannot roll DDL back (it auto-commits), so the runner writes a
  `pending` marker before a file and flips it to `applied` only after every
  statement succeeds. A file that fails midway leaves a `pending` row; the next
  `Migrate` call **stops with an error** naming that file so a half-applied
  schema is reconciled by hand rather than silently skipped. Files are executed
  one statement at a time on a single pinned connection (so session state such
  as the `PREPARE`/`EXECUTE` guards in 005/008 survives).

The runner does **not** swallow "already exists" errors. To bring an existing,
untracked database under management, seed `schema_migrations` with the files it
has already had applied (`INSERT INTO schema_migrations (filename, status) ...`).
You may still use `golang-migrate` or apply the `.sql` files by hand instead;
the runner is a convenience, not a requirement.

**008_drop_projection_fks** removes the `invoices` / `credit_notes` /
`usage_records` → `contract_read_models` foreign keys added by 003. Those keys
pointed write-side tables at a **projection** table, so under the asynchronous
projection mode core officially supports, a lagging projector could reject a
legitimate write for a contract whose read model had not been built yet. See
[Read-model foreign keys and projection mode](#read-model-foreign-keys-and-projection-mode).

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

**009_drop_dead_balance_index** (postgres only) drops the partial index
`idx_balance_entries_available` that 004 created with `WHERE remaining_amount
> 0`. Issue #11 reworked `FindAvailable` to decide availability in Go on the
precise remaining amount from the state JSON, so the query no longer carries a
`remaining_amount > 0` predicate; a PostgreSQL partial index is unusable to the
planner unless the query implies its predicate, so the index was pure write
amplification (the plain `idx_balance_entries_account_currency` from 003 already
serves the scan). There is **no mysql 009**: MySQL has no partial indexes, so
its 004 already created a plain composite index over `(account_id, currency,
created_at)` that the planner still uses for the same lookup — it is not dead.
`DROP INDEX IF EXISTS` makes the migration idempotent. The runner is
forward-only (no `.down` files); the file's header documents the exact
`CREATE INDEX` to restore it by hand.

**017_payments_gateway_transaction_id_index** (postgres; mysql 016 is the
mirror) indexes `payments.gateway_transaction_id` for the webhook settlement
lookup (issue #72). The postgres runner wraps each file in a transaction and
`CREATE INDEX CONCURRENTLY` cannot run inside one, so this is a plain
(locking) index build: writes to `payments` block while it runs. On
deployments where `payments` is large, run
`CREATE INDEX CONCURRENTLY idx_payments_gateway_transaction_id ON payments
(gateway_transaction_id) WHERE gateway_transaction_id <> '';` by hand **before**
applying the migration — its `IF NOT EXISTS` guard then makes the migration a
fast no-op. The mysql 016 build is generally less disruptive: MySQL 8's InnoDB
`CREATE INDEX` is online DDL (typically `ALGORITHM=INPLACE`), which normally
permits concurrent writes during the build, though brief locks around the
start/end phases still apply.

## MySQL schema & connection

Apply the DDL in `mysql/migrations/` before use; `mysql/schema.sql`
contains the standalone event-store tables. Configure the MySQL DSN with
`loc=UTC` (and typically `parseTime=true`), e.g.
`user:pass@tcp(host:3306)/db?loc=UTC&parseTime=true`. All timestamps are stored
and returned in UTC; `DATETIME` columns are scanned correctly under either
`parseTime` setting.

### MySQL vs PostgreSQL translation notes

- Optimistic concurrency: `UNIQUE (stream_id, version)` (event store) and a
  version-guarded `UPDATE ... WHERE version = ?` (balance) — same as postgres.
- Constraint juggling during `Rebuild`: none needed in either dialect. Migration
  008 (issue #29) dropped the write-side → `contract_read_models` foreign keys, so
  the contract read-model `Rebuild` no longer disables/defers constraints while it
  empties and repopulates the projection — the former mysql `SET SESSION
  foreign_key_checks = 0/1` and postgres `SET CONSTRAINTS ALL DEFERRED` were both
  removed (`mysql/contract_projector.go`, `postgres/contract_projector.go`).
- No `LISTEN/NOTIFY`: `Subscribe` tails new events by polling `LoadAll`
  (replay-then-tail, honours `fromPosition`, back-pressures slow consumers).
  Poll failures (e.g. a DB outage) are reported via
  `mysql.WithSubscriptionErrorHandler`; the loop keeps polling and resumes
  delivery once the database recovers.
- Partial indexes are emulated as full indexes; `JSONB`→`JSON`,
  `BIGSERIAL`→`AUTO_INCREMENT`, `NOW()`→`NOW(6)`.

## Operational semantics

### `global_position` ordering and gap-free tailing (issue #60)

`global_position` is a `BIGSERIAL` / `AUTO_INCREMENT` column: the value is
**assigned when a row is INSERTed** but only becomes **visible when the inserting
transaction COMMITs**. Left unserialized those two moments can reorder: if
transaction A grabs position `N` and transaction B grabs `N+1`, and B commits
first, a reader polling `WHERE global_position > last_seen` could observe `N+1`,
advance its cursor past it, and then **never see `N`** once A finally commits.
That is permanent, silent event loss for everything that tails by position —
`EventStore.Subscribe` (both adapters) and any position-based projector driven
off `LoadAll` / a `CheckpointStore`.

**`Append` closes this gap by serializing appends** so that commit order always
equals `global_position` order:

- **postgres** takes `pg_advisory_xact_lock` (a fixed per-store key) at the start
  of the append transaction — no schema change.
- **mysql** takes an exclusive row lock on a single `event_append_lock` row via
  `SELECT ... FOR UPDATE` (migration `017_event_append_lock`), since MySQL has no
  transaction-scoped advisory lock.

Both locks are held until the transaction ends, so the *assign-position →ᅠcommit*
windows of concurrent appends never overlap: the holder assigns the lower
positions **and** commits them before any other append assigns a position.
A reader that has advanced past position `N` can therefore never later be shown a
lower, as-yet-uncommitted position. In the ambient-transaction case the lock is
held until the caller commits, serializing appends against that wider unit too.

Consequences and guidance:

- **Position-based subscription/projection is now gap-free** — lossless up to DB
  durability. Delivery is still **at-least-once** (a retried/idempotent append or
  a re-subscribe can redeliver a position), so keep position-based consumers
  idempotent / dedup by event ID.
- **Trade-off — global append serialization.** This is a *hard serialization*: a
  single lock orders **all** appends, so even appends to *different* streams wait
  behind each other and total write throughput is bounded by how fast append
  transactions commit. For high-frequency append workloads this cost is not
  negligible. It was chosen deliberately over the reader-side alternative
  (leave appends concurrent and gate the read cursor on the oldest in-flight
  transaction, e.g. `pg_snapshot_xmin` / a `txid` low-water mark), which pushes
  complexity and a correctness burden onto every consumer; the write-side lock is
  the most robust and keeps consumers trivial. Keep append transactions short so
  the lock is not held across unrelated work; an ambient transaction that calls
  `Append` holds the lock until *it* commits.
- **Deadlock surface (ambient transactions).** Because the append lock is held to
  the caller's commit, a caller that also takes other row locks around `Append`
  can deadlock against another transaction that appends first and then touches the
  same rows inside the append transaction — notably core's pattern of running a
  **synchronous projection or an outbox writer inside the append transaction**.
  Lock order crosses (`R → L` vs `L → R`) and the DB detects it, returning a
  retryable error (postgres `40P01` / mysql `1213`) — it is **not** silent, but
  callers combining ambient row locks with `Append` should keep the append
  transaction short and be prepared to retry on deadlock.
- **Synchronous projection remains available** for consumers that want the read
  model updated atomically with the write — project inside the append
  transaction via the adapter `TxManager`. It is no longer *required* to avoid
  position gaps, only a stronger-consistency option.
- Aggregate rehydration (per-stream, version-ordered via `Load`) never depended
  on global-position visibility and is unaffected.

`Subscribe` additionally distinguishes a broken subscription from a clean
shutdown in both adapters. The core `eventstore.Store` interface returns only a
channel, which cannot signal failure: a broken subscription either closes the
channel (postgres with a reconnect bound via `WithSubscriptionMaxReconnects`)
or leaves it silently open while retrying (mysql, and postgres by default) —
so register a callback to observe failures:
postgres reports every failed connection cycle (acquire / LISTEN / catch-up /
notification-wait, including each reconnect attempt during an outage); mysql
reports each failed `LoadAll` poll (at the `WithPollInterval` cadence during an
outage, no debounce):

```go
es := postgres.NewEventStore(pool,
    postgres.WithSubscriptionErrorHandler(func(err error) { log.Printf("subscribe: %v", err) }),
    postgres.WithCatchUpBatchSize(1000), // bound replay/tail page size
)

mes := mysql.New(db, clock,
    mysql.WithSubscriptionErrorHandler(func(err error) { log.Printf("subscribe: %v", err) }),
)
```

Context cancellation is treated as a normal shutdown and is **not** reported by
either adapter.

### Read-model foreign keys and projection mode

Historically `invoices`, `credit_notes`, and `usage_records` had a foreign key
onto `contract_read_models` (a projection table). Migration
**008_drop_projection_fks** removes them, because under **asynchronous**
projection the read model lags the event log: a contract created and immediately
invoiced in one request could fail the FK simply because its read model had not
been projected yet. Referential integrity for contracts lives in the event log
(the source of truth), not in a derived read model.

If you have **not** applied 008 and run projections asynchronously, a write can
fail with a foreign-key violation whenever the projector is behind. Either apply
008 (recommended) or run projections **synchronously** (project in the append
transaction) so the read-model row always exists before dependent rows are
written.

### Known limitation: contract list queries are N+1

The contract-repository fan-out queries (`FindDueForRenewal`, `FindExpiring`,
`FindTrialsEndingBefore`) resolve a batch of contract IDs from the read model and
then rehydrate each aggregate with a per-ID `FindByID` — one snapshot query plus
one event-load query apiece, i.e. ~2N+1 round trips for N contracts. This is
acceptable for interactive lookups but shows up on large renewal/expiration
batches. A future optimization can collapse most of it by batch-loading events
with `WHERE stream_id = ANY($1)` (postgres) / `WHERE stream_id IN (?, …)`
(mysql) and grouping in memory; it is deferred rather than shipped here because
doing it correctly in both dialects (snapshot + post-snapshot event windows,
per-stream ordering) is a larger change than this housekeeping pass warrants.
Until then, keep batch sizes bounded on very large renewal runs.

## Releasing

This module is versioned **separately** from
[`contract-to-cash/core`](https://github.com/contract-to-cash/core) and follows
[Semantic Versioning](https://semver.org/). Releases are **driven by
[CHANGELOG.md](CHANGELOG.md)** — the same convention `core` uses.

To cut a release:

1. Add a new `## [x.y.z] - YYYY-MM-DD` section to `CHANGELOG.md` (move items out
   of `## [Unreleased]`), and update the compare/tag links at the bottom.
2. Merge that change to `main`.
3. The [`Release` workflow](.github/workflows/release.yml) reads the newest
   `## [x.y.z]` heading, and if that tag does not yet exist it:
   - builds, lints (`gofmt` + `golangci-lint` + `go vet`), and runs the **full
     unit + PostgreSQL/MySQL integration suite** — a broken version is never
     published;
   - creates an annotated `vx.y.z` tag; and
   - publishes a GitHub Release whose notes are extracted from that CHANGELOG
     section.

The workflow is **idempotent**: if the newest CHANGELOG version already has a
tag (e.g. the merge touched `CHANGELOG.md` without cutting a new version), it is
a no-op. You can also run it manually from the Actions tab
(`workflow_dispatch`), optionally passing an explicit tag and/or marking the
release as a prerelease.

Pin compatible tags of `adapters` and `core` together — check `go.mod` for the
core version a given release targets.

## License

[contract-to-cash Adapters License](LICENSE) — MIT-style permissive terms with a
**commercial-use attribution** requirement: any commercial product or service
that uses these adapters must clearly and conspicuously acknowledge that it uses
`contract-to-cash/adapters` (for example, in its documentation or an
About/Credits/Third-Party Notices screen). Non-commercial use — internal
evaluation, research, education, and personal projects not provided for
commercial gain — carries no such requirement. See [LICENSE](LICENSE) for the
exact terms.
