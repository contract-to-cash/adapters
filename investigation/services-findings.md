# contract-to-cash/core: services & repositories investigation

Scope: `application/service/*`, `application/projection/service.go`, `application/query/temporal_query_service.go`, `application/port/gateway.go`, `batch/contract_renewal.go`, `batch/processor.go`, `examples/billing-demo/main.go`. Supporting files: `application/tx/tx.go`, `domain/{invoice,payment}/{entity,repository}.go`, `domain/invoice/credit_note_repository.go`, `eventstore/{store,event}.go`.

All line numbers are from `main` at the time of investigation.

---

## 1. Repository method usage patterns

### 1.1 BillingService (`application/service/billing_service.go`)

Dependencies (lines 60–73):

```
contractRepo  contract.Repository
invoiceRepo   invoice.Repository
usageRepo     usage.Repository
balanceRepo   balance.Repository     // optional via WithBalanceRepo
priceRepo     pricing.PriceRepository
productRepo   product.Repository
txManager     tx.TxManager
```

Methods called:

| Call site | Method | Purpose |
|---|---|---|
| `GenerateInvoice` L143 | `contractRepo.FindByID` | Load contract aggregate |
| `RegenerateInvoice` L202 | `contractRepo.FindByID` | Load aggregate |
| `RegenerateInvoice` L226 | `invoiceRepo.FindByContractAndPeriod` | Check voided+dup |
| `GenerateProrationInvoice` L294 | `contractRepo.FindByID` | Load aggregate |
| `checkDuplicateInvoice` L719 | `invoiceRepo.FindByContractAndStatus` (Draft branch) | Dup check |
| `checkDuplicateInvoice` L730 | `invoiceRepo.FindByContractID` (OneTime branch) | Dup check |
| `checkDuplicateInvoice` L743 | `invoiceRepo.FindByContractAndPeriod` (default branch) | Dup check |
| `calculateSubtotal` L541 & `executeBillingPipeline` L383 | `priceRepo.FindByID` | Load price |
| `calculateUsageCharge` L582 | `productRepo.FindByID` | Load product (usage metrics) |
| `calculateUsageCharge` L602 | `usageRepo.GetSummary(contractID, metricName, period)` | Usage totals |
| `applyBalances` L642 | `balanceRepo.FindAvailable(accountID, currency)` | FIFO credit list |
| `applyBalances` L703 | `balanceRepo.Save(entry)` | Persist mutated BalanceEntry |
| `applyBalances` L706 | `balanceRepo.SaveApplication(application)` | Persist BalanceApplication audit row |
| `executeBillingPipeline` L508 | `repos.Invoices.Save` (inside `RunInTx`) | Persist new Invoice |

**Does it expect `Invoice.Save` to persist the full entity state?** Yes. The closure creates the Invoice with status=`Draft`, line items, subtotals, discount, tax, applied balance, amount due, issue date, due date, payment method, metadata, revision links — then calls `repos.Invoices.Save(txCtx, inv)` exactly once per flow (L508). No subsequent mutations and no "update status" verb; Save must be a full upsert of every field that `invoice.Invoice` exposes (see §2).

### 1.2 CreditNoteService (`application/service/credit_note_service.go`)

Dependencies (lines 41–49): `invoiceRepo`, `creditNoteRepo`, `billingSvc` (optional), `txManager`.

| Method | Repo calls |
|---|---|
| `CreateCreditNote` (L89) | `invoiceRepo.FindByID` (L96); `creditNoteRepo.Save` (L148) — no tx |
| `IssueCreditNote` (L156) | `creditNoteRepo.FindByID` (L157); `cn.Issue(now)`; `creditNoteRepo.Save` (L166) — no tx |
| `ApplyCreditNote` (L185) | `FindByID`; `cn.Apply(creditAmount)`; `Save` — no tx |
| `RefundCreditNote` (L203) | `FindByID`; `cn.Refund(refundAmount)`; `Save` — no tx |
| `ReissueInvoice` (L225) | see §7 |

**Does it ever load a CreditNote and mutate it?** Yes — `IssueCreditNote`, `ApplyCreditNote`, `RefundCreditNote` all load an existing CreditNote, invoke a domain state-transition method (`Issue/Apply/Refund`), and save. The CreditNote’s prior state (status, items, amounts, memo, timestamps) must round-trip through the repository. These methods are **not wrapped in `RunInTx`** — a single `Save` per call, no transaction.

### 1.3 PaymentService (`application/service/payment_service.go`)

Dependencies (lines 74–85): `gateway`, `paymentRepo`, `invoiceRepo`, `contractRepo`, `customerGateway`, `eventStore` (unused at runtime inside this file — no direct event store calls found), `registry`, `txManager`.

`ProcessPayment` (L125) flow:
1. L127 `invoiceRepo.FindByID`
2. L139 `inv.ValidatePayment(amount)` (read-only)
3. L147 `ResolvePaymentMethod` → may call `contractRepo.FindByID` (L437) and `customerGateway.GetCustomer`
4. L169 `gateway.Charge` (outside tx)
5. On failure: `paymentRepo.Save(failedPayment)` best-effort (L193)
6. On `RequiresAction`: `paymentRepo.FindByIdempotencyKey` (L219) then `paymentRepo.Save(pendingPayment)` (L239) — outside tx
7. On success: `s.txManager.RunInTx` (L294) with:
   - `repos.Payments.FindByIdempotencyKey` (L298)
   - `p.Complete()` (L308) — mutates in-memory payment
   - `inv.RecordPayment(amount, now)` (L311) — mutates in-memory invoice
   - `repos.Payments.Save(p)` (L315)
   - `repos.Invoices.Save(inv)` (L318)
8. After commit: AfterCharge hooks (L341)

`Refund` (L355):
1. L357 `paymentRepo.FindByID`
2. L368 `gateway.Refund` (outside tx)
3. L378 computes `remaining = p.Amount().Subtract(p.RefundedAmount())` — **critically depends on the reloaded Payment having the correct cumulative `refundedAmount` from storage**
4. L386 `RunInTx`: `p.RecordRefund(refundAmount)` then `repos.Payments.Save(p)`
5. L404 Post-commit: `invoiceRepo.FindByID(p.InvoiceID())` (for hooks only)

**Does `payment_service` rely on Payment being reloaded with intact status/refundedAmount?** **YES**, explicitly. In `Refund` (L378), `p.Amount().Subtract(p.RefundedAmount())` presupposes that `refundedAmount` persisted across the reload. `RecordRefund` (payment entity L145) itself validates cumulative refunds against `p.amount`. The adapter **must** persist and restore `status`, `refundedAmount`, `failureReason`, `gatewayTransactionID`, `idempotencyKey`, `method`, `processedAt`, `amount`, `invoiceID` verbatim.

Does it ever create fresh Payments only? No — refund paths re-load.

### 1.4 SnapshotService (`application/service/snapshot_service.go`)

Works against `eventstore.AggregateRoot` generically (L44), not specifically `ContractAggregate`. It:
- Checks `ShouldCreateSnapshot(version)` (L37) — fires every `interval` events (default 100).
- `CreateSnapshot` serializes via `SnapshotMarshaler` if implemented, else `json.Marshal`.
- Calls `eventStore.SaveSnapshot` (L64).

Nothing in `application/service/` or `batch/` or `examples/billing-demo/` invokes `SnapshotService` — it is an opt-in helper, not automatically triggered by repositories. The repository for contracts is the only place where snapshots would plausibly be written, but `snapshot_service.go` itself does not hard-code Contract — any aggregate that implements `eventstore.AggregateRoot` is supported. See §6.

---

## 2. Entity state that must survive round-trip (non-ES entities)

### Invoice (`domain/invoice/entity.go`)

The Invoice struct holds (L85–L117):

```
id, invoiceNumber, accountID, contractID,
lineItems[]                 (id, description, quantity, unitPrice, amount, taxRate, priceID, metadata)
subtotal, taxAmount, discountAmount, total,
appliedBalance, amountDue, paidAmount, balance,
status, billingPeriod, issueDate, dueDate, paidAt,
paymentMethodID, metadata, allowPartialPay,
originalInvoiceID, revisionOf, voidReason
```

All of the above are read by services after a `FindByID` reload. Specifically:
- `billing_service.executeBillingPipeline` reads nothing back — it creates and saves in one flow.
- `billing_service.RegenerateInvoice` (L226, L240–L253) reads `inv.Status()`, `inv.OriginalInvoiceID()`, `inv.ID()` after loading via `FindByContractAndPeriod`.
- `credit_note_service.CreateCreditNote` (L101, L123) reads `inv.Status()` and `inv.Total()`; `ReissueInvoice` (L240–L242, L262) reads `OriginalInvoiceID()`, `ContractID()`, `BillingPeriod()` on the reloaded original.
- `payment_service.ProcessPayment` (L134, L139, L311) reads `inv.AmountDue()`, calls `ValidatePayment` (which reads status, amountDue, paidAmount), and calls `RecordPayment` which mutates `paidAmount`, `balance`, `paidAt`, `status`.
- `payment_service.ResolvePaymentMethod` reads `inv.PaymentMethodID()`, `inv.ContractID()`, `inv.AccountID()`.

Conclusion: **every field of Invoice must be restorable**, in particular `status`, `paidAmount`, `balance`, `amountDue`, `appliedBalance`, `paidAt`, `voidReason`, `revisionOf`, `originalInvoiceID`, `paymentMethodID`, `billingPeriod`, `lineItems[]` (with `priceID` and `metadata`), `metadata`, `allowPartialPay`. None of these is derivable from other fields once the invoice is finalized/mutated.

### Payment (`domain/payment/entity.go`)

```
id, invoiceID, amount, refundedAmount,
method, status, gatewayTransactionID, idempotencyKey,
failureReason, processedAt, metadata
```

Services require `status`, `refundedAmount`, `amount`, `invoiceID`, `gatewayTransactionID`, `idempotencyKey` (for `FindByIdempotencyKey` dedup), `method`, `processedAt`. `RecordRefund` (entity L145) guard-checks `refundedAmount.Add(amount) <= amount`, so `refundedAmount` loss = data corruption.

### CreditNote (`domain/invoice/credit_note.go` — not fully read, but clear from service)

The service calls `cn.Issue(now)`, `cn.Apply(amount)`, `cn.Refund(amount)` on reloaded credit notes. Fields implied: id, invoiceID, accountID, contractID, reason, items[], issuedAt/createdAt, memo, status, amounts (applied/refunded).

### BalanceEntry / BalanceApplication (balance package)

`billing_service.applyBalances` iterates `FindAvailable`, calls `entry.IsExpired`, `entry.IsFullyConsumed`, `entry.Consume(remaining)`, then saves mutated entry AND a fresh `BalanceApplication` (L703–L708). The entry’s "how much consumed" state **must** round-trip — otherwise FIFO credit application double-spends.

### UsageRecord (usage package)

Only read via `usageRepo.GetSummary(contractID, metricName, period) → {TotalUsage}` (L602). BillingService never writes usage. An adapter needs enough raw data to compute the sum on demand.

### Product

Read-only: `productRepo.FindByID` → `prod.UsageMetrics()` (L601). Each metric has `Name` and `IncludedQuantity` fields.

### Price

Read-only: `priceRepo.FindByID` → `price.Amount()`, `price.ProductID()`, `price.PricingModel()`, `price.ID()`, `price.Interval()`. Accessed in billing_service (L541, L582) and contract_renewal (L241).

---

## 3. Transaction usage patterns

`tx.TxManager` interface (`application/tx/tx.go` L32–L37):

```go
type TxManager interface {
    RunInTx(ctx context.Context, fn func(ctx context.Context, repos Repos) error) error
}
```

`tx.Repos` (L25–L30) holds **only** write repos: `Contracts`, `Invoices`, `Payments`, `Balances`. No credit-note repo, no price/product/usage — reads happen outside the transaction.

Observed patterns:

| Site | Pattern |
|---|---|
| `billing_service.executeBillingPipeline` L448 | Load read-only data outside tx (contract, price, product, usage), then single `RunInTx` that does: FIFO balance consume+save, build Invoice, run AfterCalculation hooks, save Invoice. |
| `credit_note_service.ReissueInvoice` L249 | Loads original Invoice outside tx, then opens `RunInTx`: voids original → save → **calls `s.billingSvc.GenerateInvoice(txCtx, …)`** which itself calls `RunInTx` → the closure sets revision links → saves replacement. |
| `payment_service.ProcessPayment` L294 | Loads Invoice outside tx, calls gateway outside tx, then `RunInTx`: idempotency lookup → `p.Complete` → `inv.RecordPayment` → save Payment → save Invoice. |
| `payment_service.Refund` L386 | Loads Payment outside tx, calls gateway outside tx, then `RunInTx`: `p.RecordRefund` → save Payment. |
| `batch/contract_renewal.processOne` L173 | Loads contract via `FindDueForRenewal` outside tx, mutates via `agg.RenewWithInterval`, then `RunInTx`: `repos.Contracts.Save(agg)`. |

Common rules:
1. **Loads usually happen outside the transaction**, mutations are applied in memory, then writes happen inside the transaction.
2. **Nested `RunInTx` is used**: `ReissueInvoice` wraps a `RunInTx` around a call to `BillingService.GenerateInvoice` which itself calls `RunInTx`. The comment at L245–247 is explicit:

   > ```
   > // Note: BillingService.GenerateInvoice uses its own RunInTx internally.
   > // With NoopTxManager this nests transparently. Real DB implementations
   > // must support savepoints or reuse the outer transaction.
   > ```

   This confirms nested `RunInTx` is a real production requirement; the PG adapter must either reuse the outer tx via `ctx` or use savepoints.
3. **Repo methods used inside the tx that must be tx-scoped**: `Invoices.Save`, `Payments.Save`, `Payments.FindByIdempotencyKey`, `Contracts.Save`, `Balances.FindAvailable`, `Balances.Save`, `Balances.SaveApplication`. Note `SaveApplication` is used on `tx.Repos.Balances` — the adapter’s balance repo interface must expose it.
4. **`CreditNoteRepository` is NOT in `tx.Repos`**. So `CreditNoteService` mutating methods operate outside the tx boundary. For `ReissueInvoice`, the credit note repo is not touched — only invoice repo via `tx.Repos` is used (see L256, L271). Good: PG adapter does not need to put credit notes into `tx.Repos`.

---

## 4. Projection triggers

`application/projection/service.go`:

- `ProjectionService.Start(ctx)` (L57) subscribes via `eventStore.Subscribe(ctx, 0)` and dispatches to projectors on each received event. This is **async** relative to the writes — the caller runs `Start` in its own goroutine.
- `RebuildAll` (L99) uses `LoadAll(fromPosition, batchSize)` and advances by `GlobalPosition`. If `GlobalPosition` does not advance, it errors out:
  > `return fmt.Errorf("LoadAll returned events but global position did not advance (stuck at %d)", fromPosition)` (L123)

So the PG adapter **must** assign monotonically increasing `GlobalPosition` values in `Append` and return them via `LoadAll`/`Subscribe`.

Projection updates are NOT triggered inside the write transaction. They are decoupled via the `Subscribe` stream. There is no "inside tx" projection write.

---

## 5. Event publication

Nothing in any service calls `eventStore.Append` directly. Events are published via `contract.Repository.Save(agg)` (the contract is an event-sourced aggregate; save = append new events). Subscribers observe via `eventStore.Subscribe`.

`ProjectionService.Start` (L57–L83) and `TemporalQueryService` (query package) are the only consumers. The contract adapter’s `Save` must call `eventStore.Append`, which must then make events available on `Subscribe` channels. Whether delivery is synchronous from Append to subscriber or batched via polling is an adapter choice, but `projection.ProcessEvent` is invoked per-event and expects to see each append.

`payment_service` takes an `eventStore` dependency (L95) but does not use it at runtime within this file. (It may be reserved for future use or a test injection point; it never appears in any Append/Load/Subscribe call here.)

---

## 6. Snapshot creation

- `SnapshotService` is generic over `eventstore.AggregateRoot`. Not ContractAggregate-specific.
- Default interval: 100 events (L13).
- **No service in `application/service/`, no batch processor, and no `examples/billing-demo/main.go` invokes `SnapshotService.CreateSnapshot`.** It is a passive helper used by repositories or external orchestration, not triggered by business flows.
- `TemporalQueryService.GetContractAsOf` (query L42–L77) consumes snapshots via `eventStore.LoadSnapshotBefore` + replay-from-snapshot.

Implication for the adapter: the PG event store must support `SaveSnapshot` / `LoadSnapshot` / `LoadSnapshotBefore`, but it does not need to *drive* snapshot creation itself. Snapshots can be deferred to whoever owns the repo’s Save path (the contract repo, typically).

---

## 7. CreditNote reissue flow (`ReissueInvoice`)

`credit_note_service.ReissueInvoice` L225–L294:

1. Pre-flight: requires `billingSvc != nil` (L227).
2. `invoiceRepo.FindByID(originalInvoiceID)` — **outside** tx.
3. Compute `rootID` — outside tx: if `original.OriginalInvoiceID() != nil` use it, else use `originalInvoiceID`.
4. `s.txManager.RunInTx` opens the outer transaction:
   - `original.VoidWithReason(reason)` — in-memory mutation.
   - `repos.Invoices.Save(txCtx, original)` — inside tx.
   - `replacement, err = s.billingSvc.GenerateInvoice(txCtx, original.ContractID(), original.BillingPeriod())` — **this call itself opens a nested `RunInTx` via the BillingService's own TxManager** (L448 of billing_service).
   - `replacement.SetRevisionOf(originalInvoiceID)` — in-memory.
   - `replacement.SetOriginalInvoiceID(rootID)` — in-memory.
   - `repos.Invoices.Save(txCtx, replacement)` — inside outer tx (second save of replacement, first save was inside the nested tx).
5. After commit: `OnInvoiceRevised` hooks.

**Does `CreditNoteRepository` need to be inside `tx.Repos`?** **No.** The code only touches `repos.Invoices` here. Credit note is never saved inside this flow — it is a separate prior step (`CreateCreditNote`). So the current PG adapter does not need to extend `tx.Repos` with a CreditNote field.

**What the adapter does need**: nested `RunInTx` support. The inner `BillingService` uses its own `txManager`; if that is a separate `TxManager` instance bound to a separate PG connection, the outer tx will not see the inner writes. **Options**:
- Use context-threading: the outer `RunInTx` stashes the current tx in `ctx`, the inner `RunInTx` looks up context and joins the existing tx (no new BEGIN). This is the idiomatic Go PG pattern.
- Use savepoints: each nested `RunInTx` opens a savepoint.
- Share the `TxManager` instance such that both see the same tx pool (but this is not enough if the PG connection is per-call).

The comment at L246–247 of `credit_note_service.go` explicitly names this as the adapter contract:

> `// With NoopTxManager this nests transparently. Real DB implementations`
> `// must support savepoints or reuse the outer transaction.`

---

## 8. Batch processing (`batch/contract_renewal.go`)

- Entry point: `contractRepo.FindDueForRenewal(ctx, now)` (L59) — returns a slice of `*ContractAggregate`. No cursor, no pagination, no streaming; the adapter must be able to return the full set in one call. It is *not* `FindExpiring` but `FindDueForRenewal` — the semantics (exact predicate) are inside the repo implementation.
- Concurrency: optional, controlled by `opts.Concurrency`. Sequential by default (L77).
- Per contract: `processOne` (L139):
  - Dry run: read-only checks against `agg.Status()`, `agg.CancelAtPeriodEnd()`, `agg.AutoRenew()`.
  - Live: `p.resolveInterval(ctx, agg)` → may call `priceRepo.FindByID(pendingPriceID)` (L241); then `agg.RenewWithInterval(interval, metadata)` (L166); then `RunInTx { repos.Contracts.Save(agg) }` (L173).
  - Post-commit non-fatal hooks.

`batch/processor.go` is just the `BatchProcessor` interface and `BatchOptions`/`BatchResult` types — trivial.

No "scan all contracts" is needed — only `FindDueForRenewal` (time-scoped).

---

## 9. Invoice state transitions needed

From `domain/invoice/entity.go`:
- `Finalize()` (L231): Draft → Finalized.
- `RecordPayment(amount, paidAt)` (L266): Finalized/Issued/PartialPaid/Overdue → Paid or PartialPaid.
- `ValidatePayment(amount)` (L242): read-only pre-check.
- `Void()` (L336): Draft/Finalized → Voided.
- `VoidWithReason(reason)` (L349): anything except Voided/Refunded → Voided (sets voidReason).
- `SetRevisionOf(id)` (L374), `SetOriginalInvoiceID(id)` (L382): mutate revision pointers post-creation.

Callers in services:
- `billing_service.executeBillingPipeline`: creates via `invoice.NewInvoice` (status=Draft) and writes.
- `credit_note_service.ReissueInvoice`: calls `original.VoidWithReason`, then `replacement.SetRevisionOf` + `SetOriginalInvoiceID`.
- `payment_service.ProcessPayment`: calls `inv.RecordPayment`.
- `examples/billing-demo/main.go`: explicitly calls `inv.Finalize()` (L119).

**Not called from any service** (searched `application/service/`, `batch/`, `examples/billing-demo/`):
- `Void()` — only `VoidWithReason` is used.
- There is no `MarkIssued` / `MarkOverdue` in the entity file we read. (If they exist elsewhere, the overdue/issued statuses are set by some other means, possibly via the repository from projection/read-model.)
- `SetRevisionOf` and `SetOriginalInvoiceID` — only via `ReissueInvoice`.

**Critical for adapter**: the state `voided` can only be reached via `VoidWithReason`, which also sets `voidReason`. Restoring from DB must populate `status + voidReason` together, otherwise a round-tripped Voided invoice will lose the reason and any auditing query will return empty. Same for `revisionOf`, `originalInvoiceID` — these are set via direct setters (not constructors), so the DB row must store them and the reload must populate them.

---

## 10. Payment state transitions needed

From `domain/payment/entity.go`:
- `Complete()` (L90): Pending → Completed.
- `Fail(reason)` (L100): Pending → Failed, sets `failureReason`.
- `MarkRefunded()` (L111): Completed/PartiallyRefunded → Refunded. (Legacy, retained.)
- `MarkPartiallyRefunded()` (L121): Completed → PartiallyRefunded. (Legacy.)
- `MarkChargedBack()` (L131): Completed → ChargedBack.
- `RecordRefund(amount)` (L145): Completed/PartiallyRefunded → PartiallyRefunded or Refunded, accumulates `refundedAmount`. **Preferred over MarkRefunded/MarkPartiallyRefunded for new code.**
- `SetIdempotencyKey(key)` (L87): direct setter.

Callers:
- `payment_service.ProcessPayment`: `NewPayment` (fresh) → `Complete()` on success path; `Fail(err.Error())` on failure path; `SetIdempotencyKey(key)` on both success and pending paths.
- `payment_service.Refund`: `p.RecordRefund(refundAmount)` on reloaded payment.

`MarkRefunded`, `MarkPartiallyRefunded`, `MarkChargedBack` — **not called** from any of the services we read. They may be called from tests or external integrations (e.g. dunning).

**Critical for adapter**:
- `refundedAmount` is cumulative and must round-trip.
- `status` is derived from `refundedAmount` vs `amount` for the refund path, but the DB must still store status explicitly because `Complete/Fail/Pending/ChargedBack` transitions do not feed into `refundedAmount`.
- `failureReason` is only set on `Fail`; must round-trip.
- `idempotencyKey` is used for dedupe lookups (`FindByIdempotencyKey`) — the adapter needs a unique index on it.
- `processedAt`, `gatewayTransactionID`, `method`, `amount`, `invoiceID` are all set at construction but never updated; plain columns.

---

## 11. Direct eventstore access from services

Searched all files under `application/service/`, `batch/`, `examples/billing-demo/`.

- `snapshot_service.go` L64 calls `eventStore.SaveSnapshot` directly.
- `query/temporal_query_service.go` L43, L57, L84 calls `eventStore.LoadSnapshotBefore`, `LoadUntil`, `Load` directly.
- `projection/service.go` L58, L107 calls `eventStore.Subscribe`, `LoadAll`.
- `payment_service.go` holds an `eventStore` dependency but does not invoke it in the file we read.
- `billing_service.go`, `credit_note_service.go`, `batch/contract_renewal.go`, `examples/billing-demo/main.go` do **not** call eventStore directly.

So the event store is accessed directly only from: snapshot service, temporal query service, projection service. Everything else goes through repositories.

---

## 12. Reliance on `Event.GlobalPosition` being non-zero after `Append`

From `eventstore/event.go` L27: `GlobalPosition int64 json:"global_position,omitempty"`.

From `projection/service.go` L99–L128: `RebuildAll` uses `events[len(events)-1].GlobalPosition` as the cursor (L121), and explicitly errors out if it does not advance (L123):

```go
newPosition := events[len(events)-1].GlobalPosition
if newPosition == fromPosition {
    return fmt.Errorf("LoadAll returned events but global position did not advance (stuck at %d)", fromPosition)
}
```

And `eventStore.LoadAll` signature (`eventstore/store.go` L30) takes `fromPosition int64` (exclusive).

**Conclusion**: `Append` must populate `GlobalPosition` on stored events, `LoadAll` must return them in monotonically increasing `GlobalPosition` order, and `Subscribe(ctx, fromPosition)` must use the same cursor. The PG adapter cannot leave `GlobalPosition=0` — projections and rebuilds will loop or deadlock. A `BIGSERIAL` / sequence column on the events table is the standard solution.

Note: `projection.Start` (L58) calls `Subscribe(ctx, 0)` meaning "from the beginning" — so 0 must mean "before any event" in Subscribe semantics. Adapter must be careful not to assign `GlobalPosition=0` to any real event.

---

## 13. Sample trace: billing_service "create invoice from contract"

Triggered by `BillingService.GenerateInvoice(ctx, contractID, billingPeriod)` (L141).

Order of operations:

1. L143 `contractRepo.FindByID(ctx, contractID)` → `*ContractAggregate`. (read-only, outside tx)
2. L149 status guard (`billableStatuses`).
3. L155–L165 suspension-behavior guard.
4. L168 `checkDuplicateInvoice(ctx, agg, billingPeriod)` → calls one of:
   - `invoiceRepo.FindByContractAndStatus(contractID, Draft)` (L719), or
   - `invoiceRepo.FindByContractID(contractID)` (L730), or
   - `invoiceRepo.FindByContractAndPeriod(contractID, period)` (L743).
5. L173 `calculateSubtotal(ctx, agg, period)`:
   - Period-match validation (L528).
   - `priceRepo.FindByID(agg.PriceID())` (L541) → `*Price`.
   - If PricingModel == nil: build a single `invoice.NewLineItem` with the flat amount, return.
   - Else: `calculateUsageCharge` which calls `productRepo.FindByID(price.ProductID())` (L582) and for each metric `usageRepo.GetSummary(contractID, metricName, period)` (L602), summing via `PricingModel.CalculatePrice`.
6. L178 `executeBillingPipeline(ctx, pipelineInput)`:
   - L383 `priceRepo.FindByID(agg.PriceID())` again (to set ProductID on plugin calc context). (Note: duplicate load — not obviously cached.)
   - L391 `BeforeCalculation` hooks (plugin).
   - L401 `DiscountHook.CalculateDiscount` in a loop, capped at subtotal.
   - L426 `TaxHook.CalculateTax` in a loop, summed.
   - L444 `invoiceID := shared.NewInvoiceID()` (generated before Tx so balance applications can reference it).
   - L448 **`s.txManager.RunInTx`** opens the transaction:
     - L453 `applyBalances(txCtx, repos.Balances, accountID, invoiceID, total, currency)`:
       - `balanceRepo.FindAvailable(accountID, currency)` (L642).
       - For each balance: `entry.Consume(remaining)`, build BalanceApplication record.
       - For each mutation: `balanceRepo.Save(entry)` + `balanceRepo.SaveApplication(application)` (L703, L706).
     - L460 compute `amountDue = total - appliedBalance`.
     - L486 `invoice.NewInvoice(...)` with all option fields populated.
     - L501 `AfterCalculation` hooks (can mutate invoice in-memory).
     - L508 `repos.Invoices.Save(txCtx, inv)` — single Save of fully-constructed Invoice.
   - Return `inv`.

Repo call sequence summary (ignoring plugin logic):
```
contractRepo.FindByID
invoiceRepo.Find{ByContractAndStatus|ByContractID|ByContractAndPeriod}
priceRepo.FindByID
productRepo.FindByID                       (usage-based only)
usageRepo.GetSummary                        (usage-based only, per metric)
priceRepo.FindByID                           (again for plugin ctx)
— RunInTx {
    balanceRepo.FindAvailable
    balanceRepo.Save                         (per consumed entry)
    balanceRepo.SaveApplication              (per consumed entry)
    invoiceRepo.Save                         (single)
  }
```

---

## Adapter-level implications (summary)

1. **`Invoice.Save` must be an unconditional full upsert** of every entity field including revision pointers, `voidReason`, `paidAt`, `metadata`, `allowPartialPay`, `lineItems[]` (with priceID + metadata per line), `billingPeriod`. No partial updates.
2. **`Payment.Save` similarly** must persist `status`, `refundedAmount`, `failureReason`, `idempotencyKey` (unique index needed), `gatewayTransactionID`, `method`, `processedAt`.
3. **`CreditNote.Save` must persist all status-transition state**: `Issue`, `Apply`, `Refund` all mutate and save without going through `tx.Repos`. Each call is a single Save; no tx wrapper, no optimistic lock contract implied beyond what the repository itself provides.
4. **`tx.Repos` does NOT include `CreditNotes`.** Credit-note service operations run outside `RunInTx` (except via ReissueInvoice which only touches Invoices inside the tx).
5. **Nested `RunInTx` is required.** `ReissueInvoice` wraps `BillingService.GenerateInvoice`, which opens its own `RunInTx`. The PG adapter must either propagate the outer tx via context or use savepoints.
6. **`BalanceRepository` must expose both `Save(*BalanceEntry)` and `SaveApplication(*BalanceApplication)`** and `FindAvailable(accountID, currency)`. The entry's consumed-amount must round-trip.
7. **`PaymentRepository.FindByIdempotencyKey`** is invoked both inside and outside transactions (L219 outside, L298 inside). It must be callable on both the root repo and the tx-scoped repo.
8. **`InvoiceRepository.FindByContractAndPeriod`, `FindByContractAndStatus`, `FindByContractID`** are all used for duplicate prevention — the adapter must return a full list (no pagination) that includes voided invoices.
9. **`eventStore.Append` must assign a monotonically increasing `GlobalPosition`** and `Subscribe`/`LoadAll` must honour it. Projection rebuilds depend on this invariant and will error out if violated.
10. **Projection is async**: subscribers run in a separate goroutine via `Subscribe`. Writes do not block on projection catchup. The adapter's `Subscribe` must deliver events eventually after `Append` commits, but does not need to be synchronous within `Append`.
11. **No service writes events directly.** All event writes go through contract repo `Save`. Payment/Invoice/CreditNote are CRUD entities (non-event-sourced).
12. **Snapshot creation is opt-in**, not auto-driven by services. PG adapter must support save/load/loadBefore snapshot APIs, but does not need to hook `Append` to drive snapshotting itself.
13. **`BatchProcessor` only needs `contractRepo.FindDueForRenewal`** — no full-scan requirement.
