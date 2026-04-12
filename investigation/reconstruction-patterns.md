# Reconstructing DDD Aggregates from Persistence in Go — A Comparative Report

**Context**: Designing a PostgreSQL adapter for the `contract-to-cash/core` billing library. The core domain (e.g. `invoice.Invoice`) uses unexported fields (`paidAmount`, `voidReason`, `status`, …) with invariant-protecting methods (`RecordPayment`, `VoidWithReason`, `Finalize`). There are no setters. The adapter must reload entities from Postgres rows **without** going through business flows, because some states (e.g. `Refunded`) are unreachable via the public API and all mutators enforce preconditions that do not hold mid-reload.

This report compares four candidate strategies, draws on real Go DDD projects and the Java/C# DDD literature, and recommends a concrete approach for this codebase.

---

## 1. The hard Go-specific constraints

Before comparing options, several Go-specific facts bound the design space:

1. **`encoding/json` cannot populate unexported fields.** This is explicitly documented and enforced ([pkg.go.dev/encoding/json](https://pkg.go.dev/encoding/json)). Only exported (capitalized) fields are visible to the reflection-driven marshaling machinery. Issue [golang/go#1263](https://github.com/golang/go/issues/1263) and [golang/go#18009](https://github.com/golang/go/issues/18009) confirm that even values Go *writes* through struct embedding tricks cannot be *read back* via `Unmarshal`.
2. **`reflect` alone cannot *set* unexported fields.** You can read them with `FieldByName(...)` but calling `.Set()` panics: *"reflect: reflect.Value.Set using value obtained using unexported field"*. This is a deliberate design decision — otherwise any package could trivially defeat another package's encapsulation ([yourbasic.org/golang/access-private-field-reflection](https://yourbasic.org/golang/access-private-field-reflection/)).
3. **`reflect` + `unsafe.Pointer` via `reflect.NewAt(t, unsafe.Pointer(f.UnsafeAddr())).Elem()` *can* write unexported fields.** This is the documented escape hatch ([Emad Elsaid — Access unexported struct fields](https://www.emadelsaid.com/Access%20unexported%20struct%20fields/), [Medium — Darshan N A](https://medium.com/@darshan.na185/modifying-private-variables-of-a-struct-in-go-using-unsafe-and-reflect-5447b3019a80)). It is also universally considered a last resort, bypasses the type system, is brittle across refactors, and is rejected by most production Go codebases.
4. **Subpackages have no special access to parent-package unexported identifiers.** Go's visibility rule is per-package, not per-directory tree. So a package at `core/domain/invoice/persistence/` is exactly as locked out of `invoice.Invoice`'s private fields as an external module would be.

**Net effect**: Options 2 (memento *subpackage*) and 3 (`UnmarshalJSON` on the domain type without a manual DTO bridge) are **not viable in their pure form**. They can only be made to work by either (a) exporting the fields, (b) living in the same package as the domain type, or (c) writing the field-by-field DTO bridge manually inside the domain package. That bridge, when you implement it, is essentially Option 1 with extra steps.

---

## 2. How real Go DDD projects handle this

### 2.1 ThreeDotsLabs / wild-workouts-go-ddd-example — canonical pattern

The single most-cited Go DDD repo in the community is [`ThreeDotsLabs/wild-workouts-go-ddd-example`](https://github.com/ThreeDotsLabs/wild-workouts-go-ddd-example). Their `Hour` aggregate is encapsulated with private fields (`hour`, `availability`) and exposes both normal factory methods **and a dedicated factory for reconstitution from the database**, living in the same package as the aggregate:

```go
// internal/trainer/domain/hour/hour.go
type Hour struct {
    hour         time.Time
    availability Availability
}

// UnmarshalHourFromDatabase unmarshals Hour from the database.
//
// It should be used only for unmarshalling from the database!
// You can't use UnmarshalHourFromDatabase as constructor — It may put domain
// into the invalid state!
func (f Factory) UnmarshalHourFromDatabase(hour time.Time, availability Availability) (*Hour, error) {
    if err := f.validateTime(hour); err != nil {
        return nil, err
    }
    if availability.IsZero() {
        return nil, errors.New("empty availability")
    }
    return &Hour{
        hour:         hour,
        availability: availability,
    }, nil
}
```

Key observations about this pattern:

- It lives **in the domain package** (same package as `Hour`) — this is what gives it access to the unexported fields.
- It hangs off a **`Factory` type** so that cross-cutting configuration (validation windows, tenant config, clock) can be passed in once and reused.
- It **keeps the "structural" invariants that must always hold** (e.g. "hour must be a full hour") but **skips the "temporal" invariants** that would make reload impossible (e.g. "must not be in the past" — though in this specific example they *do* validate, because their domain allows it; billing domains typically would not).
- The name (`UnmarshalHourFromDatabase`) and doc-comment make the intent unmistakable: this is *not* a normal constructor; violating its contract puts the aggregate into an invalid state.

This is the pattern that Three Dots Labs promotes in their DDD Lite series ([threedots.tech/post/ddd-lite-in-go-introduction](https://threedots.tech/post/ddd-lite-in-go-introduction/) and the companion [Repository post](https://threedots.tech/post/repository-pattern-in-go/)).

### 2.2 marcusolsson/goddd (the shipping example)

[`marcusolsson/goddd`](https://github.com/marcusolsson/goddd) — a Go port of Eric Evans' DDDSample — **sidesteps the problem entirely by exporting all fields**:

```go
type Cargo struct {
    TrackingID         TrackingID
    Origin             UNLocode
    RouteSpecification RouteSpecification
    Itinerary          Itinerary
    Delivery           Delivery
}
```

It pays for this with a weaker encapsulation story (any caller can mutate `cargo.Delivery` directly), but gets free `database/sql`, `encoding/json`, and `mgo`/`bson` interop. This is a legitimate trade-off: **in Go, exporting fields is an accepted DDD compromise**, and it is what a substantial fraction of "Go DDD" projects end up doing once persistence enters the picture.

### 2.3 sklinkert/go-ddd

[`sklinkert/go-ddd`](https://github.com/sklinkert/go-ddd) takes the same route as `goddd`: its `Product` entity exports `Id`, `Name`, `Price`, etc., and methods like `UpdateName` mutate the struct directly. Validation is applied in mutator methods via a private `validate()`, but the fields themselves are public so that `sqlc`-generated code and `database/sql` scanning "just work."

### 2.4 Summary of the Go landscape

| Project | Field visibility | Reconstruction mechanism |
|---|---|---|
| ThreeDotsLabs wild-workouts | Private | Same-package `Factory.UnmarshalHourFromDatabase` |
| marcusolsson/goddd | **All public** | Direct struct scanning |
| sklinkert/go-ddd | **All public** | Direct struct scanning (sqlc-generated) |
| takashabe/go-ddd-sample | Mostly public | Direct mapping in infra layer |
| programmingpercy / Ompluscator tutorials | Private | Same-package factory method recommended |

**There are essentially only two idioms in Go DDD**: (a) expose the fields and let standard tooling reconstruct them, or (b) keep fields private and add a same-package "unmarshal-from-persistence" factory. Nobody in the canonical Go DDD corpus uses subpackage mementos or `unsafe`-based reflection for routine persistence, because both are painful and non-idiomatic.

---

## 3. How Java / C# / Kotlin DDD solves this

The Go community's "same-package factory" idiom is a direct analogue of what the mainstream DDD literature has quietly been doing for a decade, simply adapted to Go's visibility rules.

### 3.1 Hibernate / JPA (Java)

Hibernate **requires a no-arg constructor** (public, protected, or package-private — and in fact often private via reflection tricks) and **uses reflection via `Field.setAccessible(true)` to write private fields directly** ([Baeldung — no-arg constructor for JPA](https://www.baeldung.com/jpa-no-argument-constructor-entity-class), [Xavier Bouclet — JPA and private setter/constructor](https://www.xavierbouclet.com/2019/05/10/Jpa-and-private-setter-and-private-constructor.html)). In Java this is acceptable because the JVM's `setAccessible` mechanism is a sanctioned, platform-supported reflection escape hatch.

This is conceptually identical to Go's `reflect+unsafe` workaround — but Java has first-class tooling (`setAccessible`) and a community tradition that Go does not. In Go, reaching for `unsafe` is a red flag; in Java, `setAccessible` is daily bread.

### 3.2 Spring Data JDBC (not JPA)

Spring Data JDBC deliberately **does not use reflection to mutate private fields**. Instead it requires either a constructor with matching parameters or exposed properties. This is the closest Java-world analogue to Go's options 1 and 2 — "dedicated persistence constructor."

### 3.3 Marten / EventStoreDB (C#)

For state-sourced persistence Marten uses a DTO-like document and rehydrates via `System.Text.Json`, which **does** populate private setters (C# allows this via `JsonInclude`). For event-sourced aggregates it expects a **static `Create` factory plus `Apply(TEvent)` methods** ([martendb.io — Aggregate Projections](https://martendb.io/events/projections/aggregate-projections.html), [martendb.io — Event Sourced Aggregate tutorial](https://martendb.io/tutorials/event-sourced-aggregate)). The `Apply` methods directly mutate private state — which is fine because they live on the aggregate itself, inside its encapsulation boundary.

### 3.4 Axon Framework (Java)

Axon reconstructs aggregates by **replaying events onto an empty instance via event sourcing handlers** ([docs.axoniq.io — Aggregates](https://docs.axoniq.io/axon-framework-reference/4.11/axon-framework-commands/modeling/aggregate/)). When replaying, validation logic in command handlers is skipped — only the "apply" side runs. The aggregate is then "live" and ready to accept new commands. Snapshots are an optimization layer on top.

### 3.5 Matthias Noback's "ORMless memento-like pattern" (PHP)

Noback's classic post ([matthiasnoback.nl — ORMless](https://matthiasnoback.nl/2018/03/ormless-a-memento-like-pattern-for-object-persistence/)) proposes **entities expose `getState(): array` and a static `fromState(array): Entity` factory**. The repository does:

```php
public function add(Entity $e): void {
    $this->connection->insert($this->table, $e->getState());
}
public function getById($id): Entity {
    return Entity::fromState($this->connection->findOne($this->table, ['id' => $id]));
}
```

In Go, `fromState` must live in the same package as `Entity` to touch unexported fields — which maps exactly onto the ThreeDotsLabs `UnmarshalHourFromDatabase` pattern.

### 3.6 Vaughn Vernon / Evans / Fowler

- **Evans (Blue Book)**: Introduces "Factories" as the construction responsibility, and notes repositories should "reconstitute" aggregates. He is deliberately language-agnostic; the mechanism is left open.
- **Vernon (IDDD, "Effective Aggregate Design")**: Recommends a dedicated *reconstitution* path through repositories, distinguishes "creating a new aggregate" from "loading an existing one," and treats the loading path as a separate concern that does **not** need to re-run all creation invariants ([Vernon — Effective Aggregate Design Part I](https://www.dddcommunity.org/wp-content/uploads/files/pdf_articles/Vernon_2011_1.pdf)). He explicitly calls out that loading should not trigger domain events.
- **Fowler (PoEAA — Data Mapper)**: The mapper must be able to populate the domain object. Fowler notes the tension: *"Mappers need access to the fields in the domain objects, which can be problematic because you need public methods [or reflection] to support the mappers, which you don't want for domain logic."* ([martinfowler.com — Data Mapper](https://martinfowler.com/eaaCatalog/dataMapper.html)). He lists three solutions: (a) reflection (the Java/JPA default), (b) create empty and populate, (c) "a rich constructor so that it's created with all its mandatory data" — which is exactly Option 1 below.

**Key takeaway from the literature**: Every serious DDD book/framework treats reconstitution as a **separate concern from construction**, bypasses creation-time invariants, and relies on either reflection (where the language allows it) or a dedicated factory path.

---

## 4. Evaluating the four candidate options

### Option 1 — Public `Reconstruct*` / `UnmarshalFromDatabase` function in the domain package

```go
// domain/invoice/reconstruct.go
package invoice

// ReconstructInvoice rebuilds an Invoice from persisted state.
//
// WARNING: For persistence-adapter use only. This bypasses all creation
// invariants and must only be called with state previously produced by the
// aggregate's own getters. Passing arbitrary values will put the aggregate
// into an invalid state.
func ReconstructInvoice(
    id shared.InvoiceID,
    invoiceNumber string,
    accountID shared.AccountID,
    status InvoiceStatus,
    paidAmount shared.Money,
    voidReason string,
    // ... ~25 more fields
) *Invoice {
    return &Invoice{
        id:            id,
        invoiceNumber: invoiceNumber,
        accountID:     accountID,
        status:        status,
        paidAmount:    paidAmount,
        voidReason:    voidReason,
        // ...
    }
}
```

**Pros**:
- **Idiomatic Go.** This is the ThreeDotsLabs pattern, the recommended pattern in every Go DDD tutorial that keeps fields private ([programmingpercy.tech](https://programmingpercy.tech/blog/how-to-domain-driven-design-ddd-golang/), [ompluscator.com — Factory](https://www.ompluscator.com/article/golang/practical-ddd-domain-factory/)).
- **Zero reflection, zero `unsafe`, zero magic.** Trivial to read, trivial to debug, trivial to refactor with `gopls` rename.
- **Explicit and auditable.** Every field that can be restored is visible at the call site. Adding a new private field forces a compile error in the adapter, which is exactly the safety you want.
- **Works for unreachable states.** You can reconstruct `status = Refunded` even if no public mutator produces it.
- **Can still enforce structural invariants.** You can return `(*Invoice, error)` and validate things like "`paidAmount <= total`", "`status` is a known value," "`id` is non-zero."

**Cons**:
- **Large parameter list.** Invoice has ~25 fields, so the signature is long. Can be mitigated with a parameter struct (`type InvoiceSnapshot struct { ... }`) — see Option 1b below.
- **Visible from any caller in the same module.** Nothing stops application code from calling `ReconstructInvoice` to sidestep business rules. Mitigated by: (a) documentation, (b) linter rules (`depguard`, `forbidigo`), (c) naming convention (`UnmarshalInvoiceFromDatabase` is louder than `NewInvoice`), (d) placing it in a file named `reconstruct.go` with a file-level comment.
- **Tight coupling between core and adapter.** Adding a new field means bumping the core API. But that coupling is **inherent** — any persistence strategy that stores the new field has to know about it.

**Variant — Option 1b (snapshot struct parameter)**: Define a public `InvoiceSnapshot` struct inside the `invoice` package, with all-public fields, and a `ReconstructInvoice(s InvoiceSnapshot) (*Invoice, error)`. Also expose `(*Invoice).Snapshot() InvoiceSnapshot`. This is isomorphic to Noback's memento pattern, keeps the domain package's "no setters on the aggregate" rule intact, and makes the adapter code extremely clean:

```go
// In adapter:
row := db.QueryRow(...)
var s invoice.InvoiceSnapshot
if err := row.Scan(&s.ID, &s.Status, &s.PaidAmount, ...); err != nil { ... }
inv, err := invoice.ReconstructInvoice(s)
```

**Verdict**: This is the recommended option — specifically **Option 1b**, the snapshot-struct variant.

### Option 2 — Memento subpackage (`domain/invoice/persistence/`)

**Not viable in pure form in Go.** A subpackage has no access to parent-package unexported identifiers. You would have to either:

- Put the aggregate's fields **`Public`**, defeating the purpose; or
- Define a thin bridge inside `domain/invoice/` that the subpackage calls — in which case the bridge is Option 1 and the subpackage adds zero value; or
- Use `unsafe`-reflection in the subpackage — see Option 5.

In Java/C#/PHP this pattern does work (package-private, internal, friend classes). In Go, it doesn't translate. **Skip.**

### Option 3 — `MarshalJSON` / `UnmarshalJSON` on the domain type

Go's `encoding/json` **cannot** read or write unexported fields via reflection. To make this work you must **hand-write** both `MarshalJSON` and `UnmarshalJSON` on every domain type, each one marshaling through a private `invoiceJSON` DTO struct. Example:

```go
type invoiceJSON struct {
    ID            shared.InvoiceID `json:"id"`
    Status        InvoiceStatus    `json:"status"`
    PaidAmount    shared.Money     `json:"paid_amount"`
    // ... all fields
}

func (inv *Invoice) MarshalJSON() ([]byte, error) {
    return json.Marshal(invoiceJSON{
        ID: inv.id, Status: inv.status, PaidAmount: inv.paidAmount, ...
    })
}

func (inv *Invoice) UnmarshalJSON(data []byte) error {
    var j invoiceJSON
    if err := json.Unmarshal(data, &j); err != nil { return err }
    inv.id = j.ID
    inv.status = j.Status
    inv.paidAmount = j.PaidAmount
    // ...
    return nil
}
```

**Pros**:
- Convenient if you want to store the aggregate as a JSONB blob in Postgres and round-trip the whole thing opaquely.
- The adapter code becomes `json.Unmarshal(bytes, &inv)` — one line.

**Cons**:
- **Still requires writing out every field by hand**, because the middle DTO must be hand-coded. So the "bulk" of the work is identical to Option 1 — you are writing the same field list in the same package. You have just traded `ReconstructInvoice(snap)` for `UnmarshalJSON(data)`.
- **Conflates serialization with reconstitution.** If anywhere else in the codebase calls `json.Marshal(inv)` (e.g. to emit a public HTTP response or a logging line), it now leaks the entire internal state. You almost never want HTTP-facing JSON to be identical to persistence JSON (field names, precision, historical fields, PII, …).
- **JSONB blobs are a poor relational design** for a billing/contract-to-cash system, where you genuinely want to query `WHERE status = 'overdue' AND due_date < NOW()`. You lose index-ability, partial updates, and schema evolution is painful.
- **Every domain type needs its own pair of methods.** `Invoice`, `LineItem`, `CreditNote`, `Payment`, `Contract`, `BillingCycle`, … — all of it.
- **Fragile against new fields.** Forgetting to add a field to `UnmarshalJSON` silently drops data on reload instead of producing a compile error.

**Verdict**: Not recommended as the primary strategy. Acceptable as a supplementary technique for *value objects* that naturally live inside JSONB columns (e.g. `metadata map[string]string`, `LineItem[]` if you chose to store line items inline).

### Option 4 — Event sourcing / state replay

Reconstruct by replaying the command sequence: `NewInvoice → AddLineItem(...) → Finalize → RecordPayment(...) → VoidWithReason(...)`.

**Pros**:
- Aligns with Axon/Marten-style event sourcing.
- No new APIs on the domain — uses the existing public behavior.
- Provides full audit history as a side effect.

**Cons**:
- **Requires an event store.** The current `core` library is not event-sourced; it is state-oriented. Adopting event sourcing is a fundamental architectural change that touches every write path.
- **Unreachable states break it.** `InvoiceStatusRefunded` has no mutator. You cannot "replay" your way into it. You would have to add mutators for every state — which re-introduces the exact setters you wanted to avoid.
- **Temporal invariants break replay.** `RecordPayment` validates "invoice is not voided," "amount <= balance," "paid_at is after issue_date." During replay you are feeding historical values that were valid *then* but may fail *now* if business rules have evolved. Axon solves this by versioning events and using dedicated `@EventSourcingHandler` methods that **bypass** command-side validation — i.e. a parallel code path, which is effectively... Option 1 at the event-application level.
- **Massive scope increase.** Event sourcing is justified by audit, temporal queries, and projection flexibility — not by "we want to reload an entity from a row."

**Verdict**: Not recommended unless the business independently wants event sourcing. For a PostgreSQL adapter on an existing state-oriented core, this is the wrong tool.

### Option 5 — `reflect` + `unsafe` to set unexported fields (the JPA approach)

Conceptually: the adapter loads a row into a DTO, then walks the DTO via `reflect` and writes values into the aggregate's unexported fields using `reflect.NewAt(t, unsafe.Pointer(f.UnsafeAddr())).Elem().Set(...)`.

**Pros**:
- Zero changes to the `core` package. The adapter is the *only* thing that has to know about persistence.
- Mirrors the JPA/Hibernate approach.

**Cons**:
- **`unsafe` is a strong negative signal in Go.** Code reviews will flag it. It bypasses the type system. A field rename in `core` silently becomes a runtime panic or, worse, silent data corruption (fields that no longer exist are just not written).
- **No compile-time safety.** The "adapter knows about field X" invariant is only checked at runtime in integration tests.
- **Field-by-field `FieldByName` is slow.** Not a dealbreaker but not free.
- **Nothing in the Go DDD corpus does this for routine persistence.** The only legitimate uses of `unsafe`-reflection in the wild are testing libraries (`go-cmp`, some mocks) and serialization libraries that have extreme performance requirements — not domain persistence.

**Verdict**: Not recommended. Use Option 1 instead — it costs roughly the same amount of keystrokes and gives you compile-time safety, IDE support, and no `unsafe`.

---

## 5. Recommendation

### Primary: Option 1b — Snapshot struct + same-package `Reconstruct` constructor

Add a file `domain/invoice/reconstruct.go` inside the `core` module:

```go
package invoice

import (
    "time"

    "github.com/contract-to-cash/core/domain/shared"
)

// InvoiceSnapshot is a flat, fully-exported representation of an Invoice's
// persisted state. It exists solely to mediate between the domain aggregate
// (whose fields are unexported) and persistence adapters (which need to read
// and write every field).
//
// A Snapshot is NOT a domain concept. It carries no invariants beyond what the
// underlying storage guarantees, and must only be produced by:
//   - Invoice.Snapshot(), when saving,
//   - or a trusted persistence adapter, when loading.
//
// The struct is append-only: new fields on Invoice require adding the
// corresponding field here, which is a deliberate, compiler-enforced contract
// change that adapters must react to.
type InvoiceSnapshot struct {
    ID                shared.InvoiceID
    InvoiceNumber     string
    AccountID         shared.AccountID
    ContractID        shared.ContractID
    LineItems         []LineItemSnapshot
    Subtotal          shared.Money
    TaxAmount         shared.Money
    DiscountAmount    shared.Money
    Total             shared.Money
    AppliedBalance    shared.Money
    AmountDue         shared.Money
    PaidAmount        shared.Money
    Balance           shared.Money
    Status            InvoiceStatus
    BillingPeriod     shared.DateRange
    IssueDate         time.Time
    DueDate           time.Time
    PaidAt            *time.Time
    Metadata          map[string]string
    AllowPartialPay   bool
    OriginalInvoiceID *shared.InvoiceID
    RevisionOf        *shared.InvoiceID
    VoidReason        string
    PaymentMethodID   *string
    // ... etc
}

// Snapshot returns the persisted state of the Invoice. Intended for use
// by persistence adapters only.
func (inv *Invoice) Snapshot() InvoiceSnapshot {
    lis := make([]LineItemSnapshot, len(inv.lineItems))
    for i, li := range inv.lineItems {
        lis[i] = li.Snapshot()
    }
    cp := make(map[string]string, len(inv.metadata))
    for k, v := range inv.metadata {
        cp[k] = v
    }
    return InvoiceSnapshot{
        ID:                inv.id,
        InvoiceNumber:     inv.invoiceNumber,
        AccountID:         inv.accountID,
        ContractID:        inv.contractID,
        LineItems:         lis,
        Subtotal:          inv.subtotal,
        // ... etc
        Metadata:          cp,
        VoidReason:        inv.voidReason,
        OriginalInvoiceID: inv.originalInvoiceID,
        RevisionOf:        inv.revisionOf,
        PaymentMethodID:   inv.paymentMethodID,
    }
}

// ReconstructInvoice rebuilds an Invoice from a previously produced Snapshot.
//
// WARNING: Persistence-adapter use only. This bypasses all creation-time
// invariants (e.g. "status must start as Draft", "paidAmount must be zero").
// It enforces only structural invariants that MUST hold at all times
// (e.g. a known status value, consistent money currencies).
//
// Callers: the repository layer in the persistence adapter. Do not call this
// from application code, domain services, or use-case handlers. Linter rule
// `forbidigo` is configured to flag non-adapter callers.
func ReconstructInvoice(s InvoiceSnapshot) (*Invoice, error) {
    if s.ID.IsZero() {
        return nil, shared.NewDomainError(shared.ErrCodeValidation,
            "reconstruct: invoice id must not be zero")
    }
    if !s.Status.IsValid() {
        return nil, shared.NewDomainError(shared.ErrCodeValidation,
            fmt.Sprintf("reconstruct: unknown invoice status %q", s.Status))
    }
    // ... any other permanent structural invariants

    lis := make([]LineItem, len(s.LineItems))
    for i, ls := range s.LineItems {
        li, err := ReconstructLineItem(ls)
        if err != nil {
            return nil, fmt.Errorf("reconstruct line item %d: %w", i, err)
        }
        lis[i] = li
    }

    md := make(map[string]string, len(s.Metadata))
    for k, v := range s.Metadata {
        md[k] = v
    }

    return &Invoice{
        id:                s.ID,
        invoiceNumber:     s.InvoiceNumber,
        accountID:         s.AccountID,
        contractID:        s.ContractID,
        lineItems:         lis,
        subtotal:          s.Subtotal,
        // ... etc
        metadata:          md,
        voidReason:        s.VoidReason,
        originalInvoiceID: s.OriginalInvoiceID,
        revisionOf:        s.RevisionOf,
        paymentMethodID:   s.PaymentMethodID,
    }, nil
}
```

Then the adapter becomes beautifully simple:

```go
// adapters/postgres/invoice_repository.go
func (r *InvoiceRepository) FindByID(ctx context.Context, id shared.InvoiceID) (*invoice.Invoice, error) {
    row := r.db.QueryRowContext(ctx, `SELECT ... FROM invoices WHERE id = $1`, id)

    var s invoice.InvoiceSnapshot
    var metaJSON []byte
    if err := row.Scan(
        &s.ID, &s.InvoiceNumber, &s.AccountID, &s.ContractID,
        &s.Subtotal, &s.TaxAmount, &s.DiscountAmount, &s.Total,
        &s.AppliedBalance, &s.AmountDue, &s.PaidAmount, &s.Balance,
        &s.Status, &s.IssueDate, &s.DueDate, &s.PaidAt,
        &metaJSON, &s.AllowPartialPay, &s.VoidReason,
        /* ... */
    ); err != nil {
        if errors.Is(err, sql.ErrNoRows) {
            return nil, invoice.ErrNotFound
        }
        return nil, err
    }
    if err := json.Unmarshal(metaJSON, &s.Metadata); err != nil {
        return nil, err
    }

    // Load line items into s.LineItems via a separate query or JOIN ...

    return invoice.ReconstructInvoice(s)
}

func (r *InvoiceRepository) Save(ctx context.Context, inv *invoice.Invoice) error {
    s := inv.Snapshot()
    _, err := r.db.ExecContext(ctx, `INSERT INTO invoices ... ON CONFLICT ...`,
        s.ID, s.InvoiceNumber, s.Status, s.PaidAmount /* ... */)
    return err
}
```

### Why this is the right answer for *this* codebase specifically

1. **The `core` library explicitly supports the concept of a persistence boundary** (it already has `repository.go` interfaces in `domain/invoice/`). Adding `Snapshot()` + `ReconstructInvoice()` to the same package is a natural extension of the existing boundary, not a new architectural concept.
2. **The billing domain has unreachable states** (`Refunded`) and paths that are only reachable through complex workflows. Options 3 (event replay) and 4 (hydrate-via-command) cannot express these without adding setters.
3. **Postgres is a relational store, not a document store.** You want columns you can index, constraint, and query. That rules out the "just use JSONB" variant of Option 3.
4. **Go's visibility rules rule out Option 2.** Memento subpackages do not work.
5. **`unsafe` is not idiomatic Go.** Option 5 fails the smell test.
6. **Option 1b is exactly the ThreeDotsLabs pattern**, the most widely-cited Go DDD reference. You are not inventing anything, and future maintainers will recognize the pattern immediately.

### Supplementary: use JSONB (Option 3 techniques) for genuinely opaque sub-values

For fields like `metadata map[string]string` or ad-hoc `LineItem.metadata`, storing them as a JSONB column and using `json.Marshal` / `json.Unmarshal` on the *exported* sub-struct `LineItemSnapshot` / the map itself is perfectly fine. The rule of thumb: **use JSONB for things you never query by**, use columns for things you do.

### Guardrails to add alongside Option 1b

To prevent abuse of `ReconstructInvoice` by non-adapter callers:

1. **Naming.** Consider `UnmarshalInvoiceFromStorage` over `ReconstructInvoice` — the former is visibly "not a normal constructor."
2. **Doc-comment.** Loud, explicit warning (see example above).
3. **Linter rule.** Add a `forbidigo` or `depguard` rule that forbids calling `invoice.Reconstruct*` / `invoice.Unmarshal*FromStorage` from anywhere under `core/application/`, `core/billing-demo/`, etc. Only `adapters/...` may call it.
4. **A `//go:build` or internal package.** Not really possible here since `core` is an external module from `adapters`' perspective — but if the adapters eventually move into the same module, a `core/domain/invoice/internal/reconstruct.go` could restrict visibility. Until then, rely on linting + review.
5. **Structural invariants inside `Reconstruct`.** Still validate the things that must *always* be true (e.g. status is in the enum, money is consistent). Just skip the *temporal* ones (e.g. "status must start as Draft").

---

## 6. Summary comparison table

| Option | Works with private fields? | Idiomatic Go? | Compile-time safe? | Handles unreachable states? | Extra runtime cost? | Verdict |
|---|---|---|---|---|---|---|
| 1 — Same-package `Reconstruct` | Yes | Yes (canonical) | Yes | Yes | None | **Recommended** |
| 1b — Snapshot struct + `Reconstruct` | Yes | Yes (canonical) | Yes | Yes | None | **Recommended (preferred variant)** |
| 2 — Memento subpackage | **No** (Go rule) | N/A | — | — | — | Rejected (not possible) |
| 3 — `MarshalJSON` / `UnmarshalJSON` | Only with hand-written DTO inside domain pkg | Sometimes | Partly | Yes | json overhead | Rejected as primary; OK for JSONB sub-values |
| 4 — Event replay | Yes | Only if event-sourced | N/A | **No** (blocked by validation & unreachable states) | High | Rejected |
| 5 — `reflect` + `unsafe` | Yes | **No** (strong negative signal) | **No** | Yes | Reflection cost | Rejected |

---

## 7. Open questions / next steps

1. **Does `core` accept a `Snapshot()` + `ReconstructInvoice()` addition?** This is the correct place for this code, but it requires a PR against `core`. If the core maintainers prefer to keep `core` completely persistence-unaware, the fallback is Option 5 (`unsafe`) inside the adapter — which this report recommends against, but which is the only alternative that leaves core untouched.
2. **How are `LineItem`s stored?** As rows in a child table (preferred) or JSONB column on the invoice row? This determines whether `LineItemSnapshot` needs its own Postgres mapping or can be serialized into a JSONB column.
3. **Versioning / schema evolution.** When a new private field is added to `Invoice`, the `Snapshot` struct grows, the adapter's SQL grows, and a migration is needed. The compiler will force the first two; the third is a process discipline. Consider adding a `schemaVersion int` field to `InvoiceSnapshot` so that `ReconstructInvoice` can handle multiple storage generations during a migration window.
4. **Optimistic concurrency.** Vernon strongly recommends aggregate-level version numbers. If `Invoice` doesn't have one yet, this is the moment to add it — and to include it in `InvoiceSnapshot`.

---

## Sources

### Go DDD projects
- [ThreeDotsLabs/wild-workouts-go-ddd-example](https://github.com/ThreeDotsLabs/wild-workouts-go-ddd-example) — the canonical Go DDD reference; `internal/trainer/domain/hour/hour.go` `Factory.UnmarshalHourFromDatabase`
- [marcusolsson/goddd](https://github.com/marcusolsson/goddd) — exported-field approach
- [sklinkert/go-ddd](https://github.com/sklinkert/go-ddd) — exported-field approach (sqlc-backed)
- [takashabe/go-ddd-sample](https://github.com/takashabe/go-ddd-sample)

### Go language constraints
- [pkg.go.dev/encoding/json](https://pkg.go.dev/encoding/json) — documented behavior on unexported fields
- [golang/go#1263 — json.Marshal writes unexported fields which cannot be Unmarshaled](https://github.com/golang/go/issues/1263)
- [golang/go#18009 — unexported embedded fields marshalled but cannot be unmarshalled](https://github.com/golang/go/issues/18009)
- [yourbasic.org — Access private fields with reflection](https://yourbasic.org/golang/access-private-field-reflection/)
- [emadelsaid.com — Access unexported struct fields in Go](https://www.emadelsaid.com/Access%20unexported%20struct%20fields%20in%20Go/)
- [Medium — Modifying Private Variables using unsafe and reflect](https://medium.com/@darshan.na185/modifying-private-variables-of-a-struct-in-go-using-unsafe-and-reflect-5447b3019a80)

### Go DDD tutorials
- [Three Dots Labs — Introduction to DDD Lite](https://threedots.tech/post/ddd-lite-in-go-introduction/)
- [Three Dots Labs — Repository pattern in Go](https://threedots.tech/post/repository-pattern-in-go/)
- [Three Dots Labs — Combining DDD, CQRS, Clean Architecture](https://threedots.tech/post/ddd-cqrs-clean-architecture-combined/)
- [Ompluscator — Practical DDD in Golang: Factory](https://www.ompluscator.com/article/golang/practical-ddd-domain-factory/)
- [Ompluscator — Practical DDD in Golang: Entity](https://www.ompluscator.com/article/golang/practical-ddd-entity/)
- [Ompluscator — Practical DDD in Golang: Aggregate](https://www.ompluscator.com/article/golang/practical-ddd-domain-aggregate/)
- [programmingpercy — How To Implement DDD in Golang](https://programmingpercy.tech/blog/how-to-domain-driven-design-ddd-golang/)
- [Damiano Petrungaro — DDD in Golang - Tactical Design](https://www.damianopetrungaro.com/posts/ddd-using-golang-tactical-design/)

### Classic DDD literature
- [Vaughn Vernon — Effective Aggregate Design Part I](https://www.dddcommunity.org/wp-content/uploads/files/pdf_articles/Vernon_2011_1.pdf)
- [Vaughn Vernon — Effective Aggregate Design Part II](https://kalele.io/wp-content/uploads/2019/01/DDD_COMMUNITY_ESSAY_AGGREGATES_PART_2.pdf)
- [Implementing DDD — Aggregates (InformIT excerpt)](https://www.informit.com/articles/article.aspx?p=2020371)
- [Matthias Noback — ORMless: a Memento-like pattern for object persistence](https://matthiasnoback.nl/2018/03/ormless-a-memento-like-pattern-for-object-persistence/)
- [Martin Fowler — Data Mapper (PoEAA catalog)](https://martinfowler.com/eaaCatalog/dataMapper.html)
- [Baeldung — Persisting DDD Aggregates](https://www.baeldung.com/spring-persisting-ddd-aggregates)
- [Khalil Stemmler — How to Design & Persist Aggregates (TypeScript DDD)](https://khalilstemmler.com/articles/typescript-domain-driven-design/aggregate-design-persistence/)

### Java / C# frameworks for cross-reference
- [Axon Framework — Aggregates reference](https://docs.axoniq.io/axon-framework-reference/4.11/axon-framework-commands/modeling/aggregate/)
- [Marten — Aggregate Projections](https://martendb.io/events/projections/aggregate-projections.html)
- [Marten — Event-Sourced Aggregate tutorial](https://martendb.io/tutorials/event-sourced-aggregate)
- [Baeldung — Need for Default Constructor in JPA Entities](https://www.baeldung.com/jpa-no-argument-constructor-entity-class)
- [Xavier Bouclet — JPA and private setter / private constructor](https://www.xavierbouclet.com/2019/05/10/Jpa-and-private-setter-and-private-constructor.html)
- [Michael Plöd — Persistence Strategies for Aggregates at DDD Europe 2025](https://www.michael-ploed.com/blog/persistence-strategies-for-aggregates-at-ddd-europe-2025)
