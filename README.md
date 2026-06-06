# adapters

Concrete infrastructure adapters for
[`contract-to-cash/core`](https://github.com/contract-to-cash/core). The core
library follows a **BYO DB / BYO Gateway** model: it defines domain repository
and port interfaces but ships only in-memory reference implementations. This
module provides production-oriented implementations of those interfaces.

## Layout

```
mysql/    MySQL 8.0 implementations of the core persistence interfaces
          (event store first; repositories to follow)
```

## Interfaces implemented

| Core interface | Adapter | Status |
|---|---|---|
| `eventstore.Store` | `mysql.EventStore` | ✅ implemented |
| `contract.Repository` | `mysql.*` | ⬜ planned |
| `invoice.Repository` | `mysql.*` | ⬜ planned |
| `payment.Repository` | `mysql.*` | ⬜ planned |
| `balance.Repository` | `mysql.*` | ⬜ planned |
| `pricing.PriceRepository` | `mysql.*` | ⬜ planned |
| `product.Repository` | `mysql.*` | ⬜ planned |
| `usage.Repository` | `mysql.*` | ⬜ planned |
| `port.PaymentGateway` / webhook ports | (separate gateway adapter) | ⬜ planned |

## Testing

Unit tests use [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock) so the SQL,
argument binding, transaction boundaries, and error mapping are verified
deterministically without a running database (`go test ./... -race`).

Integration tests against a real MySQL (via docker / testcontainers) are a
recommended follow-up and are intentionally out of scope for the unit suite.

## Schema

DDL lives next to each adapter (e.g. `mysql/schema.sql`). Apply it before use.
