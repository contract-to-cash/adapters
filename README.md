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
fincode/    fincode payment gateway adapter (port.PaymentGateway / webhooks)
mysql/      MySQL 8.0 event store (eventstore.Store); more repositories to follow
```

## Coverage

| Core interface | postgres | mysql | fincode |
|---|---|---|---|
| `eventstore.Store` | ✅ | ✅ | — |
| `contract.Repository` | ✅ | ⬜ | — |
| `invoice.Repository` | ✅ | ⬜ | — |
| `payment.Repository` | ✅ | ⬜ | — |
| `balance.Repository` | ✅ | ⬜ | — |
| `pricing.PriceRepository` | ✅ | ⬜ | — |
| `product.Repository` | ✅ | ⬜ | — |
| `usage.Repository` | ✅ | ⬜ | — |
| payment gateway / webhooks | — | — | ✅ |

The `mysql` package currently provides the event store; the remaining
repositories are planned and can mirror the `postgres` package, which is the
reference for the full set.

## Testing

- **postgres**: integration tests against a real PostgreSQL.
- **mysql**: unit tests with [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock)
  so SQL, argument binding, transaction boundaries, and error mapping are verified
  deterministically without a running database (`go test ./mysql/... -race`).
  Integration tests against a real MySQL (docker / testcontainers) are a
  recommended follow-up.

## MySQL schema & connection

DDL lives in `mysql/schema.sql`; apply it before use. Configure the MySQL DSN
with `loc=UTC` (and typically `parseTime=true`), e.g.
`user:pass@tcp(host:3306)/db?loc=UTC&parseTime=true`. All timestamps are stored
and returned in UTC; the event store scans `DATETIME` columns correctly under
either `parseTime` setting.
