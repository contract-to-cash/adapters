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

`postgres` and `mysql` implement the same set of core interfaces.

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
| payment gateway / webhooks | — | — | ✅ |

## Testing

- **postgres**: integration tests against a real PostgreSQL.
- **mysql**: unit tests with [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock)
  so SQL, argument binding, transaction boundaries, and error mapping are verified
  deterministically without a running database (`go test ./mysql/... -race`).
  Integration tests against a real MySQL (docker / testcontainers) are a
  recommended follow-up.

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
