# Changelog

All notable changes to `contract-to-cash/adapters` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

This module is versioned **separately** from
[`contract-to-cash/core`](https://github.com/contract-to-cash/core); see the
README for the core version each release targets, and pin compatible tags of both
modules together.

Cutting a release is driven by this file — adding a new `## [x.y.z]` heading and
merging it to `main` triggers the release workflow, which tags the version and
publishes a GitHub Release. See the **Releasing** section of the README.

## [Unreleased]

### Fixed

- **postgres**: a subscription no longer becomes a silent zombie after its
  LISTEN connection dies (DB failover, `pg_terminate_backend`). The dead
  connection is released, a fresh one is acquired and re-LISTENed with capped
  exponential backoff, and catch-up closes the notification gap so events
  committed during the outage are still delivered. Every failed cycle is
  reported via `WithSubscriptionErrorHandler`; the new
  `WithSubscriptionMaxReconnects` option optionally bounds consecutive failed
  attempts (default: retry forever). (#61)
- **postgres, mysql**: `FindByIDAsOf` now applies the W7 snapshot-consistency
  guard from core's `TemporalQueryService`: a snapshot whose version exceeds
  the highest event version within the `asOf` horizon (possible with a
  skewed/injected clock, since snapshots are selected by `CreatedAt` while
  events are bounded by `OccurredAt`) is discarded in favor of a full replay,
  so post-`asOf` state can no longer leak into temporal reconstructions. (#62)
- **postgres, mysql**: contract projectors now run every event through the
  core contract upcaster chain (`contract.NewContractUpcasterChain`) before
  projecting, matching aggregate rehydration. Legacy payloads (e.g. a v1
  `contract.created` carrying `billing_cycle`, or a v1 `price.changed`
  without `new_price_id`) are migrated once by core's upcasters instead of
  requiring hand-rolled defensive projector code, and
  `contract_read_models.data` stores the upcasted shape. (#63)
- **mysql**: `Subscribe` poll failures are no longer silently swallowed. Each
  failed `LoadAll` poll is reported via the new `WithSubscriptionErrorHandler`
  option (no debounce: during a DB outage the handler fires at the
  `WithPollInterval` cadence, mirroring the postgres adapter's per-failed-cycle
  reporting). Context cancellation is a normal shutdown and is not reported.
  Without the option, behavior is unchanged (failures are still dropped and
  the loop keeps polling). (#80)

### Changed

- **postgres, mysql**: `Append` now validates that each event's caller-stamped
  `Version` is contiguous with `expectedVersion` (event `i` must equal
  `expectedVersion+i+1`) and rejects the whole batch with a
  `validation_error` `DomainError` on a stale version, mid-batch gap, or
  unstamped (zero) version — matching the in-memory reference store instead
  of silently renumbering server-side. Callers that let core's
  `BaseAggregate.RaiseEvent` stamp versions (including this module's own
  repositories) are unaffected. (#64)

### Documentation

- **postgres, mysql**: annotated the read-model money column
  (`invoice_read_models.total`) as a floor-truncated whole-currency-unit
  approximation for query/display only — exact `big.Rat` amounts live in the
  `data` JSON / event payloads. Documented the microsecond timestamp
  round-trip (core stamps nanoseconds; `TIMESTAMPTZ` / `DATETIME(6)` store
  microseconds) and the `RecordedAt` provenance divergence (postgres uses the
  DB's `NOW()` default; mysql stamps from the injected clock) in the
  event-store godocs. (#64)

## [0.2.0] - 2026-07-14

### Changed

- Bump `github.com/contract-to-cash/core` `v0.4.0` → **`v0.7.0`**. Dependency
  update only — no adapter code changes were required, as the intervening core
  releases are additive (Minor). New core surface now available to adapters:
  `PaymentInstructions` on `ChargeResponse` (async / push-charge instructions
  for konbini and bank-transfer virtual accounts) and the non-fatal
  `OnCompensationExecutedHook` (saga charge-reversal observability, core#257).

## [0.1.0] - 2026-07-14

Initial curated release. Establishes the versioning, licensing, and
release-tagging process for the adapters module.

### Added

- `LICENSE` (MIT with a commercial-use attribution clause): commercial use must
  clearly acknowledge that the product or service uses
  `contract-to-cash/adapters`.
- CHANGELOG-driven release workflow (`.github/workflows/release.yml`) that builds,
  lints, and runs the full unit + PostgreSQL/MySQL integration suite before
  tagging, then creates the tag and GitHub Release.
- README **License** and **Releasing** sections documenting the tag-cutting
  operation.

### Adapters in this release

- `postgres` — full persistence stack (event store, repositories, projectors, tx manager).
- `mysql` — MySQL 8.0 persistence stack mirroring `postgres`.
- `fincode` — fincode payment gateway adapter (card / JPY).
- `stripe` — Stripe payment gateway adapter (card + konbini + JP bank transfer + PayPay).

Targets `core` v0.4.0 (see `go.mod`).

[Unreleased]: https://github.com/contract-to-cash/adapters/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/contract-to-cash/adapters/releases/tag/v0.1.0
