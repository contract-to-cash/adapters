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
