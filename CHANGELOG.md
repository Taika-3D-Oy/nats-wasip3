# Changelog

All notable changes to this project will be documented in this file.

## [0.3.1] – 2026-04-13

### Fixed
- Example imports now use the correct crate name `nats_wasip3` (was `nats_wasi`).

### Changed
- Cargo.toml: added `authors`, `homepage`, `documentation`, `rust-version` fields.
- Cargo.toml: fixed `edition` from `"2024"` to `"2021"`.

## [0.3.0] – 2026-04-12

### Added
- JetStream consumer support: `pull_next`, `ack`, `nak`, `consumer_info`.
- NATS KeyValue store: `get`, `put`, `create`, `delete`, `keys`, `watch`.
- NKey authentication (`nkey` feature flag).
- TLS via `wasi:tls` host interface (`tls` feature flag).
- Header support on publish and subscribe.
- Request/reply with timeouts.
- Queue group subscriptions.

## [0.2.0] – 2026-04-08

### Added
- Initial wasip3 port from wasip2 branch.
- Native Component Model async I/O via `wasi:sockets/tcp`.
- Core NATS pub/sub, request/reply.
- JetStream stream management: create, delete, purge, info.
- JetStream publish with ack.

## [0.1.0] – 2026-04-05

### Added
- Initial release — basic NATS client for `wasm32-wasip3`.
