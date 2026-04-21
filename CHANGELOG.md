# Changelog

All notable changes to this project will be documented in this file.

## [0.5.0] – 2026-04-21

### Added
- `Client::flush(timeout)` — enqueues a PING and waits for PONG, confirming
  all prior publishes have been received by the server.
- `Subscription::unsubscribe()` — explicit unsubscribe without needing to drop.
- `Subscription::unsubscribe_after(n)` — ask server to auto-unsubscribe after
  `n` messages.
- `KeyValue::put_with_ttl(key, value, ttl)` — per-message TTL (NATS server 2.11+).
- `KeyValue::create_with_ttl(key, value, ttl)` — CAS create with per-message TTL.
- `KeyValue::update_with_ttl(key, value, revision, ttl)` — CAS update with TTL.
- `KeyValue::status()` — returns a `KvStatus` with message count, bytes, history
  depth, TTL, and last sequence number.
- `KvConfig::allow_msg_ttl` — enables per-message TTL on the backing stream.
- `StreamConfig::allow_msg_ttl` — exposed on `StreamConfig` for JetStream users.
- `ConnectConfig::nkey_seed` (behind `nkey` feature) — NKey authentication is
  now fully wired end-to-end; the server nonce is signed on connect and reconnect.

### Fixed
- `KvConfig::history` was parsed but never sent to the server; buckets always
  got the server default. `max_msgs_per_subject` is now correctly set.
- `Subscription::Drop` did not wake the flush loop, so the UNSUB frame was
  delayed until the next user operation.
- `flush_loop` discarded outbound data on a write error; data is now re-queued
  and retried after reconnection.
- `JetStream::fetch()` swallowed real subscription errors (e.g. `Disconnected`)
  by treating them the same as a timeout.
- `KvWatcher` and `ConsumerMessages` now send a `CONSUMER.DELETE` request on
  drop so ephemeral consumers are cleaned up server-side immediately.
- README quick-start example used the wrong crate name (`nats_wasi`) and an
  incompatible `fn main + block_on` entry-point pattern.
- `ConnectOptions::lang` changed from `"rust-wasi"` to `"nats-wasip3"`.

### Changed
- Reconnect backoff now applies ±25 % jitter to avoid thundering-herd
  reconnection storms.
- `StreamConfig` gains `max_msgs_per_subject` field (zero-value skipped in JSON).

## [0.4.0] – 2026-04-15

### Added
- DNS hostname resolution via WASI `ip-name-lookup` interface. Addresses like
  `nats.example.com:4222` now work in addition to literal IPs.
- `Error::Dns` variant for DNS-specific errors.
- Integration test for DNS hostname connectivity.

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
