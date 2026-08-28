# Changelog

All notable changes to this project will be documented in this file.

## [0.12.0] – 2026-08-28

### Added

- **Microservices Framework ([ADR-32])**:
  - Full implementation of NATS Microservices architecture under `service` feature (enabled by default).
  - Standardized control verbs: `$SRV.PING`, `$SRV.INFO`, `$SRV.STATS`, `$SRV.SCHEMA` with filtering by service name and ID.
  - Hierarchical routing groups (`Group`) with automatic subject prefixing and queue group inheritance.
  - Per-endpoint latency, error, and request counting with thread-safe atomic stats counters.
  - Support for custom stats, schema URLs/JSON definitions, and typed response builders.
  - Added `examples/microservice.rs` demonstrating microservice setup, endpoint handlers, and service discovery.
- **JetStream Feature Parity**:
  - `JetStream::direct_get_last_for_subject` and `JetStream::direct_get` for zero-overhead direct message lookups without creating consumers.
  - `JetStream::update_stream` with stream config mutation support (e.g. sealing streams).
  - `OrderedConsumer`: Ordered push consumer with automatic heartbeat tracking, flow control response, and transparent ephemeral consumer recreation upon sequence gap detection.
  - `MsgMetadata`: Parser for delivery reply-to subjects supporting both v1 (9-token) and v2 (12-token) formats.
  - `JsMessage::nak_with_delay(Duration)` for delayed redelivery backoff.
  - `JsMessage::term_with_reason(&str)` for terminating message redelivery with explicit reason metadata.
- **KV Store Enhancements**:
  - `KvStore::keys_matching(&str)` for wildcard-filtered key discovery.
- **Object Store Enhancements**:
  - `ObjectStore::seal()` to permanently make an object store bucket read-only.
  - `ObjectStore::watch()` returning `ObjectWatcher` for real-time object mutations.
  - `ObjectStore::link()` and `ObjectStore::link_bucket()` supporting cross-bucket and multi-object links via `ObjectLink`.

## [0.11.5] – 2026-08-28

### Changed

- **Multiplexed Request-Reply Inbox**: Requests now share a single multiplexed inbox subscription (`_INBOX.<client_id>.*`) established at connection time instead of creating dynamic `SUB`/`UNSUB` subscriptions per request. This reduces wire protocol traffic per request by 66% (from 3 frames to 1 frame).
- **RAII Request Future Cleanup**: `RequestFuture` now implements `Drop` to automatically remove pending request routing slots on completion, timeout, or cancellation, preventing memory leaks in high-throughput workloads.
- **Concurrent `flush()` Tracking**: Replaced single `pong_waker` with cumulative PONG counter and waker queue, allowing multiple async tasks to call `flush()` concurrently without waker overwrites or deadlocks.

### Fixed

- **Host Socket Lifecycle**: Preserved active `TcpSocket` in client state and dropped it explicitly on `Client::close` / `Client::drop` to prevent host file descriptor leaks.
- **Background Future Draining**: Replaced `std::mem::forget` on send and receive futures with explicit background drains via `wit_bindgen::spawn`.
- **Zero-Copy Outbound Streaming & Disconnect Requeueing**: `stream_write_vec` avoids cloning data vectors, and `flush_loop` preserves unwritten bytes upon disconnect, requeueing them automatically so outbound data is never lost during reconnects.
- **Zero-Allocation TLS Forwarding**: `forward_stream` in `tls.rs` now reuses a single 16 KB buffer across iterations instead of allocating per loop.

### Added

- **Unit tests**: Added tests for multiplexed inbox dispatch, RAII cancellation slot cleanup, and concurrent PONG sequence tracking.

## [0.11.4] – 2026-08-24

### Security

- **Header injection prevention**: `Headers::insert` now strips `\r` and `\n`
  characters from both keys and values before storing them. A key that becomes
  empty after stripping is silently dropped. This prevents crafted user input
  from injecting extra NATS header lines into an `HPUB` frame.

### Fixed

- **Randomised inbox subjects**: `Client::new_inbox()` now generates
  `_INBOX.<32-hex-chars>` using a Xorshift64 PRNG seeded from the monotonic
  clock at connection time. Previously inboxes were sequential integers
  (`_INBOX.1`, `_INBOX.2`, …), which allowed any subscriber on the same server
  to predict and intercept request/reply traffic via `_INBOX.>`.

- **True jitter for reconnect backoff**: The ±25% jitter applied during
  reconnection now uses the same Xorshift64 PRNG seeded from the connection
  clock. Previously a deterministic `wrapping_mul` hash was used, meaning all
  clients that connected at the same time produced identical backoff sequences,
  defeating the thundering-herd prevention.

- **Eliminate per-read allocation in `stream_read`**: The internal
  `stream_read` helper previously allocated a fresh `Vec::with_capacity(8192)`
  on every call. The read buffer is now threaded through callers in tight loops
  (`read_loop`, `attempt_reconnect`) so the allocation is made once and reused
  across reads.

- **Dead code warning in `object_store`**: The local `format_rfc3339` wrapper
  was only called from unit tests; it is now conditionally compiled with
  `#[cfg(test)]`.

### Added

- **CI workflow** (`.github/workflows/ci.yml`): Two new jobs run automatically
  on every push and pull request:
  - `test` — runs `cargo test --lib` on the native host (stable Rust,
    `x86_64-unknown-linux-gnu`) covering protocol, nkey, kv, and client unit
    tests without needing wasmtime.
  - `check` — runs `cargo check --all-features` and `cargo check --examples` targeting
    `wasm32-wasip2` using stable Rust to catch regressions against the primary target.

- **New unit tests** for all fixes:
  - `proto::tests::headers_inject_crlf_stripped_from_value` — verifies that a
    value with embedded `\r\n` cannot inject a new header line.
  - `proto::tests::headers_inject_crlf_stripped_from_key` — verifies key
    sanitisation.
  - `proto::tests::headers_empty_key_after_strip_is_dropped` — verifies that a
    key reduced to empty by stripping is not stored.
  - `client::tests::xorshift64_never_repeats_in_short_run` — Xorshift64 PRNG
    property test.
  - `client::tests::xorshift64_nonzero_seed_stays_nonzero` — verifies the PRNG
    never outputs 0 from a valid seed.
  - `client::tests::inbox_format_is_32_hex_chars` — verifies inbox subject
    format.

## [0.11.3] – 2026-08-24

### Changed

- **Stable Rust & WASI 0.3 Build Support**:
  - Migrated build toolchain from nightly `wasm32-wasip3` to stable Rust targeting `wasm32-wasip2`.
  - Removed nightly Tier 3 sysroot copy workarounds.
  - Standardized runtime requirement to Wasmtime ≥ 47 / wasmCloud ≥ 2.7.0.

### Fixed

- **JetStream `fetch()` Pull Batch Completion**:
  - Inspected `Nats-Pending-Messages: 0` / `Nats-Num-Pending: 0` / `Nats-Pending: 0` headers on delivered messages in pull batches (`no_wait: true`) to complete fetches immediately instead of waiting for full API timeout.
  - Reduced subsequent message wait timeout to 100ms once initial messages in a `no_wait` batch have arrived.

## [0.11.2] – 2026-08-22

### Changed

- **wasmCloud 2.7.0 & WASI 0.3.0 Compatibility**:
  - Upgraded to `wasip3 0.7` and `wit-bindgen 0.57`.
  - Prevented socket resource drop traps by managing lifecycle of child stream futures.
  - Aligned clock bindings with standard WASI `system-clock` interfaces.

## [0.11.1] – 2026-05-05

### Fixed

- **Linker duplicate symbol with `build-std`**: Added `--allow-multiple-definition`
  to `rustflags` in `.cargo/config.toml`. When building `cdylib` components with
  `-Zbuild-std`, the `wasm32-wasip3` std and user code each compile their own copy
  of `wit-bindgen`'s `cabi_realloc` static lib, causing a linker conflict. The flag
  tells `wasm-ld` to accept the first definition (both are identical). Fixes binary
  example builds (`cargo build --example pubsub`).

## [0.11.0] – 2026-05-04

### Added

- **Message scheduling support (NATS 2.14+, [ADR-51])** — new `nats_wasip3::schedule`
  module (behind the existing `jetstream` feature flag):
  - `Schedule` enum — `At(rfc3339)`, `Every(interval)`, `Cron(expr)`,
    `Hourly`, `Daily`, `Weekly`, `Monthly`, `Yearly`; each variant's
    `to_header_value()` returns the correct `Nats-Schedule` header value.
  - `ScheduleSpec` — builder struct with `schedule`, `target`, `source`,
    `ttl`, `time_zone`, and `rollup` fields. `to_headers()` returns a
    ready-to-publish `Headers` map.
  - `now_rfc3339() -> String` — current wall-clock time as RFC 3339 UTC,
    via `wasi:clocks/system-clock`.
  - `after_secs_rfc3339(delta_secs: u64) -> String` — wall-clock time
    `delta_secs` seconds from now; convenient for one-shot delayed publishes.
  - `format_rfc3339(unix_secs: u64, nanoseconds: u32) -> String` — public
    RFC 3339 formatter for custom timestamps.
  - All 8 schedule header name constants (`HEADER_SCHEDULE`,
    `HEADER_SCHEDULE_TARGET`, `HEADER_SCHEDULE_SOURCE`,
    `HEADER_SCHEDULE_TTL`, `HEADER_SCHEDULE_TIME_ZONE`,
    `HEADER_SCHEDULE_ROLLUP`, `HEADER_SCHEDULER`, `HEADER_SCHEDULE_NEXT`).

- **`StreamConfig::allow_msg_schedules: bool`** — new field wired to the
  `allow_msg_schedules` JSON key required to enable message scheduling on a
  stream. Serialized with `omitempty`; defaults to `false`.

[ADR-51]: https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-51.md

## [0.10.0] – 2026-05-04

### Added

- **`Error::MaxPayloadExceeded { size, max }`** — `publish`, `publish_with_reply`,
  and `publish_with_headers` now check the payload length against the server's
  advertised `max_payload` limit before writing to the outbound buffer. A
  payload that exceeds the limit is rejected immediately with
  `Error::MaxPayloadExceeded` rather than being sent and causing the server to
  close the connection with `-ERR 'Maximum Payload Violation'`.

- **`ConnectConfig::no_echo: bool`** (default `false`) — when set to `true`
  the NATS server will not echo messages published by this client back to its
  own subscriptions. Wired through to the `echo` field in the `CONNECT`
  payload. Previously `echo` was always sent as `true`.

- **`StreamConfig` sourcing, mirroring and routing fields**:
  - `mirror: Option<StreamSource>` — mirror another stream into this one.
  - `sources: Option<Vec<StreamSource>>` — fan-in from multiple source streams.
  - `republish: Option<Republish>` — re-publish matching messages to a second
    subject without moving them.
  - `subject_transform: Option<SubjectTransform>` — apply a single
    subject-mapping rule to all messages stored on the stream.
  - `placement: Option<Placement>` — direct the server to place the stream
    leader in a specific cluster or on nodes with specific tags.

- **New supporting types** (all in `nats_wasip3::jetstream`):
  - `StreamSource` — source reference used by `mirror` / `sources`, with
    `opt_start_seq`, `opt_start_time`, `filter_subject`,
    `subject_transforms`, and `external` fields.
  - `ExternalStream` — API/deliver prefix pair for leaf-node / account-import
    topologies.
  - `Republish` — `src` pattern, `dest` template, and `headers_only` flag.
  - `SubjectTransform` — `src` filter and `dest` template (uses `{{wildcard(N)}}`
    tokens as per the NATS server spec).
  - `Placement` — `cluster` name and `tags` list.

### Internal

- Existing `StreamConfig` literals in `kv.rs` and `object_store.rs` updated to
  use `..Default::default()` for forward-compatibility with the new optional
  fields.

## [0.9.1] – 2026-04-30

### Fixed

- **KV — watcher/load_all/history revision extraction**: Consumer-delivered
  messages (push watchers, pull fetch) carry the stream sequence in the
  JetStream reply-to subject (`$JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>...`),
  not in the `Nats-Sequence` header (which only exists in `$JS.API.DIRECT.GET`
  responses). Previously all three code paths — `KvWatcher::next()`,
  `load_all()`, and `history()` — extracted revision from the header alone,
  yielding `0` for every watcher/fetch message. This caused downstream
  consumers (e.g. lattice-db replicas) to never advance their applied-revision
  watermark, breaking cross-replica consistency.
- Added `extract_revision(msg)` helper that checks `Nats-Sequence` first, then
  falls back to parsing the `$JS.ACK` reply-to subject (index 5).

## [0.9.0] – 2026-04-30

### Added

**KV — per-key watch APIs (nats.rs alignment)**
- `KeyValue::watch(key)` — watch a single key; delivers the current value first
  (`DeliverPolicy::LastPerSubject`), then streams all future updates.
  **Breaking:** replaces the old `watch(start_after_seq: u64)` signature.
- `KeyValue::watch_with_history(key)` — watch a single key replaying its full
  history first (`DeliverPolicy::All`), then live updates.
- `KeyValue::watch_from_revision(key, revision)` — watch a single key starting
  from a specific stream revision.
- `KeyValue::entry(key)` — like `get` but returns tombstone entries
  (`Operation::Delete` / `Operation::Purge`) instead of `None`.
- `KeyValue::history(key)` — return all historical revisions for a key
  (requires `history > 1` on the bucket).
- `KeyValue::delete_expect_revision(key, revision)` — canonical nats.rs name
  for compare-and-swap delete.
- `KeyValue::stream_name()` — expose the backing JetStream stream name.

### Changed

- `KeyValue::cas_delete` is now `#[deprecated]`; use `delete_expect_revision`.
- Bucket-wide watcher helpers (`watch_all`, `watch_all_with_history`,
  `watch_all_from_revision`) refactored to share an internal
  `create_watcher_subject` helper, also used by the new per-key methods.

### Breaking

- `KeyValue::watch(start_after_seq: u64)` → `KeyValue::watch(key: impl AsRef<str>)`.
  Migrate: `watch(0)` → `watch_all_with_history()` or `watch_all()`;
  `watch(rev)` → `watch_all_from_revision(rev)`.

## [0.8.2] – 2026-04-30

### Added

**KV — nats.rs-aligned watch APIs**
- `KeyValue::watch_all()` — watch all keys for new updates only.
- `KeyValue::watch_all_from_revision(revision)` — watch all keys starting from a
  stream sequence.
- `KeyValue::watch_all_with_history()` — emit latest value per key first, then
  continue with live updates (`DeliverPolicy::LastPerSubject`).
- `KeyValue::watch_many(keys)` — watch a selected set of keys for new updates.
- `KeyValue::watch_many_with_history(keys)` — selected keys with
  latest-per-key snapshot first, then live updates.
- `KeyValue::watch_many_from_revision(keys, revision)` — selected keys from a
  specific stream sequence.

**JetStream — consumer multi-filter support**
- `ConsumerConfig::filter_subjects: Option<Vec<String>>` added to support
  multi-key KV watchers.

### Changed

**KV — load_all implementation**
- `KeyValue::load_all()` now uses an ephemeral
  `DeliverPolicy::LastPerSubject` consumer snapshot instead of per-sequence
  `STREAM.MSG.GET` scanning, reducing request volume on large/history-heavy
  buckets.

### Fixed

**KV — watch_many server compatibility error message**
- On servers without `filter_subjects` support (pre-2.10), `watch_many*`
  now returns a clear compatibility error:
  `watch_many requires NATS server 2.10+ with consumer filter_subjects support`.

## [0.8.1] – 2026-04-29

### Added

**KV — compare-and-swap delete**
- `KeyValue::cas_delete(key, expected_revision)` — tombstones a key only if the
  current revision matches; returns `Err(Error::RevisionMismatch)` otherwise.

**KV — CAS and TTL purge variants**
- `KeyValue::purge_with_ttl(key, ttl)` — purge all revisions, leaving a tombstone
  that itself expires after `ttl`.
- `KeyValue::purge_expect_revision(key, expected_revision)` — purge only if the
  current revision matches.
- `KeyValue::purge_expect_revision_with_ttl(key, expected_revision, ttl)` — CAS
  purge with an expiring tombstone.

**KV — revision-scoped entry lookup**
- `KeyValue::entry_for_revision(key, revision)` — fetch the entry at a specific
  stream sequence, including `Delete` and `Purge` tombstones. Useful for history
  inspection without a full streaming iterator.

## [0.8.0] – 2026-04-22

### Added

**Server-side message timestamps**
- `StreamMessage::time: Option<String>` — RFC 3339 publish timestamp returned by
  the server in the `STREAM.MSG.GET` JSON response.
- `JsMessage::timestamp() -> Option<&str>` — RFC 3339 publish timestamp from the
  `Nats-Time-Stamp` header present on every JetStream push-consumer delivery.
- `JsMessage::timestamp_nanos() -> Option<u64>` — nanoseconds-since-Unix-epoch
  parsed directly from the JetStream ACK reply-to subject (supports both the
  9-token v1 and 12-token v2 ACK subject formats).
- `Entry::time: Option<String>` — RFC 3339 publish timestamp exposed on KV
  entries. Populated from `Nats-Time-Stamp` headers in `get` and `watch`, and
  from `StreamMessage::time` in `load_all`.

**Object Store `mtime` format fix**
- `ObjectInfo::mtime` is now written as RFC 3339
  (e.g. `"2024-04-22T00:00:00.123456789Z"`) instead of raw Unix nanoseconds.
  This matches the NATS object store specification and ensures interoperability
  with nats.go and other client libraries.

### Breaking
- `ObjectInfo::mtime` format changed from raw nanosecond integer string to RFC
  3339. Objects stored by ≤ 0.7.0 carry the old format; re-`put` them to
  upgrade, or parse defensively if you read buckets written by mixed versions.

## [0.7.0] – 2026-04-21

### Added

**Authentication**
- `ConnectConfig::jwt` — bare JWT for NATS 2.0 operator authentication.
- `ConnectConfig::credentials` (`nkey` feature) — parse a standard NATS `.creds`
  file (JWT + NKey seed blocks); overrides `jwt` / `nkey_seed`.

**Multi-server & cluster failover**
- `ConnectConfig::servers: Vec<String>` — additional server addresses tried on
  connect and on every reconnect attempt, round-robined with `address`.
- Cluster node URLs from server `INFO.connect_urls` are stored and added to the
  reconnect candidate pool automatically (live topology discovery).
- TLS SNI is now derived from the current candidate address rather than always
  using `config.address`.

**Slow-consumer protection**
- `ConnectConfig::subscription_capacity: usize` (default 512) — maximum pending
  messages per subscription mailbox. When the mailbox is full the oldest message
  is dropped (drop-head policy, matching the Go client).

**Write-buffer back-pressure**
- `ConnectConfig::max_pending_write_bytes: usize` (default 8 MiB) — publish/
  subscribe calls return `Err(Error::BufferFull)` when the outbound buffer
  exceeds this limit.
- `Error::BufferFull` variant added.

**Object Store**
- SHA-256 digest computed on `put`; stored as `"SHA-256=<base64url-no-pad>"`
  in object metadata per the NATS spec. Verified on `get` — a mismatch returns
  `Err(Error::Protocol("object digest mismatch …"))`.
- `ObjectInfo::digest: Option<String>` exposed publicly.

**API**
- `Client`, `ConnectConfig`, `Headers`, `ServerInfo`, `Message`, `Subscription`,
  `millis`, `secs`, `with_timeout` re-exported from the crate root — no more
  `use nats_wasip3::client::…` or `use nats_wasip3::proto::…` required.
- `proto` module is now `pub(crate)` — internal wire types (`ServerOp`, `Msg`,
  `HMsg`, `ConnectOptions`) are no longer part of the public API.
- `#[non_exhaustive]` added to all output/receive types (`Error`, `Message`,
  `StreamInfo`, `StreamState`, `PubAck`, `ConsumerInfo`, `PurgeResponse`,
  `KvStatus`, `Entry`, `Object`, `ObjectInfo`, `ObjectStoreStatus`) and all
  server-facing enums (`Storage`, `Retention`, `DiscardPolicy`, `DeliverPolicy`,
  `ReplayPolicy`, `AckPolicy`, `Operation`).
- `[package.metadata.docs.rs]` added: `all-features = true` for docs.rs builds.

**Reconnect**
- Permanent server auth errors (`Authorization Violation`, `Authentication
  Expired/Revoked`) now abort reconnection immediately instead of exhausting the
  full retry budget.

### Breaking
- `nats_wasip3::proto` is no longer public. Replace:
  - `use nats_wasip3::proto::Headers` → `use nats_wasip3::Headers`
  - `use nats_wasip3::client::{Client, ConnectConfig, …}` → `use nats_wasip3::{Client, ConnectConfig, …}`
- All output types and enums are now `#[non_exhaustive]`. Exhaustive `match`
  arms over `Error`, `AckPolicy`, `Operation`, etc. must add a wildcard arm.

## [0.6.0] – 2026-04-21

### Added
- JetStream Object Store (`object_store` module, behind `jetstream` feature):
  `ObjectStore::new` / `open`, `put`, `put_with_chunk_size`, `get`, `info`,
  `list`, `delete`, `status`. Objects are chunked (default 128 KiB) and backed
  by an `OBJ_*` JetStream stream.
- `JetStream::purge_stream_subject(stream, subject)` — subject-filtered stream
  purge, used internally by Object Store and available to all JetStream users.

### Fixed
- `ObjectStore::get` and `ObjectStore::list` leaked ephemeral pull consumers on
  the server; they now send a fire-and-forget `CONSUMER.DELETE` after the fetch.

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
