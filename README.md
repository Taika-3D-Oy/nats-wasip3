# nats-wasip3

A NATS client for WASI 0.3 / Component Model — core pub/sub, JetStream (including Object
Store), and KV with CAS. Uses **native WASI 0.3 Component Model async I/O** (`wasip3` crate +
`wit-bindgen`) — no wstd shim, no C/FFI dependencies, builds with stable Rust.

## Requirements

- **Rust stable ≥ 1.85** with the `wasm32-wasip2` target (`rustup target add wasm32-wasip2`)
- **Runtime**: [wasmCloud](https://wasmcloud.com) ≥ 2.7.0, or [Wasmtime](https://wasmtime.dev) ≥ 47 (with WASI 0.3 / component-model-async support)
- **nats-server** (for running tests)

The included `rust-toolchain.toml` pins the stable toolchain and `wasm32-wasip2` target automatically. WASI 0.3 Component Model async I/O compiles directly with stable Rust using the `wasip3` (0.7+) and `wit-bindgen` (0.57+) crates.

## Building

```sh
# Library build:
cargo build --lib

# Examples / binaries:
cargo build --example pubsub

# All features including TLS:
cargo build --all-features
```

The `.cargo/config.toml` defaults the build target to `wasm32-wasip2` and configures the stack size.

## Running

You can run components directly using Wasmtime (≥ 47):

```sh
wasmtime run \
    -S inherit-network=y \
    -W component-model-async=y \
    --env NATS_URL=127.0.0.1:4222 \
    target/wasm32-wasip2/debug/examples/pubsub.wasm
```

Key flags:
- `-S inherit-network=y` — give the guest access to the host network
- `-W component-model-async=y` — enable component model async execution
- `-S tls=y` — enable `wasi:tls` host interface (only needed when built with `--features tls`)

## Integration tests

The tests run as a wasm component against a real NATS server.

### 1. Start a NATS server

```sh
nats-server -js -p 14222 &
```

### 2. Build and run

```sh
cargo build --example integration_tests
wasmtime run \
    -S inherit-network=y \
    -W component-model-async=y \
    --env NATS_URL=127.0.0.1:14222 \
    target/wasm32-wasip2/debug/examples/integration_tests.wasm
```

This runs ~38 tests (core pub/sub, JetStream, KV).

### TLS tests (optional, experimental)

TLS uses the `wasi:tls` host interface (`wasi:tls@0.3.0`).

To run TLS tests with a locally-trusted server:

```sh
# Start a TLS NATS server:
nats-server -js -p 4223 \
    --tls --tlscert testdata/server-cert.pem --tlskey testdata/server-key.pem &

# Build with TLS feature:
cargo build --example integration_tests --features tls

# Run with TLS enabled in wasmtime:
wasmtime run \
    -S inherit-network=y -S tls=y \
    -W component-model-async=y \
    --env NATS_URL=127.0.0.1:14222 \
    --env NATS_TLS_URL=127.0.0.1:4223 \
    target/wasm32-wasip2/debug/examples/integration_tests.wasm
```

> **Note:** TLS tests require the server certificate to be trusted by the host TLS provider (e.g. `webpki-roots`).

## Feature flags

| Feature      | Default | Description                              |
|------------- |---------|------------------------------------------|
| `tls`        | no      | TLS via `wasi:tls` host interface (experimental — custom CAs not yet supported) |
| `jetstream`  | no      | JetStream API (stream/consumer/Object Store mgmt) |
| `kv`         | yes     | NATS KV with CAS, Watch (implies `jetstream`) |
| `nkey`       | no      | NKey authentication (Ed25519)            |

## KV watch APIs

The KV API includes nats.rs-style watch variants:

- `watch(start_after_seq)` — existing API for replay/new behavior based on
  sequence.
- `watch_all()` — all keys, new updates only.
- `watch_all_from_revision(revision)` — all keys from a stream revision.
- `watch_all_with_history()` — latest value per key first, then live updates.
- `watch_many(keys)` — selected keys, new updates only.
- `watch_many_with_history(keys)` — selected keys with latest-per-key snapshot,
  then live updates.
- `watch_many_from_revision(keys, revision)` — selected keys from a stream
  revision.

Notes:

- `watch_many*` uses JetStream `filter_subjects` and requires NATS server 2.10+.
  On older servers the library returns a clear compatibility error.
- `load_all()` now uses a consumer snapshot (`DeliverPolicy::LastPerSubject`),
  returning only the latest non-deleted value per key.

## Quick start

```rust
use nats_wasip3::client::{Client, ConnectConfig};

wasip3::cli::command::export!(NatsDemo);

struct NatsDemo;

impl wasip3::exports::cli::run::Guest for NatsDemo {
    async fn run() -> Result<(), ()> {
        run().await.map_err(|e| eprintln!("error: {e}"))
    }
}

async fn run() -> Result<(), nats_wasip3::Error> {
    let client = Client::connect(ConnectConfig {
        address: "nats.example.com:4222".to_string(),
        ..Default::default()
    }).await?;
    let sub = client.subscribe("demo.>")?;
    client.publish("demo.hello", b"world")?;
    let msg = sub.next().await?;
    println!("{}: {}", msg.subject, String::from_utf8_lossy(&msg.payload));
    Ok(())
}
```

DNS hostnames (e.g. `nats.example.com:4222`) and literal IP addresses
(`127.0.0.1:4222`, `[::1]:4222`) are both supported. DNS resolution uses
the WASI `ip-name-lookup` interface — the host runtime resolves the name on
the guest's behalf.

## Architecture — native P3 async

This crate talks directly to WASI P3 Component Model primitives:

- **`wasip3::sockets::types::TcpSocket`** — `create()`, `.connect().await`
  (true CM async, not pollable-based)
- **`StreamReader<u8>` / `StreamWriter<u8>`** — unidirectional byte streams
  from `socket.receive()` / `socket.send()` (each callable once per socket)
- **`wit_bindgen::spawn`** — concurrent background tasks (read loop, flush
  loop)
- **`wit_bindgen::block_on`** — CM async executor for `fn main()`
- **`wasip3::clocks::monotonic_clock::wait_for(nanos)`** — native async
  sleep (used with `futures::select` for timeouts)

The host runtime schedules all async I/O — no userland reactor, no `Poll`
trait adapters.

### Key constraints

- `socket.receive()` and `socket.send()` can each be called **at most once**
  per socket. The returned streams are used for the lifetime of the
  connection.
- Reconnection creates a new socket + new streams, handing the new
  transport to the flush loop via shared state.
- **TLS** (when enabled) uses the host's `wasi:tls` interface — the host
  performs all encryption/decryption with native hardware-accelerated crypto.
  The guest only sees cleartext `StreamReader<u8>` / `StreamWriter<u8>`.

## License

Apache-2.0
