//! # nats-wasi
//!
//! A NATS client for `wasm32-wasip3`. Provides core pub/sub, JetStream, and
//! KV with CAS — using native WASI P3 Component Model async I/O.
//!
//! ## Feature flags
//!
//! - **`tls`** — TLS via `wasi:tls` host interface (requires wasmtime `-S tls=y`;
//!   not default — the `wasi:tls` spec is still a draft and custom CA certs
//!   are not yet supported by any host runtime)
//! - **`jetstream`** — JetStream API (stream/consumer management)
//! - **`kv`** (default, implies `jetstream`) — NATS KV with CAS, Watch
//! - **`nkey`** — NKey authentication (Ed25519)

pub mod client;
pub mod proto;

#[cfg(feature = "jetstream")]
pub mod jetstream;

#[cfg(feature = "kv")]
pub mod kv;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "nkey")]
pub mod nkey;

mod error;
pub use error::Error;
