//! # nats-wasip3
//!
//! A NATS client for `wasm32-wasip3`. Provides core pub/sub, JetStream, and
//! KV with CAS — using native WASI P3 Component Model async I/O.
//!
//! ## Quick start
//!
//! ```rust,ignore
//! use nats_wasip3::{Client, ConnectConfig};
//!
//! // Connect, subscribe, publish.
//! let client = Client::connect(ConnectConfig::default()).await?;
//! let sub = client.subscribe("greet.>")?;
//! client.publish("greet.world", b"hello")?;
//! let msg = sub.next().await?;
//! println!("{}: {}", msg.subject, String::from_utf8_lossy(&msg.payload));
//! ```
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
pub(crate) mod proto;

#[cfg(feature = "jetstream")]
pub mod jetstream;

#[cfg(feature = "jetstream")]
pub mod object_store;

#[cfg(feature = "kv")]
pub mod kv;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "nkey")]
pub mod nkey;

mod error;
pub use error::Error;

// ── Top-level re-exports ───────────────────────────────────────────
// Users should import from the crate root rather than sub-modules.

pub use client::{
    Client, ConnectConfig, Duration, Message, Subscription,
    millis, secs, with_timeout,
};
pub use proto::{Headers, ServerInfo};
