use std::fmt;

use wasip3::sockets::types::ErrorCode;
use wasip3::sockets::ip_name_lookup::ErrorCode as DnsErrorCode;

/// Errors returned by nats-wasi operations.
#[derive(Debug)]
pub enum Error {
    /// I/O error from the standard library.
    Io(std::io::Error),
    /// WASI P3 socket error.
    Socket(ErrorCode),
    /// DNS resolution error.
    Dns(DnsErrorCode),
    /// Server returned -ERR.
    Server(String),
    /// Protocol parse error.
    Protocol(String),
    /// Request timed out waiting for a reply.
    Timeout,
    /// No subscribers available for the requested subject.
    NoResponders,
    /// Connection is closed.
    Disconnected,
    /// JSON serialization/deserialization error.
    Json(String),
    /// JetStream API error.
    #[cfg(feature = "jetstream")]
    JetStream { code: u16, description: String },
    /// KV-specific errors.
    #[cfg(feature = "kv")]
    RevisionMismatch,
    #[cfg(feature = "kv")]
    KeyNotFound,
    #[cfg(feature = "kv")]
    KeyExists,
    /// TLS handshake or configuration error.
    #[cfg(feature = "tls")]
    Tls(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "io: {e}"),
            Error::Socket(e) => write!(f, "socket: {e:?}"),
            Error::Dns(e) => write!(f, "dns: {e:?}"),
            Error::Server(msg) => write!(f, "server: {msg}"),
            Error::Protocol(msg) => write!(f, "protocol: {msg}"),
            Error::Timeout => write!(f, "timeout"),
            Error::NoResponders => write!(f, "no responders"),
            Error::Disconnected => write!(f, "disconnected"),
            Error::Json(msg) => write!(f, "json: {msg}"),
            #[cfg(feature = "jetstream")]
            Error::JetStream { code, description } => {
                write!(f, "jetstream {code}: {description}")
            }
            #[cfg(feature = "kv")]
            Error::RevisionMismatch => write!(f, "revision mismatch"),
            #[cfg(feature = "kv")]
            Error::KeyNotFound => write!(f, "key not found"),
            #[cfg(feature = "kv")]
            Error::KeyExists => write!(f, "key already exists"),
            #[cfg(feature = "tls")]
            Error::Tls(msg) => write!(f, "tls: {msg}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<ErrorCode> for Error {
    fn from(e: ErrorCode) -> Self {
        Error::Socket(e)
    }
}

impl From<DnsErrorCode> for Error {
    fn from(e: DnsErrorCode) -> Self {
        Error::Dns(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Json(e.to_string())
    }
}
