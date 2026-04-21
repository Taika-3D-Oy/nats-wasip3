//! NATS KV — a key-value store built on JetStream.
//!
//! KV buckets are JetStream streams with subject `KV.{bucket}.{key}`.
//! CAS (compare-and-swap) uses the `Nats-Expected-Last-Subject-Sequence` header.
//! Watch uses an ordered ephemeral consumer.

use crate::client::{Duration, secs};
use crate::jetstream::{
    AckPolicy, ConsumerConfig, DeliverPolicy, DiscardPolicy, JetStream, Retention, Storage,
    StreamConfig,
};
use crate::proto::Headers;
use crate::Error;

const KV_STREAM_PREFIX: &str = "KV_";
const KV_SUBJECT_PREFIX: &str = "$KV";

// ── KV Store ───────────────────────────────────────────────────────

/// A NATS KV bucket.
#[derive(Clone)]
pub struct KeyValue {
    js: JetStream,
    bucket: String,
    stream_name: String,
}

/// A KV entry.
#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub value: Vec<u8>,
    pub revision: u64,
    pub operation: Operation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Put,
    Delete,
    Purge,
}

/// Configuration for creating a KV bucket.
#[derive(Debug, Clone)]
pub struct KvConfig {
    pub bucket: String,
    pub history: u8,
    pub max_value_size: i32,
    pub max_bytes: i64,
    /// Bucket-wide time-to-live in nanoseconds. Applies to every entry.
    pub ttl: Option<Duration>,
    /// Allow per-message TTL via [`KeyValue::put_with_ttl`] etc.
    /// Requires NATS server 2.11+.
    pub allow_msg_ttl: bool,
    pub storage: Storage,
    pub num_replicas: u32,
}

impl Default for KvConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            history: 1,
            max_value_size: -1,
            max_bytes: -1,
            ttl: None,
            allow_msg_ttl: false,
            storage: Storage::File,
            num_replicas: 1,
        }
    }
}

impl KeyValue {
    /// Open or create a KV bucket.
    pub async fn new(js: JetStream, config: KvConfig) -> Result<Self, Error> {
        let stream_name = format!("{KV_STREAM_PREFIX}{}", config.bucket);

        let stream_config = StreamConfig {
            name: stream_name.clone(),
            subjects: vec![format!("{KV_SUBJECT_PREFIX}.{}.>", config.bucket)],
            retention: Retention::Limits,
            max_consumers: -1,
            max_msgs: -1,
            max_bytes: config.max_bytes,
            max_msg_size: config.max_value_size,
            max_msgs_per_subject: config.history as i64,
            storage: config.storage,
            num_replicas: config.num_replicas,
            discard: DiscardPolicy::New,
            max_age: config.ttl,
            duplicate_window: None,
            allow_direct: true,
            allow_rollup_hdrs: true,
            allow_msg_ttl: config.allow_msg_ttl,
        };

        js.create_stream(&stream_config).await?;

        Ok(KeyValue {
            js,
            bucket: config.bucket,
            stream_name,
        })
    }

    /// Open an existing KV bucket (does not create it).
    pub fn open(js: JetStream, bucket: impl Into<String>) -> Self {
        let bucket = bucket.into();
        let stream_name = format!("{KV_STREAM_PREFIX}{bucket}");
        KeyValue {
            js,
            bucket,
            stream_name,
        }
    }

    fn subject(&self, key: &str) -> String {
        format!("{KV_SUBJECT_PREFIX}.{}.{key}", self.bucket)
    }

    /// Get a key's value. Returns `None` if the key doesn't exist or is deleted.
    pub async fn get(&self, key: &str) -> Result<Option<Entry>, Error> {
        let subject = self.subject(key);
        // Use direct get for single-key lookups.
        let direct_subject = format!("$JS.API.DIRECT.GET.{}.{}", self.stream_name, subject);
        let reply = match self
            .js
            .client()
            .request(&direct_subject, b"", secs(5))
            .await
        {
            Ok(msg) => msg,
            Err(Error::Timeout) => return Ok(None),
            Err(e) => return Err(e),
        };

        // Check for 404 status in headers.
        if let Some(ref headers) = reply.headers {
            if headers.status == Some(404) {
                return Ok(None);
            }
        }

        let revision = reply
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Sequence"))
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let operation = match reply.headers.as_ref().and_then(|h| h.get("KV-Operation")) {
            Some("DEL") => Operation::Delete,
            Some("PURGE") => Operation::Purge,
            _ => Operation::Put,
        };

        if operation != Operation::Put {
            return Ok(None);
        }

        Ok(Some(Entry {
            key: key.to_string(),
            value: reply.payload,
            revision,
            operation,
        }))
    }

    /// Put a value (unconditional write). Returns the revision.
    pub async fn put(&self, key: &str, value: &[u8]) -> Result<u64, Error> {
        let subject = self.subject(key);
        let ack = self.js.publish(&subject, value).await?;
        Ok(ack.seq)
    }

    /// Put a value with a per-message TTL. Returns the revision.
    ///
    /// Requires the bucket to have been created with `allow_msg_ttl: true`
    /// (NATS server 2.11+). The TTL is rounded down to whole seconds, with
    /// a minimum effective value of 1 second.
    pub async fn put_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<u64, Error> {
        let mut headers = Headers::new();
        headers.insert("Nats-TTL", format_ttl(ttl));
        let subject = self.subject(key);
        let ack = self
            .js
            .publish_with_headers(&subject, &headers, value)
            .await?;
        Ok(ack.seq)
    }

    /// Create a key only if it doesn't exist. Returns the revision.
    pub async fn create(&self, key: &str, value: &[u8]) -> Result<u64, Error> {
        let mut headers = Headers::new();
        headers.insert("Nats-Expected-Last-Subject-Sequence", "0");
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, value)
            .await
        {
            Ok(ack) => Ok(ack.seq),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::KeyExists),
            Err(e) => Err(e),
        }
    }

    /// Create a key only if it doesn't exist, with a per-message TTL.
    /// See [`KeyValue::put_with_ttl`] for TTL semantics.
    pub async fn create_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        ttl: Duration,
    ) -> Result<u64, Error> {
        let mut headers = Headers::new();
        headers.insert("Nats-Expected-Last-Subject-Sequence", "0");
        headers.insert("Nats-TTL", format_ttl(ttl));
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, value)
            .await
        {
            Ok(ack) => Ok(ack.seq),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::KeyExists),
            Err(e) => Err(e),
        }
    }

    /// Compare-and-swap: update only if the current revision matches.
    pub async fn update(
        &self,
        key: &str,
        value: &[u8],
        expected_revision: u64,
    ) -> Result<u64, Error> {
        let mut headers = Headers::new();
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            expected_revision.to_string(),
        );
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, value)
            .await
        {
            Ok(ack) => Ok(ack.seq),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::RevisionMismatch),
            Err(e) => Err(e),
        }
    }

    /// Compare-and-swap update with a per-message TTL.
    /// See [`KeyValue::put_with_ttl`] for TTL semantics.
    pub async fn update_with_ttl(
        &self,
        key: &str,
        value: &[u8],
        expected_revision: u64,
        ttl: Duration,
    ) -> Result<u64, Error> {
        let mut headers = Headers::new();
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            expected_revision.to_string(),
        );
        headers.insert("Nats-TTL", format_ttl(ttl));
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, value)
            .await
        {
            Ok(ack) => Ok(ack.seq),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::RevisionMismatch),
            Err(e) => Err(e),
        }
    }

    /// Delete a key (tombstone).
    pub async fn delete(&self, key: &str) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "DEL");
        let subject = self.subject(key);
        self.js
            .publish_with_headers(&subject, &headers, b"")
            .await?;
        Ok(())
    }

    /// Purge a key (remove all revisions).
    pub async fn purge(&self, key: &str) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "PURGE");
        headers.insert("Nats-Rollup", "sub");
        let subject = self.subject(key);
        self.js
            .publish_with_headers(&subject, &headers, b"")
            .await?;
        Ok(())
    }

    /// List all keys in the bucket.
    pub async fn keys(&self) -> Result<Vec<String>, Error> {
        let subject = format!("{KV_SUBJECT_PREFIX}.{}.>", self.bucket);
        // Create an ephemeral ordered consumer to iterate subjects.
        let consumer_cfg = ConsumerConfig {
            durable_name: None,
            filter_subject: Some(subject),
            deliver_policy: DeliverPolicy::LastPerSubject,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            ..Default::default()
        };

        let info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await?;

        let msgs = self.js.fetch(&self.stream_name, &info.name, 1000).await?;

        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);
        let mut keys = Vec::new();
        for msg in msgs {
            if let Some(key) = msg.subject.strip_prefix(&prefix) {
                // Skip deleted/purged entries.
                let is_deleted = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("KV-Operation"))
                    .is_some();
                if !is_deleted {
                    keys.push(key.to_string());
                }
            }
        }

        Ok(keys)
    }

    /// Bulk-load all live entries from the bucket.
    /// Returns the latest value for each non-deleted key.
    pub async fn load_all(&self) -> Result<Vec<Entry>, Error> {
        let info = match self.js.stream_info(&self.stream_name).await {
            Ok(info) => info,
            Err(Error::JetStream { code: 404, .. }) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        if info.state.messages == 0 {
            return Ok(Vec::new());
        }

        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);
        let first = info.state.first_seq;
        let last = info.state.last_seq;

        // Track latest entry per key: (seq, data, is_deleted)
        let mut latest: std::collections::HashMap<String, (u64, Vec<u8>, bool)> =
            std::collections::HashMap::new();

        for seq in first..=last {
            let msg = match self.js.stream_get_msg(&self.stream_name, seq).await {
                Ok(Some(m)) => m,
                Ok(None) => continue,
                Err(_) => continue,
            };

            let Some(key) = msg.subject.strip_prefix(&prefix) else {
                continue;
            };

            // Check if this is a delete/purge operation by looking for KV-Operation header.
            let is_deleted = msg
                .headers_b64
                .as_ref()
                .and_then(|h| crate::jetstream::base64_decode(h).ok())
                .is_some_and(|bytes| bytes.windows(12).any(|w| w == b"KV-Operation"));

            latest.insert(key.to_string(), (msg.seq, msg.data, is_deleted));
        }

        Ok(latest
            .into_iter()
            .filter(|(_, (_, _, deleted))| !*deleted)
            .map(|(key, (revision, value, _))| Entry {
                key,
                value,
                revision,
                operation: Operation::Put,
            })
            .collect())
    }

    /// Access the underlying JetStream context.
    pub fn jetstream(&self) -> &JetStream {
        &self.js
    }

    /// The bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Watch for live changes to this KV bucket.
    ///
    /// Creates an ordered push consumer starting after `last_seq` (use 0 to
    /// get all history, or the stream's `last_seq` to only get future changes).
    /// Returns a `KvWatcher` that yields `Entry` items as they arrive.
    pub async fn watch(&self, start_after_seq: u64) -> Result<KvWatcher, Error> {
        // Unique deliver subject for this watcher.
        let deliver = format!("_kv_watch.{}.{}", self.bucket, {
            use std::cell::Cell;
            thread_local! {
                static CTR: Cell<u64> = const { Cell::new(0) };
            }
            CTR.with(|c| {
                let v = c.get();
                c.set(v + 1);
                v
            })
        });

        let filter = format!("{KV_SUBJECT_PREFIX}.{}.>", self.bucket);

        let consumer_cfg = ConsumerConfig {
            filter_subject: Some(filter),
            deliver_subject: Some(deliver.clone()),
            deliver_policy: if start_after_seq == 0 {
                DeliverPolicy::All
            } else {
                DeliverPolicy::ByStartSequence
            },
            opt_start_seq: if start_after_seq == 0 {
                None
            } else {
                Some(start_after_seq + 1)
            },
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            replay_policy: Some(crate::jetstream::ReplayPolicy::Instant),
            mem_storage: Some(true),
            ..Default::default()
        };

        let _info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await?;

        // Subscribe to the deliver subject to receive pushed messages.
        let sub = self.js.client().subscribe(&deliver)?;

        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);

        Ok(KvWatcher { sub, prefix })
    }
}

/// A live watcher on a KV bucket. Yields `Entry` items as changes occur.
pub struct KvWatcher {
    sub: crate::client::Subscription,
    prefix: String,
}

impl KvWatcher {
    /// Wait for the next KV change event.
    pub async fn next(&self) -> Result<Entry, Error> {
        loop {
            let msg = self.sub.next().await?;

            // Extract key from subject.
            let key = match msg.subject.strip_prefix(&self.prefix) {
                Some(k) => k.to_string(),
                None => continue, // heartbeat or control message
            };

            // Determine operation from headers.
            let operation = match msg.headers.as_ref().and_then(|h| h.get("KV-Operation")) {
                Some("DEL") => Operation::Delete,
                Some("PURGE") => Operation::Purge,
                _ => Operation::Put,
            };

            // Extract revision from Nats-Sequence header.
            let revision = msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Nats-Sequence"))
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            return Ok(Entry {
                key,
                value: msg.payload,
                revision,
                operation,
            });
        }
    }
}

/// Format a nanosecond `Duration` as a NATS `Nats-TTL` header value.
///
/// NATS expects a Go-style duration string (e.g. `"30s"`, `"5m"`, `"1h"`).
/// We round down to whole seconds with a minimum of 1 second, since the
/// server enforces TTL with second granularity.
fn format_ttl(ttl: Duration) -> String {
    let secs = (ttl / 1_000_000_000).max(1);
    format!("{secs}s")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{millis, secs};

    #[test]
    fn format_ttl_seconds() {
        assert_eq!(format_ttl(secs(30)), "30s");
        assert_eq!(format_ttl(secs(3600)), "3600s");
    }

    #[test]
    fn format_ttl_subsecond_rounds_to_one() {
        assert_eq!(format_ttl(millis(500)), "1s");
        assert_eq!(format_ttl(0), "1s");
    }

    #[test]
    fn format_ttl_truncates_partial() {
        // 1.999 seconds → 1s (truncation, server has second granularity)
        assert_eq!(format_ttl(1_999_000_000), "1s");
    }
}
