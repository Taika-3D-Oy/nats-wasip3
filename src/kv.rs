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

/// Runtime status / statistics for a KV bucket.
#[non_exhaustive]
#[derive(Debug)]
pub struct KvStatus {
    pub bucket: String,
    /// Number of live (non-deleted) entries.
    pub values: u64,
    /// Total bytes stored in the backing stream.
    pub bytes: u64,
    /// History depth (max revisions per key).
    pub history: i64,
    /// Bucket-wide TTL in nanoseconds, if configured.
    pub ttl: Option<Duration>,
    /// Whether per-message TTL is enabled.
    pub allow_msg_ttl: bool,
    /// Stream sequence of the last message.
    pub last_seq: u64,
}

/// A KV entry.
#[non_exhaustive]
#[derive(Debug)]
pub struct Entry {
    pub key: String,
    pub value: Vec<u8>,
    pub revision: u64,
    pub operation: Operation,
    /// Server-side publish timestamp in RFC 3339 format
    /// (e.g. `"2024-01-15T12:34:56.789Z"`), as returned by the server.
    /// `None` if the server did not supply a timestamp.
    pub time: Option<String>,
}

#[non_exhaustive]
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

        let time = reply
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Time-Stamp"))
            .map(str::to_string);

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
            time,
        }))
    }

    /// Get the latest entry for a key, including deleted and purged entries.
    ///
    /// Unlike [`KeyValue::get`] which returns `None` for tombstones, this returns
    /// the actual [`Entry`] with [`Operation::Delete`] or [`Operation::Purge`] set.
    /// Returns `None` only when the key has never existed.
    /// Mirrors `async_nats::jetstream::kv::Store::entry`.
    pub async fn entry(&self, key: &str) -> Result<Option<Entry>, Error> {
        let subject = self.subject(key);
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

        let time = reply
            .headers
            .as_ref()
            .and_then(|h| h.get("Nats-Time-Stamp"))
            .map(str::to_string);

        let operation = match reply.headers.as_ref().and_then(|h| h.get("KV-Operation")) {
            Some("DEL") => Operation::Delete,
            Some("PURGE") => Operation::Purge,
            _ => Operation::Put,
        };

        Ok(Some(Entry {
            key: key.to_string(),
            value: reply.payload,
            revision,
            operation,
            time,
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

    /// Delete a key only if the current revision matches.
    /// Returns `Err(Error::RevisionMismatch)` if the revision has changed.
    ///
    /// Mirrors `async_nats::jetstream::kv::Store::delete_expect_revision`.
    pub async fn delete_expect_revision(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "DEL");
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            expected_revision.to_string(),
        );
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, b"")
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::RevisionMismatch),
            Err(e) => Err(e),
        }
    }

    /// Deprecated alias for [`KeyValue::delete_expect_revision`].
    #[deprecated(since = "0.9.0", note = "use `delete_expect_revision` instead")]
    pub async fn cas_delete(&self, key: &str, expected_revision: u64) -> Result<(), Error> {
        self.delete_expect_revision(key, expected_revision).await
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

    /// Purge a key with a TTL on the tombstone.
    /// The purge marker itself expires after `ttl`; see [`KeyValue::put_with_ttl`] for TTL semantics.
    pub async fn purge_with_ttl(&self, key: &str, ttl: Duration) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "PURGE");
        headers.insert("Nats-Rollup", "sub");
        headers.insert("Nats-TTL", format_ttl(ttl));
        let subject = self.subject(key);
        self.js
            .publish_with_headers(&subject, &headers, b"")
            .await?;
        Ok(())
    }

    /// Compare-and-swap purge: remove all revisions only if the current revision matches.
    /// Returns `Err(Error::RevisionMismatch)` if the revision has changed.
    pub async fn purge_expect_revision(
        &self,
        key: &str,
        expected_revision: u64,
    ) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "PURGE");
        headers.insert("Nats-Rollup", "sub");
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            expected_revision.to_string(),
        );
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, b"")
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::RevisionMismatch),
            Err(e) => Err(e),
        }
    }

    /// Compare-and-swap purge with a TTL tombstone.
    /// See [`KeyValue::purge_expect_revision`] and [`KeyValue::purge_with_ttl`].
    pub async fn purge_expect_revision_with_ttl(
        &self,
        key: &str,
        expected_revision: u64,
        ttl: Duration,
    ) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("KV-Operation", "PURGE");
        headers.insert("Nats-Rollup", "sub");
        headers.insert(
            "Nats-Expected-Last-Subject-Sequence",
            expected_revision.to_string(),
        );
        headers.insert("Nats-TTL", format_ttl(ttl));
        let subject = self.subject(key);
        match self
            .js
            .publish_with_headers(&subject, &headers, b"")
            .await
        {
            Ok(_) => Ok(()),
            Err(Error::JetStream { code: 400, .. }) => Err(Error::RevisionMismatch),
            Err(e) => Err(e),
        }
    }

    /// Get the entry at a specific stream sequence (revision).
    ///
    /// Unlike [`KeyValue::get`], this returns deleted and purged entries too,
    /// making it suitable for history inspection.
    /// Returns `None` if no message exists at that sequence or it does not
    /// belong to this key.
    pub async fn entry_for_revision(
        &self,
        key: &str,
        revision: u64,
    ) -> Result<Option<Entry>, Error> {
        let msg = match self.js.stream_get_msg(&self.stream_name, revision).await? {
            Some(m) => m,
            None => return Ok(None),
        };

        // Confirm the message belongs to this key.
        if msg.subject != self.subject(key) {
            return Ok(None);
        }

        // Decode the operation from raw NATS wire headers.
        let operation = if let Some(hdrs) = &msg.headers_b64 {
            crate::jetstream::base64_decode(hdrs)
                .ok()
                .and_then(|bytes| {
                    if bytes.windows(17).any(|w| w == b"KV-Operation: DEL") {
                        Some(Operation::Delete)
                    } else if bytes.windows(19).any(|w| w == b"KV-Operation: PURGE") {
                        Some(Operation::Purge)
                    } else {
                        None
                    }
                })
                .unwrap_or(Operation::Put)
        } else {
            Operation::Put
        };

        Ok(Some(Entry {
            key: key.to_string(),
            value: msg.data,
            revision: msg.seq,
            operation,
            time: msg.time,
        }))
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

        // Best-effort cleanup for ephemeral consumer.
        let _ = self.js.delete_consumer(&self.stream_name, &info.name).await;

        Ok(keys)
    }

    /// Bulk-load all live entries from the bucket.
    /// Returns the latest value for each non-deleted key.
    pub async fn load_all(&self) -> Result<Vec<Entry>, Error> {
        let subject = format!("{KV_SUBJECT_PREFIX}.{}.>", self.bucket);
        let consumer_cfg = ConsumerConfig {
            durable_name: None,
            filter_subject: Some(subject),
            deliver_policy: DeliverPolicy::LastPerSubject,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            ..Default::default()
        };

        let info = match self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await
        {
            Ok(info) => info,
            Err(Error::JetStream { code: 404, .. }) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut entries = Vec::new();
        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);
        const LOAD_BATCH: u32 = 256;

        loop {
            let msgs = self.js.fetch(&self.stream_name, &info.name, LOAD_BATCH).await?;
            if msgs.is_empty() {
                break;
            }

            let msgs_len = msgs.len();

            for msg in msgs {
                let Some(key) = msg.subject.strip_prefix(&prefix) else {
                    continue;
                };

                let operation = match msg.headers.as_ref().and_then(|h| h.get("KV-Operation")) {
                    Some("DEL") => Operation::Delete,
                    Some("PURGE") => Operation::Purge,
                    _ => Operation::Put,
                };

                if operation != Operation::Put {
                    continue;
                }

                let revision = extract_revision(&msg);

                let time = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Nats-Time-Stamp"))
                    .map(str::to_string);

                entries.push(Entry {
                    key: key.to_string(),
                    value: msg.payload,
                    revision,
                    operation,
                    time,
                });
            }

            if msgs_len < LOAD_BATCH as usize {
                break;
            }
        }

        // Best-effort cleanup for ephemeral consumer.
        let _ = self.js.delete_consumer(&self.stream_name, &info.name).await;

        Ok(entries)
    }

    /// Return all historical revisions for a key, including tombstones.
    ///
    /// Requires the bucket to have been created with `history > 1` to hold
    /// more than the latest revision. All operations (Put, Delete, Purge) are
    /// included in sequence order.
    /// Mirrors `async_nats::jetstream::kv::Store::history`.
    pub async fn history(&self, key: &str) -> Result<Vec<Entry>, Error> {
        let subject = self.subject(key);
        let consumer_cfg = ConsumerConfig {
            durable_name: None,
            filter_subject: Some(subject),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            ..Default::default()
        };

        let info = match self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await
        {
            Ok(info) => info,
            Err(Error::JetStream { code: 404, .. }) => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };

        let mut entries = Vec::new();
        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);
        const HISTORY_BATCH: u32 = 256;

        loop {
            let msgs = self
                .js
                .fetch(&self.stream_name, &info.name, HISTORY_BATCH)
                .await?;
            if msgs.is_empty() {
                break;
            }
            let msgs_len = msgs.len();

            for msg in msgs {
                let Some(key_str) = msg.subject.strip_prefix(&prefix) else {
                    continue;
                };

                let operation = match msg.headers.as_ref().and_then(|h| h.get("KV-Operation")) {
                    Some("DEL") => Operation::Delete,
                    Some("PURGE") => Operation::Purge,
                    _ => Operation::Put,
                };

                let revision = extract_revision(&msg);

                let time = msg
                    .headers
                    .as_ref()
                    .and_then(|h| h.get("Nats-Time-Stamp"))
                    .map(str::to_string);

                entries.push(Entry {
                    key: key_str.to_string(),
                    value: msg.payload,
                    revision,
                    operation,
                    time,
                });
            }

            if msgs_len < HISTORY_BATCH as usize {
                break;
            }
        }

        let _ = self.js.delete_consumer(&self.stream_name, &info.name).await;
        Ok(entries)
    }

    /// Return runtime status / statistics for this bucket.
    pub async fn status(&self) -> Result<KvStatus, Error> {
        let info = self.js.stream_info(&self.stream_name).await?;
        Ok(KvStatus {
            bucket: self.bucket.clone(),
            values: info.state.messages,
            bytes: info.state.bytes,
            history: info.config.max_msgs_per_subject,
            ttl: info.config.max_age,
            allow_msg_ttl: info.config.allow_msg_ttl,
            last_seq: info.state.last_seq,
        })
    }

    /// Access the underlying JetStream context.
    pub fn jetstream(&self) -> &JetStream {
        &self.js
    }

    /// The bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// The name of the backing JetStream stream (`KV_{bucket}`).
    pub fn stream_name(&self) -> &str {
        &self.stream_name
    }

    // ── Per-key watchers ───────────────────────────────────────────

    /// Watch a single key for changes, receiving the latest value first.
    ///
    /// Delivers the current value for the key (if one exists) immediately,
    /// then streams all future updates. Uses `DeliverPolicy::LastPerSubject`
    /// filtered to the specific key subject.
    /// Mirrors `async_nats::jetstream::kv::Store::watch`.
    pub async fn watch(&self, key: impl AsRef<str>) -> Result<KvWatcher, Error> {
        let subject = self.subject(key.as_ref());
        self.create_watcher_subject(subject, DeliverPolicy::LastPerSubject, None)
            .await
    }

    /// Watch a single key, replaying all history first then streaming updates.
    ///
    /// Uses `DeliverPolicy::All` filtered to the specific key subject.
    /// Requires `history > 1` on the bucket to see more than one revision.
    /// Mirrors `async_nats::jetstream::kv::Store::watch_with_history`.
    pub async fn watch_with_history(&self, key: impl AsRef<str>) -> Result<KvWatcher, Error> {
        let subject = self.subject(key.as_ref());
        self.create_watcher_subject(subject, DeliverPolicy::All, None)
            .await
    }

    /// Watch a single key starting from a specific stream revision.
    ///
    /// Mirrors `async_nats::jetstream::kv::Store::watch_from_revision`.
    pub async fn watch_from_revision(
        &self,
        key: impl AsRef<str>,
        revision: u64,
    ) -> Result<KvWatcher, Error> {
        let subject = self.subject(key.as_ref());
        self.create_watcher_subject(subject, DeliverPolicy::ByStartSequence, Some(revision))
            .await
    }

    // ── Bucket-wide watchers ───────────────────────────────────────

    /// Watch all keys for new changes only (no history replay).
    ///
    /// Mirrors `async_nats::jetstream::kv::Store::watch_all`.
    pub async fn watch_all(&self) -> Result<KvWatcher, Error> {
        self.create_watcher(DeliverPolicy::New, None).await
    }

    /// Watch all keys starting from a specific stream revision.
    ///
    /// Mirrors `async_nats::jetstream::kv::Store::watch_all_from_revision`.
    pub async fn watch_all_from_revision(&self, revision: u64) -> Result<KvWatcher, Error> {
        self.create_watcher(DeliverPolicy::ByStartSequence, Some(revision))
            .await
    }

    /// Watch all keys delivering the latest value per key first, then live updates.
    ///
    /// Mirrors `async_nats::jetstream::kv::Store::watch_all_with_history`
    /// (`DeliverPolicy::LastPerSubject`).
    pub async fn watch_all_with_history(&self) -> Result<KvWatcher, Error> {
        self.create_watcher(DeliverPolicy::LastPerSubject, None)
            .await
    }

    // ── Multi-key watchers ─────────────────────────────────────────

    /// Watch a set of keys for new changes only.
    ///
    /// Requires NATS server 2.10+ (`filter_subjects`).
    /// Mirrors `async_nats::jetstream::kv::Store::watch_many`.
    pub async fn watch_many<T, K>(&self, keys: K) -> Result<KvWatcher, Error>
    where
        T: AsRef<str>,
        K: IntoIterator<Item = T>,
    {
        self.create_watcher_many(keys, DeliverPolicy::New, None).await
    }

    /// Watch a set of keys delivering the latest value per key first, then live updates.
    ///
    /// Requires NATS server 2.10+ (`filter_subjects`).
    /// Mirrors `async_nats::jetstream::kv::Store::watch_many_with_history`.
    pub async fn watch_many_with_history<T, K>(&self, keys: K) -> Result<KvWatcher, Error>
    where
        T: AsRef<str>,
        K: IntoIterator<Item = T>,
    {
        self.create_watcher_many(keys, DeliverPolicy::LastPerSubject, None)
            .await
    }

    /// Watch a set of keys starting from a specific stream revision.
    ///
    /// Requires NATS server 2.10+ (`filter_subjects`).
    pub async fn watch_many_from_revision<T, K>(
        &self,
        keys: K,
        revision: u64,
    ) -> Result<KvWatcher, Error>
    where
        T: AsRef<str>,
        K: IntoIterator<Item = T>,
    {
        self.create_watcher_many(keys, DeliverPolicy::ByStartSequence, Some(revision))
            .await
    }

    // ── Internal watcher helpers ───────────────────────────────────

    /// Bucket-wide watcher (subject = `$KV.{bucket}.>`).
    async fn create_watcher(
        &self,
        deliver_policy: DeliverPolicy,
        opt_start_seq: Option<u64>,
    ) -> Result<KvWatcher, Error> {
        let filter = format!("{KV_SUBJECT_PREFIX}.{}.>", self.bucket);
        self.create_watcher_subject(filter, deliver_policy, opt_start_seq)
            .await
    }

    /// Core watcher factory: single `filter_subject`, any deliver policy.
    async fn create_watcher_subject(
        &self,
        filter_subject: String,
        deliver_policy: DeliverPolicy,
        opt_start_seq: Option<u64>,
    ) -> Result<KvWatcher, Error> {
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

        let consumer_cfg = ConsumerConfig {
            filter_subject: Some(filter_subject),
            filter_subjects: None,
            deliver_subject: Some(deliver.clone()),
            deliver_policy,
            opt_start_seq,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            replay_policy: Some(crate::jetstream::ReplayPolicy::Instant),
            mem_storage: Some(true),
            ..Default::default()
        };

        let info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await?;

        let sub = self.js.client().subscribe(&deliver)?;
        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);

        Ok(KvWatcher {
            sub,
            prefix,
            js: self.js.clone(),
            stream_name: self.stream_name.clone(),
            consumer_name: info.name,
        })
    }

    /// Multi-key watcher using `filter_subjects` (NATS server 2.10+).
    async fn create_watcher_many<T, K>(
        &self,
        keys: K,
        deliver_policy: DeliverPolicy,
        opt_start_seq: Option<u64>,
    ) -> Result<KvWatcher, Error>
    where
        T: AsRef<str>,
        K: IntoIterator<Item = T>,
    {
        let filter_subjects = keys
            .into_iter()
            .map(|k| format!("{KV_SUBJECT_PREFIX}.{}.{}", self.bucket, k.as_ref()))
            .collect::<Vec<_>>();

        if filter_subjects.is_empty() {
            return Err(Error::Protocol("watch_many requires at least one key".into()));
        }

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

        let consumer_cfg = ConsumerConfig {
            filter_subject: None,
            filter_subjects: Some(filter_subjects),
            deliver_subject: Some(deliver.clone()),
            deliver_policy,
            opt_start_seq,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            replay_policy: Some(crate::jetstream::ReplayPolicy::Instant),
            mem_storage: Some(true),
            ..Default::default()
        };

        let info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await
            .map_err(map_watch_many_create_error)?;

        let sub = self.js.client().subscribe(&deliver)?;
        let prefix = format!("{KV_SUBJECT_PREFIX}.{}.", self.bucket);

        Ok(KvWatcher {
            sub,
            prefix,
            js: self.js.clone(),
            stream_name: self.stream_name.clone(),
            consumer_name: info.name,
        })
    }
}

fn map_watch_many_create_error(err: Error) -> Error {
    match err {
        Error::JetStream { code, description }
            if code == 400
                && (description.to_ascii_lowercase().contains("filter_subjects")
                    || description.to_ascii_lowercase().contains("filter subjects")) =>
        {
            Error::Protocol(
                "watch_many requires NATS server 2.10+ with consumer filter_subjects support"
                    .into(),
            )
        }
        other => other,
    }
}

/// A live watcher on a KV bucket. Yields `Entry` items as changes occur.
pub struct KvWatcher {
    sub: crate::client::Subscription,
    prefix: String,
    /// Held so Drop can delete the ephemeral consumer.
    js: crate::jetstream::JetStream,
    stream_name: String,
    consumer_name: String,
}

impl Drop for KvWatcher {
    fn drop(&mut self) {
        // Fire-and-forget consumer deletion. JetStream API calls require a
        // reply-to subject — we use a throwaway inbox. The server will send
        // a response there but we never subscribe to it; the consumer is
        // still deleted.
        let subject = format!(
            "$JS.API.CONSUMER.DELETE.{}.{}",
            self.stream_name, self.consumer_name
        );
        let inbox = self.js.client().new_inbox();
        let _ = self.js.client().publish_with_reply(&subject, &inbox, b"");
    }
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

            // Extract revision from Nats-Sequence header or reply-to subject.
            let revision = extract_revision(&msg);

            let time = msg
                .headers
                .as_ref()
                .and_then(|h| h.get("Nats-Time-Stamp"))
                .map(str::to_string);

            return Ok(Entry {
                key,
                value: msg.payload,
                revision,
                operation,
                time,
            });
        }
    }
}

/// Extract the stream sequence (revision) from a JetStream consumer message.
///
/// Consumer-delivered messages carry the stream sequence in the reply-to subject
/// (`$JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>`)
/// rather than in a `Nats-Sequence` header (which is only present in direct-get responses).
/// This function checks the header first, then falls back to parsing the reply-to.
fn extract_revision(msg: &crate::client::Message) -> u64 {
    // Prefer the Nats-Sequence header (present in $JS.API.DIRECT.GET responses).
    if let Some(rev) = msg
        .headers
        .as_ref()
        .and_then(|h| h.get("Nats-Sequence"))
        .and_then(|s| s.parse::<u64>().ok())
    {
        return rev;
    }
    // Fall back to the reply-to subject which encodes stream_seq at index 5.
    // Format: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts>.<pending>[.<token>]
    if let Some(ref reply) = msg.reply_to {
        let parts: Vec<&str> = reply.split('.').collect();
        if parts.len() >= 9 && parts[0] == "$JS" && parts[1] == "ACK" {
            if let Ok(seq) = parts[5].parse::<u64>() {
                return seq;
            }
        }
    }
    0
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

    #[test]
    fn map_watch_many_error_for_unsupported_filter_subjects() {
        let mapped = map_watch_many_create_error(Error::JetStream {
            code: 400,
            description: "unknown field: filter_subjects".to_string(),
        });

        match mapped {
            Error::Protocol(msg) => assert!(msg.contains("2.10+")),
            other => panic!("expected protocol error, got: {other:?}"),
        }
    }

    #[test]
    fn map_watch_many_error_passthrough_other_errors() {
        let mapped = map_watch_many_create_error(Error::JetStream {
            code: 503,
            description: "service unavailable".to_string(),
        });

        match mapped {
            Error::JetStream { code, description } => {
                assert_eq!(code, 503);
                assert_eq!(description, "service unavailable");
            }
            other => panic!("expected original jetstream error, got: {other:?}"),
        }
    }
}
