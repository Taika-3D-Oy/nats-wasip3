//! JetStream Object Store.
//!
//! Object Store buckets are JetStream streams with subjects:
//! - `$O.{bucket}.C.{nuid}` for chunk data
//! - `$O.{bucket}.M.{name}` for object metadata

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::client::Duration;
use crate::jetstream::{
    AckPolicy, ConsumerConfig, DeliverPolicy, DiscardPolicy, JetStream, Retention, Storage,
    StreamConfig,
};
use crate::proto::Headers;
use crate::Error;

const OBJ_STREAM_PREFIX: &str = "OBJ_";
const OBJ_SUBJECT_PREFIX: &str = "$O";
const DEFAULT_CHUNK_SIZE: usize = 128 * 1024;
const MAX_LIST_BATCH_SIZE: u32 = 1000;

static NEXT_CHUNK_ID: AtomicU64 = AtomicU64::new(1);

/// A JetStream Object Store bucket.
#[derive(Clone)]
pub struct ObjectStore {
    js: JetStream,
    bucket: String,
    stream_name: String,
    max_chunk_size: usize,
}

/// Configuration for creating an Object Store bucket.
#[derive(Debug, Clone)]
pub struct ObjectStoreConfig {
    pub bucket: String,
    pub max_bytes: i64,
    pub max_value_size: i32,
    pub ttl: Option<Duration>,
    pub storage: Storage,
    pub num_replicas: u32,
    pub max_chunk_size: usize,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            max_bytes: -1,
            max_value_size: -1,
            ttl: None,
            storage: Storage::File,
            num_replicas: 1,
            max_chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

/// Runtime status / statistics for an Object Store bucket.
#[derive(Debug)]
pub struct ObjectStoreStatus {
    pub bucket: String,
    pub objects: u64,
    pub bytes: u64,
    pub ttl: Option<Duration>,
    pub stream_name: String,
}

/// An object returned by [`ObjectStore::get`].
#[derive(Debug)]
pub struct Object {
    pub info: ObjectInfo,
    pub data: Vec<u8>,
}

/// Public object metadata.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub name: String,
    pub bucket: String,
    pub nuid: String,
    pub size: u64,
    pub chunks: u32,
    pub mtime: String,
    pub deleted: bool,
    pub max_chunk_size: Option<usize>,
}

impl ObjectStore {
    /// Open or create an Object Store bucket.
    pub async fn new(js: JetStream, config: ObjectStoreConfig) -> Result<Self, Error> {
        let stream_name = format!("{OBJ_STREAM_PREFIX}{}", config.bucket);
        let stream_config = StreamConfig {
            name: stream_name.clone(),
            subjects: vec![
                format!("{OBJ_SUBJECT_PREFIX}.{}.C.>", config.bucket),
                format!("{OBJ_SUBJECT_PREFIX}.{}.M.>", config.bucket),
            ],
            retention: Retention::Limits,
            max_consumers: -1,
            max_msgs: -1,
            max_bytes: config.max_bytes,
            max_msg_size: config.max_value_size,
            max_msgs_per_subject: -1,
            storage: config.storage,
            num_replicas: config.num_replicas,
            discard: DiscardPolicy::New,
            max_age: config.ttl,
            duplicate_window: None,
            allow_direct: true,
            allow_rollup_hdrs: true,
            allow_msg_ttl: false,
        };

        js.create_stream(&stream_config).await?;

        Ok(Self {
            js,
            bucket: config.bucket,
            stream_name,
            max_chunk_size: config.max_chunk_size,
        })
    }

    /// Open an existing Object Store bucket (does not create it).
    pub fn open(js: JetStream, bucket: impl Into<String>) -> Self {
        let bucket = bucket.into();
        let stream_name = format!("{OBJ_STREAM_PREFIX}{bucket}");
        Self {
            js,
            bucket,
            stream_name,
            max_chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }

    /// Put an object into the bucket.
    pub async fn put(&self, name: &str, data: &[u8]) -> Result<ObjectInfo, Error> {
        self.put_with_chunk_size(name, data, self.max_chunk_size)
            .await
    }

    /// Put an object with a custom chunk size.
    pub async fn put_with_chunk_size(
        &self,
        name: &str,
        data: &[u8],
        chunk_size: usize,
    ) -> Result<ObjectInfo, Error> {
        if name.is_empty() {
            return Err(Error::Protocol("object name must not be empty".into()));
        }
        if chunk_size == 0 {
            return Err(Error::Protocol("chunk size must be > 0".into()));
        }

        let old = self.get_meta(name).await?;
        let nuid = generate_chunk_subject_id()?;
        let chunk_subject = self.chunk_subject(&nuid);

        let mut chunk_count: u32 = 0;
        for chunk in data.chunks(chunk_size) {
            self.js.publish(&chunk_subject, chunk).await?;
            chunk_count = chunk_count
                .checked_add(1)
                .ok_or_else(|| Error::Protocol("object chunk count overflow".into()))?;
        }

        let meta = ObjectMeta {
            name: name.to_string(),
            bucket: self.bucket.clone(),
            nuid: nuid.clone(),
            size: data.len() as u64,
            chunks: chunk_count,
            mtime: now_nanos_timestamp()?,
            deleted: false,
            options: Some(ObjectMetaOptions {
                max_chunk_size: Some(chunk_size),
            }),
        };
        self.publish_meta(&meta).await?;

        if let Some(prev) = old {
            let _ = self
                .js
                .purge_stream_subject(&self.stream_name, &self.chunk_subject(&prev.nuid))
                .await;
        }

        Ok(meta_to_info(meta))
    }

    /// Get an object by name.
    pub async fn get(&self, name: &str) -> Result<Option<Object>, Error> {
        let meta = match self.get_meta(name).await? {
            Some(m) if !m.deleted => m,
            _ => return Ok(None),
        };

        let consumer_cfg = ConsumerConfig {
            durable_name: None,
            filter_subject: Some(self.chunk_subject(&meta.nuid)),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            ..Default::default()
        };
        let info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await?;

        let msgs = if meta.chunks == 0 {
            Vec::new()
        } else {
            self.js
                .fetch(&self.stream_name, &info.name, meta.chunks)
                .await?
        };
        let mut out = Vec::with_capacity(meta.size as usize);
        for msg in msgs {
            out.extend_from_slice(&msg.payload);
        }

        if out.len() as u64 != meta.size {
            return Err(Error::Protocol("object data size mismatch".into()));
        }

        Ok(Some(Object {
            info: meta_to_info(meta),
            data: out,
        }))
    }

    /// Get object metadata by name.
    pub async fn info(&self, name: &str) -> Result<Option<ObjectInfo>, Error> {
        Ok(self.get_meta(name).await?.map(meta_to_info))
    }

    /// List current objects in the bucket.
    pub async fn list(&self) -> Result<Vec<ObjectInfo>, Error> {
        let consumer_cfg = ConsumerConfig {
            durable_name: None,
            filter_subject: Some(format!("{OBJ_SUBJECT_PREFIX}.{}.M.>", self.bucket)),
            deliver_policy: DeliverPolicy::LastPerSubject,
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            ..Default::default()
        };

        let info = self
            .js
            .create_consumer(&self.stream_name, &consumer_cfg)
            .await?;
        let msgs = self
            .js
            .fetch(&self.stream_name, &info.name, MAX_LIST_BATCH_SIZE)
            .await?;

        let mut out = Vec::new();
        for msg in msgs {
            let meta: ObjectMeta = serde_json::from_slice(&msg.payload)?;
            if !meta.deleted {
                out.push(meta_to_info(meta));
            }
        }
        Ok(out)
    }

    /// Delete an object and purge its chunk data.
    pub async fn delete(&self, name: &str) -> Result<bool, Error> {
        let Some(meta) = self.get_meta(name).await? else {
            return Ok(false);
        };

        let _ = self
            .js
            .purge_stream_subject(&self.stream_name, &self.chunk_subject(&meta.nuid))
            .await;

        let tombstone = ObjectMeta {
            name: meta.name,
            bucket: meta.bucket,
            nuid: meta.nuid,
            size: 0,
            chunks: 0,
            mtime: now_nanos_timestamp()?,
            deleted: true,
            options: meta.options,
        };
        self.publish_meta(&tombstone).await?;

        Ok(true)
    }

    /// Return runtime status / statistics for this bucket.
    pub async fn status(&self) -> Result<ObjectStoreStatus, Error> {
        let info = self.js.stream_info(&self.stream_name).await?;
        let objects = self.list().await?.len() as u64;
        Ok(ObjectStoreStatus {
            bucket: self.bucket.clone(),
            objects,
            bytes: info.state.bytes,
            ttl: info.config.max_age,
            stream_name: self.stream_name.clone(),
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

    fn chunk_subject(&self, nuid: &str) -> String {
        format!("{OBJ_SUBJECT_PREFIX}.{}.C.{nuid}", self.bucket)
    }

    fn meta_subject(&self, name: &str) -> String {
        format!(
            "{OBJ_SUBJECT_PREFIX}.{}.M.{}",
            self.bucket,
            name_to_subject_token(name)
        )
    }

    async fn publish_meta(&self, meta: &ObjectMeta) -> Result<(), Error> {
        let mut headers = Headers::new();
        headers.insert("Nats-Rollup", "sub");
        let payload = serde_json::to_vec(meta)?;
        self.js
            .publish_with_headers(&self.meta_subject(&meta.name), &headers, &payload)
            .await?;
        Ok(())
    }

    async fn get_meta(&self, name: &str) -> Result<Option<ObjectMeta>, Error> {
        let subject = format!(
            "$JS.API.DIRECT.GET.{}.{}",
            self.stream_name,
            self.meta_subject(name)
        );
        let reply = match self
            .js
            .client()
            .request(&subject, b"", crate::client::secs(5))
            .await
        {
            Ok(msg) => msg,
            Err(Error::Timeout) => return Ok(None),
            Err(e) => return Err(e),
        };

        if let Some(headers) = &reply.headers {
            if headers.status == Some(404) {
                return Ok(None);
            }
        }

        let meta: ObjectMeta = serde_json::from_slice(&reply.payload)?;
        Ok(Some(meta))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectMeta {
    name: String,
    bucket: String,
    nuid: String,
    size: u64,
    chunks: u32,
    mtime: String,
    #[serde(default)]
    deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    options: Option<ObjectMetaOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ObjectMetaOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_chunk_size: Option<usize>,
}

fn meta_to_info(meta: ObjectMeta) -> ObjectInfo {
    ObjectInfo {
        name: meta.name,
        bucket: meta.bucket,
        nuid: meta.nuid,
        size: meta.size,
        chunks: meta.chunks,
        mtime: meta.mtime,
        deleted: meta.deleted,
        max_chunk_size: meta.options.and_then(|o| o.max_chunk_size),
    }
}

fn generate_chunk_subject_id() -> Result<String, Error> {
    let now = now_unix_nanos()?;
    let id = NEXT_CHUNK_ID.fetch_add(1, Ordering::Relaxed);
    Ok(format!("O{now:032x}{id:016x}"))
}

fn name_to_subject_token(name: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(name.as_bytes())
}

fn now_nanos_timestamp() -> Result<String, Error> {
    let nanos = now_unix_nanos()?;
    Ok(format!("{nanos}"))
}

fn now_unix_nanos() -> Result<u128, Error> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| Error::Protocol(format!("system time before unix epoch: {e}")))?
        .as_nanos())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_token_is_url_safe_base64() {
        assert_eq!(name_to_subject_token("hello/world"), "aGVsbG8vd29ybGQ");
    }

    #[test]
    fn chunk_id_monotonic() {
        let a = generate_chunk_subject_id().unwrap();
        let b = generate_chunk_subject_id().unwrap();
        assert!(b > a);
    }
}
