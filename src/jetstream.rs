//! JetStream API layer.
//!
//! JetStream operations are performed via JSON request/reply on `$JS.API.*`
//! subjects. This module provides typed wrappers around those API calls.

use serde::{Deserialize, Serialize};

use crate::client::{secs, Client, Duration, Message, Subscription};
use crate::proto::Headers;
use crate::Error;

fn js_api_timeout() -> Duration {
    secs(5)
}

// ── API error response ─────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct ApiResponse<T> {
    #[serde(default)]
    error: Option<ApiError>,
    #[serde(flatten)]
    data: Option<T>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: u16,
    description: String,
}

fn parse_response<T: for<'de> Deserialize<'de>>(msg: &Message) -> Result<T, Error> {
    let resp: ApiResponse<T> = serde_json::from_slice(&msg.payload)?;
    if let Some(err) = resp.error {
        return Err(Error::JetStream {
            code: err.code,
            description: err.description,
        });
    }
    resp.data
        .ok_or_else(|| Error::Protocol("empty JetStream response".into()))
}

// ── JetStream context ──────────────────────────────────────────────

/// JetStream context, wrapping a `Client`.
#[derive(Clone)]
pub struct JetStream {
    client: Client,
    prefix: String,
}

impl JetStream {
    /// Create a JetStream context with default `$JS.API` prefix.
    pub fn new(client: Client) -> Self {
        Self {
            client,
            prefix: "$JS.API".to_string(),
        }
    }

    /// Create with a custom API prefix (for domains/imports).
    pub fn with_prefix(client: Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            prefix: prefix.into(),
        }
    }

    fn api_subject(&self, op: &str) -> String {
        format!("{}.{}", self.prefix, op)
    }

    async fn api_request<Req: Serialize, Resp: for<'de> Deserialize<'de>>(
        &self,
        op: &str,
        req: &Req,
    ) -> Result<Resp, Error> {
        let subject = self.api_subject(op);
        let payload = serde_json::to_vec(req)?;
        let reply = self
            .client
            .request(&subject, &payload, js_api_timeout())
            .await?;
        parse_response(&reply)
    }

    async fn api_request_empty<Resp: for<'de> Deserialize<'de>>(
        &self,
        op: &str,
    ) -> Result<Resp, Error> {
        let subject = self.api_subject(op);
        let reply = self.client.request(&subject, b"", js_api_timeout()).await?;
        parse_response(&reply)
    }

    // ── Stream management ──────────────────────────────────────

    /// Create a stream.
    pub async fn create_stream(&self, config: &StreamConfig) -> Result<StreamInfo, Error> {
        self.api_request(&format!("STREAM.CREATE.{}", config.name), config)
            .await
    }

    /// Update an existing stream configuration.
    pub async fn update_stream(&self, config: &StreamConfig) -> Result<StreamInfo, Error> {
        self.api_request(&format!("STREAM.UPDATE.{}", config.name), config)
            .await
    }

    /// Delete a stream.
    pub async fn delete_stream(&self, name: &str) -> Result<bool, Error> {
        let resp: DeleteResponse = self
            .api_request_empty(&format!("STREAM.DELETE.{name}"))
            .await?;
        Ok(resp.success)
    }

    /// Get stream info.
    pub async fn stream_info(&self, name: &str) -> Result<StreamInfo, Error> {
        self.api_request_empty(&format!("STREAM.INFO.{name}")).await
    }

    /// Purge all messages from a stream.
    pub async fn purge_stream(&self, name: &str) -> Result<PurgeResponse, Error> {
        self.api_request_empty(&format!("STREAM.PURGE.{name}"))
            .await
    }

    /// Purge messages from a stream for a specific subject.
    pub async fn purge_stream_subject(
        &self,
        name: &str,
        subject: &str,
    ) -> Result<PurgeResponse, Error> {
        #[derive(Serialize)]
        struct PurgeReq<'a> {
            filter: &'a str,
        }
        self.api_request(
            &format!("STREAM.PURGE.{name}"),
            &PurgeReq { filter: subject },
        )
        .await
    }

    // ── Publish ────────────────────────────────────────────────

    /// Publish to a JetStream subject and wait for ack.
    pub async fn publish(&self, subject: &str, payload: &[u8]) -> Result<PubAck, Error> {
        let reply = self
            .client
            .request(subject, payload, js_api_timeout())
            .await?;
        parse_response(&reply)
    }

    /// Publish with headers (e.g., for expected-last-subject-sequence).
    pub async fn publish_with_headers(
        &self,
        subject: &str,
        headers: &Headers,
        payload: &[u8],
    ) -> Result<PubAck, Error> {
        let reply = self
            .client
            .request_with_headers(subject, headers, payload, js_api_timeout())
            .await?;
        parse_response(&reply)
    }

    // ── Consumer management ────────────────────────────────────

    /// Create or update a consumer on a stream.
    pub async fn create_consumer(
        &self,
        stream: &str,
        config: &ConsumerConfig,
    ) -> Result<ConsumerInfo, Error> {
        let op = match &config.durable_name {
            Some(name) => format!("CONSUMER.CREATE.{stream}.{name}"),
            None => format!("CONSUMER.CREATE.{stream}"),
        };
        #[derive(Serialize)]
        struct Req<'a> {
            stream_name: &'a str,
            config: &'a ConsumerConfig,
        }
        self.api_request(
            &op,
            &Req {
                stream_name: stream,
                config,
            },
        )
        .await
    }

    /// Delete a consumer.
    pub async fn delete_consumer(&self, stream: &str, consumer: &str) -> Result<bool, Error> {
        let resp: DeleteResponse = self
            .api_request_empty(&format!("CONSUMER.DELETE.{stream}.{consumer}"))
            .await?;
        Ok(resp.success)
    }

    /// Fetch messages from a pull consumer (simple one-shot fetch).
    pub async fn fetch(
        &self,
        stream: &str,
        consumer: &str,
        batch: u32,
    ) -> Result<Vec<Message>, Error> {
        use crate::client::with_timeout;

        #[derive(Serialize)]
        struct FetchReq {
            batch: u32,
            no_wait: bool,
        }
        let subject = format!("$JS.API.CONSUMER.MSG.NEXT.{stream}.{consumer}");
        let payload = serde_json::to_vec(&FetchReq {
            batch,
            no_wait: true,
        })?;

        let inbox = self.client.new_inbox();
        let sub = self.client.subscribe(&inbox)?;
        self.client
            .publish_with_headers(&subject, Some(&inbox), &Headers::new(), &payload)?;

        let mut messages = Vec::new();
        // First message can take up to js_api_timeout(), but subsequent messages
        // in a no_wait batch arrive in immediate succession.
        let first_timeout = js_api_timeout();
        let subsequent_timeout = crate::client::millis(100);

        loop {
            let timeout = if messages.is_empty() {
                first_timeout
            } else {
                subsequent_timeout
            };

            let msg = match with_timeout(timeout, sub.next()).await {
                Ok(Ok(msg)) => msg,
                Ok(Err(e)) => return Err(e), // subscription error (disconnected etc.)
                Err(_) => break,             // timeout: no more messages from server
            };
            // A 404 or 408 status means no more messages.
            if let Some(ref h) = msg.headers {
                if let Some(status) = h.status {
                    if status == 404 || status == 408 || status == 409 {
                        break;
                    }
                }
            }

            let is_last = msg.headers.as_ref().map_or(false, |h| {
                h.get("Nats-Pending-Messages")
                    .or_else(|| h.get("Nats-Pending"))
                    .or_else(|| h.get("Nats-Num-Pending"))
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    == Some(0)
            });

            messages.push(msg);
            if is_last || messages.len() >= batch as usize {
                break;
            }
        }

        Ok(messages)
    }

    /// Get a single message from a stream by sequence number (direct get).
    /// Returns `None` if the sequence doesn't exist.
    pub async fn stream_get_msg(
        &self,
        stream: &str,
        seq: u64,
    ) -> Result<Option<StreamMessage>, Error> {
        #[derive(Serialize)]
        struct GetReq {
            seq: u64,
        }
        let subject = format!("{}.STREAM.MSG.GET.{stream}", self.prefix);
        let payload = serde_json::to_vec(&GetReq { seq })?;
        let reply = match self
            .client
            .request(&subject, &payload, js_api_timeout())
            .await
        {
            Ok(msg) => msg,
            Err(Error::Timeout) => return Ok(None),
            Err(e) => return Err(e),
        };

        #[derive(Deserialize)]
        struct MsgGetResp {
            #[serde(default)]
            error: Option<ApiError>,
            message: Option<StreamMsgWire>,
        }

        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct StreamMsgWire {
            subject: String,
            seq: u64,
            data: Option<String>, // base64
            #[serde(default)]
            hdrs: Option<String>, // base64 encoded NATS headers
            #[serde(default)]
            time: Option<String>, // RFC 3339 server-side timestamp
        }

        let resp: MsgGetResp = serde_json::from_slice(&reply.payload)?;
        if let Some(err) = resp.error {
            if err.code == 404 {
                return Ok(None);
            }
            return Err(Error::JetStream {
                code: err.code,
                description: err.description,
            });
        }
        let wire = resp
            .message
            .ok_or_else(|| Error::Protocol("empty msg get".into()))?;
        let data = match wire.data {
            Some(b64) => base64_decode(&b64)?,
            None => Vec::new(),
        };
        Ok(Some(StreamMessage {
            subject: wire.subject,
            seq: wire.seq,
            data,
            headers_b64: wire.hdrs,
            time: wire.time,
        }))
    }

    /// Get the last message for a subject directly using `$JS.API.DIRECT.GET.LAST.<stream>.<subject>`.
    /// Requires `allow_direct: true` on the stream.
    ///
    /// This bypasses JSON wrapping and returns the raw message payload directly with headers.
    pub async fn direct_get_last_for_subject(
        &self,
        stream: &str,
        subject: &str,
    ) -> Result<Option<DirectMessage>, Error> {
        let direct_subject = format!("{}.DIRECT.GET.{stream}.{subject}", self.prefix);
        let reply = match self
            .client
            .request(&direct_subject, b"", js_api_timeout())
            .await
        {
            Ok(msg) => msg,
            Err(Error::Timeout) => return Ok(None),
            Err(e) => return Err(e),
        };

        if let Some(ref h) = reply.headers {
            if let Some(status) = h.status {
                if status == 404 || status == 408 {
                    return Ok(None);
                }
            }
        }

        let headers = reply.headers.clone().unwrap_or_default();
        let seq = headers
            .get("Nats-Sequence")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0);
        let timestamp = headers.get("Nats-Time-Stamp").map(|s| s.to_string());
        let orig_subject = headers
            .get("Nats-Subject")
            .map(|s| s.to_string())
            .unwrap_or_else(|| subject.to_string());

        Ok(Some(DirectMessage {
            subject: orig_subject,
            sequence: seq,
            timestamp,
            headers: reply.headers,
            payload: reply.payload,
        }))
    }

    /// Get a message by stream sequence number directly using `$JS.API.DIRECT.GET.<stream>`.
    /// Requires `allow_direct: true` on the stream.
    pub async fn direct_get(
        &self,
        stream: &str,
        seq: u64,
    ) -> Result<Option<DirectMessage>, Error> {
        #[derive(Serialize)]
        struct DirectReq {
            seq: u64,
        }
        let direct_subject = format!("{}.DIRECT.GET.{stream}", self.prefix);
        let payload = serde_json::to_vec(&DirectReq { seq })?;
        let reply = match self
            .client
            .request(&direct_subject, &payload, js_api_timeout())
            .await
        {
            Ok(msg) => msg,
            Err(Error::Timeout) => return Ok(None),
            Err(e) => return Err(e),
        };

        if let Some(ref h) = reply.headers {
            if let Some(status) = h.status {
                if status == 404 || status == 408 {
                    return Ok(None);
                }
            }
        }

        let headers = reply.headers.clone().unwrap_or_default();
        let seq_num = headers
            .get("Nats-Sequence")
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(seq);
        let timestamp = headers.get("Nats-Time-Stamp").map(|s| s.to_string());
        let subject = headers
            .get("Nats-Subject")
            .map(|s| s.to_string())
            .unwrap_or_default();

        Ok(Some(DirectMessage {
            subject,
            sequence: seq_num,
            timestamp,
            headers: reply.headers,
            payload: reply.payload,
        }))
    }

    /// Create an [`OrderedConsumer`] for sequenced, gap-detected, and auto-recovering stream consumption.
    pub async fn ordered_consumer(
        &self,
        stream: &str,
        config: OrderedConsumerConfig,
    ) -> Result<OrderedConsumer, Error> {
        OrderedConsumer::new(self.clone(), stream, config).await
    }

    /// Access the underlying client (for direct publish, subscribe, etc.).
    pub fn client(&self) -> &Client {
        &self.client
    }

    // ── Push consumer ──────────────────────────────────────────

    /// Create a push consumer and return a `ConsumerMessages` iterator that
    /// yields messages as they arrive. Each message must be acknowledged via
    /// [`JsMessage::ack`] (or [`JsMessage::nak`] / [`JsMessage::in_progress`]).
    ///
    /// The consumer is created with the given config; `deliver_subject` is
    /// set automatically if not already provided.
    pub async fn consume(
        &self,
        stream: &str,
        config: &ConsumerConfig,
    ) -> Result<ConsumerMessages, Error> {
        // Generate a unique deliver subject if not provided.
        let deliver = match &config.deliver_subject {
            Some(d) => d.clone(),
            None => {
                use std::cell::Cell;
                thread_local! {
                    static CTR: Cell<u64> = const { Cell::new(0) };
                }
                let id = CTR.with(|c| {
                    let v = c.get();
                    c.set(v + 1);
                    v
                });
                format!("_DELIVER.{stream}.{id}")
            }
        };

        let mut cfg = config.clone();
        cfg.deliver_subject = Some(deliver.clone());

        let info = self.create_consumer(stream, &cfg).await?;
        let sub = self.client.subscribe(&deliver)?;

        Ok(ConsumerMessages {
            sub,
            client: self.client.clone(),
            stream: stream.to_string(),
            consumer: info.name,
        })
    }
}

// ── Push consumer message iterator ─────────────────────────────────

/// Iterator over messages from a push consumer.
/// Obtained via [`JetStream::consume`].
pub struct ConsumerMessages {
    sub: crate::client::Subscription,
    client: Client,
    stream: String,
    consumer: String,
}

impl ConsumerMessages {
    /// Receive the next message from the push consumer.
    ///
    /// Heartbeats and control messages are silently skipped.
    pub async fn next(&self) -> Result<JsMessage, Error> {
        loop {
            let msg = self.sub.next().await?;

            // Skip idle heartbeats (empty payload, no reply-to, Status: 100).
            if msg.reply_to.is_none() {
                if let Some(ref h) = msg.headers {
                    if let Some(status) = h.get("Status") {
                        if status.starts_with("100") {
                            continue;
                        }
                    }
                }
            }

            // Skip server flow-control messages (Status: 100 with reply).
            if let Some(ref reply) = msg.reply_to {
                if let Some(ref h) = msg.headers {
                    if let Some(status) = h.get("Status") {
                        if status.starts_with("100") {
                            // Respond to flow control.
                            let _ = self.client.publish(reply, b"");
                            continue;
                        }
                    }
                }
            }

            let reply_to = msg.reply_to.clone();
            return Ok(JsMessage {
                message: msg,
                reply_to,
                client: self.client.clone(),
            });
        }
    }

    /// The server-assigned consumer name.
    pub fn consumer_name(&self) -> &str {
        &self.consumer
    }

    /// The stream this consumer reads from.
    pub fn stream_name(&self) -> &str {
        &self.stream
    }
}

impl Drop for ConsumerMessages {
    fn drop(&mut self) {
        // Fire-and-forget ephemeral consumer cleanup. Requires a reply-to so
        // the JetStream server actually processes the DELETE; we never read
        // the reply.
        let subject = format!("$JS.API.CONSUMER.DELETE.{}.{}", self.stream, self.consumer);
        let inbox = self.client.new_inbox();
        let _ = self.client.publish_with_reply(&subject, &inbox, b"");
    }
}

/// A message received from a JetStream push consumer. Must be acknowledged.
pub struct JsMessage {
    /// The underlying NATS message.
    pub message: Message,
    reply_to: Option<String>,
    client: Client,
}

impl JsMessage {
    /// The message subject.
    pub fn subject(&self) -> &str {
        &self.message.subject
    }

    /// The message payload.
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    /// The message headers.
    pub fn headers(&self) -> Option<&Headers> {
        self.message.headers.as_ref()
    }

    /// Acknowledge successful processing.
    pub fn ack(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            self.client.publish(reply, b"+ACK")?;
        }
        Ok(())
    }

    /// Negative-acknowledge: ask the server to redeliver.
    pub fn nak(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            self.client.publish(reply, b"-NAK")?;
        }
        Ok(())
    }

    /// Negative-acknowledge with a backoff delay before redelivery.
    /// Tells the NATS server to delay redelivery of this message by the specified duration.
    pub fn nak_with_delay(&self, delay: Duration) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            let nanos = delay;
            let payload = format!("-NAK {{\"delay\": {nanos}}}");
            self.client.publish(reply, payload.as_bytes())?;
        }
        Ok(())
    }

    /// Signal that processing is still in progress (extend ack deadline).
    pub fn in_progress(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            self.client.publish(reply, b"+WPI")?;
        }
        Ok(())
    }

    /// Terminate further redelivery of this message.
    pub fn term(&self) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            self.client.publish(reply, b"+TERM")?;
        }
        Ok(())
    }

    /// Terminate further redelivery of this message with a reason string.
    pub fn term_with_reason(&self, reason: &str) -> Result<(), Error> {
        if let Some(ref reply) = self.reply_to {
            let payload = format!("+TERM {reason}");
            self.client.publish(reply, payload.as_bytes())?;
        }
        Ok(())
    }

    /// Parse and extract full JetStream ACK metadata from the delivery reply subject.
    /// Supports both v1 (9-token) and v2 (12-token) NATS JetStream reply formats.
    pub fn metadata(&self) -> Option<MsgMetadata> {
        self.reply_to.as_deref().and_then(MsgMetadata::parse)
    }

    /// Server-side publish timestamp as an RFC 3339 string
    /// (e.g. `"2024-01-15T12:34:56.789Z"`), taken from the `Nats-Time-Stamp`
    /// header that JetStream adds to every delivered message.
    ///
    /// Returns `None` if the header is absent (non-JetStream messages,
    /// or very old server versions).
    pub fn timestamp(&self) -> Option<&str> {
        self.message.headers.as_ref()?.get("Nats-Time-Stamp")
    }

    /// Server-side publish timestamp as **nanoseconds since the Unix epoch**,
    /// parsed from the JetStream ACK reply-to subject that NATS embeds in
    /// every push-consumer delivery.
    ///
    /// Returns `None` if the reply-to subject is absent or has an unexpected
    /// format (e.g. a plain pub/sub message not delivered via JetStream).
    pub fn timestamp_nanos(&self) -> Option<u64> {
        self.metadata().map(|m| m.timestamp_nanos)
    }
}

// ── Stream types ───────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamConfig {
    pub name: String,
    #[serde(default)]
    pub subjects: Vec<String>,
    #[serde(default)]
    pub retention: Retention,
    #[serde(default)]
    pub max_consumers: i64,
    #[serde(default)]
    pub max_msgs: i64,
    #[serde(default)]
    pub max_bytes: i64,
    #[serde(default)]
    pub max_msg_size: i32,
    /// Maximum number of messages per unique subject. Used by KV for history.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub max_msgs_per_subject: i64,
    #[serde(default)]
    pub storage: Storage,
    #[serde(default)]
    pub num_replicas: u32,
    #[serde(default)]
    pub discard: DiscardPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duplicate_window: Option<u64>,
    #[serde(default)]
    pub allow_direct: bool,
    #[serde(default)]
    pub allow_rollup_hdrs: bool,
    /// Allow per-message TTL via the `Nats-TTL` header (NATS server 2.11+).
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_msg_ttl: bool,
    /// Enable server-side message scheduling via `Nats-Schedule` headers
    /// (NATS server 2.14+). When `true`, publishing a message with a
    /// `Nats-Schedule` header causes the server to repeatedly re-publish it
    /// to the `Nats-Schedule-Target` subject on the given schedule.
    ///
    /// Implicitly enables `allow_rollup_hdrs`. Cannot be set on mirror or
    /// source streams. Once enabled it cannot be disabled.
    /// See [`nats_wasip3::schedule`] for the client-side API.
    #[serde(default, skip_serializing_if = "is_false")]
    pub allow_msg_schedules: bool,
    /// Mirror another stream into this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mirror: Option<StreamSource>,
    /// Additional streams whose subjects are sourced into this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<StreamSource>>,
    /// Republish rule: matching messages are re-published to a second subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub republish: Option<Republish>,
    /// Single subject transform applied to all subjects stored on this stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_transform: Option<SubjectTransform>,
    /// Cluster placement directives.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<Placement>,
    /// Permanently seal the stream, preventing any further write or delete operations.
    #[serde(default, skip_serializing_if = "is_false")]
    pub sealed: bool,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

/// Source reference for mirror / sources stream configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamSource {
    /// Name of the source stream.
    pub name: String,
    /// Start replicating from this sequence (inclusive). `None` = from the start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_seq: Option<u64>,
    /// Start replicating from this RFC 3339 timestamp. `None` = from the start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_time: Option<String>,
    /// Filter to a single subject (simple case; use `subject_transforms` for
    /// multi-subject fan-in).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_subject: Option<String>,
    /// Subject transforms applied to messages from this source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject_transforms: Option<Vec<SubjectTransform>>,
    /// External stream reference for hub-and-spoke / leaf-node topologies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external: Option<ExternalStream>,
}

/// Reference to a stream accessible through a leaf-node or account import.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ExternalStream {
    /// The JetStream API subject prefix used to reach the remote server.
    pub api: String,
    /// The delivery subject prefix for push consumers on the remote stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver: Option<String>,
}

/// Republish rule: messages whose subject matches `src` are re-published to `dest`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Republish {
    /// Source subject filter (supports wildcards).
    pub src: String,
    /// Destination subject (tokens from `src` wildcards are substituted).
    pub dest: String,
    /// When `true` only headers are republished, not the payload.
    #[serde(default)]
    pub headers_only: bool,
}

/// Maps one subject pattern to another.
/// Used in `StreamConfig::subject_transform` and `StreamSource::subject_transforms`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SubjectTransform {
    /// Source subject filter (supports wildcards). `None` matches everything.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub src: Option<String>,
    /// Destination subject template (use `{{wildcard(N)}}` tokens).
    pub dest: String,
}

/// Directs the server to place a stream's leader in a specific cluster or
/// on nodes with specific tags.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Placement {
    /// Preferred cluster name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster: Option<String>,
    /// Required server tags. All listed tags must be present on the chosen node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Retention {
    #[default]
    Limits,
    Interest,
    Workqueue,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Storage {
    #[default]
    File,
    Memory,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscardPolicy {
    #[default]
    Old,
    New,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct StreamInfo {
    pub config: StreamConfig,
    pub state: StreamState,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct StreamState {
    pub messages: u64,
    pub bytes: u64,
    pub first_seq: u64,
    pub last_seq: u64,
    pub consumer_count: u32,
}

#[derive(Debug, Deserialize)]
struct DeleteResponse {
    success: bool,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct PurgeResponse {
    pub success: bool,
    pub purged: u64,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct PubAck {
    pub stream: String,
    pub seq: u64,
    #[serde(default)]
    pub duplicate: bool,
}

// ── Consumer types ─────────────────────────────────────────────────

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsumerConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durable_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filter_subjects: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deliver_subject: Option<String>,
    #[serde(default)]
    pub deliver_policy: DeliverPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_seq: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_time: Option<String>,
    #[serde(default)]
    pub ack_policy: AckPolicy,
    #[serde(default)]
    pub max_deliver: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_wait: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_ack_pending: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replay_policy: Option<ReplayPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_storage: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_control: Option<bool>,
    #[serde(
        default,
        rename = "idle_heartbeat",
        skip_serializing_if = "Option::is_none"
    )]
    pub idle_heartbeat_nanos: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub headers_only: Option<bool>,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliverPolicy {
    #[default]
    All,
    Last,
    New,
    #[serde(rename = "by_start_sequence")]
    ByStartSequence,
    #[serde(rename = "by_start_time")]
    ByStartTime,
    #[serde(rename = "last_per_subject")]
    LastPerSubject,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPolicy {
    #[default]
    Instant,
    Original,
}

#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckPolicy {
    None,
    #[default]
    Explicit,
    All,
}

#[non_exhaustive]
#[derive(Debug, Deserialize)]
pub struct ConsumerInfo {
    pub name: String,
    pub config: ConsumerConfig,
    pub num_pending: u64,
    pub num_ack_pending: u64,
}

// ── Stream message (from MSG.GET) ──────────────────────────────────

/// A message retrieved from a stream by sequence number.
#[derive(Debug)]
pub struct StreamMessage {
    pub subject: String,
    pub seq: u64,
    pub data: Vec<u8>,
    /// Raw base64-encoded NATS headers (if present).
    pub headers_b64: Option<String>,
    /// Server-side publish timestamp in RFC 3339 format
    /// (e.g. `"2024-01-15T12:34:56.789456789Z"`), as returned by the server.
    pub time: Option<String>,
}

// ── Helpers ────────────────────────────────────────────────────────

pub fn base64_decode(input: &str) -> Result<Vec<u8>, Error> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .map_err(|e| Error::Protocol(format!("base64: {e}")))
}

// ── Direct message (from DIRECT.GET) ───────────────────────────────

/// A message retrieved directly from a stream via Direct Get.
#[derive(Debug, Clone)]
pub struct DirectMessage {
    pub subject: String,
    pub sequence: u64,
    pub timestamp: Option<String>,
    pub headers: Option<Headers>,
    pub payload: Vec<u8>,
}

// ── ACK metadata ───────────────────────────────────────────────────

/// Parsed ACK metadata extracted from JetStream delivery reply-to subjects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MsgMetadata {
    pub stream: String,
    pub consumer: String,
    pub stream_sequence: u64,
    pub consumer_sequence: u64,
    pub num_delivered: u64,
    pub num_pending: u64,
    pub timestamp_nanos: u64,
    pub domain: Option<String>,
    pub account: Option<String>,
}

impl MsgMetadata {
    /// Parse ACK metadata from a NATS reply-to subject string.
    pub fn parse(reply: &str) -> Option<Self> {
        let tokens: Vec<&str> = reply.split('.').collect();
        match tokens.len() {
            // v1: $JS.ACK.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts_nanos>.<pending>
            9 => {
                if tokens[0] != "$JS" || tokens[1] != "ACK" {
                    return None;
                }
                Some(MsgMetadata {
                    stream: tokens[2].to_string(),
                    consumer: tokens[3].to_string(),
                    num_delivered: tokens[4].parse().ok()?,
                    stream_sequence: tokens[5].parse().ok()?,
                    consumer_sequence: tokens[6].parse().ok()?,
                    timestamp_nanos: tokens[7].parse().ok()?,
                    num_pending: tokens[8].parse().ok()?,
                    domain: None,
                    account: None,
                })
            }
            // v2: $JS.ACK.<domain>.<account>.<stream>.<consumer>.<delivered>.<stream_seq>.<consumer_seq>.<ts_nanos>.<pending>.<token>
            12 => {
                if tokens[0] != "$JS" || tokens[1] != "ACK" {
                    return None;
                }
                Some(MsgMetadata {
                    domain: if tokens[2].is_empty() { None } else { Some(tokens[2].to_string()) },
                    account: if tokens[3].is_empty() { None } else { Some(tokens[3].to_string()) },
                    stream: tokens[4].to_string(),
                    consumer: tokens[5].to_string(),
                    num_delivered: tokens[6].parse().ok()?,
                    stream_sequence: tokens[7].parse().ok()?,
                    consumer_sequence: tokens[8].parse().ok()?,
                    timestamp_nanos: tokens[9].parse().ok()?,
                    num_pending: tokens[10].parse().ok()?,
                })
            }
            _ => None,
        }
    }
}

// ── Ordered consumer ───────────────────────────────────────────────

/// Configuration for an [`OrderedConsumer`].
#[derive(Debug, Clone, Default)]
pub struct OrderedConsumerConfig {
    pub filter_subject: Option<String>,
    pub opt_start_seq: Option<u64>,
    pub opt_start_time: Option<String>,
    pub deliver_policy: DeliverPolicy,
    pub replay_policy: ReplayPolicy,
}

/// An ordered push consumer that provides ordered delivery and transparently
/// recovers from sequence gaps by recreating an ephemeral consumer from the
/// last received sequence.
pub struct OrderedConsumer {
    js: JetStream,
    stream: String,
    config: OrderedConsumerConfig,
    deliver_subject: String,
    sub: Subscription,
    consumer_name: String,
    last_stream_seq: u64,
    last_consumer_seq: u64,
}

impl OrderedConsumer {
    /// Create a new ordered consumer.
    pub async fn new(
        js: JetStream,
        stream: impl Into<String>,
        config: OrderedConsumerConfig,
    ) -> Result<Self, Error> {
        let stream = stream.into();
        let deliver_subject = js.client.new_inbox();
        let sub = js.client.subscribe(&deliver_subject)?;

        let consumer_cfg = ConsumerConfig {
            deliver_subject: Some(deliver_subject.clone()),
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            flow_control: Some(true),
            idle_heartbeat_nanos: Some(crate::client::secs(5)),
            filter_subject: config.filter_subject.clone(),
            opt_start_seq: config.opt_start_seq,
            opt_start_time: config.opt_start_time.clone(),
            deliver_policy: config.deliver_policy,
            replay_policy: Some(config.replay_policy),
            ..Default::default()
        };

        let info = js.create_consumer(&stream, &consumer_cfg).await?;

        Ok(Self {
            js,
            stream,
            config,
            deliver_subject,
            sub,
            consumer_name: info.name,
            last_stream_seq: 0,
            last_consumer_seq: 0,
        })
    }

    /// Read the next ordered message, transparently recreating the consumer if a gap is detected.
    pub async fn next(&mut self) -> Result<Message, Error> {
        loop {
            let msg = match self.sub.next().await {
                Ok(m) => m,
                Err(_) => {
                    self.reset_consumer().await?;
                    continue;
                }
            };

            // Handle idle heartbeats & flow control
            if let Some(ref h) = msg.headers {
                if h.status == Some(100) {
                    if let Some(ref reply) = msg.reply_to {
                        let _ = self.js.client.publish(reply, b"");
                    }
                    continue;
                }
            }

            // Extract metadata from reply-to subject
            if let Some(ref reply) = msg.reply_to {
                if let Some(meta) = MsgMetadata::parse(reply) {
                    if self.last_consumer_seq > 0 && meta.consumer_sequence != self.last_consumer_seq + 1 {
                        // Gap detected in consumer sequence! Reset consumer.
                        self.reset_consumer().await?;
                        continue;
                    }
                    self.last_stream_seq = meta.stream_sequence;
                    self.last_consumer_seq = meta.consumer_sequence;
                }
            }

            return Ok(msg);
        }
    }

    async fn reset_consumer(&mut self) -> Result<(), Error> {
        let _ = self.sub.unsubscribe();
        let delete_subject = format!("$JS.API.CONSUMER.DELETE.{}.{}", self.stream, self.consumer_name);
        let inbox = self.js.client.new_inbox();
        let _ = self.js.client.publish_with_reply(&delete_subject, &inbox, b"");

        self.deliver_subject = self.js.client.new_inbox();
        self.sub = self.js.client.subscribe(&self.deliver_subject)?;
        self.last_consumer_seq = 0;

        let start_seq = if self.last_stream_seq > 0 {
            Some(self.last_stream_seq + 1)
        } else {
            self.config.opt_start_seq
        };

        let consumer_cfg = ConsumerConfig {
            deliver_subject: Some(self.deliver_subject.clone()),
            ack_policy: AckPolicy::None,
            max_deliver: 1,
            flow_control: Some(true),
            idle_heartbeat_nanos: Some(crate::client::secs(5)),
            filter_subject: self.config.filter_subject.clone(),
            opt_start_seq: start_seq,
            deliver_policy: if start_seq.is_some() {
                DeliverPolicy::ByStartSequence
            } else {
                self.config.deliver_policy
            },
            replay_policy: Some(self.config.replay_policy),
            ..Default::default()
        };

        let info = self.js.create_consumer(&self.stream, &consumer_cfg).await?;
        self.consumer_name = info.name;
        Ok(())
    }
}

impl Drop for OrderedConsumer {
    fn drop(&mut self) {
        let _ = self.sub.unsubscribe();
        let subject = format!("$JS.API.CONSUMER.DELETE.{}.{}", self.stream, self.consumer_name);
        let inbox = self.js.client.new_inbox();
        let _ = self.js.client.publish_with_reply(&subject, &inbox, b"");
    }
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_metadata_parsing_v1() {
        let reply = "$JS.ACK.mystream.myconsumer.1.100.200.1700000000000000000.5";
        let meta = MsgMetadata::parse(reply).expect("parse metadata");
        assert_eq!(meta.stream, "mystream");
        assert_eq!(meta.consumer, "myconsumer");
        assert_eq!(meta.num_delivered, 1);
        assert_eq!(meta.stream_sequence, 100);
        assert_eq!(meta.consumer_sequence, 200);
        assert_eq!(meta.timestamp_nanos, 1700000000000000000);
        assert_eq!(meta.num_pending, 5);
        assert_eq!(meta.domain, None);
        assert_eq!(meta.account, None);
    }

    #[test]
    fn test_ack_metadata_parsing_v2() {
        let reply = "$JS.ACK.hub.acc1.orders.cons1.2.42.84.1700000000000000123.10.tok123";
        let meta = MsgMetadata::parse(reply).expect("parse metadata");
        assert_eq!(meta.domain.as_deref(), Some("hub"));
        assert_eq!(meta.account.as_deref(), Some("acc1"));
        assert_eq!(meta.stream, "orders");
        assert_eq!(meta.consumer, "cons1");
        assert_eq!(meta.num_delivered, 2);
        assert_eq!(meta.stream_sequence, 42);
        assert_eq!(meta.consumer_sequence, 84);
        assert_eq!(meta.timestamp_nanos, 1700000000000000123);
        assert_eq!(meta.num_pending, 10);
    }
}

