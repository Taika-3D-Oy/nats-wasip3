//! JetStream API layer.
//!
//! JetStream operations are performed via JSON request/reply on `$JS.API.*`
//! subjects. This module provides typed wrappers around those API calls.

use serde::{Deserialize, Serialize};

use crate::client::{Client, Message, Duration, secs};
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

    /// Create or update a stream.
    pub async fn create_stream(&self, config: &StreamConfig) -> Result<StreamInfo, Error> {
        self.api_request(&format!("STREAM.CREATE.{}", config.name), config)
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
        let timeout = js_api_timeout();
        loop {
            let msg = match with_timeout(timeout, sub.next()).await {
                Ok(Ok(msg)) => msg,
                Ok(Err(e)) => return Err(e),    // subscription error (disconnected etc.)
                Err(_) => break,                // timeout: no more messages from server
            };
            // A 404 or 408 status means no more messages.
            if let Some(ref h) = msg.headers {
                if let Some(status) = h.status {
                    if status == 404 || status == 408 || status == 409 {
                        break;
                    }
                }
            }
            messages.push(msg);
            if messages.len() >= batch as usize {
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
        }))
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
        let subject = format!(
            "$JS.API.CONSUMER.DELETE.{}.{}",
            self.stream, self.consumer
        );
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
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_false(v: &bool) -> bool {
    !*v
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Retention {
    #[default]
    Limits,
    Interest,
    Workqueue,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Storage {
    #[default]
    File,
    Memory,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscardPolicy {
    #[default]
    Old,
    New,
}

#[derive(Debug, Deserialize)]
pub struct StreamInfo {
    pub config: StreamConfig,
    pub state: StreamState,
}

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

#[derive(Debug, Deserialize)]
pub struct PurgeResponse {
    pub success: bool,
    pub purged: u64,
}

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
    pub deliver_subject: Option<String>,
    #[serde(default)]
    pub deliver_policy: DeliverPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_start_seq: Option<u64>,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeliverPolicy {
    #[default]
    All,
    Last,
    New,
    #[serde(rename = "by_start_sequence")]
    ByStartSequence,
    #[serde(rename = "last_per_subject")]
    LastPerSubject,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReplayPolicy {
    #[default]
    Instant,
    Original,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckPolicy {
    None,
    #[default]
    Explicit,
    All,
}

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
}

// ── Helpers ────────────────────────────────────────────────────────

pub fn base64_decode(input: &str) -> Result<Vec<u8>, Error> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(input)
        .map_err(|e| Error::Protocol(format!("base64: {e}")))
}
