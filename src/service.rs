//! NATS Microservices Framework ([ADR-32]).
//!
//! Provides structured microservice definition, endpoint routing, group hierarchies,
//! discovery verbs (`$SRV.PING`, `$SRV.INFO`, `$SRV.STATS`, `$SRV.SCHEMA`), and
//! standardized request/error handling.
//!
//! [ADR-32]: https://github.com/nats-io/nats-architecture-and-design/blob/main/adr/ADR-32.md

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::client::{Client, Message, Subscription};
use crate::proto::Headers;
use crate::Error;

/// Type identifier for PING response.
pub const PING_RESPONSE_TYPE: &str = "io.nats.micro.v1.ping_response";
/// Type identifier for INFO response.
pub const INFO_RESPONSE_TYPE: &str = "io.nats.micro.v1.info_response";
/// Type identifier for STATS response.
pub const STATS_RESPONSE_TYPE: &str = "io.nats.micro.v1.stats_response";
/// Type identifier for SCHEMA response.
pub const SCHEMA_RESPONSE_TYPE: &str = "io.nats.micro.v1.schema_response";

/// Standard NATS Microservice error code header.
pub const HEADER_ERROR_CODE: &str = "Nats-Service-Error-Code";
/// Standard NATS Microservice error description header.
pub const HEADER_ERROR_DESCRIPTION: &str = "Nats-Service-Error";

// ── Service Configuration & Builder ────────────────────────────────

/// Configuration for creating a [`Service`].
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    /// Human-readable service name (e.g. `"orders"`, `"payment-processor"`).
    pub name: String,
    /// Service version in SemVer format (e.g. `"1.0.0"`).
    pub version: String,
    /// Service description.
    pub description: Option<String>,
    /// Unique instance identifier. If `None`, a random 16-character hex ID is generated.
    pub id: Option<String>,
    /// Optional service-level metadata key-value pairs.
    pub metadata: HashMap<String, String>,
    /// Default queue group for endpoints. Defaults to `"q"`.
    pub queue_group: Option<String>,
}

impl ServiceConfig {
    /// Create a new service configuration with the given name and version.
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            description: None,
            id: None,
            metadata: HashMap::new(),
            queue_group: Some("q".to_string()),
        }
    }

    /// Set service description.
    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    /// Set service instance ID.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set default queue group.
    pub fn queue_group(mut self, q: impl Into<String>) -> Self {
        self.queue_group = Some(q.into());
        self
    }

    /// Add metadata key-value pair.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

// ── Endpoint Configuration ─────────────────────────────────────────

/// Configuration for a service endpoint.
#[derive(Debug, Clone)]
pub struct EndpointConfig {
    /// Name of the endpoint.
    pub name: String,
    /// Subject on which this endpoint listens. If `None`, defaults to `name`.
    pub subject: Option<String>,
    /// Queue group for load-balanced delivery. Defaults to service queue group.
    pub queue_group: Option<String>,
    /// Endpoint-specific metadata.
    pub metadata: HashMap<String, String>,
    /// Optional request schema (URI or description).
    pub request_schema: Option<String>,
    /// Optional response schema (URI or description).
    pub response_schema: Option<String>,
}

impl EndpointConfig {
    /// Create an endpoint configuration with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            subject: None,
            queue_group: None,
            metadata: HashMap::new(),
            request_schema: None,
            response_schema: None,
        }
    }

    /// Set listen subject.
    pub fn subject(mut self, subj: impl Into<String>) -> Self {
        self.subject = Some(subj.into());
        self
    }

    /// Set queue group.
    pub fn queue_group(mut self, q: impl Into<String>) -> Self {
        self.queue_group = Some(q.into());
        self
    }

    /// Set request schema.
    pub fn request_schema(mut self, schema: impl Into<String>) -> Self {
        self.request_schema = Some(schema.into());
        self
    }

    /// Set response schema.
    pub fn response_schema(mut self, schema: impl Into<String>) -> Self {
        self.response_schema = Some(schema.into());
        self
    }

    /// Add metadata.
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

// ── Telemetry & Stats ──────────────────────────────────────────────

#[derive(Debug, Default)]
struct EndpointStatsInner {
    num_requests: AtomicU64,
    num_errors: AtomicU64,
    total_time_nanos: AtomicU64,
    last_error: RefCell<Option<String>>,
    data: RefCell<Option<serde_json::Value>>,
}

/// Statistics for a single service endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointStats {
    pub name: String,
    pub num_requests: u64,
    pub num_errors: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    pub processing_time: u64,
    pub average_processing_time: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

/// Public endpoint schema information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointSchema {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,
}

/// Public endpoint information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointInfo {
    pub name: String,
    pub subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_group: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
}

// ── Control Protocol Messages ──────────────────────────────────────

/// PING response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PingResponse {
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    #[serde(rename = "type")]
    pub response_type: String,
}

/// INFO response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfoResponse {
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    pub endpoints: Vec<EndpointInfo>,
    #[serde(rename = "type")]
    pub response_type: String,
}

/// STATS response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatsResponse {
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    #[serde(rename = "type")]
    pub response_type: String,
    pub started: String,
    pub endpoints: Vec<EndpointStats>,
}

/// SCHEMA response payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaResponse {
    pub name: String,
    pub id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,
    #[serde(rename = "type")]
    pub response_type: String,
    pub endpoints: Vec<EndpointSchema>,
}

// ── Service Struct ─────────────────────────────────────────────────

struct EndpointEntry {
    config: EndpointConfig,
    stats: Rc<EndpointStatsInner>,
}

/// A running NATS Microservice instance adhering to ADR-32.
pub struct Service {
    client: Client,
    config: ServiceConfig,
    started_rfc3339: String,
    endpoints: Rc<RefCell<Vec<EndpointEntry>>>,
}

impl Service {
    /// Create and start a new NATS Microservice.
    ///
    /// Automatically subscribes to all standard ADR-32 discovery verbs:
    /// - `$SRV.PING`, `$SRV.PING.<name>`, `$SRV.PING.<name>.<id>`
    /// - `$SRV.INFO`, `$SRV.INFO.<name>`, `$SRV.INFO.<name>.<id>`
    /// - `$SRV.STATS`, `$SRV.STATS.<name>`, `$SRV.STATS.<name>.<id>`
    /// - `$SRV.SCHEMA`, `$SRV.SCHEMA.<name>`, `$SRV.SCHEMA.<name>.<id>`
    pub async fn add(client: Client, mut config: ServiceConfig) -> Result<Self, Error> {
        let id = config.id.take().unwrap_or_else(generate_service_id);
        config.id = Some(id.clone());

        let name = &config.name;
        let started_rfc3339 = get_now_rfc3339();

        let endpoints = Rc::new(RefCell::new(Vec::new()));
        let mut control_subs = Vec::new();

        // Subscribe to control verbs.
        let verbs = ["PING", "INFO", "STATS", "SCHEMA"];
        for verb in verbs {
            let subjects = [
                format!("$SRV.{verb}"),
                format!("$SRV.{verb}.{name}"),
                format!("$SRV.{verb}.{name}.{id}"),
            ];
            for subj in subjects {
                let sub = client.subscribe(&subj)?;
                control_subs.push(sub);
            }
        }

        let svc = Self {
            client: client.clone(),
            config: config.clone(),
            started_rfc3339: started_rfc3339.clone(),
            endpoints: Rc::clone(&endpoints),
        };

        // Spawn background control handler subtasks.
        spawn_control_handlers(
            control_subs,
            client,
            config,
            id,
            started_rfc3339,
            endpoints,
        );

        Ok(svc)
    }

    /// Service unique ID.
    pub fn id(&self) -> &str {
        self.config.id.as_deref().unwrap_or_default()
    }

    /// Service name.
    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Service version.
    pub fn version(&self) -> &str {
        &self.config.version
    }

    /// Service description.
    pub fn description(&self) -> Option<&str> {
        self.config.description.as_deref()
    }

    /// Service started timestamp in RFC 3339 format.
    pub fn started(&self) -> &str {
        &self.started_rfc3339
    }

    /// Create a routing group prefixed with `prefix`.
    pub fn group(&self, prefix: impl Into<String>) -> Group {
        Group {
            service_client: self.client.clone(),
            service_queue_group: self.config.queue_group.clone(),
            prefix: prefix.into(),
            queue_group: None,
            endpoints: Rc::clone(&self.endpoints),
        }
    }

    /// Add a top-level endpoint to the service.
    pub async fn add_endpoint(&self, config: EndpointConfig) -> Result<EndpointSubscription, Error> {
        let subject = config.subject.clone().unwrap_or_else(|| config.name.clone());
        let queue_group = config
            .queue_group
            .clone()
            .or_else(|| self.config.queue_group.clone());

        let sub = match &queue_group {
            Some(q) => self.client.subscribe_queue(&subject, q)?,
            None => self.client.subscribe(&subject)?,
        };

        let stats = Rc::new(EndpointStatsInner::default());
        let mut resolved_cfg = config;
        resolved_cfg.subject = Some(subject);
        resolved_cfg.queue_group = queue_group;

        self.endpoints.borrow_mut().push(EndpointEntry {
            config: resolved_cfg.clone(),
            stats: Rc::clone(&stats),
        });

        Ok(EndpointSubscription {
            client: self.client.clone(),
            sub,
            stats,
        })
    }

    /// Build current [`InfoResponse`].
    pub fn info(&self) -> InfoResponse {
        let endpoints: Vec<EndpointInfo> = self
            .endpoints
            .borrow()
            .iter()
            .map(|e| EndpointInfo {
                name: e.config.name.clone(),
                subject: e.config.subject.clone().unwrap_or_default(),
                queue_group: e.config.queue_group.clone(),
                metadata: e.config.metadata.clone(),
            })
            .collect();

        InfoResponse {
            name: self.config.name.clone(),
            id: self.id().to_string(),
            version: self.config.version.clone(),
            description: self.config.description.clone(),
            metadata: self.config.metadata.clone(),
            endpoints,
            response_type: INFO_RESPONSE_TYPE.to_string(),
        }
    }

    /// Build current [`StatsResponse`].
    pub fn stats(&self) -> StatsResponse {
        let endpoints: Vec<EndpointStats> = self
            .endpoints
            .borrow()
            .iter()
            .map(|e| {
                let reqs = e.stats.num_requests.load(Ordering::Relaxed);
                let errors = e.stats.num_errors.load(Ordering::Relaxed);
                let time = e.stats.total_time_nanos.load(Ordering::Relaxed);
                let avg = if reqs > 0 { time / reqs } else { 0 };
                let last_err = e.stats.last_error.borrow().clone();
                let data = e.stats.data.borrow().clone();

                EndpointStats {
                    name: e.config.name.clone(),
                    num_requests: reqs,
                    num_errors: errors,
                    last_error: last_err,
                    processing_time: time,
                    average_processing_time: avg,
                    data,
                }
            })
            .collect();

        StatsResponse {
            name: self.config.name.clone(),
            id: self.id().to_string(),
            version: self.config.version.clone(),
            metadata: self.config.metadata.clone(),
            response_type: STATS_RESPONSE_TYPE.to_string(),
            started: self.started_rfc3339.clone(),
            endpoints,
        }
    }

    /// Build current [`PingResponse`].
    pub fn ping(&self) -> PingResponse {
        PingResponse {
            name: self.config.name.clone(),
            id: self.id().to_string(),
            version: self.config.version.clone(),
            metadata: self.config.metadata.clone(),
            response_type: PING_RESPONSE_TYPE.to_string(),
        }
    }

    /// Build current [`SchemaResponse`].
    pub fn schema(&self) -> SchemaResponse {
        let endpoints: Vec<EndpointSchema> = self
            .endpoints
            .borrow()
            .iter()
            .map(|e| EndpointSchema {
                name: e.config.name.clone(),
                request: e.config.request_schema.clone(),
                response: e.config.response_schema.clone(),
            })
            .collect();

        SchemaResponse {
            name: self.config.name.clone(),
            id: self.id().to_string(),
            version: self.config.version.clone(),
            metadata: self.config.metadata.clone(),
            response_type: SCHEMA_RESPONSE_TYPE.to_string(),
            endpoints,
        }
    }

    /// Stop the service by clearing registered endpoints.
    pub fn stop(&mut self) {
        self.endpoints.borrow_mut().clear();
    }
}

fn spawn_control_handlers(
    control_subs: Vec<Subscription>,
    client: Client,
    config: ServiceConfig,
    id: String,
    started: String,
    endpoints: Rc<RefCell<Vec<EndpointEntry>>>,
) {
    let name = config.name.clone();
    let version = config.version.clone();
    let description = config.description.clone();
    let metadata = config.metadata.clone();

    for sub in control_subs {
        let name = name.clone();
        let id = id.clone();
        let version = version.clone();
        let description = description.clone();
        let metadata = metadata.clone();
        let started = started.clone();
        let client = client.clone();
        let endpoints = Rc::clone(&endpoints);

        wit_bindgen::spawn(async move {
            while let Ok(msg) = sub.next().await {
                let reply = match msg.reply_to.as_deref() {
                    Some(r) => r,
                    None => continue,
                };

                let subject = &msg.subject;
                let payload = if subject.starts_with("$SRV.PING") {
                    let resp = PingResponse {
                        name: name.clone(),
                        id: id.clone(),
                        version: version.clone(),
                        metadata: metadata.clone(),
                        response_type: PING_RESPONSE_TYPE.to_string(),
                    };
                    serde_json::to_vec(&resp).unwrap_or_default()
                } else if subject.starts_with("$SRV.INFO") {
                    let eps: Vec<EndpointInfo> = endpoints
                        .borrow()
                        .iter()
                        .map(|e| EndpointInfo {
                            name: e.config.name.clone(),
                            subject: e.config.subject.clone().unwrap_or_default(),
                            queue_group: e.config.queue_group.clone(),
                            metadata: e.config.metadata.clone(),
                        })
                        .collect();
                    let resp = InfoResponse {
                        name: name.clone(),
                        id: id.clone(),
                        version: version.clone(),
                        description: description.clone(),
                        metadata: metadata.clone(),
                        endpoints: eps,
                        response_type: INFO_RESPONSE_TYPE.to_string(),
                    };
                    serde_json::to_vec(&resp).unwrap_or_default()
                } else if subject.starts_with("$SRV.STATS") {
                    let eps: Vec<EndpointStats> = endpoints
                        .borrow()
                        .iter()
                        .map(|e| {
                            let reqs = e.stats.num_requests.load(Ordering::Relaxed);
                            let errors = e.stats.num_errors.load(Ordering::Relaxed);
                            let time = e.stats.total_time_nanos.load(Ordering::Relaxed);
                            let avg = if reqs > 0 { time / reqs } else { 0 };
                            let last_err = e.stats.last_error.borrow().clone();
                            let data = e.stats.data.borrow().clone();

                            EndpointStats {
                                name: e.config.name.clone(),
                                num_requests: reqs,
                                num_errors: errors,
                                last_error: last_err,
                                processing_time: time,
                                average_processing_time: avg,
                                data,
                            }
                        })
                        .collect();
                    let resp = StatsResponse {
                        name: name.clone(),
                        id: id.clone(),
                        version: version.clone(),
                        metadata: metadata.clone(),
                        response_type: STATS_RESPONSE_TYPE.to_string(),
                        started: started.clone(),
                        endpoints: eps,
                    };
                    serde_json::to_vec(&resp).unwrap_or_default()
                } else if subject.starts_with("$SRV.SCHEMA") {
                    let eps: Vec<EndpointSchema> = endpoints
                        .borrow()
                        .iter()
                        .map(|e| EndpointSchema {
                            name: e.config.name.clone(),
                            request: e.config.request_schema.clone(),
                            response: e.config.response_schema.clone(),
                        })
                        .collect();
                    let resp = SchemaResponse {
                        name: name.clone(),
                        id: id.clone(),
                        version: version.clone(),
                        metadata: metadata.clone(),
                        response_type: SCHEMA_RESPONSE_TYPE.to_string(),
                        endpoints: eps,
                    };
                    serde_json::to_vec(&resp).unwrap_or_default()
                } else {
                    continue;
                };

                let _ = client.publish(reply, &payload);
            }
        });
    }
}

// ── Routing Group ──────────────────────────────────────────────────

/// Hierarchical group builder for organizing endpoints under shared subject prefixes.
pub struct Group {
    service_client: Client,
    service_queue_group: Option<String>,
    prefix: String,
    queue_group: Option<String>,
    endpoints: Rc<RefCell<Vec<EndpointEntry>>>,
}

impl Group {
    /// Set a custom queue group for all endpoints within this group.
    pub fn queue_group(mut self, q: impl Into<String>) -> Self {
        self.queue_group = Some(q.into());
        self
    }

    /// Create a child group prefixed with `prefix`.
    pub fn group(&self, prefix: impl Into<String>) -> Group {
        let child_prefix = format!("{}.{}", self.prefix, prefix.into());
        Group {
            service_client: self.service_client.clone(),
            service_queue_group: self.service_queue_group.clone(),
            prefix: child_prefix,
            queue_group: self.queue_group.clone(),
            endpoints: Rc::clone(&self.endpoints),
        }
    }

    /// Add an endpoint to this group.
    pub async fn add_endpoint(
        &self,
        config: EndpointConfig,
    ) -> Result<EndpointSubscription, Error> {
        let subject = match &config.subject {
            Some(s) => format!("{}.{}", self.prefix, s),
            None => format!("{}.{}", self.prefix, config.name),
        };

        let queue_group = config
            .queue_group
            .clone()
            .or_else(|| self.queue_group.clone())
            .or_else(|| self.service_queue_group.clone());

        let sub = match &queue_group {
            Some(q) => self.service_client.subscribe_queue(&subject, q)?,
            None => self.service_client.subscribe(&subject)?,
        };

        let stats = Rc::new(EndpointStatsInner::default());
        let mut resolved_cfg = config;
        resolved_cfg.subject = Some(subject);
        resolved_cfg.queue_group = queue_group;

        self.endpoints.borrow_mut().push(EndpointEntry {
            config: resolved_cfg.clone(),
            stats: Rc::clone(&stats),
        });

        Ok(EndpointSubscription {
            client: self.service_client.clone(),
            sub,
            stats,
        })
    }
}

// ── Endpoint Subscription & Service Request ────────────────────────

/// Subscription stream yielding incoming [`ServiceRequest`]s for an endpoint.
pub struct EndpointSubscription {
    client: Client,
    sub: Subscription,
    stats: Rc<EndpointStatsInner>,
}

impl EndpointSubscription {
    /// Receive the next request from this endpoint.
    pub async fn next(&self) -> Result<ServiceRequest, Error> {
        let msg = self.sub.next().await?;
        let start_time_nanos = get_now_nanos();
        self.stats.num_requests.fetch_add(1, Ordering::Relaxed);

        Ok(ServiceRequest {
            message: msg,
            client: self.client.clone(),
            stats: Rc::clone(&self.stats),
            start_time_nanos,
        })
    }

    /// Custom endpoint data reporter.
    pub fn set_data(&self, data: serde_json::Value) {
        *self.stats.data.borrow_mut() = Some(data);
    }
}

/// An incoming request to a service endpoint.
pub struct ServiceRequest {
    /// The incoming NATS message.
    pub message: Message,
    client: Client,
    stats: Rc<EndpointStatsInner>,
    start_time_nanos: u64,
}

impl ServiceRequest {
    /// Request subject.
    pub fn subject(&self) -> &str {
        &self.message.subject
    }

    /// Request payload.
    pub fn payload(&self) -> &[u8] {
        &self.message.payload
    }

    /// Request headers.
    pub fn headers(&self) -> Option<&Headers> {
        self.message.headers.as_ref()
    }

    /// Reply subject (if any).
    pub fn reply(&self) -> Option<&str> {
        self.message.reply_to.as_deref()
    }

    /// Send a successful reply.
    pub fn respond(&self, payload: &[u8]) -> Result<(), Error> {
        self.record_timing();
        if let Some(reply) = self.reply() {
            self.client.publish(reply, payload)?;
        }
        Ok(())
    }

    /// Send a reply with headers.
    pub fn respond_with_headers(&self, headers: &Headers, payload: &[u8]) -> Result<(), Error> {
        self.record_timing();
        if let Some(reply) = self.reply() {
            self.client
                .publish_with_headers(reply, None, headers, payload)?;
        }
        Ok(())
    }

    /// Respond with a standardized ADR-32 service error.
    ///
    /// Sets headers:
    /// - `Nats-Service-Error-Code`: e.g. `"404"` or `"500"`
    /// - `Nats-Service-Error`: error message description
    pub fn respond_error(&self, code: u16, description: &str) -> Result<(), Error> {
        self.stats.num_errors.fetch_add(1, Ordering::Relaxed);
        *self.stats.last_error.borrow_mut() = Some(description.to_string());
        self.record_timing();

        if let Some(reply) = self.reply() {
            let mut headers = Headers::new();
            headers.insert(HEADER_ERROR_CODE, code.to_string());
            headers.insert(HEADER_ERROR_DESCRIPTION, description);
            self.client
                .publish_with_headers(reply, None, &headers, description.as_bytes())?;
        }
        Ok(())
    }

    fn record_timing(&self) {
        let elapsed = get_now_nanos().saturating_sub(self.start_time_nanos);
        self.stats
            .total_time_nanos
            .fetch_add(elapsed, Ordering::Relaxed);
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn generate_service_id() -> String {
    use std::sync::atomic::AtomicU64;
    static CTR: AtomicU64 = AtomicU64::new(1);
    let id = CTR.fetch_add(1, Ordering::Relaxed);
    let now = get_now_nanos();
    format!("{:016x}{:08x}", now, id)
}

fn get_now_nanos() -> u64 {
    #[cfg(target_arch = "wasm32")]
    {
        wasip3::clocks::monotonic_clock::now()
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        use std::time::SystemTime;
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0)
    }
}

fn get_now_rfc3339() -> String {
    crate::schedule::now_rfc3339()
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_config_and_id() {
        let cfg = ServiceConfig::new("echo", "1.0.0")
            .description("echo service")
            .metadata("env", "prod")
            .queue_group("workers");

        assert_eq!(cfg.name, "echo");
        assert_eq!(cfg.version, "1.0.0");
        assert_eq!(cfg.description.as_deref(), Some("echo service"));
        assert_eq!(cfg.metadata.get("env").map(|s| s.as_str()), Some("prod"));
        assert_eq!(cfg.queue_group.as_deref(), Some("workers"));

        let id = generate_service_id();
        assert!(!id.is_empty());
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_stats_aggregation() {
        let stats = EndpointStatsInner::default();
        stats.num_requests.fetch_add(2, Ordering::Relaxed);
        stats.num_errors.fetch_add(1, Ordering::Relaxed);
        stats.total_time_nanos.fetch_add(10_000, Ordering::Relaxed);
        *stats.last_error.borrow_mut() = Some("database timeout".to_string());

        let reqs = stats.num_requests.load(Ordering::Relaxed);
        let errs = stats.num_errors.load(Ordering::Relaxed);
        let time = stats.total_time_nanos.load(Ordering::Relaxed);
        let avg = time / reqs;

        let ep_stats = EndpointStats {
            name: "test".to_string(),
            num_requests: reqs,
            num_errors: errs,
            last_error: stats.last_error.borrow().clone(),
            processing_time: time,
            average_processing_time: avg,
            data: None,
        };

        assert_eq!(ep_stats.num_requests, 2);
        assert_eq!(ep_stats.num_errors, 1);
        assert_eq!(ep_stats.average_processing_time, 5000);
        assert_eq!(ep_stats.last_error.as_deref(), Some("database timeout"));
    }

    #[test]
    fn test_serialization_responses() {
        let ping = PingResponse {
            name: "test".to_string(),
            id: "123".to_string(),
            version: "0.1.0".to_string(),
            metadata: HashMap::new(),
            response_type: PING_RESPONSE_TYPE.to_string(),
        };
        let json = serde_json::to_string(&ping).unwrap();
        assert!(json.contains("io.nats.micro.v1.ping_response"));
    }
}
