//! NATS wire protocol parser and serializer.
//!
//! Reference: <https://docs.nats.io/reference/reference-protocols/nats-protocol>
//!
//! The NATS protocol is text-based (lines terminated by `\r\n`) with optional
//! binary payloads of a declared length.

/// A server-to-client message parsed from the wire.
#[derive(Debug)]
pub enum ServerOp {
    Info(ServerInfo),
    Msg(Msg),
    HMsg(HMsg),
    Ping,
    Pong,
    Ok,
    Err(String),
}

/// The INFO payload sent by the server on connect.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ServerInfo {
    pub server_id: String,
    #[serde(default)]
    pub server_name: String,
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
    #[serde(default)]
    pub max_payload: usize,
    #[serde(default)]
    pub headers: bool,
    #[serde(default)]
    pub tls_required: bool,
    #[serde(default)]
    pub tls_available: bool,
    #[serde(default)]
    pub connect_urls: Vec<String>,
    #[serde(default)]
    pub nonce: Option<String>,
    #[serde(default)]
    pub jetstream: bool,
}

/// A regular MSG (no headers).
#[derive(Debug)]
pub struct Msg {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub payload: Vec<u8>,
}

/// A header-bearing HMSG.
#[derive(Debug)]
pub struct HMsg {
    pub subject: String,
    pub sid: String,
    pub reply_to: Option<String>,
    pub headers: Headers,
    pub payload: Vec<u8>,
}

/// Simple header map (case-insensitive keys).
#[derive(Debug, Clone, Default)]
pub struct Headers {
    entries: Vec<(String, String)>,
    /// Status code from the NATS status line (e.g. 503 for no responders).
    pub status: Option<u16>,
}

impl Headers {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            status: None,
        }
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.entries.push((key.into(), value.into()));
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        let key_lower = key.to_ascii_lowercase();
        self.entries
            .iter()
            .find(|(k, _)| k.to_ascii_lowercase() == key_lower)
            .map(|(_, v)| v.as_str())
    }

    /// Get all values for a header key (case-insensitive).
    pub fn get_all(&self, key: &str) -> Vec<&str> {
        let key_lower = key.to_ascii_lowercase();
        self.entries
            .iter()
            .filter(|(k, _)| k.to_ascii_lowercase() == key_lower)
            .map(|(_, v)| v.as_str())
            .collect()
    }

    /// Serialize headers into NATS header block format:
    /// `NATS/1.0\r\nKey: Value\r\n\r\n`
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"NATS/1.0\r\n");
        for (k, v) in &self.entries {
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(v.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }

    /// Encode a status-line header block: `NATS/1.0 {status}\r\n...\r\n`
    pub fn encode_with_status(&self, status: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(b"NATS/1.0 ");
        buf.extend_from_slice(status.as_bytes());
        buf.extend_from_slice(b"\r\n");
        for (k, v) in &self.entries {
            buf.extend_from_slice(k.as_bytes());
            buf.extend_from_slice(b": ");
            buf.extend_from_slice(v.as_bytes());
            buf.extend_from_slice(b"\r\n");
        }
        buf.extend_from_slice(b"\r\n");
        buf
    }
}

/// The CONNECT payload sent by the client.
#[derive(Debug, serde::Serialize)]
pub struct ConnectOptions {
    pub verbose: bool,
    pub pedantic: bool,
    pub name: Option<String>,
    pub lang: &'static str,
    pub version: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pass: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sig: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nkey: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jwt: Option<String>,
    pub headers: bool,
    pub no_responders: bool,
    pub tls_required: bool,
    pub echo: bool,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            verbose: false,
            pedantic: false,
            name: None,
            lang: "nats-wasip3",
            version: env!("CARGO_PKG_VERSION"),
            user: None,
            pass: None,
            auth_token: None,
            sig: None,
            nkey: None,
            jwt: None,
            headers: true,
            no_responders: true,
            tls_required: false,
            echo: true,
        }
    }
}

// ── Client-to-server serialization ─────────────────────────────────

/// Format a CONNECT command.
pub fn encode_connect(opts: &ConnectOptions) -> Vec<u8> {
    let json = serde_json::to_string(opts).expect("ConnectOptions is always serializable");
    format!("CONNECT {json}\r\n").into_bytes()
}

/// Format a SUB command.
pub fn encode_sub(subject: &str, sid: &str) -> Result<Vec<u8>, crate::Error> {
    validate_subject(subject)?;
    Ok(format!("SUB {subject} {sid}\r\n").into_bytes())
}

/// Format a SUB with queue group.
pub fn encode_sub_queue(subject: &str, queue: &str, sid: &str) -> Result<Vec<u8>, crate::Error> {
    validate_subject(subject)?;
    validate_subject(queue)?;
    Ok(format!("SUB {subject} {queue} {sid}\r\n").into_bytes())
}

/// Format an UNSUB command.
pub fn encode_unsub(sid: &str, max_msgs: Option<u64>) -> Vec<u8> {
    match max_msgs {
        Some(n) => format!("UNSUB {sid} {n}\r\n").into_bytes(),
        None => format!("UNSUB {sid}\r\n").into_bytes(),
    }
}

/// Format a PUB command (no headers).
pub fn encode_pub(
    subject: &str,
    reply_to: Option<&str>,
    payload: &[u8],
) -> Result<Vec<u8>, crate::Error> {
    validate_subject(subject)?;
    if let Some(r) = reply_to {
        validate_subject(r)?;
    }
    let mut buf = Vec::new();
    match reply_to {
        Some(reply) => {
            buf.extend_from_slice(
                format!("PUB {subject} {reply} {}\r\n", payload.len()).as_bytes(),
            );
        }
        None => {
            buf.extend_from_slice(format!("PUB {subject} {}\r\n", payload.len()).as_bytes());
        }
    }
    buf.extend_from_slice(payload);
    buf.extend_from_slice(b"\r\n");
    Ok(buf)
}

/// Format an HPUB command (with headers).
pub fn encode_hpub(
    subject: &str,
    reply_to: Option<&str>,
    headers: &Headers,
    payload: &[u8],
) -> Result<Vec<u8>, crate::Error> {
    validate_subject(subject)?;
    if let Some(r) = reply_to {
        validate_subject(r)?;
    }
    let hdr_bytes = headers.encode();
    let total_len = hdr_bytes.len() + payload.len();
    let hdr_len = hdr_bytes.len();

    let mut buf = Vec::new();
    match reply_to {
        Some(reply) => {
            buf.extend_from_slice(
                format!("HPUB {subject} {reply} {hdr_len} {total_len}\r\n").as_bytes(),
            );
        }
        None => {
            buf.extend_from_slice(format!("HPUB {subject} {hdr_len} {total_len}\r\n").as_bytes());
        }
    }
    buf.extend_from_slice(&hdr_bytes);
    buf.extend_from_slice(payload);
    buf.extend_from_slice(b"\r\n");
    Ok(buf)
}

pub const PING: &[u8] = b"PING\r\n";
pub const PONG: &[u8] = b"PONG\r\n";

/// Validate a NATS subject or token for illegal characters.
///
/// Subjects must not be empty or contain spaces, tabs, `\r`, or `\n`.
fn validate_subject(subject: &str) -> Result<(), crate::Error> {
    if subject.is_empty() {
        return Err(crate::Error::Protocol("subject must not be empty".into()));
    }
    if subject
        .bytes()
        .any(|b| b == b' ' || b == b'\t' || b == b'\r' || b == b'\n' || b == b'\0')
    {
        return Err(crate::Error::Protocol(format!(
            "subject contains illegal character: {subject:?}"
        )));
    }
    Ok(())
}

// ── Server-to-client parsing ───────────────────────────────────────

/// Parse one server operation from a buffered reader.
/// Returns the parsed op and the number of bytes consumed from `buf`.
pub fn parse_op(buf: &[u8]) -> Result<Option<(ServerOp, usize)>, crate::Error> {
    // Find the first \r\n to get the command line.
    let Some(line_end) = find_crlf(buf) else {
        return Ok(None); // need more data
    };

    let line = &buf[..line_end];
    let line_str = std::str::from_utf8(line)
        .map_err(|e| crate::Error::Protocol(format!("invalid utf8: {e}")))?;

    if let Some(json) = line_str.strip_prefix("INFO ") {
        let info: ServerInfo = serde_json::from_str(json)?;
        Ok(Some((ServerOp::Info(info), line_end + 2)))
    } else if line_str == "PING" {
        Ok(Some((ServerOp::Ping, line_end + 2)))
    } else if line_str == "PONG" {
        Ok(Some((ServerOp::Pong, line_end + 2)))
    } else if line_str == "+OK" {
        Ok(Some((ServerOp::Ok, line_end + 2)))
    } else if let Some(err_body) = line_str.strip_prefix("-ERR ") {
        let msg = err_body.trim_matches('\'').to_string();
        Ok(Some((ServerOp::Err(msg), line_end + 2)))
    } else if line_str.starts_with("MSG ") {
        parse_msg(line_str, buf, line_end)
    } else if line_str.starts_with("HMSG ") {
        parse_hmsg(line_str, buf, line_end)
    } else {
        Err(crate::Error::Protocol(format!(
            "unknown server op: {line_str}"
        )))
    }
}

fn parse_msg(
    line: &str,
    buf: &[u8],
    line_end: usize,
) -> Result<Option<(ServerOp, usize)>, crate::Error> {
    // MSG <subject> <sid> [reply-to] <#bytes>\r\n[payload]\r\n
    let parts: Vec<&str> = line[4..].split_whitespace().collect();
    let (subject, sid, reply_to, payload_len) = match parts.len() {
        3 => (parts[0], parts[1], None, parse_usize(parts[2])?),
        4 => (
            parts[0],
            parts[1],
            Some(parts[2].to_string()),
            parse_usize(parts[3])?,
        ),
        _ => return Err(crate::Error::Protocol(format!("invalid MSG line: {line}"))),
    };

    let payload_start = line_end + 2;
    let payload_end = payload_start + payload_len;
    let total = payload_end + 2; // trailing \r\n

    if buf.len() < total {
        return Ok(None); // need more data
    }

    Ok(Some((
        ServerOp::Msg(Msg {
            subject: subject.to_string(),
            sid: sid.to_string(),
            reply_to,
            payload: buf[payload_start..payload_end].to_vec(),
        }),
        total,
    )))
}

fn parse_hmsg(
    line: &str,
    buf: &[u8],
    line_end: usize,
) -> Result<Option<(ServerOp, usize)>, crate::Error> {
    // HMSG <subject> <sid> [reply-to] <#hdr_bytes> <#total_bytes>\r\n
    // [headers]\r\n[payload]\r\n
    let parts: Vec<&str> = line[5..].split_whitespace().collect();
    let (subject, sid, reply_to, hdr_len, total_len) = match parts.len() {
        4 => (
            parts[0],
            parts[1],
            None,
            parse_usize(parts[2])?,
            parse_usize(parts[3])?,
        ),
        5 => (
            parts[0],
            parts[1],
            Some(parts[2].to_string()),
            parse_usize(parts[3])?,
            parse_usize(parts[4])?,
        ),
        _ => return Err(crate::Error::Protocol(format!("invalid HMSG line: {line}"))),
    };

    let data_start = line_end + 2;
    let data_end = data_start + total_len;
    let total = data_end + 2; // trailing \r\n

    if buf.len() < total {
        return Ok(None); // need more data
    }

    let hdr_bytes = &buf[data_start..data_start + hdr_len];
    let payload = &buf[data_start + hdr_len..data_end];

    let headers = parse_headers(hdr_bytes)?;

    Ok(Some((
        ServerOp::HMsg(HMsg {
            subject: subject.to_string(),
            sid: sid.to_string(),
            reply_to,
            headers,
            payload: payload.to_vec(),
        }),
        total,
    )))
}

fn parse_headers(data: &[u8]) -> Result<Headers, crate::Error> {
    let text = std::str::from_utf8(data)
        .map_err(|e| crate::Error::Protocol(format!("invalid header utf8: {e}")))?;

    let mut headers = Headers::new();

    let mut lines = text.lines();
    // Parse the status line: "NATS/1.0" or "NATS/1.0 503"
    if let Some(status_line) = lines.next() {
        if let Some(rest) = status_line.strip_prefix("NATS/1.0") {
            let rest = rest.trim();
            if !rest.is_empty() {
                // Could be "503" or "503 No Responders"
                let code_str = rest.split_whitespace().next().unwrap_or("");
                headers.status = code_str.parse().ok();
            }
        }
    }

    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some((key, value)) = line.split_once(':') {
            headers.insert(key.trim(), value.trim());
        }
    }

    Ok(headers)
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\r\n")
}

fn parse_usize(s: &str) -> Result<usize, crate::Error> {
    s.parse()
        .map_err(|_| crate::Error::Protocol(format!("invalid number: {s}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Headers ────────────────────────────────────────────────

    #[test]
    fn headers_insert_and_get() {
        let mut h = Headers::new();
        h.insert("Foo", "bar");
        assert_eq!(h.get("Foo"), Some("bar"));
    }

    #[test]
    fn headers_case_insensitive() {
        let mut h = Headers::new();
        h.insert("Content-Type", "text/plain");
        assert_eq!(h.get("content-type"), Some("text/plain"));
        assert_eq!(h.get("CONTENT-TYPE"), Some("text/plain"));
    }

    #[test]
    fn headers_missing_key() {
        let h = Headers::new();
        assert_eq!(h.get("nope"), None);
    }

    #[test]
    fn headers_encode_roundtrip() {
        let mut h = Headers::new();
        h.insert("X-Key", "value1");
        h.insert("X-Other", "value2");
        let encoded = h.encode();
        let decoded = parse_headers(&encoded).unwrap();
        assert_eq!(decoded.get("X-Key"), Some("value1"));
        assert_eq!(decoded.get("X-Other"), Some("value2"));
    }

    #[test]
    fn headers_encode_with_status() {
        let mut h = Headers::new();
        h.insert("Nats-Sequence", "42");
        let encoded = h.encode_with_status("100");
        let s = std::str::from_utf8(&encoded).unwrap();
        assert!(s.starts_with("NATS/1.0 100\r\n"));
        assert!(s.contains("Nats-Sequence: 42\r\n"));
    }

    // ── Subject validation ─────────────────────────────────────

    #[test]
    fn valid_subjects() {
        assert!(validate_subject("foo").is_ok());
        assert!(validate_subject("foo.bar.baz").is_ok());
        assert!(validate_subject("$KV.bucket.key").is_ok());
        assert!(validate_subject("_INBOX.123").is_ok());
        assert!(validate_subject("events.>").is_ok());
        assert!(validate_subject("events.*").is_ok());
    }

    #[test]
    fn empty_subject_rejected() {
        assert!(validate_subject("").is_err());
    }

    #[test]
    fn subject_with_space_rejected() {
        assert!(validate_subject("foo bar").is_err());
    }

    #[test]
    fn subject_with_newline_rejected() {
        assert!(validate_subject("foo\r\nbar").is_err());
        assert!(validate_subject("foo\nbar").is_err());
    }

    #[test]
    fn subject_with_null_rejected() {
        assert!(validate_subject("foo\0bar").is_err());
    }

    #[test]
    fn subject_with_tab_rejected() {
        assert!(validate_subject("foo\tbar").is_err());
    }

    // ── Encode functions ───────────────────────────────────────

    #[test]
    fn encode_sub_basic() {
        let data = encode_sub("foo.bar", "1").unwrap();
        assert_eq!(data, b"SUB foo.bar 1\r\n");
    }

    #[test]
    fn encode_sub_queue_basic() {
        let data = encode_sub_queue("foo.bar", "grp", "2").unwrap();
        assert_eq!(data, b"SUB foo.bar grp 2\r\n");
    }

    #[test]
    fn encode_pub_no_reply() {
        let data = encode_pub("test.subject", None, b"hello").unwrap();
        assert_eq!(data, b"PUB test.subject 5\r\nhello\r\n");
    }

    #[test]
    fn encode_pub_with_reply() {
        let data = encode_pub("test.subject", Some("_INBOX.1"), b"hi").unwrap();
        assert_eq!(data, b"PUB test.subject _INBOX.1 2\r\nhi\r\n");
    }

    #[test]
    fn encode_pub_empty_payload() {
        let data = encode_pub("test", None, b"").unwrap();
        assert_eq!(data, b"PUB test 0\r\n\r\n");
    }

    #[test]
    fn encode_pub_invalid_subject() {
        assert!(encode_pub("bad subject", None, b"x").is_err());
    }

    #[test]
    fn encode_hpub_basic() {
        let mut h = Headers::new();
        h.insert("X-Test", "val");
        let data = encode_hpub("subj", None, &h, b"body").unwrap();
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("HPUB subj "));
        assert!(s.contains("X-Test: val\r\n"));
        assert!(s.ends_with("body\r\n"));
    }

    // ── Parse functions ────────────────────────────────────────

    #[test]
    fn parse_ping() {
        let buf = b"PING\r\n";
        let (op, consumed) = parse_op(buf).unwrap().unwrap();
        assert!(matches!(op, ServerOp::Ping));
        assert_eq!(consumed, 6);
    }

    #[test]
    fn parse_pong() {
        let buf = b"PONG\r\n";
        let (op, consumed) = parse_op(buf).unwrap().unwrap();
        assert!(matches!(op, ServerOp::Pong));
        assert_eq!(consumed, 6);
    }

    #[test]
    fn parse_ok() {
        let buf = b"+OK\r\n";
        let (op, consumed) = parse_op(buf).unwrap().unwrap();
        assert!(matches!(op, ServerOp::Ok));
        assert_eq!(consumed, 5);
    }

    #[test]
    fn parse_err() {
        let buf = b"-ERR 'Authorization Violation'\r\n";
        let (op, _) = parse_op(buf).unwrap().unwrap();
        match op {
            ServerOp::Err(msg) => assert_eq!(msg, "Authorization Violation"),
            _ => panic!("expected Err"),
        }
    }

    #[test]
    fn parse_info() {
        let buf = br#"INFO {"server_id":"test","version":"2.10.0","port":4222,"headers":true,"jetstream":true}"#;
        let mut full = buf.to_vec();
        full.extend_from_slice(b"\r\n");
        let (op, _) = parse_op(&full).unwrap().unwrap();
        match op {
            ServerOp::Info(info) => {
                assert_eq!(info.server_id, "test");
                assert_eq!(info.version, "2.10.0");
                assert!(info.headers);
                assert!(info.jetstream);
            }
            _ => panic!("expected Info"),
        }
    }

    #[test]
    fn parse_msg_no_reply() {
        let payload = b"hello";
        let header = format!("MSG foo.bar 1 {}\r\n", payload.len());
        let mut buf = header.into_bytes();
        buf.extend_from_slice(payload);
        buf.extend_from_slice(b"\r\n");

        let (op, consumed) = parse_op(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        match op {
            ServerOp::Msg(msg) => {
                assert_eq!(msg.subject, "foo.bar");
                assert_eq!(msg.sid, "1");
                assert!(msg.reply_to.is_none());
                assert_eq!(msg.payload, b"hello");
            }
            _ => panic!("expected Msg"),
        }
    }

    #[test]
    fn parse_msg_with_reply() {
        let payload = b"data";
        let header = format!("MSG foo 2 _INBOX.1 {}\r\n", payload.len());
        let mut buf = header.into_bytes();
        buf.extend_from_slice(payload);
        buf.extend_from_slice(b"\r\n");

        let (op, _) = parse_op(&buf).unwrap().unwrap();
        match op {
            ServerOp::Msg(msg) => {
                assert_eq!(msg.subject, "foo");
                assert_eq!(msg.reply_to, Some("_INBOX.1".to_string()));
                assert_eq!(msg.payload, b"data");
            }
            _ => panic!("expected Msg"),
        }
    }

    #[test]
    fn parse_hmsg() {
        let mut h = Headers::new();
        h.insert("X-Key", "val");
        let hdr_bytes = h.encode();
        let payload = b"body";
        let total = hdr_bytes.len() + payload.len();
        let header = format!("HMSG subj 1 {} {}\r\n", hdr_bytes.len(), total);
        let mut buf = header.into_bytes();
        buf.extend_from_slice(&hdr_bytes);
        buf.extend_from_slice(payload);
        buf.extend_from_slice(b"\r\n");

        let (op, consumed) = parse_op(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        match op {
            ServerOp::HMsg(hmsg) => {
                assert_eq!(hmsg.subject, "subj");
                assert_eq!(hmsg.headers.get("X-Key"), Some("val"));
                assert_eq!(hmsg.payload, b"body");
            }
            _ => panic!("expected HMsg"),
        }
    }

    #[test]
    fn parse_incomplete_returns_none() {
        // Incomplete line (no \r\n)
        assert!(parse_op(b"PIN").unwrap().is_none());

        // MSG with incomplete payload
        let buf = b"MSG foo 1 5\r\nhe";
        assert!(parse_op(buf).unwrap().is_none());
    }

    #[test]
    fn parse_multiple_ops() {
        let buf = b"PING\r\nPONG\r\n";
        let (op1, consumed1) = parse_op(buf).unwrap().unwrap();
        assert!(matches!(op1, ServerOp::Ping));
        let (op2, consumed2) = parse_op(&buf[consumed1..]).unwrap().unwrap();
        assert!(matches!(op2, ServerOp::Pong));
        assert_eq!(consumed1 + consumed2, buf.len());
    }

    // ── Connect encoding ───────────────────────────────────────

    #[test]
    fn encode_connect_default() {
        let opts = ConnectOptions::default();
        let data = encode_connect(&opts);
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.starts_with("CONNECT {"));
        assert!(s.ends_with("}\r\n"));
        assert!(s.contains("\"lang\":\"nats-wasip3\""));
        assert!(s.contains("\"headers\":true"));
        assert!(s.contains("\"echo\":true"));
        assert!(s.contains("\"no_responders\":true"));
    }

    #[test]
    fn encode_connect_with_auth() {
        let opts = ConnectOptions {
            name: Some("myapp".into()),
            user: Some("admin".into()),
            pass: Some("secret".into()),
            ..Default::default()
        };
        let data = encode_connect(&opts);
        let s = std::str::from_utf8(&data).unwrap();
        assert!(s.contains("\"name\":\"myapp\""));
        assert!(s.contains("\"user\":\"admin\""));
        assert!(s.contains("\"pass\":\"secret\""));
    }

    // ── UNSUB encoding ─────────────────────────────────────────

    #[test]
    fn encode_unsub_no_max() {
        let data = encode_unsub("5", None);
        assert_eq!(data, b"UNSUB 5\r\n");
    }

    #[test]
    fn encode_unsub_with_max() {
        let data = encode_unsub("3", Some(10));
        assert_eq!(data, b"UNSUB 3 10\r\n");
    }

    // ── Headers: status parsing ────────────────────────────────

    #[test]
    fn parse_headers_status_503() {
        let data = b"NATS/1.0 503 No Responders\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status, Some(503));
    }

    #[test]
    fn parse_headers_status_100() {
        let data = b"NATS/1.0 100 Idle Heartbeat\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status, Some(100));
    }

    #[test]
    fn parse_headers_no_status() {
        let data = b"NATS/1.0\r\nX-Key: val\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status, None);
        assert_eq!(h.get("X-Key"), Some("val"));
    }

    #[test]
    fn parse_headers_status_with_kvs() {
        let data = b"NATS/1.0 404\r\nStatus: 404\r\nDescription: Not Found\r\n\r\n";
        let h = parse_headers(data).unwrap();
        assert_eq!(h.status, Some(404));
        assert_eq!(h.get("Description"), Some("Not Found"));
    }

    // ── Headers: multiple values for same key ──────────────────

    #[test]
    fn headers_multiple_same_key() {
        let mut h = Headers::new();
        h.insert("X-Tag", "a");
        h.insert("X-Tag", "b");
        // get() returns the first match
        assert_eq!(h.get("X-Tag"), Some("a"));
    }

    // ── HMSG with reply-to ─────────────────────────────────────

    #[test]
    fn parse_hmsg_with_reply() {
        let mut h = Headers::new();
        h.insert("X-R", "1");
        let hdr_bytes = h.encode();
        let payload = b"data";
        let total = hdr_bytes.len() + payload.len();
        let header = format!("HMSG subj 5 _INBOX.99 {} {}\r\n", hdr_bytes.len(), total);
        let mut buf = header.into_bytes();
        buf.extend_from_slice(&hdr_bytes);
        buf.extend_from_slice(payload);
        buf.extend_from_slice(b"\r\n");

        let (op, _) = parse_op(&buf).unwrap().unwrap();
        match op {
            ServerOp::HMsg(hmsg) => {
                assert_eq!(hmsg.subject, "subj");
                assert_eq!(hmsg.sid, "5");
                assert_eq!(hmsg.reply_to, Some("_INBOX.99".to_string()));
                assert_eq!(hmsg.headers.get("X-R"), Some("1"));
                assert_eq!(hmsg.payload, b"data");
            }
            _ => panic!("expected HMsg"),
        }
    }

    // ── Parse: malformed inputs ────────────────────────────────

    #[test]
    fn parse_err_strips_quotes() {
        let buf = b"-ERR 'Permissions Violation for Subscription'\r\n";
        let (op, _) = parse_op(buf).unwrap().unwrap();
        match op {
            ServerOp::Err(msg) => {
                assert!(!msg.contains('\''), "quotes should be stripped");
                assert!(msg.contains("Permissions Violation"));
            }
            _ => panic!("expected Err"),
        }
    }

    #[test]
    fn parse_msg_bad_field_count() {
        // MSG with too few fields
        let buf = b"MSG foo\r\n";
        assert!(parse_op(buf).is_err());
    }

    #[test]
    fn parse_unknown_op() {
        let buf = b"UNKNOWN_OP\r\n";
        assert!(parse_op(buf).is_err());
    }

    // ── Large payload ──────────────────────────────────────────

    #[test]
    fn encode_pub_large_payload() {
        let payload = vec![0x42; 10_000];
        let data = encode_pub("big.msg", None, &payload).unwrap();
        let s = std::str::from_utf8(&data[..20]).unwrap();
        assert!(s.starts_with("PUB big.msg 10000\r\n"));
        assert_eq!(data.len(), "PUB big.msg 10000\r\n".len() + 10_000 + 2);
    }

    #[test]
    fn parse_msg_large_payload() {
        let payload = vec![0xAB; 5_000];
        let header = format!("MSG big.subj 1 {}\r\n", payload.len());
        let mut buf = header.into_bytes();
        buf.extend_from_slice(&payload);
        buf.extend_from_slice(b"\r\n");

        let (op, consumed) = parse_op(&buf).unwrap().unwrap();
        assert_eq!(consumed, buf.len());
        match op {
            ServerOp::Msg(msg) => {
                assert_eq!(msg.payload.len(), 5_000);
            }
            _ => panic!("expected Msg"),
        }
    }
}
