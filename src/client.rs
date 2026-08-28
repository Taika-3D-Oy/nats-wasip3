//! NATS client connection using native WASI P3 Component Model async I/O.
//!
//! Architecture: P3 sockets expose unidirectional `StreamReader<u8>` /
//! `StreamWriter<u8>` pairs for receive/send. The host runtime schedules
//! the async — no userland reactor needed. `wit_bindgen::spawn` provides
//! concurrent tasks (read loop, flush loop).

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use futures::future::select;
use futures::pin_mut;

use wasip3::sockets::ip_name_lookup;
use wasip3::sockets::types::{
    ErrorCode, IpAddressFamily, IpSocketAddress, Ipv4SocketAddress, Ipv6SocketAddress, TcpSocket,
};
use wasip3::wit_stream;
use wit_bindgen::{FutureReader, StreamReader, StreamResult, StreamWriter};

use crate::proto::{self, ConnectOptions, Headers, ServerInfo, ServerOp};
use crate::Error;



// ── Public types ───────────────────────────────────────────────────

/// A received message (unified from MSG and HMSG).
#[non_exhaustive]
#[derive(Debug)]
pub struct Message {
    pub subject: String,
    pub reply_to: Option<String>,
    pub headers: Option<Headers>,
    pub payload: Vec<u8>,
}

/// A subscription that yields messages.
pub struct Subscription {
    sid: String,
    inner: Rc<RefCell<Inner>>,
}

impl Subscription {
    /// Receive the next message on this subscription.
    pub async fn next(&self) -> Result<Message, Error> {
        NextMessage {
            sid: &self.sid,
            inner: &self.inner,
        }
        .await
    }

    /// Unsubscribe immediately. Equivalent to dropping the subscription, but
    /// explicit and does not require ownership.
    pub fn unsubscribe(&self) {
        let mut inner = self.inner.borrow_mut();
        if inner.mailboxes.remove(&self.sid).is_none() {
            return; // already unsubscribed
        }
        inner.subscriptions.remove(&self.sid);
        let unsub = proto::encode_unsub(&self.sid, None);
        inner.write_buf.extend_from_slice(&unsub);
        if let Some(w) = inner.flush_waker.take() {
            w.wake();
        }
    }

    /// Ask the server to deliver at most `max_msgs` more messages, then
    /// automatically unsubscribe. Useful for request-after-subscribe patterns.
    pub fn unsubscribe_after(&self, max_msgs: u64) {
        let unsub = proto::encode_unsub(&self.sid, Some(max_msgs));
        let mut inner = self.inner.borrow_mut();
        inner.write_buf.extend_from_slice(&unsub);
        if let Some(w) = inner.flush_waker.take() {
            w.wake();
        }
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();
        // If unsubscribe() was called explicitly, mailbox is already gone.
        if inner.mailboxes.remove(&self.sid).is_none() {
            return;
        }
        inner.subscriptions.remove(&self.sid);
        let unsub = proto::encode_unsub(&self.sid, None);
        inner.write_buf.extend_from_slice(&unsub);
        if let Some(w) = inner.flush_waker.take() {
            w.wake();
        }
    }
}

/// Duration type — nanoseconds, matching P3 monotonic clock.
pub type Duration = u64;

/// Helper: seconds → nanoseconds.
pub const fn secs(n: u64) -> Duration {
    n * 1_000_000_000
}

/// Helper: milliseconds → nanoseconds.
pub const fn millis(n: u64) -> Duration {
    n * 1_000_000
}

/// Connection configuration.
#[derive(Clone)]
pub struct ConnectConfig {
    /// Primary server address ("host:port"). Always tried first.
    pub address: String,
    /// Additional server addresses tried on connect and on reconnect,
    /// round-robined with `address` and any cluster URLs from server INFO.
    pub servers: Vec<String>,
    pub name: Option<String>,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
    /// JWT credential for NATS 2.0 operator authentication. When set together
    /// with `nkey_seed` (or via `credentials`), the server nonce is signed.
    pub jwt: Option<String>,
    /// Raw content of a NATS `.creds` file (JWT + NKey seed blocks).
    /// When set, overrides `jwt` and `nkey_seed`. Requires the `nkey` feature.
    #[cfg(feature = "nkey")]
    pub credentials: Option<String>,
    pub tls: bool,
    #[cfg(feature = "tls")]
    pub tls_server_name: Option<String>,
    /// NKey seed string (starts with `S`). When set, the server's nonce is
    /// signed with this key and `sig` + `nkey` are sent in CONNECT.
    /// Requires the `nkey` feature.
    #[cfg(feature = "nkey")]
    pub nkey_seed: Option<String>,
    /// Maximum reconnection attempts. `0` disables reconnection. Default: `5`.
    pub max_reconnect_attempts: u32,
    /// Base reconnection delay (nanoseconds, doubles each attempt, cap 8 s). Default: 250 ms.
    pub reconnect_delay: Duration,
    /// Maximum number of pending messages per subscription before the oldest
    /// is dropped (slow-consumer protection). Default: 512.
    pub subscription_capacity: usize,
    /// Maximum number of bytes buffered for outbound writes. `publish()` returns
    /// `Err(Error::BufferFull)` when this limit is exceeded. Default: 8 MiB.
    pub max_pending_write_bytes: usize,
    /// Disable echoing messages back to the publisher. When `true`, the server
    /// will not deliver messages published by this client to its own
    /// subscriptions. Default: `false`.
    pub no_echo: bool,
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:4222".to_string(),
            servers: Vec::new(),
            name: Some("nats-wasi".to_string()),
            auth_token: None,
            user: None,
            pass: None,
            jwt: None,
            #[cfg(feature = "nkey")]
            credentials: None,
            tls: false,
            #[cfg(feature = "tls")]
            tls_server_name: None,
            #[cfg(feature = "nkey")]
            nkey_seed: None,
            max_reconnect_attempts: 5,
            reconnect_delay: millis(250),
            subscription_capacity: 512,
            max_pending_write_bytes: 8 * 1024 * 1024,
            no_echo: false,
        }
    }
}

// ── Shared mutable state ───────────────────────────────────────────

#[derive(Clone)]
struct SubInfo {
    subject: String,
    queue: Option<String>,
}

struct Mailbox {
    queue: VecDeque<Message>,
    waker: Option<Waker>,
}

struct RequestSlot {
    response: Option<Message>,
    waker: Option<Waker>,
}

struct Inner {
    /// Active host TCP socket resource, kept alive so it is not dropped prematurely,
    /// and dropped on close to release host file descriptors.
    _socket: Option<TcpSocket>,
    mailboxes: HashMap<String, Mailbox>,
    subscriptions: HashMap<String, SubInfo>,
    /// Pending multiplexed request-reply slots keyed by unique token.
    pending_requests: HashMap<String, RequestSlot>,
    /// Unique prefix for multiplexed request-reply inbox: `_INBOX.<unique>.`
    inbox_prefix: String,
    write_buf: Vec<u8>,
    flush_waker: Option<Waker>,
    /// Wakers for tasks waiting for a PONG in `Client::flush()`.
    pong_wakers: Vec<Waker>,
    /// Cumulative count of PONGs received since connection start.
    pongs_received: u64,
    next_id: u64,
    /// Xorshift64 state for pseudo-random inbox tokens and jitter.
    /// Seeded from the monotonic clock at connection time.
    rng: u64,
    closed: bool,
    close_error: Option<String>,
    /// Set by reconnect logic; picked up by the flush loop.
    new_writer: Option<StreamWriter<u8>>,
    /// Cluster node URLs learned from server INFO `connect_urls`.
    known_servers: Vec<String>,
    /// Maximum pending messages per subscription mailbox.
    mailbox_capacity: usize,
    /// Maximum outbound write buffer in bytes.
    write_buf_limit: usize,
    /// Server's advertised max_payload limit (0 = unlimited).
    max_payload: usize,
}

// ── Client ─────────────────────────────────────────────────────────

/// A NATS client. Cheap to clone — all clones share the same connection.
/// When the last clone is dropped, background I/O loops are shut down.
pub struct Client {
    inner: Rc<RefCell<Inner>>,
    info: Rc<ServerInfo>,
    /// User-facing clone count. Background tasks do NOT hold this.
    /// When it reaches 0 the connection is marked closed.
    refcount: Rc<Cell<usize>>,
}

impl Clone for Client {
    fn clone(&self) -> Self {
        self.refcount.set(self.refcount.get() + 1);
        Client {
            inner: Rc::clone(&self.inner),
            info: Rc::clone(&self.info),
            refcount: Rc::clone(&self.refcount),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        let n = self.refcount.get() - 1;
        self.refcount.set(n);
        if n == 0 {
            // Last user clone dropped — shut down background loops and drop host socket.
            let mut inner = self.inner.borrow_mut();
            if !inner.closed {
                inner.closed = true;
                inner._socket = None;
                wake_all(&mut inner);
            }
        }
    }
}

impl Client {
    /// Connect to a NATS server and start background I/O loops.
    pub async fn connect(config: ConnectConfig) -> Result<Self, Error> {
        let sock_addr = parse_address(&config.address).await?;
        let family = match sock_addr {
            IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
            IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
        };

        let socket = TcpSocket::create(family)?;
        socket.connect(sock_addr).await?;

        // P3: receive() and send() can each be called at most once.
        let (mut rx, rx_fut) = socket.receive();
        let (tx, tx_rx) = wit_stream::new();
        let send_fut = socket.send(tx_rx);

        // ── Read INFO ──────────────────────────────────────────
        let mut buf = Vec::new();
        let mut scratch = Vec::with_capacity(8192);
        let info = loop {
            let (n, s) = stream_read(&mut rx, &mut buf, scratch).await;
            scratch = s;
            if n == 0 && buf.is_empty() {
                return Err(Error::Disconnected);
            }
            if let Some((op, consumed)) = proto::parse_op(&buf)? {
                let leftover = buf[consumed..].to_vec();
                buf = leftover;
                match op {
                    ServerOp::Info(info) => break info,
                    _ => return Err(Error::Protocol("expected INFO".into())),
                }
            }
        };

        // ── TLS check ──────────────────────────────────────────
        let use_tls = config.tls || info.tls_required;
        #[cfg(not(feature = "tls"))]
        if use_tls {
            return Err(Error::Protocol(
                "server requires TLS but tls feature is disabled".into(),
            ));
        }

        let (mut rx, mut tx) = if use_tls {
            #[cfg(feature = "tls")]
            {
                let server_name = config
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| {
                        config
                            .address
                            .rsplit_once(':')
                            .map(|(h, _)| h.to_string())
                            .unwrap_or_default()
                    });
                crate::tls::tls_upgrade(rx, tx, &server_name).await?
            }
            #[cfg(not(feature = "tls"))]
            unreachable!()
        } else {
            (rx, tx)
        };

        // ── Shared state setup ─────────────────────────────────
        // Seed the PRNG from the monotonic clock; XOR with a fixed salt so
        // that a zero clock reading still produces a valid non-zero state.
        let mut rng = wasip3::clocks::monotonic_clock::now() ^ 0xcafe_babe_dead_beef;
        let r1 = xorshift64(&mut rng);
        let r2 = xorshift64(&mut rng);
        let inbox_prefix = format!("_INBOX.{r1:016x}{r2:016x}.");
        let mux_subject = format!("{inbox_prefix}*");
        let mux_sid = "0".to_string();

        let mut subscriptions = HashMap::new();
        subscriptions.insert(
            mux_sid.clone(),
            SubInfo {
                subject: mux_subject.clone(),
                queue: None,
            },
        );

        // ── Send CONNECT + multiplexed inbox SUB + PING ────────
        let connect_opts = build_connect_opts(&config, &info, use_tls)?;
        let mut handshake = proto::encode_connect(&connect_opts);
        let mux_sub = proto::encode_sub(&mux_subject, &mux_sid)?;
        handshake.extend_from_slice(&mux_sub);
        handshake.extend_from_slice(proto::PING);
        stream_write_all(&mut tx, &handshake).await?;

        // ── Wait for PONG ──────────────────────────────────────
        loop {
            let (n, s) = stream_read(&mut rx, &mut buf, scratch).await;
            scratch = s;
            if n == 0 && buf.is_empty() {
                return Err(Error::Disconnected);
            }
            if let Some((op, consumed)) = proto::parse_op(&buf)? {
                buf = buf[consumed..].to_vec();
                match op {
                    ServerOp::Pong => break,
                    ServerOp::Ok | ServerOp::Ping => continue,
                    // Server may re-send INFO after TLS upgrade
                    ServerOp::Info(_) => continue,
                    ServerOp::Err(msg) => return Err(Error::Server(msg)),
                    _ => return Err(Error::Protocol("expected PONG".into())),
                }
            }
        }

        let inner = Rc::new(RefCell::new(Inner {
            _socket: Some(socket),
            mailboxes: HashMap::new(),
            subscriptions,
            pending_requests: HashMap::new(),
            inbox_prefix,
            write_buf: Vec::new(),
            flush_waker: None,
            pong_wakers: Vec::new(),
            pongs_received: 0,
            next_id: 1,
            rng,
            closed: false,
            close_error: None,
            new_writer: None,
            known_servers: info.connect_urls.clone(),
            mailbox_capacity: config.subscription_capacity,
            write_buf_limit: config.max_pending_write_bytes,
            max_payload: info.max_payload,
        }));

        // Drain send future in background so it does not leak.
        wit_bindgen::spawn(async move {
            let _ = send_fut.await;
        });

        // ── Spawn read loop ────────────────────────────────────
        {
            let inner2 = Rc::clone(&inner);
            let config2 = config.clone();
            wit_bindgen::spawn(async move {
                read_loop(rx, rx_fut, inner2, buf, config2).await;
            });
        }

        // ── Spawn flush loop ───────────────────────────────────
        {
            let inner2 = Rc::clone(&inner);
            wit_bindgen::spawn(async move {
                flush_loop(inner2, tx).await;
            });
        }

        Ok(Client {
            inner,
            info: Rc::new(info),
            refcount: Rc::new(Cell::new(1)),
        })
    }

    /// Server info from the handshake.
    pub fn server_info(&self) -> &ServerInfo {
        &self.info
    }

    /// Publish a message.
    pub fn publish(&self, subject: &str, payload: &[u8]) -> Result<(), Error> {
        self.check_closed()?;
        self.check_payload_size(payload.len())?;
        let data = proto::encode_pub(subject, None, payload)?;
        self.enqueue_write(&data)?;
        Ok(())
    }

    /// Publish with a reply-to subject.
    pub fn publish_with_reply(
        &self,
        subject: &str,
        reply_to: &str,
        payload: &[u8],
    ) -> Result<(), Error> {
        self.check_closed()?;
        self.check_payload_size(payload.len())?;
        let data = proto::encode_pub(subject, Some(reply_to), payload)?;
        self.enqueue_write(&data)?;
        Ok(())
    }

    /// Publish with headers.
    pub fn publish_with_headers(
        &self,
        subject: &str,
        reply_to: Option<&str>,
        headers: &Headers,
        payload: &[u8],
    ) -> Result<(), Error> {
        self.check_closed()?;
        self.check_payload_size(payload.len())?;
        let data = proto::encode_hpub(subject, reply_to, headers, payload)?;
        self.enqueue_write(&data)?;
        Ok(())
    }

    /// Subscribe to a subject.
    pub fn subscribe(&self, subject: &str) -> Result<Subscription, Error> {
        self.check_closed()?;
        let sid = self.next_sid();
        let data = proto::encode_sub(subject, &sid)?;
        self.enqueue_write(&data)?;

        let mut inner = self.inner.borrow_mut();
        inner.mailboxes.insert(
            sid.clone(),
            Mailbox {
                queue: VecDeque::new(),
                waker: None,
            },
        );
        inner.subscriptions.insert(
            sid.clone(),
            SubInfo {
                subject: subject.to_string(),
                queue: None,
            },
        );

        Ok(Subscription {
            sid,
            inner: Rc::clone(&self.inner),
        })
    }

    /// Subscribe with a queue group.
    pub fn subscribe_queue(&self, subject: &str, queue: &str) -> Result<Subscription, Error> {
        self.check_closed()?;
        let sid = self.next_sid();
        let data = proto::encode_sub_queue(subject, queue, &sid)?;
        self.enqueue_write(&data)?;

        let mut inner = self.inner.borrow_mut();
        inner.mailboxes.insert(
            sid.clone(),
            Mailbox {
                queue: VecDeque::new(),
                waker: None,
            },
        );
        inner.subscriptions.insert(
            sid.clone(),
            SubInfo {
                subject: subject.to_string(),
                queue: Some(queue.to_string()),
            },
        );

        Ok(Subscription {
            sid,
            inner: Rc::clone(&self.inner),
        })
    }

    /// Send a request and wait for a single reply using the shared multiplexed inbox.
    pub async fn request(
        &self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, Error> {
        self.check_closed()?;
        self.check_payload_size(payload.len())?;

        let (token, reply_to) = {
            let mut inner = self.inner.borrow_mut();
            let r = xorshift64(&mut inner.rng);
            let token = format!("{r:016x}");
            let reply_to = format!("{}{token}", inner.inbox_prefix);
            inner.pending_requests.insert(
                token.clone(),
                RequestSlot {
                    response: None,
                    waker: None,
                },
            );
            (token, reply_to)
        };

        let data = proto::encode_pub(subject, Some(&reply_to), payload)?;
        self.enqueue_write(&data)?;

        let reply_fut = RequestFuture {
            token: token.clone(),
            inner: Rc::clone(&self.inner),
        };

        let result = with_timeout(timeout, reply_fut).await?;
        let msg = result?;
        if let Some(ref hdrs) = msg.headers {
            if hdrs.status == Some(503) {
                return Err(Error::NoResponders);
            }
        }
        Ok(msg)
    }

    /// Request with headers using the shared multiplexed inbox.
    pub async fn request_with_headers(
        &self,
        subject: &str,
        headers: &Headers,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, Error> {
        self.check_closed()?;
        self.check_payload_size(payload.len())?;

        let (token, reply_to) = {
            let mut inner = self.inner.borrow_mut();
            let r = xorshift64(&mut inner.rng);
            let token = format!("{r:016x}");
            let reply_to = format!("{}{token}", inner.inbox_prefix);
            inner.pending_requests.insert(
                token.clone(),
                RequestSlot {
                    response: None,
                    waker: None,
                },
            );
            (token, reply_to)
        };

        let data = proto::encode_hpub(subject, Some(&reply_to), headers, payload)?;
        self.enqueue_write(&data)?;

        let reply_fut = RequestFuture {
            token: token.clone(),
            inner: Rc::clone(&self.inner),
        };

        let result = with_timeout(timeout, reply_fut).await?;
        let msg = result?;
        if let Some(ref hdrs) = msg.headers {
            if hdrs.status == Some(503) {
                return Err(Error::NoResponders);
            }
        }
        Ok(msg)
    }

    fn check_closed(&self) -> Result<(), Error> {
        let inner = self.inner.borrow();
        if inner.closed {
            Err(match &inner.close_error {
                Some(msg) => Error::Server(msg.clone()),
                None => Error::Disconnected,
            })
        } else {
            Ok(())
        }
    }

    /// Close the connection. Background loops will terminate.
    pub fn close(&self) {
        let mut inner = self.inner.borrow_mut();
        inner.closed = true;
        inner._socket = None;
        wake_all(&mut inner);
    }

    /// Flush pending writes and wait for a PONG from the server, confirming
    /// that all previously published messages have been received.
    /// Returns `Err(Error::Timeout)` if the server doesn't respond within `timeout`.
    pub async fn flush(&self, timeout: Duration) -> Result<(), Error> {
        self.check_closed()?;
        let target_pongs = {
            let mut inner = self.inner.borrow_mut();
            let target = inner.pongs_received + 1;
            inner.write_buf.extend_from_slice(proto::PING);
            if let Some(w) = inner.flush_waker.take() {
                w.wake();
            }
            target
        };
        with_timeout(timeout, PongWait { inner: &self.inner, target_pongs }).await?
    }

    fn next_sid(&self) -> String {
        let mut inner = self.inner.borrow_mut();
        let id = inner.next_id;
        inner.next_id += 1;
        id.to_string()
    }

    pub(crate) fn new_inbox(&self) -> String {
        let mut inner = self.inner.borrow_mut();
        let r1 = xorshift64(&mut inner.rng);
        let r2 = xorshift64(&mut inner.rng);
        format!("_INBOX.{r1:016x}{r2:016x}")
    }

    fn check_payload_size(&self, len: usize) -> Result<(), Error> {
        let max = self.inner.borrow().max_payload;
        if max > 0 && len > max {
            return Err(Error::MaxPayloadExceeded { size: len, max });
        }
        Ok(())
    }

    fn enqueue_write(&self, data: &[u8]) -> Result<(), Error> {
        let mut inner = self.inner.borrow_mut();
        if inner.write_buf.len() + data.len() > inner.write_buf_limit {
            return Err(Error::BufferFull);
        }
        inner.write_buf.extend_from_slice(data);
        if let Some(w) = inner.flush_waker.take() {
            w.wake();
        }
        Ok(())
    }
}

// ── Timeout using P3 monotonic clock ───────────────────────────────

/// Run `future` with a deadline. Returns `Err(Error::Timeout)` if it expires.
pub async fn with_timeout<F: Future>(timeout: Duration, future: F) -> Result<F::Output, Error> {
    let sleep = wasip3::clocks::monotonic_clock::wait_for(timeout);
    pin_mut!(future);
    pin_mut!(sleep);
    match select(future, sleep).await {
        futures::future::Either::Left((result, _)) => Ok(result),
        futures::future::Either::Right(((), _)) => Err(Error::Timeout),
    }
}

// ── P3 stream I/O helpers ──────────────────────────────────────────

/// Build a `ConnectOptions` from config + server INFO.
/// Handles plain auth (user/pass/token), bare JWT, NKey, and full
/// NATS 2.0 operator credentials (JWT + NKey signature).
fn build_connect_opts(
    config: &ConnectConfig,
    _info: &proto::ServerInfo,
    use_tls: bool,
) -> Result<ConnectOptions, Error> {
    #[cfg_attr(not(feature = "nkey"), allow(unused_mut))]
    let mut opts = ConnectOptions {
        name: config.name.clone(),
        user: config.user.clone(),
        pass: config.pass.clone(),
        auth_token: config.auth_token.clone(),
        jwt: config.jwt.clone(),
        tls_required: use_tls,
        echo: !config.no_echo,
        ..Default::default()
    };

    #[cfg(feature = "nkey")]
    {
        // `credentials` overrides the individual `jwt` / `nkey_seed` fields.
        if let Some(ref creds) = config.credentials {
            let (jwt, seed) = parse_creds(creds)?;
            opts.jwt = Some(jwt);
            let kp = crate::nkey::KeyPair::from_seed(&seed)?;
            let nonce = _info
                .nonce
                .as_deref()
                .ok_or_else(|| Error::Protocol("server did not send nonce for nkey auth".into()))?;
            opts.sig = Some(kp.sign(nonce.as_bytes()));
            opts.nkey = Some(kp.public_key());
        } else if let Some(ref seed) = config.nkey_seed {
            let kp = crate::nkey::KeyPair::from_seed(seed)?;
            let nonce = _info
                .nonce
                .as_deref()
                .ok_or_else(|| Error::Protocol("server did not send nonce for nkey auth".into()))?;
            opts.sig = Some(kp.sign(nonce.as_bytes()));
            opts.nkey = Some(kp.public_key());
        }
    }

    Ok(opts)
}

/// Parse a NATS `.creds` file, returning `(jwt, nkey_seed)`.
/// Tolerates any number of leading/trailing dashes in block markers.
#[cfg(feature = "nkey")]
fn parse_creds(content: &str) -> Result<(String, String), Error> {
    let jwt = extract_creds_field(content, "NATS USER JWT")
        .ok_or_else(|| Error::Protocol("credentials: missing NATS USER JWT block".into()))?;
    let seed = extract_creds_field(content, "USER NKEY SEED")
        .ok_or_else(|| Error::Protocol("credentials: missing USER NKEY SEED block".into()))?;
    Ok((jwt, seed))
}

/// Extract the value from a PEM-like block identified by `tag`.
/// Matches any line containing `BEGIN {tag}` / `END {tag}` (dash-count agnostic).
#[cfg(feature = "nkey")]
fn extract_creds_field(content: &str, tag: &str) -> Option<String> {
    let begin_marker = format!("BEGIN {tag}");
    let end_marker = format!("END {tag}");
    let mut in_block = false;
    let mut value = String::new();
    for line in content.lines() {
        if line.contains(&begin_marker) {
            in_block = true;
            continue;
        }
        if line.contains(&end_marker) {
            break;
        }
        if in_block {
            let t = line.trim();
            if !t.is_empty() {
                value.push_str(t);
            }
        }
    }
    if value.is_empty() { None } else { Some(value) }
}

/// Parse "host:port" into a P3 `IpSocketAddress`.
async fn parse_address(addr: &str) -> Result<IpSocketAddress, Error> {
    let (host, port_str) = addr
        .rsplit_once(':')
        .ok_or_else(|| Error::Protocol(format!("invalid address (no port): {addr}")))?;
    let port: u16 = port_str
        .parse()
        .map_err(|_| Error::Protocol(format!("invalid port: {port_str}")))?;

    if let Ok(v4) = host.parse::<std::net::Ipv4Addr>() {
        let o = v4.octets();
        Ok(IpSocketAddress::Ipv4(Ipv4SocketAddress {
            port,
            address: (o[0], o[1], o[2], o[3]),
        }))
    } else if let Ok(v6) = host.parse::<std::net::Ipv6Addr>() {
        let s = v6.segments();
        Ok(IpSocketAddress::Ipv6(Ipv6SocketAddress {
            port,
            address: (s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]),
            flow_info: 0,
            scope_id: 0,
        }))
    } else {
        // DNS hostname — resolve via WASI ip-name-lookup.
        use wasip3::sockets::types::IpAddress;
        let addrs = ip_name_lookup::resolve_addresses(host.to_string()).await?;
        let ip = addrs
            .first()
            .ok_or_else(|| Error::Protocol(format!("DNS resolved no addresses for: {host}")))?;
        match ip {
            IpAddress::Ipv4(a) => Ok(IpSocketAddress::Ipv4(Ipv4SocketAddress {
                port,
                address: *a,
            })),
            IpAddress::Ipv6(a) => Ok(IpSocketAddress::Ipv6(Ipv6SocketAddress {
                port,
                address: *a,
                flow_info: 0,
                scope_id: 0,
            })),
        }
    }
}

/// Read from a P3 `StreamReader<u8>`, appending to `buf`. Returns `(bytes_read, scratch)`.
/// The caller should pass the returned `scratch` back on the next call to reuse the allocation.
async fn stream_read(rx: &mut StreamReader<u8>, buf: &mut Vec<u8>, scratch: Vec<u8>) -> (usize, Vec<u8>) {
    let (status, mut data) = rx.read(scratch).await;
    match status {
        StreamResult::Complete(n) => {
            buf.extend_from_slice(&data[..n]);
            data.clear();
            (n, data)
        }
        StreamResult::Dropped | StreamResult::Cancelled => (0, data),
    }
}

/// Write owned bytes to a P3 `StreamWriter<u8>` without extra allocation.
/// Returns unwritten bytes on failure so they can be requeued without loss.
async fn stream_write_vec(tx: &mut StreamWriter<u8>, data: Vec<u8>) -> Result<(), (Vec<u8>, Error)> {
    let remaining = tx.write_all(data).await;
    if !remaining.is_empty() {
        return Err((remaining, Error::Disconnected));
    }
    Ok(())
}

/// Write all bytes from a slice to a P3 `StreamWriter<u8>`.
async fn stream_write_all(tx: &mut StreamWriter<u8>, data: &[u8]) -> Result<(), Error> {
    stream_write_vec(tx, data.to_vec()).await.map_err(|(_, e)| e)
}

// ── Futures: subscription / request ────────────────────────────────

struct NextMessage<'a> {
    sid: &'a str,
    inner: &'a Rc<RefCell<Inner>>,
}

impl<'a> Future for NextMessage<'a> {
    type Output = Result<Message, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Poll::Ready(Err(match &inner.close_error {
                Some(msg) => Error::Server(msg.clone()),
                None => Error::Disconnected,
            }));
        }
        if let Some(mailbox) = inner.mailboxes.get_mut(self.sid) {
            if let Some(msg) = mailbox.queue.pop_front() {
                return Poll::Ready(Ok(msg));
            }
            mailbox.waker = Some(cx.waker().clone());
        } else {
            return Poll::Ready(Err(Error::Disconnected));
        }
        Poll::Pending
    }
}

struct RequestFuture {
    token: String,
    inner: Rc<RefCell<Inner>>,
}

impl Future for RequestFuture {
    type Output = Result<Message, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Poll::Ready(Err(match &inner.close_error {
                Some(msg) => Error::Server(msg.clone()),
                None => Error::Disconnected,
            }));
        }
        if let Some(slot) = inner.pending_requests.get_mut(&self.token) {
            if let Some(msg) = slot.response.take() {
                return Poll::Ready(Ok(msg));
            }
            slot.waker = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(Err(Error::Disconnected))
        }
    }
}

impl Drop for RequestFuture {
    fn drop(&mut self) {
        // Automatically reclaim slot on normal completion, timeout, or cancellation.
        self.inner.borrow_mut().pending_requests.remove(&self.token);
    }
}

struct PongWait<'a> {
    inner: &'a Rc<RefCell<Inner>>,
    target_pongs: u64,
}

impl<'a> Future for PongWait<'a> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Poll::Ready(Err(match &inner.close_error {
                Some(msg) => Error::Server(msg.clone()),
                None => Error::Disconnected,
            }));
        }
        if inner.pongs_received >= self.target_pongs {
            Poll::Ready(Ok(()))
        } else {
            inner.pong_wakers.push(cx.waker().clone());
            Poll::Pending
        }
    }
}

// ── Background read loop ───────────────────────────────────────────

async fn read_loop(
    mut reader: StreamReader<u8>,
    _rx_result: FutureReader<Result<(), ErrorCode>>,
    inner: Rc<RefCell<Inner>>,
    leftover: Vec<u8>,
    config: ConnectConfig,
) {
    let mut buf = leftover;
    let mut scratch = Vec::with_capacity(8192);

    loop {
        // Exit if the client was closed (all user clones dropped).
        if inner.borrow().closed {
            return;
        }

        let prev_len = buf.len();
        let (_, s) = stream_read(&mut reader, &mut buf, scratch).await;
        scratch = s;

        // Re-check after await — client may have been closed while we waited.
        if inner.borrow().closed {
            return;
        }

        if buf.len() == prev_len {
            // Stream ended — connection lost.
            if config.max_reconnect_attempts == 0 {
                let mut inner = inner.borrow_mut();
                inner.closed = true;
                wake_all(&mut inner);
                return;
            }

            match attempt_reconnect(&inner, &config).await {
                Some(new_reader) => {
                    reader = new_reader;
                    buf.clear();
                    continue;
                }
                None => {
                    let mut inner = inner.borrow_mut();
                    inner.closed = true;
                    // Preserve a permanent error set by attempt_reconnect
                    // (e.g. authorization violation) over the generic message.
                    if inner.close_error.is_none() {
                        inner.close_error = Some("reconnection failed".into());
                    }
                    wake_all(&mut inner);
                    return;
                }
            }
        }

        let mut consumed_total = 0;
        loop {
            match proto::parse_op(&buf[consumed_total..]) {
                Ok(Some((op, consumed))) => {
                    consumed_total += consumed;
                    dispatch_op(&inner, op);
                }
                Ok(None) => break,
                Err(_) => {
                    let mut inner = inner.borrow_mut();
                    inner.closed = true;
                    inner.close_error = Some("protocol error".into());
                    wake_all(&mut inner);
                    return;
                }
            }
        }

        if consumed_total > 0 {
            buf = buf[consumed_total..].to_vec();
        }
    }
}

// ── Reconnection ───────────────────────────────────────────────────

async fn attempt_reconnect(
    inner: &Rc<RefCell<Inner>>,
    config: &ConnectConfig,
) -> Option<StreamReader<u8>> {
    let max = config.max_reconnect_attempts;
    let mut delay = config.reconnect_delay;
    let cap = secs(8);

    // Build the full candidate list: primary + user-supplied extras +
    // cluster URLs learned from server INFO. Deduplicate, preserve order.
    let mut candidates: Vec<String> = {
        let mut v = Vec::with_capacity(1 + config.servers.len() + 4);
        v.push(config.address.clone());
        v.extend(config.servers.iter().cloned());
        v.extend(inner.borrow().known_servers.iter().cloned());
        let mut seen = std::collections::HashSet::new();
        v.retain(|s| seen.insert(s.clone()));
        v
    };
    let ncandidates = candidates.len().max(1);
    // Reusable read buffer shared across reconnect attempts.
    let mut reconnect_scratch = Vec::with_capacity(8192);

    for attempt in 0..max {
        wasip3::clocks::monotonic_clock::wait_for(delay).await;
        if inner.borrow().closed {
            return None;
        }
        delay = if delay + delay < cap { delay + delay } else { cap };
        // Add ±25% jitter using the PRNG to spread out reconnection storms.
        let jitter_range = delay / 4;
        let rand_val = xorshift64(&mut inner.borrow_mut().rng);
        let jitter_offset = rand_val % jitter_range.max(1);
        delay = delay.saturating_sub(jitter_range / 2).saturating_add(jitter_offset);

        let addr = candidates[attempt as usize % ncandidates].clone();
        let sock_addr = match parse_address(&addr).await {
            Ok(a) => a,
            Err(_) => continue,
        };
        let family = match sock_addr {
            IpSocketAddress::Ipv4(_) => IpAddressFamily::Ipv4,
            IpSocketAddress::Ipv6(_) => IpAddressFamily::Ipv6,
        };
        let socket = match TcpSocket::create(family) {
            Ok(s) => s,
            Err(_) => continue,
        };
        if socket.connect(sock_addr).await.is_err() {
            continue;
        }

        let (mut rx, rx_fut) = socket.receive();
        let (tx, tx_rx) = wit_stream::new();
        let send_fut = socket.send(tx_rx);

        // Read INFO.
        let mut buf = Vec::new();
        let info = loop {
            let prev = buf.len();
            let (_, s) = stream_read(&mut rx, &mut buf, reconnect_scratch).await;
            reconnect_scratch = s;
            if buf.len() == prev {
                break None;
            }
            if let Ok(Some((ServerOp::Info(info), _))) = proto::parse_op(&buf) {
                break Some(info);
            }
        };
        let info = match info {
            Some(i) => i,
            None => continue,
        };

        // Absorb any new cluster URLs so subsequent attempts can try them.
        if !info.connect_urls.is_empty() {
            let mut inner_ref = inner.borrow_mut();
            for url in &info.connect_urls {
                if !candidates.contains(url) {
                    inner_ref.known_servers.push(url.clone());
                    candidates.push(url.clone());
                }
            }
        }

        // Upgrade to TLS if needed.
        let use_tls = config.tls || info.tls_required;
        #[cfg(not(feature = "tls"))]
        if use_tls {
            continue;
        }

        let (mut rx, mut tx) = if use_tls {
            #[cfg(feature = "tls")]
            {
                let server_name = config
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| {
                        addr.rsplit_once(':')
                            .map(|(h, _)| h.to_string())
                            .unwrap_or_default()
                    });
                match crate::tls::tls_upgrade(rx, tx, &server_name).await {
                    Ok(pair) => pair,
                    Err(_) => continue,
                }
            }
            #[cfg(not(feature = "tls"))]
            unreachable!()
        } else {
            (rx, tx)
        };

        // CONNECT + PING.
        let connect_opts = match build_connect_opts(config, &info, use_tls) {
            Ok(o) => o,
            Err(_) => continue,
        };
        let mut hdata = proto::encode_connect(&connect_opts);
        hdata.extend_from_slice(proto::PING);
        if stream_write_all(&mut tx, &hdata).await.is_err() {
            continue;
        }

        // Wait for PONG; detect and abort on permanent auth failures.
        buf.clear();
        let mut got_pong = false;
        'pong: for _ in 0..50 {
            let prev = buf.len();
            let (_, s) = stream_read(&mut rx, &mut buf, reconnect_scratch).await;
            reconnect_scratch = s;
            if buf.len() == prev {
                break;
            }
            while let Ok(Some((op, consumed))) = proto::parse_op(&buf) {
                buf = buf[consumed..].to_vec();
                match op {
                    ServerOp::Pong => {
                        got_pong = true;
                        break 'pong;
                    }
                    ServerOp::Ok => {}
                    ServerOp::Info(new_info) => {
                        // Server may resend INFO after TLS upgrade.
                        if !new_info.connect_urls.is_empty() {
                            inner.borrow_mut().known_servers = new_info.connect_urls;
                        }
                    }
                    ServerOp::Err(msg) => {
                        if is_permanent_auth_error(&msg) {
                            // Permanent failure — no point retrying.
                            inner.borrow_mut().close_error = Some(msg);
                            return None;
                        }
                        break 'pong;
                    }
                    _ => break 'pong,
                }
            }
        }
        if !got_pong {
            continue;
        }

        // Re-subscribe all live subscriptions on the new connection.
        let subs: Vec<(String, SubInfo)> = {
            let inner = inner.borrow();
            inner
                .subscriptions
                .iter()
                .map(|(sid, info)| (sid.clone(), info.clone()))
                .collect()
        };
        let mut resub = Vec::new();
        for (sid, info) in &subs {
            match &info.queue {
                Some(q) => {
                    if let Ok(d) = proto::encode_sub_queue(&info.subject, q, sid) {
                        resub.extend_from_slice(&d);
                    }
                }
                None => {
                    if let Ok(d) = proto::encode_sub(&info.subject, sid) {
                        resub.extend_from_slice(&d);
                    }
                }
            }
        }
        if !resub.is_empty() && stream_write_all(&mut tx, &resub).await.is_err() {
            continue;
        }

        // Hand new socket & writer to inner / flush loop.
        {
            let mut inner_ref = inner.borrow_mut();
            inner_ref._socket = Some(socket);
            inner_ref.new_writer = Some(tx);
            if let Some(w) = inner_ref.flush_waker.take() {
                w.wake();
            }
        }

        // Drain background futures so they don't leak.
        wit_bindgen::spawn(async move {
            let _ = rx_fut.await;
        });
        wit_bindgen::spawn(async move {
            let _ = send_fut.await;
        });

        return Some(rx);
    }

    None
}

/// Returns `true` for -ERR messages that indicate a permanent authentication
/// failure that no amount of reconnecting will fix.
fn is_permanent_auth_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("authorization violation")
        || m.contains("authentication expired")
        || m.contains("authentication revoked")
        || m.contains("user authentication expired")
        || m.contains("user authentication revoked")
}

/// Xorshift64 PRNG — advances `state` and returns the next pseudo-random u64.
/// `state` must never be 0; the seed initialisation in `Inner` guarantees this
/// via the XOR with a non-zero salt.
#[inline]
fn xorshift64(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

// ── Dispatch & helpers ─────────────────────────────────────────────

fn dispatch_op(inner: &Rc<RefCell<Inner>>, op: ServerOp) {
    match op {
        ServerOp::Msg(msg) => {
            dispatch_msg(
                inner,
                &msg.sid,
                Message {
                    subject: msg.subject,
                    reply_to: msg.reply_to,
                    headers: None,
                    payload: msg.payload,
                },
            );
        }
        ServerOp::HMsg(hmsg) => {
            dispatch_msg(
                inner,
                &hmsg.sid,
                Message {
                    subject: hmsg.subject,
                    reply_to: hmsg.reply_to,
                    headers: Some(hmsg.headers),
                    payload: hmsg.payload,
                },
            );
        }
        ServerOp::Ping => {
            let mut inner = inner.borrow_mut();
            inner.write_buf.extend_from_slice(proto::PONG);
            if let Some(w) = inner.flush_waker.take() {
                w.wake();
            }
        }
        ServerOp::Pong => {
            let mut inner = inner.borrow_mut();
            inner.pongs_received += 1;
            for w in std::mem::take(&mut inner.pong_wakers) {
                w.wake();
            }
        }
        ServerOp::Ok => {}
        ServerOp::Info(new_info) => {
            // Update known cluster nodes when server pushes topology changes.
            if !new_info.connect_urls.is_empty() {
                inner.borrow_mut().known_servers = new_info.connect_urls;
            }
        }
        ServerOp::Err(msg) => {
            let mut inner = inner.borrow_mut();
            inner.closed = true;
            inner.close_error = Some(msg);
            wake_all(&mut inner);
        }
    }
}

fn dispatch_msg(inner: &Rc<RefCell<Inner>>, sid: &str, message: Message) {
    let mut inner = inner.borrow_mut();

    // Check if message is a reply to a multiplexed request.
    if message.subject.starts_with(&inner.inbox_prefix) {
        let token = &message.subject[inner.inbox_prefix.len()..];
        if let Some(slot) = inner.pending_requests.get_mut(token) {
            slot.response = Some(message);
            if let Some(w) = slot.waker.take() {
                w.wake();
            }
            return;
        }
    }

    let capacity = inner.mailbox_capacity;
    if let Some(mailbox) = inner.mailboxes.get_mut(sid) {
        // Slow-consumer protection: drop the oldest message when at capacity.
        if mailbox.queue.len() >= capacity {
            mailbox.queue.pop_front();
        }
        mailbox.queue.push_back(message);
        if let Some(w) = mailbox.waker.take() {
            w.wake();
        }
    }
}

fn wake_all(inner: &mut Inner) {
    for mailbox in inner.mailboxes.values_mut() {
        if let Some(w) = mailbox.waker.take() {
            w.wake();
        }
    }
    for slot in inner.pending_requests.values_mut() {
        if let Some(w) = slot.waker.take() {
            w.wake();
        }
    }
    if let Some(w) = inner.flush_waker.take() {
        w.wake();
    }
    for w in std::mem::take(&mut inner.pong_wakers) {
        w.wake();
    }
}

// ── Background flush loop ──────────────────────────────────────────

async fn flush_loop(inner: Rc<RefCell<Inner>>, mut writer: StreamWriter<u8>) {
    loop {
        FlushWait { inner: &inner }.await;

        {
            let mut inner_ref = inner.borrow_mut();
            if let Some(new_writer) = inner_ref.new_writer.take() {
                writer = new_writer;
            }
        }

        let data = {
            let mut inner = inner.borrow_mut();
            if inner.closed {
                return;
            }
            if inner.write_buf.is_empty() {
                continue;
            }
            std::mem::take(&mut inner.write_buf)
        };

        if let Err((unwritten, _)) = stream_write_vec(&mut writer, data).await {
            // Re-queue the unwritten data so it will be retried once the reconnect
            // logic delivers a new writer via `inner.new_writer`.
            let mut inner_ref = inner.borrow_mut();
            let mut requeued = unwritten;
            requeued.extend_from_slice(&inner_ref.write_buf);
            inner_ref.write_buf = requeued;
            continue;
        }
    }
}

struct FlushWait<'a> {
    inner: &'a Rc<RefCell<Inner>>,
}

impl<'a> Future for FlushWait<'a> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed || !inner.write_buf.is_empty() {
            Poll::Ready(())
        } else {
            inner.flush_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xorshift64_never_repeats_in_short_run() {
        let mut state = 0xcafe_babe_dead_beef_u64;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..1_000 {
            let v = xorshift64(&mut state);
            assert!(seen.insert(v), "xorshift64 produced a duplicate in the first 1000 outputs");
        }
    }

    #[test]
    fn xorshift64_nonzero_seed_stays_nonzero() {
        let mut state = 1u64;
        for _ in 0..100 {
            let v = xorshift64(&mut state);
            assert_ne!(v, 0, "xorshift64 must never output 0 given a valid seed");
        }
    }

    #[test]
    fn inbox_format_is_32_hex_chars() {
        // Verify the _INBOX. prefix + exactly 32 hex characters (two u64s).
        let mut rng = 0x1234_5678_9abc_def0_u64;
        let r1 = xorshift64(&mut rng);
        let r2 = xorshift64(&mut rng);
        let inbox = format!("_INBOX.{r1:016x}{r2:016x}");
        assert!(inbox.starts_with("_INBOX."), "wrong prefix");
        let token = &inbox["_INBOX.".len()..];
        assert_eq!(token.len(), 32, "token should be 32 hex chars");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()), "non-hex char in token");
    }

    #[test]
    fn mux_inbox_dispatch_and_raii_cleanup() {
        let inner = Rc::new(RefCell::new(Inner {
            _socket: None,
            mailboxes: HashMap::new(),
            subscriptions: HashMap::new(),
            pending_requests: HashMap::new(),
            inbox_prefix: "_INBOX.testclient1234.".to_string(),
            write_buf: Vec::new(),
            flush_waker: None,
            pong_wakers: Vec::new(),
            pongs_received: 0,
            next_id: 1,
            rng: 12345,
            closed: false,
            close_error: None,
            new_writer: None,
            known_servers: Vec::new(),
            mailbox_capacity: 64,
            write_buf_limit: 1024,
            max_payload: 1024,
        }));

        let token = "deadbeef01020304".to_string();
        inner.borrow_mut().pending_requests.insert(
            token.clone(),
            RequestSlot {
                response: None,
                waker: None,
            },
        );

        // Verify message for this token is routed to pending_requests
        let reply_subject = format!("_INBOX.testclient1234.{token}");
        dispatch_msg(
            &inner,
            "0",
            Message {
                subject: reply_subject.clone(),
                reply_to: None,
                headers: None,
                payload: b"pong-response".to_vec(),
            },
        );

        {
            let mut inner_mut = inner.borrow_mut();
            let slot = inner_mut.pending_requests.get_mut(&token).expect("slot exists");
            let resp = slot.response.take().expect("got response");
            assert_eq!(resp.payload, b"pong-response");
        }

        // Test RAII Drop cleanup
        let req_fut = RequestFuture {
            token: token.clone(),
            inner: Rc::clone(&inner),
        };
        assert!(inner.borrow().pending_requests.contains_key(&token));
        drop(req_fut);
        assert!(!inner.borrow().pending_requests.contains_key(&token), "drop must remove pending request slot");
    }

    #[test]
    fn pong_tracking_wakes_all_waiters() {
        let inner = Rc::new(RefCell::new(Inner {
            _socket: None,
            mailboxes: HashMap::new(),
            subscriptions: HashMap::new(),
            pending_requests: HashMap::new(),
            inbox_prefix: "_INBOX.testclient.".to_string(),
            write_buf: Vec::new(),
            flush_waker: None,
            pong_wakers: Vec::new(),
            pongs_received: 0,
            next_id: 1,
            rng: 12345,
            closed: false,
            close_error: None,
            new_writer: None,
            known_servers: Vec::new(),
            mailbox_capacity: 64,
            write_buf_limit: 1024,
            max_payload: 1024,
        }));

        assert_eq!(inner.borrow().pongs_received, 0);
        dispatch_op(&inner, ServerOp::Pong);
        assert_eq!(inner.borrow().pongs_received, 1);
        dispatch_op(&inner, ServerOp::Pong);
        assert_eq!(inner.borrow().pongs_received, 2);
    }
}
