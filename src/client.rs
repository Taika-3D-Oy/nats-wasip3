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
    pub address: String,
    pub name: Option<String>,
    pub auth_token: Option<String>,
    pub user: Option<String>,
    pub pass: Option<String>,
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
}

impl Default for ConnectConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:4222".to_string(),
            name: Some("nats-wasi".to_string()),
            auth_token: None,
            user: None,
            pass: None,
            tls: false,
            #[cfg(feature = "tls")]
            tls_server_name: None,
            #[cfg(feature = "nkey")]
            nkey_seed: None,
            max_reconnect_attempts: 5,
            reconnect_delay: millis(250),
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

struct Inner {
    mailboxes: HashMap<String, Mailbox>,
    subscriptions: HashMap<String, SubInfo>,
    write_buf: Vec<u8>,
    flush_waker: Option<Waker>,
    /// Woken when a PONG arrives — used by Client::flush().
    pong_waker: Option<Waker>,
    next_id: u64,
    closed: bool,
    close_error: Option<String>,
    /// Set by reconnect logic; picked up by the flush loop.
    new_writer: Option<StreamWriter<u8>>,
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
            // Last user clone dropped — shut down background loops.
            let mut inner = self.inner.borrow_mut();
            if !inner.closed {
                inner.closed = true;
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
        let _send_fut = socket.send(tx_rx);

        // ── Read INFO ──────────────────────────────────────────
        let mut buf = Vec::new();
        let info = loop {
            if stream_read(&mut rx, &mut buf).await == 0 && buf.is_empty() {
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

        // ── Send CONNECT + PING ────────────────────────────────
        let connect_opts = build_connect_opts(&config, &info, use_tls)?;
        let mut handshake = proto::encode_connect(&connect_opts);
        handshake.extend_from_slice(proto::PING);
        stream_write_all(&mut tx, &handshake).await?;

        // ── Wait for PONG ──────────────────────────────────────
        loop {
            if stream_read(&mut rx, &mut buf).await == 0 && buf.is_empty() {
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

        // ── Shared state ───────────────────────────────────────
        let inner = Rc::new(RefCell::new(Inner {
            mailboxes: HashMap::new(),
            subscriptions: HashMap::new(),
            write_buf: Vec::new(),
            flush_waker: None,
            pong_waker: None,
            next_id: 1,
            closed: false,
            close_error: None,
            new_writer: None,
        }));

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
        let data = proto::encode_pub(subject, None, payload)?;
        self.enqueue_write(&data);
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
        let data = proto::encode_pub(subject, Some(reply_to), payload)?;
        self.enqueue_write(&data);
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
        let data = proto::encode_hpub(subject, reply_to, headers, payload)?;
        self.enqueue_write(&data);
        Ok(())
    }

    /// Subscribe to a subject.
    pub fn subscribe(&self, subject: &str) -> Result<Subscription, Error> {
        self.check_closed()?;
        let sid = self.next_sid();
        let data = proto::encode_sub(subject, &sid)?;
        self.enqueue_write(&data);

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
        self.enqueue_write(&data);

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

    /// Send a request and wait for a single reply.
    pub async fn request(
        &self,
        subject: &str,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, Error> {
        self.check_closed()?;
        let inbox = self.new_inbox();
        let sid = self.next_sid();

        let mut batch = Vec::new();
        batch.extend_from_slice(&proto::encode_sub(&inbox, &sid)?);
        batch.extend_from_slice(&proto::encode_unsub(&sid, Some(1)));
        batch.extend_from_slice(&proto::encode_pub(subject, Some(&inbox), payload)?);

        {
            let mut inner = self.inner.borrow_mut();
            inner.mailboxes.insert(
                sid.clone(),
                Mailbox {
                    queue: VecDeque::new(),
                    waker: None,
                },
            );
        }
        self.enqueue_write(&batch);

        let reply_fut = RequestFuture {
            sid: sid.clone(),
            inner: Rc::clone(&self.inner),
        };

        let result = with_timeout(timeout, reply_fut).await?;
        self.inner.borrow_mut().mailboxes.remove(&sid);

        let msg = result?;
        if let Some(ref hdrs) = msg.headers {
            if hdrs.status == Some(503) {
                return Err(Error::NoResponders);
            }
        }
        Ok(msg)
    }

    /// Request with headers.
    pub async fn request_with_headers(
        &self,
        subject: &str,
        headers: &Headers,
        payload: &[u8],
        timeout: Duration,
    ) -> Result<Message, Error> {
        self.check_closed()?;
        let inbox = self.new_inbox();
        let sid = self.next_sid();

        let mut batch = Vec::new();
        batch.extend_from_slice(&proto::encode_sub(&inbox, &sid)?);
        batch.extend_from_slice(&proto::encode_unsub(&sid, Some(1)));
        batch.extend_from_slice(&proto::encode_hpub(
            subject,
            Some(&inbox),
            headers,
            payload,
        )?);

        {
            let mut inner = self.inner.borrow_mut();
            inner.mailboxes.insert(
                sid.clone(),
                Mailbox {
                    queue: VecDeque::new(),
                    waker: None,
                },
            );
        }
        self.enqueue_write(&batch);

        let reply_fut = RequestFuture {
            sid: sid.clone(),
            inner: Rc::clone(&self.inner),
        };

        let result = with_timeout(timeout, reply_fut).await?;
        self.inner.borrow_mut().mailboxes.remove(&sid);

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
        wake_all(&mut inner);
    }

    /// Flush pending writes and wait for a PONG from the server, confirming
    /// that all previously published messages have been received.
    /// Returns `Err(Error::Timeout)` if the server doesn't respond within `timeout`.
    pub async fn flush(&self, timeout: Duration) -> Result<(), Error> {
        self.check_closed()?;
        // Enqueue a PING — server will echo back PONG once it processes everything.
        {
            let mut inner = self.inner.borrow_mut();
            inner.write_buf.extend_from_slice(proto::PING);
            if let Some(w) = inner.flush_waker.take() {
                w.wake();
            }
        }
        with_timeout(timeout, PongWait { inner: &self.inner, registered: false }).await?
    }

    fn next_sid(&self) -> String {
        let mut inner = self.inner.borrow_mut();
        let id = inner.next_id;
        inner.next_id += 1;
        id.to_string()
    }

    pub(crate) fn new_inbox(&self) -> String {
        let mut inner = self.inner.borrow_mut();
        let id = inner.next_id;
        inner.next_id += 1;
        format!("_INBOX.{id}")
    }

    fn enqueue_write(&self, data: &[u8]) {
        let mut inner = self.inner.borrow_mut();
        inner.write_buf.extend_from_slice(data);
        if let Some(w) = inner.flush_waker.take() {
            w.wake();
        }
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

/// Build a `ConnectOptions` from config + server INFO, signing the nonce
/// with an NKey if `nkey_seed` is set.
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
        tls_required: use_tls,
        ..Default::default()
    };

    #[cfg(feature = "nkey")]
    if let Some(ref seed) = config.nkey_seed {
        let kp = crate::nkey::KeyPair::from_seed(seed)?;
        let nonce = _info
            .nonce
            .as_deref()
            .ok_or_else(|| Error::Protocol("server did not send nonce for nkey auth".into()))?;
        opts.sig = Some(kp.sign(nonce.as_bytes()));
        opts.nkey = Some(kp.public_key());
    }

    Ok(opts)
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

/// Read from a P3 `StreamReader<u8>`, appending to `buf`. Returns bytes read.
async fn stream_read(rx: &mut StreamReader<u8>, buf: &mut Vec<u8>) -> usize {
    let read_buf = Vec::with_capacity(8192);
    let (status, data) = rx.read(read_buf).await;
    match status {
        StreamResult::Complete(n) => {
            buf.extend_from_slice(&data[..n]);
            n
        }
        StreamResult::Dropped | StreamResult::Cancelled => 0,
    }
}

/// Write all bytes to a P3 `StreamWriter<u8>`.
async fn stream_write_all(tx: &mut StreamWriter<u8>, data: &[u8]) -> Result<(), Error> {
    let remaining = tx.write_all(data.to_vec()).await;
    if !remaining.is_empty() {
        return Err(Error::Disconnected);
    }
    Ok(())
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
    sid: String,
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
        if let Some(mailbox) = inner.mailboxes.get_mut(&self.sid) {
            if let Some(msg) = mailbox.queue.pop_front() {
                return Poll::Ready(Ok(msg));
            }
            mailbox.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

struct PongWait<'a> {
    inner: &'a Rc<RefCell<Inner>>,
    /// Tracks whether we've installed the waker in `inner.pong_waker` yet.
    registered: bool,
}

impl<'a> Future for PongWait<'a> {
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();
        if inner.closed {
            return Poll::Ready(Err(match &inner.close_error {
                Some(msg) => Error::Server(msg.clone()),
                None => Error::Disconnected,
            }));
        }
        if self.registered {
            // We've already installed the waker. If dispatch_op fired it,
            // pong_waker is now None — that means the PONG has arrived.
            if inner.pong_waker.is_none() {
                return Poll::Ready(Ok(()));
            }
            // Spurious wakeup — update the waker in case it changed.
            inner.pong_waker = Some(cx.waker().clone());
        } else {
            // First poll: install our waker and wait.
            inner.pong_waker = Some(cx.waker().clone());
            self.registered = true;
        }
        Poll::Pending
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

    loop {
        // Exit if the client was closed (all user clones dropped).
        if inner.borrow().closed {
            return;
        }

        let prev_len = buf.len();
        stream_read(&mut reader, &mut buf).await;

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
                    inner.close_error = Some("reconnection failed".into());
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

    for _ in 0..max {
        wasip3::clocks::monotonic_clock::wait_for(delay).await;
        // Abort reconnection if the client was closed.
        if inner.borrow().closed {
            return None;
        }
        delay = if delay + delay < cap { delay + delay } else { cap };
        // Add ±25% jitter to spread out reconnection storms.
        // Use a simple deterministic mix derived from the current delay value.
        let jitter_range = delay / 4;
        let jitter_offset = (delay.wrapping_mul(0x9e3779b97f4a7c15) >> 32) % jitter_range.max(1);
        delay = delay.saturating_sub(jitter_range / 2).saturating_add(jitter_offset);

        let sock_addr = match parse_address(&config.address).await {
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

        let (mut rx, _rx_fut) = socket.receive();
        let (tx, tx_rx) = wit_stream::new();
        let _send_fut = socket.send(tx_rx);

        // Read INFO.
        let mut buf = Vec::new();
        let info = loop {
            let prev = buf.len();
            stream_read(&mut rx, &mut buf).await;
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
                        config
                            .address
                            .rsplit_once(':')
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

        // Wait for PONG.
        buf.clear();
        let mut got_pong = false;
        for _ in 0..50 {
            let prev = buf.len();
            stream_read(&mut rx, &mut buf).await;
            if buf.len() == prev {
                break;
            }
            while let Ok(Some((op, consumed))) = proto::parse_op(&buf) {
                buf = buf[consumed..].to_vec();
                match op {
                    ServerOp::Pong => {
                        got_pong = true;
                        break;
                    }
                    ServerOp::Ok => continue,
                    _ => break,
                }
            }
            if got_pong {
                break;
            }
        }
        if !got_pong {
            continue;
        }

        // Re-subscribe.
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

        // Hand new writer to flush loop.
        {
            let mut inner_ref = inner.borrow_mut();
            inner_ref.new_writer = Some(tx);
            if let Some(w) = inner_ref.flush_waker.take() {
                w.wake();
            }
        }

        return Some(rx);
    }

    None
}

// ── Dispatch & helpers ─────────────────────────────────────────────

fn dispatch_op(inner: &Rc<RefCell<Inner>>, op: ServerOp) {
    match op {
        ServerOp::Msg(msg) => {
            deliver(
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
            deliver(
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
            // Wake any pending flush() call waiting for the PONG.
            if let Some(w) = inner.borrow_mut().pong_waker.take() {
                w.wake();
            }
        }
        ServerOp::Ok | ServerOp::Info(_) => {}
        ServerOp::Err(msg) => {
            let mut inner = inner.borrow_mut();
            inner.closed = true;
            inner.close_error = Some(msg);
            wake_all(&mut inner);
        }
    }
}

fn deliver(inner: &Rc<RefCell<Inner>>, sid: &str, message: Message) {
    let mut inner = inner.borrow_mut();
    if let Some(mailbox) = inner.mailboxes.get_mut(sid) {
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
    if let Some(w) = inner.flush_waker.take() {
        w.wake();
    }
    if let Some(w) = inner.pong_waker.take() {
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

        if stream_write_all(&mut writer, &data).await.is_err() {
            // Re-queue the data so it will be retried once the reconnect
            // logic delivers a new writer via `inner.new_writer`.
            let mut inner_ref = inner.borrow_mut();
            let mut requeued = data;
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
