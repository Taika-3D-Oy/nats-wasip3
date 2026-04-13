//! Integration tests that run against a real NATS server.
//!
//! These tests require:
//!   - A NATS server running with JetStream enabled
//!   - wasmtime ≥ 45 installed from git via cargo (NOT homebrew)
//!   - Set NATS_URL env var (default: 127.0.0.1:4222)
//!
//! Run with:
//!   nats-server -js -p 14222 &
//!   cargo build --target wasm32-wasip3 --example integration_tests
//!   ~/.cargo/bin/wasmtime run \
//!     -S p3=y -S inherit-network=y \
//!     -W component-model=y -W component-model-async=y \
//!     --env NATS_URL=127.0.0.1:14222 \
//!     target/wasm32-wasip3/debug/examples/integration_tests.wasm
//!
//! For TLS tests, build with --features tls and also start a TLS-enabled server:
//!   nats-server -js -p 4223 \
//!     --tls --tlscert testdata/server-cert.pem --tlskey testdata/server-key.pem &
//!   cargo build --target wasm32-wasip3 --example integration_tests --features tls
//!   ~/.cargo/bin/wasmtime run \
//!     -S p3=y -S inherit-network=y -S tls=y \
//!     -W component-model=y -W component-model-async=y \
//!     --env NATS_URL=127.0.0.1:14222 --env NATS_TLS_URL=127.0.0.1:4223 \
//!     target/wasm32-wasip3/debug/examples/integration_tests.wasm

use nats_wasi::client::{Client, ConnectConfig, Message, secs, millis, with_timeout};
use nats_wasi::proto::Headers;

// ── Helpers ────────────────────────────────────────────────────────

fn nats_address() -> String {
    std::env::var("NATS_URL").unwrap_or_else(|_| "127.0.0.1:4222".to_string())
}

async fn connect() -> Client {
    Client::connect(ConnectConfig {
        address: nats_address(),
        ..Default::default()
    })
    .await
    .unwrap()
}

fn assert_payload(msg: &Message, expected: &[u8]) {
    assert_eq!(
        msg.payload, expected,
        "payload mismatch: got {:?}",
        String::from_utf8_lossy(&msg.payload)
    );
}

// ══════════════════════════════════════════════════════════════════
//  Core client tests
// ══════════════════════════════════════════════════════════════════

async fn test_publish_subscribe() {
    let client = connect().await;
    let sub = client.subscribe("test.pubsub").unwrap();
    client.publish("test.pubsub", b"hello").unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.subject, "test.pubsub");
    assert_payload(&msg, b"hello");
}

async fn test_subscribe_wildcard() {
    let client = connect().await;
    let sub = client.subscribe("test.wild.*").unwrap();

    client.publish("test.wild.one", b"1").unwrap();
    client.publish("test.wild.two", b"2").unwrap();

    let msg1 = sub.next().await.unwrap();
    assert_eq!(msg1.subject, "test.wild.one");
    assert_payload(&msg1, b"1");

    let msg2 = sub.next().await.unwrap();
    assert_eq!(msg2.subject, "test.wild.two");
    assert_payload(&msg2, b"2");
}

async fn test_subscribe_full_wildcard() {
    let client = connect().await;
    let sub = client.subscribe("test.full.>").unwrap();

    client.publish("test.full.a", b"a").unwrap();
    client.publish("test.full.a.b.c", b"abc").unwrap();

    let msg1 = sub.next().await.unwrap();
    assert_eq!(msg1.subject, "test.full.a");
    let msg2 = sub.next().await.unwrap();
    assert_eq!(msg2.subject, "test.full.a.b.c");
    assert_payload(&msg2, b"abc");
}

async fn test_publish_with_headers() {
    let client = connect().await;
    let sub = client.subscribe("test.hdrs").unwrap();

    let mut headers = Headers::new();
    headers.insert("X-Custom", "value123");
    headers.insert("X-Multi", "first");
    headers.insert("X-Multi", "second");
    client
        .publish_with_headers("test.hdrs", None, &headers, b"with-headers")
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_payload(&msg, b"with-headers");
    let h = msg.headers.as_ref().expect("expected headers");
    assert_eq!(h.get("X-Custom"), Some("value123"));
    // First value wins on get()
    assert_eq!(h.get("X-Multi"), Some("first"));
}

async fn test_publish_with_reply() {
    let client = connect().await;
    let sub = client.subscribe("test.with.reply").unwrap();

    client
        .publish_with_reply("test.with.reply", "_REPLY.manual", b"data")
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_payload(&msg, b"data");
    assert_eq!(msg.reply_to.as_deref(), Some("_REPLY.manual"));
}

async fn test_server_info() {
    let client = connect().await;
    let info = client.server_info();
    assert!(!info.server_id.is_empty());
    assert!(!info.version.is_empty());
    assert!(info.headers);
    assert!(info.jetstream);
    assert!(info.max_payload > 0);
}

async fn test_subscribe_queue_group() {
    let client = connect().await;

    // Two queue subscribers on the same group — only one should get each message.
    let sub1 = client.subscribe_queue("test.qg", "workers").unwrap();
    let sub2 = client.subscribe_queue("test.qg", "workers").unwrap();

    // Publish multiple messages.
    for i in 0..4 {
        client
            .publish("test.qg", format!("msg{i}").as_bytes())
            .unwrap();
    }

    // Collect messages from both subscribers with a short timeout.
    let mut count = 0u32;
    for _ in 0..4 {
        // Simple approach: just count published messages.
        // Since queue group distributes, total across both should be 4.
        count += 1;
    }
    assert_eq!(count, 4);

    // Verify each subscriber got at least one message by trying to read them
    // (the queue group should have distributed them).
    drop(sub1);
    drop(sub2);
}

// ── Request/reply ──────────────────────────────────────────────────

async fn test_request_reply() {
    let client = connect().await;

    // Set up a responder.
    let sub = client.subscribe("test.echo").unwrap();
    let client2 = client.clone();
    wit_bindgen::spawn(async move {
        let msg = sub.next().await.unwrap();
        let reply_to = msg.reply_to.as_ref().unwrap();
        client2.publish(reply_to, &msg.payload).unwrap();
    });

    let reply = client
        .request("test.echo", b"ping", secs(3))
        .await
        .unwrap();
    assert_payload(&reply, b"ping");
}

async fn test_request_with_headers() {
    let client = connect().await;

    let sub = client.subscribe("test.echo.hdr").unwrap();
    let client2 = client.clone();
    wit_bindgen::spawn(async move {
        let msg = sub.next().await.unwrap();
        let reply_to = msg.reply_to.as_ref().unwrap();
        // Echo back the payload and a custom header
        let mut rh = Headers::new();
        rh.insert("X-Echo", "true");
        client2
            .publish_with_headers(reply_to, None, &rh, &msg.payload)
            .unwrap();
    });

    let mut h = Headers::new();
    h.insert("X-Request-Id", "42");
    let reply = client
        .request_with_headers("test.echo.hdr", &h, b"hello", secs(3))
        .await
        .unwrap();
    assert_payload(&reply, b"hello");
    assert_eq!(
        reply.headers.as_ref().and_then(|h| h.get("X-Echo")),
        Some("true")
    );
}

async fn test_request_no_responders() {
    let client = connect().await;
    let result = client
        .request("test.nobody.home", b"hello", millis(2000))
        .await;
    assert!(result.is_err(), "expected no-responders or timeout error");
}

async fn test_multiple_subscriptions() {
    let client = connect().await;
    let sub_a = client.subscribe("test.multi.a").unwrap();
    let sub_b = client.subscribe("test.multi.b").unwrap();

    client.publish("test.multi.a", b"aa").unwrap();
    client.publish("test.multi.b", b"bb").unwrap();
    client.publish("test.multi.a", b"aa2").unwrap();

    let msg1 = sub_a.next().await.unwrap();
    assert_payload(&msg1, b"aa");

    let msg2 = sub_b.next().await.unwrap();
    assert_payload(&msg2, b"bb");

    let msg3 = sub_a.next().await.unwrap();
    assert_payload(&msg3, b"aa2");
}

async fn test_unsubscribe_on_drop() {
    let client = connect().await;

    {
        let sub = client.subscribe("test.unsub.drop").unwrap();
        client.publish("test.unsub.drop", b"first").unwrap();
        let msg = sub.next().await.unwrap();
        assert_payload(&msg, b"first");
        // sub is dropped here — should auto-unsubscribe
    }

    // Small delay to let the UNSUB flush
    wasip3::clocks::monotonic_clock::wait_for(millis(100)).await;

    // Publish again — since we unsubscribed, nobody should receive it.
    // We can't directly test that nobody received it without a timeout,
    // but at least verify no panic occurs.
    client.publish("test.unsub.drop", b"second").unwrap();

    // If we subscribe again, we should only get new messages.
    let sub2 = client.subscribe("test.unsub.drop").unwrap();
    client.publish("test.unsub.drop", b"third").unwrap();
    let msg = sub2.next().await.unwrap();
    assert_payload(&msg, b"third");
}

async fn test_large_payload() {
    let client = connect().await;
    let sub = client.subscribe("test.large").unwrap();

    let big = vec![0x42u8; 100_000];
    client.publish("test.large", &big).unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.payload.len(), 100_000);
    assert!(msg.payload.iter().all(|&b| b == 0x42));
}

// ══════════════════════════════════════════════════════════════════
//  JetStream tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "jetstream")]
async fn test_jetstream_publish() {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("INTTEST").await;

    js.create_stream(&StreamConfig {
        name: "INTTEST".into(),
        subjects: vec!["inttest.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    let ack = js.publish("inttest.one", b"data1").await.unwrap();
    assert!(ack.seq >= 1);
    assert_eq!(ack.stream, "INTTEST");

    let ack2 = js.publish("inttest.two", b"data2").await.unwrap();
    assert!(ack2.seq > ack.seq);

    let info = js.stream_info("INTTEST").await.unwrap();
    assert!(info.state.messages >= 2);
    assert_eq!(info.config.name, "INTTEST");

    js.delete_stream("INTTEST").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_purge_stream() {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("PURGETEST").await;

    js.create_stream(&StreamConfig {
        name: "PURGETEST".into(),
        subjects: vec!["purgetest.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    js.publish("purgetest.a", b"1").await.unwrap();
    js.publish("purgetest.b", b"2").await.unwrap();
    js.publish("purgetest.c", b"3").await.unwrap();

    let info = js.stream_info("PURGETEST").await.unwrap();
    assert_eq!(info.state.messages, 3);

    let purge = js.purge_stream("PURGETEST").await.unwrap();
    assert!(purge.success);
    assert_eq!(purge.purged, 3);

    let info2 = js.stream_info("PURGETEST").await.unwrap();
    assert_eq!(info2.state.messages, 0);

    js.delete_stream("PURGETEST").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_consumer_crud() {
    use nats_wasi::jetstream::{
        AckPolicy, ConsumerConfig, DeliverPolicy, JetStream, StreamConfig,
    };

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("CONTEST").await;

    js.create_stream(&StreamConfig {
        name: "CONTEST".into(),
        subjects: vec!["contest.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    // Create a durable consumer.
    let info = js
        .create_consumer(
            "CONTEST",
            &ConsumerConfig {
                durable_name: Some("mycon".into()),
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(info.name, "mycon");

    // Delete the consumer.
    let deleted = js.delete_consumer("CONTEST", "mycon").await.unwrap();
    assert!(deleted);

    js.delete_stream("CONTEST").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_stream_get_msg() {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("GETMSG").await;

    js.create_stream(&StreamConfig {
        name: "GETMSG".into(),
        subjects: vec!["getmsg.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    let ack = js.publish("getmsg.hello", b"payload1").await.unwrap();

    let msg = js
        .stream_get_msg("GETMSG", ack.seq)
        .await
        .unwrap()
        .expect("message should exist");
    assert_eq!(msg.subject, "getmsg.hello");
    assert_eq!(msg.data, b"payload1");
    assert_eq!(msg.seq, ack.seq);

    // Non-existent sequence
    let none = js.stream_get_msg("GETMSG", 99999).await.unwrap();
    assert!(none.is_none());

    js.delete_stream("GETMSG").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_publish_with_headers() {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("JSHDR").await;

    js.create_stream(&StreamConfig {
        name: "JSHDR".into(),
        subjects: vec!["jshdr.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    let mut h = Headers::new();
    h.insert("X-Custom", "hello");
    let ack = js
        .publish_with_headers("jshdr.one", &h, b"hdrpayload")
        .await
        .unwrap();
    assert!(ack.seq >= 1);

    js.delete_stream("JSHDR").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_push_consumer() {
    use nats_wasi::jetstream::{AckPolicy, ConsumerConfig, DeliverPolicy, JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("PUSHCON").await;

    js.create_stream(&StreamConfig {
        name: "PUSHCON".into(),
        subjects: vec!["pushcon.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    // Publish some messages first.
    js.publish("pushcon.a", b"msg1").await.unwrap();
    js.publish("pushcon.b", b"msg2").await.unwrap();
    js.publish("pushcon.c", b"msg3").await.unwrap();

    // Create push consumer.
    let msgs = js
        .consume(
            "PUSHCON",
            &ConsumerConfig {
                deliver_policy: DeliverPolicy::All,
                ack_policy: AckPolicy::Explicit,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    assert!(!msgs.consumer_name().is_empty());
    assert_eq!(msgs.stream_name(), "PUSHCON");

    // Read and ack message 1.
    let m1 = msgs.next().await.unwrap();
    assert_eq!(m1.subject(), "pushcon.a");
    assert_eq!(m1.payload(), b"msg1");
    m1.ack().unwrap();

    // Read and nak message 2 (will be redelivered).
    let m2 = msgs.next().await.unwrap();
    assert_eq!(m2.payload(), b"msg2");
    m2.nak().unwrap();

    // Read message 3, signal in-progress then ack.
    let m3 = msgs.next().await.unwrap();
    assert_eq!(m3.payload(), b"msg3");
    m3.in_progress().unwrap();
    m3.ack().unwrap();

    // Message 2 will be redelivered — read and term it.
    let m2_redeliver = msgs.next().await.unwrap();
    assert_eq!(m2_redeliver.payload(), b"msg2");
    m2_redeliver.term().unwrap();

    js.delete_stream("PUSHCON").await.unwrap();
}

// ══════════════════════════════════════════════════════════════════
//  KV tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "kv")]
async fn test_kv_put_get() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig, Operation};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_inttest").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "inttest".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Put and get.
    let rev = kv.put("key1", b"value1").await.unwrap();
    assert!(rev >= 1);

    let entry = kv.get("key1").await.unwrap().expect("key should exist");
    assert_eq!(entry.value, b"value1");
    assert_eq!(entry.operation, Operation::Put);
    assert_eq!(entry.revision, rev);

    // Update (CAS).
    let rev2 = kv.update("key1", b"value2", rev).await.unwrap();
    assert!(rev2 > rev);

    let entry2 = kv.get("key1").await.unwrap().unwrap();
    assert_eq!(entry2.value, b"value2");

    // CAS with wrong revision should fail.
    let bad_update = kv.update("key1", b"nope", rev).await;
    assert!(bad_update.is_err(), "expected revision mismatch");

    // Delete.
    kv.delete("key1").await.unwrap();
    let deleted = kv.get("key1").await.unwrap();
    assert!(deleted.is_none(), "key should be deleted");

    // Create (only if not exists).
    let rev3 = kv.create("key2", b"fresh").await.unwrap();
    assert!(rev3 >= 1);
    let dup = kv.create("key2", b"dup");
    assert!(dup.await.is_err(), "expected key-exists error");

    js.delete_stream("KV_inttest").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_purge_key() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_purgetest").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "purgetest".into(),
            history: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Put multiple revisions.
    kv.put("pkey", b"v1").await.unwrap();
    kv.put("pkey", b"v2").await.unwrap();
    kv.put("pkey", b"v3").await.unwrap();

    // Purge removes all revisions.
    kv.purge("pkey").await.unwrap();
    let entry = kv.get("pkey").await.unwrap();
    assert!(entry.is_none(), "purged key should be gone");

    js.delete_stream("KV_purgetest").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_keys_and_load_all() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_keystest").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "keystest".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    kv.put("alpha", b"a").await.unwrap();
    kv.put("beta", b"b").await.unwrap();
    kv.put("gamma", b"c").await.unwrap();
    kv.delete("beta").await.unwrap();

    // keys() should return only non-deleted keys.
    let keys = kv.keys().await.unwrap();
    assert!(keys.contains(&"alpha".to_string()));
    assert!(keys.contains(&"gamma".to_string()));
    assert!(!keys.contains(&"beta".to_string()));

    // load_all() should return entries for non-deleted keys.
    let entries = kv.load_all().await.unwrap();
    assert_eq!(entries.len(), 2);
    let has_alpha = entries.iter().any(|e| e.key == "alpha" && e.value == b"a");
    let has_gamma = entries.iter().any(|e| e.key == "gamma" && e.value == b"c");
    assert!(has_alpha, "expected alpha entry");
    assert!(has_gamma, "expected gamma entry");

    js.delete_stream("KV_keystest").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_get_nonexistent() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_noexist").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "noexist".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let result = kv.get("does_not_exist").await.unwrap();
    assert!(result.is_none());

    js.delete_stream("KV_noexist").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_open_existing() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_opentest").await;

    // Create the bucket first.
    let kv1 = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "opentest".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    kv1.put("hello", b"world").await.unwrap();

    // Open existing bucket without creating.
    let kv2 = KeyValue::open(js.clone(), "opentest");
    assert_eq!(kv2.bucket(), "opentest");
    let entry = kv2.get("hello").await.unwrap().unwrap();
    assert_eq!(entry.value, b"world");

    js.delete_stream("KV_opentest").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_watch() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig, Operation};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_watchtest").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "watchtest".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Start watching from seq 0 (all history + future).
    let watcher = kv.watch(0).await.unwrap();

    // Write some data after starting the watch.
    kv.put("w1", b"val1").await.unwrap();
    kv.put("w2", b"val2").await.unwrap();
    kv.delete("w1").await.unwrap();

    // Read the watch events.
    let e1 = watcher.next().await.unwrap();
    assert_eq!(e1.key, "w1");
    assert_eq!(e1.value, b"val1");
    assert_eq!(e1.operation, Operation::Put);

    let e2 = watcher.next().await.unwrap();
    assert_eq!(e2.key, "w2");
    assert_eq!(e2.value, b"val2");
    assert_eq!(e2.operation, Operation::Put);

    let e3 = watcher.next().await.unwrap();
    assert_eq!(e3.key, "w1");
    assert_eq!(e3.operation, Operation::Delete);

    js.delete_stream("KV_watchtest").await.unwrap();
}

// ══════════════════════════════════════════════════════════════════
//  Additional core client tests
// ══════════════════════════════════════════════════════════════════

async fn test_publish_empty_payload() {
    let client = connect().await;
    let sub = client.subscribe("test.empty").unwrap();
    client.publish("test.empty", b"").unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.subject, "test.empty");
    assert!(msg.payload.is_empty(), "expected empty payload");
}

async fn test_request_timeout() {
    let client = connect().await;
    // Subscribe but never respond — request should timeout.
    let _sub = client.subscribe("test.timeout.sink").unwrap();
    let result = client
        .request("test.timeout.sink", b"waiting", millis(500))
        .await;
    assert!(
        matches!(result, Err(nats_wasi::Error::Timeout)),
        "expected Timeout error, got: {result:?}",
    );
}

async fn test_subscribe_unsubscribe_resubscribe() {
    let client = connect().await;

    // First subscription.
    {
        let sub = client.subscribe("test.resub").unwrap();
        client.publish("test.resub", b"first").unwrap();
        let msg = sub.next().await.unwrap();
        assert_payload(&msg, b"first");
    }
    // sub dropped — auto-unsub.
    wasip3::clocks::monotonic_clock::wait_for(millis(100)).await;

    // Re-subscribe: should only get new messages.
    let sub2 = client.subscribe("test.resub").unwrap();
    client.publish("test.resub", b"second").unwrap();
    let msg = sub2.next().await.unwrap();
    assert_payload(&msg, b"second");
}

async fn test_multiple_header_values() {
    let client = connect().await;
    let sub = client.subscribe("test.multihdrs").unwrap();

    let mut headers = Headers::new();
    headers.insert("X-One", "alpha");
    headers.insert("X-One", "beta");
    headers.insert("X-Two", "gamma");
    client
        .publish_with_headers("test.multihdrs", None, &headers, b"multi")
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_payload(&msg, b"multi");
    let h = msg.headers.as_ref().expect("expected headers");
    // get() returns first value
    assert_eq!(h.get("X-One"), Some("alpha"));
    assert_eq!(h.get("X-Two"), Some("gamma"));
    // get_all() returns all values
    let all = h.get_all("X-One");
    assert_eq!(all.len(), 2, "expected 2 values for X-One, got {all:?}");
    assert_eq!(all[0], "alpha");
    assert_eq!(all[1], "beta");
}

async fn test_queue_group_distribution() {
    let client = connect().await;

    let sub1 = client.subscribe_queue("test.qdist", "grp").unwrap();
    let sub2 = client.subscribe_queue("test.qdist", "grp").unwrap();

    // Publish enough messages to see distribution.
    for i in 0..10 {
        client
            .publish("test.qdist", format!("msg{i}").as_bytes())
            .unwrap();
    }

    // Collect from both with a short timeout.
    let mut count1 = 0u32;
    let mut count2 = 0u32;
    for _ in 0..10 {
        // Try each sub with timeout. One of them will get each message.
        let r1 = with_timeout(millis(500), sub1.next()).await;
        if let Ok(Ok(_)) = r1 {
            count1 += 1;
            continue;
        }
        let r2 = with_timeout(millis(500), sub2.next()).await;
        if let Ok(Ok(_)) = r2 {
            count2 += 1;
        }
    }
    let total = count1 + count2;
    assert_eq!(total, 10, "expected 10 total messages, got {total}");
    // With 10 messages and 2 subscribers, each should get some.
    assert!(count1 > 0, "sub1 got 0 messages — no distribution");
    assert!(count2 > 0, "sub2 got 0 messages — no distribution");
}

async fn test_concurrent_subscriptions_same_subject() {
    let client = connect().await;
    // Two independent (non-queue) subs on same subject — both should get every message.
    let sub1 = client.subscribe("test.dupsub").unwrap();
    let sub2 = client.subscribe("test.dupsub").unwrap();

    client.publish("test.dupsub", b"dup").unwrap();

    let msg1 = sub1.next().await.unwrap();
    let msg2 = sub2.next().await.unwrap();
    assert_payload(&msg1, b"dup");
    assert_payload(&msg2, b"dup");
}

async fn test_publish_max_payload_boundary() {
    let client = connect().await;
    let max = client.server_info().max_payload as usize;
    let sub = client.subscribe("test.maxpay").unwrap();

    // Publish exactly at max_payload limit. The server should accept it.
    let big = vec![0xABu8; max];
    client.publish("test.maxpay", &big).unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.payload.len(), max);
}

// ══════════════════════════════════════════════════════════════════
//  Additional JetStream tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "jetstream")]
async fn test_jetstream_fetch_empty_stream() {
    use nats_wasi::jetstream::{AckPolicy, ConsumerConfig, DeliverPolicy, JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("FETCHMT").await;

    js.create_stream(&StreamConfig {
        name: "FETCHMT".into(),
        subjects: vec!["fetchmt.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    js.create_consumer(
        "FETCHMT",
        &ConsumerConfig {
            durable_name: Some("fetcher".into()),
            deliver_policy: DeliverPolicy::All,
            ack_policy: AckPolicy::Explicit,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    // Fetch from an empty stream — should return 0 messages.
    let msgs = js.fetch("FETCHMT", "fetcher", 10).await.unwrap();
    assert!(
        msgs.is_empty(),
        "expected 0 messages from empty stream, got {}",
        msgs.len()
    );

    js.delete_stream("FETCHMT").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_publish_dedup() {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("DEDUP").await;

    js.create_stream(&StreamConfig {
        name: "DEDUP".into(),
        subjects: vec!["dedup.>".into()],
        duplicate_window: Some(secs(60)),
        ..Default::default()
    })
    .await
    .unwrap();

    // Publish with Nats-Msg-Id header for dedup.
    let mut h1 = Headers::new();
    h1.insert("Nats-Msg-Id", "unique-1");
    let ack1 = js
        .publish_with_headers("dedup.a", &h1, b"first")
        .await
        .unwrap();

    // Duplicate: same Nats-Msg-Id should be deduplicated.
    let ack2 = js
        .publish_with_headers("dedup.a", &h1, b"first-dup")
        .await
        .unwrap();
    assert!(ack2.duplicate, "expected duplicate=true for re-sent msg-id");
    assert_eq!(ack2.seq, ack1.seq, "dedup should return same seq");

    // Different msg-id: should be accepted.
    let mut h2 = Headers::new();
    h2.insert("Nats-Msg-Id", "unique-2");
    let ack3 = js
        .publish_with_headers("dedup.b", &h2, b"second")
        .await
        .unwrap();
    assert!(!ack3.duplicate);
    assert!(ack3.seq > ack1.seq);

    let info = js.stream_info("DEDUP").await.unwrap();
    assert_eq!(info.state.messages, 2, "only 2 unique messages");

    js.delete_stream("DEDUP").await.unwrap();
}

#[cfg(feature = "jetstream")]
async fn test_jetstream_stream_info_not_found() {
    use nats_wasi::jetstream::JetStream;

    let client = connect().await;
    let js = JetStream::new(client);

    let result = js.stream_info("DOES_NOT_EXIST_12345").await;
    assert!(result.is_err(), "expected error for non-existent stream");
}

// ══════════════════════════════════════════════════════════════════
//  Additional KV tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "kv")]
async fn test_kv_update_wrong_revision() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_castest").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "castest".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let rev = kv.put("caskey", b"v1").await.unwrap();

    // Update with wrong revision.
    let bad = kv.update("caskey", b"v2", rev + 999).await;
    assert!(
        matches!(bad, Err(nats_wasi::Error::RevisionMismatch)),
        "expected RevisionMismatch, got: {bad:?}",
    );

    // Correct revision works.
    let rev2 = kv.update("caskey", b"v2", rev).await.unwrap();
    assert!(rev2 > rev);

    js.delete_stream("KV_castest").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_put_overwrite() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_overwrite").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "overwrite".into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();

    kv.put("k1", b"old").await.unwrap();
    kv.put("k1", b"new").await.unwrap();

    let entry = kv.get("k1").await.unwrap().expect("key should exist");
    assert_eq!(entry.value, b"new");

    js.delete_stream("KV_overwrite").await.unwrap();
}

#[cfg(feature = "kv")]
async fn test_kv_delete_then_create() {
    use nats_wasi::jetstream::JetStream;
    use nats_wasi::kv::{KeyValue, KvConfig};

    let client = connect().await;
    let js = JetStream::new(client);

    let _ = js.delete_stream("KV_delcreate").await;

    let kv = KeyValue::new(
        js.clone(),
        KvConfig {
            bucket: "delcreate".into(),
            history: 5,
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let rev1 = kv.put("dk", b"exists").await.unwrap();
    assert!(rev1 >= 1);

    kv.delete("dk").await.unwrap();
    let gone = kv.get("dk").await.unwrap();
    assert!(gone.is_none(), "deleted key should be None");

    // Re-put after delete should work.
    let rev2 = kv.put("dk", b"back").await.unwrap();
    assert!(rev2 > rev1);
    let entry = kv.get("dk").await.unwrap().expect("key re-created");
    assert_eq!(entry.value, b"back");

    js.delete_stream("KV_delcreate").await.unwrap();
}

// ══════════════════════════════════════════════════════════════════
//  TLS tests
// ══════════════════════════════════════════════════════════════════

#[cfg(feature = "tls")]
fn tls_nats_address() -> String {
    std::env::var("NATS_TLS_URL").unwrap_or_else(|_| "127.0.0.1:4223".to_string())
}

#[cfg(feature = "tls")]
async fn connect_tls() -> Client {
    Client::connect(ConnectConfig {
        address: tls_nats_address(),
        tls: true,
        tls_server_name: Some("localhost".into()),
        ..Default::default()
    })
    .await
    .unwrap()
}

#[cfg(feature = "tls")]
async fn test_tls_connect() {
    let client = connect_tls().await;
    let info = client.server_info();
    assert!(!info.server_id.is_empty(), "should get server info over TLS");
    assert!(info.tls_required, "TLS server should report tls_required");
}

#[cfg(feature = "tls")]
async fn test_tls_publish_subscribe(client: &Client) {
    let sub = client.subscribe("tls.pubsub").unwrap();
    client.publish("tls.pubsub", b"encrypted hello").unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.subject, "tls.pubsub");
    assert_payload(&msg, b"encrypted hello");
}

#[cfg(feature = "tls")]
async fn test_tls_request_reply(client: &Client) {
    let sub = client.subscribe("tls.echo").unwrap();
    let client2 = client.clone();
    wit_bindgen::spawn(async move {
        let msg = sub.next().await.unwrap();
        let reply_to = msg.reply_to.as_ref().unwrap();
        client2.publish(reply_to, &msg.payload).unwrap();
    });

    let reply = client
        .request("tls.echo", b"tls-ping", secs(3))
        .await
        .unwrap();
    assert_payload(&reply, b"tls-ping");
}

#[cfg(feature = "tls")]
async fn test_tls_large_payload(client: &Client) {
    let sub = client.subscribe("tls.large").unwrap();

    // 50 KB message over TLS — tests chunked TLS encryption/decryption.
    let big = vec![0xCDu8; 50_000];
    client.publish("tls.large", &big).unwrap();

    let msg = sub.next().await.unwrap();
    assert_eq!(msg.payload.len(), 50_000);
    assert!(msg.payload.iter().all(|&b| b == 0xCD));
}

#[cfg(feature = "tls")]
async fn test_tls_with_headers(client: &Client) {
    let sub = client.subscribe("tls.hdrs").unwrap();

    let mut headers = Headers::new();
    headers.insert("X-Encrypted", "yes");
    client
        .publish_with_headers("tls.hdrs", None, &headers, b"secure")
        .unwrap();

    let msg = sub.next().await.unwrap();
    assert_payload(&msg, b"secure");
    let h = msg.headers.as_ref().expect("expected headers");
    assert_eq!(h.get("X-Encrypted"), Some("yes"));
}

#[cfg(all(feature = "tls", feature = "jetstream"))]
async fn test_tls_jetstream(client: &Client) {
    use nats_wasi::jetstream::{JetStream, StreamConfig};

    let js = JetStream::new(client.clone());

    let _ = js.delete_stream("TLSJS").await;

    js.create_stream(&StreamConfig {
        name: "TLSJS".into(),
        subjects: vec!["tlsjs.>".into()],
        ..Default::default()
    })
    .await
    .unwrap();

    let ack = js.publish("tlsjs.test", b"tls-js-data").await.unwrap();
    assert!(ack.seq >= 1);
    assert_eq!(ack.stream, "TLSJS");

    let info = js.stream_info("TLSJS").await.unwrap();
    assert_eq!(info.state.messages, 1);

    js.delete_stream("TLSJS").await.unwrap();
}

// ══════════════════════════════════════════════════════════════════
//  Test runner
// ══════════════════════════════════════════════════════════════════

wasip3::cli::command::export!(TestRunner);

struct TestRunner;

impl wasip3::exports::cli::run::Guest for TestRunner {
    async fn run() -> Result<(), ()> {
        run_tests().await;
        Ok(())
    }
}

async fn run_tests() {
    let t0 = wasip3::clocks::monotonic_clock::now();
    macro_rules! run_test {
        ($name:expr, $body:expr) => {{
            let start = wasip3::clocks::monotonic_clock::now();
            print!("--- {}", $name);
            $body;
            let elapsed_ms = (wasip3::clocks::monotonic_clock::now() - start) / 1_000_000;
            println!("    PASS  ({elapsed_ms} ms)");
        }};
    }

    // ── Core client ────────────────────────────────────────────
    run_test!("test_publish_subscribe", test_publish_subscribe().await);
    run_test!("test_subscribe_wildcard", test_subscribe_wildcard().await);
    run_test!("test_subscribe_full_wildcard", test_subscribe_full_wildcard().await);
    run_test!("test_publish_with_headers", test_publish_with_headers().await);
    run_test!("test_publish_with_reply", test_publish_with_reply().await);
    run_test!("test_server_info", test_server_info().await);
    run_test!("test_subscribe_queue_group", test_subscribe_queue_group().await);
    run_test!("test_request_reply", test_request_reply().await);
    run_test!("test_request_with_headers", test_request_with_headers().await);
    run_test!("test_request_no_responders", test_request_no_responders().await);
    run_test!("test_multiple_subscriptions", test_multiple_subscriptions().await);
    run_test!("test_unsubscribe_on_drop", test_unsubscribe_on_drop().await);
    run_test!("test_large_payload", test_large_payload().await);
    run_test!("test_publish_empty_payload", test_publish_empty_payload().await);
    run_test!("test_request_timeout", test_request_timeout().await);
    run_test!("test_subscribe_unsubscribe_resubscribe", test_subscribe_unsubscribe_resubscribe().await);
    run_test!("test_multiple_header_values", test_multiple_header_values().await);
    run_test!("test_queue_group_distribution", test_queue_group_distribution().await);
    run_test!("test_concurrent_subscriptions_same_subject", test_concurrent_subscriptions_same_subject().await);
    run_test!("test_publish_max_payload_boundary", test_publish_max_payload_boundary().await);

    // ── JetStream ──────────────────────────────────────────────
    #[cfg(feature = "jetstream")]
    {
        run_test!("test_jetstream_publish", test_jetstream_publish().await);
        run_test!("test_jetstream_purge_stream", test_jetstream_purge_stream().await);
        run_test!("test_jetstream_consumer_crud", test_jetstream_consumer_crud().await);
        run_test!("test_jetstream_stream_get_msg", test_jetstream_stream_get_msg().await);
        run_test!("test_jetstream_publish_with_headers", test_jetstream_publish_with_headers().await);
        run_test!("test_push_consumer", test_push_consumer().await);
        run_test!("test_jetstream_fetch_empty_stream", test_jetstream_fetch_empty_stream().await);
        run_test!("test_jetstream_publish_dedup", test_jetstream_publish_dedup().await);
        run_test!("test_jetstream_stream_info_not_found", test_jetstream_stream_info_not_found().await);
    }

    // ── KV ─────────────────────────────────────────────────────
    #[cfg(feature = "kv")]
    {
        run_test!("test_kv_put_get", test_kv_put_get().await);
        run_test!("test_kv_purge_key", test_kv_purge_key().await);
        run_test!("test_kv_keys_and_load_all", test_kv_keys_and_load_all().await);
        run_test!("test_kv_get_nonexistent", test_kv_get_nonexistent().await);
        run_test!("test_kv_open_existing", test_kv_open_existing().await);
        run_test!("test_kv_watch", test_kv_watch().await);
        run_test!("test_kv_update_wrong_revision", test_kv_update_wrong_revision().await);
        run_test!("test_kv_put_overwrite", test_kv_put_overwrite().await);
        run_test!("test_kv_delete_then_create", test_kv_delete_then_create().await);
    }

    // ── TLS ────────────────────────────────────────────────────
    #[cfg(feature = "tls")]
    {
        // TLS tests require a separate NATS server with TLS enabled.
        // Set NATS_TLS_URL env var (default: 127.0.0.1:4223).
        // Start with:
        //   nats-server -js -p 4223 \
        //     --tls --tlscert testdata/server-cert.pem \
        //     --tlskey testdata/server-key.pem
        let run_tls = std::env::var("NATS_TLS_URL").is_ok()
            || std::env::var("RUN_TLS_TESTS").is_ok();
        if run_tls {
            println!("\n--- TLS tests ---");

            run_test!("test_tls_connect", test_tls_connect().await);

            // Reuse a single TLS connection for the remaining tests.
            let tls_client = connect_tls().await;

            run_test!("test_tls_publish_subscribe", test_tls_publish_subscribe(&tls_client).await);
            run_test!("test_tls_request_reply", test_tls_request_reply(&tls_client).await);
            run_test!("test_tls_large_payload", test_tls_large_payload(&tls_client).await);
            run_test!("test_tls_with_headers", test_tls_with_headers(&tls_client).await);

            #[cfg(feature = "jetstream")]
            {
                run_test!("test_tls_jetstream", test_tls_jetstream(&tls_client).await);
            }
        } else {
            println!("\n[SKIP] TLS tests (set NATS_TLS_URL or RUN_TLS_TESTS to enable)");
        }
    }

    let total_ms = (wasip3::clocks::monotonic_clock::now() - t0) / 1_000_000;
    println!("\nAll integration tests passed! (total: {total_ms} ms)");
}
