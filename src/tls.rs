//! TLS support via the `wasi:tls` host interface (P3).
//!
//! The host runtime performs all TLS encryption/decryption using native
//! code with hardware-accelerated crypto. The guest only sees cleartext
//! `StreamReader<u8>` / `StreamWriter<u8>` pairs — no in-wasm crypto.

use wit_bindgen::{StreamReader, StreamWriter};

use crate::Error;

// Generate guest-side bindings for wasi:tls@0.3.0-draft
wit_bindgen::generate!({
    path: "wit",
    world: "nats-tls",
    generate_all,
});

use wasi::tls::client::Connector;

/// Perform a TLS handshake over raw P3 TCP streams using the host's
/// `wasi:tls` implementation. Returns cleartext stream halves.
///
/// The returned `StreamReader<u8>` / `StreamWriter<u8>` carry decrypted
/// application data; all encryption is handled transparently by the host.
pub async fn tls_upgrade(
    tcp_rx: StreamReader<u8>,
    tcp_tx: StreamWriter<u8>,
    server_name: &str,
) -> Result<(StreamReader<u8>, StreamWriter<u8>), Error> {
    let connector = Connector::new();

    // Create a cleartext stream pair: we write into cleartext_tx,
    // the connector reads from cleartext_rx, encrypts it.
    let (cleartext_tx, cleartext_rx) = wasip3::wit_stream::new::<u8>();

    // Set up encryption: cleartext → host encrypts → ciphertext.
    let (ciphertext_to_net, send_error_future) = connector.send(cleartext_rx);

    // Set up decryption: network ciphertext → host decrypts → cleartext.
    let (cleartext_from_net, recv_error_future) = connector.receive(tcp_rx);

    // Forward encrypted output to the TCP socket in the background.
    wit_bindgen::spawn(forward_stream(ciphertext_to_net, tcp_tx));

    // Perform the TLS handshake.
    Connector::connect(connector, server_name.to_string())
        .await
        .map_err(|e| Error::Tls(e.to_debug_string()))?;

    // Monitor error futures so they don't leak.
    wit_bindgen::spawn(async move {
        let _ = send_error_future.await;
    });
    wit_bindgen::spawn(async move {
        let _ = recv_error_future.await;
    });

    Ok((cleartext_from_net, cleartext_tx))
}

/// Forward all data from a StreamReader to a StreamWriter.
async fn forward_stream(mut rx: StreamReader<u8>, mut tx: StreamWriter<u8>) {
    loop {
        let buf = Vec::with_capacity(16384);
        let (status, data) = rx.read(buf).await;
        match status {
            wit_bindgen::StreamResult::Complete(n) if n > 0 => {
                let remaining = tx.write_all(data[..n].to_vec()).await;
                if !remaining.is_empty() {
                    break;
                }
            }
            _ => break,
        }
    }
}
