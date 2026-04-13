//! Basic pub/sub example.
//!
//! Requires a NATS server running on localhost:4222.
//!
//! ```sh
//! cargo build --example pubsub --target wasm32-wasip3
//! ```

use nats_wasi::client::{Client, ConnectConfig};

wasip3::cli::command::export!(PubSub);

struct PubSub;

impl wasip3::exports::cli::run::Guest for PubSub {
    async fn run() -> Result<(), ()> {
        run().await.unwrap();
        Ok(())
    }
}

async fn run() -> Result<(), nats_wasi::Error> {
    let client = Client::connect(ConnectConfig::default()).await?;
    println!("connected to {}", client.server_info().server_name);

    // Subscribe to a wildcard subject.
    let sub = client.subscribe("demo.>")?;

    // Publish a few messages.
    client.publish("demo.hello", b"world")?;
    client.publish("demo.goodbye", b"see ya")?;

    // Receive them.
    for _ in 0..2 {
        let msg = sub.next().await?;
        println!(
            "[{}] {}",
            msg.subject,
            String::from_utf8_lossy(&msg.payload),
        );
    }

    Ok(())
}
