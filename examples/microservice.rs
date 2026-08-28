//! NATS Microservice (ADR-32) example.
//!
//! Demonstrates creating an ADR-32 compliant microservice with discovery
//! ($SRV.PING, $SRV.INFO, $SRV.STATS), endpoint routing, and error handling.
//!
//! ```sh
//! cargo build --example microservice --target wasm32-wasip3
//! ```

use nats_wasip3::{Client, ConnectConfig, EndpointConfig, Service, ServiceConfig};

wasip3::cli::command::export!(Microservice);

struct Microservice;

impl wasip3::exports::cli::run::Guest for Microservice {
    async fn run() -> Result<(), ()> {
        run().await.unwrap();
        Ok(())
    }
}

async fn run() -> Result<(), nats_wasip3::Error> {
    let client = Client::connect(ConnectConfig::default()).await?;
    println!("connected to {}", client.server_info().server_name);

    // 1. Create service
    let config = ServiceConfig::new("math-service", "1.0.0")
        .description("Performs basic arithmetic operations")
        .metadata("environment", "production");

    let service = Service::add(client.clone(), config).await?;
    println!(
        "started service '{}' with id '{}'",
        service.name(),
        service.id()
    );

    // 2. Add an endpoint to the service
    let echo_ep = service
        .add_endpoint(EndpointConfig::new("echo").subject("math.echo"))
        .await?;

    // 3. Add a group with nested endpoints
    let v1 = service.group("math.v1");
    let add_ep = v1.add_endpoint(EndpointConfig::new("add")).await?;

    // Handle requests on echo endpoint in background
    let echo_handle = wit_bindgen::spawn(async move {
        while let Ok(req) = echo_ep.next().await {
            let _ = req.respond(req.payload());
        }
    });

    // Handle requests on add endpoint in background
    let add_handle = wit_bindgen::spawn(async move {
        while let Ok(req) = add_ep.next().await {
            let input = String::from_utf8_lossy(req.payload());
            let nums: Vec<i64> = input
                .split(',')
                .filter_map(|s| s.trim().parse::<i64>().ok())
                .collect();

            if nums.len() < 2 {
                let _ = req.respond_error(400, "expected comma-separated numbers (e.g. '3,4')");
                continue;
            }

            let sum: i64 = nums.iter().sum();
            let _ = req.respond(sum.to_string().as_bytes());
        }
    });

    // 4. Test calling the microservice
    let reply = client
        .request("math.v1.add", b"10,25", nats_wasip3::secs(5))
        .await?;
    println!("10 + 25 = {}", String::from_utf8_lossy(&reply.payload));

    // 5. Test ADR-32 discovery
    let ping = client
        .request("$SRV.PING.math-service", b"", nats_wasip3::secs(5))
        .await?;
    println!("PING response: {}", String::from_utf8_lossy(&ping.payload));

    let stats = client
        .request("$SRV.STATS.math-service", b"", nats_wasip3::secs(5))
        .await?;
    println!("STATS response: {}", String::from_utf8_lossy(&stats.payload));

    let _ = echo_handle;
    let _ = add_handle;
    Ok(())
}
