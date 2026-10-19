use std::error::Error;
use std::net::{Ipv6Addr, SocketAddr};
use std::time::Duration;

use futures_util::StreamExt;
use quinn::rustls::pki_types::CertificateDer;
use quinn::rustls::pki_types::pem::PemObject;

#[path = "shared/cert.rs"]
mod cert;
#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_NAME: &str = "TrevRPC";
const AUTH_TOKEN: &str = "trevrpc-example-token";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_else(|| DEFAULT_NAME.to_owned());
    let addr = args
        .next()
        .unwrap_or_else(|| DEFAULT_ADDR.to_owned())
        .parse::<SocketAddr>()?;

    let endpoint = make_client_endpoint()?;
    let channel = trevrpc::client::Channel::connect(endpoint.clone(), addr, "localhost").await?;
    let client = greeter::GreeterClient::new(channel.clone());

    let reply = client
        .say_hello(&greeter::HelloRequest { name: name.clone() })
        .options(|_| call_options())
        .await?;
    println!("unary: {}", reply.message);

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest { name: name.clone() })
        .options(|_| call_options())
        .await?;
    while let Some(reply) = replies.next().await {
        println!("server-streaming: {}", reply?.message);
    }

    let greetings = client
        .lots_of_greetings()
        .options(|_| call_options())
        .await?;
    for suffix in ["client stream 1", "client stream 2"] {
        greetings
            .send(greeter::HelloRequest {
                name: format!("{name} {suffix}"),
            })
            .await?;
    }
    let summary = greetings.close_and_recv().await?;
    println!("client-streaming: {}", summary.message);

    let call = client.bidi_hello().options(|_| call_options()).await?;
    let (sender, mut replies) = call.split();
    let send_name = name.clone();
    let send_task = tokio::spawn(async move {
        for suffix in ["bidi 1", "bidi 2"] {
            sender
                .send(greeter::HelloRequest {
                    name: format!("{send_name} {suffix}"),
                })
                .await?;
        }
        sender.finish()
    });
    let receive_task = tokio::spawn(async move {
        while let Some(reply) = replies.next().await {
            println!("bidi: {}", reply?.message);
        }
        Ok::<_, trevrpc::Error>(replies.terminal_metadata().cloned().unwrap_or_default())
    });

    send_task.await??;
    let terminal_metadata = receive_task.await??;
    println!(
        "bidi terminal metadata entries: {}",
        terminal_metadata.len()
    );

    channel.close();
    endpoint.wait_idle().await;

    Ok(())
}

fn call_options() -> trevrpc::client::CallOptions {
    trevrpc::client::CallOptions::new()
        .with_timeout(Duration::from_secs(5))
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

fn make_client_endpoint() -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_pem = std::fs::read(cert::certificate_path()?)?;
    let cert_der = CertificateDer::from_pem_slice(&cert_pem)?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;

    let client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    let client_config = trevrpc::client::ChannelConfig::new().quic_config(client_crypto)?;
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}
