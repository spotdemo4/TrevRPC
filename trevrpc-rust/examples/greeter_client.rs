use std::error::Error;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::QuicClientConfig;
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
    let transport =
        trevrpc::quinn::ManagedClient::connect(endpoint.clone(), addr, "localhost").await?;
    let client = greeter::GreeterClient::new(transport.clone());

    let reply = client
        .say_hello(greeter::HelloRequest { name: name.clone() }, call_options())
        .await?;
    println!("unary: {}", reply.message);

    let mut replies = client
        .lots_of_replies(greeter::HelloRequest { name: name.clone() }, call_options())
        .await?;
    while let Some(reply) = replies.next().await {
        println!("server-streaming: {}", reply?.message);
    }

    let mut greetings = client.lots_of_greetings(call_options()).await?;
    for suffix in ["client stream 1", "client stream 2"] {
        greetings
            .send(greeter::HelloRequest {
                name: format!("{name} {suffix}"),
            })
            .await?;
    }
    let summary = greetings.close_and_recv().await?;
    println!("client-streaming: {}", summary.message);

    let mut replies = client.bidi_hello(call_options()).await?;
    for suffix in ["bidi 1", "bidi 2"] {
        replies
            .send(greeter::HelloRequest {
                name: format!("{name} {suffix}"),
            })
            .await?;

        let reply = replies.recv().await?.ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "bidi stream ended early")
        })?;
        println!("bidi: {}", reply.message);
    }
    replies.close_send()?;
    while let Some(reply) = replies.recv().await? {
        println!("bidi: {}", reply.message);
    }

    transport.close();
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

    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    trevrpc::quinn::configure_client_config(
        &mut client_config,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        trevrpc::quinn::TransportMode::Native,
    );
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}
