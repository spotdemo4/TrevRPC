use std::error::Error;
use std::net::{Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::pki_types::CertificateDer;

#[path = "shared/cert.rs"]
mod cert;
#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_NAME: &str = "TrevRPC";
const AUTH_TOKEN: &str = "local-example-token";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_else(|| DEFAULT_NAME.to_owned());
    let addr = args
        .next()
        .unwrap_or_else(|| DEFAULT_ADDR.to_owned())
        .parse::<SocketAddr>()?;

    let endpoint = make_client_endpoint()?;
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let transport = trevrpc::quinn::QuinnTransport::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

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

    let summary = client
        .lots_of_greetings(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: format!("{name} client stream 1"),
                },
                greeter::HelloRequest {
                    name: format!("{name} client stream 2"),
                },
            ]),
            call_options(),
        )
        .await?;
    println!("client-streaming: {}", summary.message);

    let mut replies = client
        .bidi_hello(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: format!("{name} bidi 1"),
                },
                greeter::HelloRequest {
                    name: format!("{name} bidi 2"),
                },
            ]),
            call_options(),
        )
        .await?;
    while let Some(reply) = replies.next().await {
        println!("bidi: {}", reply?.message);
    }

    connection.close(0_u32.into(), b"client done");
    endpoint.wait_idle().await;

    Ok(())
}

fn call_options() -> trevrpc::client::CallOptions {
    trevrpc::client::CallOptions::new()
        .with_timeout(Duration::from_secs(5))
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

fn make_client_endpoint() -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = std::fs::read(cert::certificate_path()?)?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(cert_der))?;

    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client_crypto)?,
    )));

    Ok(endpoint)
}
