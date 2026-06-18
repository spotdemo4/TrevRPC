use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

#[path = "shared/cert.rs"]
mod cert;
#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_QUIC_ADDR: &str = "127.0.0.1:5000";
const AUTH_TOKEN: &str = "trevrpc-example-token";

struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
    }

    async fn lots_of_replies(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(trevrpc::stream::from_iter([
            greeter::HelloReply {
                message: format!("hello, {}", request.name),
            },
            greeter::HelloReply {
                message: format!("hello again, {}", request.name),
            },
            greeter::HelloReply {
                message: format!("goodbye, {}", request.name),
            },
        ]))
    }

    async fn lots_of_greetings(
        &self,
        mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        let mut names = Vec::new();

        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        let message = if names.is_empty() {
            "hello, nobody".to_owned()
        } else {
            format!("hello, {}", names.join(", "))
        };

        Ok(greeter::HelloReply { message })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(Box::new(EchoReplies { requests }))
    }
}

struct EchoReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<greeter::HelloReply> for EchoReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("stream hello, {}", request.name),
            })
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .unwrap_or_else(|| DEFAULT_QUIC_ADDR.to_owned())
        .parse::<SocketAddr>()?;

    let identity = make_identity()?;
    let certificate_path = cert::certificate_path()?;
    write_certificate(&identity, &certificate_path)?;

    let endpoint = make_endpoint(addr, &identity)?;

    let mut server = trevrpc::server::Server::new();
    server.set_options(
        trevrpc::server::ServerOptions::new()
            .with_max_concurrent_connections(Some(512))
            .with_max_concurrent_streams_per_connection(Some(64))
            .with_max_concurrent_requests(Some(1024)),
    );
    server.set_authorizer(trevrpc::server::MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    greeter::register_greeter(&mut server, GreeterService);

    println!(
        "TrevRPC greeter native QUIC server listening on {}",
        endpoint.local_addr()?
    );
    println!(
        "TrevRPC greeter WebTransport server listening on https://{}/trevrpc",
        endpoint.local_addr()?
    );
    println!("certificate written to {}", certificate_path.display());
    println!("bearer token: {AUTH_TOKEN}");
    println!("press Ctrl+C to shut down");

    server
        .serve_quinn_and_webtransport_with_shutdown(endpoint, shutdown_signal())
        .await?;

    Ok(())
}

fn make_identity() -> Result<rcgen::CertifiedKey<rcgen::KeyPair>, Box<dyn Error + Send + Sync>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut params =
        rcgen::CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    params.not_before = not_before;
    params.not_after = not_before + time::Duration::hours(25);
    let cert = params.self_signed(&signing_key)?;

    Ok(rcgen::CertifiedKey { cert, signing_key })
}

fn make_endpoint(
    addr: SocketAddr,
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
) -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = identity.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![
        trevrpc::ALPN.to_vec(),
        web_transport_quinn::ALPN.as_bytes().to_vec(),
    ];

    let server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));

    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn write_certificate(
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, identity.cert.pem())?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
