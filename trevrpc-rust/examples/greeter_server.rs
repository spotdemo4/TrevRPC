use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

#[path = "shared/cert.rs"]
mod cert;
#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_QUIC_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_WEBTRANSPORT_ADDR: &str = "127.0.0.1:5001";
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
    let quic_addr = args
        .next()
        .unwrap_or_else(|| DEFAULT_QUIC_ADDR.to_owned())
        .parse::<SocketAddr>()?;
    let webtransport_addr = args
        .next()
        .unwrap_or_else(|| DEFAULT_WEBTRANSPORT_ADDR.to_owned())
        .parse::<SocketAddr>()?;

    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"])?;
    let certificate_path = cert::certificate_path()?;
    write_certificate(&identity, &certificate_path)?;

    let quic_endpoint = make_quic_endpoint(quic_addr, &identity)?;
    let webtransport_endpoint =
        make_webtransport_endpoint(webtransport_addr, identity.clone_identity())?;

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
        quic_endpoint.local_addr()?
    );
    println!(
        "TrevRPC greeter WebTransport server listening on https://{}/trevrpc",
        webtransport_endpoint.local_addr()?
    );
    println!("certificate written to {}", certificate_path.display());
    println!("bearer token: {AUTH_TOKEN}");
    println!("press Ctrl+C to shut down");

    tokio::try_join!(
        server
            .clone()
            .serve_quinn_with_shutdown(quic_endpoint, shutdown_signal()),
        server.serve_webtransport_with_shutdown(webtransport_endpoint, shutdown_signal()),
    )?;

    Ok(())
}

fn make_quic_endpoint(
    addr: SocketAddr,
    identity: &wtransport::Identity,
) -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = CertificateDer::from(identity.certificate_chain().as_slice()[0].der().to_vec());
    let key_der = PrivatePkcs8KeyDer::from(identity.private_key().secret_der().to_vec());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport)
        .expect("server config should have one transport reference");
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn make_webtransport_endpoint(
    addr: SocketAddr,
    identity: wtransport::Identity,
) -> Result<
    wtransport::Endpoint<wtransport::endpoint::endpoint_side::Server>,
    Box<dyn Error + Send + Sync>,
> {
    let config = wtransport::ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build();

    Ok(wtransport::Endpoint::server(config)?)
}

fn write_certificate(
    identity: &wtransport::Identity,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let certificate = identity.certificate_chain().as_slice()[0].to_pem();
    std::fs::write(path, certificate)?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
