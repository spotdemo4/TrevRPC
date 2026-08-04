use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use futures_util::StreamExt;
use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

#[path = "shared/cert.rs"]
mod cert;
#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_QUIC_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_BROWSER_EXAMPLE_ORIGIN: &str = "http://127.0.0.1:8080";
const DEFAULT_AUTH_TOKEN: &str = "trevrpc-example-token";

struct GreeterService;

#[trevrpc::async_trait]
impl greeter::Greeter for GreeterService {
    async fn say_hello(
        &self,
        _context: trevrpc::server::RequestContext,
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, trevrpc::Status> {
        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        }))
    }

    async fn lots_of_replies(
        &self,
        _context: trevrpc::server::RequestContext,
        request: greeter::HelloRequest,
    ) -> core::result::Result<
        trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>,
        trevrpc::Status,
    > {
        Ok(trevrpc::ResponseEnvelope::new(trevrpc::stream::from_iter(
            [
                greeter::HelloReply {
                    message: format!("hello, {}", request.name),
                },
                greeter::HelloReply {
                    message: format!("hello again, {}", request.name),
                },
                greeter::HelloReply {
                    message: format!("goodbye, {}", request.name),
                },
            ],
        )))
    }

    async fn lots_of_greetings(
        &self,
        _context: trevrpc::server::RequestContext,
        mut requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, trevrpc::Status> {
        let mut names = Vec::new();

        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        let message = if names.is_empty() {
            "hello, nobody".to_owned()
        } else {
            format!("hello, {}", names.join(", "))
        };

        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message,
        }))
    }

    async fn bidi_hello(
        &self,
        _context: trevrpc::server::RequestContext,
        requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> core::result::Result<
        trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>,
        trevrpc::Status,
    > {
        let replies: trevrpc::BoxStream<greeter::HelloReply> = Box::pin(requests.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("stream hello, {}", request.name),
            })
        }));
        let mut metadata = trevrpc::Metadata::new();
        metadata.insert("x-trevrpc-example".to_owned(), b"complete".to_vec());
        Ok(trevrpc::ResponseEnvelope::new(replies).with_metadata(metadata))
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
    let webtransport_authorities =
        env_list("TREVRPC_EXAMPLE_AUTHORITIES").unwrap_or_else(|| vec![addr.to_string()]);
    let browser_example_origin = env_or("TREVRPC_EXAMPLE_ORIGIN", DEFAULT_BROWSER_EXAMPLE_ORIGIN);
    let auth_token = env_or("TREVRPC_EXAMPLE_TOKEN", DEFAULT_AUTH_TOKEN);

    let mut server = trevrpc::server::Server::new();
    let options = trevrpc::server::ServerOptions::new()
        .with_max_concurrent_connections(Some(512))
        .with_max_concurrent_streams_per_connection(Some(64))
        .with_max_concurrent_requests(Some(1024))
        .with_http3_enabled(true)
        .with_webtransport_allowed_authorities(webtransport_authorities)
        .with_webtransport_allowed_origins([browser_example_origin]);
    server.set_options(options);
    server.set_authorizer(trevrpc::server::MetadataValueAuthorizer::bearer(
        &auth_token,
    ));
    greeter::register_greeter(&mut server, GreeterService);

    let endpoint = make_endpoint(addr, &identity, server.options())?;

    println!(
        "TrevRPC greeter native QUIC server listening on {}",
        endpoint.local_addr()?
    );
    println!(
        "TrevRPC greeter HTTP/3 server listening on https://{}/trevrpc",
        endpoint.local_addr()?
    );
    println!(
        "TrevRPC greeter WebTransport server listening on https://{}/trevrpc",
        endpoint.local_addr()?
    );
    println!("certificate written to {}", certificate_path.display());
    println!("bearer token: {auth_token}");
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
    options: &trevrpc::server::ServerOptions,
) -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = identity.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec(), trevrpc::HTTP3_ALPN.to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(
        &mut server_config,
        options,
        trevrpc::quinn::TransportMode::WebTransport,
    );

    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn env_or(name: &str, fallback: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => fallback.to_owned(),
    }
}

fn env_list(name: &str) -> Option<Vec<String>> {
    let values = std::env::var(name).ok()?;
    let values = values
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    (!values.is_empty()).then_some(values)
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
