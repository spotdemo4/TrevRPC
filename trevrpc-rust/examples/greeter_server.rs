use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_ADDR: &str = "127.0.0.1:5000";
const AUTH_TOKEN: &str = "local-example-token";

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
    let addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| DEFAULT_ADDR.to_owned())
        .parse::<SocketAddr>()?;
    let (endpoint, cert_der) = make_server_endpoint(addr)?;
    write_certificate(&cert_der)?;

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
        "TrevRPC greeter server listening on {}",
        endpoint.local_addr()?
    );
    println!("certificate written to {}", certificate_path().display());
    println!("press Ctrl+C to shut down");

    server
        .serve_quinn_with_shutdown(endpoint, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;

    Ok(())
}

fn make_server_endpoint(
    addr: SocketAddr,
) -> Result<(quinn::Endpoint, CertificateDer<'static>), Box<dyn Error + Send + Sync>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport)
        .expect("server config should have one transport reference");
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((quinn::Endpoint::server(server_config, addr)?, cert_der))
}

fn write_certificate(cert_der: &CertificateDer<'_>) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = certificate_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, cert_der.as_ref())?;
    Ok(())
}

fn certificate_path() -> PathBuf {
    std::env::var_os("TREVRPC_EXAMPLE_CERT").map_or_else(
        || PathBuf::from("target/trevrpc-example-cert.der"),
        PathBuf::from,
    )
}
