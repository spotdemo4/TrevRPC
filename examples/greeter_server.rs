use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use quinn::rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};

#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_ADDR: &str = "127.0.0.1:5000";

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
    greeter::register_greeter(&mut server, GreeterService);

    println!(
        "TrevRPC greeter server listening on {}",
        endpoint.local_addr()?
    );
    println!("certificate written to {}", certificate_path().display());

    server.serve_quinn(endpoint).await?;

    Ok(())
}

fn make_server_endpoint(
    addr: SocketAddr,
) -> Result<(quinn::Endpoint, CertificateDer<'static>), Box<dyn Error + Send + Sync>> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_config = quinn::ServerConfig::with_single_cert(
        vec![cert_der.clone()],
        quinn::rustls::pki_types::PrivateKeyDer::from(key_der),
    )?;
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
