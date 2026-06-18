use std::error::Error;
use std::net::{Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use quinn::rustls::pki_types::CertificateDer;

#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const DEFAULT_ADDR: &str = "127.0.0.1:5000";
const DEFAULT_NAME: &str = "TrevRPC";

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
    let transport = trevrpc::quinn::QuinnTransport::new(connection);
    let client = greeter::GreeterClient::new(transport);

    let reply = client.say_hello(greeter::HelloRequest { name }).await?;
    println!("{}", reply.message);

    endpoint.wait_idle().await;

    Ok(())
}

fn make_client_endpoint() -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = std::fs::read(certificate_path())?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(CertificateDer::from(cert_der))?;

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::with_root_certificates(Arc::new(
        roots,
    ))?);

    Ok(endpoint)
}

fn certificate_path() -> PathBuf {
    std::env::var_os("TREVRPC_EXAMPLE_CERT").map_or_else(
        || PathBuf::from("target/trevrpc-example-cert.der"),
        PathBuf::from,
    )
}
