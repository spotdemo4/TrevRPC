//! Connection flags shared by the `client` and `bench` subcommands.

use std::net::SocketAddr;

use quinn::rustls::pki_types::CertificateDer;
use quinn::rustls::pki_types::pem::PemObject;
use trevrpc::client::ChannelConfig;

use crate::{BoxError, Flags};

/// The TLS server name to verify, `localhost` unless `--server-name` says otherwise.
pub fn server_name(flags: &Flags) -> &str {
    flags.get("server-name").unwrap_or("localhost")
}

/// Resolves the address in the required flag `name` to its first socket address.
pub async fn resolve(flags: &Flags, name: &str) -> Result<SocketAddr, BoxError> {
    tokio::net::lookup_host(flags.require(name)?)
        .await?
        .next()
        .ok_or_else(|| format!("--{name} did not resolve to an address").into())
}

/// TLS settings that trust the certificates in the `--ca` PEM file.
pub fn tls_config(flags: &Flags) -> Result<quinn::rustls::ClientConfig, BoxError> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(flags.require("ca")?)? {
        roots.add(cert?)?;
    }
    Ok(quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

/// A client QUIC endpoint bound to the unspecified address of `remote`'s family.
pub fn client_endpoint(
    remote: SocketAddr,
    config: &ChannelConfig,
    tls: quinn::rustls::ClientConfig,
) -> Result<quinn::Endpoint, BoxError> {
    let bind: SocketAddr = if remote.is_ipv6() {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let mut endpoint = quinn::Endpoint::client(bind)?;
    endpoint.set_default_client_config(config.quic_config(tls)?);
    Ok(endpoint)
}

/// The message types the interop compression dictionary compresses.
const MESSAGE_TYPES: [&str; 2] = [
    "trevrpc.interop.v1.EchoRequest",
    "trevrpc.interop.v1.EchoResponse",
];

/// The compression configured by `--dictionary`: its dictionary for the Echo messages, or none.
pub fn compression(flags: &Flags) -> Result<trevrpc::Compression, BoxError> {
    let mut compression = trevrpc::Compression::new();
    if let Some(path) = flags.get("dictionary") {
        compression = compression.with_dictionary(
            trevrpc::Dictionary::new(std::fs::read(path)?).with_messages(MESSAGE_TYPES),
        );
    }
    Ok(compression)
}
