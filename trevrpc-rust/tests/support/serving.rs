//! A server on loopback ports for a test: a self-signed certificate for `localhost`, the client
//! TLS configuration that trusts it, and a server that serves native QUIC and the TCP fallback
//! until it is stopped.
//!
//! The benchmarks include this file by path, so it uses nothing else of `tests/support`.

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::server::Server;
use trevrpc::tcp::{IpPrefix, TcpFallback};
use trevrpc::udp::QuicEndpoint;

type BoxError = Box<dyn Error + Send + Sync>;

/// The loopback address, on a port the system picks.
const LOOPBACK: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

/// A self-signed certificate for `localhost`, and its private key.
pub fn self_signed_key() -> Result<(CertificateDer<'static>, PrivateKeyDer<'static>), BoxError> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    Ok((CertificateDer::from(cert.cert), key))
}

/// A self-signed certificate for `localhost`, and the TLS configuration of a server that presents
/// it without asking clients for one.
pub fn self_signed() -> Result<(CertificateDer<'static>, ServerConfig), BoxError> {
    let (cert, key) = self_signed_key()?;
    let tls = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)?;
    Ok((cert, tls))
}

/// A client TLS configuration that trusts only `cert` and offers the protocols of `alpn`.
///
/// The library's clients, through `ChannelConfig::quic_config` and `QmuxTransport::connect`, set
/// their own protocol, so `alpn` only matters to the TLS of a raw client.
pub fn client_tls(
    cert: &CertificateDer<'static>,
    alpn: &[&[u8]],
) -> Result<ClientConfig, BoxError> {
    let mut roots = RootCertStore::empty();
    roots.add(cert.clone())?;
    let mut tls = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
    Ok(tls)
}

/// A server running on loopback ports until [`Serving::stop`].
pub struct Serving {
    /// The address of the native QUIC endpoint.
    pub udp: SocketAddr,
    /// The address of the TCP fallback.
    pub tcp: SocketAddr,
    /// The certificate the server presents.
    pub cert: CertificateDer<'static>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<trevrpc::Result<()>>,
}

impl Serving {
    /// Shuts the server down and waits at most `timeout` for it to finish, failing with its error.
    pub async fn stop(self, timeout: Duration) -> Result<(), BoxError> {
        let _ = self.shutdown.send(());
        tokio::time::timeout(timeout, self.task)
            .await
            .map_err(|_| {
                format!("timed out after {timeout:?} waiting for the server to stop")
            })???;
        Ok(())
    }
}

/// Serves `server` on loopback ports, over native QUIC and its TCP fallback, both presenting the
/// TLS configuration of `(cert, tls)`. Clients trust it through `cert`.
pub async fn serve(
    server: Server,
    tls: (CertificateDer<'static>, ServerConfig),
) -> Result<Serving, BoxError> {
    serve_with(server, tls, Vec::new(), |tcp| tcp).await
}

/// [`serve`], with the native QUIC endpoint behind the UDP proxies of `trusted_udp_proxies`, and
/// the TCP fallback as `configure_tcp` adjusts it.
pub async fn serve_with(
    server: Server,
    (cert, tls): (CertificateDer<'static>, ServerConfig),
    trusted_udp_proxies: Vec<IpPrefix>,
    configure_tcp: impl FnOnce(TcpFallback) -> TcpFallback,
) -> Result<Serving, BoxError> {
    let endpoint = QuicEndpoint::server(
        server.quic_config(tls.clone())?,
        LOOPBACK,
        trusted_udp_proxies,
    )?;
    let listener = TcpListener::bind(LOOPBACK).await?;
    let udp = endpoint.endpoint().local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = configure_tcp(TcpFallback::new(listener, server.tcp_tls_config(tls)));
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));
    Ok(Serving {
        udp,
        tcp,
        cert,
        shutdown,
        task,
    })
}
