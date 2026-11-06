//! The TCP listener that serves clients that cannot use UDP: `QMux` over TLS for native clients,
//! HTTP/2 for HTTP clients, and `QMux` over WebSocket for browsers, dispatched on the TLS ALPN.
//!
//! HTTP/2 and HTTP/1.1 also carry the requests of the HTTP fallback.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::num::NonZeroU16;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use http::HeaderValue;
use tokio::io::AsyncWriteExt as _;
use tokio::net::TcpStream;
use tokio::sync::{Semaphore, watch};

use crate::proxy_protocol::{self, Rewound};
use crate::qmux_conn::{MAX_RECORD_SIZE, QmuxConnection, server_config};
use crate::server::{Server, canonical_addr};
use crate::{http1, http2, native};

/// Bounds the PROXY header and TLS handshake of a TCP connection, and the `QMux` handshake or
/// HTTP request head that follows them.
pub(crate) const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

const H2_ALPN: &[u8] = b"h2";
const HTTP1_ALPN: &[u8] = b"http/1.1";
/// The TLS application protocol of the ACME TLS-ALPN-01 challenge (RFC 8737), which a
/// certificate authority validates over TCP only.
const ACME_TLS_ALPN: &[u8] = b"acme-tls/1";

/// The TCP side of a server: a bound listener, the TLS configuration from
/// [`Server::tcp_tls_config`], the load balancers trusted to name the client, and the port its
/// HTTP responses advertise for HTTP/3.
pub struct TcpFallback {
    pub listener: tokio::net::TcpListener,
    pub tls: Arc<quinn::rustls::ServerConfig>,
    /// The networks of the TCP load balancers in front of the listener. A connection from one of
    /// them must start with a PROXY protocol header, version 1 or 2, and the client address it
    /// carries becomes the connection's [`crate::server::RequestContext::remote_addr`]; a
    /// connection that does not is closed. Connections from anywhere else are never read for a
    /// header. Empty by default.
    ///
    /// QUIC is not affected: UDP load balancers in front of the endpoint are trusted by
    /// [`QuicEndpoint::server`](crate::udp::QuicEndpoint::server).
    pub trusted_proxies: Vec<IpPrefix>,
    /// The UDP port number that HTTP/2 and HTTP/1.1 responses advertise for HTTP/3 in their
    /// `Alt-Svc` header. `None`, the default, advertises the port of the Quinn endpoint; set it
    /// when clients reach the endpoint on another port, such as through a reverse proxy, load
    /// balancer or NAT.
    pub alt_svc_port: Option<NonZeroU16>,
}

impl TcpFallback {
    /// Serves `listener` with `tls`, trusting no proxies.
    #[must_use]
    pub const fn new(
        listener: tokio::net::TcpListener,
        tls: Arc<quinn::rustls::ServerConfig>,
    ) -> Self {
        Self {
            listener,
            tls,
            trusted_proxies: Vec::new(),
            alt_svc_port: None,
        }
    }

    /// Sets [`TcpFallback::trusted_proxies`].
    #[must_use]
    pub fn with_trusted_proxies(mut self, proxies: impl IntoIterator<Item = IpPrefix>) -> Self {
        self.trusted_proxies = proxies.into_iter().collect();
        self
    }

    /// Sets [`TcpFallback::alt_svc_port`].
    #[must_use]
    pub const fn with_alt_svc_port(mut self, port: Option<NonZeroU16>) -> Self {
        self.alt_svc_port = port;
        self
    }

    /// Whether a connection from `peer` must start with a PROXY header.
    pub(crate) fn trusts(&self, peer: IpAddr) -> bool {
        trusts(&self.trusted_proxies, peer)
    }
}

/// Whether `peer`, IPv4-mapped IPv6 addresses taken as IPv4, belongs to one of `trusted_proxies`.
pub(crate) fn trusts(trusted_proxies: &[IpPrefix], peer: IpAddr) -> bool {
    let peer = canonical_addr(SocketAddr::new(peer, 0)).ip();
    trusted_proxies.iter().any(|prefix| prefix.contains(peer))
}

/// An IP network: an address and the length of its routing prefix, such as `10.0.0.0/8` or
/// `2001:db8::/32`. The addresses the server compares are IPv4 for IPv4 clients, even on a
/// dual-stack socket, so an IPv4 network written as IPv4-mapped IPv6 (`::ffff:10.0.0.0/104`) is
/// kept as IPv4.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct IpPrefix {
    /// The address with every bit past the prefix cleared.
    addr: IpAddr,
    len: u8,
}

impl IpPrefix {
    /// The network of `addr` with a `len`-bit prefix, or `None` when `len` is longer than the
    /// address.
    #[must_use]
    pub fn new(addr: IpAddr, len: u8) -> Option<Self> {
        let (addr, len) = match addr {
            IpAddr::V6(v6) if (96..=128).contains(&len) => match v6.to_ipv4_mapped() {
                Some(v4) => (IpAddr::V4(v4), len.saturating_sub(96)),
                None => (addr, len),
            },
            _ => (addr, len),
        };
        let addr = match addr {
            IpAddr::V4(addr) => IpAddr::V4(Ipv4Addr::from(u32::from(addr) & mask_u32(len)?)),
            IpAddr::V6(addr) => IpAddr::V6(Ipv6Addr::from(u128::from(addr) & mask_u128(len)?)),
        };
        Some(Self { addr, len })
    }

    /// The network's address, with every bit past the prefix cleared.
    #[must_use]
    pub const fn addr(&self) -> IpAddr {
        self.addr
    }

    /// The length of the prefix in bits.
    #[must_use]
    pub const fn prefix_len(&self) -> u8 {
        self.len
    }

    /// Whether `addr` is in the network. An IPv6 network never contains an IPv4 address, nor the
    /// reverse.
    #[must_use]
    pub fn contains(&self, addr: IpAddr) -> bool {
        match (self.addr, addr) {
            (IpAddr::V4(network), IpAddr::V4(addr)) => {
                mask_u32(self.len).is_some_and(|mask| u32::from(addr) & mask == u32::from(network))
            }
            (IpAddr::V6(network), IpAddr::V6(addr)) => mask_u128(self.len)
                .is_some_and(|mask| u128::from(addr) & mask == u128::from(network)),
            _ => false,
        }
    }
}

/// The mask of a `len`-bit IPv4 prefix, or `None` past 32 bits.
fn mask_u32(len: u8) -> Option<u32> {
    match len {
        0 => Some(0),
        1..=32 => u32::MAX.checked_shl(32_u32.saturating_sub(u32::from(len))),
        _ => None,
    }
}

/// The mask of a `len`-bit IPv6 prefix, or `None` past 128 bits.
fn mask_u128(len: u8) -> Option<u128> {
    match len {
        0 => Some(0),
        1..=128 => u128::MAX.checked_shl(128_u32.saturating_sub(u32::from(len))),
        _ => None,
    }
}

impl fmt::Display for IpPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.addr, self.len)
    }
}

/// The error of parsing an [`IpPrefix`] that is not `<address>/<length>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidIpPrefix;

impl fmt::Display for InvalidIpPrefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid IP prefix: expected <address>/<length>")
    }
}

impl std::error::Error for InvalidIpPrefix {}

impl FromStr for IpPrefix {
    type Err = InvalidIpPrefix;

    /// Parses `<address>/<length>`, such as `10.0.0.0/8`. Address bits past the prefix are
    /// cleared.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (addr, len) = text.split_once('/').ok_or(InvalidIpPrefix)?;
        if len.is_empty() || !len.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(InvalidIpPrefix);
        }
        let addr = addr.parse().map_err(|_| InvalidIpPrefix)?;
        let len = len.parse().map_err(|_| InvalidIpPrefix)?;
        Self::new(addr, len).ok_or(InvalidIpPrefix)
    }
}

impl Server {
    /// Builds the TLS configuration for the TCP listener of [`Server::serve_with_tcp`].
    ///
    /// It advertises [`crate::QMUX_ALPN`] for native clients, `h2` when
    /// [`crate::server::ServerOptions::http_enabled`] is set, and `http/1.1` (WebSocket) when
    /// [`crate::server::ServerOptions::webtransport_enabled`] is set, replacing any ALPN list
    /// already on `tls`. An [`crate::server::ServerOptions::http_fallback`] advertises both.
    ///
    /// It keeps `acme-tls/1` when `tls` lists it, so that the certificate resolver of an ACME
    /// client, such as `rustls-acme`'s, can answer TLS-ALPN-01 challenges (RFC 8737). The
    /// certificate authority's connection is closed after the handshake.
    /// [`Server::quic_config`] never offers it, as certificate authorities validate that
    /// challenge over TCP only.
    #[must_use]
    pub fn tcp_tls_config(
        &self,
        mut tls: quinn::rustls::ServerConfig,
    ) -> Arc<quinn::rustls::ServerConfig> {
        let acme = tls
            .alpn_protocols
            .iter()
            .any(|protocol| protocol == ACME_TLS_ALPN);
        tls.alpn_protocols = vec![crate::QMUX_ALPN.to_vec()];
        if self.options().serves_http2() {
            tls.alpn_protocols.push(H2_ALPN.to_vec());
        }
        if self.options().serves_http1() {
            tls.alpn_protocols.push(HTTP1_ALPN.to_vec());
        }
        if acme {
            tls.alpn_protocols.push(ACME_TLS_ALPN.to_vec());
        }
        Arc::new(tls)
    }
}

/// Serves one TCP connection from `peer` on its negotiated ALPN until it ends or the server
/// shuts down. A `proxied` connection, from a trusted proxy, starts with a PROXY header that
/// names the client.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn handle_connection(
    server: Server,
    mut stream: TcpStream,
    peer: SocketAddr,
    proxied: bool,
    tls: Arc<quinn::rustls::ServerConfig>,
    alt_svc: Option<HeaderValue>,
    request_limit: Option<Arc<Semaphore>>,
    shutdown: watch::Receiver<bool>,
) {
    let _ = stream.set_nodelay(true);
    let peer = canonical_addr(peer);
    let acceptor = tokio_rustls::TlsAcceptor::from(tls);
    let handshake = async {
        let (remote_addr, read_ahead) = if proxied {
            match proxy_protocol::read_header(&mut stream).await {
                Ok((source, read_ahead)) => (source.map_or(peer, canonical_addr), read_ahead),
                Err(error) => {
                    tracing::warn!(%error, %peer, "invalid PROXY header from trusted proxy");
                    return None;
                }
            }
        } else {
            (peer, BytesMut::new())
        };
        let stream = acceptor
            .accept(Rewound::new(read_ahead, stream))
            .await
            .ok()?;
        Some((stream, remote_addr))
    };
    let Ok(Some((stream, remote_addr))) = tokio::time::timeout(HANDSHAKE_TIMEOUT, handshake).await
    else {
        return;
    };
    let protocol = stream.get_ref().1.alpn_protocol().map(<[u8]>::to_vec);
    let options = server.options();
    match protocol.as_deref() {
        Some(crate::QMUX_ALPN) => {
            let transport =
                qmux::transport::Stream::new(stream, qmux::Version::QMux02, MAX_RECORD_SIZE);
            let mut config = server_config(options);
            config.handshake_timeout = HANDSHAKE_TIMEOUT;
            let Ok(session) = qmux::Session::accept(transport, config).await else {
                return;
            };
            native::handle_connection(
                server,
                QmuxConnection::new(session, Some(remote_addr)),
                request_limit,
                shutdown,
            )
            .await;
        }
        Some(H2_ALPN) if options.serves_http2() => {
            http2::handle_connection(
                server,
                stream,
                remote_addr,
                alt_svc,
                HANDSHAKE_TIMEOUT,
                request_limit,
                shutdown,
            )
            .await;
        }
        Some(HTTP1_ALPN) | None if options.serves_http1() => {
            http1::handle_connection(
                server,
                stream,
                remote_addr,
                alt_svc,
                request_limit,
                shutdown,
            )
            .await;
        }
        Some(ACME_TLS_ALPN) => {
            // The certificate authority validates the challenge certificate the handshake
            // presented and sends nothing after it.
            let mut stream = stream;
            let _ = tokio::time::timeout(HANDSHAKE_TIMEOUT, stream.shutdown()).await;
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use super::IpPrefix;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn prefixes_parse_mask_and_contain() {
        let prefix: IpPrefix = "10.1.2.3/8".parse().unwrap();
        assert_eq!(prefix.to_string(), "10.0.0.0/8");
        assert!(prefix.contains(ip("10.255.0.1")));
        assert!(!prefix.contains(ip("11.0.0.1")));
        assert!(!prefix.contains(ip("::ffff:10.0.0.1")));

        let prefix: IpPrefix = "2001:db8::/32".parse().unwrap();
        assert!(prefix.contains(ip("2001:db8:1::7")));
        assert!(!prefix.contains(ip("2001:db9::7")));
        assert!(!prefix.contains(ip("10.0.0.1")));

        let host: IpPrefix = "192.0.2.7/32".parse().unwrap();
        assert!(host.contains(ip("192.0.2.7")));
        assert!(!host.contains(ip("192.0.2.8")));
        let all: IpPrefix = "::/0".parse().unwrap();
        assert!(all.contains(ip("::1")));
        assert!(
            "0.0.0.0/0"
                .parse::<IpPrefix>()
                .unwrap()
                .contains(ip("192.0.2.7"))
        );

        // An IPv4 network written as IPv4-mapped IPv6 matches the IPv4 addresses clients have.
        let mapped: IpPrefix = "::ffff:192.168.0.0/112".parse().unwrap();
        assert_eq!(mapped.to_string(), "192.168.0.0/16");
        assert!(mapped.contains(ip("192.168.1.1")));
    }

    #[test]
    fn invalid_prefixes_are_rejected() {
        for text in [
            "10.0.0.0",
            "10.0.0.0/33",
            "::/129",
            "10.0.0.0/",
            "10.0.0.0/+8",
            "x/8",
        ] {
            assert!(text.parse::<IpPrefix>().is_err(), "{text}");
        }
    }
}
