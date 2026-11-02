//! Connecting a [`Channel`] to a URL, with name resolution, endpoints and TLS handled.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use quinn::rustls;
use quinn::rustls::pki_types::CertificateDer;

use super::channel::{Channel, ChannelConfig};
use super::resolve::{HostTarget, Route, Target};
use crate::{Error, Result, Status};

const DEFAULT_PORT: u16 = 443;

/// Connects a [`Channel`] to a URL such as `https://example.com:8443`; see [`Channel::builder`].
#[must_use = "a builder does nothing until it connects"]
pub struct ChannelBuilder {
    host: String,
    port: u16,
    server_name: String,
    config: ChannelConfig,
    roots: Option<rustls::RootCertStore>,
    extra_roots: Vec<CertificateDer<'static>>,
    tls: Option<rustls::ClientConfig>,
    tcp_fallback: bool,
    connect_timeout: Option<Duration>,
}

impl Channel {
    /// Starts connecting a channel to `url`: `https://host`, `https://host:port` or `host:port`,
    /// where the host is a name or an IP address (in brackets for IPv6) and the port defaults to
    /// 443.
    ///
    /// The channel resolves the host before every dial, tries each of its addresses Happy Eyeballs
    /// style (RFC 8305), alternating address families and starting the next attempt 250
    /// milliseconds after the one before, and binds a Quinn endpoint for each address family it
    /// dials. It verifies the server with the [`webpki-roots`](https://docs.rs/webpki-roots) roots
    /// (the `webpki-roots` feature, on by default), or those of [`ChannelBuilder::roots`], and
    /// falls back to `QMux` over TLS at the same host and port when QUIC does not connect (see
    /// [`ChannelBuilder::tcp_fallback`]).
    ///
    /// Use [`Channel::connect_with_config`] instead for full control over the endpoint and the
    /// address.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` when `url` is not one of those forms.
    pub fn builder(url: &str) -> Result<ChannelBuilder> {
        let (host, port) = parse_url(url)?;
        Ok(ChannelBuilder {
            server_name: host.clone(),
            host,
            port,
            config: ChannelConfig::new(),
            roots: None,
            extra_roots: Vec::new(),
            tls: None,
            tcp_fallback: true,
            connect_timeout: None,
        })
    }
}

impl ChannelBuilder {
    /// Replaces the channel's configuration. The builder's own settings apply on top of it.
    pub fn config(mut self, config: ChannelConfig) -> Self {
        self.config = config;
        self
    }

    /// Replaces the default roots that verify the server.
    pub fn roots(mut self, roots: rustls::RootCertStore) -> Self {
        self.roots = Some(roots);
        self
    }

    /// Trusts `certificates` as roots, in addition to the default roots or those of
    /// [`Self::roots`]. For PEM, see [`PemObject::pem_slice_iter`].
    ///
    /// [`PemObject::pem_slice_iter`]: quinn::rustls::pki_types::pem::PemObject::pem_slice_iter
    pub fn add_root_certificates(
        mut self,
        certificates: impl IntoIterator<Item = CertificateDer<'static>>,
    ) -> Self {
        self.extra_roots.extend(certificates);
        self
    }

    /// Replaces the TLS configuration, for example to present a client certificate; the roots of
    /// [`Self::roots`] and [`Self::add_root_certificates`] are then not used. Its ALPN list is
    /// replaced for each transport.
    pub fn tls(mut self, tls: rustls::ClientConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Sets the name the server's certificate is verified against. It defaults to the URL's host.
    pub fn server_name(mut self, server_name: impl Into<String>) -> Self {
        self.server_name = server_name.into();
        self
    }

    /// Sets whether the channel falls back to `QMux` over TLS, with the same TLS configuration, at
    /// the URL's host and port, or at [`ChannelConfig::with_tcp_addr`]. The default is `true`.
    /// A TCP fallback of the channel's configuration ([`ChannelConfig::with_tcp_fallback`]) takes
    /// precedence either way: it is used with its own TLS configuration even when this is `false`.
    pub const fn tcp_fallback(mut self, enabled: bool) -> Self {
        self.tcp_fallback = enabled;
        self
    }

    /// Bounds each connection attempt; see [`ChannelConfig::with_connect_timeout`].
    pub const fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Connects the channel and starts its reconnect supervisor.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` when there are no roots, `Unavailable` when no address of the host
    /// connects, and the errors of [`Channel::connect_with_config`].
    pub async fn connect(self) -> Result<Channel> {
        let tls = match self.tls {
            Some(tls) => tls,
            None => client_tls(self.roots, self.extra_roots)?,
        };
        let mut config = self.config;
        if self.tcp_fallback {
            config = config.with_default_tcp_fallback(tls.clone());
        }
        if let Some(timeout) = self.connect_timeout {
            config = config.with_connect_timeout(timeout);
        }
        let quic = config.quic_config(tls)?;
        let route = Route {
            addr: SocketAddr::from((Ipv4Addr::UNSPECIFIED, self.port)),
            endpoint: None,
        };
        let target = Target::Host(Arc::new(HostTarget::new(self.host, self.port, quic)));
        Channel::connect_to(target, route, self.server_name, config).await
    }
}

/// The TLS configuration that verifies the server with `roots`, or the default roots, and
/// `extra` roots.
fn client_tls(
    roots: Option<rustls::RootCertStore>,
    extra: Vec<CertificateDer<'static>>,
) -> Result<rustls::ClientConfig> {
    let mut roots = roots.unwrap_or_else(default_roots);
    for certificate in extra {
        roots
            .add(certificate)
            .map_err(|error| invalid_argument(format!("invalid root certificate: {error}")))?;
    }
    if roots.is_empty() {
        return Err(invalid_argument(
            "no root certificates: enable the webpki-roots feature or set ChannelBuilder::roots",
        ));
    }
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(Error::transport)?
    .with_root_certificates(roots)
    .with_no_client_auth())
}

#[cfg(feature = "webpki-roots")]
fn default_roots() -> rustls::RootCertStore {
    rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    }
}

#[cfg(not(feature = "webpki-roots"))]
fn default_roots() -> rustls::RootCertStore {
    rustls::RootCertStore::empty()
}

/// The host, without brackets, and port of a channel URL.
fn parse_url(url: &str) -> Result<(String, u16)> {
    let invalid = || {
        invalid_argument(format!(
            "channel URL {url:?} must be https://host[:port] or host:port"
        ))
    };
    let uri = url.parse::<http::Uri>().map_err(|_| invalid())?;
    let authority = uri.authority().ok_or_else(invalid)?;
    let default_port = match uri.scheme_str() {
        Some("https") => Some(DEFAULT_PORT),
        // Without a scheme, only the authority form names a host.
        None => None,
        Some(_) => return Err(invalid()),
    };
    if authority.as_str().contains('@')
        || uri.query().is_some()
        || !matches!(uri.path(), "" | "/")
        || authority.host().is_empty()
    {
        return Err(invalid());
    }
    let host = authority.host();
    // http::Uri leaves out a port that is not a u16 rather than rejecting it.
    let port = match authority.as_str().strip_prefix(host) {
        Some("") => default_port.ok_or_else(invalid)?,
        Some(port) => port
            .strip_prefix(':')
            .filter(|port| port.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|port| port.parse::<u16>().ok())
            .filter(|&port| port != 0)
            .ok_or_else(invalid)?,
        None => return Err(invalid()),
    };
    let host = host
        .strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host);
    Ok((host.to_owned(), port))
}

fn invalid_argument(message: impl Into<String>) -> Error {
    Error::from(Status::invalid_argument(message))
}

#[cfg(test)]
mod tests {
    use super::parse_url;

    #[test]
    fn urls_name_a_host_and_port() {
        for (url, host, port) in [
            ("https://example.com:8443", "example.com", 8443),
            ("https://example.com", "example.com", 443),
            ("https://example.com/", "example.com", 443),
            ("example.com:8443", "example.com", 8443),
            ("https://127.0.0.1:5000", "127.0.0.1", 5000),
            ("https://[::1]:5000", "::1", 5000),
            ("[::1]:5000", "::1", 5000),
        ] {
            assert_eq!(parse_url(url).ok(), Some((host.to_owned(), port)), "{url}");
        }
    }

    #[test]
    fn other_urls_are_invalid() {
        for url in [
            "",
            "example.com",
            "http://example.com",
            "https://",
            "https://example.com:0",
            "https://example.com:port",
            "https://example.com:",
            "https://example.com:65536",
            "example.com:+5",
            "https://user@example.com",
            "https://example.com/path",
            "https://example.com?query",
            "/path",
        ] {
            let error = parse_url(url).err().map(trevrpc_code);
            assert_eq!(error, Some(crate::Code::InvalidArgument), "{url}");
        }
    }

    fn trevrpc_code(error: crate::Error) -> crate::Code {
        error.into_status().code()
    }
}
