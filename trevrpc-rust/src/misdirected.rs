//! Answers 421 Misdirected Request (RFC 9110 §15.5.20) to HTTP requests for another name than
//! their connection's TLS server name.
//!
//! Browsers send such requests over a connection whose certificate also covers the name they want
//! (connection coalescing, RFC 9113 §9.1.1 and RFC 9114 §3.3); a 421 makes them retry on a
//! connection of its own, which a proxy that routes by server name sends to the right server.

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use http::Request;
use http::header::HOST;

use crate::http_admission::authority_host;
use crate::server::ServerOptions;

/// The server name a connection's HTTP requests must name, or none when they are not checked.
#[derive(Clone, Debug, Default)]
pub struct HostCheck(Option<Arc<str>>);

impl HostCheck {
    /// Checks the requests of a connection whose client sent `server_name`, unless `options`
    /// disable the check. A server name that is an IP address, which RFC 6066 forbids but some
    /// clients send, counts as none.
    pub fn new(options: &ServerOptions, server_name: Option<&str>) -> Self {
        Self(
            server_name
                .filter(|name| {
                    options.misdirected_request_check() && !name.is_empty() && !is_ip_address(name)
                })
                .map(Arc::from),
        )
    }

    /// Returns whether `request`, from the client at `remote_addr`, is for another name than the
    /// connection's and must be answered with 421, which it logs.
    pub fn rejects<B>(&self, request: &Request<B>, remote_addr: SocketAddr) -> bool {
        let Some(server_name) = self.0.as_deref() else {
            return false;
        };
        let host = request
            .uri()
            .authority()
            .map(http::uri::Authority::as_str)
            .or_else(|| request.headers().get(HOST)?.to_str().ok());
        let Some(host) = host.filter(|host| misdirected(host, server_name)) else {
            return false;
        };
        tracing::debug!(
            host,
            server_name,
            %remote_addr,
            "misdirected HTTP request answered with 421"
        );
        true
    }
}

/// Returns whether a request for `host`, an HTTP authority, came over a connection for
/// `server_name`. The port, a trailing dot and case do not count. Browsers coalesce connections
/// between names only, so a request for an IP address, which a client sends when it reaches the
/// server by address and names it only in TLS, is never misdirected.
fn misdirected<'a>(host: &'a str, server_name: &'a str) -> bool {
    let host = authority_host(host);
    if host.is_empty() || is_ip_address(host) {
        return false;
    }
    let trim = |name: &'a str| name.strip_suffix('.').unwrap_or(name);
    !trim(host).eq_ignore_ascii_case(trim(server_name))
}

fn is_ip_address(host: &str) -> bool {
    host.parse::<IpAddr>().is_ok()
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use http::Request;

    use super::{HostCheck, misdirected};
    use crate::server::ServerOptions;

    #[test]
    fn compares_hosts_without_port_case_or_trailing_dot() {
        for (host, server_name, want) in [
            ("b.example", "a.example", true),
            ("b.example:443", "a.example", true),
            ("a.example.evil", "a.example", true),
            ("a.example", "a.example", false),
            ("a.example:8443", "a.example", false),
            ("A.Example.", "a.example", false),
            ("a.example.:443", "A.EXAMPLE", false),
            ("a.example", "a.example.", false),
            ("", "a.example", false),
            ("127.0.0.1:8080", "localhost", false),
            ("[::1]:8080", "localhost", false),
            ("[::1]", "localhost", false),
        ] {
            assert_eq!(
                misdirected(host, server_name),
                want,
                "misdirected({host:?}, {server_name:?})"
            );
        }
    }

    #[test]
    fn checks_only_connections_with_a_server_name() {
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, 1));
        let request = |host: &str| {
            Request::builder()
                .uri("/index.html")
                .header("host", host)
                .body(())
                .unwrap()
        };
        let options = ServerOptions::new();
        assert!(HostCheck::new(&options, Some("a.example")).rejects(&request("b.example"), addr));
        assert!(!HostCheck::new(&options, Some("a.example")).rejects(&request("a.example"), addr));
        assert!(!HostCheck::new(&options, None).rejects(&request("b.example"), addr));
        assert!(!HostCheck::new(&options, Some("127.0.0.1")).rejects(&request("b.example"), addr));
        let disabled = options.clone().with_misdirected_request_check(false);
        assert!(!HostCheck::new(&disabled, Some("a.example")).rejects(&request("b.example"), addr));

        // The authority of an HTTP/2 or HTTP/3 request comes before its Host header.
        let request = Request::builder()
            .uri("https://b.example/index.html")
            .header("host", "a.example")
            .body(())
            .unwrap();
        assert!(HostCheck::new(&options, Some("a.example")).rejects(&request, addr));
    }
}
