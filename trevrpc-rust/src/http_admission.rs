//! Validation and admission shared by every HTTP binding: HTTP/3 and HTTP/2 requests, and
//! `WebTransport` and WebSocket browser sessions.

use std::net::SocketAddr;

use http::{HeaderValue, Method, Request, Response, StatusCode, header};

use crate::server::{AdmissionHeader, Server};

/// The media type of `TrevRPC` request and response bodies.
pub const MEDIA_TYPE: &str = "application/trevrpc";

/// The head of an accepted RPC response, advertising HTTP/3 through `Alt-Svc` when given.
pub fn rpc_response_head(alt_svc: Option<HeaderValue>) -> Response<()> {
    let mut response = Response::new(());
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(MEDIA_TYPE));
    if let Some(alt_svc) = alt_svc {
        headers.insert(header::ALT_SVC, alt_svc);
    }
    response
}

/// The head of a response rejecting a request with `status`.
pub fn rejection_head(status: StatusCode) -> Response<()> {
    let mut response = Response::new(());
    *response.status_mut() = status;
    if status == StatusCode::METHOD_NOT_ALLOWED {
        response
            .headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
    }
    response
}

/// Validates an HTTP RPC request from the client at `remote_addr` and runs the HTTP admission
/// callback, returning the rejection status when it is not accepted.
pub fn validate_http_request(
    server: &Server,
    request: &Request<()>,
    remote_addr: SocketAddr,
) -> Option<StatusCode> {
    let options = server.options();
    if !options.http_enabled() || request.uri().path() != options.http_path() {
        return Some(StatusCode::NOT_FOUND);
    }
    if request.method() != Method::POST {
        return Some(StatusCode::METHOD_NOT_ALLOWED);
    }
    if !valid_content_type(request.headers()) {
        return Some(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    if let Some(admission) = options.http_admission() {
        let headers = admission_headers(request.headers());
        let admission_request = crate::server::HttpAdmissionRequest {
            request,
            remote_addr,
            path: request.uri().path(),
            authority: request.uri().authority().map(http::uri::Authority::as_str),
            secure: request.uri().scheme_str() == Some("https"),
            headers: &headers,
        };
        if !admission.admits(&admission_request) {
            return Some(StatusCode::FORBIDDEN);
        }
    }
    None
}

pub fn valid_content_type(headers: &http::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    values.next().is_none() && value.as_bytes().eq_ignore_ascii_case(MEDIA_TYPE.as_bytes())
}

pub fn admission_headers(headers: &http::HeaderMap) -> Vec<AdmissionHeader<'_>> {
    headers
        .iter()
        .map(|(name, value)| AdmissionHeader {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

/// Validates a browser session request (`WebTransport` or WebSocket) from the client at
/// `remote_addr` against the shared path and the session admission policy, returning the
/// rejection status when it is not accepted.
pub fn validate_webtransport_admission(
    server: &Server,
    remote_addr: SocketAddr,
    path: &str,
    authority: Option<&str>,
    origin: Option<&str>,
    secure: bool,
    headers: &[crate::server::AdmissionHeader<'_>],
) -> Option<StatusCode> {
    let options = server.options();
    if !options.webtransport_enabled() || path != options.http_path() {
        return Some(StatusCode::NOT_FOUND);
    }
    if let Some(admission) = options.webtransport_admission() {
        let request = crate::server::WebTransportAdmissionRequest {
            remote_addr,
            path,
            authority,
            origin,
            secure,
            headers,
        };
        return (!admission.admits(&request)).then_some(StatusCode::FORBIDDEN);
    }
    let allowed_authorities = options.webtransport_allowed_authorities();
    if !allowed_authorities.is_empty()
        && !authority.is_some_and(|authority| {
            allowed_authorities
                .iter()
                .any(|allowed| allowed == authority || allowed == authority_host(authority))
        })
    {
        return Some(StatusCode::FORBIDDEN);
    }
    if origin.is_some_and(|origin| {
        let origin = parse_origin(origin);
        !options
            .webtransport_allowed_origins()
            .iter()
            .any(|allowed| origin.is_some() && parse_origin(allowed) == origin)
    }) {
        return Some(StatusCode::FORBIDDEN);
    }
    None
}

/// Returns the normalized form of an origin serialized as `scheme://host[:port]` (see
/// [`normalize_origin`]), or `None` when `origin` is not one, such as the opaque origin `null`.
pub fn parse_origin(origin: &str) -> Option<String> {
    let (scheme, hostport) = origin.split_once("://")?;
    normalize_origin(scheme, hostport)
}

/// Returns `scheme://host[:port]` with the scheme and host in lower case and the port in decimal,
/// omitted when it is the scheme's default, or `None` when `scheme` or `hostport` is malformed.
pub fn normalize_origin(scheme: &str, hostport: &str) -> Option<String> {
    if scheme.is_empty()
        || !scheme
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"+-.".contains(&byte))
        || hostport.is_empty()
        || hostport.contains(['/', '?', '#', '@', '\\', ' '])
    {
        return None;
    }
    let scheme = scheme.to_ascii_lowercase();

    let (host, port) = if let Some(rest) = hostport.strip_prefix('[') {
        // An IPv6 address, which must be in brackets.
        let (host, after) = rest.split_once(']')?;
        if !host.contains(':') {
            return None;
        }
        let port = if after.is_empty() {
            None
        } else {
            Some(after.strip_prefix(':').filter(|port| !port.is_empty())?)
        };
        (host, port)
    } else if let Some((host, port)) = hostport.split_once(':') {
        if port.is_empty() || port.contains(':') {
            return None;
        }
        (host, Some(port))
    } else {
        (hostport, None)
    };
    if host.is_empty() {
        return None;
    }

    let port = match port {
        Some(port) if port.bytes().all(|byte| byte.is_ascii_digit()) => {
            let port = port.parse::<u16>().ok()?;
            (Some(port) != default_port(&scheme)).then_some(port)
        }
        Some(_) => return None,
        None => None,
    };

    let host = host.to_ascii_lowercase();
    let mut origin = scheme;
    origin.push_str("://");
    if host.contains(':') {
        origin.push('[');
        origin.push_str(&host);
        origin.push(']');
    } else {
        origin.push_str(&host);
    }
    if let Some(port) = port {
        origin.push(':');
        origin.push_str(&port.to_string());
    }
    Some(origin)
}

/// Returns the port a URL of `scheme` has when it names none.
fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "https" | "wss" => Some(443),
        "http" | "ws" => Some(80),
        _ => None,
    }
}

fn authority_host(authority: &str) -> &str {
    if let Some(authority) = authority.strip_prefix('[')
        && let Some((host, _)) = authority.split_once(']')
    {
        return host;
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

#[cfg(test)]
mod tests {
    use http::StatusCode;

    use super::validate_webtransport_admission;
    use crate::server::{Server, ServerOptions};

    #[test]
    fn allowed_origins_compare_as_browsers_serialize_them() {
        let mut server = Server::new();
        server.set_options(
            ServerOptions::new()
                .with_webtransport_enabled(true)
                .with_webtransport_allowed_origins(["HTTPS://App.Example:443", "null"]),
        );
        let admit = |origin| {
            validate_webtransport_admission(
                &server,
                "127.0.0.1:1".parse().unwrap(),
                "/trevrpc",
                Some("rpc.example"),
                Some(origin),
                true,
                &[],
            )
        };
        assert_eq!(admit("https://app.example"), None);
        assert_eq!(admit("https://APP.example:443"), None);
        assert_eq!(
            admit("https://app.example:8443"),
            Some(StatusCode::FORBIDDEN)
        );
        assert_eq!(admit("http://app.example"), Some(StatusCode::FORBIDDEN));
        // The opaque origin is never admitted, even when listed.
        assert_eq!(admit("null"), Some(StatusCode::FORBIDDEN));
    }
}
