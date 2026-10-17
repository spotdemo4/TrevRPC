//! Validation and admission shared by every HTTP binding: HTTP/3 and HTTP/2 requests, and
//! `WebTransport` and WebSocket browser sessions.

use http::{Method, Request, StatusCode, header};

use crate::server::{AdmissionHeader, Server};

/// The media type of `TrevRPC` request and response bodies.
pub(crate) const MEDIA_TYPE: &str = "application/trevrpc";

/// Validates an HTTP RPC request and runs the HTTP admission callback, returning the rejection
/// status when it is not accepted.
pub(crate) fn validate_http_request(server: &Server, request: &Request<()>) -> Option<StatusCode> {
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
            path: request.uri().path(),
            authority: request.uri().authority().map(http::uri::Authority::as_str),
            secure: request.uri().scheme_str() == Some("https"),
            headers: &headers,
        };
        if !admission(&admission_request) {
            return Some(StatusCode::FORBIDDEN);
        }
    }
    None
}

pub(crate) fn valid_content_type(headers: &http::HeaderMap) -> bool {
    let mut values = headers.get_all(header::CONTENT_TYPE).iter();
    let Some(value) = values.next() else {
        return false;
    };
    values.next().is_none() && value.as_bytes().eq_ignore_ascii_case(MEDIA_TYPE.as_bytes())
}

pub(crate) fn admission_headers(headers: &http::HeaderMap) -> Vec<AdmissionHeader<'_>> {
    headers
        .iter()
        .map(|(name, value)| AdmissionHeader {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect()
}

/// Validates a browser session request (`WebTransport` or WebSocket) against the shared path and
/// the session admission policy, returning the rejection status when it is not accepted.
pub(crate) fn validate_webtransport_admission(
    server: &Server,
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
            path,
            authority,
            origin,
            secure,
            headers,
        };
        return (!admission(&request)).then_some(StatusCode::FORBIDDEN);
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
        !options
            .webtransport_allowed_origins()
            .iter()
            .any(|allowed| allowed == origin)
    }) {
        return Some(StatusCode::FORBIDDEN);
    }
    None
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
