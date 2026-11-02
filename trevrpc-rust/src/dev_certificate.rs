//! A rotating self-signed certificate for local development, which browsers trust through the
//! `WebTransport` `serverCertificateHashes` option.

use std::convert::Infallible;
use std::fmt::{self, Write as _};
use std::future::{Ready, ready};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use http::{HeaderValue, Method, Request, Response, StatusCode, header};
use http_body_util::Full;
use quinn::rustls;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::rustls::server::{ClientHello, ResolvesServerCert};
use quinn::rustls::sign::CertifiedKey;

use crate::sync::MutexExt as _;
use crate::{Result, Status};

const DAY: Duration = Duration::from_hours(24);
/// How long each certificate is valid. Browsers trust a certificate by hash only when it is valid
/// for at most 14 days.
const LIFETIME: Duration = DAY.saturating_mul(13);
/// How long each certificate is presented before the next replaces it, which leaves it a week of
/// validity for the connections that verified it.
const PERIOD: Duration = DAY.saturating_mul(6);
/// How long before it is first presented each certificate's validity starts, for clients whose
/// clocks are behind.
const BACKDATE: Duration = Duration::from_hours(1);

/// The names a [`DevelopmentCertificate`] covers when none are given.
const DEFAULT_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// A self-signed ECDSA P-256 certificate for local development that rotates itself, so that a
/// server works without any certificate setup and browsers can trust it through the
/// `WebTransport` `serverCertificateHashes` option, which accepts only certificates valid for at
/// most 14 days.
///
/// Each certificate is valid for 13 days and presented for 6, and the next one is generated a
/// period ahead. [`DevelopmentCertificate::hashes`] and the JSON the certificate serves as a
/// [`tower_service::Service`] list both, so a page that fetched them before a rotation still
/// trusts the certificate presented after it. Production servers use a certificate from a
/// certificate authority instead.
///
/// Clones share the certificates. As a service it answers `GET` and `HEAD` requests with the
/// hashes as JSON, such as `[{"algorithm":"sha-256","value":"<hex>"}]`, which the JavaScript
/// client's `serverCertificateHashesUrl` option reads: route a path of the
/// [`HttpFallback`](crate::fallback::HttpFallback), or of the server that serves the web app, to
/// it.
#[derive(Clone)]
pub struct DevelopmentCertificate(Arc<Inner>);

struct Inner {
    hosts: Vec<String>,
    certificates: Mutex<Certificates>,
}

/// The certificate being presented and the one that replaces it.
#[derive(Clone)]
struct Certificates {
    current: Arc<Generated>,
    next: Arc<Generated>,
}

struct Generated {
    key: Arc<CertifiedKey>,
    hash: [u8; 32],
    pem: String,
    /// When the next certificate replaces this one.
    present_until: SystemTime,
}

impl DevelopmentCertificate {
    /// Generates a development certificate for `localhost`, `127.0.0.1` and `::1`.
    pub fn new() -> Result<Self> {
        Self::for_hosts(DEFAULT_HOSTS)
    }

    /// Generates a development certificate for `hosts`, which are DNS names or IP addresses;
    /// the defaults of [`DevelopmentCertificate::new`] when empty.
    pub fn for_hosts<I>(hosts: I) -> Result<Self>
    where
        I: IntoIterator,
        I::Item: Into<String>,
    {
        let mut hosts = hosts.into_iter().map(Into::into).collect::<Vec<_>>();
        if hosts.is_empty() {
            hosts = DEFAULT_HOSTS.map(str::to_owned).to_vec();
        }
        Self::starting_at(hosts, SystemTime::now())
    }

    fn starting_at(hosts: Vec<String>, now: SystemTime) -> Result<Self> {
        let certificates = generate_pair(&hosts, now)?;
        Ok(Self(Arc::new(Inner {
            hosts,
            certificates: Mutex::new(certificates),
        })))
    }

    /// A rustls server configuration that presents the current certificate, for
    /// [`Server::quic_config`](crate::server::Server::quic_config) and
    /// [`Server::tcp_tls_config`](crate::server::Server::tcp_tls_config).
    #[must_use]
    pub fn server_config(&self) -> rustls::ServerConfig {
        rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(self.clone()))
    }

    /// The SHA-256 hashes of the current and the next certificate, for
    /// `serverCertificateHashes`.
    pub fn hashes(&self) -> Result<[[u8; 32]; 2]> {
        let certificates = self.certificates_at(SystemTime::now())?;
        Ok([certificates.current.hash, certificates.next.hash])
    }

    /// The current and the next certificate PEM-encoded, for native clients to trust as roots. A
    /// client that loads them keeps trusting the server across one rotation.
    pub fn pem(&self) -> Result<String> {
        let certificates = self.certificates_at(SystemTime::now())?;
        Ok(format!(
            "{}{}",
            certificates.current.pem, certificates.next.pem
        ))
    }

    /// The hashes as the JSON the service answers.
    fn hashes_json(&self) -> Result<String> {
        let mut json = String::from("[");
        for (index, hash) in self.hashes()?.iter().enumerate() {
            if index > 0 {
                json.push(',');
            }
            json.push_str(r#"{"algorithm":"sha-256","value":""#);
            push_hex(&mut json, hash);
            json.push_str(r#""}"#);
        }
        json.push(']');
        Ok(json)
    }

    /// The certificates for `now`, rotated when the current one's period is over. A failed
    /// rotation keeps the old certificates for the next attempt.
    fn certificates_at(&self, now: SystemTime) -> Result<Certificates> {
        let mut certificates = self.0.certificates.lock_unpoisoned();
        if now < certificates.current.present_until {
            return Ok(certificates.clone());
        }
        *certificates = if now < certificates.next.present_until {
            Certificates {
                current: Arc::clone(&certificates.next),
                next: generate(&self.0.hosts, certificates.next.present_until)?,
            }
        } else {
            // Both periods are over, as after the machine slept: start afresh.
            generate_pair(&self.0.hosts, now)?
        };
        Ok(certificates.clone())
    }
}

impl fmt::Debug for DevelopmentCertificate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DevelopmentCertificate")
            .field("hosts", &self.0.hosts)
            .finish_non_exhaustive()
    }
}

impl ResolvesServerCert for DevelopmentCertificate {
    fn resolve(&self, _client_hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        match self.certificates_at(SystemTime::now()) {
            Ok(certificates) => Some(Arc::clone(&certificates.current.key)),
            Err(error) => {
                tracing::error!(%error, "cannot rotate the development certificate");
                None
            }
        }
    }
}

impl<B> tower_service::Service<Request<B>> for DevelopmentCertificate {
    type Response = Response<Full<Bytes>>;
    type Error = Infallible;
    type Future = Ready<std::result::Result<Self::Response, Infallible>>;

    fn poll_ready(
        &mut self,
        _context: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let mut response = Response::new(Full::default());
        if request.method() != Method::GET && request.method() != Method::HEAD {
            *response.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
            response
                .headers_mut()
                .insert(header::ALLOW, HeaderValue::from_static("GET, HEAD"));
            return ready(Ok(response));
        }
        match self.hashes_json() {
            Ok(json) => {
                *response.body_mut() = Full::new(Bytes::from(json));
                let headers = response.headers_mut();
                headers.insert(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                );
                headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            }
            Err(_) => *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR,
        }
        ready(Ok(response))
    }
}

/// Appends `bytes` to `out` in lowercase hex.
fn push_hex(out: &mut String, bytes: &[u8]) {
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
}

/// Generates a certificate presented from `now` and the one after it.
fn generate_pair(hosts: &[String], now: SystemTime) -> Result<Certificates> {
    let current = generate(hosts, now)?;
    let next = generate(hosts, current.present_until)?;
    Ok(Certificates { current, next })
}

/// Generates a certificate presented from `present_from` for one period.
fn generate(hosts: &[String], present_from: SystemTime) -> Result<Arc<Generated>> {
    let out_of_range = || Status::internal("development certificate time is out of range");
    let not_before = present_from
        .checked_sub(BACKDATE)
        .ok_or_else(out_of_range)?;
    let not_after = not_before.checked_add(LIFETIME).ok_or_else(out_of_range)?;
    let present_until = present_from.checked_add(PERIOD).ok_or_else(out_of_range)?;

    let failed = |error: rcgen::Error| {
        Status::internal(format!(
            "cannot generate a development certificate: {error}"
        ))
    };
    let signing_key =
        rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).map_err(failed)?;
    // Names that parse as IP addresses become IP address names.
    let mut params = rcgen::CertificateParams::new(hosts.to_vec()).map_err(failed)?;
    params.distinguished_name = rcgen::DistinguishedName::new();
    params.not_before = not_before.into();
    params.not_after = not_after.into();
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let certificate = params.self_signed(&signing_key).map_err(failed)?;

    let der = CertificateDer::from(certificate.der().to_vec());
    let private_key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(signing_key.serialize_der()));
    let key = rustls::crypto::ring::sign::any_ecdsa_type(&private_key).map_err(|error| {
        Status::internal(format!(
            "cannot load the development certificate key: {error}"
        ))
    })?;
    let hash = ring::digest::digest(&ring::digest::SHA256, &der);
    let hash = <[u8; 32]>::try_from(hash.as_ref())
        .map_err(|_| Status::internal("SHA-256 digest is not 32 bytes"))?;
    Ok(Arc::new(Generated {
        key: Arc::new(CertifiedKey::new(vec![der], key)),
        hash,
        pem: certificate.pem(),
        present_until,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn start() -> SystemTime {
        at(Duration::ZERO)
    }

    /// The time `offset` after the test's start.
    fn at(offset: Duration) -> SystemTime {
        SystemTime::UNIX_EPOCH
            .checked_add(DAY.saturating_mul(20_000).saturating_add(offset))
            .unwrap()
    }

    fn development_certificate() -> DevelopmentCertificate {
        DevelopmentCertificate::starting_at(DEFAULT_HOSTS.map(str::to_owned).to_vec(), start())
            .unwrap()
    }

    fn presented_hash(certificate: &DevelopmentCertificate, now: SystemTime) -> [u8; 32] {
        let certificates = certificate.certificates_at(now).unwrap();
        let der = certificates.current.key.cert.first().unwrap();
        let hash = ring::digest::digest(&ring::digest::SHA256, der);
        <[u8; 32]>::try_from(hash.as_ref()).unwrap()
    }

    #[test]
    fn rotates_to_the_advertised_next_certificate() {
        let certificate = development_certificate();
        let first = certificate.certificates_at(start()).unwrap();
        assert_ne!(first.current.hash, first.next.hash);
        assert_eq!(presented_hash(&certificate, start()), first.current.hash);

        let almost = at(PERIOD.saturating_sub(Duration::from_secs(1)));
        assert_eq!(presented_hash(&certificate, almost), first.current.hash);

        let rotated = at(PERIOD);
        assert_eq!(presented_hash(&certificate, rotated), first.next.hash);
        let second = certificate.certificates_at(rotated).unwrap();
        assert_eq!(second.current.hash, first.next.hash);
        assert_ne!(second.next.hash, first.current.hash);
        assert_ne!(second.next.hash, first.next.hash);
    }

    #[test]
    fn starts_afresh_after_both_periods() {
        let certificate = development_certificate();
        let first = certificate.certificates_at(start()).unwrap();
        let later = at(PERIOD.saturating_mul(3));
        let hash = presented_hash(&certificate, later);
        assert_ne!(hash, first.current.hash);
        assert_ne!(hash, first.next.hash);
        let certificates = certificate.certificates_at(later).unwrap();
        assert_eq!(
            certificates.current.present_until,
            at(PERIOD.saturating_mul(4))
        );
    }

    #[test]
    fn stays_within_the_browser_validity_limit() {
        assert!(LIFETIME <= DAY.saturating_mul(14));
        assert!(PERIOD.saturating_add(BACKDATE) < LIFETIME);
    }

    #[test]
    fn serves_its_hashes_as_json() {
        use tower_service::Service as _;

        let mut certificate = DevelopmentCertificate::new().unwrap();
        let hashes = certificate.hashes().unwrap();
        let response = certificate
            .call(Request::get("/certificate-hashes").body(()).unwrap())
            .into_inner()
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CONTENT_TYPE], "application/json");
        let hex = |hash: &[u8; 32]| {
            let mut hex = String::new();
            push_hex(&mut hex, hash);
            hex
        };
        let expected = format!(
            r#"[{{"algorithm":"sha-256","value":"{}"}},{{"algorithm":"sha-256","value":"{}"}}]"#,
            hex(&hashes[0]),
            hex(&hashes[1]),
        );
        let body = response.into_body();
        let body = futures_util::FutureExt::now_or_never(http_body_util::BodyExt::collect(body))
            .unwrap()
            .unwrap()
            .to_bytes();
        assert_eq!(body, expected.as_bytes());

        let response = certificate
            .call(Request::post("/certificate-hashes").body(()).unwrap())
            .into_inner()
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}
