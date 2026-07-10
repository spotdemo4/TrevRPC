use std::error::Error;
use std::future;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use quinn::crypto::rustls::QuicServerConfig;
use quinn::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::watch;

#[path = "shared/cert.rs"]
mod cert;

const DEFAULT_ADDR: &str = "127.0.0.1:0";
const DEFAULT_ORIGIN: &str = "http://127.0.0.1:8080";
const DEFAULT_AUTH_TOKEN: &str = "trevrpc-example-token";
const SERVICE_NAME: &str = "browser.lifecycle.Lifecycle";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .unwrap_or_else(|| env_or_owned("TREVRPC_EXAMPLE_ADDR", DEFAULT_ADDR))
        .parse::<SocketAddr>()?;

    let identity = make_identity()?;
    let certificate_path = cert::certificate_path()?;
    write_certificate(&identity, &certificate_path)?;

    let authorities =
        env_list("TREVRPC_EXAMPLE_AUTHORITIES").unwrap_or_else(|| webtransport_authorities(addr));
    let origin = env_or("TREVRPC_EXAMPLE_ORIGIN", DEFAULT_ORIGIN);
    let token = env_or("TREVRPC_EXAMPLE_TOKEN", DEFAULT_AUTH_TOKEN);
    let mut options = trevrpc::server::ServerOptions::new()
        .with_webtransport_allowed_authorities(authorities)
        .with_webtransport_allowed_origins(leak_slice([origin]));
    if let Some(max_streams) = env_usize("TREVRPC_EXAMPLE_MAX_STREAMS")? {
        options = options.with_max_concurrent_streams_per_connection(Some(max_streams));
    }

    let mut server = trevrpc::server::Server::new();
    server.set_options(options);
    server.set_authorizer(trevrpc::server::MetadataValueAuthorizer::bearer(token));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    register_lifecycle_routes(&mut server, shutdown_tx);

    let endpoint = make_endpoint(addr, &identity, server.options())?;
    println!("READY https://{}/trevrpc", endpoint.local_addr()?);
    println!("certificate written to {}", certificate_path.display());

    server
        .serve_quinn_and_webtransport_with_shutdown(endpoint, shutdown_signal(shutdown_rx))
        .await?;

    Ok(())
}

fn register_lifecycle_routes(server: &mut trevrpc::server::Server, shutdown: watch::Sender<bool>) {
    server.route_streaming(
        SERVICE_NAME,
        "EarlyOk",
        trevrpc::RpcKind::ClientStreaming,
        |_body, _requests| async { Ok(trevrpc::stream::from_iter([encode_value("early ok")])) },
    );
    server.route_streaming(
        SERVICE_NAME,
        "EarlyError",
        trevrpc::RpcKind::BidirectionalStreaming,
        |_body, _requests| async {
            Err::<trevrpc::BoxMessageStream<Vec<u8>>, _>(trevrpc::Error::from(
                trevrpc::Status::new(trevrpc::Code::PermissionDenied, "remote rejected upload"),
            ))
        },
    );
    server.route_streaming(
        SERVICE_NAME,
        "Pending",
        trevrpc::RpcKind::ServerStreaming,
        |_body, _requests| async {
            Ok(boxed_stream(PendingStream::new("EVENT pending_cancelled")))
        },
    );
    server.route_streaming(
        SERVICE_NAME,
        "FirstThenPending",
        trevrpc::RpcKind::ServerStreaming,
        |_body, _requests| async {
            Ok(boxed_stream(FirstThenPendingStream::new(
                encode_value("first"),
                "EVENT response_stream_closed",
            )))
        },
    );
    server.route_streaming(
        SERVICE_NAME,
        "LongReplies",
        trevrpc::RpcKind::ServerStreaming,
        |_body, _requests| async { Ok(boxed_stream(SequenceStream::new("reply", 256, None))) },
    );
    server.route_streaming(
        SERVICE_NAME,
        "BidiEchoMany",
        trevrpc::RpcKind::BidirectionalStreaming,
        |_body, requests| async { Ok(requests) },
    );
    server.route_streaming(
        SERVICE_NAME,
        "ErrorAfterMessages",
        trevrpc::RpcKind::ServerStreaming,
        |_body, _requests| async {
            Ok(boxed_stream(SequenceStream::new(
                "before-error",
                32,
                Some(trevrpc::Status::new(
                    trevrpc::Code::PermissionDenied,
                    "stream failed after messages",
                )),
            )))
        },
    );
    server.route_streaming(
        SERVICE_NAME,
        "ShutdownAfterFirst",
        trevrpc::RpcKind::ServerStreaming,
        move |_body, _requests| {
            let shutdown = shutdown.clone();
            async move {
                Ok(boxed_stream(FirstThenShutdownStream::new(
                    encode_value("first"),
                    "EVENT server_shutdown_mid_stream",
                    shutdown,
                )))
            }
        },
    );
}

fn boxed_stream(
    stream: impl trevrpc::MessageStream<Vec<u8>> + 'static,
) -> trevrpc::BoxMessageStream<Vec<u8>> {
    Box::new(stream)
}

struct SequenceStream {
    prefix: &'static str,
    count: usize,
    next: usize,
    final_status: Option<trevrpc::Status>,
}

impl SequenceStream {
    const fn new(
        prefix: &'static str,
        count: usize,
        final_status: Option<trevrpc::Status>,
    ) -> Self {
        Self {
            prefix,
            count,
            next: 0,
            final_status,
        }
    }
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<Vec<u8>> for SequenceStream {
    async fn next(&mut self) -> Option<trevrpc::Result<Vec<u8>>> {
        if self.next >= self.count {
            return self
                .final_status
                .take()
                .map(|status| Err(trevrpc::Error::from(status)));
        }

        let value = format!("{}-{:03}", self.prefix, self.next);
        self.next += 1;
        Some(Ok(encode_value(&value)))
    }

    fn is_non_blocking(&self) -> bool {
        true
    }
}

struct PendingStream {
    event: &'static str,
    logged: Arc<AtomicBool>,
}

impl PendingStream {
    fn new(event: &'static str) -> Self {
        Self {
            event,
            logged: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<Vec<u8>> for PendingStream {
    async fn next(&mut self) -> Option<trevrpc::Result<Vec<u8>>> {
        future::pending::<Option<trevrpc::Result<Vec<u8>>>>().await
    }
}

impl Drop for PendingStream {
    fn drop(&mut self) {
        log_event(self.event, &self.logged);
    }
}

struct FirstThenPendingStream {
    first: Option<Vec<u8>>,
    event: &'static str,
    logged: Arc<AtomicBool>,
}

impl FirstThenPendingStream {
    fn new(first: Vec<u8>, event: &'static str) -> Self {
        Self {
            first: Some(first),
            event,
            logged: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<Vec<u8>> for FirstThenPendingStream {
    async fn next(&mut self) -> Option<trevrpc::Result<Vec<u8>>> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }

        future::pending::<Option<trevrpc::Result<Vec<u8>>>>().await
    }
}

impl Drop for FirstThenPendingStream {
    fn drop(&mut self) {
        log_event(self.event, &self.logged);
    }
}

struct FirstThenShutdownStream {
    first: Option<Vec<u8>>,
    event: &'static str,
    shutdown: watch::Sender<bool>,
    logged: Arc<AtomicBool>,
}

impl FirstThenShutdownStream {
    fn new(first: Vec<u8>, event: &'static str, shutdown: watch::Sender<bool>) -> Self {
        Self {
            first: Some(first),
            event,
            shutdown,
            logged: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<Vec<u8>> for FirstThenShutdownStream {
    async fn next(&mut self) -> Option<trevrpc::Result<Vec<u8>>> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }

        log_event(self.event, &self.logged);
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = shutdown.send(true);
        });
        Some(Err(trevrpc::Error::from(trevrpc::Status::new(
            trevrpc::Code::Cancelled,
            "server shutdown",
        ))))
    }
}

fn log_event(event: &str, logged: &AtomicBool) {
    if !logged.swap(true, Ordering::Relaxed) {
        println!("{event}");
    }
}

fn encode_value(value: &str) -> Vec<u8> {
    assert!(
        value.len() <= 127,
        "test value is too long for single-byte protobuf length"
    );
    let mut body = Vec::with_capacity(value.len() + 2);
    body.push(0x0a);
    body.push(value.len().try_into().expect("value length is checked"));
    body.extend_from_slice(value.as_bytes());
    body
}

fn make_identity() -> Result<rcgen::CertifiedKey<rcgen::KeyPair>, Box<dyn Error + Send + Sync>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut params =
        rcgen::CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    params.not_before = not_before;
    params.not_after = not_before + time::Duration::hours(25);
    let cert = params.self_signed(&signing_key)?;

    Ok(rcgen::CertifiedKey { cert, signing_key })
}

fn make_endpoint(
    addr: SocketAddr,
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    options: &trevrpc::server::ServerOptions,
) -> Result<quinn::Endpoint, Box<dyn Error + Send + Sync>> {
    let cert_der = identity.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![
        trevrpc::ALPN.to_vec(),
        web_transport_quinn::ALPN.as_bytes().to_vec(),
    ];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(
        &mut server_config,
        options,
        trevrpc::quinn::TransportMode::WebTransport,
    );

    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn webtransport_authorities(addr: SocketAddr) -> &'static [&'static str] {
    let mut values = vec![addr.to_string()];
    match addr.ip() {
        IpAddr::V4(ip) if ip.octets() == [127, 0, 0, 1] => {
            values.push(format!("localhost:{}", addr.port()));
        }
        _ if addr.ip().is_loopback() => {
            values.push(format!("127.0.0.1:{}", addr.port()));
        }
        _ => {}
    }

    leak_vec(values)
}

fn env_or(name: &str, fallback: &'static str) -> &'static str {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => Box::leak(value.into_boxed_str()),
        _ => fallback,
    }
}

fn env_or_owned(name: &str, fallback: &str) -> String {
    match std::env::var(name) {
        Ok(value) if !value.is_empty() => value,
        _ => fallback.to_owned(),
    }
}

fn env_usize(name: &str) -> Result<Option<usize>, Box<dyn Error + Send + Sync>> {
    let Some(value) = std::env::var(name).ok().filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    Ok(Some(value.parse()?))
}

fn env_list(name: &str) -> Option<&'static [&'static str]> {
    let values = std::env::var(name).ok()?;
    let values = values
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    (!values.is_empty()).then(|| leak_vec(values))
}

fn leak_vec(values: Vec<String>) -> &'static [&'static str] {
    let values = values
        .into_iter()
        .map(|value| Box::leak(value.into_boxed_str()) as &'static str)
        .collect::<Vec<_>>();
    Box::leak(values.into_boxed_slice())
}

fn leak_slice<const N: usize>(values: [&'static str; N]) -> &'static [&'static str] {
    Box::leak(Vec::from(values).into_boxed_slice())
}

fn write_certificate(
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    path: &Path,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, identity.cert.pem())?;
    Ok(())
}

async fn shutdown_signal(mut shutdown_rx: watch::Receiver<bool>) {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt()).expect("listen for SIGINT");
        let mut terminate = signal(SignalKind::terminate()).expect("listen for SIGTERM");
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
            () = app_shutdown(&mut shutdown_rx) => {}
        }
    }

    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            () = app_shutdown(&mut shutdown_rx) => {}
        }
    }
}

async fn app_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    while shutdown_rx.changed().await.is_ok() {
        if *shutdown_rx.borrow() {
            break;
        }
    }
}
