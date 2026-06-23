use std::error::Error;
use std::fs;
use std::future::Future;
use std::net::{Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls;
use quinn::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};

#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const REQUEST_NAME: &str = "TrevRPC benchmark";
const STREAM_MESSAGE_COUNT: usize = 16;

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct SplitGreeter;

#[tokio::main]
async fn main() -> BenchResult {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("client") => {
            let addr = required_arg(&mut args, "addr")?.parse::<SocketAddr>()?;
            let cert = required_arg(&mut args, "cert")?;
            let iterations = required_arg(&mut args, "iterations")?.parse::<u32>()?;
            run_client(addr, Path::new(&cert), iterations).await
        }
        Some("server") => {
            let addr = args
                .next()
                .unwrap_or_else(|| "127.0.0.1:0".to_owned())
                .parse::<SocketAddr>()?;
            run_server(addr).await
        }
        Some("webtransport-server") => {
            let addr = required_arg(&mut args, "addr")?.parse::<SocketAddr>()?;
            let cert = required_arg(&mut args, "cert")?;
            let origin = required_arg(&mut args, "origin")?;
            run_webtransport_server(addr, Path::new(&cert), origin).await
        }
        _ => Err(
            "usage: rpc_split_bench client <addr> <cert> <iterations> | server [addr] | webtransport-server <addr> <cert> <origin>"
                .into(),
        ),
    }
}

async fn run_client(addr: SocketAddr, cert_path: &Path, iterations: u32) -> BenchResult {
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let endpoint = make_client_endpoint(cert_path)?;
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let client = greeter::GreeterClient::new(trevrpc::quinn::Client::new(connection.clone()));

    warm_client(&client).await?;
    run_case("unary_round_trip", iterations, || {
        trevrpc_unary_call(&client)
    })
    .await?;
    run_case("server_stream_16_messages", iterations, || {
        trevrpc_server_streaming_call(&client)
    })
    .await?;
    run_case("client_stream_16_messages", iterations, || {
        trevrpc_client_streaming_call(&client)
    })
    .await?;
    run_case("bidi_stream_16_messages", iterations, || {
        trevrpc_bidi_streaming_call(&client)
    })
    .await?;

    connection.close(0_u32.into(), b"split benchmark complete");
    endpoint.wait_idle().await;
    Ok(())
}

async fn run_server(addr: SocketAddr) -> BenchResult {
    let mut server = trevrpc::server::Server::new();
    server.set_options(benchmark_server_options());
    greeter::register_greeter(&mut server, SplitGreeter);
    let endpoint = make_server_endpoint(addr, server.options())?;
    let local_addr = endpoint.local_addr()?;

    println!("PORT {}", local_addr.port());
    server
        .serve_quinn_with_shutdown(endpoint, shutdown_signal())
        .await?;
    Ok(())
}

async fn run_webtransport_server(
    addr: SocketAddr,
    cert_path: &Path,
    origin: String,
) -> BenchResult {
    let mut server = trevrpc::server::Server::new();
    server.set_options(benchmark_webtransport_server_options(origin));
    greeter::register_greeter(&mut server, SplitGreeter);
    let endpoint = make_webtransport_server_endpoint(addr, server.options(), cert_path)?;
    let local_addr = endpoint.local_addr()?;

    println!("PORT {}", local_addr.port());
    println!("CERT {}", cert_path.display());
    server
        .serve_quinn_and_webtransport_with_shutdown(endpoint, shutdown_signal())
        .await?;
    Ok(())
}

async fn warm_client(client: &greeter::GreeterClient<trevrpc::quinn::Client>) -> BenchResult {
    trevrpc_unary_call(client).await?;
    trevrpc_server_streaming_call(client).await?;
    trevrpc_client_streaming_call(client).await?;
    trevrpc_bidi_streaming_call(client).await?;
    Ok(())
}

async fn run_case<F, Fut>(name: &str, iterations: u32, mut call: F) -> BenchResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = BenchResult>,
{
    let start = Instant::now();
    for _ in 0..iterations {
        call().await?;
    }
    let elapsed = start.elapsed();
    let ops = f64::from(iterations) / elapsed.as_secs_f64();
    println!(
        "{name}: {ops:.0} ops/s ({iterations} iterations in {:.3}s)",
        elapsed.as_secs_f64()
    );
    Ok(())
}

async fn trevrpc_unary_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult {
    let response = client
        .say_hello(
            greeter::HelloRequest {
                name: REQUEST_NAME.to_owned(),
            },
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    if response.message != REQUEST_NAME {
        return Err(format!("unary response = {:?}", response.message).into());
    }
    Ok(())
}

async fn trevrpc_server_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult {
    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: REQUEST_NAME.to_owned(),
            },
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    let mut count = 0;
    while let Some(reply) = replies.next().await {
        if reply?.message != "server stream" {
            return Err("unexpected server-stream response".into());
        }
        count += 1;
    }
    if count != STREAM_MESSAGE_COUNT {
        return Err(format!("server stream count = {count}").into());
    }
    Ok(())
}

async fn trevrpc_client_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult {
    let response = client
        .lots_of_greetings_from_stream(
            trevrpc::stream::from_iter(benchmark_requests()),
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    let expected = format!("streamed {STREAM_MESSAGE_COUNT} greetings");
    if response.message != expected {
        return Err(format!("client stream response = {:?}", response.message).into());
    }
    Ok(())
}

async fn trevrpc_bidi_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult {
    let mut replies = client
        .bidi_hello_from_stream(
            trevrpc::stream::from_iter(benchmark_requests()),
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    let mut count = 0;
    while let Some(reply) = replies.next().await.transpose()? {
        if reply.message != REQUEST_NAME {
            return Err("unexpected bidi response".into());
        }
        count += 1;
    }
    if count != STREAM_MESSAGE_COUNT {
        return Err(format!("bidi stream count = {count}").into());
    }
    Ok(())
}

fn benchmark_requests() -> impl Iterator<Item = greeter::HelloRequest> {
    (0..STREAM_MESSAGE_COUNT).map(|_| greeter::HelloRequest {
        name: REQUEST_NAME.to_owned(),
    })
}

#[trevrpc::async_trait]
impl greeter::Greeter for SplitGreeter {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply {
            message: request.name,
        })
    }

    async fn lots_of_replies(
        &self,
        _request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(trevrpc::stream::from_iter((0..STREAM_MESSAGE_COUNT).map(
            |_| greeter::HelloReply {
                message: "server stream".to_owned(),
            },
        )))
    }

    async fn lots_of_greetings(
        &self,
        mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        let mut count = 0;
        while let Some(request) = requests.next().await {
            request?;
            count += 1;
        }
        Ok(greeter::HelloReply {
            message: format!("streamed {count} greetings"),
        })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(Box::new(BidiReplies { requests }))
    }
}

struct BidiReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<greeter::HelloReply> for BidiReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: request.name,
            })
        })
    }
}

fn benchmark_server_options() -> trevrpc::server::ServerOptions {
    trevrpc::server::ServerOptions::new()
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
        .with_max_concurrent_connections(Some(512))
        .with_max_concurrent_streams_per_connection(Some(128))
        .with_max_concurrent_requests(Some(1024))
}

fn benchmark_webtransport_server_options(origin: String) -> trevrpc::server::ServerOptions {
    let origin: &'static str = Box::leak(origin.into_boxed_str());
    let origins: &'static [&'static str] = Box::leak(vec![origin].into_boxed_slice());
    benchmark_server_options()
        .with_max_concurrent_streams_per_connection(Some(65_535))
        .with_webtransport_allowed_origins(origins)
}

fn make_server_endpoint(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
) -> BenchResult<quinn::Endpoint> {
    let identity = make_identity()?;
    make_server_endpoint_with_identity(
        addr,
        options,
        &identity,
        vec![trevrpc::ALPN.to_vec()],
        false,
    )
}

fn make_webtransport_server_endpoint(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
    cert_path: &Path,
) -> BenchResult<quinn::Endpoint> {
    let identity = make_identity()?;
    write_certificate(&identity, cert_path)?;
    make_server_endpoint_with_identity(
        addr,
        options,
        &identity,
        vec![
            trevrpc::ALPN.to_vec(),
            web_transport_quinn::ALPN.as_bytes().to_vec(),
        ],
        true,
    )
}

fn make_identity() -> BenchResult<rcgen::CertifiedKey<rcgen::KeyPair>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut params =
        rcgen::CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    params.not_before = not_before;
    params.not_after = not_before + time::Duration::hours(25);
    let cert = params.self_signed(&signing_key)?;

    Ok(rcgen::CertifiedKey { cert, signing_key })
}

fn make_server_endpoint_with_identity(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    alpn_protocols: Vec<Vec<u8>>,
    enable_webtransport: bool,
) -> BenchResult<quinn::Endpoint> {
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            PrivateKeyDer::from(key_der),
        )?;
    server_crypto.alpn_protocols = alpn_protocols;

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(&mut server_config, options, enable_webtransport);
    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn write_certificate(identity: &rcgen::CertifiedKey<rcgen::KeyPair>, path: &Path) -> BenchResult {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, identity.cert.pem())?;
    Ok(())
}

fn make_client_endpoint(cert_path: &Path) -> BenchResult<quinn::Endpoint> {
    let _ = cert_path;
    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    trevrpc::quinn::configure_client_config(
        &mut client_config,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        false,
    );
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = signal.recv() => {}
            }
            return;
        }
    }

    let _ = tokio::signal::ctrl_c().await;
}

fn required_arg(args: &mut impl Iterator<Item = String>, name: &str) -> BenchResult<String> {
    args.next()
        .ok_or_else(|| format!("missing {name} argument").into())
}
