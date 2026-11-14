//! The rate-limit interceptor end to end: a real server over QUIC and `QMux` over TLS rejects a
//! client that spends its tokens, and the client reads the retry delay from the error.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::greeter;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::client::{ChannelConfig, QmuxTransport, QuicTransport};
use trevrpc::server::{
    RateLimit, RateLimitOptions, RatePolicy, RequestContext, Server, ServerOptions,
};
use trevrpc::tcp::TcpFallback;
use trevrpc::{Code, Status};

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Replies to `SayHello` and counts the calls that reach it.
struct Greeter(Arc<AtomicUsize>);

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(greeter::HelloReply {
            message: request.name,
        })
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        _request: greeter::HelloRequest,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<greeter::HelloReply, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

struct RunningServer {
    udp: SocketAddr,
    tcp: SocketAddr,
    cert: CertificateDer<'static>,
    handled: Arc<AtomicUsize>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<trevrpc::Result<()>>,
}

impl RunningServer {
    async fn stop(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        within!(TEST_TIMEOUT, &mut self.task)???;
        Ok(())
    }
}

/// Spawns a greeter server that allows two `SayHello` calls an hour per client.
async fn spawn_server() -> TestResult<RunningServer> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;

    let handled = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new();
    server.set_options(
        ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(500))),
    );
    server.add_interceptor(RateLimit::new(RateLimitOptions::new().with_policy(
        greeter::greeter_paths::SAY_HELLO,
        RatePolicy::new(2, Duration::from_hours(1)),
    )));
    greeter::register_greeter(&mut server, Greeter(Arc::clone(&handled)));
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let udp = endpoint.local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = TcpFallback::new(listener, server.tcp_tls_config(tls));
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));
    Ok(RunningServer {
        udp,
        tcp,
        cert: cert_der,
        handled,
        shutdown: Some(shutdown),
        task,
    })
}

fn client_tls(
    cert: &CertificateDer<'static>,
    alpn: &[u8],
) -> TestResult<quinn::rustls::ClientConfig> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.clone())?;
    let mut tls = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    Ok(tls)
}

/// Makes `SayHello` calls through `client` until the server rejects one, and checks the rejection.
async fn spend_the_bucket<T>(client: &greeter::GreeterClient<T>) -> TestResult
where
    T: trevrpc::client::RpcTransport,
{
    let request = greeter::HelloRequest {
        name: "hi".to_owned(),
    };
    for _ in 0..2 {
        within!(TEST_TIMEOUT, client.say_hello(&request))??;
    }
    let Err(error) = within!(TEST_TIMEOUT, client.say_hello(&request))? else {
        return Err("the third call should be rejected".into());
    };

    // One token takes 30 minutes to refill; none of that has passed.
    let delay = error.retry_delay().ok_or("no retry delay")?;
    ensure!(
        delay > Duration::from_mins(29) && delay <= Duration::from_mins(30),
        "retry delay {delay:?}"
    );
    let status = error.into_status();
    ensure_eq!(status.code(), Code::ResourceExhausted);
    ensure!(
        status
            .message()
            .starts_with("rate limit exceeded: 2 requests")
    );
    Ok(())
}

#[tokio::test]
async fn a_client_over_quic_reads_the_retry_delay() -> TestResult {
    let server = spawn_server().await?;
    let config = ChannelConfig::new().quic_config(client_tls(&server.cert, trevrpc::ALPN)?)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config);
    let connection = within!(TEST_TIMEOUT, endpoint.connect(server.udp, "localhost")?)??;
    let client = greeter::GreeterClient::new(QuicTransport::new(connection.clone()));

    spend_the_bucket(&client).await?;

    ensure_eq!(server.handled.load(Ordering::SeqCst), 2);
    connection.close(0_u32.into(), b"done");
    server.stop().await
}

#[tokio::test]
async fn a_client_over_qmux_reads_the_retry_delay() -> TestResult {
    let server = spawn_server().await?;
    let transport = within!(
        TEST_TIMEOUT,
        QmuxTransport::connect(
            server.tcp,
            "localhost",
            client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
        ),
    )??;
    let client = greeter::GreeterClient::new(transport.clone());

    spend_the_bucket(&client).await?;

    ensure_eq!(server.handled.load(Ordering::SeqCst), 2);
    transport.close();
    server.stop().await
}
