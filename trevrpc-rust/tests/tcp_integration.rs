//! The TCP listener: `QMux` over TLS, HTTP/2 and `QMux` over WebSocket, and the HTTP fallback it
//! shares with the HTTP/3 endpoint.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use prost::Message;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Barrier, mpsc, oneshot};
use tokio::task::JoinHandle;
use trevrpc::client::{
    CallOptions, Channel, ChannelConfig, QmuxTransport, RpcTransport, StreamingRpcTransport,
    TransportKind, TransportPolicy,
};
use trevrpc::fallback::{FallbackBody, HttpFallback};
use trevrpc::server::{
    CancellationSource, RequestContext, Server, ServerOptions, WebTransportAdmissionRequest,
};
use trevrpc::tcp::TcpFallback;
use trevrpc::{Code, RpcKind, Status};
use web_transport_trait::{RecvStream as _, SendStream as _, Session as _};

#[allow(dead_code)]
#[path = "../examples/shared/greeter.rs"]
mod greeter;
#[macro_use]
mod support;

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);
const ORIGIN: &str = "https://app.example.test";

struct Greeter;

fn response_headers() -> trevrpc::Metadata {
    trevrpc::Metadata::from([("x-header".to_owned(), b"h".to_vec())])
}

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
        .with_headers(response_headers()))
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(trevrpc::stream::from_iter([
            greeter::HelloReply {
                message: format!("hello, {}", request.name),
            },
            greeter::HelloReply {
                message: format!("goodbye, {}", request.name),
            },
        ]))
        .with_headers(response_headers()))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        mut requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, Status> {
        let mut names = Vec::new();
        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }
        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message: names.join(","),
        }))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(Box::pin(requests.map(
            |request| {
                request.map(|request| greeter::HelloReply {
                    message: format!("echo, {}", request.name),
                })
            },
        ))))
    }
}

fn admit_test_origin(request: &WebTransportAdmissionRequest<'_>) -> bool {
    request.origin == Some(ORIGIN)
}

struct RunningServer {
    udp: SocketAddr,
    tcp: SocketAddr,
    cert: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<trevrpc::Result<()>>,
}

impl RunningServer {
    async fn stop(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        tokio::time::timeout(TEST_TIMEOUT, &mut self.task).await???;
        Ok(())
    }
}

async fn spawn_server(options: ServerOptions) -> TestResult<RunningServer> {
    spawn_server_with(options, |_| {}).await
}

/// Spawns a server with the greeter and whatever routes `configure` adds.
async fn spawn_server_with(
    options: ServerOptions,
    configure: impl FnOnce(&mut Server),
) -> TestResult<RunningServer> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;

    let mut server = Server::new();
    server.set_options(options);
    greeter::register_greeter(&mut server, Greeter);
    configure(&mut server);
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let udp = endpoint.local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = TcpFallback {
        listener,
        tls: server.tcp_tls_config(tls),
    };
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));
    Ok(RunningServer {
        udp,
        tcp,
        cert: cert_der,
        shutdown: Some(shutdown),
        task,
    })
}

fn test_options() -> ServerOptions {
    ServerOptions::new()
        .with_http_enabled(true)
        .with_webtransport_enabled(true)
        .with_webtransport_admission(Some(admit_test_origin))
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(500)))
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

async fn connect_tls(
    server: &RunningServer,
    alpn: &[u8],
) -> TestResult<tokio_rustls::client::TlsStream<TcpStream>> {
    connect_tls_at(server.tcp, &server.cert, alpn).await
}

async fn connect_tls_at(
    addr: SocketAddr,
    cert: &CertificateDer<'static>,
    alpn: &[u8],
) -> TestResult<tokio_rustls::client::TlsStream<TcpStream>> {
    let stream = TcpStream::connect(addr).await?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_tls(cert, alpn)?));
    Ok(connector
        .connect(ServerName::try_from("localhost")?, stream)
        .await?)
}

/// The request frames of a unary `SayHello` call: REQUEST, then its one MESSAGE.
fn unary_frame(name: &str) -> TestResult<Vec<u8>> {
    request_frames(
        greeter::greeter_paths::SAY_HELLO,
        trevrpc::RpcKind::Unary,
        name,
    )
}

/// The frames that start an RPC: REQUEST, then one MESSAGE carrying a `HelloRequest`.
fn request_frames(path: &str, kind: trevrpc::RpcKind, name: &str) -> TestResult<Vec<u8>> {
    let request = trevrpc::RpcRequest::new(path).with_kind(kind);
    let mut frames = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;
    frames.extend(trevrpc::framing::encode_message_frame(
        &greeter::HelloRequest {
            name: name.to_owned(),
        }
        .encode_to_vec(),
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?);
    Ok(frames)
}

/// Decodes the response frames of a unary `SayHello` call: the headers, one MESSAGE, and an `OK`
/// STATUS.
fn decode_hello(response: &[u8]) -> TestResult<String> {
    use trevrpc::framing::frame_type;

    let frames = support::parse_frames(response)?;
    let [headers, message, status] = frames.as_slice() else {
        return Err(format!("expected HEADERS, MESSAGE and STATUS frames, got {frames:?}").into());
    };
    ensure_eq!(headers.frame_type, frame_type::HEADERS);
    let headers = trevrpc::framing::decode_payload::<trevrpc::RpcHeaders>(&headers.payload)?;
    ensure_eq!(headers.metadata, response_headers());
    ensure_eq!(message.frame_type, frame_type::MESSAGE);
    ensure_eq!(status.frame_type, frame_type::STATUS);
    let status = Status::from(trevrpc::framing::decode_payload::<trevrpc::RpcStatus>(
        &status.payload,
    )?);
    ensure_eq!(status.code(), trevrpc::Code::Ok, "{}", status.message());
    Ok(greeter::HelloReply::decode(message.payload.as_slice())?.message)
}

async fn exercise_greeter<T: StreamingRpcTransport>(
    client: &greeter::GreeterClient<T>,
) -> TestResult {
    let reply = client
        .say_hello_envelope(greeter::HelloRequest {
            name: "unary".to_owned(),
        })
        .await?;
    assert_eq!(reply.message().message, "hello, unary");
    assert_eq!(reply.headers(), &response_headers());

    let mut replies = client
        .lots_of_replies(greeter::HelloRequest {
            name: "stream".to_owned(),
        })
        .await?;
    assert_eq!(replies.headers().await?, &response_headers());
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    assert_eq!(messages, ["hello, stream", "goodbye, stream"]);

    let call = client
        .lots_of_greetings_with_options(CallOptions::default())
        .await?;
    for name in ["one", "two"] {
        call.send(greeter::HelloRequest {
            name: name.to_owned(),
        })
        .await?;
    }
    assert_eq!(call.close_and_recv().await?.message, "one,two");

    let mut call = client
        .bidi_hello_with_options(CallOptions::default())
        .await?;
    for name in ["left", "right"] {
        call.send(greeter::HelloRequest {
            name: name.to_owned(),
        })
        .await?;
    }
    call.close_send()?;
    let mut messages = Vec::new();
    while let Some(reply) = call.recv().await? {
        messages.push(reply.message);
    }
    assert_eq!(messages, ["echo, left", "echo, right"]);
    Ok(())
}

#[tokio::test]
async fn qmux_over_tls_round_trips_every_rpc_shape() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let transport = QmuxTransport::connect(
        server.tcp,
        "localhost",
        client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
    )
    .await?;
    let client = greeter::GreeterClient::new(transport.clone());
    tokio::time::timeout(TEST_TIMEOUT, exercise_greeter(&client)).await??;

    // Large messages cross many records and flow-control updates.
    let large = "x".repeat(1 << 20);
    let reply = client
        .say_hello(greeter::HelloRequest {
            name: large.clone(),
        })
        .await?;
    ensure_eq!(reply.message, format!("hello, {large}"));
    transport.close();
    server.stop().await
}

#[tokio::test]
async fn shutdown_closes_qmux_connections() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let transport = QmuxTransport::connect(
        server.tcp,
        "localhost",
        client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
    )
    .await?;
    let client = greeter::GreeterClient::new(transport.clone());
    client
        .say_hello(greeter::HelloRequest {
            name: "before".to_owned(),
        })
        .await?;
    server.stop().await?;
    tokio::time::timeout(TEST_TIMEOUT, transport.closed()).await?;
    Ok(())
}

#[tokio::test]
async fn http2_serves_post_rpcs_and_advertises_http3() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let tls = connect_tls(&server, b"h2").await?;
    let (sender, connection) = h2::client::handshake(tls).await?;
    tokio::spawn(connection);
    let mut sender = sender.ready().await?;
    let request = http::Request::post(format!("https://localhost:{}/trevrpc", server.tcp.port()))
        .header(http::header::CONTENT_TYPE, "application/trevrpc")
        .body(())?;
    let (response, mut body) = sender.send_request(request, false)?;
    body.send_data(Bytes::from(unary_frame("h2")?), true)?;
    let response = tokio::time::timeout(TEST_TIMEOUT, response).await??;
    ensure_eq!(response.status(), http::StatusCode::OK);
    ensure_eq!(
        response.headers().get(http::header::ALT_SVC),
        Some(&http::HeaderValue::try_from(format!(
            "h3=\":{}\"; ma=86400",
            server.udp.port()
        ))?)
    );
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        let _ = body.flow_control().release_capacity(chunk.len());
        bytes.extend_from_slice(&chunk);
    }
    ensure_eq!(decode_hello(&bytes)?, "hello, h2");
    server.stop().await
}

#[tokio::test]
async fn http2_rejects_requests_that_are_not_trevrpc() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let tls = connect_tls(&server, b"h2").await?;
    let (sender, connection) = h2::client::handshake(tls).await?;
    tokio::spawn(connection);
    for (method, path, content_type, expected) in [
        (
            "POST",
            "/elsewhere",
            "application/trevrpc",
            http::StatusCode::NOT_FOUND,
        ),
        (
            "GET",
            "/trevrpc",
            "application/trevrpc",
            http::StatusCode::METHOD_NOT_ALLOWED,
        ),
        (
            "POST",
            "/trevrpc",
            "text/plain",
            http::StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ),
    ] {
        let mut sender = sender.clone().ready().await?;
        let request = http::Request::builder()
            .method(method)
            .uri(format!("https://localhost:{}{path}", server.tcp.port()))
            .header(http::header::CONTENT_TYPE, content_type)
            .body(())?;
        let (response, _) = sender.send_request(request, true)?;
        let response = tokio::time::timeout(TEST_TIMEOUT, response).await??;
        ensure_eq!(
            response.status(),
            expected,
            "{method} {path} {content_type}"
        );
    }
    server.stop().await
}

async fn websocket_session(
    server: &RunningServer,
    origin: &str,
    subprotocol: &str,
) -> TestResult<std::result::Result<qmux::Session, http::StatusCode>> {
    websocket_session_at(server.tcp, &server.cert, origin, subprotocol).await
}

async fn websocket_session_at(
    addr: SocketAddr,
    cert: &CertificateDer<'static>,
    origin: &str,
    subprotocol: &str,
) -> TestResult<std::result::Result<qmux::Session, http::StatusCode>> {
    let tls = connect_tls_at(addr, cert, b"http/1.1").await?;
    let request = http::Request::get(format!("wss://localhost:{}/trevrpc", addr.port()))
        .header("Host", format!("localhost:{}", addr.port()))
        .header("Origin", origin)
        .header("Sec-WebSocket-Protocol", subprotocol)
        .header(
            "Sec-WebSocket-Key",
            qmux::ws::tungstenite::handshake::client::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .body(())?;
    match qmux::ws::tokio_tungstenite::client_async(request, tls).await {
        Ok((websocket, _)) => Ok(Ok(qmux::ws::Upgraded::new(websocket)
            .with_alpn("qmux-02.trevrpc-1")
            .connect())),
        Err(qmux::ws::tungstenite::Error::Http(response)) => Ok(Err(response.status())),
        Err(error) => Err(error.into()),
    }
}

#[tokio::test]
async fn qmux_over_websocket_serves_rpcs() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let session = websocket_session(&server, ORIGIN, "qmux-02.trevrpc-1")
        .await?
        .map_err(|status| format!("WebSocket upgrade refused with {status}"))?;
    let (mut send, mut recv) = session.open_bi().await?;
    send.write_all(&unary_frame("websocket")?).await?;
    send.finish()?;
    let response = tokio::time::timeout(TEST_TIMEOUT, recv.read_all()).await??;
    ensure_eq!(decode_hello(&response)?, "hello, websocket");
    session.close(0, "done");
    server.stop().await
}

#[tokio::test]
async fn websocket_upgrades_require_admission_and_the_subprotocol() -> TestResult {
    let server = spawn_server(test_options()).await?;
    ensure_eq!(
        websocket_session(&server, "https://evil.example.test", "qmux-02.trevrpc-1")
            .await?
            .err(),
        Some(http::StatusCode::FORBIDDEN)
    );
    ensure_eq!(
        websocket_session(&server, ORIGIN, "chat").await?.err(),
        Some(http::StatusCode::BAD_REQUEST)
    );
    server.stop().await
}

#[tokio::test]
async fn tcp_offers_only_enabled_protocols() -> TestResult {
    let server = spawn_server(ServerOptions::new()).await?;
    for alpn in [&b"h2"[..], b"http/1.1"] {
        ensure!(
            connect_tls(&server, alpn).await.is_err(),
            "{} negotiated with HTTP disabled",
            String::from_utf8_lossy(alpn)
        );
    }
    let transport = QmuxTransport::connect(
        server.tcp,
        "localhost",
        client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
    )
    .await?;
    let reply = greeter::GreeterClient::new(transport)
        .say_hello(greeter::HelloRequest {
            name: "native".to_owned(),
        })
        .await?;
    ensure_eq!(reply.message, "hello, native");
    server.stop().await
}

async fn channel(
    server: &RunningServer,
    remote_addr: SocketAddr,
    config: ChannelConfig,
) -> TestResult<Channel> {
    let quic_tls = client_tls(&server.cert, trevrpc::ALPN)?;
    let config = config
        .with_tcp_fallback(client_tls(&server.cert, trevrpc::QMUX_ALPN)?)
        .with_tcp_addr(server.tcp);
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config.quic_config(quic_tls)?);
    Ok(tokio::time::timeout(
        TEST_TIMEOUT,
        Channel::connect_with_config(endpoint, remote_addr, "localhost", config),
    )
    .await??)
}

#[tokio::test]
async fn channel_falls_back_to_qmux_when_udp_is_unreachable() -> TestResult {
    let server = spawn_server(test_options()).await?;
    // Nothing answers QUIC on this UDP port.
    let dead = std::net::UdpSocket::bind("127.0.0.1:0")?;
    let channel = channel(
        &server,
        dead.local_addr()?,
        ChannelConfig::new().with_fallback_delay(Duration::from_millis(50)),
    )
    .await?;
    ensure_eq!(channel.transport(), Some(TransportKind::Qmux));
    tokio::time::timeout(
        TEST_TIMEOUT,
        exercise_greeter(&greeter::GreeterClient::new(channel.clone())),
    )
    .await??;
    channel.close();
    server.stop().await
}

#[tokio::test]
async fn channel_prefers_quic() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let channel = channel(&server, server.udp, ChannelConfig::new()).await?;
    ensure_eq!(channel.transport(), Some(TransportKind::Quic));
    channel.close();
    server.stop().await
}

#[tokio::test]
async fn channel_can_use_tcp_only() -> TestResult {
    let server = spawn_server(test_options()).await?;
    let channel = channel(
        &server,
        server.udp,
        ChannelConfig::new().with_transport_policy(TransportPolicy::TcpOnly),
    )
    .await?;
    ensure_eq!(channel.transport(), Some(TransportKind::Qmux));
    let reply = greeter::GreeterClient::new(channel.clone())
        .say_hello(greeter::HelloRequest {
            name: "tcp".to_owned(),
        })
        .await?;
    ensure_eq!(reply.message, "hello, tcp");
    channel.close();
    server.stop().await
}

// A `QMux` session can die while calls are in flight. Every call must then fail, and the server
// must cancel its handlers, without waiting for a deadline or an idle timeout.

/// How long an in-flight call has to notice that its session died. None of these calls has a
/// deadline, so a hang would otherwise last forever.
const PROMPT: Duration = Duration::from_secs(1);

const HANG_UNARY: &str = "/loss.Hang/Unary";
const HANG_SERVER_STREAM: &str = "/loss.Hang/ServerStream";
const HANG_BIDI: &str = "/loss.Hang/Bidi";
const IDLE_BIDI: &str = "/loss.Hang/IdleBidi";
const BARRIER: &str = "/loss.Hang/Barrier";
const HANGING_ROUTES: usize = 4;

/// What the server's hanging handlers report.
struct HandlerEvents {
    started: mpsc::UnboundedReceiver<&'static str>,
    cancelled: mpsc::UnboundedReceiver<(&'static str, CancellationSource)>,
}

impl HandlerEvents {
    async fn all_started(&mut self) -> TestResult {
        for _ in 0..HANGING_ROUTES {
            tokio::time::timeout(TEST_TIMEOUT, self.started.recv())
                .await?
                .ok_or("server dropped its handler events")?;
        }
        Ok(())
    }

    /// Waits for every hanging handler to see its call cancelled because the connection was lost.
    async fn all_connection_lost(&mut self) -> TestResult {
        let mut names = Vec::new();
        for _ in 0..HANGING_ROUTES {
            let (name, source) = tokio::time::timeout(PROMPT, self.cancelled.recv())
                .await
                .map_err(|_| format!("handlers cancelled so far: {names:?}"))?
                .ok_or("server dropped its handler events")?;
            ensure_eq!(source, CancellationSource::ConnectionLost, "{name}");
            names.push(name);
        }
        Ok(())
    }
}

/// Registers handlers that wait until their call is cancelled, a `BARRIER` route that completes
/// once `HANGING_ROUTES` calls are in it at once, and returns what the hanging handlers report.
fn register_hanging_routes(server: &mut Server) -> HandlerEvents {
    let (started_tx, started) = mpsc::unbounded_channel();
    let (cancelled_tx, cancelled) = mpsc::unbounded_channel();

    // The server stops polling a handler once its call is cancelled, so a task reports that.
    let watch = move |name: &'static str, context: RequestContext| {
        let _ = started_tx.send(name);
        let cancelled_tx = cancelled_tx.clone();
        tokio::spawn(async move {
            let source = context.cancelled_signal().await;
            let _ = cancelled_tx.send((name, source));
        });
    };

    let hang = watch.clone();
    server.route_with_context(HANG_UNARY, move |context, _body| {
        hang(HANG_UNARY, context);
        std::future::pending()
    });
    for (path, kind) in [
        (HANG_SERVER_STREAM, RpcKind::ServerStreaming),
        (HANG_BIDI, RpcKind::BidirectionalStreaming),
    ] {
        let hang = watch.clone();
        server.route_streaming_with_context(path, kind, move |context, _body, _requests| {
            hang(path, context);
            std::future::pending()
        });
    }
    // This one has responded already: the server is writing its response, which never has more.
    let idle = watch;
    server.route_streaming_with_context(
        IDLE_BIDI,
        RpcKind::BidirectionalStreaming,
        move |context, _body, _requests| {
            idle(IDLE_BIDI, context);
            let silence: trevrpc::BoxStream<Vec<u8>> = Box::pin(futures_util::stream::pending());
            std::future::ready(Ok(silence))
        },
    );

    let barrier = Arc::new(Barrier::new(HANGING_ROUTES));
    server.route(BARRIER, move |_body| {
        let barrier = barrier.clone();
        async move {
            barrier.wait().await;
            Ok(Vec::new())
        }
    });

    HandlerEvents { started, cancelled }
}

/// A TCP relay for one connection that can be cut abruptly, like a network failure.
struct Relay {
    addr: SocketAddr,
    task: JoinHandle<()>,
}

impl Relay {
    async fn to(target: SocketAddr) -> TestResult<Self> {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
        let addr = listener.local_addr()?;
        let task = tokio::spawn(async move {
            let Ok((mut client, _)) = listener.accept().await else {
                return;
            };
            let Ok(mut server) = TcpStream::connect(target).await else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
        });
        Ok(Self { addr, task })
    }

    /// Drops both TCP connections.
    async fn cut(self) {
        self.task.abort();
        let _ = self.task.await;
    }
}

fn hanging_options() -> ServerOptions {
    // Few enough request slots that a pinned one is noticed.
    test_options().with_max_concurrent_requests(Some(HANGING_ROUTES))
}

/// Asserts that a call without a deadline failed within `PROMPT` as `Unavailable`.
async fn ensure_unavailable(name: &str, call: JoinHandle<trevrpc::Result<()>>) -> TestResult {
    let result = tokio::time::timeout(PROMPT, call)
        .await
        .map_err(|_| format!("{name} is still waiting {PROMPT:?} after the session died"))??;
    let error = result.err().ok_or_else(|| format!("{name} succeeded"))?;
    let status = error.into_status();
    ensure_eq!(
        status.code(),
        Code::Unavailable,
        "{name}: {}",
        status.message()
    );
    Ok(())
}

/// Checks that the server released the request slots of the lost calls: the barrier route only
/// completes when `HANGING_ROUTES` calls are in it at once.
async fn ensure_request_slots_released(server: &RunningServer) -> TestResult {
    let transport = Arc::new(
        QmuxTransport::connect(
            server.tcp,
            "localhost",
            client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
        )
        .await?,
    );
    let calls = (0..HANGING_ROUTES).map(|_| {
        let transport = transport.clone();
        tokio::spawn(async move {
            transport
                .call(trevrpc::RpcRequest::new(BARRIER), Vec::new())
                .await
                .map(drop)
        })
    });
    let results = tokio::time::timeout(PROMPT, futures_util::future::join_all(calls))
        .await
        .map_err(|_| "the server still holds the request slots of the lost calls")?;
    for result in results {
        result??;
    }
    transport.close();
    Ok(())
}

#[tokio::test]
async fn qmux_calls_fail_promptly_when_the_session_dies() -> TestResult {
    let mut events = None;
    let server = spawn_server_with(hanging_options(), |server| {
        events = Some(register_hanging_routes(server));
    })
    .await?;
    let mut events = events.ok_or("routes were not registered")?;
    let relay = Relay::to(server.tcp).await?;
    let transport = QmuxTransport::connect(
        relay.addr,
        "localhost",
        client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
    )
    .await?;

    let hello = greeter::HelloRequest {
        name: "hang".to_owned(),
    };
    let unary = tokio::spawn({
        let (transport, hello) = (transport.clone(), hello.clone());
        async move {
            trevrpc::client::unary::<_, _, greeter::HelloReply>(
                &transport,
                HANG_UNARY,
                &hello,
                CallOptions::default(),
            )
            .await
            .map(drop)
        }
    });
    let server_stream = tokio::spawn({
        let (transport, hello) = (transport.clone(), hello.clone());
        async move {
            let mut replies = trevrpc::client::server_streaming::<_, _, greeter::HelloReply>(
                &transport,
                HANG_SERVER_STREAM,
                &hello,
                CallOptions::default(),
            )
            .await?;
            match replies.next().await {
                Some(Err(error)) => Err(error),
                _ => Ok(()),
            }
        }
    });
    let bidi = |path: &'static str| {
        let transport = transport.clone();
        tokio::spawn(async move {
            let mut call = trevrpc::client::bidirectional_streaming::<
                _,
                greeter::HelloRequest,
                greeter::HelloReply,
            >(&transport, path, CallOptions::default())
            .await?;
            call.recv().await.map(drop)
        })
    };
    let bidi_calls = [("bidi", bidi(HANG_BIDI)), ("idle bidi", bidi(IDLE_BIDI))];
    events.all_started().await?;

    relay.cut().await;

    ensure_unavailable("unary", unary).await?;
    ensure_unavailable("server-streaming read", server_stream).await?;
    for (name, call) in bidi_calls {
        ensure_unavailable(name, call).await?;
    }
    // The dead session also refuses new calls rather than waiting.
    let new_call = tokio::time::timeout(
        PROMPT,
        transport.call(trevrpc::RpcRequest::new(HANG_UNARY), Vec::new()),
    )
    .await
    .map_err(|_| "a call on the dead session is still waiting")?;
    ensure_eq!(
        new_call.err().map(|error| error.into_status().code()),
        Some(Code::Unavailable)
    );
    // The handlers see the loss too, since the cut ended the server's session as well.
    events.all_connection_lost().await?;
    ensure_request_slots_released(&server).await?;
    server.stop().await
}

#[tokio::test]
async fn qmux_over_websocket_handlers_are_cancelled_when_the_client_connection_dies() -> TestResult
{
    let mut events = None;
    let server = spawn_server_with(hanging_options(), |server| {
        events = Some(register_hanging_routes(server));
    })
    .await?;
    let mut events = events.ok_or("routes were not registered")?;
    let relay = Relay::to(server.tcp).await?;
    let session = websocket_session_at(relay.addr, &server.cert, ORIGIN, "qmux-02.trevrpc-1")
        .await?
        .map_err(|status| format!("WebSocket upgrade refused with {status}"))?;

    // Each stream is left open for the client's request, as a real client's would be.
    let mut streams = Vec::new();
    for (path, kind) in [
        (HANG_UNARY, RpcKind::Unary),
        (HANG_SERVER_STREAM, RpcKind::ServerStreaming),
        (HANG_BIDI, RpcKind::BidirectionalStreaming),
        (IDLE_BIDI, RpcKind::BidirectionalStreaming),
    ] {
        let (mut send, recv) = session.open_bi().await?;
        send.write_all(&request_frames(path, kind, "hang")?).await?;
        if kind != RpcKind::BidirectionalStreaming {
            send.finish()?;
        }
        streams.push((send, recv));
    }
    events.all_started().await?;

    relay.cut().await;

    events.all_connection_lost().await?;
    ensure_request_slots_released(&server).await?;
    drop(streams);
    server.stop().await
}

/// Answers with the request's HTTP version, method and path, then the request body. A request to
/// `/hold` first signals `held` and waits for `release`.
#[derive(Clone, Default)]
struct EchoFallback {
    held: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

type EchoFallbackFuture = std::pin::Pin<
    Box<
        dyn Future<
                Output = Result<
                    http::Response<http_body_util::Full<Bytes>>,
                    std::convert::Infallible,
                >,
            > + Send,
    >,
>;

impl tower_service::Service<http::Request<FallbackBody>> for EchoFallback {
    type Response = http::Response<http_body_util::Full<Bytes>>;
    type Error = std::convert::Infallible;
    type Future = EchoFallbackFuture;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<FallbackBody>) -> Self::Future {
        let this = self.clone();
        Box::pin(async move {
            use http_body_util::BodyExt as _;
            if request.uri().path() == "/hold" {
                this.held.notify_one();
                this.release.notified().await;
            }
            let mut reply = format!(
                "fallback {:?} {} {}\n",
                request.version(),
                request.method(),
                request.uri().path()
            )
            .into_bytes();
            if let Ok(body) = request.into_body().collect().await {
                reply.extend_from_slice(&body.to_bytes());
            }
            Ok(http::Response::new(http_body_util::Full::new(Bytes::from(
                reply,
            ))))
        })
    }
}

fn fallback_options(options: ServerOptions, fallback: &EchoFallback) -> ServerOptions {
    options.with_http_fallback(Some(HttpFallback::new(fallback.clone())))
}

/// A response's status, its `Alt-Svc` header and its body.
type FallbackReply = (http::StatusCode, Option<String>, String);

fn alt_svc(headers: &http::HeaderMap) -> Option<String> {
    headers
        .get(http::header::ALT_SVC)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

/// Sends one HTTP/1.1 request on its own connection.
async fn http1_request(
    server: &RunningServer,
    method: &str,
    path: &str,
    body: &str,
) -> TestResult<FallbackReply> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let mut tls = connect_tls(server, b"http/1.1").await?;
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    );
    tls.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    tokio::time::timeout(TEST_TIMEOUT, tls.read_to_end(&mut response)).await??;
    let response = String::from_utf8(response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or("HTTP/1.1 response has no head")?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .ok_or("HTTP/1.1 response has no status line")?;
    let alt_svc = lines.find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case("alt-svc")
            .then(|| value.trim().to_owned())
    });
    Ok((
        http::StatusCode::from_bytes(status.as_bytes())?,
        alt_svc,
        body.to_owned(),
    ))
}

async fn http2_sender(server: &RunningServer) -> TestResult<h2::client::SendRequest<Bytes>> {
    let tls = connect_tls(server, b"h2").await?;
    let (sender, connection) = h2::client::handshake(tls).await?;
    tokio::spawn(connection);
    Ok(sender)
}

async fn http2_request(
    sender: &h2::client::SendRequest<Bytes>,
    request: http::Request<()>,
    body: Bytes,
) -> TestResult<(http::StatusCode, Option<String>, Vec<u8>)> {
    let mut sender = sender.clone().ready().await?;
    let (response, mut send) = sender.send_request(request, body.is_empty())?;
    if !body.is_empty() {
        send.send_data(body, true)?;
    }
    let response = tokio::time::timeout(TEST_TIMEOUT, response).await??;
    let status = response.status();
    let alt_svc = alt_svc(response.headers());
    let mut body = response.into_body();
    let mut bytes = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk?;
        let _ = body.flow_control().release_capacity(chunk.len());
        bytes.extend_from_slice(&chunk);
    }
    Ok((status, alt_svc, bytes))
}

async fn http2_fallback_request(
    server: &RunningServer,
    sender: &h2::client::SendRequest<Bytes>,
    method: &str,
    path: &str,
    body: &str,
) -> TestResult<FallbackReply> {
    let request = http::Request::builder()
        .method(method)
        .uri(format!("https://localhost:{}{path}", server.tcp.port()))
        .body(())?;
    let (status, alt_svc, body) =
        http2_request(sender, request, Bytes::from(body.to_owned())).await?;
    Ok((status, alt_svc, String::from_utf8(body)?))
}

type Http3Sender = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;

/// Opens an HTTP/3 connection to the server, able to carry a WebTransport session when
/// `webtransport` is set. The endpoint must outlive the connection.
async fn http3_sender(
    server: &RunningServer,
    webtransport: bool,
) -> TestResult<(quinn::Endpoint, Http3Sender)> {
    let tls = client_tls(&server.cert, trevrpc::HTTP3_ALPN)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
    )));
    let connection = endpoint.connect(server.udp, "localhost")?.await?;
    let mut builder = h3::client::builder();
    if webtransport {
        builder
            .enable_extended_connect(true)
            .enable_datagram(true)
            .enable_webtransport(true);
    }
    let (mut driver, sender) = builder.build(h3_quinn::Connection::new(connection)).await?;
    tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });
    Ok((endpoint, sender))
}

/// Sends `request` with `body` and reads the whole response.
async fn http3_exchange(
    sender: &mut Http3Sender,
    request: http::Request<()>,
    body: Bytes,
) -> TestResult<(http::Response<()>, Vec<u8>)> {
    use bytes::Buf as _;

    let mut stream = sender.send_request(request).await?;
    if !body.is_empty() {
        stream.send_data(body).await?;
    }
    stream.finish().await?;
    let response = tokio::time::timeout(TEST_TIMEOUT, stream.recv_response()).await??;
    let mut bytes = Vec::new();
    while let Some(mut chunk) = stream.recv_data().await? {
        bytes.extend_from_slice(&chunk.copy_to_bytes(chunk.remaining()));
    }
    Ok((response, bytes))
}

/// Sends one HTTP/3 request on its own connection.
async fn http3_request(
    server: &RunningServer,
    method: &str,
    path: &str,
    body: &str,
) -> TestResult<FallbackReply> {
    let (_endpoint, mut sender) = http3_sender(server, false).await?;
    let request = http::Request::builder()
        .method(method)
        .uri(format!("https://localhost:{}{path}", server.udp.port()))
        .body(())?;
    let (response, body) =
        http3_exchange(&mut sender, request, Bytes::from(body.to_owned())).await?;
    Ok((
        response.status(),
        alt_svc(response.headers()),
        String::from_utf8(body)?,
    ))
}

#[tokio::test]
async fn http_fallback_serves_every_http_version_alongside_rpcs() -> TestResult {
    let fallback = EchoFallback::default();
    let server = spawn_server(fallback_options(test_options(), &fallback)).await?;
    let advertised = Some(format!("h3=\":{}\"; ma=86400", server.udp.port()));
    let sender = http2_sender(&server).await?;

    ensure_eq!(
        http1_request(&server, "GET", "/app/index.html", "").await?,
        (
            http::StatusCode::OK,
            advertised.clone(),
            "fallback HTTP/1.1 GET /app/index.html\n".to_owned()
        )
    );
    ensure_eq!(
        http2_fallback_request(&server, &sender, "POST", "/app/upload", "h2 body").await?,
        (
            http::StatusCode::OK,
            advertised.clone(),
            "fallback HTTP/2.0 POST /app/upload\nh2 body".to_owned()
        )
    );
    ensure_eq!(
        http3_request(&server, "POST", "/app/upload", "h3 body").await?,
        (
            http::StatusCode::OK,
            None,
            "fallback HTTP/3.0 POST /app/upload\nh3 body".to_owned()
        )
    );

    // The `TrevRPC` path stays `TrevRPC`'s, on every HTTP version.
    ensure_eq!(
        http1_request(&server, "GET", "/trevrpc", "").await?.0,
        http::StatusCode::HTTP_VERSION_NOT_SUPPORTED
    );
    ensure_eq!(
        http2_fallback_request(&server, &sender, "GET", "/trevrpc", "")
            .await?
            .0,
        http::StatusCode::METHOD_NOT_ALLOWED
    );
    ensure_eq!(
        http3_request(&server, "GET", "/trevrpc", "").await?.0,
        http::StatusCode::METHOD_NOT_ALLOWED
    );

    let rpc = http::Request::post(format!("https://localhost:{}/trevrpc", server.tcp.port()))
        .header(http::header::CONTENT_TYPE, "application/trevrpc")
        .body(())?;
    let (status, _, reply) = http2_request(&sender, rpc, Bytes::from(unary_frame("h2")?)).await?;
    ensure_eq!(status, http::StatusCode::OK);
    ensure_eq!(decode_hello(&reply)?, "hello, h2");
    server.stop().await
}

#[tokio::test]
async fn http_fallback_alone_serves_every_http_version() -> TestResult {
    let fallback = EchoFallback::default();
    let server = spawn_server(fallback_options(ServerOptions::new(), &fallback)).await?;
    // `TrevRPC` claims no HTTP path, so even its default path is the fallback's.
    let expected = |version| format!("fallback {version} GET /trevrpc\n");
    ensure_eq!(
        http1_request(&server, "GET", "/trevrpc", "").await?.2,
        expected("HTTP/1.1")
    );
    let sender = http2_sender(&server).await?;
    ensure_eq!(
        http2_fallback_request(&server, &sender, "GET", "/trevrpc", "")
            .await?
            .2,
        expected("HTTP/2.0")
    );
    ensure_eq!(
        http3_request(&server, "GET", "/trevrpc", "").await?.2,
        expected("HTTP/3.0")
    );
    server.stop().await
}

#[tokio::test]
async fn http_fallback_requests_hold_a_stream_slot() -> TestResult {
    let fallback = EchoFallback::default();
    let options = test_options().with_max_concurrent_streams_per_connection(Some(1));
    let server = spawn_server(fallback_options(options, &fallback)).await?;
    let sender = http2_sender(&server).await?;

    let held = {
        let sender = sender.clone();
        let port = server.tcp.port();
        tokio::spawn(async move {
            let request = http::Request::get(format!("https://localhost:{port}/hold")).body(())?;
            http2_request(&sender, request, Bytes::new()).await
        })
    };
    tokio::time::timeout(TEST_TIMEOUT, fallback.held.notified()).await?;
    ensure_eq!(
        http2_fallback_request(&server, &sender, "GET", "/other", "")
            .await?
            .0,
        http::StatusCode::SERVICE_UNAVAILABLE
    );

    fallback.release.notify_one();
    let (status, _, _) = tokio::time::timeout(TEST_TIMEOUT, held).await???;
    ensure_eq!(status, http::StatusCode::OK);
    // The held request released its slot when its response ended.
    ensure_eq!(
        http2_fallback_request(&server, &sender, "GET", "/other", "")
            .await?
            .0,
        http::StatusCode::OK
    );
    server.stop().await
}

#[tokio::test]
async fn http3_serves_requests_alongside_a_webtransport_session() -> TestResult {
    let fallback = EchoFallback::default();
    let server = spawn_server(fallback_options(test_options(), &fallback)).await?;
    let (_endpoint, mut sender) = http3_sender(&server, true).await?;
    let port = server.udp.port();
    let connect = || {
        http::Request::builder()
            .method(http::Method::CONNECT)
            .uri(format!("https://localhost:{port}/trevrpc"))
            .header(http::header::ORIGIN, ORIGIN)
            .extension(h3::ext::Protocol::WEB_TRANSPORT)
            .body(())
    };
    let mut session = sender.send_request(connect()?).await?;
    let response = tokio::time::timeout(TEST_TIMEOUT, session.recv_response()).await??;
    ensure_eq!(response.status(), http::StatusCode::OK);

    // While the session runs, the connection still serves the fallback and HTTP RPCs.
    let get = http::Request::get(format!("https://localhost:{port}/app/index.html")).body(())?;
    let (response, body) = http3_exchange(&mut sender, get, Bytes::new()).await?;
    ensure_eq!(response.status(), http::StatusCode::OK);
    ensure_eq!(
        String::from_utf8(body)?,
        "fallback HTTP/3.0 GET /app/index.html\n"
    );
    let rpc = http::Request::post(format!("https://localhost:{port}/trevrpc"))
        .header(http::header::CONTENT_TYPE, "application/trevrpc")
        .body(())?;
    let (response, reply) =
        http3_exchange(&mut sender, rpc, Bytes::from(unary_frame("h3")?)).await?;
    ensure_eq!(response.status(), http::StatusCode::OK);
    ensure_eq!(decode_hello(&reply)?, "hello, h3");

    // A connection carries one session, so another CONNECT is reset.
    let mut second = sender.send_request(connect()?).await?;
    match tokio::time::timeout(TEST_TIMEOUT, second.recv_response()).await? {
        Ok(response) => {
            return Err(format!("a second session was answered {}", response.status()).into());
        }
        Err(error) => ensure!(
            matches!(
                &error,
                h3::error::StreamError::RemoteTerminate { code, .. }
                    if *code == h3::error::Code::H3_REQUEST_REJECTED
            ),
            "a second session failed with {error}"
        ),
    }
    drop(session);
    server.stop().await
}
