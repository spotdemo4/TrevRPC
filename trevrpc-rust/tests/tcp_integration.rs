//! The TCP listener: `QMux` over TLS, HTTP/2 and `QMux` over WebSocket.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use prost::Message;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::Status;
use trevrpc::client::{
    CallOptions, Channel, ChannelConfig, QmuxTransport, StreamingRpcTransport, TransportKind,
    TransportPolicy,
};
use trevrpc::server::{RequestContext, Server, ServerOptions, WebTransportAdmissionRequest};
use trevrpc::tcp::TcpFallback;
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
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;

    let mut server = Server::new();
    server.set_options(options);
    greeter::register_greeter(&mut server, Greeter);
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
    let stream = TcpStream::connect(server.tcp).await?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_tls(&server.cert, alpn)?));
    Ok(connector
        .connect(ServerName::try_from("localhost")?, stream)
        .await?)
}

/// The request frames of a unary `SayHello` call: REQUEST, then its one MESSAGE.
fn unary_frame(name: &str) -> TestResult<Vec<u8>> {
    let request = trevrpc::RpcRequest::new(greeter::GreeterClient::<()>::SERVICE, "SayHello");
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
    let tls = connect_tls(server, b"http/1.1").await?;
    let request = http::Request::get(format!("wss://localhost:{}/trevrpc", server.tcp.port()))
        .header("Host", format!("localhost:{}", server.tcp.port()))
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
