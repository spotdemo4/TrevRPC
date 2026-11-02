//! The client address handlers, authorizers and admission callbacks see: the QUIC or TCP peer's,
//! or the one in the PROXY header of a trusted proxy in front of the TCP listener.

use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use bytes::Bytes;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::Status;
use trevrpc::client::{Channel, ChannelConfig, QmuxTransport};
use trevrpc::server::{
    AuthorizationRequest, Authorizer, HttpAdmission, HttpAdmissionRequest, RequestContext, Server,
    ServerOptions,
};
use trevrpc::tcp::{IpPrefix, TcpFallback};

#[allow(dead_code)]
#[path = "../examples/shared/greeter.rs"]
mod greeter;
#[macro_use]
mod support;

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// The client address the PROXY headers of these tests carry.
const CLIENT: &str = "203.0.113.7:51000";

/// Replies with the client address its context reports.
struct Greeter;

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        context: RequestContext,
        _request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, Status> {
        Ok(greeter::HelloReply {
            message: context
                .remote_addr()
                .map_or_else(|| "unknown".to_owned(), |addr| addr.to_string()),
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

/// Records the client address of every call it authorizes.
#[derive(Clone, Default)]
struct RecordingAuthorizer(Arc<Mutex<Vec<Option<SocketAddr>>>>);

#[trevrpc::async_trait]
impl Authorizer for RecordingAuthorizer {
    async fn authorize(&self, request: &AuthorizationRequest<'_>) -> Result<(), Status> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(request.remote_addr);
        Ok(())
    }
}

/// Admits only HTTP RPCs from [`CLIENT`].
fn admit_client(request: &HttpAdmissionRequest<'_>) -> bool {
    request.remote_addr.ip() == IpAddr::from([203, 0, 113, 7])
}

struct RunningServer {
    udp: SocketAddr,
    tcp: SocketAddr,
    cert: CertificateDer<'static>,
    authorized: RecordingAuthorizer,
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

    fn authorized(&self) -> Vec<Option<SocketAddr>> {
        self.authorized
            .0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

/// Spawns a greeter server on loopback whose TCP listener trusts `trusted_proxies`.
async fn spawn_server(trusted_proxies: &[&str]) -> TestResult<RunningServer> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;

    let mut server = Server::new();
    server.set_options(
        ServerOptions::new()
            .with_http_enabled(true)
            .with_http_admission(Some(HttpAdmission::new(admit_client)))
            .with_graceful_shutdown_timeout(Some(Duration::from_millis(500))),
    );
    greeter::register_greeter(&mut server, Greeter);
    let authorized = RecordingAuthorizer::default();
    server.set_authorizer(authorized.clone());
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let udp = endpoint.local_addr()?;
    let tcp = listener.local_addr()?;
    let proxies = trusted_proxies
        .iter()
        .map(|prefix| prefix.parse::<IpPrefix>())
        .collect::<Result<Vec<_>, _>>()?;
    let fallback =
        TcpFallback::new(listener, server.tcp_tls_config(tls)).with_trusted_proxies(proxies);
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));
    Ok(RunningServer {
        udp,
        tcp,
        cert: cert_der,
        authorized,
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

/// Starts a TCP proxy in front of `upstream` that opens each connection with `header`.
async fn spawn_proxy(upstream: SocketAddr, header: Vec<u8>) -> TestResult<SocketAddr> {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        while let Ok((mut client, _)) = listener.accept().await {
            let header = header.clone();
            tokio::spawn(async move {
                let Ok(mut server) = TcpStream::connect(upstream).await else {
                    return;
                };
                if server.write_all(&header).await.is_ok() {
                    let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                }
            });
        }
    });
    Ok(addr)
}

/// The version 2 PROXY header of a TCP over IPv4 connection from [`CLIENT`].
fn proxy_v2_header() -> Vec<u8> {
    let mut header = b"\r\n\r\n\0\r\nQUIT\n\x21\x11\x00\x0c".to_vec();
    header.extend_from_slice(&[203, 0, 113, 7, 127, 0, 0, 1]);
    header.extend_from_slice(&51_000_u16.to_be_bytes());
    header.extend_from_slice(&443_u16.to_be_bytes());
    header
}

/// Says hello over `QMux` over TLS to `addr` and returns the address the server reports.
async fn qmux_remote_addr(server: &RunningServer, addr: SocketAddr) -> TestResult<String> {
    let transport = tokio::time::timeout(
        TEST_TIMEOUT,
        QmuxTransport::connect(
            addr,
            "localhost",
            client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
        ),
    )
    .await??;
    let reply = greeter::GreeterClient::new(transport.clone())
        .say_hello(&greeter::HelloRequest::default())
        .await;
    transport.close();
    Ok(reply?.message)
}

#[tokio::test]
async fn native_quic_calls_see_the_client_address() -> TestResult {
    let server = spawn_server(&[]).await?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let client_addr = endpoint.local_addr()?;
    let config = ChannelConfig::new();
    endpoint
        .set_default_client_config(config.quic_config(client_tls(&server.cert, trevrpc::ALPN)?)?);
    let channel = tokio::time::timeout(
        TEST_TIMEOUT,
        Channel::connect_with_config(endpoint, server.udp, "localhost", config),
    )
    .await??;
    let reply = greeter::GreeterClient::new(channel.clone())
        .say_hello(&greeter::HelloRequest::default())
        .await?;
    ensure_eq!(reply.message, client_addr.to_string());
    ensure_eq!(server.authorized(), [Some(client_addr)]);
    channel.close();
    server.stop().await
}

#[tokio::test]
async fn trusted_proxies_name_the_client_with_either_header_version() -> TestResult {
    let server = spawn_server(&["127.0.0.0/8"]).await?;
    let v1 = spawn_proxy(
        server.tcp,
        b"PROXY TCP4 203.0.113.7 127.0.0.1 51000 443\r\n".to_vec(),
    )
    .await?;
    ensure_eq!(qmux_remote_addr(&server, v1).await?, CLIENT);
    let v2 = spawn_proxy(server.tcp, proxy_v2_header()).await?;
    ensure_eq!(qmux_remote_addr(&server, v2).await?, CLIENT);
    let client = Some(CLIENT.parse()?);
    ensure_eq!(server.authorized(), [client, client]);

    // A health check of the proxy itself names no client: the proxy stands for it.
    let local = spawn_proxy(server.tcp, b"\r\n\r\n\0\r\nQUIT\n\x20\x00\x00\x00".to_vec()).await?;
    let reported: SocketAddr = qmux_remote_addr(&server, local).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
    server.stop().await
}

#[tokio::test]
async fn untrusted_peers_are_not_read_for_a_header() -> TestResult {
    let server = spawn_server(&["192.0.2.0/24"]).await?;
    // The PROXY header of an untrusted peer is the start of its TLS handshake, which fails.
    let proxy = spawn_proxy(server.tcp, proxy_v2_header()).await?;
    ensure!(qmux_remote_addr(&server, proxy).await.is_err());
    // Without one, the peer's own address is the client's.
    let reported: SocketAddr = qmux_remote_addr(&server, server.tcp).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
    server.stop().await
}

#[tokio::test]
async fn trusted_proxies_must_send_a_header() -> TestResult {
    let server = spawn_server(&["127.0.0.1/32"]).await?;
    ensure!(qmux_remote_addr(&server, server.tcp).await.is_err());
    ensure_eq!(server.authorized(), []);
    server.stop().await
}

/// Posts a unary `SayHello` over HTTP/2 to `addr` and returns the response status.
async fn http2_status(server: &RunningServer, addr: SocketAddr) -> TestResult<http::StatusCode> {
    let stream = TcpStream::connect(addr).await?;
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_tls(&server.cert, b"h2")?));
    let tls = connector
        .connect(ServerName::try_from("localhost")?, stream)
        .await?;
    let (sender, connection) = h2::client::handshake(tls).await?;
    tokio::spawn(connection);
    let mut sender = sender.ready().await?;
    let request = http::Request::post("https://localhost/trevrpc")
        .header(http::header::CONTENT_TYPE, "application/trevrpc")
        .body(())?;
    let (response, mut body) = sender.send_request(request, false)?;
    let mut frames = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &trevrpc::RpcRequest::new(greeter::greeter_paths::SAY_HELLO),
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;
    frames.extend(trevrpc::framing::encode_message_frame(
        &[],
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?);
    body.send_data(Bytes::from(frames), true)?;
    Ok(tokio::time::timeout(TEST_TIMEOUT, response)
        .await??
        .status())
}

#[tokio::test]
async fn http_admission_sees_the_client_address() -> TestResult {
    let server = spawn_server(&["127.0.0.0/8"]).await?;
    let proxy = spawn_proxy(server.tcp, proxy_v2_header()).await?;
    ensure_eq!(http2_status(&server, proxy).await?, http::StatusCode::OK);
    let local = spawn_proxy(server.tcp, b"PROXY UNKNOWN\r\n".to_vec()).await?;
    ensure_eq!(
        http2_status(&server, local).await?,
        http::StatusCode::FORBIDDEN
    );
    server.stop().await
}

#[test]
fn contexts_built_outside_a_connection_have_no_address() {
    let context = RequestContext::new(&trevrpc::RpcRequest::new("/a.B/C"), None);
    assert_eq!(context.remote_addr(), None);
    let addr: SocketAddr = CLIENT.parse().unwrap();
    assert_eq!(context.with_remote_addr(addr).remote_addr(), Some(addr));
}
