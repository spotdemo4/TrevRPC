//! The client address handlers, authorizers and admission callbacks see: the QUIC or TCP peer's,
//! or the one in the PROXY headers of a trusted proxy in front of the TCP listener or the UDP
//! endpoint.

use std::error::Error;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use crate::greeter;
use bytes::Bytes;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use tokio::io::AsyncWriteExt as _;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::Status;
use trevrpc::client::{Channel, ChannelConfig, QmuxTransport};
use trevrpc::server::{
    AddressValidation, AuthorizationRequest, Authorizer, HttpAdmission, HttpAdmissionRequest,
    RequestContext, Server, ServerOptions,
};
use trevrpc::tcp::{IpPrefix, TcpFallback};
use trevrpc::udp::QuicEndpoint;

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
        within!(TEST_TIMEOUT, &mut self.task)???;
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

fn prefixes(networks: &[&str]) -> TestResult<Vec<IpPrefix>> {
    Ok(networks
        .iter()
        .map(|prefix| prefix.parse::<IpPrefix>())
        .collect::<Result<Vec<_>, _>>()?)
}

/// Spawns a greeter server on loopback whose TCP listener trusts `trusted_proxies` and whose
/// QUIC endpoint trusts `trusted_udp_proxies`.
async fn spawn_server(
    trusted_proxies: &[&str],
    trusted_udp_proxies: &[&str],
) -> TestResult<RunningServer> {
    spawn_server_validating(trusted_proxies, trusted_udp_proxies, AddressValidation::Off).await
}

/// [`spawn_server`] whose QUIC endpoint validates client addresses as `address_validation` says.
async fn spawn_server_validating(
    trusted_proxies: &[&str],
    trusted_udp_proxies: &[&str],
    address_validation: AddressValidation,
) -> TestResult<RunningServer> {
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
            .with_graceful_shutdown_timeout(Some(Duration::from_millis(500)))
            .with_address_validation(address_validation),
    );
    greeter::register_greeter(&mut server, Greeter);
    let authorized = RecordingAuthorizer::default();
    server.set_authorizer(authorized.clone());
    let endpoint = QuicEndpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from(([127, 0, 0, 1], 0)),
        prefixes(trusted_udp_proxies)?,
    )?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let udp = endpoint.endpoint().local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = TcpFallback::new(listener, server.tcp_tls_config(tls))
        .with_trusted_proxies(prefixes(trusted_proxies)?);
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
    let transport = within!(
        TEST_TIMEOUT,
        QmuxTransport::connect(
            addr,
            "localhost",
            client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
        ),
    )??;
    let reply = greeter::GreeterClient::new(transport.clone())
        .say_hello(&greeter::HelloRequest::default())
        .await;
    transport.close();
    Ok(reply?.message)
}

#[tokio::test]
async fn native_quic_calls_see_the_client_address() -> TestResult {
    let server = spawn_server(&[], &[]).await?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let client_addr = endpoint.local_addr()?;
    let config = ChannelConfig::new();
    endpoint
        .set_default_client_config(config.quic_config(client_tls(&server.cert, trevrpc::ALPN)?)?);
    let channel = within!(
        TEST_TIMEOUT,
        Channel::connect_with_config(endpoint, server.udp, "localhost", config),
    )??;
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
    let server = spawn_server(&["127.0.0.0/8"], &[]).await?;
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
    let server = spawn_server(&["192.0.2.0/24"], &[]).await?;
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
    let server = spawn_server(&["127.0.0.1/32"], &[]).await?;
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
    Ok(within!(TEST_TIMEOUT, response)??.status())
}

#[tokio::test]
async fn http_admission_sees_the_client_address() -> TestResult {
    let server = spawn_server(&["127.0.0.0/8"], &[]).await?;
    let proxy = spawn_proxy(server.tcp, proxy_v2_header()).await?;
    ensure_eq!(http2_status(&server, proxy).await?, http::StatusCode::OK);
    let local = spawn_proxy(server.tcp, b"PROXY UNKNOWN\r\n".to_vec()).await?;
    ensure_eq!(
        http2_status(&server, local).await?,
        http::StatusCode::FORBIDDEN
    );
    server.stop().await
}

/// Starts a UDP proxy in front of `upstream` that forwards the datagrams of each client from a
/// socket of its own, each preceded by the header `header` returns for the client, and forwards
/// the replies back.
async fn spawn_udp_proxy(
    upstream: SocketAddr,
    header: impl Fn(SocketAddr) -> Vec<u8> + Send + 'static,
) -> TestResult<SocketAddr> {
    spawn_udp_relay(upstream, header, |_| ()).await
}

/// [`spawn_udp_proxy`] that shows every reply to `on_reply` first.
async fn spawn_udp_relay(
    upstream: SocketAddr,
    header: impl Fn(SocketAddr) -> Vec<u8> + Send + 'static,
    on_reply: impl Fn(&[u8]) + Send + Sync + 'static,
) -> TestResult<SocketAddr> {
    let on_reply = Arc::new(on_reply);
    let front = Arc::new(UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?);
    let addr = front.local_addr()?;
    tokio::spawn(async move {
        let mut upstreams = std::collections::HashMap::<SocketAddr, Arc<UdpSocket>>::new();
        let mut buf = vec![0; 65_536];
        while let Ok((len, client)) = front.recv_from(&mut buf).await {
            let socket = if let Some(socket) = upstreams.get(&client) {
                Arc::clone(socket)
            } else {
                let Ok(socket) = UdpSocket::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await
                else {
                    continue;
                };
                if socket.connect(upstream).await.is_err() {
                    continue;
                }
                let socket = Arc::new(socket);
                let (replies, front) = (Arc::clone(&socket), Arc::clone(&front));
                let on_reply = Arc::clone(&on_reply);
                tokio::spawn(async move {
                    let mut buf = vec![0; 65_536];
                    while let Ok(len) = replies.recv(&mut buf).await {
                        let Some(reply) = buf.get(..len) else { break };
                        on_reply(reply);
                        let _ = front.send_to(reply, client).await;
                    }
                });
                upstreams.insert(client, Arc::clone(&socket));
                socket
            };
            let mut datagram = header(client);
            datagram.extend_from_slice(buf.get(..len).unwrap_or_default());
            let _ = socket.send(&datagram).await;
        }
    });
    Ok(addr)
}

/// The version 2 PROXY header of a datagram over UDP from 203.0.113.7, on the port of `client`.
fn proxy_v2_udp_header(client: SocketAddr) -> Vec<u8> {
    let mut header = b"\r\n\r\n\0\r\nQUIT\n\x21\x12\x00\x0c".to_vec();
    header.extend_from_slice(&[203, 0, 113, 7, 127, 0, 0, 1]);
    header.extend_from_slice(&client.port().to_be_bytes());
    header.extend_from_slice(&443_u16.to_be_bytes());
    header
}

/// The client address a native QUIC call through `addr` reports.
async fn native_remote_addr(server: &RunningServer, addr: SocketAddr) -> TestResult<String> {
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let config = ChannelConfig::new();
    endpoint
        .set_default_client_config(config.quic_config(client_tls(&server.cert, trevrpc::ALPN)?)?);
    let channel = within!(
        Duration::from_secs(1),
        Channel::connect_with_config(endpoint, addr, "localhost", config),
    )??;
    let reply = greeter::GreeterClient::new(channel.clone())
        .say_hello(&greeter::HelloRequest::default())
        .await;
    channel.close();
    Ok(reply?.message)
}

/// Posts a unary `SayHello` over HTTP/3 to `addr` and returns the response status.
async fn http3_status(server: &RunningServer, addr: SocketAddr) -> TestResult<http::StatusCode> {
    let tls = client_tls(&server.cert, trevrpc::HTTP3_ALPN)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
    )));
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let (mut driver, mut sender) = h3::client::builder()
        .build::<_, _, Bytes>(h3_quinn::Connection::new(connection))
        .await?;
    tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });
    let request = http::Request::post("https://localhost/trevrpc")
        .header(http::header::CONTENT_TYPE, "application/trevrpc")
        .body(())?;
    let mut stream = sender.send_request(request).await?;
    let mut frames = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &trevrpc::RpcRequest::new(greeter::greeter_paths::SAY_HELLO),
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;
    frames.extend(trevrpc::framing::encode_message_frame(
        &[],
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?);
    stream.send_data(Bytes::from(frames)).await?;
    stream.finish().await?;
    let response = within!(TEST_TIMEOUT, stream.recv_response())??;
    Ok(response.status())
}

#[tokio::test]
async fn trusted_udp_proxies_name_the_client() -> TestResult {
    let server = spawn_server(&[], &["127.0.0.0/8"]).await?;
    let proxy = spawn_udp_proxy(server.udp, proxy_v2_udp_header).await?;
    let reported: SocketAddr = native_remote_addr(&server, proxy).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([203, 0, 113, 7]));
    ensure_eq!(server.authorized(), [Some(reported)]);
    // HTTP admission admits only that client.
    ensure_eq!(http3_status(&server, proxy).await?, http::StatusCode::OK);

    // Without the proxy, the trusted address's datagrams lack a header and are dropped.
    ensure!(native_remote_addr(&server, server.udp).await.is_err());
    // The TCP listener does not trust UDP proxies.
    let reported: SocketAddr = qmux_remote_addr(&server, server.tcp).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
    server.stop().await
}

#[tokio::test]
async fn udp_proxy_local_headers_leave_the_proxy_address() -> TestResult {
    let server = spawn_server(&[], &["127.0.0.1/32"]).await?;
    let proxy = spawn_udp_proxy(server.udp, |_| {
        b"\r\n\r\n\0\r\nQUIT\n\x20\x00\x00\x00".to_vec()
    })
    .await?;
    let reported: SocketAddr = native_remote_addr(&server, proxy).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
    ensure_eq!(
        http3_status(&server, proxy).await?,
        http::StatusCode::FORBIDDEN
    );
    server.stop().await
}

#[tokio::test]
async fn untrusted_udp_peers_are_not_read_for_a_header() -> TestResult {
    let server = spawn_server(&[], &["192.0.2.0/24"]).await?;
    let reported: SocketAddr = native_remote_addr(&server, server.udp).await?.parse()?;
    ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
    // The header of an untrusted peer reaches QUIC, which cannot read the datagram.
    let proxy = spawn_udp_proxy(server.udp, proxy_v2_udp_header).await?;
    ensure!(native_remote_addr(&server, proxy).await.is_err());
    server.stop().await
}

#[tokio::test]
async fn udp_proxy_clients_are_looked_up_at_the_handshake() -> TestResult {
    let server = spawn_server(&[], &["127.0.0.0/8"]).await?;
    let forged = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let proxy = spawn_udp_proxy(server.udp, {
        let forged = Arc::clone(&forged);
        move |client| {
            let mut header = proxy_v2_udp_header(client);
            if forged.load(std::sync::atomic::Ordering::Relaxed) {
                header.splice(16..20, [198, 51, 100, 66]);
            }
            header
        }
    })
    .await?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let config = ChannelConfig::new();
    endpoint
        .set_default_client_config(config.quic_config(client_tls(&server.cert, trevrpc::ALPN)?)?);
    let channel = within!(
        TEST_TIMEOUT,
        Channel::connect_with_config(endpoint, proxy, "localhost", config),
    )??;
    let client = greeter::GreeterClient::new(channel.clone());
    let before: SocketAddr = client
        .say_hello(&greeter::HelloRequest::default())
        .await?
        .message
        .parse()?;
    ensure_eq!(before.ip(), IpAddr::from([203, 0, 113, 7]));
    // Headers that name another client later do not move the connection.
    forged.store(true, std::sync::atomic::Ordering::Relaxed);
    let after: SocketAddr = client
        .say_hello(&greeter::HelloRequest::default())
        .await?
        .message
        .parse()?;
    ensure_eq!(after, before);
    channel.close();
    server.stop().await
}

/// Whether `datagram` is a QUIC version 1 Retry packet.
fn is_retry(datagram: &[u8]) -> bool {
    matches!(datagram, [first, 0, 0, 0, 1, ..] if first & 0xb0 == 0xb0)
}

#[tokio::test]
async fn address_validation_sends_retry() -> TestResult {
    for (validation, retries_expected) in [
        (AddressValidation::Off, false),
        (AddressValidation::Always, true),
        // The first connection fits the burst; the second does not.
        (
            AddressValidation::AboveRate(std::num::NonZeroU32::MIN),
            true,
        ),
    ] {
        let server = spawn_server_validating(&[], &[], validation).await?;
        let retries = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let relay = spawn_udp_relay(server.udp, |_| Vec::new(), {
            let retries = Arc::clone(&retries);
            move |reply| {
                if is_retry(reply) {
                    retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        })
        .await?;
        for _ in 0..2 {
            let reported: SocketAddr = native_remote_addr(&server, relay).await?.parse()?;
            ensure_eq!(reported.ip(), IpAddr::from([127, 0, 0, 1]));
        }
        ensure_eq!(
            retries.load(std::sync::atomic::Ordering::Relaxed) > 0,
            retries_expected,
            "{validation:?}"
        );
        server.stop().await?;
    }
    Ok(())
}

#[test]
fn contexts_built_outside_a_connection_have_no_address() {
    let context = RequestContext::new(&trevrpc::RpcRequest::new("/a.B/C"), None);
    assert_eq!(context.remote_addr(), None);
    let addr: SocketAddr = CLIENT.parse().unwrap();
    assert_eq!(context.with_remote_addr(addr).remote_addr(), Some(addr));
}
