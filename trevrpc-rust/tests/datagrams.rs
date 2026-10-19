//! Unreliable messages in datagrams: native QUIC, `WebTransport`, and the transports without
//! datagrams.

use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::client::{ChannelConfig, QmuxTransport, QuicTransport, StreamingRpcTransport};
use trevrpc::server::{RouteOptions, Server, ServerOptions};
use trevrpc::tcp::TcpFallback;
use trevrpc::{Metadata, ResponseEnvelope, ResponseFrame, RpcKind, RpcRequest, Status, Unreliable};

#[macro_use]
mod support;

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Reads every request, then echoes them all; its trailers count the requests and how many of them
/// arrived as datagrams.
const ECHO_ALL: &str = "/dgram.Echo/All";
/// Echoes each request as it arrives, so request and response datagrams overlap.
const ECHO_LIVE: &str = "/dgram.Echo/Live";
/// Like [`ECHO_ALL`], but the route does not mark its responses unreliable.
const ECHO_ALL_RELIABLE_RESPONSES: &str = "/dgram.Echo/AllReliable";
/// Never reads its requests; waits for the call to end.
const HOLD: &str = "/dgram.Echo/Hold";

fn counted_trailers(received: usize, datagrams: u64) -> Metadata {
    Metadata::from([
        ("received".to_owned(), received.to_string().into_bytes()),
        ("datagrams".to_owned(), datagrams.to_string().into_bytes()),
    ])
}

fn register_routes(server: &mut Server) {
    for (path, options) in [
        (
            ECHO_ALL,
            RouteOptions::new().with_unreliable(Unreliable::BOTH),
        ),
        (ECHO_ALL_RELIABLE_RESPONSES, RouteOptions::new()),
    ] {
        server.route_streaming_envelope_with_options(
            path,
            RpcKind::BidirectionalStreaming,
            options,
            |context, _body, mut requests| async move {
                let mut messages = Vec::new();
                while let Some(message) = requests.next().await {
                    messages.push(message?);
                }
                let trailers =
                    counted_trailers(messages.len(), context.datagram_requests_received());
                Ok(ResponseEnvelope::new(trevrpc::stream::from_iter(messages))
                    .with_metadata(trailers))
            },
        );
    }
    server.route_streaming_envelope_with_options(
        ECHO_LIVE,
        RpcKind::BidirectionalStreaming,
        RouteOptions::new().with_unreliable(Unreliable::BOTH),
        |_context, _body, requests| async move {
            let echoes: trevrpc::BoxStream<Vec<u8>> = Box::pin(requests);
            Ok(ResponseEnvelope::new(echoes))
        },
    );
    server.route_streaming_envelope_with_options(
        HOLD,
        RpcKind::BidirectionalStreaming,
        RouteOptions::new().with_unreliable(Unreliable::BOTH),
        |context, _body, requests| async move {
            // Dropping the request stream would stop the call's request side.
            let _requests = requests;
            context.cancelled_signal().await;
            Err(Status::cancelled("held call ended").into())
        },
    );
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
    register_routes(&mut server);
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

fn server_options() -> ServerOptions {
    ServerOptions::new()
        .with_webtransport_enabled(true)
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(500)))
}

fn client_tls(cert: &CertificateDer<'static>) -> TestResult<quinn::rustls::ClientConfig> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.clone())?;
    Ok(quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

async fn connect_quic(server: &RunningServer) -> TestResult<(quinn::Endpoint, quinn::Connection)> {
    let config = ChannelConfig::new().quic_config(client_tls(&server.cert)?)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config);
    let connection = endpoint.connect(server.udp, "localhost")?.await?;
    Ok((endpoint, connection))
}

async fn connect_qmux(server: &RunningServer) -> TestResult<QmuxTransport> {
    Ok(QmuxTransport::connect(server.tcp, "localhost", client_tls(&server.cert)?).await?)
}

/// What a bidirectional call received.
struct Reply {
    messages: Vec<Vec<u8>>,
    trailers: Metadata,
}

impl Reply {
    fn trailer(&self, key: &str) -> TestResult<u64> {
        let value = self
            .trailers
            .get(key)
            .ok_or_else(|| format!("missing trailer {key}"))?;
        Ok(std::str::from_utf8(value)?.parse()?)
    }
}

/// Runs a bidirectional call through `transport`: sends `requests` without waiting for echoes,
/// then reads every frame through the STATUS.
async fn run_call<T: StreamingRpcTransport>(
    transport: &T,
    path: &str,
    unreliable: Unreliable,
    requests: Vec<Vec<u8>>,
) -> TestResult<Reply> {
    let request = RpcRequest::new(path).with_kind(RpcKind::BidirectionalStreaming);
    let mut frames = transport
        .streaming_call_unreliable(request, trevrpc::stream::from_iter(requests), unreliable)
        .await?;
    let mut messages = Vec::new();
    loop {
        let frame = tokio::time::timeout(TEST_TIMEOUT, frames.next())
            .await?
            .ok_or("response ended without a STATUS")??;
        match frame {
            ResponseFrame::Headers(_) => {}
            ResponseFrame::Message(body) => messages.push(body.to_vec()),
            ResponseFrame::Status(status) => {
                ensure_eq!(status.code(), trevrpc::Code::Ok, "{}", status.message());
                return Ok(Reply {
                    messages,
                    trailers: status.metadata().clone(),
                });
            }
        }
    }
}

fn numbered(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|index| index.to_string().into_bytes())
        .collect()
}

/// Checks that every delivered message was sent and none arrived twice, which is all unreliable
/// messages promise, and that at least one arrived.
fn ensure_delivered_at_most_once(sent: &[Vec<u8>], delivered: &[Vec<u8>]) -> TestResult {
    let mut unseen: Vec<&Vec<u8>> = sent.iter().collect();
    for message in delivered {
        let position = unseen
            .iter()
            .position(|candidate| *candidate == message)
            .ok_or_else(|| format!("{message:?} was not sent or arrived twice"))?;
        unseen.swap_remove(position);
    }
    ensure!(!delivered.is_empty(), "nothing was delivered");
    Ok(())
}

#[tokio::test]
async fn native_quic_carries_unreliable_messages_in_datagrams_both_ways() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let transport = QuicTransport::new(connection.clone());

    let sent = numbered(40);
    let reply = run_call(&transport, ECHO_ALL, Unreliable::BOTH, sent.clone()).await?;

    // Datagram messages may be lost or reordered, but never repeated.
    ensure_delivered_at_most_once(&sent, &reply.messages)?;
    ensure!((1..=40).contains(&reply.trailer("received")?));
    ensure!(
        reply.trailer("datagrams")? > 0,
        "no request arrived as a datagram"
    );
    let stats = connection.stats();
    ensure!(
        stats.frame_tx.datagram > 0,
        "the client sent no DATAGRAM frame"
    );
    ensure!(
        stats.frame_rx.datagram > 0,
        "the client received no DATAGRAM frame"
    );
    // The messages that were ready together shared datagrams, on both sides.
    ensure!(
        stats.frame_tx.datagram < 40 && stats.frame_rx.datagram < 40,
        "{} messages sent in {} datagrams, {} received in {}",
        40,
        stats.frame_tx.datagram,
        reply.messages.len(),
        stats.frame_rx.datagram
    );

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn messages_too_large_for_a_datagram_travel_on_the_stream() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let transport = QuicTransport::new(connection.clone());

    let max = connection
        .max_datagram_size()
        .ok_or("the server does not accept datagrams")?;
    let large = vec![b'x'; max.saturating_mul(3)];
    let reply = run_call(&transport, ECHO_ALL, Unreliable::BOTH, vec![large.clone()]).await?;

    ensure_eq!(reply.messages, vec![large]);
    ensure_eq!(reply.trailer("received")?, 1);
    ensure_eq!(reply.trailer("datagrams")?, 0);
    ensure_eq!(connection.stats().frame_rx.datagram, 0);

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn a_reliable_call_uses_no_datagrams_even_on_a_route_that_marks_responses() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let transport = QuicTransport::new(connection.clone());

    let sent = numbered(10);
    let reply = run_call(&transport, ECHO_ALL, Unreliable::NONE, sent.clone()).await?;

    ensure_eq!(reply.messages, sent);
    ensure_eq!(reply.trailer("datagrams")?, 0);
    ensure_eq!(connection.stats().frame_rx.datagram, 0);
    ensure_eq!(connection.stats().frame_tx.datagram, 0);

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn qmux_has_no_datagrams_and_keeps_every_message_on_the_stream() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let transport = connect_qmux(&server).await?;

    let sent = numbered(20);
    let reply = run_call(&transport, ECHO_ALL, Unreliable::BOTH, sent.clone()).await?;

    ensure_eq!(reply.messages, sent);
    ensure_eq!(reply.trailer("received")?, 20);
    ensure_eq!(reply.trailer("datagrams")?, 0);

    transport.close();
    server.stop().await
}

/// A raw native client call: a REQUEST that names a datagram flow, written to a bidirectional
/// stream by hand.
struct RawCall {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
}

async fn open_raw(connection: &quinn::Connection, path: &str, flow: u64) -> TestResult<RawCall> {
    let (mut send, recv) = connection.open_bi().await?;
    let request = RpcRequest::new(path)
        .with_kind(RpcKind::BidirectionalStreaming)
        .with_datagram_flow(flow);
    support::write_request(&mut send, &request).await?;
    Ok(RawCall { send, recv })
}

/// The datagram that carries `message` for `flow` in one MESSAGE frame. The flow and the message
/// length must each fit one byte of a variable-length integer.
fn datagram(flow: u8, message: &[u8]) -> Bytes {
    let mut datagram = vec![flow, 0, u8::try_from(message.len()).unwrap_or(63)];
    datagram.extend_from_slice(message);
    Bytes::from(datagram)
}

/// Splits a datagram of one-byte flow and lengths into its flow and the payloads of its MESSAGE
/// frames, skipping every other frame.
fn unframe(datagram: &[u8]) -> TestResult<(u8, Vec<Vec<u8>>)> {
    let (&flow, mut rest) = datagram.split_first().ok_or("an empty datagram")?;
    let mut messages = Vec::new();
    while let [frame_type, len, tail @ ..] = rest {
        let len = usize::from(*len);
        let payload = tail.get(..len).ok_or("a truncated frame")?;
        if *frame_type == 0 {
            messages.push(payload.to_vec());
        }
        rest = tail.get(len..).ok_or("a truncated frame")?;
    }
    ensure!(rest.is_empty(), "bytes after the last frame");
    Ok((flow, messages))
}

async fn read_frame(recv: &mut quinn::RecvStream) -> TestResult<ResponseFrame> {
    tokio::time::timeout(TEST_TIMEOUT, support::read_response_frame(recv))
        .await?
        .map_err(|error| format!("reading a frame failed: {error}"))?
        .ok_or_else(|| "the response ended early".into())
}

#[tokio::test]
async fn a_handler_that_sends_only_datagrams_still_gets_its_headers_flushed() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let mut call = open_raw(&connection, ECHO_ALL, 1).await?;
    for part in ["a", "b", "c"] {
        support::write_message(&mut call.send, part.as_bytes()).await?;
    }
    call.send.finish()?;

    // The handler returned no headers, but it sends datagrams, so empty HEADERS come first.
    ensure_eq!(
        read_frame(&mut call.recv).await?,
        ResponseFrame::Headers(Metadata::new())
    );
    let ResponseFrame::Status(status) = read_frame(&mut call.recv).await? else {
        return Err("expected only a STATUS after the headers".into());
    };
    ensure_eq!(status.code(), trevrpc::Code::Ok);

    // The responses came as datagrams of flow 1.
    let mut echoed = Vec::new();
    while let Ok(datagram) =
        tokio::time::timeout(Duration::from_millis(500), connection.read_datagram()).await
    {
        let datagram = datagram?;
        let (flow, messages) = unframe(&datagram)?;
        ensure_eq!(flow, 1);
        echoed.extend(messages);
    }
    let sent = [b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
    ensure_delivered_at_most_once(&sent, &echoed)?;

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn datagrams_that_overtake_their_request_are_held_and_then_claimed() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;

    connection.send_datagram(datagram(7, b"early"))?;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut call = open_raw(&connection, ECHO_ALL_RELIABLE_RESPONSES, 7).await?;
    support::write_message(&mut call.send, b"stream").await?;
    call.send.finish()?;

    let reply = support::read_unary_reply(&mut call.recv).await?;
    ensure_eq!(reply.status.code(), trevrpc::Code::Ok);
    let mut echoed = reply.messages;
    echoed.sort();
    ensure_eq!(echoed, [b"early".to_vec(), b"stream".to_vec()]);
    ensure_eq!(
        reply.status.metadata().get("datagrams"),
        Some(&b"1".to_vec())
    );

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn a_datagram_carries_every_message_frame_in_it_and_a_malformed_one_none() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let mut call = open_raw(&connection, ECHO_ALL_RELIABLE_RESPONSES, 5).await?;

    // Two messages around an unknown frame, then a good message ahead of a truncated frame.
    connection.send_datagram(Bytes::from_static(&[
        5, 0x21, 1, 0xff, 0, 1, b'a', 0, 1, b'b',
    ]))?;
    connection.send_datagram(Bytes::from_static(&[5, 0, 1, b'c', 0, 9, b'd']))?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    call.send.finish()?;

    let reply = support::read_unary_reply(&mut call.recv).await?;
    ensure_eq!(reply.status.code(), trevrpc::Code::Ok);
    ensure_eq!(reply.messages, [b"a".to_vec(), b"b".to_vec()]);
    ensure_eq!(
        reply.status.metadata().get("datagrams"),
        Some(&b"2".to_vec())
    );

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn held_datagrams_that_wait_too_long_for_their_request_are_dropped() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;

    connection.send_datagram(datagram(7, b"stale"))?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut call = open_raw(&connection, ECHO_ALL_RELIABLE_RESPONSES, 7).await?;
    call.send.finish()?;

    let reply = support::read_unary_reply(&mut call.recv).await?;
    ensure_eq!(reply.status.code(), trevrpc::Code::Ok);
    ensure!(reply.messages.is_empty(), "{:?}", reply.messages);

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn a_request_for_a_live_flow_fails_with_invalid_argument() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (endpoint, connection) = connect_quic(&server).await?;

    let mut first = open_raw(&connection, HOLD, 3).await?;
    // Give the server time to register the first call before the second names its flow.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut second = open_raw(&connection, HOLD, 3).await?;
    let status =
        tokio::time::timeout(TEST_TIMEOUT, support::read_lone_status(&mut second.recv)).await??;
    ensure_eq!(status.code(), trevrpc::Code::InvalidArgument);
    ensure!(
        status.message().contains("already in use"),
        "{}",
        status.message()
    );

    // The first call is untouched.
    ensure!(
        tokio::time::timeout(
            Duration::from_millis(200),
            support::read_response_frame(&mut first.recv)
        )
        .await
        .is_err(),
        "the first call ended"
    );

    // Once the first call ends, its flow is free again.
    first.send.reset(1_u32.into())?;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let mut third = open_raw(&connection, HOLD, 3).await?;
    if let Ok(frame) = tokio::time::timeout(
        Duration::from_millis(300),
        support::read_response_frame(&mut third.recv),
    )
    .await
    {
        return Err(format!("the reused flow was rejected: {frame:?}").into());
    }

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn a_nonzero_flow_is_ignored_where_there_are_no_datagrams() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let transport = connect_qmux(&server).await?;

    // Even a client that names a flow gets an ordinary call over QMux, twice, so the flow does
    // not count as live.
    for _ in 0..2 {
        let request = RpcRequest::new(ECHO_ALL)
            .with_kind(RpcKind::BidirectionalStreaming)
            .with_datagram_flow(9);
        let mut frames = transport
            .streaming_call(request, trevrpc::stream::from_iter(vec![b"x".to_vec()]))
            .await?;
        let mut messages = Vec::new();
        while let Some(frame) = frames.next().await.transpose()? {
            if let ResponseFrame::Message(body) = frame {
                messages.push(body.to_vec());
            }
        }
        ensure_eq!(messages, [b"x".to_vec()]);
    }

    transport.close();
    server.stop().await
}

#[tokio::test]
async fn request_datagrams_count_toward_the_message_limit() -> TestResult {
    let server = spawn_server(server_options().with_max_stream_messages(Some(5))).await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let transport = QuicTransport::new(connection.clone());

    // Ten messages, all small enough for datagrams.
    let request = RpcRequest::new(ECHO_ALL).with_kind(RpcKind::BidirectionalStreaming);
    let mut frames = transport
        .streaming_call_unreliable(
            request,
            trevrpc::stream::from_iter(numbered(10)),
            Unreliable::REQUESTS,
        )
        .await?;
    let status = loop {
        match tokio::time::timeout(TEST_TIMEOUT, frames.next()).await? {
            Some(Ok(ResponseFrame::Status(status))) => break status,
            Some(Ok(_)) => {}
            Some(Err(error)) => break error.into_status(),
            None => return Err("the response ended without a STATUS".into()),
        }
    };
    ensure_eq!(status.code(), trevrpc::Code::ResourceExhausted);
    ensure!(
        connection.stats().frame_tx.datagram > 0,
        "no datagram was sent"
    );

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

#[tokio::test]
async fn datagram_only_traffic_keeps_the_idle_timer_alive() -> TestResult {
    let server =
        spawn_server(server_options().with_stream_idle_timeout(Some(Duration::from_millis(400))))
            .await?;
    let (endpoint, connection) = connect_quic(&server).await?;
    let mut call = open_raw(&connection, ECHO_ALL, 1).await?;

    // A datagram every 150 ms for a second: far longer than the 400 ms idle timeout, with
    // nothing at all on the request stream.
    for _ in 0..7 {
        connection.send_datagram(datagram(1, b"tick"))?;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    call.send.finish()?;

    let reply = loop {
        match read_frame(&mut call.recv).await? {
            ResponseFrame::Status(status) => break status,
            ResponseFrame::Headers(_) | ResponseFrame::Message(_) => {}
        }
    };
    ensure_eq!(reply.code(), trevrpc::Code::Ok, "{}", reply.message());
    ensure_eq!(reply.metadata().get("received"), Some(&b"7".to_vec()));

    connection.close(0_u32.into(), b"done");
    endpoint.wait_idle().await;
    server.stop().await
}

async fn connect_webtransport(
    server: &RunningServer,
) -> TestResult<(web_transport_quinn::Client, web_transport_quinn::Session)> {
    let client = web_transport_quinn::ClientBuilder::new()
        .with_server_certificates(vec![server.cert.clone()])?;
    let url: url::Url = format!("https://127.0.0.1:{}/trevrpc", server.udp.port()).parse()?;
    let session = client
        .connect(web_transport_quinn::proto::ConnectRequest::new(url))
        .await?;
    Ok((client, session))
}

#[tokio::test]
async fn webtransport_carries_unreliable_messages_in_session_datagrams_both_ways() -> TestResult {
    let server = spawn_server(server_options()).await?;
    let (_client, session) = connect_webtransport(&server).await?;

    let (mut send, mut recv) = session.open_bi().await?;
    let request = RpcRequest::new(ECHO_ALL)
        .with_kind(RpcKind::BidirectionalStreaming)
        .with_datagram_flow(1);
    support::write_request(&mut send, &request).await?;
    // Ten messages as session datagrams, which the library prefixes with the session's ID.
    for index in 0..10_u8 {
        session.send_datagram(datagram(1, &[b'a' + index]))?;
    }
    // Wait until the server has them, then end the request.
    tokio::time::sleep(Duration::from_millis(200)).await;
    send.finish()?;

    let mut status = None;
    while status.is_none() {
        let frame = tokio::time::timeout(TEST_TIMEOUT, support::read_response_frame(&mut recv))
            .await??
            .ok_or("the response ended early")?;
        if let ResponseFrame::Status(received) = frame {
            status = Some(received);
        }
    }
    let status = status.ok_or("no STATUS")?;
    ensure_eq!(status.code(), trevrpc::Code::Ok, "{}", status.message());
    ensure_eq!(status.metadata().get("received"), Some(&b"10".to_vec()));
    // The server read the request datagrams from the session.
    ensure!(
        status.metadata().get("datagrams") != Some(&b"0".to_vec()),
        "no request arrived as a datagram"
    );

    // The responses came back as session datagrams.
    let mut echoed = Vec::new();
    while let Ok(received) =
        tokio::time::timeout(Duration::from_millis(500), session.read_datagram()).await
    {
        let received = received?;
        let (flow, messages) = unframe(&received)?;
        ensure_eq!(flow, 1);
        echoed.extend(messages);
    }
    let expected: Vec<Vec<u8>> = (0..10_u8).map(|index| vec![b'a' + index]).collect();
    ensure_delivered_at_most_once(&expected, &echoed)?;

    session.close(0, b"done");
    server.stop().await
}
