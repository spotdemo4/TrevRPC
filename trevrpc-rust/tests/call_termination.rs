//! How a call ends: the server gives back its stream slots before the client can see the call
//! finish, and a client upload fails promptly once the server has ended the call.
//!
//! Every test runs over native QUIC and over `QMux` over TLS, served by one `serve_with_tcp`.

use std::error::Error;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use trevrpc::client::{
    CallOptions, ChannelConfig, QmuxTransport, QuicTransport, StreamingRpcTransport,
};
use trevrpc::server::{RequestContext, Server, ServerOptions};
use trevrpc::tcp::TcpFallback;
use trevrpc::{Code, RpcKind, Status};

#[allow(dead_code)]
#[path = "../examples/shared/greeter.rs"]
mod greeter;
#[macro_use]
mod support;

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a send may take to fail once the server has ended the call.
const SEND_FAILURE_BOUND: Duration = Duration::from_secs(2);
/// A deadline far beyond the test, so only the server ending the call can stop an upload.
const LONG_DEADLINE: Duration = Duration::from_secs(30);

const SLOT_LIMIT: usize = 4;
const CALLS_PER_WORKER: usize = 400;
const MESSAGE_LIMIT: usize = 3;

struct Greeter;

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(greeter::HelloReply {
            message: request.name,
        }))
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(trevrpc::stream::from_iter(
            [greeter::HelloReply {
                message: request.name,
            }],
        )))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        mut requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::ResponseEnvelope<greeter::HelloReply>, Status> {
        while let Some(request) = requests.next().await {
            request?;
        }
        Ok(trevrpc::ResponseEnvelope::new(
            greeter::HelloReply::default(),
        ))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<greeter::HelloReply>>, Status> {
        Ok(trevrpc::ResponseEnvelope::new(Box::pin(requests.map(
            |request| {
                request.map(|request| greeter::HelloReply {
                    message: request.name,
                })
            },
        ))))
    }
}

/// Method of a server-streaming route that sends one reply and then never ends.
const HOLD: &str = "Hold";

/// Method of a route that ends a call with `PermissionDenied` without reading its requests.
const REJECT: &str = "Reject";

fn register_routes(server: &mut Server) {
    server.route_streaming(
        support::greeter_path(HOLD),
        RpcKind::ServerStreaming,
        |_body, _requests| async {
            let reply = prost::Message::encode_to_vec(&greeter::HelloReply::default());
            let replies: trevrpc::BoxStream<Vec<u8>> = Box::pin(
                trevrpc::stream::from_iter([reply]).chain(futures_util::stream::pending()),
            );
            Ok(replies)
        },
    );
    for (method, kind) in [
        (REJECT, RpcKind::ClientStreaming),
        ("RejectBidi", RpcKind::BidirectionalStreaming),
    ] {
        server.route_streaming(
            support::greeter_path(method),
            kind,
            |_body, _requests| async {
                Err(trevrpc::Error::from(Status::new(
                    Code::PermissionDenied,
                    "rejected",
                )))
            },
        );
    }
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

async fn connect_quic(server: &RunningServer) -> TestResult<(quinn::Endpoint, QuicTransport)> {
    connect_quic_to(server.udp, &server.cert).await
}

async fn connect_quic_to(
    addr: SocketAddr,
    cert: &CertificateDer<'static>,
) -> TestResult<(quinn::Endpoint, QuicTransport)> {
    let config = ChannelConfig::new().quic_config(client_tls(cert, trevrpc::ALPN)?)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config);
    let connection = endpoint.connect(addr, "localhost")?.await?;
    Ok((endpoint, QuicTransport::new(connection)))
}

async fn connect_qmux(server: &RunningServer) -> TestResult<QmuxTransport> {
    Ok(QmuxTransport::connect(
        server.tcp,
        "localhost",
        client_tls(&server.cert, trevrpc::QMUX_ALPN)?,
    )
    .await?)
}

#[derive(Clone, Copy)]
enum Wire {
    Quic,
    Qmux,
}

/// Runs `scenario` against a fresh server over `wire`.
async fn run<F, Fut>(wire: Wire, options: ServerOptions, scenario: F) -> TestResult
where
    F: FnOnce(Arc<dyn StreamingRpcTransport>) -> Fut,
    Fut: Future<Output = TestResult>,
{
    let server = spawn_server(options).await?;
    match wire {
        Wire::Quic => {
            let (endpoint, quic) = connect_quic(&server).await?;
            tokio::time::timeout(TEST_TIMEOUT, scenario(Arc::new(quic.clone())))
                .await
                .map_err(|_| "scenario timed out")??;
            quic.connection().close(0_u32.into(), b"done");
            endpoint.wait_idle().await;
        }
        Wire::Qmux => {
            let qmux = connect_qmux(&server).await?;
            tokio::time::timeout(TEST_TIMEOUT, scenario(Arc::new(qmux.clone())))
                .await
                .map_err(|_| "scenario timed out")??;
            qmux.close();
        }
    }
    server.stop().await
}

/// Defines `$name::quic` and `$name::qmux`, which run the same scenario over each transport.
///
/// The `QMux` test runs on a single thread: `qmux` 0.6.3 sends its `MAX_STREAMS` updates after
/// computing them without holding a lock, so two streams closing on different threads can send
/// them out of order, and the peer then ends the session with a flow control error
/// (<https://github.com/moq-dev/web-transport/issues/439>). That is independent of what these
/// tests check.
macro_rules! over_each_transport {
    ($name:ident, $options:expr, $scenario:expr) => {
        mod $name {
            use super::*;

            #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
            async fn quic() -> TestResult {
                run(Wire::Quic, $options, $scenario).await
            }

            #[tokio::test(flavor = "current_thread")]
            async fn qmux() -> TestResult {
                run(Wire::Qmux, $options, $scenario).await
            }
        }
    };
}

fn name(value: usize) -> greeter::HelloRequest {
    greeter::HelloRequest {
        name: value.to_string(),
    }
}

/// Runs `SLOT_LIMIT` workers, each making `CALLS_PER_WORKER` calls back to back, so the server
/// never has more than `SLOT_LIMIT` calls in flight on the connection by the client's account.
async fn hammer<F, Fut>(call: F) -> TestResult
where
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: Future<Output = Result<(), trevrpc::Error>> + Send + 'static,
{
    let mut workers = JoinSet::new();
    for _ in 0..SLOT_LIMIT {
        let call = call.clone();
        workers.spawn(async move {
            for index in 0..CALLS_PER_WORKER {
                call(index).await?;
            }
            Ok::<_, trevrpc::Error>(())
        });
    }
    while let Some(result) = workers.join_next().await {
        result?.map_err(|error| format!("a call within the stream limit failed: {error}"))?;
    }
    Ok(())
}

fn slot_limited_options() -> ServerOptions {
    ServerOptions::new()
        .with_max_concurrent_streams_per_connection(Some(SLOT_LIMIT))
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

over_each_transport!(
    stream_slot_is_free_before_a_unary_call_completes,
    slot_limited_options(),
    |transport| async move {
        hammer(move |index| {
            let transport = Arc::clone(&transport);
            async move {
                let client = greeter::GreeterClient::new(transport);
                client.say_hello(name(index)).await.map(drop)
            }
        })
        .await
    }
);

over_each_transport!(
    stream_slot_is_free_before_a_server_streaming_call_completes,
    slot_limited_options(),
    |transport| async move {
        hammer(move |index| {
            let transport = Arc::clone(&transport);
            async move {
                let client = greeter::GreeterClient::new(transport);
                let mut replies = client.lots_of_replies(name(index)).await?;
                while let Some(reply) = replies.next().await {
                    reply?;
                }
                Ok(())
            }
        })
        .await
    }
);

/// The server-wide request limit is shared by every connection; one connection is enough.
fn request_limited_options() -> ServerOptions {
    ServerOptions::new()
        .with_max_concurrent_requests(Some(SLOT_LIMIT))
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

over_each_transport!(
    request_slot_is_free_before_a_call_completes,
    request_limited_options(),
    |transport| async move {
        hammer(move |index| {
            let transport = Arc::clone(&transport);
            async move {
                let client = greeter::GreeterClient::new(transport);
                client.say_hello(name(index)).await?;
                let mut replies = client.lots_of_replies(name(index)).await?;
                while let Some(reply) = replies.next().await {
                    reply?;
                }
                Ok(())
            }
        })
        .await
    }
);

over_each_transport!(
    over_limit_streams_still_receive_unavailable,
    slot_limited_options().with_max_concurrent_streams_per_connection(Some(1)),
    |transport| async move {
        let client = greeter::GreeterClient::new(Arc::clone(&transport));
        let mut held = trevrpc::client::server_streaming::<_, _, greeter::HelloReply>(
            transport.as_ref(),
            &support::greeter_path(HOLD),
            &name(0),
            long_options(),
        )
        .await?;
        ensure!(held.next().await.is_some());
        // One call holds the only slot. The transport admits a spare stream, so the second
        // call reaches the server and is answered with a lone UNAVAILABLE status.
        let status = client
            .say_hello(name(1))
            .await
            .err()
            .ok_or("the call above the limit should fail")?
            .into_status();
        ensure_eq!(status.code(), Code::Unavailable);
        drop(held);
        Ok(())
    }
);

fn upload_options() -> ServerOptions {
    ServerOptions::new()
        .with_max_stream_messages(Some(MESSAGE_LIMIT))
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

/// A request message large enough that an upload nobody reads fills every flow-control window.
fn large_message() -> greeter::HelloRequest {
    greeter::HelloRequest {
        name: "x".repeat(32 * 1024),
    }
}

/// Sends until a send fails, and returns that failure. A send that is still succeeding or
/// blocked after `SEND_FAILURE_BOUND` fails the test.
#[allow(clippy::future_not_send)] // Calls borrow response streams, which are not `Sync`.
async fn send_until_failure<F, Fut>(send: F) -> TestResult<trevrpc::Error>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<(), trevrpc::Error>>,
{
    let failure = tokio::time::timeout(SEND_FAILURE_BOUND, async {
        loop {
            if let Err(error) = send().await {
                return error;
            }
        }
    })
    .await;
    failure.map_err(|_| {
        format!("sends did not fail within {SEND_FAILURE_BOUND:?} of the server ending the call")
            .into()
    })
}

fn long_options() -> CallOptions {
    CallOptions::new().with_timeout(LONG_DEADLINE)
}

over_each_transport!(
    client_streaming_upload_fails_when_the_server_limits_messages,
    upload_options(),
    |transport| async move {
        let client = greeter::GreeterClient::new(transport);
        let call = client
            .lots_of_greetings_with_options(long_options())
            .await?;
        send_until_failure(|| call.send(large_message())).await?;
        let status = call
            .close_and_recv()
            .await
            .err()
            .ok_or("call should fail")?;
        ensure_eq!(status.into_status().code(), Code::ResourceExhausted);
        Ok(())
    }
);

over_each_transport!(
    bidi_upload_fails_when_the_server_limits_messages,
    upload_options(),
    |transport| async move {
        let client = greeter::GreeterClient::new(transport);
        let mut call = client.bidi_hello_with_options(long_options()).await?;
        send_until_failure(|| call.send(large_message())).await?;
        let status = loop {
            match call.recv().await {
                Ok(Some(_)) => {}
                Ok(None) => return Err("call should end with a status".into()),
                Err(error) => break error.into_status(),
            }
        };
        ensure_eq!(status.code(), Code::ResourceExhausted);
        Ok(())
    }
);

over_each_transport!(
    client_streaming_upload_fails_when_the_handler_returns_early,
    ServerOptions::new(),
    |transport| async move {
        let call =
            trevrpc::client::client_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
                transport.as_ref(),
                &support::greeter_path(REJECT),
                long_options(),
            )
            .await?;
        send_until_failure(|| call.send(large_message())).await?;
        let error = call
            .close_and_recv()
            .await
            .err()
            .ok_or("call should fail")?;
        ensure_eq!(error.into_status().code(), Code::PermissionDenied);
        Ok(())
    }
);

over_each_transport!(
    bidi_upload_fails_when_the_handler_returns_early,
    ServerOptions::new(),
    |transport| async move {
        let mut call = trevrpc::client::bidirectional_streaming::<
            _,
            greeter::HelloRequest,
            greeter::HelloReply,
        >(
            transport.as_ref(),
            &support::greeter_path("RejectBidi"),
            long_options(),
        )
        .await?;
        send_until_failure(|| call.send(large_message())).await?;
        let error = call.recv().await.err().ok_or("call should fail")?;
        ensure_eq!(error.into_status().code(), Code::PermissionDenied);
        Ok(())
    }
);

over_each_transport!(
    bidi_send_fails_while_the_responses_are_being_read,
    upload_options(),
    |transport| async move {
        let client = greeter::GreeterClient::new(transport);
        let call = client.bidi_hello_with_options(long_options()).await?;
        let (sender, mut replies) = call.split();
        let reader = tokio::spawn(async move {
            while let Some(reply) = replies.next().await {
                reply?;
            }
            Ok::<_, trevrpc::Error>(())
        });
        let error = send_until_failure(|| sender.send(large_message())).await?;
        ensure_eq!(error.into_status().code(), Code::ResourceExhausted);
        let status = reader.await?.err().ok_or("call should fail")?.into_status();
        ensure_eq!(status.code(), Code::ResourceExhausted);
        Ok(())
    }
);

/// What a [`DeafServer`] does with the one call it accepts.
struct Script {
    /// Empty MESSAGE frames sent before anything else.
    replies: usize,
    /// Whether to send `STOP_SENDING` for the request side.
    stop_sending: bool,
    /// The terminal STATUS to send and finish with, if any.
    status: Option<Status>,
}

/// A native QUIC server that runs a [`Script`] for its first call and then never reads the request
/// side. Without `stop_sending` that is the behaviour of a server that ends a call without
/// telling the transport, so a client upload to it can only stop because the client acts on the
/// STATUS.
struct DeafServer {
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    _endpoint: quinn::Endpoint,
    _task: JoinHandle<()>,
}

fn spawn_deaf_server(status: Status) -> TestResult<DeafServer> {
    spawn_scripted_server(Script {
        replies: 0,
        stop_sending: false,
        status: Some(status),
    })
}

fn spawn_scripted_server(script: Script) -> TestResult<DeafServer> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;
    let config = Server::new().quic_config(tls)?;
    let endpoint = quinn::Endpoint::server(config, SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = endpoint.local_addr()?;
    let accepting = endpoint.clone();
    let task = tokio::spawn(async move {
        let Some(incoming) = accepting.accept().await else {
            return;
        };
        let Ok(connection) = incoming.await else {
            return;
        };
        let Ok((mut send, mut recv)) = connection.accept_bi().await else {
            return;
        };
        if support::read_request(&mut recv).await.is_err() {
            return;
        }
        let reply = prost::Message::encode_to_vec(&greeter::HelloReply::default());
        for _ in 0..script.replies {
            if support::write_message(&mut send, &reply).await.is_err() {
                return;
            }
        }
        if script.stop_sending && recv.stop(1_u32.into()).is_err() {
            return;
        }
        if let Some(status) = script.status
            && (support::write_status(&mut send, status).await.is_err() || send.finish().is_err())
        {
            return;
        }
        // Hold the stream open and unread until the test ends.
        let _recv = recv;
        std::future::pending::<()>().await;
    });
    Ok(DeafServer {
        addr,
        cert: cert_der,
        _endpoint: endpoint,
        _task: task,
    })
}

fn rejection() -> Status {
    Status::new(Code::PermissionDenied, "deaf server rejected the call")
}

#[tokio::test]
async fn client_streaming_upload_stops_on_a_status_the_server_never_follows_up() -> TestResult {
    let server = spawn_deaf_server(rejection())?;
    let (_endpoint, transport) = connect_quic_to(server.addr, &server.cert).await?;
    let call = trevrpc::client::client_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
        &transport,
        &support::greeter_path("Deaf"),
        long_options(),
    )
    .await?;

    let error = send_until_failure(|| call.send(large_message())).await?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    let error = call
        .close_and_recv()
        .await
        .err()
        .ok_or("call should fail")?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test]
async fn bidi_upload_stops_on_a_status_the_server_never_follows_up() -> TestResult {
    let server = spawn_deaf_server(rejection())?;
    let (_endpoint, transport) = connect_quic_to(server.addr, &server.cert).await?;
    let mut call = trevrpc::client::bidirectional_streaming::<
        _,
        greeter::HelloRequest,
        greeter::HelloReply,
    >(&transport, &support::greeter_path("Deaf"), long_options())
    .await?;

    // Nobody reads the responses while the upload runs.
    let error = send_until_failure(|| call.send(large_message())).await?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    let error = call.recv().await.err().ok_or("call should fail")?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test]
async fn send_reports_an_ok_status_as_cancelled() -> TestResult {
    let server = spawn_deaf_server(Status::ok())?;
    let (_endpoint, transport) = connect_quic_to(server.addr, &server.cert).await?;
    let call = trevrpc::client::client_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
        &transport,
        &support::greeter_path("Deaf"),
        long_options(),
    )
    .await?;

    let error = send_until_failure(|| call.send(large_message())).await?;
    let status = error.into_status();
    ensure_eq!(status.code(), Code::Cancelled);
    ensure!(status.message().contains("completed the call"));
    Ok(())
}

#[tokio::test]
async fn bidi_send_reports_a_status_queued_behind_unread_responses() -> TestResult {
    // Far more responses than the reading task queues for the caller come before the STATUS.
    let server = spawn_scripted_server(Script {
        replies: 20,
        stop_sending: false,
        status: Some(rejection()),
    })?;
    let (_endpoint, transport) = connect_quic_to(server.addr, &server.cert).await?;
    let mut call = trevrpc::client::bidirectional_streaming::<
        _,
        greeter::HelloRequest,
        greeter::HelloReply,
    >(&transport, &support::greeter_path("Deaf"), long_options())
    .await?;

    let error = send_until_failure(|| call.send(large_message())).await?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    // The responses that were queued ahead of the STATUS are still delivered.
    for _ in 0..20 {
        ensure!(call.recv().await?.is_some());
    }
    let error = call.recv().await.err().ok_or("call should fail")?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test]
async fn send_reports_resource_exhausted_when_the_status_is_beyond_the_buffer_bound() -> TestResult
{
    // The server stops reading the request and sends more responses than the call may buffer,
    // and no STATUS.
    let server = spawn_scripted_server(Script {
        replies: 20,
        stop_sending: true,
        status: None,
    })?;
    let (_endpoint, transport) = connect_quic_to(server.addr, &server.cert).await?;
    let call =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            &support::greeter_path("Deaf"),
            long_options().with_max_response_messages(Some(8)),
        )
        .await?;

    let error = send_until_failure(|| call.send(large_message())).await?;
    ensure_eq!(error.into_status().code(), Code::ResourceExhausted);
    Ok(())
}

/// A transport whose upload ends before the response says why.
struct TeardownFirst {
    /// Opened by the test to let the STATUS through.
    gate: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
    /// Signalled once the request body has been dropped.
    dropped: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    /// Ends the response with a lost connection instead of the STATUS.
    connection_lost: bool,
}

#[trevrpc::async_trait]
impl trevrpc::client::RpcTransport for TeardownFirst {
    async fn call(
        &self,
        _request: trevrpc::RpcRequest,
        _body: Vec<u8>,
    ) -> trevrpc::Result<trevrpc::ResponseEnvelope<bytes::Bytes>> {
        Err(Status::unimplemented("streaming only").into())
    }
}

#[trevrpc::async_trait]
impl StreamingRpcTransport for TeardownFirst {
    async fn streaming_call(
        &self,
        _request: trevrpc::RpcRequest,
        request_body: trevrpc::BoxStream<Vec<u8>>,
    ) -> trevrpc::Result<trevrpc::BoxStream<trevrpc::ResponseFrame>> {
        let gate = self.gate.lock().ok().and_then(|mut gate| gate.take());
        let connection_lost = self.connection_lost;
        let dropped = self
            .dropped
            .lock()
            .ok()
            .and_then(|mut dropped| dropped.take());
        Ok(Box::pin(futures_util::stream::once(async move {
            // Like a transport that stops the upload when it reads the STATUS, but before the
            // client has recorded that STATUS.
            drop(request_body);
            if let Some(dropped) = dropped {
                let _ = dropped.send(());
            }
            if let Some(gate) = gate {
                let _ = gate.await;
            }
            if connection_lost {
                Err(Status::unavailable("connection lost").into())
            } else {
                Ok(trevrpc::ResponseFrame::Status(rejection()))
            }
        })))
    }
}

/// Makes a call whose upload stops before the response ends with a STATUS or, if
/// `connection_lost`, with an error. Returns the failure of a send made after the upload stopped.
async fn send_after_upload_stops_first(connection_lost: bool) -> TestResult<trevrpc::Error> {
    let (open_gate, gate) = oneshot::channel();
    let (dropped, upload_dropped) = oneshot::channel();
    let transport = TeardownFirst {
        gate: std::sync::Mutex::new(Some(gate)),
        dropped: std::sync::Mutex::new(Some(dropped)),
        connection_lost,
    };
    let call = trevrpc::client::client_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
        &transport,
        &support::greeter_path("Teardown"),
        long_options(),
    )
    .await?;
    upload_dropped.await?;

    // The upload is gone and the end of the call is not recorded yet. The send has to wait for
    // it, however long that takes, instead of giving up with a generic error.
    let send = tokio::spawn(call.send(large_message()));
    tokio::time::sleep(Duration::from_secs(5)).await;
    ensure!(
        !send.is_finished(),
        "the send gave up before the end of the call was known"
    );

    let _ = open_gate.send(());
    Ok(send.await?.err().ok_or("the send should fail")?)
}

#[tokio::test(start_paused = true)]
async fn send_waits_for_the_status_when_the_upload_stops_first() -> TestResult {
    let error = send_after_upload_stops_first(false).await?;
    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    Ok(())
}

#[tokio::test(start_paused = true)]
async fn send_reports_the_response_error_when_the_upload_stops_first() -> TestResult {
    let error = send_after_upload_stops_first(true).await?;
    let status = error.into_status();
    ensure_eq!(status.code(), Code::Unavailable);
    ensure!(status.message().contains("connection lost"));
    Ok(())
}
