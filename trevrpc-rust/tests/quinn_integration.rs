#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::rustls::server::WebPkiClientVerifier;
use tokio::sync::{Notify, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use trevrpc::client::{CallOptions, RpcTransport};
use trevrpc::server::{MetadataValueAuthorizer, Metrics, RpcFinished, Server, ServerOptions};
use trevrpc::{Code, MessageStream, RpcRequest, RpcResponse, Status};

#[path = "../examples/shared/greeter.rs"]
mod greeter;

const AUTH_TOKEN: &str = "integration-token";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct TestGreeter;

#[trevrpc::async_trait]
impl greeter::Greeter for TestGreeter {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        Ok(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
    }

    async fn lots_of_replies(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, Status> {
        if request.name == "cancel" {
            return Ok(Box::new(FirstThenPendingReply {
                first: Some(greeter::HelloReply {
                    message: "first".to_owned(),
                }),
            }));
        }

        Ok(trevrpc::stream::from_iter([
            greeter::HelloReply {
                message: format!("hello, {}", request.name),
            },
            greeter::HelloReply {
                message: format!("goodbye, {}", request.name),
            },
        ]))
    }

    async fn lots_of_greetings(
        &self,
        mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        let mut names = Vec::new();

        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        Ok(greeter::HelloReply {
            message: names.join(","),
        })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, Status> {
        Ok(Box::new(BidiReplies { requests }))
    }
}

struct FirstThenPendingReply {
    first: Option<greeter::HelloReply>,
}

#[trevrpc::async_trait]
impl MessageStream<greeter::HelloReply> for FirstThenPendingReply {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        if let Some(first) = self.first.take() {
            return Some(Ok(first));
        }

        std::future::pending().await
    }
}

struct BidiReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl MessageStream<greeter::HelloReply> for BidiReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("echo, {}", request.name),
            })
        })
    }
}

#[derive(Clone, Default)]
struct DropSignal {
    dropped: Arc<AtomicBool>,
    notify: Arc<Notify>,
}

impl DropSignal {
    fn mark_dropped(&self) {
        self.dropped.store(true, Ordering::SeqCst);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        if !self.dropped.load(Ordering::SeqCst) {
            tokio::time::timeout(TEST_TIMEOUT, self.notify.notified())
                .await
                .expect("request stream should be dropped");
        }

        assert!(self.dropped.load(Ordering::SeqCst));
    }
}

struct PendingGreeterRequests {
    dropped: DropSignal,
}

impl PendingGreeterRequests {
    const fn new(dropped: DropSignal) -> Self {
        Self { dropped }
    }
}

#[trevrpc::async_trait]
impl MessageStream<greeter::HelloRequest> for PendingGreeterRequests {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloRequest>> {
        std::future::pending().await
    }
}

impl Drop for PendingGreeterRequests {
    fn drop(&mut self) {
        self.dropped.mark_dropped();
    }
}

struct RunningServer {
    addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<trevrpc::Result<()>>>,
}

impl RunningServer {
    async fn shutdown(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(task) = self.task.take() {
            tokio::time::timeout(TEST_TIMEOUT, task).await???;
        }

        Ok(())
    }
}

impl Drop for RunningServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct RunningWebTransportServer {
    addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<trevrpc::Result<()>>>,
}

impl RunningWebTransportServer {
    async fn shutdown(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(task) = self.task.take() {
            tokio::time::timeout(TEST_TIMEOUT, task).await???;
        }

        Ok(())
    }
}

impl Drop for RunningWebTransportServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Clone, Default)]
struct RecordingMetrics {
    codes: Arc<Mutex<Vec<Code>>>,
    notify: Arc<Notify>,
}

impl RecordingMetrics {
    async fn wait_for_code(&self, code: Code) {
        let wait = async {
            loop {
                if self
                    .codes
                    .lock()
                    .expect("metrics lock should not be poisoned")
                    .contains(&code)
                {
                    return;
                }

                self.notify.notified().await;
            }
        };

        tokio::time::timeout(TEST_TIMEOUT, wait)
            .await
            .expect("server metrics should record expected code");
    }
}

impl Metrics for RecordingMetrics {
    fn rpc_finished(&self, event: &RpcFinished) {
        self.codes
            .lock()
            .expect("metrics lock should not be poisoned")
            .push(event.code);
        self.notify.notify_waiters();
    }
}

#[tokio::test]
async fn quinn_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let reply = client
        .say_hello(
            greeter::HelloRequest {
                name: "unary".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(reply.message, "hello, unary");

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "server stream".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    assert_eq!(messages, ["hello, server stream", "goodbye, server stream"]);

    let summary = client
        .lots_of_greetings(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: "one".to_owned(),
                },
                greeter::HelloRequest {
                    name: "two".to_owned(),
                },
            ]),
            authenticated_options(),
        )
        .await?;
    assert_eq!(summary.message, "one,two");

    let mut replies = client
        .bidi_hello(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: "left".to_owned(),
                },
                greeter::HelloRequest {
                    name: "right".to_owned(),
                },
            ]),
            authenticated_options(),
        )
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    assert_eq!(messages, ["echo, left", "echo, right"]);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_webtransport_client(&server).await?;

    let reply = client
        .say_hello(
            greeter::HelloRequest {
                name: "unary".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(reply.message, "hello, unary");

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "server stream".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    assert_eq!(messages, ["hello, server stream", "goodbye, server stream"]);

    let summary = client
        .lots_of_greetings(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: "one".to_owned(),
                },
                greeter::HelloRequest {
                    name: "two".to_owned(),
                },
            ]),
            authenticated_options(),
        )
        .await?;
    assert_eq!(summary.message, "one,two");

    let mut replies = client
        .bidi_hello(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: "left".to_owned(),
                },
                greeter::HelloRequest {
                    name: "right".to_owned(),
                },
            ]),
            authenticated_options(),
        )
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    assert_eq!(messages, ["echo, left", "echo, right"]);

    close_webtransport_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_unexpected_path() -> TestResult {
    let server = spawn_webtransport_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/wrong")?);

    let error = expect_webtransport_connect_error(&client, request).await;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::NOT_FOUND);
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_browser_origin_without_allowlist() -> TestResult {
    let server = spawn_webtransport_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_header(
                web_transport_quinn::http::header::ORIGIN,
                web_transport_quinn::http::HeaderValue::from_static("https://example.invalid"),
            );

    let error = expect_webtransport_connect_error(&client, request).await;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::FORBIDDEN);
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_allows_configured_browser_origin() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_webtransport_allowed_origins(&["https://example.invalid"]);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_header(
                web_transport_quinn::http::header::ORIGIN,
                web_transport_quinn::http::HeaderValue::from_static("https://example.invalid"),
            );
    let session = client.connect(request).await?;
    let transport = trevrpc::webtransport::Client::new(session.clone());
    let greeter_client = greeter::GreeterClient::new(transport);

    let reply = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "origin".to_owned(),
            },
            authenticated_options(),
        )
        .await?;

    assert_eq!(reply.message, "hello, origin");
    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_unexpected_authority_when_allowlist_is_set() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_webtransport_allowed_authorities(&["expected.example"]);
    })?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);

    let error = expect_webtransport_connect_error(&client, request).await;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::FORBIDDEN);
    server.shutdown().await
}

#[tokio::test]
async fn quinn_auth_failures_return_status_errors() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "missing auth".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await
        .expect_err("request without auth should fail");

    assert_eq!(error.into_status().code(), Code::Unauthenticated);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_oversized_timeouts_are_rejected_over_the_wire() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::quinn::Client::new(connection.clone());
    let request = RpcRequest::new(greeter::GreeterClient::<()>::SERVICE, "Missing", Vec::new())
        .with_timeout_nanos(u64::MAX);

    let response = transport.call(request).await?;

    assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_request_stream_limits_return_resource_exhausted() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_stream_messages(Some(1))
                .with_stream_idle_timeout(Some(TEST_TIMEOUT)),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let error = client
        .lots_of_greetings(
            trevrpc::stream::from_iter([
                greeter::HelloRequest {
                    name: "one".to_owned(),
                },
                greeter::HelloRequest {
                    name: "two".to_owned(),
                },
            ]),
            authenticated_options(),
        )
        .await
        .expect_err("request stream should exceed message limit");

    assert_eq!(error.into_status().code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_response_stream_limits_return_resource_exhausted() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "limited".to_owned(),
            },
            authenticated_options().with_max_response_messages(Some(1)),
        )
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    assert_eq!(first.message, "hello, limited");

    let error = replies
        .next()
        .await
        .expect("response stream should yield limit error")
        .expect_err("second response should exceed client limit");
    assert_eq!(error.into_status().code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_stream_concurrency_limit_returns_unavailable() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_concurrent_streams_per_connection(Some(1))
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let hanging = hold_server_stream_open(&client).await?;

    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "second".to_owned(),
            },
            authenticated_options(),
        )
        .await
        .expect_err("second stream should exceed stream concurrency limit");

    assert_eq!(error.into_status().code(), Code::Unavailable);

    drop(hanging);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_request_concurrency_limit_returns_unavailable() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_concurrent_requests(Some(1))
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let hanging = hold_server_stream_open(&client).await?;

    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "second".to_owned(),
            },
            authenticated_options(),
        )
        .await
        .expect_err("second request should exceed global request concurrency limit");

    assert_eq!(error.into_status().code(), Code::Unavailable);

    drop(hanging);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_connection_limit_refuses_new_connections() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_concurrent_connections(Some(1))
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let hanging = hold_server_stream_open(&client).await?;
    let second_endpoint = make_client_endpoint(server.cert_der.clone())?;

    let connect = tokio::time::timeout(
        TEST_TIMEOUT,
        second_endpoint.connect(server.addr, "localhost")?,
    )
    .await
    .expect("second connection attempt should complete");

    if let Ok(connection) = connect {
        let transport = trevrpc::quinn::Client::new(connection.clone());
        let error = trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "SayHello",
            &greeter::HelloRequest {
                name: "refused".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("RPC on refused connection should fail");
        assert_eq!(error.into_status().code(), Code::Unavailable);
        connection.close(0_u32.into(), b"second done");
    }

    second_endpoint.close(0_u32.into(), b"second done");
    drop(hanging);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_dropped_response_stream_cancels_server_work() -> TestResult {
    let metrics = RecordingMetrics::default();
    let server_metrics = metrics.clone();
    let server = spawn_greeter_server(move |server| {
        server.set_metrics(server_metrics);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    assert_eq!(first.message, "first");

    drop(replies);
    metrics.wait_for_code(Code::Cancelled).await;

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_status_drops_pending_request_stream() -> TestResult {
    let dropped = DropSignal::default();
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::quinn::Client::new(connection.clone());
    let requests = Box::new(PendingGreeterRequests::new(dropped.clone()));
    let mut replies = trevrpc::client::bidirectional_streaming::<_, _, greeter::HelloReply>(
        &transport,
        greeter::GreeterClient::<()>::SERVICE,
        "RejectUpload",
        requests,
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;

    let error = replies
        .next()
        .await
        .expect("terminal status should be delivered")
        .expect_err("terminal status should reject the stream");

    assert_eq!(error.into_status().code(), Code::PermissionDenied);
    dropped.wait().await;
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_terminal_status_drops_pending_request_stream() -> TestResult {
    let dropped = DropSignal::default();
    let server = spawn_webtransport_greeter_server(register_reject_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = trevrpc::webtransport::Client::new(session.clone());
    let requests = Box::new(PendingGreeterRequests::new(dropped.clone()));
    let mut replies = trevrpc::client::bidirectional_streaming::<_, _, greeter::HelloReply>(
        &transport,
        greeter::GreeterClient::<()>::SERVICE,
        "RejectUpload",
        requests,
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;

    let error = replies
        .next()
        .await
        .expect("terminal status should be delivered")
        .expect_err("terminal status should reject the stream");

    assert_eq!(error.into_status().code(), Code::PermissionDenied);
    dropped.wait().await;
    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_shutdown_closes_active_connections() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    server.shutdown().await?;

    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "after shutdown".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("RPC on drained connection should fail");
    assert_eq!(error.into_status().code(), Code::Unavailable);

    close_client(endpoint, connection).await;
    Ok(())
}

#[tokio::test]
async fn quinn_shutdown_is_bounded_with_pending_unary_handler() -> TestResult {
    let started = Arc::new(Notify::new());
    let started_server = Arc::clone(&started);
    let server = spawn_greeter_server(move |server| {
        server.set_options(
            fast_server_options().with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.route(
            greeter::GreeterClient::<()>::SERVICE,
            "Never",
            move |_body| {
                let started = Arc::clone(&started_server);
                async move {
                    started.notify_waiters();
                    std::future::pending::<trevrpc::Result<Vec<u8>>>().await
                }
            },
        );
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::quinn::Client::new(connection.clone());
    let call = tokio::spawn(async move {
        trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "Never",
            &greeter::HelloRequest {
                name: "shutdown".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await
    });

    tokio::time::timeout(TEST_TIMEOUT, started.notified())
        .await
        .expect("pending unary handler should start");
    server.shutdown().await?;
    let result = tokio::time::timeout(TEST_TIMEOUT, call)
        .await
        .expect("pending client call should finish after shutdown")?;
    let error = result.expect_err("shutdown should fail the pending RPC");
    assert!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable | Code::DeadlineExceeded
    ));
    close_client(endpoint, connection).await;
    Ok(())
}

#[tokio::test]
async fn quinn_local_close_maps_to_cancelled() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    connection.close(0_u32.into(), b"client closed");

    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "after local close".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("RPC on locally closed connection should fail");
    assert_eq!(error.into_status().code(), Code::Cancelled);

    tokio::time::timeout(TEST_TIMEOUT, endpoint.wait_idle())
        .await
        .expect("client endpoint should become idle");
    server.shutdown().await
}

#[tokio::test]
async fn quinn_rejects_alpn_mismatch() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let endpoint = make_client_endpoint_with_alpn(server.cert_der.clone(), b"not-trevrpc")?;

    let result = tokio::time::timeout(TEST_TIMEOUT, endpoint.connect(server.addr, "localhost")?)
        .await
        .expect("ALPN mismatch handshake should complete");

    assert!(result.is_err(), "ALPN mismatch should reject connection");

    endpoint.close(0_u32.into(), b"test complete");
    server.shutdown().await
}

#[tokio::test]
async fn quinn_rejects_tls_identity_mismatch() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let endpoint = make_client_endpoint(server.cert_der.clone())?;

    let result = tokio::time::timeout(TEST_TIMEOUT, endpoint.connect(server.addr, "wronghost")?)
        .await
        .expect("identity mismatch handshake should complete");

    assert!(result.is_err(), "SNI mismatch should reject connection");

    endpoint.close(0_u32.into(), b"test complete");
    server.shutdown().await
}

#[tokio::test]
async fn quinn_mtls_rejects_clients_without_certificates() -> TestResult {
    let server = spawn_greeter_server_with_endpoint(make_mtls_server_endpoint()?, |_| {})?;
    let endpoint = make_client_endpoint(server.cert_der.clone())?;

    let result = tokio::time::timeout(TEST_TIMEOUT, endpoint.connect(server.addr, "localhost")?)
        .await
        .expect("mTLS rejection handshake should complete");

    if let Ok(connection) = result {
        let transport = trevrpc::quinn::Client::new(connection.clone());
        let status = trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "SayHello",
            &greeter::HelloRequest {
                name: "anonymous".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .map_err(trevrpc::Error::into_status)
        .expect_err("server requiring mTLS should reject anonymous clients");
        assert_eq!(status.code(), Code::Unavailable);
        connection.close(0_u32.into(), b"test complete");
    }

    endpoint.close(0_u32.into(), b"test complete");
    server.shutdown().await
}

#[tokio::test]
async fn quinn_malformed_request_frames_return_invalid_argument_status() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;

    send.write_all(&2_u32.to_be_bytes()).await?;
    send.write_all(&[0xff, 0xff]).await?;
    send.finish()?;

    let response = trevrpc::quinn::read_frame::<RpcResponse>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;

    assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_initial_request_timeout_rejects_partial_header() -> TestResult {
    let server = spawn_greeter_server_with_initial_request_timeout(Duration::from_millis(50))?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&[0, 0]).await?;

    let response = read_raw_quinn_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_initial_request_timeout_rejects_partial_body() -> TestResult {
    let server = spawn_greeter_server_with_initial_request_timeout(Duration::from_millis(50))?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&8_u32.to_be_bytes()).await?;
    send.write_all(&[1]).await?;

    let response = read_raw_quinn_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_oversized_initial_frame_is_rejected_before_body() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let oversized = (trevrpc::framing::DEFAULT_MAX_FRAME_SIZE + 1)
        .try_into()
        .expect("default frame size should fit in u32");
    send.write_all(&u32::to_be_bytes(oversized)).await?;

    let response = read_raw_quinn_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::ResourceExhausted);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_handles_many_concurrent_unary_calls() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let mut calls = JoinSet::new();

    for index in 0..64 {
        let client = client.clone();
        calls.spawn(async move {
            let name = format!("concurrent-{index}");
            let response = client
                .say_hello(
                    greeter::HelloRequest { name: name.clone() },
                    authenticated_options(),
                )
                .await?;
            Ok::<_, trevrpc::Error>((name, response.message))
        });
    }

    let mut completed = 0;
    while let Some(result) = calls.join_next().await {
        let (name, message) = result??;
        assert_eq!(message, format!("hello, {name}"));
        completed += 1;
    }
    assert_eq!(completed, 64);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_handles_bounded_mixed_load() -> TestResult {
    run_mixed_workload(192, 64).await
}

#[tokio::test]
#[ignore = "longer soak test; run with `cargo test --test quinn_integration -- --ignored quinn_soaks_mixed_workload`"]
async fn quinn_soaks_mixed_workload() -> TestResult {
    let calls = std::env::var("TREVRPC_SOAK_CALLS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2048);

    run_mixed_workload(calls, 128).await
}

async fn run_mixed_workload(total_calls: usize, batch_size: usize) -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_concurrent_streams_per_connection(Some(512))
                .with_max_concurrent_requests(Some(1024)),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut completed = 0;
    for batch_start in (0..total_calls).step_by(batch_size.max(1)) {
        let batch_end = total_calls.min(batch_start + batch_size.max(1));
        let mut calls = JoinSet::new();

        for index in batch_start..batch_end {
            let client = client.clone();
            calls.spawn(async move { run_mixed_call(client, index).await });
        }

        while let Some(result) = calls.join_next().await {
            result??;
            completed += 1;
        }
    }

    assert_eq!(completed, total_calls);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

async fn run_mixed_call(
    client: greeter::GreeterClient<trevrpc::quinn::Client>,
    index: usize,
) -> trevrpc::Result<()> {
    match index % 4 {
        0 => {
            let name = format!("load-unary-{index}");
            let response = client
                .say_hello(
                    greeter::HelloRequest { name: name.clone() },
                    authenticated_options(),
                )
                .await?;
            assert_eq!(response.message, format!("hello, {name}"));
        }
        1 => {
            let name = format!("load-server-stream-{index}");
            let mut responses = client
                .lots_of_replies(
                    greeter::HelloRequest { name: name.clone() },
                    authenticated_options(),
                )
                .await?;
            let mut count = 0;
            while let Some(response) = responses.next().await {
                assert!(response?.message.contains(&name));
                count += 1;
            }
            assert_eq!(count, 2);
        }
        2 => {
            let response = client
                .lots_of_greetings(
                    trevrpc::stream::from_iter([
                        greeter::HelloRequest {
                            name: format!("load-client-{index}-a"),
                        },
                        greeter::HelloRequest {
                            name: format!("load-client-{index}-b"),
                        },
                    ]),
                    authenticated_options(),
                )
                .await?;
            assert_eq!(
                response.message,
                format!("load-client-{index}-a,load-client-{index}-b")
            );
        }
        _ => {
            let mut responses = client
                .bidi_hello(
                    trevrpc::stream::from_iter([
                        greeter::HelloRequest {
                            name: format!("load-bidi-{index}-a"),
                        },
                        greeter::HelloRequest {
                            name: format!("load-bidi-{index}-b"),
                        },
                    ]),
                    authenticated_options(),
                )
                .await?;
            let mut count = 0;
            while let Some(response) = responses.next().await {
                assert!(response?.message.starts_with("echo, load-bidi-"));
                count += 1;
            }
            assert_eq!(count, 2);
        }
    }

    Ok(())
}

async fn hold_server_stream_open(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> TestResult<trevrpc::BoxMessageStream<greeter::HelloReply>> {
    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    assert_eq!(first.message, "first");

    Ok(replies)
}

fn spawn_greeter_server(configure: impl FnOnce(&mut Server)) -> TestResult<RunningServer> {
    spawn_greeter_server_with_endpoint(make_server_endpoint()?, configure)
}

fn register_reject_upload_route(server: &mut Server) {
    server.route_streaming(
        greeter::GreeterClient::<()>::SERVICE,
        "RejectUpload",
        trevrpc::RpcKind::BidirectionalStreaming,
        |_body, _requests| async {
            Err(trevrpc::Error::from(Status::new(
                Code::PermissionDenied,
                "upload rejected",
            )))
        },
    );
}

fn spawn_greeter_server_with_initial_request_timeout(
    timeout: Duration,
) -> TestResult<RunningServer> {
    spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_initial_request_timeout(Some(timeout)));
    })
}

fn spawn_greeter_server_with_endpoint(
    (endpoint, cert_der): (quinn::Endpoint, CertificateDer<'static>),
    configure: impl FnOnce(&mut Server),
) -> TestResult<RunningServer> {
    let addr = endpoint.local_addr()?;
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    greeter::register_greeter(&mut server, TestGreeter);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_quinn_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(RunningServer {
        addr,
        cert_der,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

fn spawn_webtransport_greeter_server(
    configure: impl FnOnce(&mut Server),
) -> TestResult<RunningWebTransportServer> {
    let (endpoint, cert_der) = make_server_endpoint_with_alpns(
        &[trevrpc::ALPN, web_transport_quinn::ALPN.as_bytes()],
        true,
    )?;
    let addr = endpoint.local_addr()?;
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    greeter::register_greeter(&mut server, TestGreeter);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_quinn_and_webtransport_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(RunningWebTransportServer {
        addr,
        cert_der,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

fn fast_server_options() -> ServerOptions {
    ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

async fn read_raw_quinn_response(recv: &mut quinn::RecvStream) -> TestResult<RpcResponse> {
    Ok(tokio::time::timeout(
        TEST_TIMEOUT,
        trevrpc::quinn::read_frame::<RpcResponse>(recv, trevrpc::framing::DEFAULT_MAX_FRAME_SIZE),
    )
    .await??)
}

async fn connect_client(
    server: &RunningServer,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<trevrpc::quinn::Client>,
)> {
    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let transport = trevrpc::quinn::Client::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

    Ok((endpoint, connection, client))
}

async fn connect_webtransport_client(
    server: &RunningWebTransportServer,
) -> TestResult<(
    web_transport_quinn::Client,
    web_transport_quinn::Session,
    greeter::GreeterClient<trevrpc::webtransport::Client>,
)> {
    let webtransport_client = make_webtransport_client(server)?;
    let session = webtransport_client
        .connect(web_transport_quinn::proto::ConnectRequest::new(
            webtransport_url(server, "/trevrpc")?,
        ))
        .await?;
    let transport = trevrpc::webtransport::Client::new(session.clone());
    let greeter_client = greeter::GreeterClient::new(transport);

    Ok((webtransport_client, session, greeter_client))
}

fn make_webtransport_client(
    server: &RunningWebTransportServer,
) -> TestResult<web_transport_quinn::Client> {
    Ok(web_transport_quinn::ClientBuilder::new()
        .with_server_certificates(vec![server.cert_der.clone()])?)
}

fn webtransport_url(server: &RunningWebTransportServer, path: &str) -> TestResult<url::Url> {
    Ok(format!("https://127.0.0.1:{}{path}", server.addr.port()).parse()?)
}

async fn expect_webtransport_connect_error(
    client: &web_transport_quinn::Client,
    request: web_transport_quinn::proto::ConnectRequest,
) -> web_transport_quinn::ClientError {
    match client.connect(request).await {
        Ok(session) => {
            session.close(0, b"unexpected test session");
            panic!("WebTransport connect should have failed");
        }
        Err(error) => error,
    }
}

fn assert_webtransport_connect_rejected(
    error: web_transport_quinn::ClientError,
    expected: web_transport_quinn::http::StatusCode,
) {
    match error {
        web_transport_quinn::ClientError::HttpError(
            web_transport_quinn::ConnectError::ErrorStatus(actual),
        ) => assert_eq!(actual, expected),
        web_transport_quinn::ClientError::HttpError(
            web_transport_quinn::ConnectError::ProtoError(_),
        ) => {}
        other => panic!("expected WebTransport HTTP status {expected}, got {other:?}"),
    }
}

async fn close_webtransport_client(
    _client: web_transport_quinn::Client,
    session: web_transport_quinn::Session,
) {
    session.close(0, b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("client WebTransport session should close");
}

async fn close_client(endpoint: quinn::Endpoint, connection: quinn::Connection) {
    connection.close(0_u32.into(), b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, endpoint.wait_idle())
        .await
        .expect("client endpoint should become idle");
}

fn authenticated_options() -> CallOptions {
    CallOptions::new()
        .with_timeout(TEST_TIMEOUT)
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

fn make_server_endpoint() -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    make_server_endpoint_with_alpns(&[trevrpc::ALPN], false)
}

fn make_server_endpoint_with_alpns(
    alpns: &[&[u8]],
    allow_uni_streams: bool,
) -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = alpns.iter().map(|alpn| (*alpn).to_vec()).collect();

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    if !allow_uni_streams {
        let transport_config = Arc::get_mut(&mut server_config.transport)
            .expect("server config should have one transport reference");
        transport_config.max_concurrent_uni_streams(0_u8.into());
    }

    Ok((
        quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn make_mtls_server_endpoint() -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let mut client_roots = quinn::rustls::RootCertStore::empty();
    client_roots.add(cert_der.clone())?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots)).build()?;

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_client_cert_verifier(verifier)
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    let transport_config = Arc::get_mut(&mut server_config.transport)
        .expect("server config should have one transport reference");
    transport_config.max_concurrent_uni_streams(0_u8.into());

    Ok((
        quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn make_client_endpoint(cert_der: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    make_client_endpoint_with_alpn(cert_der, trevrpc::ALPN)
}

fn make_client_endpoint_with_alpn(
    cert_der: CertificateDer<'static>,
    alpn: &[u8],
) -> TestResult<quinn::Endpoint> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;

    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![alpn.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(client_crypto)?,
    )));

    Ok(endpoint)
}
