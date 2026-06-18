#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    cert_hash: wtransport::tls::Sha256Digest,
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
async fn quinn_expired_deadlines_are_rejected_over_the_wire() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::quinn::QuinnTransport::new(connection.clone());
    let request = RpcRequest::new(greeter::GreeterClient::<()>::SERVICE, "Missing", Vec::new())
        .with_deadline_unix_nanos(expired_deadline_unix_nanos());

    let response = transport.call(request).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);

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
        let transport = trevrpc::quinn::QuinnTransport::new(connection.clone());
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
        let transport = trevrpc::quinn::QuinnTransport::new(connection.clone());
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
async fn quinn_malformed_request_frames_return_internal_status() -> TestResult {
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

    assert_eq!(Code::from_u32(response.status), Code::Internal);

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
    client: greeter::GreeterClient<trevrpc::quinn::QuinnTransport>,
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
    client: &greeter::GreeterClient<trevrpc::quinn::QuinnTransport>,
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
    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"])?;
    let cert_hash = identity.certificate_chain().as_slice()[0].hash();
    let config = wtransport::ServerConfig::builder()
        .with_bind_address(SocketAddr::from(([127, 0, 0, 1], 0)))
        .with_identity(identity)
        .build();
    let endpoint = wtransport::Endpoint::server(config)?;
    let addr = endpoint.local_addr()?;
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    greeter::register_greeter(&mut server, TestGreeter);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_webtransport_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(RunningWebTransportServer {
        addr,
        cert_hash,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

fn fast_server_options() -> ServerOptions {
    ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

async fn connect_client(
    server: &RunningServer,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<trevrpc::quinn::QuinnTransport>,
)> {
    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let transport = trevrpc::quinn::QuinnTransport::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

    Ok((endpoint, connection, client))
}

async fn connect_webtransport_client(
    server: &RunningWebTransportServer,
) -> TestResult<(
    wtransport::Endpoint<wtransport::endpoint::endpoint_side::Client>,
    wtransport::Connection,
    greeter::GreeterClient<trevrpc::webtransport::WebTransportTransport>,
)> {
    let config = wtransport::ClientConfig::builder()
        .with_bind_default()
        .with_server_certificate_hashes([server.cert_hash.clone()])
        .build();
    let endpoint = wtransport::Endpoint::client(config)?;
    let connection = endpoint
        .connect(format!("https://127.0.0.1:{}/trevrpc", server.addr.port()))
        .await?;
    let transport = trevrpc::webtransport::WebTransportTransport::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

    Ok((endpoint, connection, client))
}

async fn close_webtransport_client(
    endpoint: wtransport::Endpoint<wtransport::endpoint::endpoint_side::Client>,
    connection: wtransport::Connection,
) {
    connection.close(wtransport::VarInt::from_u32(0), b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, endpoint.wait_idle())
        .await
        .expect("client endpoint should become idle");
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

fn expired_deadline_unix_nanos() -> u64 {
    SystemTime::now()
        .checked_sub(Duration::from_secs(1))
        .expect("time should support one second subtraction")
        .duration_since(UNIX_EPOCH)
        .expect("deadline should be after Unix epoch")
        .as_nanos()
        .try_into()
        .expect("deadline should fit in u64")
}

fn make_server_endpoint() -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
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
