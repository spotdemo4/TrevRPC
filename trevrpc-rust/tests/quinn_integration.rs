#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{Buf, Bytes};
use prost::Message;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::rustls::server::WebPkiClientVerifier;
use tokio::sync::{Mutex as AsyncMutex, Notify, oneshot};
use tokio::task::{JoinHandle, JoinSet};
use trevrpc::client::{CallOptions, RpcTransport};
use trevrpc::server::{
    MetadataValueAuthorizer, Metrics, RpcFinished, RpcStarted, Server, ServerOptions,
};
use trevrpc::{Code, MessageStream, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, Status};

#[path = "../examples/shared/greeter.rs"]
mod greeter;

const AUTH_TOKEN: &str = "integration-token";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn reject_http3_admission(request: &trevrpc::server::Http3AdmissionRequest<'_>) -> bool {
    assert!(request.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("content-type")
            && header.value.eq_ignore_ascii_case(b"application/trevrpc")
    }));
    false
}

fn require_webtransport_admission_header(
    request: &trevrpc::server::WebTransportAdmissionRequest<'_>,
) -> bool {
    request.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("x-trevrpc-admission") && header.value == b"allowed"
    })
}

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
    cert_der: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<trevrpc::Result<()>>>,
}

type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type H3RecvStream = h3::client::RequestStream<h3_quinn::RecvStream, Bytes>;

#[derive(Clone)]
struct Http3Transport {
    sender: Arc<AsyncMutex<H3SendRequest>>,
    authority: String,
}

impl Http3Transport {
    async fn open(
        &self,
        method: http::Method,
        path: &str,
        content_type: Option<&str>,
    ) -> trevrpc::Result<h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>> {
        let content_types = content_type.map_or_else(Vec::new, |value| vec![value]);
        self.open_with_content_types(method, path, &content_types)
            .await
    }

    async fn open_with_content_types(
        &self,
        method: http::Method,
        path: &str,
        content_types: &[&str],
    ) -> trevrpc::Result<h3::client::RequestStream<h3_quinn::BidiStream<Bytes>, Bytes>> {
        let uri = format!("https://{}{path}", self.authority);
        let mut request = http::Request::builder().method(method).uri(uri);
        for content_type in content_types {
            request = request.header(http::header::CONTENT_TYPE, *content_type);
        }
        let request = request.body(()).map_err(trevrpc::Error::transport)?;
        self.sender
            .lock()
            .await
            .send_request(request)
            .await
            .map_err(trevrpc::Error::transport)
    }
}

#[trevrpc::async_trait]
impl RpcTransport for Http3Transport {
    async fn call(&self, request: RpcRequest) -> trevrpc::Result<RpcResponse> {
        let mut stream = self
            .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
            .await?;
        stream
            .send_data(Bytes::from(trevrpc::framing::encode_frame(&request)?))
            .await
            .map_err(trevrpc::Error::transport)?;
        stream.finish().await.map_err(trevrpc::Error::transport)?;
        let response = stream
            .recv_response()
            .await
            .map_err(trevrpc::Error::transport)?;
        validate_http3_response(&response)?;
        let body = read_http3_body(&mut stream).await?;
        decode_single_frame(&body)
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: trevrpc::BoxMessageStream<Vec<u8>>,
    ) -> trevrpc::Result<trevrpc::BoxMessageStream<RpcStreamFrame>> {
        let stream = self
            .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
            .await?;
        let (mut send, mut recv) = stream.split();
        let write_task = tokio::spawn(async move {
            send.send_data(Bytes::from(trevrpc::framing::encode_frame(&request)?))
                .await
                .map_err(trevrpc::Error::transport)?;
            while let Some(body) = request_body.next().await.transpose()? {
                let frame = trevrpc::framing::encode_message_stream_frame(
                    &body,
                    trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
                )?;
                send.send_data(Bytes::from(frame))
                    .await
                    .map_err(trevrpc::Error::transport)?;
            }
            send.finish().await.map_err(trevrpc::Error::transport)
        });
        let response = recv
            .recv_response()
            .await
            .map_err(trevrpc::Error::transport)?;
        validate_http3_response(&response)?;
        Ok(Box::new(Http3ResponseStream {
            recv,
            chunk: Bytes::new(),
            write_task: Some(write_task),
            done: false,
        }))
    }
}

struct Http3ResponseStream {
    recv: H3RecvStream,
    chunk: Bytes,
    write_task: Option<JoinHandle<trevrpc::Result<()>>>,
    done: bool,
}

#[trevrpc::async_trait]
impl MessageStream<RpcStreamFrame> for Http3ResponseStream {
    async fn next(&mut self) -> Option<trevrpc::Result<RpcStreamFrame>> {
        if self.done {
            return None;
        }
        let body = match read_http3_frame_body(&mut self.recv, &mut self.chunk).await {
            Ok(Some(body)) => body,
            Ok(None) => {
                self.done = true;
                return None;
            }
            Err(error) => {
                self.done = true;
                return Some(Err(error));
            }
        };
        let frame = match trevrpc::framing::decode_stream_frame_body(&body) {
            Ok(frame) => frame,
            Err(error) => return Some(Err(error)),
        };
        if frame.frame_kind() == Some(trevrpc::RpcStreamFrameKind::Status) {
            self.done = true;
            if let Some(write_task) = self.write_task.take()
                && !write_task.is_finished()
            {
                write_task.abort();
            }
        }
        Some(Ok(frame))
    }
}

impl Drop for Http3ResponseStream {
    fn drop(&mut self) {
        if let Some(write_task) = &self.write_task {
            write_task.abort();
        }
    }
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
    started: Arc<Mutex<Vec<RpcStarted>>>,
    finished: Arc<Mutex<Vec<RpcFinished>>>,
    codes: Arc<Mutex<Vec<Code>>>,
    notify: Arc<Notify>,
}

impl RecordingMetrics {
    fn started(&self) -> Vec<RpcStarted> {
        self.started
            .lock()
            .expect("metrics lock should not be poisoned")
            .clone()
    }

    fn finished(&self) -> Vec<RpcFinished> {
        self.finished
            .lock()
            .expect("metrics lock should not be poisoned")
            .clone()
    }

    fn codes(&self) -> Vec<Code> {
        self.codes
            .lock()
            .expect("metrics lock should not be poisoned")
            .clone()
    }

    async fn wait_for_code(&self, code: Code) {
        let wait = async {
            loop {
                if self.codes().contains(&code) {
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
    fn rpc_started(&self, event: &RpcStarted) {
        self.started
            .lock()
            .expect("metrics lock should not be poisoned")
            .push(event.clone());
    }

    fn rpc_finished(&self, event: &RpcFinished) {
        self.finished
            .lock()
            .expect("metrics lock should not be poisoned")
            .push(event.clone());
        self.codes
            .lock()
            .expect("metrics lock should not be poisoned")
            .push(event.code);
        self.notify.notify_waiters();
    }
}

fn assert_pre_handler_metrics(metrics: &RecordingMetrics, code: Code) {
    let started = metrics.started();
    let finished = metrics.finished();
    assert_eq!(started.len(), 1, "started metrics: {started:#?}");
    assert_eq!(finished.len(), 1, "finished metrics: {finished:#?}");
    assert_eq!(started[0].service, "");
    assert_eq!(started[0].method, "");
    assert_eq!(started[0].request_body_len, 0);
    assert_eq!(finished[0].service, "");
    assert_eq!(finished[0].method, "");
    assert_eq!(finished[0].request_body_len, 0);
    assert_eq!(finished[0].response_body_len, 0);
    assert_eq!(finished[0].code, code);
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

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    assert_eq!(summary.message, "one,two");

    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
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

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    assert_eq!(summary.message, "one,two");

    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
    assert_eq!(messages, ["echo, left", "echo, right"]);

    close_webtransport_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn managed_webtransport_channel_round_trips_unary() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let webtransport_client = make_webtransport_client(&server)?;
    let origin = format!("https://127.0.0.1:{}", server.addr.port());
    let channel =
        trevrpc::client::Channel::connect_webtransport(webtransport_client, &origin).await?;
    let client = greeter::GreeterClient::new(channel.clone());

    let reply = client
        .say_hello(
            greeter::HelloRequest {
                name: "channel WebTransport".to_owned(),
            },
            authenticated_options(),
        )
        .await?;

    assert_eq!(reply.message, "hello, channel WebTransport");
    assert!(channel.is_ready());

    let mut states = channel.subscribe_state();
    server.shutdown().await?;
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if states.borrow_and_update().phase() == trevrpc::client::ChannelPhase::Reconnecting {
                break;
            }
            states
                .changed()
                .await
                .expect("channel state should remain open");
        }
    })
    .await?;
    let error = tokio::time::timeout(
        Duration::from_millis(20),
        client.say_hello(
            greeter::HelloRequest {
                name: "must fail fast".to_owned(),
            },
            authenticated_options(),
        ),
    )
    .await?
    .expect_err("WebTransport calls must fail while reconnecting");
    assert_eq!(error.into_status().code(), Code::Unavailable);

    channel.close();
    Ok(())
}

#[tokio::test]
async fn http3_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_http3_enabled(true);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_http3_client(&server).await?;

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

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    assert_eq!(summary.message, "one,two");
    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
    assert_eq!(messages, ["echo, left", "echo, right"]);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn combined_endpoint_accepts_quinn_and_webtransport_clients() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_http3_enabled(true);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;

    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let quinn_client = greeter::GreeterClient::new(trevrpc::advanced::RawQuinnTransport::new(
        connection.clone(),
    ));
    let quinn_reply = quinn_client
        .say_hello(
            greeter::HelloRequest {
                name: "quinn".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(quinn_reply.message, "hello, quinn");
    close_client(endpoint, connection).await;

    let (webtransport_client, session, greeter_client) =
        connect_webtransport_client(&server).await?;
    let webtransport_reply = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "webtransport".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(webtransport_reply.message, "hello, webtransport");

    let (http3_endpoint, http3_connection, http3_client) = connect_http3_client(&server).await?;
    let http3_reply = http3_client
        .say_hello(
            greeter::HelloRequest {
                name: "http3".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(http3_reply.message, "hello, http3");

    let webtransport_reply = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "webtransport interleaved".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(
        webtransport_reply.message,
        "hello, webtransport interleaved"
    );
    let http3_reply = http3_client
        .say_hello(
            greeter::HelloRequest {
                name: "http3 interleaved".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(http3_reply.message, "hello, http3 interleaved");

    close_client(http3_endpoint, http3_connection).await;
    close_webtransport_client(webtransport_client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn unified_h3_shutdown_is_bounded_with_active_http3_and_webtransport_rpcs() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_http3_enabled(true)
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (webtransport_client, session, webtransport_rpc) =
        connect_webtransport_client(&server).await?;
    let (http3_endpoint, http3_connection, http3_rpc) = connect_http3_client(&server).await?;

    let mut webtransport_replies = webtransport_rpc
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    let mut http3_replies = http3_rpc
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(
        webtransport_replies
            .next()
            .await
            .expect("WebTransport stream should yield first response")?
            .message,
        "first"
    );
    assert_eq!(
        http3_replies
            .next()
            .await
            .expect("HTTP/3 stream should yield first response")?
            .message,
        "first"
    );

    tokio::time::timeout(TEST_TIMEOUT, server.shutdown())
        .await
        .expect("unified h3 shutdown should be bounded")?;
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("WebTransport session should close during shutdown");
    tokio::time::timeout(TEST_TIMEOUT, http3_connection.closed())
        .await
        .expect("HTTP/3 connection should close during shutdown");

    drop(webtransport_replies);
    drop(http3_replies);
    close_webtransport_client(webtransport_client, session).await;
    http3_endpoint.close(0_u32.into(), b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, http3_endpoint.wait_idle())
        .await
        .expect("HTTP/3 endpoint should become idle");
    Ok(())
}

#[tokio::test]
async fn http3_rejects_invalid_path_method_and_media_type() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_http3_enabled(true);
        server.set_http3_admission(Some(reject_http3_admission));
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;

    assert_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/wrong",
            Some("application/trevrpc")
        )
        .await?,
        http::StatusCode::NOT_FOUND
    );
    assert_eq!(
        http3_status(
            &transport,
            http::Method::GET,
            "/trevrpc",
            Some("application/trevrpc")
        )
        .await?,
        http::StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/json")
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("Application/TrevRPC")
        )
        .await?,
        http::StatusCode::FORBIDDEN
    );
    assert_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/trevrpc; charset=utf-8")
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    assert_eq!(
        http3_status_with_content_types(
            &transport,
            http::Method::POST,
            "/trevrpc",
            &["application/trevrpc", "application/trevrpc"],
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn http3_enforces_stream_limits_and_detects_cancellation() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_webtransport_greeter_server(move |server| {
        server.set_options(
            fast_server_options()
                .with_http3_enabled(true)
                .with_max_stream_messages(Some(1)),
        );
        server.set_metrics(metrics);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    let client = greeter::GreeterClient::new(transport.clone());

    let mut call = client.lots_of_greetings(authenticated_options()).await?;
    call.send(greeter::HelloRequest {
        name: "one".to_owned(),
    })
    .await?;
    call.send(greeter::HelloRequest {
        name: "two".to_owned(),
    })
    .await?;
    let error = call
        .close_and_recv()
        .await
        .expect_err("HTTP/3 request stream should exceed message limit");
    assert_eq!(error.into_status().code(), Code::ResourceExhausted);

    let mut request = RpcRequest::new(
        greeter::GreeterClient::<()>::SERVICE,
        "LotsOfReplies",
        greeter::HelloRequest {
            name: "cancel".to_owned(),
        }
        .encode_to_vec(),
    )
    .with_kind(RpcKind::ServerStreaming)
    .with_timeout_nanos(TEST_TIMEOUT.as_nanos().try_into()?);
    request.metadata.insert(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    );
    let raw = transport
        .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
        .await?;
    let (mut send, mut recv) = raw.split();
    send.send_data(Bytes::from(trevrpc::framing::encode_frame(&request)?))
        .await?;
    validate_http3_response(&recv.recv_response().await?)?;
    let mut chunk = Bytes::new();
    let first = read_http3_frame_body(&mut recv, &mut chunk)
        .await?
        .expect("HTTP/3 response should contain the first message");
    let first = trevrpc::framing::decode_stream_frame_body(&first)?;
    assert_eq!(
        greeter::HelloReply::decode(first.body.as_slice())?.message,
        "first"
    );
    send.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
    tokio::time::timeout(
        Duration::from_millis(500),
        observed_metrics.wait_for_code(Code::Cancelled),
    )
    .await
    .expect("HTTP/3 request reset should promptly cancel the handler");

    let reply = client
        .say_hello(
            greeter::HelloRequest {
                name: "after cancellation".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(reply.message, "hello, after cancellation");
    drop(recv);
    close_client(endpoint, connection).await;
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
    let transport = trevrpc::advanced::RawWebTransport::new(session.clone());
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
async fn webtransport_admission_receives_generic_request_headers() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_webtransport_admission(Some(require_webtransport_admission_header));
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let client = make_webtransport_client(&server)?;
    let denied =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);
    assert_webtransport_connect_rejected(
        expect_webtransport_connect_error(&client, denied).await,
        web_transport_quinn::http::StatusCode::FORBIDDEN,
    );

    let allowed =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_header(
                web_transport_quinn::http::HeaderName::from_static("x-trevrpc-admission"),
                web_transport_quinn::http::HeaderValue::from_static("allowed"),
            );
    let session = client.connect(allowed).await?;
    let rpc = greeter::GreeterClient::new(trevrpc::advanced::RawWebTransport::new(session.clone()));
    let reply = rpc
        .say_hello(
            greeter::HelloRequest {
                name: "admitted".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(reply.message, "hello, admitted");

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
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
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

    let mut call = client.lots_of_greetings(authenticated_options()).await?;
    call.send(greeter::HelloRequest {
        name: "one".to_owned(),
    })
    .await?;
    call.send(greeter::HelloRequest {
        name: "two".to_owned(),
    })
    .await?;
    let error = call
        .close_and_recv()
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
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_concurrent_requests(Some(1))
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
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
    observed_metrics.wait_for_code(Code::Unavailable).await;
    assert_eq!(observed_metrics.codes(), vec![Code::Unavailable]);

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
        let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
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
async fn webtransport_request_reset_cancels_only_one_rpc() -> TestResult {
    let metrics = RecordingMetrics::default();
    let server_metrics = metrics.clone();
    let server = spawn_webtransport_greeter_server(move |server| {
        server.set_metrics(server_metrics);
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let mut request = RpcRequest::new(
        greeter::GreeterClient::<()>::SERVICE,
        "LotsOfReplies",
        greeter::HelloRequest {
            name: "cancel".to_owned(),
        }
        .encode_to_vec(),
    )
    .with_kind(RpcKind::ServerStreaming)
    .with_timeout_nanos(TEST_TIMEOUT.as_nanos().try_into()?);
    request.metadata.insert(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    );
    trevrpc::webtransport::write_frame(
        &mut send,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    let first = trevrpc::webtransport::read_frame::<RpcStreamFrame>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    assert_eq!(
        greeter::HelloReply::decode(first.body.as_slice())?.message,
        "first"
    );

    send.reset(1)?;
    tokio::time::timeout(
        Duration::from_millis(500),
        metrics.wait_for_code(Code::Cancelled),
    )
    .await
    .expect("WebTransport request reset should promptly cancel the handler");
    let reply = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "after cancellation".to_owned(),
            },
            authenticated_options(),
        )
        .await?;
    assert_eq!(reply.message, "hello, after cancellation");
    drop(recv);

    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_server_streaming_deadline_while_response_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            short_authenticated_options(),
        )
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    assert_eq!(first.message, "first");

    let error = replies
        .next()
        .await
        .expect("deadline error should be emitted")
        .expect_err("pending response should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_server_streaming_deadline_while_response_pending() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let mut replies = greeter_client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "cancel".to_owned(),
            },
            short_authenticated_options(),
        )
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    assert_eq!(first.message, "first");

    let error = replies
        .next()
        .await
        .expect("deadline error should be emitted")
        .expect_err("pending response should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_client_streaming_deadline_drops_pending_upload() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let call = client
        .lots_of_greetings(short_authenticated_options())
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let error = call
        .close_and_recv()
        .await
        .expect_err("pending upload should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_client_streaming_deadline_drops_pending_upload() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let call = greeter_client
        .lots_of_greetings(short_authenticated_options())
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let error = call
        .close_and_recv()
        .await
        .expect_err("pending upload should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_bidi_deadline_drops_pending_upload_and_response() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client.bidi_hello(short_authenticated_options()).await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let error = replies
        .recv()
        .await
        .expect_err("pending bidi response should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_bidi_deadline_drops_pending_upload_and_response() -> TestResult {
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let mut replies = greeter_client
        .bidi_hello(short_authenticated_options())
        .await?;
    tokio::time::sleep(Duration::from_millis(60)).await;
    let error = replies
        .recv()
        .await
        .expect_err("pending bidi response should hit deadline");
    assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_status_drops_pending_request_stream() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "RejectUpload",
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    let error = replies
        .recv()
        .await
        .expect_err("terminal status should reject the stream");

    assert_eq!(error.into_status().code(), Code::PermissionDenied);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_terminal_status_drops_pending_request_stream() -> TestResult {
    let server = spawn_webtransport_greeter_server(register_reject_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = trevrpc::advanced::RawWebTransport::new(session.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "RejectUpload",
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    let error = replies
        .recv()
        .await
        .expect_err("terminal status should reject the stream");

    assert_eq!(error.into_status().code(), Code::PermissionDenied);
    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_response_status_drains_fin_without_stop_sending() -> TestResult {
    let (server_endpoint, cert_der) = make_server_endpoint(&fast_server_options())?;
    let server_addr = server_endpoint.local_addr()?;
    let (stopped_tx, stopped_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let incoming = tokio::time::timeout(TEST_TIMEOUT, server_endpoint.accept())
            .await?
            .ok_or_else(|| std::io::Error::other("server endpoint closed before accept"))?;
        let connection = incoming.await?;
        let (mut send, mut recv) = connection.accept_bi().await?;

        let _request = trevrpc::quinn::read_frame::<RpcRequest>(
            &mut recv,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )
        .await?;
        read_quinn_fin(&mut recv).await?;

        trevrpc::quinn::write_frame(
            &mut send,
            &RpcStreamFrame::status(Status::ok()),
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )
        .await?;
        tokio::time::sleep(Duration::from_millis(20)).await;
        send.finish()?;
        let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
        let _ = stopped_tx.send(stopped);
        connection.close(0_u32.into(), b"test complete");

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    let endpoint = make_client_endpoint(cert_der)?;
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
    let mut response = transport
        .streaming_call(
            RpcRequest::new("example.greeter.Greeter", "StatusOnly", Vec::new())
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::empty(),
        )
        .await?;

    let frame = tokio::time::timeout(TEST_TIMEOUT, response.next())
        .await?
        .expect("response status should arrive")?;

    assert_eq!(
        frame.frame_kind(),
        Some(trevrpc::RpcStreamFrameKind::Status)
    );
    assert!(frame.status_value().is_ok());
    drop(response);
    assert_eq!(stopped_rx.await?, None);

    close_client(endpoint, connection).await;
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn quinn_rejects_response_frame_after_terminal_status() -> TestResult {
    let (server_endpoint, cert_der) = make_server_endpoint(&fast_server_options())?;
    let server_addr = server_endpoint.local_addr()?;
    let (done_tx, done_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let incoming = tokio::time::timeout(TEST_TIMEOUT, server_endpoint.accept())
            .await?
            .ok_or_else(|| std::io::Error::other("server endpoint closed before accept"))?;
        let connection = incoming.await?;
        let (mut send, mut recv) = connection.accept_bi().await?;

        let _request = trevrpc::quinn::read_frame::<RpcRequest>(
            &mut recv,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )
        .await?;
        read_quinn_fin(&mut recv).await?;

        trevrpc::quinn::write_frame(
            &mut send,
            &RpcStreamFrame::status(Status::ok()),
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )
        .await?;
        trevrpc::quinn::write_frame(
            &mut send,
            &RpcStreamFrame::message(b"unexpected".to_vec()),
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )
        .await?;
        send.finish()?;
        let _ = done_rx.await;
        connection.close(0_u32.into(), b"test complete");

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    let endpoint = make_client_endpoint(cert_der)?;
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
    let mut response = transport
        .streaming_call(
            RpcRequest::new("example.greeter.Greeter", "StatusThenMessage", Vec::new())
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::empty(),
        )
        .await?;

    let error = tokio::time::timeout(TEST_TIMEOUT, response.next())
        .await?
        .expect("post-terminal frame should produce an error")
        .expect_err("post-terminal frame should be rejected");
    assert_eq!(error.into_status().code(), Code::InvalidArgument);
    drop(response);
    let _ = done_tx.send(());

    close_client(endpoint, connection).await;
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn quinn_terminal_request_status_drains_fin_without_stop_sending() -> TestResult {
    let server = spawn_greeter_server(register_status_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let request = RpcRequest::new(
        greeter::GreeterClient::<()>::SERVICE,
        "StatusUpload",
        Vec::new(),
    )
    .with_kind(RpcKind::ClientStreaming);

    trevrpc::quinn::write_frame(
        &mut send,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    trevrpc::quinn::write_frame(
        &mut send,
        &RpcStreamFrame::status(Status::ok()),
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    tokio::time::sleep(Duration::from_millis(20)).await;
    send.finish()?;

    let frame = trevrpc::quinn::read_frame::<RpcStreamFrame>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    assert_eq!(
        frame.frame_kind(),
        Some(trevrpc::RpcStreamFrameKind::Status)
    );
    assert!(frame.status_value().is_ok());
    read_quinn_fin(&mut recv).await?;

    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    assert_eq!(stopped, None);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_ok_surfaces_local_upload_error() -> TestResult {
    let server = spawn_greeter_server(register_accept_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "AcceptUpload",
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    replies
        .send(greeter::HelloRequest {
            name: "uploaded".to_owned(),
        })
        .await?;
    replies.close_send()?;

    assert!(replies.recv().await?.is_none());
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_terminal_ok_surfaces_local_upload_error() -> TestResult {
    let server = spawn_webtransport_greeter_server(register_accept_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = trevrpc::advanced::RawWebTransport::new(session.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            greeter::GreeterClient::<()>::SERVICE,
            "AcceptUpload",
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    replies
        .send(greeter::HelloRequest {
            name: "uploaded".to_owned(),
        })
        .await?;
    replies.close_send()?;

    assert!(replies.recv().await?.is_none());
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
async fn webtransport_shutdown_closes_active_sessions() -> TestResult {
    let server = spawn_webtransport_greeter_server(|_| {})?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    server.shutdown().await?;
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("client should observe WebTransport server shutdown");

    let error = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "after shutdown".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("RPC on drained WebTransport session should fail");
    assert!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable
    ));

    close_webtransport_client(client, session).await;
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
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
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
async fn webtransport_shutdown_is_bounded_with_pending_unary_handler() -> TestResult {
    let started = Arc::new(Notify::new());
    let started_server = Arc::clone(&started);
    let server = spawn_webtransport_greeter_server(move |server| {
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
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = trevrpc::advanced::RawWebTransport::new(session.clone());
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
        .expect("pending WebTransport unary handler should start");
    server.shutdown().await?;
    let result = tokio::time::timeout(TEST_TIMEOUT, call)
        .await
        .expect("pending WebTransport client call should finish after shutdown")?;
    let error = result.expect_err("shutdown should fail the pending WebTransport RPC");
    assert!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable | Code::DeadlineExceeded
    ));

    close_webtransport_client(client, session).await;
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
        let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
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
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_metrics(metrics);
    })?;
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
    observed_metrics.wait_for_code(Code::InvalidArgument).await;
    assert_pre_handler_metrics(&observed_metrics, Code::InvalidArgument);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_malformed_request_frames_return_invalid_argument_status() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;

    send.write_all(&2_u32.to_be_bytes()).await?;
    send.write_all(&[0xff, 0xff]).await?;
    send.finish()?;

    let response = trevrpc::webtransport::read_frame::<RpcResponse>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;

    assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);
    observed_metrics.wait_for_code(Code::InvalidArgument).await;
    assert_pre_handler_metrics(&observed_metrics, Code::InvalidArgument);

    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_unknown_request_stream_frame_kind_returns_invalid_argument_status() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let request = RpcRequest::new(
        greeter::GreeterClient::<()>::SERVICE,
        "LotsOfGreetings",
        Vec::new(),
    )
    .with_kind(RpcKind::ClientStreaming);

    trevrpc::quinn::write_frame(
        &mut send,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    trevrpc::quinn::write_frame(
        &mut send,
        &RpcStreamFrame {
            kind: 99,
            status: Code::Ok.as_u32(),
            message: String::new(),
            body: Vec::new(),
            metadata: trevrpc::Metadata::new(),
        },
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    send.finish()?;

    let frame = trevrpc::quinn::read_frame::<RpcStreamFrame>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;

    assert_eq!(
        frame.frame_kind(),
        Some(trevrpc::RpcStreamFrameKind::Status)
    );
    assert_eq!(Code::from_u32(frame.status), Code::InvalidArgument);

    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_initial_request_timeout_rejects_partial_header() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&[0, 0]).await?;

    let response = read_raw_quinn_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    observed_metrics.wait_for_code(Code::DeadlineExceeded).await;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_initial_request_timeout_rejects_partial_body() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&8_u32.to_be_bytes()).await?;
    send.write_all(&[1]).await?;

    let response = read_raw_quinn_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    observed_metrics.wait_for_code(Code::DeadlineExceeded).await;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded);
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_initial_request_timeout_rejects_partial_header() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    send.write_all(&[0, 0]).await?;

    let response = trevrpc::webtransport::read_frame::<RpcResponse>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    observed_metrics.wait_for_code(Code::DeadlineExceeded).await;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded);
    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_initial_request_timeout_rejects_partial_body() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    send.write_all(&8_u32.to_be_bytes()).await?;
    send.write_all(&[1]).await?;

    let response = read_raw_webtransport_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::DeadlineExceeded);
    observed_metrics.wait_for_code(Code::DeadlineExceeded).await;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded);
    close_webtransport_client(client, session).await;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_large_partial_initial_body_does_not_hold_request_permit() -> TestResult {
    const LARGE_FRAME_SIZE: usize = 1 << 30;
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_frame_size(LARGE_FRAME_SIZE)
                .with_max_concurrent_requests(Some(1))
                .with_initial_request_timeout(Some(TEST_TIMEOUT)),
        );
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let (mut send, _recv) = connection.open_bi().await?;
    let large_frame_size = u32::try_from(LARGE_FRAME_SIZE)?;
    send.write_all(&large_frame_size.to_be_bytes()).await?;
    send.write_all(&[1]).await?;

    let reply = client
        .say_hello(
            greeter::HelloRequest {
                name: "after partial".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;
    assert_eq!(reply.message, "hello, after partial");

    let _ = send.reset(1_u32.into());
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_large_partial_initial_body_does_not_hold_request_permit() -> TestResult {
    const LARGE_FRAME_SIZE: usize = 1 << 30;
    let server = spawn_webtransport_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_frame_size(LARGE_FRAME_SIZE)
                .with_max_concurrent_requests(Some(1))
                .with_initial_request_timeout(Some(TEST_TIMEOUT)),
        );
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, _recv) = session.open_bi().await?;
    let large_frame_size = u32::try_from(LARGE_FRAME_SIZE)?;
    send.write_all(&large_frame_size.to_be_bytes()).await?;
    send.write_all(&[1]).await?;

    let reply = greeter_client
        .say_hello(
            greeter::HelloRequest {
                name: "after partial".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;
    assert_eq!(reply.message, "hello, after partial");

    let _ = send.reset(1);
    close_webtransport_client(client, session).await;
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
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    assert_eq!(stopped, Some(1_u32.into()));
    close_client(endpoint, connection).await;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_oversized_initial_frame_is_rejected_before_body() -> TestResult {
    let server = spawn_webtransport_greeter_server(|_| {})?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let oversized = (trevrpc::framing::DEFAULT_MAX_FRAME_SIZE + 1)
        .try_into()
        .expect("default frame size should fit in u32");
    send.write_all(&u32::to_be_bytes(oversized)).await?;

    let response = read_raw_webtransport_response(&mut recv).await?;

    assert_eq!(Code::from_u32(response.status), Code::ResourceExhausted);
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    assert!(
        stopped.is_some(),
        "server should stop the oversized request stream"
    );
    close_webtransport_client(client, session).await;
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
    client: greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
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
            let left = format!("load-client-{index}-a");
            let right = format!("load-client-{index}-b");
            let response = run_client_greetings(
                &client,
                &[left.as_str(), right.as_str()],
                authenticated_options(),
            )
            .await?;
            assert_eq!(
                response.message,
                format!("load-client-{index}-a,load-client-{index}-b")
            );
        }
        _ => {
            let left = format!("load-bidi-{index}-a");
            let right = format!("load-bidi-{index}-b");
            let responses = run_bidi_greetings(
                &client,
                &[left.as_str(), right.as_str()],
                authenticated_options(),
            )
            .await?;
            assert_eq!(responses.len(), 2);
            assert!(
                responses
                    .iter()
                    .all(|message| message.starts_with("echo, load-bidi-"))
            );
        }
    }

    Ok(())
}

async fn hold_server_stream_open(
    client: &greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
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
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    let endpoint = make_server_endpoint(server.options())?;
    spawn_configured_greeter_server(endpoint, server)
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

fn register_accept_upload_route(server: &mut Server) {
    server.route_streaming(
        greeter::GreeterClient::<()>::SERVICE,
        "AcceptUpload",
        trevrpc::RpcKind::BidirectionalStreaming,
        |_body, _requests| async { Ok(trevrpc::stream::empty()) },
    );
}

fn register_status_upload_route(server: &mut Server) {
    server.route_streaming(
        greeter::GreeterClient::<()>::SERVICE,
        "StatusUpload",
        trevrpc::RpcKind::ClientStreaming,
        |_body, mut requests| async move {
            match requests.next().await {
                None => Ok(trevrpc::stream::empty()),
                Some(Ok(_)) => Err(trevrpc::Error::from(Status::invalid_argument(
                    "unexpected request message after status",
                ))),
                Some(Err(error)) => Err(error),
            }
        },
    );
}

fn spawn_greeter_server_with_endpoint(
    (endpoint, cert_der): (quinn::Endpoint, CertificateDer<'static>),
    configure: impl FnOnce(&mut Server),
) -> TestResult<RunningServer> {
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    spawn_configured_greeter_server((endpoint, cert_der), server)
}

fn spawn_configured_greeter_server(
    (endpoint, cert_der): (quinn::Endpoint, CertificateDer<'static>),
    mut server: Server,
) -> TestResult<RunningServer> {
    let addr = endpoint.local_addr()?;
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
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    let (endpoint, cert_der) = make_server_endpoint_with_alpns(
        &[trevrpc::ALPN, web_transport_quinn::ALPN.as_bytes()],
        trevrpc::quinn::TransportMode::WebTransport,
        server.options(),
    )?;
    let addr = endpoint.local_addr()?;
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

async fn read_raw_webtransport_response(
    recv: &mut web_transport_quinn::RecvStream,
) -> TestResult<RpcResponse> {
    Ok(tokio::time::timeout(
        TEST_TIMEOUT,
        trevrpc::webtransport::read_frame::<RpcResponse>(
            recv,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        ),
    )
    .await??)
}

async fn read_quinn_fin(recv: &mut quinn::RecvStream) -> TestResult {
    let mut scratch = [0_u8; 1];
    let read = tokio::time::timeout(TEST_TIMEOUT, recv.read(&mut scratch)).await??;
    assert_eq!(read, None, "stream should end without trailing data");
    Ok(())
}

async fn connect_client(
    server: &RunningServer,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
)> {
    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

    Ok((endpoint, connection, client))
}

async fn connect_webtransport_client(
    server: &RunningWebTransportServer,
) -> TestResult<(
    web_transport_quinn::Client,
    web_transport_quinn::Session,
    greeter::GreeterClient<trevrpc::advanced::RawWebTransport>,
)> {
    let webtransport_client = make_webtransport_client(server)?;
    let session = webtransport_client
        .connect(web_transport_quinn::proto::ConnectRequest::new(
            webtransport_url(server, "/trevrpc")?,
        ))
        .await?;
    let transport = trevrpc::advanced::RawWebTransport::new(session.clone());
    let greeter_client = greeter::GreeterClient::new(transport);

    Ok((webtransport_client, session, greeter_client))
}

async fn connect_http3_client(
    server: &RunningWebTransportServer,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<Http3Transport>,
)> {
    let (endpoint, connection, transport) = connect_http3_transport(server).await?;
    Ok((endpoint, connection, greeter::GreeterClient::new(transport)))
}

async fn connect_http3_transport(
    server: &RunningWebTransportServer,
) -> TestResult<(quinn::Endpoint, quinn::Connection, Http3Transport)> {
    let endpoint = make_http3_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let (mut driver, sender) =
        h3::client::new(h3_quinn::Connection::new(connection.clone())).await?;
    tokio::spawn(async move {
        let _ = driver.wait_idle().await;
    });
    let transport = Http3Transport {
        sender: Arc::new(AsyncMutex::new(sender)),
        authority: format!("localhost:{}", server.addr.port()),
    };
    Ok((endpoint, connection, transport))
}

async fn http3_status(
    transport: &Http3Transport,
    method: http::Method,
    path: &str,
    content_type: Option<&str>,
) -> TestResult<http::StatusCode> {
    let mut stream = transport.open(method, path, content_type).await?;
    stream.finish().await?;
    Ok(stream.recv_response().await?.status())
}

async fn http3_status_with_content_types(
    transport: &Http3Transport,
    method: http::Method,
    path: &str,
    content_types: &[&str],
) -> TestResult<http::StatusCode> {
    let mut stream = transport
        .open_with_content_types(method, path, content_types)
        .await?;
    stream.finish().await?;
    Ok(stream.recv_response().await?.status())
}

fn validate_http3_response(response: &http::Response<()>) -> trevrpc::Result<()> {
    if response.status() != http::StatusCode::OK
        || response
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            != Some("application/trevrpc")
    {
        return Err(trevrpc::Error::transport(std::io::Error::other(format!(
            "unexpected HTTP/3 response: {response:?}"
        ))));
    }
    Ok(())
}

async fn read_http3_body<S>(
    stream: &mut h3::client::RequestStream<S, Bytes>,
) -> trevrpc::Result<Vec<u8>>
where
    S: h3::quic::RecvStream,
{
    let mut body = Vec::new();
    while let Some(mut chunk) = stream
        .recv_data()
        .await
        .map_err(trevrpc::Error::transport)?
    {
        let len = chunk.remaining();
        body.extend_from_slice(&chunk.copy_to_bytes(len));
    }
    Ok(body)
}

fn decode_single_frame<M>(frame: &[u8]) -> trevrpc::Result<M>
where
    M: Message + Default,
{
    if frame.len() < 4 {
        return Err(trevrpc::Error::transport(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "HTTP/3 response ended before the TrevRPC frame header",
        )));
    }
    let len = u32::from_be_bytes(frame[..4].try_into().expect("frame header is four bytes"));
    if usize::try_from(len).ok() != Some(frame.len() - 4) {
        return Err(trevrpc::Error::from(Status::invalid_argument(
            "HTTP/3 response did not contain exactly one TrevRPC frame",
        )));
    }
    M::decode(&frame[4..]).map_err(trevrpc::Error::from)
}

async fn read_http3_frame_body(
    recv: &mut H3RecvStream,
    chunk: &mut Bytes,
) -> trevrpc::Result<Option<Vec<u8>>> {
    let mut header = [0_u8; 4];
    if !read_http3_exact(recv, chunk, &mut header, true).await? {
        return Ok(None);
    }
    let len = usize::try_from(u32::from_be_bytes(header)).expect("u32 should fit usize");
    if len > trevrpc::framing::DEFAULT_MAX_FRAME_SIZE {
        return Err(trevrpc::Error::FrameTooLarge {
            len,
            max: trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        });
    }
    let mut body = vec![0_u8; len];
    read_http3_exact(recv, chunk, &mut body, false).await?;
    Ok(Some(body))
}

async fn read_http3_exact(
    recv: &mut H3RecvStream,
    chunk: &mut Bytes,
    output: &mut [u8],
    allow_clean_eof: bool,
) -> trevrpc::Result<bool> {
    let mut offset = 0;
    while offset < output.len() {
        while chunk.is_empty() {
            let Some(mut next) = recv.recv_data().await.map_err(trevrpc::Error::transport)? else {
                if allow_clean_eof && offset == 0 {
                    return Ok(false);
                }
                return Err(trevrpc::Error::transport(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "HTTP/3 body ended in the middle of a TrevRPC frame",
                )));
            };
            *chunk = next.copy_to_bytes(next.remaining());
        }
        let len = (output.len() - offset).min(chunk.len());
        chunk.copy_to_slice(&mut output[offset..offset + len]);
        offset += len;
    }
    Ok(true)
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

fn short_authenticated_options() -> CallOptions {
    CallOptions::new()
        .with_timeout(Duration::from_millis(50))
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

async fn run_client_greetings<T>(
    client: &greeter::GreeterClient<T>,
    names: &[&str],
    options: CallOptions,
) -> trevrpc::Result<greeter::HelloReply>
where
    T: RpcTransport,
{
    let mut call = client.lots_of_greetings(options).await?;
    for name in names {
        call.send(greeter::HelloRequest {
            name: (*name).to_owned(),
        })
        .await?;
    }
    call.close_and_recv().await
}

async fn run_bidi_greetings<T>(
    client: &greeter::GreeterClient<T>,
    names: &[&str],
    options: CallOptions,
) -> trevrpc::Result<Vec<String>>
where
    T: RpcTransport,
{
    let mut call = client.bidi_hello(options).await?;
    for name in names {
        call.send(greeter::HelloRequest {
            name: (*name).to_owned(),
        })
        .await?;
    }
    call.close_send()?;

    let mut messages = Vec::new();
    while let Some(reply) = call.recv().await? {
        messages.push(reply.message);
    }
    Ok(messages)
}

fn make_server_endpoint(
    options: &ServerOptions,
) -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    make_server_endpoint_with_alpns(
        &[trevrpc::ALPN],
        trevrpc::quinn::TransportMode::Native,
        options,
    )
}

fn make_server_endpoint_with_alpns(
    alpns: &[&[u8]],
    mode: trevrpc::quinn::TransportMode,
    options: &ServerOptions,
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
    trevrpc::quinn::configure_server_config(&mut server_config, options, mode);

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
    trevrpc::quinn::configure_server_config(
        &mut server_config,
        &ServerOptions::new(),
        trevrpc::quinn::TransportMode::Native,
    );

    Ok((
        quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn make_client_endpoint(cert_der: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    make_client_endpoint_with_alpn(cert_der, trevrpc::ALPN)
}

fn make_http3_client_endpoint(cert_der: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;
    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::HTTP3_ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    trevrpc::quinn::configure_client_config(
        &mut client_config,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        trevrpc::quinn::TransportMode::WebTransport,
    );
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
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
    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    trevrpc::quinn::configure_client_config(
        &mut client_config,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        trevrpc::quinn::TransportMode::Native,
    );
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}
