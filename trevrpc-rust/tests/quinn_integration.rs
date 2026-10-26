#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use bytes::{Buf, Bytes};
use futures_util::StreamExt;
use prost::Message;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use quinn::rustls::server::WebPkiClientVerifier;
use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};
use trevrpc::client::{CallOptions, ChannelConfig, RpcTransport, StreamingRpcTransport};
use trevrpc::server::{
    CancellationSource, MetadataValueAuthorizer, Metrics, RequestContext, RpcFinished, RpcStarted,
    Server, ServerOptions,
};
use trevrpc::{Code, RpcKind, RpcRequest, Status};

#[allow(dead_code)]
#[path = "../examples/shared/greeter.rs"]
mod greeter;
#[macro_use]
mod support;

const AUTH_TOKEN: &str = "integration-token";
const TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn reject_http3_admission(request: &trevrpc::server::HttpAdmissionRequest<'_>) -> bool {
    assert!(request.headers.iter().any(|header| {
        header.name.eq_ignore_ascii_case("content-type")
            && header.value.eq_ignore_ascii_case(b"application/trevrpc")
    }));
    false
}

const fn reject_webtransport_admission(
    _request: &trevrpc::server::WebTransportAdmissionRequest<'_>,
) -> bool {
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

/// Answers with `response`, a response header and trailing metadata naming the RPC shape.
fn successful_response<T>(
    context: &RequestContext,
    shape: &str,
    response: T,
) -> core::result::Result<T, Status> {
    context.set_response_header("x-rpc-header", shape)?;
    context.set_response_metadata("x-rpc-shape", shape)?;
    Ok(response)
}

#[trevrpc::async_trait]
impl greeter::Greeter for TestGreeter {
    async fn say_hello(
        &self,
        context: RequestContext,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        successful_response(
            &context,
            "unary",
            greeter::HelloReply {
                message: format!("hello, {}", request.name),
            },
        )
    }

    async fn lots_of_replies(
        &self,
        context: RequestContext,
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        if request.name == "cancel" {
            let replies: trevrpc::BoxStream<greeter::HelloReply> = Box::pin(
                trevrpc::stream::from_iter([greeter::HelloReply {
                    message: "first".to_owned(),
                }])
                .chain(futures_util::stream::pending()),
            );
            return successful_response(&context, "server-streaming", replies);
        }

        successful_response(
            &context,
            "server-streaming",
            trevrpc::stream::from_iter([
                greeter::HelloReply {
                    message: format!("hello, {}", request.name),
                },
                greeter::HelloReply {
                    message: format!("goodbye, {}", request.name),
                },
            ]),
        )
    }

    async fn lots_of_greetings(
        &self,
        context: RequestContext,
        mut requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        let mut names = Vec::new();

        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        successful_response(
            &context,
            "client-streaming",
            greeter::HelloReply {
                message: names.join(","),
            },
        )
    }

    async fn bidi_hello(
        &self,
        context: RequestContext,
        requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        let replies: trevrpc::BoxStream<greeter::HelloReply> = Box::pin(requests.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("echo, {}", request.name),
            })
        }));
        successful_response(&context, "bidirectional-streaming", replies)
    }
}

struct RunningServer {
    addr: SocketAddr,
    cert_der: CertificateDer<'static>,
    ready: watch::Receiver<bool>,
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

type H3SendRequest = h3::client::SendRequest<h3_quinn::OpenStreams, Bytes>;
type H3RecvStream = h3::client::RequestStream<h3_quinn::RecvStream, Bytes>;
type H3ClientSendStream = h3::client::RequestStream<h3_quinn::SendStream<Bytes>, Bytes>;

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
    async fn call(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
    ) -> trevrpc::Result<trevrpc::ResponseEnvelope<Bytes>> {
        let mut stream = self
            .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
            .await?;
        let mut frames = trevrpc::framing::encode_protobuf_frame(
            trevrpc::framing::frame_type::REQUEST,
            &request,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )?;
        frames.extend(trevrpc::framing::encode_message_frame(
            &body,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )?);
        stream
            .send_data(Bytes::from(frames))
            .await
            .map_err(trevrpc::Error::transport)?;
        stream.finish().await.map_err(trevrpc::Error::transport)?;
        let response = stream
            .recv_response()
            .await
            .map_err(trevrpc::Error::transport)?;
        validate_http3_response(&response)?;
        let (_send, recv) = stream.split();
        trevrpc::client::collect_unary_response(http3_response_stream(recv, None)).await
    }
}

#[trevrpc::async_trait]
impl StreamingRpcTransport for Http3Transport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: trevrpc::BoxStream<Vec<u8>>,
    ) -> trevrpc::Result<trevrpc::BoxStream<trevrpc::ResponseFrame>> {
        let stream = self
            .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
            .await?;
        let (mut send, mut recv) = stream.split();
        let write_task = tokio::spawn(async move {
            let frame = trevrpc::framing::encode_protobuf_frame(
                trevrpc::framing::frame_type::REQUEST,
                &request,
                trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
            )?;
            send.send_data(Bytes::from(frame))
                .await
                .map_err(trevrpc::Error::transport)?;
            while let Some(body) = request_body.next().await.transpose()? {
                let frame = trevrpc::framing::encode_message_frame(
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
        Ok(http3_response_stream(recv, Some(write_task)))
    }
}

/// An HTTP/3 response body read as a byte stream.
struct H3Body {
    recv: H3RecvStream,
    chunk: Bytes,
}

impl support::RawRecv for H3Body {
    async fn read_some(&mut self, buf: &mut [u8]) -> trevrpc::Result<Option<usize>> {
        while self.chunk.is_empty() {
            let Some(mut next) = self
                .recv
                .recv_data()
                .await
                .map_err(trevrpc::Error::transport)?
            else {
                return Ok(None);
            };
            self.chunk = next.copy_to_bytes(next.remaining());
        }
        let len = buf.len().min(self.chunk.len());
        self.chunk
            .copy_to_slice(buf.get_mut(..len).unwrap_or_default());
        Ok(Some(len))
    }
}

fn http3_response_stream(
    recv: H3RecvStream,
    write_task: Option<JoinHandle<trevrpc::Result<()>>>,
) -> trevrpc::BoxStream<trevrpc::ResponseFrame> {
    Box::pin(futures_util::stream::unfold(
        (
            H3Body {
                recv,
                chunk: Bytes::new(),
            },
            write_task,
        ),
        |(mut body, mut write_task)| async move {
            let frame = match support::read_response_frame(&mut body).await {
                Ok(Some(frame)) => frame,
                Ok(None) => return None,
                Err(error) => return Some((Err(error), (body, write_task))),
            };
            if frame.is_status()
                && let Some(task) = write_task.take()
                && !task.is_finished()
            {
                task.abort();
            }
            Some((Ok(frame), (body, write_task)))
        },
    ))
}

async fn wait_for_server_ready(ready: &watch::Receiver<bool>) -> TestResult {
    let mut ready = ready.clone();
    tokio::time::timeout(TEST_TIMEOUT, ready.wait_for(|is_ready| *is_ready)).await??;
    Ok(())
}

async fn signal_ready_then_wait_for_shutdown(
    ready: watch::Sender<bool>,
    shutdown: oneshot::Receiver<()>,
) {
    let _ = ready.send(true);
    let _ = shutdown.await;
}

#[derive(Clone, Default)]
struct RecordingMetrics {
    started: Arc<Mutex<Vec<RpcStarted>>>,
    finished: Arc<Mutex<Vec<RpcFinished>>>,
    codes: Arc<Mutex<Vec<Code>>>,
    notify: Arc<Notify>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl RecordingMetrics {
    fn started(&self) -> Vec<RpcStarted> {
        lock(&self.started).clone()
    }

    fn finished(&self) -> Vec<RpcFinished> {
        lock(&self.finished).clone()
    }

    fn codes(&self) -> Vec<Code> {
        lock(&self.codes).clone()
    }

    async fn wait_for_code(&self, code: Code) -> TestResult {
        let wait = async {
            loop {
                if self.codes().contains(&code) {
                    return;
                }

                self.notify.notified().await;
            }
        };

        tokio::time::timeout(TEST_TIMEOUT, wait).await?;
        Ok(())
    }
}

impl Metrics for RecordingMetrics {
    fn rpc_started(&self, event: &RpcStarted) {
        lock(&self.started).push(event.clone());
    }

    fn rpc_finished(&self, event: &RpcFinished) {
        lock(&self.finished).push(event.clone());
        lock(&self.codes).push(event.code);
        self.notify.notify_waiters();
    }
}

fn assert_pre_handler_metrics(metrics: &RecordingMetrics, code: Code) -> TestResult {
    let started = metrics.started();
    let finished = metrics.finished();
    let ([started_event], [finished_event]) = (started.as_slice(), finished.as_slice()) else {
        return Err(format!(
            "expected one started and one finished metric\nstarted metrics: {started:#?}\nfinished metrics: {finished:#?}"
        )
        .into());
    };
    ensure_eq!(started_event.service, "");
    ensure_eq!(started_event.method, "");
    ensure_eq!(started_event.request_body_len, 0);
    ensure_eq!(finished_event.service, "");
    ensure_eq!(finished_event.method, "");
    ensure_eq!(finished_event.request_body_len, 0);
    ensure_eq!(finished_event.response_body_len, 0);
    ensure_eq!(finished_event.code, code);
    Ok(())
}

#[tokio::test]
async fn quinn_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "unary".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, unary");

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "server stream".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    ensure_eq!(messages, ["hello, server stream", "goodbye, server stream"]);

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    ensure_eq!(summary.message, "one,two");

    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
    ensure_eq!(messages, ["echo, left", "echo, right"]);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

/// Records the thread each call starts on.
#[derive(Clone, Default)]
struct ThreadMetrics(Arc<Mutex<Vec<Option<String>>>>);

impl Metrics for ThreadMetrics {
    fn rpc_started(&self, _event: &RpcStarted) {
        lock(&self.0).push(std::thread::current().name().map(str::to_owned));
    }

    fn rpc_finished(&self, _event: &RpcFinished) {}
}

#[tokio::test]
async fn connection_threads_run_every_call_of_a_connection() -> TestResult {
    let threads = ThreadMetrics::default();
    let recorded = threads.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(server.options().clone().with_connection_threads(2));
        server.set_metrics(recorded);
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "unary".to_owned(),
        })
        .await?;
    ensure_eq!(reply.message, "hello, unary");
    let summary = run_client_greetings(&client, &["one", "two"], CallOptions::new()).await?;
    ensure_eq!(summary.message, "one,two");
    let messages = run_bidi_greetings(&client, &["left", "right"], CallOptions::new()).await?;
    ensure_eq!(messages, ["echo, left", "echo, right"]);

    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    let names = lock(&threads.0).clone();
    ensure_eq!(names.len(), 3);
    ensure!(
        names
            .iter()
            .all(|name| name.as_deref() == names[0].as_deref()
                && name
                    .as_deref()
                    .is_some_and(|name| name.starts_with("trevrpc-connections-"))),
        "calls ran on {names:?}"
    );
    Ok(())
}

#[tokio::test]
async fn quinn_preserves_success_metadata_for_every_rpc_shape() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let unary = client
        .say_hello(&greeter::HelloRequest {
            name: "metadata".to_owned(),
        })
        .options(|_| authenticated_options())
        .with_response()
        .await?;
    assert_response_shape(unary.metadata(), "unary");

    let mut server_stream = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "metadata".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    while let Some(reply) = server_stream.next().await {
        reply?;
    }
    assert_response_shape(
        server_stream
            .terminal_metadata()
            .expect("clean server stream should retain terminal metadata"),
        "server-streaming",
    );

    let client_stream = client
        .lots_of_greetings()
        .options(|_| authenticated_options())
        .await?;
    client_stream
        .send(greeter::HelloRequest {
            name: "metadata".to_owned(),
        })
        .await?;
    let client_stream = client_stream.close_and_recv_with_response().await?;
    assert_response_shape(client_stream.metadata(), "client-streaming");

    let bidi = client
        .bidi_hello()
        .options(|_| authenticated_options())
        .await?;
    let (sender, mut responses) = bidi.split();
    let send_task = tokio::spawn(async move {
        sender
            .send(greeter::HelloRequest {
                name: "metadata".to_owned(),
            })
            .await?;
        sender.finish()
    });
    let receive_task = tokio::spawn(async move {
        while let Some(response) = responses.next().await {
            response?;
        }
        Ok::<_, trevrpc::Error>(
            responses
                .terminal_metadata()
                .cloned()
                .expect("clean bidi stream should retain terminal metadata"),
        )
    });
    send_task.await??;
    let bidi_metadata = receive_task.await??;
    assert_response_shape(&bidi_metadata, "bidirectional-streaming");

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

fn assert_response_shape_header(headers: &trevrpc::Metadata, expected: &str) {
    assert_eq!(
        headers.get("x-rpc-header").map(Vec::as_slice),
        Some(expected.as_bytes())
    );
}

fn assert_response_shape(metadata: &trevrpc::Metadata, expected: &str) {
    assert_eq!(
        metadata.get("x-rpc-shape").map(Vec::as_slice),
        Some(expected.as_bytes())
    );
}

/// Records every panic raised on the current test thread, including those that tokio catches
/// inside spawned tasks and would otherwise only log.
///
/// Quinn guards its API contracts with `debug_assert!`s, such as the one in `RecvStream::drop`
/// that rejects a stream with a stale blocked-reader waker. A violation panics inside a detached
/// server task, so the RPC still succeeds and only the captured test output shows it. These
/// tests run on a current-thread runtime, so the client, the server and its pump tasks all share
/// the test's thread, which is how a panic is attributed to the test that caused it.
mod task_panics {
    use std::sync::{Mutex, Once, PoisonError};

    static INSTALL: Once = Once::new();
    static PANICS: Mutex<Vec<(Option<String>, String)>> = Mutex::new(Vec::new());

    /// Installs the recording hook once for the whole test binary, chaining the previous hook so
    /// panic output is unchanged.
    pub fn install() {
        INSTALL.call_once(|| {
            let previous = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |info| {
                let thread = std::thread::current().name().map(str::to_owned);
                let mut panics = PANICS.lock().unwrap_or_else(PoisonError::into_inner);
                panics.push((thread, info.to_string()));
                drop(panics);
                previous(info);
            }));
        });
    }

    /// Returns, and clears, the panics raised on the current thread.
    pub fn take_current_thread() -> Vec<String> {
        let current = std::thread::current().name().map(str::to_owned);
        let mut panics = PANICS.lock().unwrap_or_else(PoisonError::into_inner);
        let (mine, others): (Vec<_>, Vec<_>) =
            panics.drain(..).partition(|(thread, _)| *thread == current);
        *panics = others;
        drop(panics);
        mine.into_iter().map(|(_, message)| message).collect()
    }
}

/// Lets detached server tasks, such as the request pump and the post-response drain, finish.
async fn settle_detached_server_tasks() {
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn assert_no_task_panics() {
    let panics = task_panics::take_current_thread();
    assert!(
        panics.is_empty(),
        "a dependency contract was violated inside a server task: {panics:#?}"
    );
}

#[tokio::test]
async fn quinn_client_streaming_does_not_violate_quinn_recv_stream_contract() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    for round in 0..16 {
        let names = ["one", "two", "three", "four"];
        let summary =
            run_client_greetings(&client, &names[..=round % 4], CallOptions::new()).await?;
        ensure_eq!(summary.message, names[..=round % 4].join(","));
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn quinn_bidirectional_streaming_does_not_violate_quinn_recv_stream_contract() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    for round in 0..16 {
        let names = ["left", "right", "up", "down"];
        let replies = run_bidi_greetings(&client, &names[..=round % 4], CallOptions::new()).await?;
        ensure_eq!(replies.len(), round % 4 + 1);
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

/// Registers `BackpressuredUpload`, a client-streaming route that waits before it reads its body,
/// then answers with the number of messages it read.
fn register_backpressured_upload_route(server: &mut Server) {
    server.route_streaming(
        support::greeter_path("BackpressuredUpload"),
        RpcKind::ClientStreaming,
        |_body, mut requests| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let mut count = 0_usize;
            while let Some(request) = requests.next().await {
                request?;
                count = count.saturating_add(1);
            }
            let reply = greeter::HelloReply {
                message: count.to_string(),
            }
            .encode_to_vec();
            let replies: trevrpc::BoxStream<Vec<u8>> =
                Box::pin(trevrpc::stream::from_iter([reply]));
            Ok(replies)
        },
    );
}

/// Uploads more than the request pump's queue holds while the handler waits, so the whole upload,
/// FIN included, is buffered by QUIC while the pump waits on backpressure and watches for a
/// reset. The pump then reads the whole request to EOF without blocking on the transport, which
/// must leave no reader waker behind: Quinn asserts that when the stream is dropped.
async fn backpressured_upload<T: StreamingRpcTransport>(transport: &T) -> TestResult {
    const MESSAGES: usize = 8;

    let call = trevrpc::client::client_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
        transport,
        &support::greeter_path("BackpressuredUpload"),
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;
    for _ in 0..MESSAGES {
        call.send(greeter::HelloRequest {
            name: "x".repeat(IGNORED_UPLOAD_MESSAGE),
        })
        .await?;
    }
    let reply = call.close_and_recv().await?;
    ensure_eq!(reply.message, MESSAGES.to_string());
    Ok(())
}

#[tokio::test]
async fn quinn_backpressured_upload_does_not_violate_quinn_recv_stream_contract() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(register_backpressured_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    backpressured_upload(&trevrpc::client::QuicTransport::new(connection.clone())).await?;

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn http3_backpressured_upload_does_not_violate_quinn_recv_stream_contract() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_http_enabled(true));
        register_backpressured_upload_route(server);
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    backpressured_upload(&transport).await?;

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn webtransport_backpressured_upload_does_not_violate_quinn_recv_stream_contract()
-> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(register_backpressured_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    backpressured_upload(&support::WebTransportRpc::new(session.clone())).await?;

    settle_detached_server_tasks().await;
    close_webtransport_client(client, session).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

/// The HTTP/3 request pump notices a reset of a request body the handler is not reading, through
/// h3's `received_reset`, whether or not unread data is buffered ahead of the reset.
#[tokio::test]
async fn http3_request_reset_cancels_handler_that_is_not_reading_the_body() -> TestResult {
    task_panics::install();
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_http_enabled(true));
        register_ignored_upload_route(server, &source_tx);
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;

    for messages in IGNORED_UPLOAD_MESSAGES {
        let stream = transport
            .open(http::Method::POST, "/trevrpc", Some("application/trevrpc"))
            .await?;
        let (mut send, _recv) = stream.split();
        send.send_data(ignored_upload_frames(messages)?).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        send.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);
        expect_peer_reset(&mut source_rx).await?;
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

/// The WebTransport request pump notices a reset of a request body the handler is not reading,
/// whether or not unread data is buffered ahead of the reset.
#[tokio::test]
async fn webtransport_request_reset_cancels_handler_that_is_not_reading_the_body() -> TestResult {
    task_panics::install();
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(|server| register_ignored_upload_route(server, &source_tx))?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;

    for messages in IGNORED_UPLOAD_MESSAGES {
        let (mut send, _recv) = session.open_bi().await?;
        support::write_raw(&mut send, &ignored_upload_frames(messages)?).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        send.reset(1)?;
        expect_peer_reset(&mut source_rx).await?;
    }

    settle_detached_server_tasks().await;
    close_webtransport_client(client, session).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

/// The handler answers at once and the client then finishes its request, so the post-response
/// drain reads the request side to EOF.
#[tokio::test]
async fn quinn_post_response_drain_does_not_violate_quinn_recv_stream_contract() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;

    for _ in 0..8 {
        let (mut send, _recv) = reject_with_open_request(&connection).await?;
        for _ in 0..3 {
            support::write_message(&mut send, &[]).await?;
        }
        send.finish()?;
        ensure_eq!(
            tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??,
            None
        );
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

/// Registers `IgnoredUpload`, a client-streaming route whose handler holds its request body
/// without reading it and reports the source of its cancellation to `source_tx`.
fn register_ignored_upload_route(
    server: &mut Server,
    source_tx: &mpsc::UnboundedSender<CancellationSource>,
) {
    let source_tx = source_tx.clone();
    server.route_streaming_with_context(
        support::greeter_path("IgnoredUpload"),
        RpcKind::ClientStreaming,
        move |context, _body, requests| {
            let source_tx = source_tx.clone();
            async move {
                // Hold the body without polling it: dropping it would stop the request.
                tokio::spawn(async move {
                    let _unread_requests = requests;
                    let _ = source_tx.send(context.cancelled_signal().await);
                });
                let replies: trevrpc::BoxStream<Vec<u8>> =
                    Box::pin(futures_util::stream::pending());
                Ok(replies)
            }
        },
    );
}

/// The size of each `IgnoredUpload` message. Two fill the request pump's 64 KiB queue, so the pump
/// stops reading and waits for room, watching the transport instead.
const IGNORED_UPLOAD_MESSAGE: usize = 48 * 1024;

/// The upload sizes of the reset tests: just enough to fill the pump's queue, and more, so that
/// unread data is buffered behind it.
const IGNORED_UPLOAD_MESSAGES: [usize; 2] = [2, 6];

/// The request frames of an `IgnoredUpload` call: REQUEST, then `messages` MESSAGE frames.
fn ignored_upload_frames(messages: usize) -> TestResult<Bytes> {
    let request =
        RpcRequest::new(support::greeter_path("IgnoredUpload")).with_kind(RpcKind::ClientStreaming);
    let mut frames = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;
    for _ in 0..messages {
        frames.extend(trevrpc::framing::encode_message_frame(
            &vec![0; IGNORED_UPLOAD_MESSAGE],
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )?);
    }
    Ok(Bytes::from(frames))
}

async fn expect_peer_reset(
    source_rx: &mut mpsc::UnboundedReceiver<CancellationSource>,
) -> TestResult {
    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .ok_or("handler should report its cancellation source")?;
    ensure_eq!(source, CancellationSource::PeerReset);
    Ok(())
}

/// A peer reset of the request stream cancels a handler that is not reading the body, once the
/// pump's queue is full, whether or not unread request data is buffered behind it.
#[tokio::test]
async fn quinn_request_reset_cancels_handler_that_is_not_reading_the_body() -> TestResult {
    task_panics::install();
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(|server| register_ignored_upload_route(server, &source_tx))?;
    let (endpoint, connection, _client) = connect_client(&server).await?;

    for messages in IGNORED_UPLOAD_MESSAGES {
        let (mut send, _recv) = connection.open_bi().await?;
        send.write_all(&ignored_upload_frames(messages)?).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        send.reset(1_u32.into())?;
        expect_peer_reset(&mut source_rx).await?;
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn webtransport_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_webtransport_client(&server).await?;

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "unary".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, unary");

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "server stream".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    ensure_eq!(messages, ["hello, server stream", "goodbye, server stream"]);

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    ensure_eq!(summary.message, "one,two");

    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
    ensure_eq!(messages, ["echo, left", "echo, right"]);

    close_webtransport_client(endpoint, connection).await?;
    server.shutdown().await
}

/// Calls `SayHello` on a new stream of `session`, offering or referring to connection metadata
/// set 1, and returns the set its HEADERS confirmed (zero for none) and its status.
async fn say_hello_with_connection_metadata(
    session: &web_transport_quinn::Session,
    offered: Option<trevrpc::Metadata>,
) -> TestResult<(u32, Status)> {
    let (mut send, mut recv) = session.open_bi().await?;
    let request = RpcRequest {
        connection_metadata: Some(trevrpc::ConnectionMetadata {
            id: 1,
            metadata: offered.unwrap_or_default(),
        }),
        ..RpcRequest::new(greeter::greeter_paths::SAY_HELLO)
    };
    support::write_request(&mut send, &request).await?;
    support::write_message(
        &mut send,
        &greeter::HelloRequest {
            name: "set".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    send.finish()?;
    let mut confirmed = 0;
    loop {
        let frame = tokio::time::timeout(
            TEST_TIMEOUT,
            support::read_any_frame(&mut recv, trevrpc::framing::DEFAULT_MAX_FRAME_SIZE),
        )
        .await??
        .ok_or("the response ended without a STATUS")?;
        match frame.frame_type {
            trevrpc::framing::frame_type::HEADERS => {
                confirmed =
                    trevrpc::framing::decode_payload::<trevrpc::RpcHeaders>(&frame.payload)?
                        .connection_metadata;
            }
            trevrpc::framing::frame_type::STATUS => {
                let status =
                    trevrpc::framing::decode_payload::<trevrpc::RpcStatus>(&frame.payload)?;
                return Ok((confirmed, Status::from(status)));
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn webtransport_sessions_keep_connection_metadata() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let token = trevrpc::Metadata::from([(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    )]);
    let (client, session, _) = connect_webtransport_client(&server).await?;
    let (confirmed, status) = say_hello_with_connection_metadata(&session, Some(token)).await?;
    ensure_eq!(
        (confirmed, status.code()),
        (1, Code::Ok),
        "{}",
        status.message()
    );
    // A reference resolves to the stored set, which authorizes the call.
    let (confirmed, status) = say_hello_with_connection_metadata(&session, None).await?;
    ensure_eq!(
        (confirmed, status.code()),
        (0, Code::Ok),
        "{}",
        status.message()
    );

    // Another session has its own sets.
    let (other_client, other, _) = connect_webtransport_client(&server).await?;
    let (_, status) = say_hello_with_connection_metadata(&other, None).await?;
    ensure_eq!(status.code(), Code::InvalidArgument);

    close_webtransport_client(other_client, other).await?;
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn http3_round_trips_unary_and_all_streaming_modes() -> TestResult {
    let server = spawn_http3_greeter_server(|server| {
        server.set_options(server.options().clone().with_http_enabled(true));
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_http3_client(&server).await?;

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "unary".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, unary");

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "server stream".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    ensure_eq!(messages, ["hello, server stream", "goodbye, server stream"]);

    let summary = run_client_greetings(&client, &["one", "two"], authenticated_options()).await?;
    ensure_eq!(summary.message, "one,two");
    let messages = run_bidi_greetings(&client, &["left", "right"], authenticated_options()).await?;
    ensure_eq!(messages, ["echo, left", "echo, right"]);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn direct_http3_rejects_webtransport_connect_without_disabling_post() -> TestResult {
    let server = spawn_http3_greeter_server(|_| {})?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;

    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/trevrpc"),
        )
        .await?,
        http::StatusCode::OK
    );
    close_client(endpoint, connection).await?;

    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);
    assert_webtransport_connect_rejected(
        expect_webtransport_connect_error(&client, request).await?,
        web_transport_quinn::http::StatusCode::FORBIDDEN,
    )?;

    server.shutdown().await
}

#[tokio::test]
async fn webtransport_is_opt_in_while_http3_post_works() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_webtransport_enabled(false)
                .with_http_enabled(true),
        );
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/trevrpc"),
        )
        .await?,
        http::StatusCode::OK
    );
    close_client(endpoint, connection).await?;

    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);
    // The server does not advertise WebTransport, so the session cannot be established.
    expect_webtransport_connect_error(&client, request).await?;

    server.shutdown().await
}

#[tokio::test]
async fn http3_post_is_opt_in_while_webtransport_works() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_webtransport_enabled(true)
                .with_http_enabled(false),
        );
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/trevrpc"),
        )
        .await?,
        http::StatusCode::NOT_FOUND
    );
    close_client(endpoint, connection).await?;

    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    assert_greeter_ready(&greeter_client).await?;
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn h3_alpn_is_not_negotiated_unless_http3_or_webtransport_is_enabled() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_webtransport_enabled(false));
    })?;
    wait_for_server_ready(&server.ready).await?;
    let endpoint = make_http3_client_endpoint(server.cert_der.clone())?;
    let result = tokio::time::timeout(TEST_TIMEOUT, endpoint.connect(server.addr, "localhost")?)
        .await
        .expect("ALPN mismatch handshake should complete");
    ensure!(result.is_err(), "h3 must not be negotiated");
    endpoint.close(0_u32.into(), b"test complete");

    // Native QUIC is unaffected.
    let (endpoint, connection, client) = connect_client(&server).await?;
    assert_greeter_ready(&client).await?;
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn one_port_endpoint_accepts_quinn_http3_and_webtransport_clients() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(server.options().clone().with_http_enabled(true));
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;

    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let quinn_client =
        greeter::GreeterClient::new(trevrpc::client::QuicTransport::new(connection.clone()));
    let quinn_reply = quinn_client
        .say_hello(&greeter::HelloRequest {
            name: "quinn".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(quinn_reply.message, "hello, quinn");
    close_client(endpoint, connection).await?;

    let (webtransport_client, session, greeter_client) =
        connect_webtransport_client(&server).await?;
    let webtransport_reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "webtransport".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(webtransport_reply.message, "hello, webtransport");

    let (http3_endpoint, http3_connection, http3_client) = connect_http3_client(&server).await?;
    let http3_reply = http3_client
        .say_hello(&greeter::HelloRequest {
            name: "http3".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(http3_reply.message, "hello, http3");

    let webtransport_reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "webtransport interleaved".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(
        webtransport_reply.message,
        "hello, webtransport interleaved"
    );
    let http3_reply = http3_client
        .say_hello(&greeter::HelloRequest {
            name: "http3 interleaved".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(http3_reply.message, "hello, http3 interleaved");

    close_client(http3_endpoint, http3_connection).await?;
    close_webtransport_client(webtransport_client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn h3_shutdown_is_bounded_with_active_http3_and_webtransport_rpcs() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_http_enabled(true)
                .with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (webtransport_client, session, webtransport_rpc) =
        connect_webtransport_client(&server).await?;
    let (http3_endpoint, http3_connection, http3_rpc) = connect_http3_client(&server).await?;

    let mut webtransport_replies = webtransport_rpc
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let mut http3_replies = http3_rpc
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(
        webtransport_replies
            .next()
            .await
            .expect("WebTransport stream should yield first response")?
            .message,
        "first"
    );
    ensure_eq!(
        http3_replies
            .next()
            .await
            .expect("HTTP/3 stream should yield first response")?
            .message,
        "first"
    );

    tokio::time::timeout(TEST_TIMEOUT, server.shutdown())
        .await
        .expect("h3 shutdown should be bounded")?;
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("WebTransport session should close during shutdown");
    tokio::time::timeout(TEST_TIMEOUT, http3_connection.closed())
        .await
        .expect("HTTP/3 connection should close during shutdown");

    drop(webtransport_replies);
    drop(http3_replies);
    close_webtransport_client(webtransport_client, session).await?;
    http3_endpoint.close(0_u32.into(), b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, http3_endpoint.wait_idle())
        .await
        .expect("HTTP/3 endpoint should become idle");
    Ok(())
}

#[tokio::test]
async fn http3_rejects_invalid_path_method_and_media_type() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(server.options().clone().with_http_enabled(true));
        server.set_options(
            server
                .options()
                .clone()
                .with_http_admission(Some(reject_http3_admission)),
        );
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;

    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/wrong",
            Some("application/trevrpc")
        )
        .await?,
        http::StatusCode::NOT_FOUND
    );
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::GET,
            "/trevrpc",
            Some("application/trevrpc")
        )
        .await?,
        http::StatusCode::METHOD_NOT_ALLOWED
    );
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/json")
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("Application/TrevRPC")
        )
        .await?,
        http::StatusCode::FORBIDDEN
    );
    ensure_eq!(
        http3_status(
            &transport,
            http::Method::POST,
            "/trevrpc",
            Some("application/trevrpc; charset=utf-8")
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    ensure_eq!(
        http3_status_with_content_types(
            &transport,
            http::Method::POST,
            "/trevrpc",
            &["application/trevrpc", "application/trevrpc"],
        )
        .await?,
        http::StatusCode::UNSUPPORTED_MEDIA_TYPE
    );

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn http3_enforces_stream_limits_and_survives_request_reset() -> TestResult {
    let (source_tx, _source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        server.set_options(
            fast_server_options()
                .with_http_enabled(true)
                .with_max_stream_messages(Some(1)),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    let client = greeter::GreeterClient::new(transport.clone());

    let call = client
        .lots_of_greetings()
        .options(|_| authenticated_options())
        .await?;
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
    ensure_eq!(error.into_status().code(), Code::ResourceExhausted);

    let (mut send, body) = start_http3_probe(&transport).await?;
    let recv = body.recv;
    // The server must contain the reset to this RPC and keep the connection usable without an
    // unbounded detached observer.
    send.stop_stream(h3::error::Code::H3_REQUEST_CANCELLED);

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "after cancellation".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, after cancellation");
    drop(recv);
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

/// Starts the cancellation probe over a raw HTTP/3 request and reads its first message.
async fn start_http3_probe(transport: &Http3Transport) -> TestResult<(H3ClientSendStream, H3Body)> {
    let mut request = RpcRequest::new("/lifecycle.Probe/Watch")
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
    let mut frames = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;
    frames.extend(trevrpc::framing::encode_message_frame(
        &[],
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?);
    send.send_data(Bytes::from(frames)).await?;
    validate_http3_response(&recv.recv_response().await?)?;
    let mut body = H3Body {
        recv,
        chunk: Bytes::new(),
    };
    expect_probe_ready(&mut body).await?;
    Ok((send, body))
}

#[tokio::test]
async fn http3_stop_sending_reports_peer_reset_to_request_context() -> TestResult {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        server.set_options(fast_server_options().with_http_enabled(true));
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (endpoint, connection, transport) = connect_http3_transport(&server).await?;
    let (send, mut body) = start_http3_probe(&transport).await?;

    // The handler is waiting to produce its next response, so no write is pending: only a
    // STOP_SENDING notification can tell the server the client stopped reading.
    body.recv
        .stop_sending(h3::error::Code::H3_REQUEST_CANCELLED);

    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .expect("HTTP/3 probe should report a cancellation source");
    ensure_eq!(source, CancellationSource::PeerReset);
    drop((send, body));
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_unexpected_path() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/wrong")?);

    let error = expect_webtransport_connect_error(&client, request).await?;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::NOT_FOUND)?;
    server.shutdown().await
}

/// Connects with the WebTransport protocols `protocols` offered, checks the protocol the server
/// selected, and calls `SayHello`.
async fn say_hello_over_webtransport_offering(
    protocols: &[&str],
    selected: Option<&str>,
) -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let client = make_webtransport_client(&server)?;
    let mut request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);
    for protocol in protocols {
        request = request.with_protocol(*protocol);
    }
    let session = client.connect(request).await?;
    ensure_eq!(session.protocol(), selected);
    let greeter_client =
        greeter::GreeterClient::new(support::WebTransportRpc::new(session.clone()));

    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "version".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;

    ensure_eq!(reply.message, "hello, version");
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_an_offer_without_trevrpc_1() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_protocol("trevrpc-2");

    let error = expect_webtransport_connect_error(&client, request).await?;

    assert_webtransport_connect_rejected(
        error,
        web_transport_quinn::http::StatusCode::BAD_REQUEST,
    )?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_a_list_of_offers_without_trevrpc_1() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_protocol("trevrpc-2")
            .with_protocol("other");

    let error = expect_webtransport_connect_error(&client, request).await?;

    assert_webtransport_connect_rejected(
        error,
        web_transport_quinn::http::StatusCode::BAD_REQUEST,
    )?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_accepts_an_offer_of_trevrpc_1() -> TestResult {
    say_hello_over_webtransport_offering(&["trevrpc-1"], Some("trevrpc-1")).await
}

#[tokio::test]
async fn webtransport_accepts_an_offer_that_includes_trevrpc_1() -> TestResult {
    say_hello_over_webtransport_offering(&["trevrpc-2", "trevrpc-1"], Some("trevrpc-1")).await
}

#[tokio::test]
async fn webtransport_accepts_no_offer_as_version_1() -> TestResult {
    say_hello_over_webtransport_offering(&[], None).await
}

#[tokio::test]
async fn webtransport_rejects_browser_origin_without_allowlist() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_header(
                web_transport_quinn::http::header::ORIGIN,
                web_transport_quinn::http::HeaderValue::from_static("https://example.invalid"),
            );

    let error = expect_webtransport_connect_error(&client, request).await?;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::FORBIDDEN)?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_allows_configured_browser_origin() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            server
                .options()
                .clone()
                .with_webtransport_allowed_origins(["https://example.invalid"]),
        );
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
    let transport = support::WebTransportRpc::new(session.clone());
    let greeter_client = greeter::GreeterClient::new(transport);

    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "origin".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;

    ensure_eq!(reply.message, "hello, origin");
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_admission_receives_generic_request_headers() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            server
                .options()
                .clone()
                .with_webtransport_admission(Some(require_webtransport_admission_header)),
        );
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let client = make_webtransport_client(&server)?;
    let denied =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);
    assert_webtransport_connect_rejected(
        expect_webtransport_connect_error(&client, denied).await?,
        web_transport_quinn::http::StatusCode::FORBIDDEN,
    )?;

    let allowed =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?)
            .with_header(
                web_transport_quinn::http::HeaderName::from_static("x-trevrpc-admission"),
                web_transport_quinn::http::HeaderValue::from_static("allowed"),
            );
    let session = client.connect(allowed).await?;
    let rpc = greeter::GreeterClient::new(support::WebTransportRpc::new(session.clone()));
    let reply = rpc
        .say_hello(&greeter::HelloRequest {
            name: "admitted".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, admitted");

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_rejects_unexpected_authority_when_allowlist_is_set() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(
            server
                .options()
                .clone()
                .with_webtransport_allowed_authorities(["expected.example"]),
        );
    })?;
    let client = make_webtransport_client(&server)?;
    let request =
        web_transport_quinn::proto::ConnectRequest::new(webtransport_url(&server, "/trevrpc")?);

    let error = expect_webtransport_connect_error(&client, request).await?;

    assert_webtransport_connect_rejected(error, web_transport_quinn::http::StatusCode::FORBIDDEN)?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_auth_failures_return_status_errors() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let error = client
        .say_hello(&greeter::HelloRequest {
            name: "missing auth".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await
        .expect_err("request without auth should fail");

    ensure_eq!(error.into_status().code(), Code::Unauthenticated);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_oversized_timeouts_are_rejected_over_the_wire() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let request = RpcRequest::new(support::greeter_path("Missing")).with_timeout_nanos(u64::MAX);

    let error = transport
        .call(request, Vec::new())
        .await
        .expect_err("an oversized timeout should be rejected");

    ensure_eq!(error.into_status().code(), Code::InvalidArgument);

    close_client(endpoint, connection).await?;
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

    let call = client
        .lots_of_greetings()
        .options(|_| authenticated_options())
        .await?;
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

    ensure_eq!(error.into_status().code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_response_stream_limits_return_resource_exhausted() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "limited".to_owned(),
        })
        .options(|_| authenticated_options().with_max_response_messages(Some(1)))
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    ensure_eq!(first.message, "hello, limited");

    let error = replies
        .next()
        .await
        .expect("response stream should yield limit error")
        .expect_err("second response should exceed client limit");
    ensure_eq!(error.into_status().code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await?;
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
    let (mut send, mut recv) = connection.open_bi().await?;
    let request = RpcRequest::new(support::greeter_path("SayHello"));

    support::write_request_and_message(
        &mut send,
        &request,
        &greeter::HelloRequest {
            name: "second".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    send.finish()?;
    let status = read_raw_quinn_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::Unavailable);
    read_quinn_fin(&mut recv).await?;
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, None);

    drop(hanging);
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_stream_concurrency_limit_returns_unavailable() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server
            .set_options(fast_server_options().with_max_concurrent_streams_per_connection(Some(1)));
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let mut hanging = greeter_client
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let first = hanging
        .next()
        .await
        .expect("response stream should yield first item")?;
    ensure_eq!(first.message, "first");

    let (mut send, mut recv) = session.open_bi().await?;
    let mut request = RpcRequest::new(support::greeter_path("SayHello"));
    request.metadata.insert(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    );
    support::write_request_and_message(
        &mut send,
        &request,
        &greeter::HelloRequest {
            name: "overloaded".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    send.finish()?;
    let status = read_raw_webtransport_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::Unavailable);

    let (mut send, mut recv) = session.open_bi().await?;
    let mut request =
        RpcRequest::new(support::greeter_path("LotsOfReplies")).with_kind(RpcKind::ServerStreaming);
    request.metadata.insert(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    );
    support::write_request_and_message(
        &mut send,
        &request,
        &greeter::HelloRequest {
            name: "overloaded streaming".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    send.finish()?;
    let status = support::read_lone_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::Unavailable);

    drop((hanging, recv));
    close_webtransport_client(client, session).await?;
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
    let (mut send, mut recv) = connection.open_bi().await?;
    let request = RpcRequest::new(support::greeter_path("SayHello"));

    support::write_request_and_message(
        &mut send,
        &request,
        &greeter::HelloRequest {
            name: "second".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    send.finish()?;
    let status = read_raw_quinn_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::Unavailable);
    read_quinn_fin(&mut recv).await?;
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, None);
    observed_metrics.wait_for_code(Code::Unavailable).await?;
    ensure_eq!(observed_metrics.codes(), vec![Code::Unavailable]);

    drop(hanging);
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

/// Serves `/lifecycle.LateFailure/Upload`, whose handler returns a pending response stream and
/// signals `ready_tx`, under a limit of zero request messages.
fn spawn_late_failure_server(
    metrics: RecordingMetrics,
    ready_tx: mpsc::UnboundedSender<()>,
) -> TestResult<RunningServer> {
    spawn_greeter_server(move |server| {
        server.set_options(
            fast_server_options()
                .with_max_stream_messages(Some(0))
                .with_stream_idle_timeout(None),
        );
        server.set_metrics(metrics);
        server.route_streaming(
            "/lifecycle.LateFailure/Upload",
            RpcKind::ClientStreaming,
            move |_body, requests| {
                let ready_tx = ready_tx.clone();
                async move {
                    let _ = ready_tx.send(());
                    let stream: trevrpc::BoxStream<Vec<u8>> = Box::pin(
                        futures_util::stream::unfold(requests, |requests| async move {
                            let _requests = requests;
                            std::future::pending().await
                        }),
                    );
                    Ok(stream)
                }
            },
        );
    })
}

#[tokio::test]
async fn quinn_late_request_limit_failure_settles_metrics_once_and_stops_upload() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let server = spawn_late_failure_server(metrics, ready_tx)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let request =
        RpcRequest::new("/lifecycle.LateFailure/Upload").with_kind(RpcKind::ClientStreaming);
    support::write_request(&mut send, &request).await?;
    tokio::time::timeout(TEST_TIMEOUT, ready_rx.recv())
        .await?
        .expect("handler should return its pending response stream");

    support::write_message(&mut send, b"over limit").await?;
    let status = support::read_lone_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::ResourceExhausted);
    read_quinn_fin(&mut recv).await?;
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped())
        .await??
        .expect("server should stop the rejected upload");
    ensure_eq!(stopped.into_inner(), 1);
    observed_metrics
        .wait_for_code(Code::ResourceExhausted)
        .await?;
    ensure_eq!(observed_metrics.codes(), vec![Code::ResourceExhausted]);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_late_request_limit_failure_stops_upload() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let (ready_tx, mut ready_rx) = mpsc::unbounded_channel();
    let server = spawn_late_failure_server(metrics, ready_tx)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let request =
        RpcRequest::new("/lifecycle.LateFailure/Upload").with_kind(RpcKind::ClientStreaming);
    support::write_request(&mut send, &request).await?;
    tokio::time::timeout(TEST_TIMEOUT, ready_rx.recv())
        .await?
        .expect("handler should return its pending response stream");

    support::write_message(&mut send, b"over limit").await?;
    let status = support::read_lone_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::ResourceExhausted);
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, Some(1), "server should stop the rejected upload");
    observed_metrics
        .wait_for_code(Code::ResourceExhausted)
        .await?;

    drop((send, recv));
    close_webtransport_client(client, session).await?;
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
        let transport = trevrpc::client::QuicTransport::new(connection.clone());
        let error = trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            &support::greeter_path("SayHello"),
            &greeter::HelloRequest {
                name: "refused".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .expect_err("RPC on refused connection should fail");
        ensure_eq!(error.into_status().code(), Code::Unavailable);
        connection.close(0_u32.into(), b"second done");
    }

    second_endpoint.close(0_u32.into(), b"second done");
    drop(hanging);
    close_client(endpoint, connection).await?;
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
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    ensure_eq!(first.message, "first");

    drop(replies);
    metrics.wait_for_code(Code::Cancelled).await?;

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_response_drop_reports_peer_reset_to_request_context() -> TestResult {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (endpoint, connection, _) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut responses = trevrpc::client::server_streaming::<_, _, greeter::HelloReply>(
        &transport,
        "/lifecycle.Probe/Watch",
        &greeter::HelloRequest::default(),
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;
    responses
        .next()
        .await
        .expect("probe should yield its first response")?;

    drop(responses);

    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .expect("probe should report a cancellation source");
    ensure_eq!(source, CancellationSource::PeerReset);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_shutdown_reports_server_shutdown_to_request_context() -> TestResult {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (_endpoint, connection, _) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut responses = trevrpc::client::server_streaming::<_, _, greeter::HelloReply>(
        &transport,
        "/lifecycle.Probe/Watch",
        &greeter::HelloRequest::default(),
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;
    responses
        .next()
        .await
        .expect("probe should yield its first response")?;

    let shutdown = tokio::spawn(server.shutdown());
    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .expect("probe should report a cancellation source");
    ensure_eq!(source, CancellationSource::ServerShutdown);
    shutdown.await??;
    drop(responses);
    Ok(())
}

#[tokio::test]
async fn webtransport_request_reset_is_contained() -> TestResult {
    let (source_tx, _source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let mut request = RpcRequest::new("/lifecycle.Probe/Watch")
        .with_kind(RpcKind::ServerStreaming)
        .with_timeout_nanos(TEST_TIMEOUT.as_nanos().try_into()?);
    request.metadata.insert(
        "authorization".to_owned(),
        format!("Bearer {AUTH_TOKEN}").into_bytes(),
    );
    support::write_request_and_message(&mut send, &request, &[]).await?;
    expect_probe_ready(&mut recv).await?;

    // Contain the reset to this RPC and preserve the session.
    send.reset(1)?;
    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "after cancellation".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, after cancellation");
    drop(recv);

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_stop_sending_reports_peer_reset_to_request_context() -> TestResult {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let request = RpcRequest::new("/lifecycle.Probe/Watch")
        .with_kind(RpcKind::ServerStreaming)
        .with_timeout_nanos(TEST_TIMEOUT.as_nanos().try_into()?);
    support::write_request_and_message(&mut send, &request, &[]).await?;
    expect_probe_ready(&mut recv).await?;

    recv.stop(1)?;

    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .expect("WebTransport probe should report a cancellation source");
    ensure_eq!(source, CancellationSource::PeerReset);
    drop((send, recv));
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_shutdown_closes_the_session() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "before shutdown".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    ensure_eq!(reply.message, "hello, before shutdown");

    tokio::time::timeout(TEST_TIMEOUT, server.shutdown()).await??;
    let error = tokio::time::timeout(TEST_TIMEOUT, session.closed()).await?;
    let web_transport_quinn::SessionError::WebTransportError(
        web_transport_quinn::WebTransportError::Closed(code, reason),
    ) = error
    else {
        return Err(format!("session should end with a WebTransport close, got {error}").into());
    };
    ensure_eq!(code, 0);
    ensure_eq!(reason, "server shutdown");
    drop(client);
    Ok(())
}

#[tokio::test]
async fn webtransport_connection_close_reports_connection_loss() -> TestResult {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        register_cancellation_probe_route(server, source_tx);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    let request = RpcRequest::new("/lifecycle.Probe/Watch")
        .with_kind(RpcKind::ServerStreaming)
        .with_timeout_nanos(TEST_TIMEOUT.as_nanos().try_into()?);
    support::write_request_and_message(&mut send, &request, &[]).await?;
    expect_probe_ready(&mut recv).await?;

    quinn::Connection::close(&session, 7_u32.into(), b"adversarial connection close");
    let source = tokio::time::timeout(TEST_TIMEOUT, source_rx.recv())
        .await?
        .expect("WebTransport probe should report a cancellation source");
    ensure_eq!(source, CancellationSource::ConnectionLost);
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("client session should close");
    drop((client, send, recv));

    server.shutdown().await
}

#[tokio::test]
async fn quinn_server_streaming_deadline_while_response_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| short_authenticated_options())
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    ensure_eq!(first.message, "first");

    let error = replies
        .next()
        .await
        .expect("deadline error should be emitted")
        .expect_err("pending response should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_server_streaming_deadline_while_response_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let mut replies = greeter_client
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| short_authenticated_options())
        .await?;
    let first = replies
        .next()
        .await
        .expect("response stream should yield first item")?;
    ensure_eq!(first.message, "first");

    let error = replies
        .next()
        .await
        .expect("deadline error should be emitted")
        .expect_err("pending response should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_client_streaming_deadline_while_upload_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let requests: trevrpc::BoxStream<greeter::HelloRequest> =
        Box::pin(futures_util::stream::pending());
    let error = client
        .lots_of_greetings()
        .options(|_| short_authenticated_options())
        .send_all(requests)
        .await
        .expect_err("pending upload should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_client_streaming_deadline_while_upload_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let requests: trevrpc::BoxStream<greeter::HelloRequest> =
        Box::pin(futures_util::stream::pending());
    let error = greeter_client
        .lots_of_greetings()
        .options(|_| short_authenticated_options())
        .send_all(requests)
        .await
        .expect_err("pending upload should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_bidi_deadline_while_response_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    let mut replies = client
        .bidi_hello()
        .options(|_| short_authenticated_options())
        .await?;
    let error = replies
        .recv()
        .await
        .expect_err("pending bidi response should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);
    drop(replies);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_bidi_deadline_while_response_pending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    let mut replies = greeter_client
        .bidi_hello()
        .options(|_| short_authenticated_options())
        .await?;
    let error = replies
        .recv()
        .await
        .expect_err("pending bidi response should hit deadline");
    ensure_eq!(error.into_status().code(), Code::DeadlineExceeded);
    drop(replies);

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_status_drops_pending_request_stream() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            &support::greeter_path("RejectUpload"),
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    let error = replies
        .recv()
        .await
        .expect_err("terminal status should reject the stream");

    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_terminal_status_drops_pending_request_stream() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = support::WebTransportRpc::new(session.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            &support::greeter_path("RejectUpload"),
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    let error = replies
        .recv()
        .await
        .expect_err("terminal status should reject the stream");

    ensure_eq!(error.into_status().code(), Code::PermissionDenied);
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_unary_request_fin_is_drained_without_stop_sending() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_max_concurrent_requests(Some(1)));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let request = RpcRequest::new(support::greeter_path("SayHello"));

    support::write_request_and_message(
        &mut send,
        &request,
        &greeter::HelloRequest {
            name: "delayed fin".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;

    let reply = read_raw_quinn_reply(&mut recv).await?;
    ensure_eq!(reply.status.code(), Code::Ok);
    let [body] = reply.messages.as_slice() else {
        return Err("a unary reply carries exactly one message".into());
    };
    ensure_eq!(
        greeter::HelloReply::decode(body.as_slice())?.message,
        "hello, delayed fin",
    );

    let concurrent = client
        .say_hello(&greeter::HelloRequest {
            name: "concurrent".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(concurrent.message, "hello, concurrent");

    send.finish()?;
    read_quinn_fin(&mut recv).await?;
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, None);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

fn rejected_bidi_request() -> RpcRequest {
    RpcRequest::new(support::greeter_path("RejectUpload"))
        .with_kind(RpcKind::BidirectionalStreaming)
}

/// Opens a call the handler rejects at once, leaves its request side open, and returns the
/// streams after reading the terminal status and the end of the response.
async fn reject_with_open_request(
    connection: &quinn::Connection,
) -> TestResult<(quinn::SendStream, quinn::RecvStream)> {
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &rejected_bidi_request()).await?;
    let status = tokio::time::timeout(TEST_TIMEOUT, support::read_lone_status(&mut recv)).await??;
    assert_eq!(status.code(), Code::PermissionDenied);
    read_quinn_fin(&mut recv).await?;
    Ok((send, recv))
}

#[tokio::test]
async fn quinn_open_request_side_is_drained_not_stopped_after_handler_failure() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, _recv) = reject_with_open_request(&connection).await?;

    // The response is complete but the request is still open: the server must not stop it, or
    // peers that tear the whole stream down would lose the status above.
    ensure!(
        tokio::time::timeout(Duration::from_millis(300), send.stopped())
            .await
            .is_err(),
        "server stopped the request side while the client was still sending"
    );
    support::write_message(&mut send, &[]).await?;
    send.finish()?;
    ensure_eq!(
        tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??,
        None
    );

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_request_discard_after_handler_failure_is_bounded_by_bytes() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, _recv) = reject_with_open_request(&connection).await?;

    // Keep sending past the 1 MiB discard bound; the server then stops the stream with code 1.
    let chunk = vec![0_u8; 64 * 1024];
    let writer = tokio::spawn(async move {
        for _ in 0..64 {
            if send.write_all(&chunk).await.is_err() {
                break;
            }
        }
        send
    });
    let send = tokio::time::timeout(TEST_TIMEOUT, writer).await??;
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped.map(quinn::VarInt::into_inner), Some(1));

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_request_discard_after_handler_failure_is_bounded_by_time() -> TestResult {
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (send, _recv) = reject_with_open_request(&connection).await?;

    // A client that neither finishes nor sends is stopped after about one second.
    let started = std::time::Instant::now();
    let stopped = tokio::time::timeout(Duration::from_secs(4), send.stopped()).await??;
    ensure_eq!(stopped.map(quinn::VarInt::into_inner), Some(1));
    ensure!(started.elapsed() >= Duration::from_millis(900));

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_open_request_side_is_drained_not_stopped_after_handler_failure() -> TestResult
{
    let server = spawn_greeter_server(register_reject_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    support::write_request(&mut send, &rejected_bidi_request()).await?;
    let status = tokio::time::timeout(TEST_TIMEOUT, support::read_lone_status(&mut recv)).await??;
    ensure_eq!(status.code(), Code::PermissionDenied);

    ensure!(
        tokio::time::timeout(Duration::from_millis(300), send.stopped())
            .await
            .is_err(),
        "server stopped the request side while the client was still sending"
    );
    send.finish()?;
    ensure_eq!(
        tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??,
        None
    );

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_unary_request_left_open_is_bounded_and_frees_the_stream_slot() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server
            .set_options(fast_server_options().with_max_concurrent_streams_per_connection(Some(1)));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    let (send, mut recv) = connection.open_bi().await?;
    let mut send = send;
    support::write_request_and_message(
        &mut send,
        &RpcRequest::new(support::greeter_path("SayHello")),
        &greeter::HelloRequest {
            name: "never finished".to_owned(),
        }
        .encode_to_vec(),
    )
    .await?;
    let reply = read_raw_quinn_reply(&mut recv).await?;
    ensure_eq!(reply.status.code(), Code::Ok);
    read_quinn_fin(&mut recv).await?;

    // The unfinished request must not pin the connection's only stream slot.
    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "second".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(reply.message, "hello, second");

    // The server stops the abandoned request after about one second.
    let started = std::time::Instant::now();
    let stopped = tokio::time::timeout(Duration::from_secs(4), send.stopped()).await??;
    ensure_eq!(stopped.map(quinn::VarInt::into_inner), Some(1));
    ensure!(started.elapsed() <= Duration::from_secs(3));

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_terminal_response_status_drains_fin_without_stop_sending() -> TestResult {
    let (server_endpoint, cert_der) = make_server_endpoint(&fast_server_options())?;
    let server_addr = server_endpoint.local_addr()?;
    let (stopped_tx, stopped_rx) = oneshot::channel();
    let (status_written_tx, status_written_rx) = oneshot::channel();
    let (finish_tx, finish_rx) = oneshot::channel();
    let server_task = tokio::spawn(async move {
        let incoming = tokio::time::timeout(TEST_TIMEOUT, server_endpoint.accept())
            .await?
            .ok_or_else(|| std::io::Error::other("server endpoint closed before accept"))?;
        let connection = incoming.await?;
        let (mut send, mut recv) = connection.accept_bi().await?;

        let _request = support::read_request(&mut recv).await?;
        let _message = support::read_message(&mut recv).await?;
        read_quinn_fin(&mut recv).await?;

        support::write_status(&mut send, Status::ok()).await?;
        let _ = status_written_tx.send(());
        let _ = finish_rx.await;
        send.finish()?;
        let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
        let _ = stopped_tx.send(stopped);
        connection.close(0_u32.into(), b"test complete");

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    let endpoint = make_client_endpoint(cert_der)?;
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut response = transport
        .streaming_call(
            RpcRequest::new("/example.greeter.Greeter/StatusOnly")
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::from_iter([Vec::new()]),
        )
        .await?;

    let (next_started_tx, next_started_rx) = oneshot::channel();
    let next_task = tokio::spawn(async move {
        let _ = next_started_tx.send(());
        let item = response.next().await;
        (item, response)
    });
    next_started_rx.await?;
    status_written_rx.await?;
    ensure!(
        !next_task.is_finished(),
        "terminal response status must wait for clean response FIN"
    );
    let _ = finish_tx.send(());
    let (frame, response) = tokio::time::timeout(TEST_TIMEOUT, next_task).await??;
    let frame = frame.expect("response status should arrive")?;

    ensure_eq!(frame, trevrpc::ResponseFrame::Status(Status::ok()));
    drop(response);
    ensure_eq!(stopped_rx.await?, None);

    close_client(endpoint, connection).await?;
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

        let _request = support::read_request(&mut recv).await?;
        let _message = support::read_message(&mut recv).await?;
        read_quinn_fin(&mut recv).await?;

        support::write_status(&mut send, Status::ok()).await?;
        support::write_message(&mut send, b"unexpected").await?;
        send.finish()?;
        let _ = done_rx.await;
        connection.close(0_u32.into(), b"test complete");

        Ok::<_, Box<dyn Error + Send + Sync>>(())
    });
    let endpoint = make_client_endpoint(cert_der)?;
    let connection = endpoint.connect(server_addr, "localhost")?.await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut response = transport
        .streaming_call(
            RpcRequest::new("/example.greeter.Greeter/StatusThenMessage")
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::from_iter([Vec::new()]),
        )
        .await?;

    let error = tokio::time::timeout(TEST_TIMEOUT, response.next())
        .await?
        .expect("post-terminal frame should produce an error")
        .expect_err("post-terminal frame should be rejected");
    ensure_eq!(error.into_status().code(), Code::Internal);
    drop(response);
    let _ = done_tx.send(());

    close_client(endpoint, connection).await?;
    server_task.await??;
    Ok(())
}

#[tokio::test]
async fn quinn_terminal_request_status_drains_fin_without_stop_sending() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(register_status_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    let request =
        RpcRequest::new(support::greeter_path("StatusUpload")).with_kind(RpcKind::ClientStreaming);

    support::write_request(&mut send, &request).await?;
    support::write_status(&mut send, Status::ok()).await?;

    let (read_started_tx, read_started_rx) = oneshot::channel();
    let response_task = tokio::spawn(async move {
        let _ = read_started_tx.send(());
        let status = support::read_lone_status(&mut recv).await;
        (status, recv)
    });
    read_started_rx.await?;
    ensure!(
        !response_task.is_finished(),
        "terminal request status must wait for clean request FIN"
    );
    send.finish()?;
    let (status, mut recv) = tokio::time::timeout(TEST_TIMEOUT, response_task).await??;
    ensure!(status?.is_ok());
    read_quinn_fin(&mut recv).await?;

    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, None);

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn quinn_terminal_ok_surfaces_local_upload_error() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(register_accept_upload_route)?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            &support::greeter_path("AcceptUpload"),
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    replies
        .send(greeter::HelloRequest {
            name: "uploaded".to_owned(),
        })
        .await?;
    replies.close_send()?;

    ensure!(replies.recv().await?.is_none());
    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
}

#[tokio::test]
async fn webtransport_terminal_ok_surfaces_local_upload_error() -> TestResult {
    let server = spawn_greeter_server(register_accept_upload_route)?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = support::WebTransportRpc::new(session.clone());
    let mut replies =
        trevrpc::client::bidirectional_streaming::<_, greeter::HelloRequest, greeter::HelloReply>(
            &transport,
            &support::greeter_path("AcceptUpload"),
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await?;

    replies
        .send(greeter::HelloRequest {
            name: "uploaded".to_owned(),
        })
        .await?;
    replies.close_send()?;

    ensure!(replies.recv().await?.is_none());
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_shutdown_closes_active_connections() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;
    assert_greeter_ready(&client).await?;

    server.shutdown().await?;

    let error = client
        .say_hello(&greeter::HelloRequest {
            name: "after shutdown".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .expect_err("RPC on drained connection should fail");
    ensure_eq!(error.into_status().code(), Code::Unavailable);

    close_client(endpoint, connection).await?;
    Ok(())
}

#[tokio::test]
async fn webtransport_shutdown_closes_active_sessions() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    assert_greeter_ready(&greeter_client).await?;

    server.shutdown().await?;
    tokio::time::timeout(TEST_TIMEOUT, session.closed())
        .await
        .expect("client should observe WebTransport server shutdown");

    let error = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "after shutdown".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .expect_err("RPC on drained WebTransport session should fail");
    ensure!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable
    ));

    close_webtransport_client(client, session).await?;
    Ok(())
}

#[tokio::test]
async fn http3_shutdown_closes_active_connections() -> TestResult {
    let server = spawn_http3_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_http3_client(&server).await?;
    assert_greeter_ready(&client).await?;

    server.shutdown().await?;
    tokio::time::timeout(TEST_TIMEOUT, connection.closed())
        .await
        .expect("client should observe HTTP/3 server shutdown");

    let error = client
        .say_hello(&greeter::HelloRequest {
            name: "after shutdown".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .expect_err("RPC on drained HTTP/3 connection should fail");
    ensure!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable
    ));

    close_client(endpoint, connection).await?;
    Ok(())
}

#[tokio::test]
async fn quinn_shutdown_is_bounded_with_pending_unary_handler() -> TestResult {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        server.set_options(
            fast_server_options().with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.route(support::greeter_path("Never"), move |_body| {
            let started_tx = started_tx.clone();
            async move {
                let _ = started_tx.send(());
                std::future::pending::<trevrpc::Result<Vec<u8>>>().await
            }
        });
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let call = tokio::spawn(async move {
        trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            &support::greeter_path("Never"),
            &greeter::HelloRequest {
                name: "shutdown".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await
    });

    tokio::time::timeout(TEST_TIMEOUT, started_rx.recv())
        .await
        .expect("pending unary handler should start")
        .expect("pending unary handler signal should remain open");
    server.shutdown().await?;
    let result = tokio::time::timeout(TEST_TIMEOUT, call)
        .await
        .expect("pending client call should finish after shutdown")?;
    let error = result.expect_err("shutdown should fail the pending RPC");
    ensure!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable | Code::DeadlineExceeded
    ));
    close_client(endpoint, connection).await?;
    Ok(())
}

#[tokio::test]
async fn webtransport_shutdown_is_bounded_with_pending_unary_handler() -> TestResult {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let server = spawn_greeter_server(move |server| {
        server.set_options(
            fast_server_options().with_graceful_shutdown_timeout(Some(Duration::from_millis(50))),
        );
        server.route(support::greeter_path("Never"), move |_body| {
            let started_tx = started_tx.clone();
            async move {
                let _ = started_tx.send(());
                std::future::pending::<trevrpc::Result<Vec<u8>>>().await
            }
        });
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let transport = support::WebTransportRpc::new(session.clone());
    let call = tokio::spawn(async move {
        trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            &support::greeter_path("Never"),
            &greeter::HelloRequest {
                name: "shutdown".to_owned(),
            },
            CallOptions::new().with_timeout(TEST_TIMEOUT),
        )
        .await
    });

    tokio::time::timeout(TEST_TIMEOUT, started_rx.recv())
        .await
        .expect("pending WebTransport unary handler should start")
        .expect("pending WebTransport unary handler signal should remain open");
    server.shutdown().await?;
    let result = tokio::time::timeout(TEST_TIMEOUT, call)
        .await
        .expect("pending WebTransport client call should finish after shutdown")?;
    let error = result.expect_err("shutdown should fail the pending WebTransport RPC");
    ensure!(matches!(
        error.into_status().code(),
        Code::Cancelled | Code::Unavailable | Code::DeadlineExceeded
    ));

    close_webtransport_client(client, session).await?;
    Ok(())
}

#[tokio::test]
async fn quinn_local_close_maps_to_cancelled() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    connection.close(0_u32.into(), b"client closed");

    let error = client
        .say_hello(&greeter::HelloRequest {
            name: "after local close".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(Duration::from_millis(100)))
        .await
        .expect_err("RPC on locally closed connection should fail");
    ensure_eq!(error.into_status().code(), Code::Cancelled);

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

    ensure!(result.is_err(), "ALPN mismatch should reject connection");

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

    ensure!(result.is_err(), "SNI mismatch should reject connection");

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
        let transport = trevrpc::client::QuicTransport::new(connection.clone());
        let status = trevrpc::client::unary::<_, _, greeter::HelloReply>(
            &transport,
            &support::greeter_path("SayHello"),
            &greeter::HelloRequest {
                name: "anonymous".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(100)),
        )
        .await
        .map_err(trevrpc::Error::into_status)
        .expect_err("server requiring mTLS should reject anonymous clients");
        ensure_eq!(status.code(), Code::Unavailable);
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

    // A REQUEST frame whose payload is a truncated protobuf varint.
    send.write_all(&[0x01, 0x02, 0xff, 0xff]).await?;
    send.finish()?;

    let status = support::read_lone_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::InvalidArgument);
    observed_metrics
        .wait_for_code(Code::InvalidArgument)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::InvalidArgument)?;

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_malformed_request_frames_return_invalid_argument_status() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;

    // A REQUEST frame whose payload is a truncated protobuf varint.
    send.write_all(&[0x01, 0x02, 0xff, 0xff]).await?;
    send.finish()?;

    let status = support::read_lone_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::InvalidArgument);
    observed_metrics
        .wait_for_code(Code::InvalidArgument)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::InvalidArgument)?;

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_misplaced_request_stream_frames_return_invalid_argument_status() -> TestResult {
    task_panics::install();
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let request = RpcRequest::new(support::greeter_path("LotsOfGreetings"))
        .with_kind(RpcKind::ClientStreaming);
    let second_request = trevrpc::framing::encode_protobuf_frame(
        trevrpc::framing::frame_type::REQUEST,
        &request,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?;

    // HEADERS and a second REQUEST have no place on a request stream.
    for misplaced in [
        trevrpc::framing::encode_frame(
            trevrpc::framing::frame_type::HEADERS,
            &[],
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        )?,
        second_request,
    ] {
        let (mut send, mut recv) = connection.open_bi().await?;
        support::write_request(&mut send, &request).await?;
        support::write_raw(&mut send, &misplaced).await?;
        send.finish()?;

        let status = support::read_lone_status(&mut recv).await?;

        ensure_eq!(status.code(), Code::InvalidArgument);
    }

    settle_detached_server_tasks().await;
    close_client(endpoint, connection).await?;
    server.shutdown().await?;
    assert_no_task_panics();
    Ok(())
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
    // One byte of a frame header that needs at least two.
    send.write_all(&[0x01]).await?;

    let status = read_raw_quinn_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::DeadlineExceeded);
    observed_metrics
        .wait_for_code(Code::DeadlineExceeded)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded)?;
    close_client(endpoint, connection).await?;
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
    // A REQUEST frame that declares eight payload bytes and delivers one.
    send.write_all(&[0x01, 0x08, 0x01]).await?;

    let status = read_raw_quinn_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::DeadlineExceeded);
    observed_metrics
        .wait_for_code(Code::DeadlineExceeded)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded)?;
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_initial_request_timeout_rejects_partial_header() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    // One byte of a frame header that needs at least two.
    send.write_all(&[0x01]).await?;

    let status = support::read_lone_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::DeadlineExceeded);
    observed_metrics
        .wait_for_code(Code::DeadlineExceeded)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded)?;
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_initial_request_timeout_rejects_partial_body() -> TestResult {
    let metrics = RecordingMetrics::default();
    let observed_metrics = metrics.clone();
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options().with_initial_request_timeout(Some(Duration::from_millis(50))),
        );
        server.set_metrics(metrics);
    })?;
    let (client, session, _greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    // A REQUEST frame that declares eight payload bytes and delivers one.
    send.write_all(&[0x01, 0x08, 0x01]).await?;

    let status = read_raw_webtransport_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::DeadlineExceeded);
    observed_metrics
        .wait_for_code(Code::DeadlineExceeded)
        .await?;
    assert_pre_handler_metrics(&observed_metrics, Code::DeadlineExceeded)?;
    close_webtransport_client(client, session).await?;
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
    send.write_all(&large_request_header(LARGE_FRAME_SIZE)?)
        .await?;
    send.write_all(&[1]).await?;

    let reply = client
        .say_hello(&greeter::HelloRequest {
            name: "after partial".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(reply.message, "hello, after partial");

    let _ = send.reset(1_u32.into());
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_large_partial_initial_body_does_not_hold_request_permit() -> TestResult {
    const LARGE_FRAME_SIZE: usize = 1 << 30;
    let server = spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_max_frame_size(LARGE_FRAME_SIZE)
                .with_max_concurrent_requests(Some(1))
                .with_initial_request_timeout(Some(TEST_TIMEOUT)),
        );
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, _recv) = session.open_bi().await?;
    send.write_all(&large_request_header(LARGE_FRAME_SIZE)?)
        .await?;
    send.write_all(&[1]).await?;

    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "after partial".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(reply.message, "hello, after partial");

    let _ = send.reset(1);
    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_oversized_initial_frame_is_rejected_before_body() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&large_request_header(
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE + 1,
    )?)
    .await?;

    let status = read_raw_quinn_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::ResourceExhausted);
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(stopped, Some(1_u32.into()));
    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_oversized_initial_frame_is_rejected_before_body() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;
    let (mut send, mut recv) = session.open_bi().await?;
    send.write_all(&large_request_header(
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE + 1,
    )?)
    .await?;

    let status = read_raw_webtransport_status(&mut recv).await?;

    ensure_eq!(status.code(), Code::ResourceExhausted);
    let stopped = tokio::time::timeout(TEST_TIMEOUT, send.stopped()).await??;
    ensure_eq!(
        stopped,
        None,
        "h3-WebTransport should use bounded stream drop instead of unsafe stop_sending"
    );

    let reply = greeter_client
        .say_hello(&greeter::HelloRequest {
            name: "after oversized".to_owned(),
        })
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(reply.message, "hello, after oversized");

    close_webtransport_client(client, session).await?;
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
                .say_hello(&greeter::HelloRequest { name: name.clone() })
                .options(|_| authenticated_options())
                .await?;
            Ok::<_, trevrpc::Error>((name, response.message))
        });
    }

    let mut completed = 0;
    while let Some(result) = calls.join_next().await {
        let (name, message) = result??;
        ensure_eq!(message, format!("hello, {name}"));
        completed += 1;
    }
    ensure_eq!(completed, 64);

    close_client(endpoint, connection).await?;
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

    let mut completed = 0_usize;
    for batch_start in (0..total_calls).step_by(batch_size.max(1)) {
        let batch_end = total_calls.min(batch_start.saturating_add(batch_size.max(1)));
        let mut calls = JoinSet::new();

        for index in batch_start..batch_end {
            let client = client.clone();
            calls.spawn(async move { run_mixed_call(client, index).await });
        }

        while let Some(result) = calls.join_next().await {
            result??;
            completed = completed.saturating_add(1);
        }
    }

    assert_eq!(completed, total_calls);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

async fn run_mixed_call(
    client: greeter::GreeterClient<trevrpc::client::QuicTransport>,
    index: usize,
) -> trevrpc::Result<()> {
    match index % 4 {
        0 => {
            let name = format!("load-unary-{index}");
            let response = client
                .say_hello(&greeter::HelloRequest { name: name.clone() })
                .options(|_| authenticated_options())
                .await?;
            assert_eq!(response.message, format!("hello, {name}"));
        }
        1 => {
            let name = format!("load-server-stream-{index}");
            let mut responses = client
                .lots_of_replies(&greeter::HelloRequest { name: name.clone() })
                .options(|_| authenticated_options())
                .await?;
            let mut count = 0_usize;
            while let Some(response) = responses.next().await {
                assert!(response?.message.contains(&name));
                count = count.saturating_add(1);
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
    client: &greeter::GreeterClient<trevrpc::client::QuicTransport>,
) -> TestResult<trevrpc::client::ResponseStream<greeter::HelloReply>> {
    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "cancel".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    let first = replies
        .next()
        .await
        .ok_or("response stream should yield first item")??;
    ensure_eq!(first.message, "first");

    Ok(replies)
}

fn spawn_greeter_server(configure: impl FnOnce(&mut Server)) -> TestResult<RunningServer> {
    let mut server = Server::new();
    server.set_options(fast_server_options());
    configure(&mut server);
    let endpoint = make_server_endpoint(server.options())?;
    spawn_configured_greeter_server(endpoint, server)
}

fn register_cancellation_probe_route(
    server: &mut Server,
    source_tx: mpsc::UnboundedSender<CancellationSource>,
) {
    server.route_streaming_with_context(
        "/lifecycle.Probe/Watch",
        RpcKind::ServerStreaming,
        move |context, _body, _requests| {
            let source_tx = source_tx.clone();
            async move {
                let probe = context.clone();
                tokio::spawn(async move {
                    let source = probe.cancelled_signal().await;
                    let _ = source_tx.send(source);
                });
                let first = greeter::HelloReply {
                    message: "ready".to_owned(),
                }
                .encode_to_vec();
                let stream: trevrpc::BoxStream<Vec<u8>> = Box::pin(
                    trevrpc::stream::from_iter([first]).chain(futures_util::stream::pending()),
                );
                Ok(successful_response(
                    &context,
                    "server-streaming-probe",
                    stream,
                )?)
            }
        },
    );
}

fn register_reject_upload_route(server: &mut Server) {
    server.route_streaming(
        support::greeter_path("RejectUpload"),
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
        support::greeter_path("AcceptUpload"),
        trevrpc::RpcKind::BidirectionalStreaming,
        |_body, _requests| async { Ok(trevrpc::stream::empty()) },
    );
}

fn register_status_upload_route(server: &mut Server) {
    server.route_streaming(
        support::greeter_path("StatusUpload"),
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
    let (ready_tx, ready_rx) = watch::channel(false);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_with_shutdown(
                endpoint,
                signal_ready_then_wait_for_shutdown(ready_tx, shutdown_rx),
            )
            .await
    });

    Ok(RunningServer {
        addr,
        cert_der,
        ready: ready_rx,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

fn spawn_http3_greeter_server(configure: impl FnOnce(&mut Server)) -> TestResult<RunningServer> {
    spawn_greeter_server(|server| {
        server.set_options(
            fast_server_options()
                .with_http_enabled(true)
                .with_webtransport_admission(Some(reject_webtransport_admission)),
        );
        configure(server);
    })
}

fn fast_server_options() -> ServerOptions {
    ServerOptions::new()
        .with_webtransport_enabled(true)
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
}

/// The header of a REQUEST frame that declares `payload_len` payload bytes.
fn large_request_header(payload_len: usize) -> TestResult<Vec<u8>> {
    let mut header = Vec::new();
    trevrpc::varint::push(&mut header, trevrpc::framing::frame_type::REQUEST)?;
    trevrpc::varint::push(&mut header, u64::try_from(payload_len)?)?;
    Ok(header)
}

/// Reads a response that must be a lone STATUS, as for failures before dispatch.
async fn read_raw_quinn_status(recv: &mut quinn::RecvStream) -> TestResult<Status> {
    Ok(tokio::time::timeout(TEST_TIMEOUT, support::read_lone_status(recv)).await??)
}

async fn read_raw_webtransport_status(
    recv: &mut web_transport_quinn::RecvStream,
) -> TestResult<Status> {
    Ok(tokio::time::timeout(TEST_TIMEOUT, support::read_lone_status(recv)).await??)
}

/// Reads a response through its terminal STATUS.
async fn read_raw_quinn_reply(recv: &mut quinn::RecvStream) -> TestResult<support::UnaryReply> {
    Ok(tokio::time::timeout(TEST_TIMEOUT, support::read_unary_reply(recv)).await??)
}

/// Reads the response headers and first message of the cancellation probe route.
async fn expect_probe_ready<R: support::RawRecv>(recv: &mut R) -> TestResult {
    let reply_headers =
        tokio::time::timeout(TEST_TIMEOUT, support::read_response_frame(recv)).await??;
    let Some(trevrpc::ResponseFrame::Headers(headers)) = reply_headers else {
        return Err(format!("expected response headers first, got {reply_headers:?}").into());
    };
    assert_response_shape_header(&headers, "server-streaming-probe");
    let first = tokio::time::timeout(TEST_TIMEOUT, support::read_response_frame(recv)).await??;
    let Some(trevrpc::ResponseFrame::Message(first)) = first else {
        return Err(format!("expected the first message, got {first:?}").into());
    };
    ensure_eq!(greeter::HelloReply::decode(first)?.message, "ready");
    Ok(())
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
    greeter::GreeterClient<trevrpc::client::QuicTransport>,
)> {
    wait_for_server_ready(&server.ready).await?;
    let endpoint = make_client_endpoint(server.cert_der.clone())?;
    let connection = endpoint.connect(server.addr, "localhost")?.await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());
    let client = greeter::GreeterClient::new(transport);

    Ok((endpoint, connection, client))
}

async fn connect_webtransport_client(
    server: &RunningServer,
) -> TestResult<(
    web_transport_quinn::Client,
    web_transport_quinn::Session,
    greeter::GreeterClient<support::WebTransportRpc>,
)> {
    wait_for_server_ready(&server.ready).await?;
    let webtransport_client = make_webtransport_client(server)?;
    let session = webtransport_client
        .connect(web_transport_quinn::proto::ConnectRequest::new(
            webtransport_url(server, "/trevrpc")?,
        ))
        .await?;
    let transport = support::WebTransportRpc::new(session.clone());
    let greeter_client = greeter::GreeterClient::new(transport);

    Ok((webtransport_client, session, greeter_client))
}

async fn connect_http3_client(
    server: &RunningServer,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<Http3Transport>,
)> {
    let (endpoint, connection, transport) = connect_http3_transport(server).await?;
    Ok((endpoint, connection, greeter::GreeterClient::new(transport)))
}

async fn connect_http3_transport(
    server: &RunningServer,
) -> TestResult<(quinn::Endpoint, quinn::Connection, Http3Transport)> {
    wait_for_server_ready(&server.ready).await?;
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

fn make_webtransport_client(server: &RunningServer) -> TestResult<web_transport_quinn::Client> {
    Ok(web_transport_quinn::ClientBuilder::new()
        .with_server_certificates(vec![server.cert_der.clone()])?)
}

fn webtransport_url(server: &RunningServer, path: &str) -> TestResult<url::Url> {
    Ok(format!("https://127.0.0.1:{}{path}", server.addr.port()).parse()?)
}

async fn expect_webtransport_connect_error(
    client: &web_transport_quinn::Client,
    request: web_transport_quinn::proto::ConnectRequest,
) -> TestResult<web_transport_quinn::ClientError> {
    match client.connect(request).await {
        Ok(session) => {
            session.close(0, b"unexpected test session");
            Err("WebTransport connect should have failed".into())
        }
        Err(error) => Ok(error),
    }
}

fn assert_webtransport_connect_rejected(
    error: web_transport_quinn::ClientError,
    expected: web_transport_quinn::http::StatusCode,
) -> TestResult {
    match error {
        web_transport_quinn::ClientError::HttpError(
            web_transport_quinn::ConnectError::ErrorStatus(actual),
        ) => ensure_eq!(actual, expected),
        web_transport_quinn::ClientError::HttpError(
            web_transport_quinn::ConnectError::ProtoError(_),
        ) => {}
        other => {
            return Err(
                format!("expected WebTransport HTTP status {expected}, got {other:?}").into(),
            );
        }
    }
    Ok(())
}

async fn close_webtransport_client(
    _client: web_transport_quinn::Client,
    session: web_transport_quinn::Session,
) -> TestResult {
    session.close(0, b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, session.closed()).await?;
    Ok(())
}

async fn close_client(endpoint: quinn::Endpoint, connection: quinn::Connection) -> TestResult {
    connection.close(0_u32.into(), b"test complete");
    tokio::time::timeout(TEST_TIMEOUT, endpoint.wait_idle()).await?;
    Ok(())
}

fn authenticated_options() -> CallOptions {
    CallOptions::new()
        .with_timeout(TEST_TIMEOUT)
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

async fn assert_greeter_ready<T>(client: &greeter::GreeterClient<T>) -> TestResult
where
    T: RpcTransport,
{
    let reply = tokio::time::timeout(
        TEST_TIMEOUT,
        client
            .say_hello(&greeter::HelloRequest {
                name: "readiness".to_owned(),
            })
            .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT)),
    )
    .await??;
    assert_eq!(reply.message, "hello, readiness");
    Ok(())
}

fn short_authenticated_options() -> CallOptions {
    CallOptions::new()
        .with_timeout(Duration::from_secs(1))
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

async fn run_client_greetings<T>(
    client: &greeter::GreeterClient<T>,
    names: &[&str],
    options: CallOptions,
) -> trevrpc::Result<greeter::HelloReply>
where
    T: StreamingRpcTransport,
{
    let call = client.lots_of_greetings().options(|_| options).await?;
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
    T: StreamingRpcTransport,
{
    let mut call = client.bidi_hello().options(|_| options).await?;
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

fn server_tls(
    verifier: Option<Arc<dyn quinn::rustls::server::danger::ClientCertVerifier>>,
) -> TestResult<(quinn::rustls::ServerConfig, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let builder = quinn::rustls::ServerConfig::builder();
    let builder = match verifier {
        Some(verifier) => builder.with_client_cert_verifier(verifier),
        None => builder.with_no_client_auth(),
    };
    let tls = builder.with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    Ok((tls, cert_der))
}

fn make_server_endpoint(
    options: &ServerOptions,
) -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let (tls, cert_der) = server_tls(None)?;
    let mut server = Server::new();
    server.set_options(options.clone());
    let config = server.quic_config(tls)?;
    Ok((
        quinn::Endpoint::server(config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn make_mtls_server_endpoint() -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    // The server certificate doubles as the only trusted client certificate.
    let (_, cert_der) = server_tls(None)?;
    let mut client_roots = quinn::rustls::RootCertStore::empty();
    client_roots.add(cert_der)?;
    let verifier = WebPkiClientVerifier::builder(Arc::new(client_roots)).build()?;
    let (tls, cert_der) = server_tls(Some(verifier))?;
    let config = Server::new().quic_config(tls)?;
    Ok((
        quinn::Endpoint::server(config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn client_tls(cert_der: CertificateDer<'static>) -> TestResult<quinn::rustls::ClientConfig> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;
    Ok(quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

fn make_client_endpoint(cert_der: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    let config = ChannelConfig::new().quic_config(client_tls(cert_der)?)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

/// A client endpoint that negotiates an arbitrary ALPN protocol instead of `TrevRPC`'s.
fn make_client_endpoint_with_alpn(
    cert_der: CertificateDer<'static>,
    alpn: &[u8],
) -> TestResult<quinn::Endpoint> {
    let mut tls = client_tls(cert_der)?;
    tls.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls)?,
    )));
    Ok(endpoint)
}

fn make_http3_client_endpoint(cert_der: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    make_client_endpoint_with_alpn(cert_der, trevrpc::HTTP3_ALPN)
}

// ---------------------------------------------------------------------------------------------
// Response headers
// ---------------------------------------------------------------------------------------------

/// Exercises the response headers of every RPC shape through a transport.
async fn exercise_headers<T>(client: &greeter::GreeterClient<T>) -> TestResult
where
    T: StreamingRpcTransport,
{
    let unary = client
        .say_hello(&greeter::HelloRequest {
            name: "headers".to_owned(),
        })
        .options(|_| authenticated_options())
        .with_response()
        .await?;
    assert_response_shape_header(unary.headers(), "unary");
    assert_response_shape(unary.metadata(), "unary");

    // Headers are available before any message is read, and the message is not lost.
    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest {
            name: "headers".to_owned(),
        })
        .options(|_| authenticated_options())
        .await?;
    assert_response_shape_header(replies.headers().await?, "server-streaming");
    ensure_eq!(
        replies.next().await.ok_or("first reply")??.message,
        "hello, headers"
    );
    ensure_eq!(
        replies.next().await.ok_or("second reply")??.message,
        "goodbye, headers"
    );
    ensure!(replies.next().await.is_none());
    assert_response_shape(
        replies.terminal_metadata().ok_or("terminal metadata")?,
        "server-streaming",
    );

    let mut upload = client
        .lots_of_greetings()
        .options(|_| authenticated_options())
        .await?;
    upload
        .send(greeter::HelloRequest {
            name: "headers".to_owned(),
        })
        .await?;
    upload.close_send()?;
    assert_response_shape_header(upload.headers().await?, "client-streaming");
    let upload = upload.close_and_recv_with_response().await?;
    assert_response_shape_header(upload.headers(), "client-streaming");
    assert_response_shape(upload.metadata(), "client-streaming");

    // The handler answers before the first request message, and so do its headers.
    let mut bidi = client
        .bidi_hello()
        .options(|_| authenticated_options())
        .await?;
    assert_response_shape_header(bidi.headers().await?, "bidirectional-streaming");
    bidi.send(greeter::HelloRequest {
        name: "headers".to_owned(),
    })
    .await?;
    ensure_eq!(
        bidi.recv().await?.ok_or("bidi reply")?.message,
        "echo, headers"
    );
    bidi.close_send()?;
    ensure!(bidi.recv().await?.is_none());
    Ok(())
}

#[tokio::test]
async fn quinn_response_headers_reach_every_rpc_shape() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_client(&server).await?;

    exercise_headers(&client).await?;

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn webtransport_response_headers_reach_every_rpc_shape() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (client, session, greeter_client) = connect_webtransport_client(&server).await?;

    exercise_headers(&greeter_client).await?;

    close_webtransport_client(client, session).await?;
    server.shutdown().await
}

#[tokio::test]
async fn http3_response_headers_reach_every_rpc_shape() -> TestResult {
    let server = spawn_http3_greeter_server(|server| {
        server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    })?;
    let (endpoint, connection, client) = connect_http3_client(&server).await?;

    exercise_headers(&client).await?;

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

/// A server-streaming route that sends its headers at once and its only message once released.
fn register_gated_headers_route(server: &mut Server, gate: Arc<Notify>) {
    server.route_streaming_with_context(
        "/test.Headers/Gate",
        RpcKind::ServerStreaming,
        move |context, _body, _requests| {
            let gate = Arc::clone(&gate);
            async move {
                let late = greeter::HelloReply {
                    message: "late".to_owned(),
                }
                .encode_to_vec();
                let messages: trevrpc::BoxStream<Vec<u8>> =
                    Box::pin(futures_util::stream::once(async move {
                        gate.notified().await;
                        Ok(late)
                    }));
                Ok(successful_response(&context, "gated", messages)?)
            }
        },
    );
}

#[tokio::test]
async fn quinn_streaming_headers_arrive_before_the_first_message_exists() -> TestResult {
    let gate = Arc::new(Notify::new());
    let server = spawn_greeter_server(|server| {
        register_gated_headers_route(server, Arc::clone(&gate));
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let transport = trevrpc::client::QuicTransport::new(connection.clone());

    let mut replies = trevrpc::client::server_streaming::<_, _, greeter::HelloReply>(
        &transport,
        "/test.Headers/Gate",
        &greeter::HelloRequest::default(),
        CallOptions::new().with_timeout(TEST_TIMEOUT),
    )
    .await?;

    // The handler holds its only message back, yet the client already has the headers.
    assert_response_shape_header(replies.headers().await?, "gated");
    gate.notify_one();
    ensure_eq!(
        replies.next().await.ok_or("the gated reply")??.message,
        "late"
    );
    ensure!(replies.next().await.is_none());

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

// ---------------------------------------------------------------------------------------------
// Unknown frames, grammar violations, and limits on the wire
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn quinn_server_skips_unknown_frames_everywhere_on_a_request() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let request = RpcRequest::new(support::greeter_path("SayHello"));
    let body = greeter::HelloRequest {
        name: "grease".to_owned(),
    }
    .encode_to_vec();

    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_grease(&mut send, b"before").await?;
    support::write_request(&mut send, &request).await?;
    support::write_grease(&mut send, &[]).await?;
    support::write_message(&mut send, &body).await?;
    support::write_grease(&mut send, &[7; 300]).await?;
    send.finish()?;
    let reply = read_raw_quinn_reply(&mut recv).await?;

    ensure_eq!(reply.status.code(), Code::Ok);
    let [message] = reply.messages.as_slice() else {
        return Err("a unary reply carries exactly one message".into());
    };
    ensure_eq!(
        greeter::HelloReply::decode(message.as_slice())?.message,
        "hello, grease"
    );
    assert_response_shape_header(&reply.headers, "unary");
    read_quinn_fin(&mut recv).await?;

    // Unknown frames also interleave with streamed request messages and the request status.
    let request = RpcRequest::new(support::greeter_path("LotsOfGreetings"))
        .with_kind(RpcKind::ClientStreaming);
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &request).await?;
    for name in ["a", "b"] {
        support::write_grease(&mut send, name.as_bytes()).await?;
        support::write_message(
            &mut send,
            &greeter::HelloRequest {
                name: name.to_owned(),
            }
            .encode_to_vec(),
        )
        .await?;
    }
    support::write_status(&mut send, Status::ok()).await?;
    support::write_grease(&mut send, b"after status").await?;
    send.finish()?;
    let reply = read_raw_quinn_reply(&mut recv).await?;
    ensure_eq!(reply.status.code(), Code::Ok);
    let [message] = reply.messages.as_slice() else {
        return Err("a client-streaming reply carries exactly one message".into());
    };
    ensure_eq!(
        greeter::HelloReply::decode(message.as_slice())?.message,
        "a,b"
    );

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_unary_requests_need_exactly_one_message() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.route(support::greeter_path("Slow"), |body| async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            Ok(body)
        });
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let request = RpcRequest::new(support::greeter_path("Slow"));

    // No message at all.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &request).await?;
    send.finish()?;
    ensure_eq!(
        read_raw_quinn_status(&mut recv).await?.code(),
        Code::InvalidArgument
    );

    // A STATUS where the message belongs.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &request).await?;
    support::write_status(&mut send, Status::ok()).await?;
    send.finish()?;
    ensure_eq!(
        read_raw_quinn_status(&mut recv).await?.code(),
        Code::InvalidArgument
    );

    // A message before the request.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_message(&mut send, b"early").await?;
    support::write_request(&mut send, &request).await?;
    send.finish()?;
    ensure_eq!(
        read_raw_quinn_status(&mut recv).await?.code(),
        Code::InvalidArgument
    );

    // A second message is rejected while the handler is still running.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request_and_message(&mut send, &request, b"one").await?;
    support::write_message(&mut send, b"two").await?;
    send.finish()?;
    let status = read_raw_quinn_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::InvalidArgument);

    // Server-streaming requests are held to the same rule.
    let streaming =
        RpcRequest::new(support::greeter_path("LotsOfReplies")).with_kind(RpcKind::ServerStreaming);
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &streaming).await?;
    send.finish()?;
    ensure_eq!(
        read_raw_quinn_status(&mut recv).await?.code(),
        Code::InvalidArgument
    );

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_grease_is_charged_to_the_request_body_limit() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_max_stream_body_size(Some(256)));
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let request = RpcRequest::new(support::greeter_path("LotsOfGreetings"))
        .with_kind(RpcKind::ClientStreaming);

    // No message is ever sent: only unknown frames, 102 bytes each.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &request).await?;
    for _ in 0..4 {
        support::write_grease(&mut send, &[0; 100]).await?;
    }
    let status = read_raw_quinn_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_oversized_unknown_frame_is_rejected_before_its_payload() -> TestResult {
    let server = spawn_greeter_server(|server| {
        server.set_options(fast_server_options().with_max_frame_size(1024));
    })?;
    let (endpoint, connection, _client) = connect_client(&server).await?;
    let request = RpcRequest::new(support::greeter_path("LotsOfGreetings"))
        .with_kind(RpcKind::ClientStreaming);

    // A grease frame that declares 1025 payload bytes and sends none.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_request(&mut send, &request).await?;
    let mut header = Vec::new();
    trevrpc::varint::push(&mut header, 0x1f * 3 + 0x21)?;
    trevrpc::varint::push(&mut header, 1025)?;
    support::write_raw(&mut send, &header).await?;
    let status = read_raw_quinn_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::ResourceExhausted);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

#[tokio::test]
async fn quinn_initial_request_with_a_truncated_header_is_unavailable() -> TestResult {
    let server = spawn_greeter_server(|_| {})?;
    let (endpoint, connection, _client) = connect_client(&server).await?;

    // The stream ends after the first byte of a two-byte frame header.
    let (mut send, mut recv) = connection.open_bi().await?;
    support::write_raw(&mut send, &[0x01]).await?;
    send.finish()?;
    let status = read_raw_quinn_status(&mut recv).await?;
    ensure_eq!(status.code(), Code::Unavailable);

    close_client(endpoint, connection).await?;
    server.shutdown().await
}

/// A one-shot server that reads a call's request frames and then writes `script` as its response.
struct ScriptedServer {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    task: JoinHandle<TestResult>,
}

impl ScriptedServer {
    async fn start(script: Vec<u8>) -> TestResult<Self> {
        let (server_endpoint, cert_der) = make_server_endpoint(&fast_server_options())?;
        let server_addr = server_endpoint.local_addr()?;
        let task = tokio::spawn(async move {
            let incoming = tokio::time::timeout(TEST_TIMEOUT, server_endpoint.accept())
                .await?
                .ok_or_else(|| std::io::Error::other("server endpoint closed before accept"))?;
            let connection = incoming.await?;
            let (mut send, mut recv) = connection.accept_bi().await?;
            let request = support::read_request(&mut recv).await?;
            if request.rpc_kind().has_single_request_message() {
                let _message = support::read_message(&mut recv).await?;
            }
            support::write_raw(&mut send, &script).await?;
            send.finish()?;
            // Hold the connection until the client is done with the response.
            let _ = tokio::time::timeout(TEST_TIMEOUT, connection.closed()).await;
            Ok(())
        });
        let endpoint = make_client_endpoint(cert_der)?;
        let connection = endpoint.connect(server_addr, "localhost")?.await?;
        Ok(Self {
            endpoint,
            connection,
            task,
        })
    }

    fn transport(&self) -> trevrpc::client::QuicTransport {
        trevrpc::client::QuicTransport::new(self.connection.clone())
    }

    async fn finish(self) -> TestResult {
        close_client(self.endpoint, self.connection).await?;
        self.task.await?
    }
}

fn frame_bytes(frames: &[trevrpc::ResponseFrame]) -> TestResult<Vec<u8>> {
    let mut bytes = Vec::new();
    for frame in frames {
        bytes.extend(match frame {
            trevrpc::ResponseFrame::Headers(metadata) => trevrpc::framing::encode_protobuf_frame(
                trevrpc::framing::frame_type::HEADERS,
                &trevrpc::RpcHeaders {
                    metadata: metadata.clone(),
                    extensions: Vec::new(),
                    dictionaries: Vec::new(),
                    stream_compression: None,
                    connection_metadata: 0,
                },
                trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
            )?,
            trevrpc::ResponseFrame::Message(body) => trevrpc::framing::encode_message_frame(
                body,
                trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
            )?,
            trevrpc::ResponseFrame::Status(status) => trevrpc::framing::encode_protobuf_frame(
                trevrpc::framing::frame_type::STATUS,
                &trevrpc::RpcStatus::from(status.clone()),
                trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
            )?,
        });
    }
    Ok(bytes)
}

fn grease_frame(payload: &[u8]) -> TestResult<Vec<u8>> {
    Ok(trevrpc::framing::encode_frame(
        0x1f * 7 + 0x21,
        payload,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )?)
}

fn reply_message(text: &str) -> trevrpc::ResponseFrame {
    trevrpc::ResponseFrame::message(
        greeter::HelloReply {
            message: text.to_owned(),
        }
        .encode_to_vec(),
    )
}

fn single_header(key: &str, value: &[u8]) -> trevrpc::Metadata {
    trevrpc::Metadata::from([(key.to_owned(), value.to_vec())])
}

#[tokio::test]
async fn quinn_client_skips_unknown_frames_everywhere_on_a_response() -> TestResult {
    let headers = single_header("x-h", b"1");
    let trailers = single_header("x-t", b"2");
    let script = [
        grease_frame(b"first")?,
        frame_bytes(&[trevrpc::ResponseFrame::Headers(headers.clone())])?,
        grease_frame(&[])?,
        frame_bytes(&[reply_message("one")])?,
        grease_frame(&[1; 500])?,
        frame_bytes(&[trevrpc::ResponseFrame::Status(
            Status::ok().with_metadata(trailers.clone()),
        )])?,
        // Unknown frames may still follow the status; the stream must then end cleanly.
        grease_frame(b"after status")?,
    ]
    .concat();
    let server = ScriptedServer::start(script).await?;
    let client = greeter::GreeterClient::new(server.transport());

    let envelope = client
        .say_hello(&greeter::HelloRequest::default())
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .with_response()
        .await?;

    ensure_eq!(envelope.message().message, "one");
    ensure_eq!(envelope.headers(), &headers);
    ensure_eq!(envelope.metadata(), &trailers);
    server.finish().await
}

#[tokio::test]
async fn quinn_client_streaming_responses_skip_unknown_frames() -> TestResult {
    let script = [
        frame_bytes(&[trevrpc::ResponseFrame::Headers(single_header("x-h", b"1"))])?,
        grease_frame(b"between")?,
        frame_bytes(&[reply_message("one")])?,
        grease_frame(b"between")?,
        frame_bytes(&[reply_message("two")])?,
        frame_bytes(&[trevrpc::ResponseFrame::Status(Status::ok())])?,
        grease_frame(b"after status")?,
    ]
    .concat();
    let server = ScriptedServer::start(script).await?;
    let client = greeter::GreeterClient::new(server.transport());

    let mut replies = client
        .lots_of_replies(&greeter::HelloRequest::default())
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await?;
    ensure_eq!(replies.headers().await?, &single_header("x-h", b"1"));
    let mut messages = Vec::new();
    while let Some(reply) = replies.next().await {
        messages.push(reply?.message);
    }
    ensure_eq!(messages, ["one", "two"]);
    ensure_eq!(replies.terminal_status().map(Status::code), Some(Code::Ok));

    server.finish().await
}

#[tokio::test]
async fn quinn_client_rejects_misplaced_response_headers() -> TestResult {
    let headers = || trevrpc::ResponseFrame::Headers(single_header("x-h", b"1"));
    let status = || trevrpc::ResponseFrame::Status(Status::ok());

    for (name, frames) in [
        ("second HEADERS", vec![headers(), headers(), status()]),
        (
            "HEADERS after MESSAGE",
            vec![reply_message("one"), headers(), status()],
        ),
        (
            "HEADERS after STATUS",
            vec![reply_message("one"), status(), headers()],
        ),
    ] {
        // Unary.
        let server = ScriptedServer::start(frame_bytes(&frames)?).await?;
        let client = greeter::GreeterClient::new(server.transport());
        let error = client
            .say_hello(&greeter::HelloRequest::default())
            .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
            .await
            .expect_err(name);
        ensure_eq!(error.into_status().code(), Code::Internal, "unary {name}");
        server.finish().await?;

        // Streaming.
        let server = ScriptedServer::start(frame_bytes(&frames)?).await?;
        let client = greeter::GreeterClient::new(server.transport());
        let mut replies = client
            .lots_of_replies(&greeter::HelloRequest::default())
            .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
            .await?;
        let mut failure = None;
        while let Some(reply) = replies.next().await {
            if let Err(error) = reply {
                failure = Some(error);
            }
        }
        let failure = failure.ok_or_else(|| format!("streaming {name} should fail"))?;
        ensure_eq!(
            failure.into_status().code(),
            Code::Internal,
            "streaming {name}"
        );
        server.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn quinn_client_rejects_unary_responses_without_exactly_one_message() -> TestResult {
    let status = || trevrpc::ResponseFrame::Status(Status::ok());
    for (name, frames) in [
        ("no message", vec![status()]),
        (
            "two messages",
            vec![reply_message("one"), reply_message("two"), status()],
        ),
        ("no status", vec![reply_message("one")]),
    ] {
        let server = ScriptedServer::start(frame_bytes(&frames)?).await?;
        let client = greeter::GreeterClient::new(server.transport());
        let error = client
            .say_hello(&greeter::HelloRequest::default())
            .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
            .await
            .expect_err(name);
        ensure_eq!(error.into_status().code(), Code::Internal, "{name}");
        server.finish().await?;
    }
    Ok(())
}

#[tokio::test]
async fn quinn_client_rejects_oversized_response_frames_before_reading_them() -> TestResult {
    // A grease frame that declares more than the client's maximum frame size and sends nothing.
    let mut script = Vec::new();
    trevrpc::varint::push(&mut script, 0x1f * 7 + 0x21)?;
    trevrpc::varint::push(
        &mut script,
        u64::try_from(trevrpc::framing::DEFAULT_MAX_FRAME_SIZE)? + 1,
    )?;
    let server = ScriptedServer::start(script).await?;
    let client = greeter::GreeterClient::new(server.transport());

    let error = client
        .say_hello(&greeter::HelloRequest::default())
        .options(|_| CallOptions::new().with_timeout(TEST_TIMEOUT))
        .await
        .expect_err("an oversized frame should fail the call");

    ensure_eq!(error.into_status().code(), Code::ResourceExhausted);
    server.finish().await
}
