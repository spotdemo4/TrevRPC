//! How a server call ends, and what its interceptors, its metrics and its response see of that:
//! each test drives `handle_stream` with an in-memory transport and ends the call in a way that
//! races its handler or its response writes.

#![allow(clippy::unused_async_trait_impl)]

use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use prost::Message as _;
use tokio::sync::{Notify, Semaphore, watch};

use crate::framed::{self, FrameRead, FrameWrite, ResponseCodec};
use crate::framing::{
    DEFAULT_MAX_FRAME_SIZE, encode_message_frame, encode_protobuf_frame, frame_type,
};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::server::{
    Finish, Interceptor, Metrics, RequestContext, RpcFinished, Server, ServerOptions,
};
use crate::server_transport::{
    ResponseWriter, handle_stream, read_initial_request, try_acquire_permit,
};
use crate::test_io::Script;
use crate::{Code, Error, ResponseFrame, Result, RpcKind, RpcRequest, RpcStatus, Status};

const PATH: &str = "/svc/m";

/// What the fake transport saw of the response.
#[derive(Clone, Default)]
struct Wire {
    bytes: Vec<u8>,
    finished: bool,
    reset: bool,
}

impl Wire {
    /// The `(type, payload)` of every frame written.
    fn frames(&self) -> Vec<(u64, Vec<u8>)> {
        let mut rest = &self.bytes[..];
        let mut frames = Vec::new();
        while !rest.is_empty() {
            let (kind, used) = crate::varint::decode(rest).expect("frame type");
            rest = &rest[used..];
            let (len, used) = crate::varint::decode(rest).expect("frame length");
            rest = &rest[used..];
            let len = usize::try_from(len).expect("frame length fits");
            frames.push((kind, rest[..len].to_vec()));
            rest = &rest[len..];
        }
        frames
    }

    /// The kinds of frame written, in order.
    fn kinds(&self) -> Vec<u64> {
        self.frames().into_iter().map(|(kind, _)| kind).collect()
    }

    fn statuses(&self) -> Vec<RpcStatus> {
        self.frames()
            .into_iter()
            .filter(|(kind, _)| *kind == frame_type::STATUS)
            .map(|(_, payload)| RpcStatus::decode(&payload[..]).expect("status"))
            .collect()
    }

    fn status_codes(&self) -> Vec<Code> {
        self.statuses()
            .into_iter()
            .map(|status| Status::from(status).code())
            .collect()
    }
}

struct FakeWriter {
    wire: Arc<Mutex<Wire>>,
    codec: ResponseCodec,
    max_frame_size: usize,
    /// The client stops reading the response when this is notified.
    stop: Arc<Notify>,
    /// Writes that complete before the transport stalls, as one that is out of flow control.
    writes_before_stall: usize,
    writes: usize,
}

impl FrameWrite for FakeWriter {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        if self.writes >= self.writes_before_stall {
            pending::<()>().await;
        }
        self.writes = self.writes.saturating_add(1);
        let bytes = chunks.iter().flat_map(|chunk| chunk.iter().copied());
        self.wire.lock().expect("wire lock").bytes.extend(bytes);
        Ok(())
    }
}

impl ResponseWriter for FakeWriter {
    async fn write_frames(&mut self, frames: &mut Vec<ResponseFrame>) -> Result<()> {
        let mut codec = std::mem::take(&mut self.codec);
        let max_frame_size = self.max_frame_size;
        let written = framed::write_response_frames(self, frames, max_frame_size, &mut codec).await;
        self.codec = codec;
        written
    }

    async fn stopped(&mut self) -> Result<()> {
        self.stop.notified().await;
        Ok(())
    }

    fn reset(&mut self) {
        self.wire.lock().expect("wire lock").reset = true;
    }

    async fn finish(&mut self) -> Result<()> {
        self.wire.lock().expect("wire lock").finished = true;
        Ok(())
    }

    fn set_codec(&mut self, codec: ResponseCodec) {
        self.codec = codec;
    }
}

/// How the request side of the fake transport fails.
#[derive(Clone)]
enum RequestFailure {
    /// The client resets the request stream.
    Reset,
    /// The request fails with this protocol error.
    Protocol(Status),
    /// The client finishes the request stream in the middle of a frame.
    Truncated,
}

/// A request side that stays open after the script, until `fail` is notified.
struct FakeReader {
    fail: Arc<Notify>,
    failure: RequestFailure,
}

impl FrameRead for FakeReader {
    async fn read_frame_bytes(&mut self, _bytes: &mut [u8]) -> Result<Option<usize>> {
        self.fail.notified().await;
        match &self.failure {
            RequestFailure::Reset => Err(Error::transport(quinn::ReadError::Reset(
                quinn::VarInt::from_u32(1),
            ))),
            RequestFailure::Protocol(status) => Err(Error::from(status.clone())),
            RequestFailure::Truncated => Err(framed::unexpected_eof()),
        }
    }
}

impl RequestPumpReader for FakeReader {
    fn stop_trevrpc(&mut self) {}

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        pending().await
    }

    fn validate_transport_end(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }
}

/// What the server's metrics and the finish function of its interceptor heard.
#[derive(Clone, Default)]
struct Observed {
    started: Arc<AtomicUsize>,
    metrics_finished: Arc<Mutex<Vec<Code>>>,
    finish_calls: Arc<Mutex<Vec<Code>>>,
}

impl Harness {
    /// What the transport has seen of the response so far.
    fn wire(&self) -> Wire {
        self.wire.lock().expect("wire lock").clone()
    }
}

impl Observed {
    fn metrics(&self) -> Vec<Code> {
        self.metrics_finished.lock().expect("lock").clone()
    }

    fn finishes(&self) -> Vec<Code> {
        self.finish_calls.lock().expect("lock").clone()
    }
}

impl Metrics for Observed {
    fn rpc_started(&self, _event: &crate::server::RpcStarted) {
        self.started.fetch_add(1, Ordering::SeqCst);
    }

    fn rpc_finished(&self, event: &RpcFinished) {
        self.metrics_finished.lock().expect("lock").push(event.code);
    }
}

#[crate::async_trait]
impl Interceptor for Observed {
    async fn intercept(
        &self,
        _context: &mut RequestContext,
    ) -> std::result::Result<Option<Finish>, Status> {
        let calls = Arc::clone(&self.finish_calls);
        Ok(Some(Box::new(move |event: &RpcFinished| {
            calls.lock().expect("lock").push(event.code);
        })))
    }
}

struct Harness {
    observed: Observed,
    wire: Arc<Mutex<Wire>>,
    stop: Arc<Notify>,
    fail: Arc<Notify>,
    shutdown: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
    request_slots: Arc<Semaphore>,
}

struct Setup {
    kind: RpcKind,
    timeout_nanos: u64,
    /// The server's frame limit.
    max_frame_size: usize,
    /// The frame limit of the transport's writer, when it is not the server's.
    writer_max_frame_size: Option<usize>,
    writes_before_stall: usize,
    failure: RequestFailure,
}

impl Setup {
    const fn new(kind: RpcKind) -> Self {
        Self {
            kind,
            timeout_nanos: 0,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            writer_max_frame_size: None,
            writes_before_stall: usize::MAX,
            failure: RequestFailure::Reset,
        }
    }
}

/// Serves one call of `setup.kind` to `server` over the fake transport, with one request slot.
async fn serve(mut server: Server, observed: Observed, setup: Setup) -> Harness {
    server.set_options(ServerOptions::new().with_max_frame_size(setup.max_frame_size));
    server.set_metrics(observed.clone());
    server.add_interceptor(observed.clone());
    let request = RpcRequest {
        timeout_nanos: setup.timeout_nanos,
        ..RpcRequest::new(PATH).with_kind(setup.kind)
    };
    let mut wire = encode_protobuf_frame(frame_type::REQUEST, &request, DEFAULT_MAX_FRAME_SIZE)
        .expect("request encodes");
    if setup.kind.has_single_request_message() {
        wire.extend(encode_message_frame(b"hi", DEFAULT_MAX_FRAME_SIZE).expect("message"));
    }
    let (shutdown, mut shutting_down) = watch::channel(false);
    let incoming = read_initial_request(&server, &mut Script::new(wire), &mut shutting_down)
        .await
        .expect("the request reads");
    let wire = Arc::new(Mutex::new(Wire::default()));
    let stop = Arc::new(Notify::new());
    let fail = Arc::new(Notify::new());
    let writer = FakeWriter {
        wire: Arc::clone(&wire),
        codec: ResponseCodec::default(),
        max_frame_size: setup.writer_max_frame_size.unwrap_or(setup.max_frame_size),
        stop: Arc::clone(&stop),
        writes_before_stall: setup.writes_before_stall,
        writes: 0,
    };
    let reader = FakeReader {
        fail: Arc::clone(&fail),
        failure: setup.failure,
    };
    let request_slots = Arc::new(Semaphore::new(1));
    let task = tokio::spawn(handle_stream(
        server,
        Some(Arc::clone(&request_slots)),
        try_acquire_permit(None).expect("an unlimited stream slot"),
        writer,
        reader,
        incoming,
        shutting_down,
    ));
    Harness {
        observed,
        wire,
        stop,
        fail,
        shutdown,
        task,
        request_slots,
    }
}

/// A server whose handler for `kind` signals `entered` and then waits forever.
fn hanging_server(kind: RpcKind, entered: &Arc<Notify>) -> Server {
    let mut server = Server::new();
    let entered = Arc::clone(entered);
    if kind == RpcKind::Unary {
        server.route(PATH, move |_body| {
            let entered = Arc::clone(&entered);
            async move {
                entered.notify_one();
                pending::<Result<Vec<u8>>>().await
            }
        });
    } else {
        server.route_streaming(PATH, kind, move |_body, requests| {
            let entered = Arc::clone(&entered);
            async move {
                // Keep reading the requests, as a handler that waits for the next one does.
                let _requests = requests;
                entered.notify_one();
                pending::<Result<crate::BoxStream<Vec<u8>>>>().await
            }
        });
    }
    server
}

/// A server-streaming route that replies once and then never ends.
fn one_reply_then_silence() -> Server {
    let mut server = Server::new();
    server.route_streaming(PATH, RpcKind::ServerStreaming, |_body, _requests| async {
        let replies: crate::BoxStream<Vec<u8>> = Box::pin(futures_util::StreamExt::chain(
            crate::stream::from_iter([b"one".to_vec()]),
            futures_util::stream::pending(),
        ));
        Ok(replies)
    });
    server
}

/// Waits for `harness`'s call to end.
async fn ends(harness: &mut Harness) {
    tokio::time::timeout(Duration::from_secs(5), &mut harness.task)
        .await
        .expect("the call ends")
        .expect("the call task");
}

#[derive(Clone, Copy, Debug)]
enum Ending {
    ResponseStopped,
    RequestReset,
    Shutdown,
}

impl Ending {
    /// What the interceptors and the metrics hear when a call ends this way.
    const fn code(self) -> Code {
        match self {
            Self::ResponseStopped | Self::RequestReset => Code::Cancelled,
            Self::Shutdown => Code::Unavailable,
        }
    }
}

/// Ends a call that is still in its handler and checks that the interceptors' finish functions
/// and the metrics hear of the end exactly once, and that the call ends in a reset.
async fn handler_running_call_ends(kind: RpcKind, ending: Ending) {
    let entered = Arc::new(Notify::new());
    let server = hanging_server(kind, &entered);
    let mut harness = serve(server, Observed::default(), Setup::new(kind)).await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("the handler runs");
    // Let the request pump start reading.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    match ending {
        Ending::ResponseStopped => harness.stop.notify_one(),
        Ending::RequestReset => harness.fail.notify_one(),
        Ending::Shutdown => harness.shutdown.send(true).expect("shutdown"),
    }
    ends(&mut harness).await;

    let case = format!("{kind:?}/{ending:?}");
    let started = harness.observed.started.load(Ordering::SeqCst);
    assert_eq!(started, 1, "{case}: started");
    assert_eq!(
        harness.observed.finishes(),
        vec![ending.code()],
        "{case}: interceptor finish functions; metrics saw {:?} of {started} started",
        harness.observed.metrics(),
    );
    assert_eq!(
        harness.observed.metrics(),
        vec![ending.code()],
        "{case}: metrics"
    );
    let wire = harness.wire();
    assert!(wire.reset, "{case}: the response was left without a reset");
    assert!(!wire.finished, "{case}: the response ended cleanly");
    assert!(wire.statuses().is_empty(), "{case}: a STATUS was written");
    assert_eq!(
        harness.request_slots.available_permits(),
        1,
        "{case}: the request slot"
    );
}

macro_rules! ends_report {
    ($name:ident, $kind:expr, $ending:expr) => {
        #[tokio::test]
        async fn $name() {
            handler_running_call_ends($kind, $ending).await;
        }
    };
}

ends_report!(
    a_unary_call_stopped_in_its_handler_reports_its_end,
    RpcKind::Unary,
    Ending::ResponseStopped
);
ends_report!(
    a_unary_call_reset_in_its_handler_reports_its_end,
    RpcKind::Unary,
    Ending::RequestReset
);
ends_report!(
    a_unary_call_shut_down_in_its_handler_reports_its_end,
    RpcKind::Unary,
    Ending::Shutdown
);
ends_report!(
    a_client_streaming_call_stopped_in_its_handler_reports_its_end,
    RpcKind::ClientStreaming,
    Ending::ResponseStopped
);
ends_report!(
    a_client_streaming_call_reset_in_its_handler_reports_its_end,
    RpcKind::ClientStreaming,
    Ending::RequestReset
);
ends_report!(
    a_client_streaming_call_shut_down_in_its_handler_reports_its_end,
    RpcKind::ClientStreaming,
    Ending::Shutdown
);
ends_report!(
    a_server_streaming_call_stopped_in_its_handler_reports_its_end,
    RpcKind::ServerStreaming,
    Ending::ResponseStopped
);
ends_report!(
    a_server_streaming_call_shut_down_in_its_handler_reports_its_end,
    RpcKind::ServerStreaming,
    Ending::Shutdown
);

/// A call whose response does not fit the frame limit.
async fn oversized_response(kind: RpcKind) -> Harness {
    let mut server = Server::new();
    if kind == RpcKind::Unary {
        server.route(PATH, |_body| async { Ok(vec![7_u8; 2000]) });
    } else {
        server.route_streaming(PATH, kind, |_body, _requests| async {
            let replies: crate::BoxStream<Vec<u8>> =
                crate::stream::from_iter([b"ok".to_vec(), vec![7_u8; 2000]]);
            Ok(replies)
        });
    }
    let mut setup = Setup::new(kind);
    setup.max_frame_size = 1024;
    let mut harness = serve(server, Observed::default(), setup).await;
    ends(&mut harness).await;
    harness
}

#[tokio::test]
async fn a_unary_response_over_the_frame_limit_ends_with_resource_exhausted() {
    let harness = oversized_response(RpcKind::Unary).await;
    let wire = harness.wire();
    assert_eq!(
        wire.status_codes(),
        vec![Code::ResourceExhausted],
        "frames written {:?}",
        wire.kinds(),
    );
    assert!(wire.finished && !wire.reset);
    assert_eq!(harness.observed.finishes(), vec![Code::ResourceExhausted]);
    assert_eq!(harness.observed.metrics(), vec![Code::ResourceExhausted]);
}

#[tokio::test]
async fn a_streaming_response_over_the_frame_limit_keeps_the_messages_before_it() {
    let harness = oversized_response(RpcKind::ServerStreaming).await;
    let wire = harness.wire();
    assert_eq!(
        wire.kinds(),
        vec![frame_type::MESSAGE, frame_type::STATUS],
        "the message before the oversized one goes out, then the STATUS"
    );
    assert_eq!(wire.status_codes(), vec![Code::ResourceExhausted]);
    assert!(wire.finished && !wire.reset);
    assert_eq!(harness.observed.finishes(), vec![Code::ResourceExhausted]);
    assert_eq!(harness.observed.metrics(), vec![Code::ResourceExhausted]);
}

/// The safety net under the checks that fail a call before it reports its end: a frame that the
/// server accepted but its writer cannot encode still ends with a STATUS, not with a clean end
/// of stream that the client would take for a protocol error.
#[tokio::test]
async fn a_frame_the_writer_cannot_encode_ends_with_a_lone_status() {
    let mut server = Server::new();
    server.route(PATH, |_body| async { Ok(vec![7_u8; 2000]) });
    let mut setup = Setup::new(RpcKind::Unary);
    setup.writer_max_frame_size = Some(1024);
    let mut harness = serve(server, Observed::default(), setup).await;
    ends(&mut harness).await;
    let wire = harness.wire();
    assert_eq!(wire.kinds(), vec![frame_type::STATUS]);
    assert_eq!(wire.status_codes(), vec![Code::ResourceExhausted]);
    assert!(wire.finished && !wire.reset);
    // The call is reported after its write, which failed: the hooks do not hear the `Ok` the
    // handler meant.
    assert_eq!(harness.observed.finishes(), vec![Code::ResourceExhausted]);
    assert_eq!(harness.observed.metrics(), vec![Code::ResourceExhausted]);
}

/// A healthy unary call is reported once its response has been written and finished.
#[tokio::test]
async fn a_unary_call_is_reported_after_its_response_went_out() {
    let mut server = Server::new();
    server.route(PATH, |_body| async { Ok(b"reply".to_vec()) });
    let mut harness = serve(server, Observed::default(), Setup::new(RpcKind::Unary)).await;
    ends(&mut harness).await;
    let wire = harness.wire();
    assert!(wire.finished && !wire.reset);
    assert_eq!(harness.observed.finishes(), vec![Code::Ok]);
    assert_eq!(harness.observed.metrics(), vec![Code::Ok]);
}

/// A server-streaming call whose transport stalls at its first write, as one does once a client
/// that stopped reading has used up its flow control.
async fn stalled_streaming_call(timeout_nanos: u64) -> Harness {
    let mut setup = Setup::new(RpcKind::ServerStreaming);
    setup.timeout_nanos = timeout_nanos;
    setup.writes_before_stall = 0;
    serve(one_reply_then_silence(), Observed::default(), setup).await
}

#[tokio::test(start_paused = true)]
async fn a_deadline_ends_a_streaming_call_whose_write_is_stalled() {
    let mut harness = stalled_streaming_call(1_000_000_000).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(
        harness.request_slots.available_permits(),
        0,
        "the call holds its request slot while it is writing"
    );
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("the call outlived its 1 s deadline by a minute")
        .expect("the call task");
    assert_eq!(harness.observed.finishes(), vec![Code::DeadlineExceeded]);
    assert_eq!(harness.observed.metrics(), vec![Code::DeadlineExceeded]);
    let wire = harness.wire();
    assert!(
        wire.reset,
        "a client that is not reading cannot be sent a STATUS"
    );
    assert_eq!(wire.bytes, Vec::<u8>::new());
    assert_eq!(harness.request_slots.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_deadline_ends_a_unary_call_whose_write_is_stalled() {
    let mut server = Server::new();
    server.route(PATH, |_body| async { Ok(b"reply".to_vec()) });
    let mut setup = Setup::new(RpcKind::Unary);
    setup.timeout_nanos = 1_000_000_000;
    setup.writes_before_stall = 0;
    let mut harness = serve(server, Observed::default(), setup).await;
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("the call outlived its 1 s deadline by a minute")
        .expect("the call task");
    assert!(harness.wire().reset);
    // The handler had answered `Ok`, but the response never went out: the hooks hear the deadline.
    assert_eq!(harness.observed.finishes(), vec![Code::DeadlineExceeded]);
    assert_eq!(harness.observed.metrics(), vec![Code::DeadlineExceeded]);
}

#[tokio::test(start_paused = true)]
async fn a_deadline_ends_a_streaming_call_whose_final_write_is_stalled() {
    let mut server = Server::new();
    server.route_streaming(PATH, RpcKind::ServerStreaming, |_body, _requests| async {
        let replies: crate::BoxStream<Vec<u8>> = crate::stream::from_iter([b"one".to_vec()]);
        Ok(replies)
    });
    let mut setup = Setup::new(RpcKind::ServerStreaming);
    setup.timeout_nanos = 1_000_000_000;
    setup.writes_before_stall = 0;
    let mut harness = serve(server, Observed::default(), setup).await;
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("the call outlived its 1 s deadline by a minute")
        .expect("the call task");
    // The reply and the STATUS(OK) are one write, which never completes.
    assert!(harness.wire().reset);
    assert_eq!(harness.observed.finishes(), vec![Code::DeadlineExceeded]);
    assert_eq!(harness.observed.metrics(), vec![Code::DeadlineExceeded]);
    assert_eq!(harness.request_slots.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_a_unary_call_whose_write_is_stalled() {
    let mut server = Server::new();
    server.route(PATH, |_body| async { Ok(b"reply".to_vec()) });
    let mut setup = Setup::new(RpcKind::Unary);
    setup.writes_before_stall = 0;
    let mut harness = serve(server, Observed::default(), setup).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    harness.shutdown.send(true).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("shutdown did not end the stalled call in a minute")
        .expect("the call task");
    assert!(harness.wire().reset);
    assert_eq!(harness.observed.finishes(), vec![Code::Unavailable]);
    assert_eq!(harness.observed.metrics(), vec![Code::Unavailable]);
}

/// Control for the stalled-write tests: the same call ends at its deadline when writes complete.
#[tokio::test(start_paused = true)]
async fn a_deadline_ends_a_streaming_call_whose_writes_complete_with_a_status() {
    let mut setup = Setup::new(RpcKind::ServerStreaming);
    setup.timeout_nanos = 1_000_000_000;
    let mut harness = serve(one_reply_then_silence(), Observed::default(), setup).await;
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("the call ends at its deadline")
        .expect("the call task");
    let wire = harness.wire();
    assert_eq!(wire.status_codes(), vec![Code::DeadlineExceeded]);
    assert!(wire.finished && !wire.reset);
    assert_eq!(harness.observed.finishes(), vec![Code::DeadlineExceeded]);
}

#[tokio::test(start_paused = true)]
async fn shutdown_ends_a_streaming_call_whose_write_is_stalled() {
    let mut harness = stalled_streaming_call(0).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    harness.shutdown.send(true).expect("shutdown");
    tokio::time::timeout(Duration::from_secs(60), &mut harness.task)
        .await
        .expect("shutdown did not end the stalled call in a minute")
        .expect("the call task");
    assert!(harness.wire().reset);
    assert_eq!(harness.observed.finishes(), vec![Code::Unavailable]);
    assert_eq!(harness.observed.metrics(), vec![Code::Unavailable]);
    assert_eq!(harness.request_slots.available_permits(), 1);
}

/// A request stream that fails with an `Internal` protocol error while the handler runs.
async fn failing_request(kind: RpcKind, failure: RequestFailure) -> Harness {
    let entered = Arc::new(Notify::new());
    let server = hanging_server(kind, &entered);
    let mut setup = Setup::new(kind);
    setup.failure = failure;
    let mut harness = serve(server, Observed::default(), setup).await;
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("the handler runs");
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    harness.fail.notify_one();
    ends(&mut harness).await;
    harness
}

fn status_messages(harness: &Harness) -> Vec<String> {
    harness
        .wire()
        .statuses()
        .into_iter()
        .map(|status| status.message)
        .collect()
}

#[tokio::test]
async fn a_unary_request_failure_with_an_internal_status_is_redacted() {
    let harness = failing_request(
        RpcKind::Unary,
        RequestFailure::Protocol(Status::internal("secret transport detail")),
    )
    .await;
    assert_eq!(
        status_messages(&harness),
        vec![crate::server::INTERNAL_SERVER_ERROR.to_owned()]
    );
    // The hooks hear the failure itself, which stays on the server.
    assert_eq!(harness.observed.finishes(), vec![Code::Internal]);
}

#[tokio::test]
async fn a_streaming_request_failure_with_an_internal_status_is_redacted() {
    let harness = failing_request(
        RpcKind::ClientStreaming,
        RequestFailure::Protocol(Status::internal("secret transport detail")),
    )
    .await;
    assert_eq!(
        status_messages(&harness),
        vec![crate::server::INTERNAL_SERVER_ERROR.to_owned()]
    );
    assert_eq!(harness.observed.finishes(), vec![Code::Internal]);
}

/// A client that finishes its request stream in the middle of a frame has broken the protocol
/// on a connection that is still up: the call is answered, not treated as a lost connection.
async fn truncated_request_is_answered(kind: RpcKind) {
    let harness = failing_request(kind, RequestFailure::Truncated).await;
    let wire = harness.wire();
    assert_eq!(wire.status_codes(), vec![Code::InvalidArgument]);
    assert!(wire.finished && !wire.reset);
    assert_eq!(harness.observed.finishes(), vec![Code::InvalidArgument]);
    assert_eq!(harness.observed.metrics(), vec![Code::InvalidArgument]);
}

#[tokio::test]
async fn a_unary_request_cut_in_a_frame_is_answered_with_invalid_argument() {
    truncated_request_is_answered(RpcKind::Unary).await;
}

#[tokio::test]
async fn a_streaming_request_cut_in_a_frame_is_answered_with_invalid_argument() {
    truncated_request_is_answered(RpcKind::ClientStreaming).await;
}
