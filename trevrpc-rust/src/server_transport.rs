use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::compression::{self, Accept, MessageEncoder, ServerCall, ServerStream};
use crate::connection_metadata::ConnectionMetadataStore;
use crate::datagram::{Demux, FlowSender, MergedRequests, MessageCount, ServerFlow};
use crate::framed::{self, FrameRead, MESSAGE_FRAME_BATCH, ResponseCodec};
use crate::request_pump::{
    RequestInputKind, RequestPumpReader, RequestPumpSettle, UnaryRequestEnd, start_request_pump,
};
use crate::server::{
    Admitted, CancellationSource, CancellationToken, RequestBudget, RequestCounts, Server,
};
use crate::{
    Code, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcAnswer, RpcKind, RpcRequest,
    Status, varint,
};

/// Sends response frames without imposing a transport framing format.
///
/// A transport adapter owns encoding, length prefixes, tracing, and the concrete stream
/// cancellation operations. The server driver only deals in logical response frames.
pub trait ResponseWriter: Send + 'static {
    /// Writes `frames` in order, as one write where the transport allows, draining the vector.
    fn write_frames(
        &mut self,
        frames: &mut Vec<ResponseFrame>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn stopped(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn reset(&mut self);

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Sets how the call's frames are written: how its messages are compressed and the ANSWER
    /// that goes out first; see [`framed::write_response_frames`].
    fn set_codec(&mut self, codec: ResponseCodec);
}

/// A request whose REQUEST frame, and the single message of a unary or server-streaming call,
/// have been read.
pub struct IncomingRequest {
    pub(crate) request: RpcRequest,
    /// The request message of a unary or server-streaming call, empty for the other kinds.
    pub(crate) body: Vec<u8>,
    /// What the rest of the request stream may still send.
    pub(crate) budget: RequestBudget,
    /// The call's datagram flow, once [`open_flow`] has registered it.
    pub(crate) flow: Option<ServerFlow>,
    /// What negotiation decided about compressed messages.
    pub(crate) compression: ServerCall,
    /// What negotiation decided about stream compression, once [`handle_stream`] has authorized
    /// the call.
    pub(crate) stream: ServerStream,
    /// Set once [`handle_stream`] has validated and authorized a streaming call.
    pub(crate) admitted: Option<Admitted>,
    /// The connection metadata set the request offered and the connection stored, which the
    /// call's ANSWER confirms; zero for none. See [`resolve_connection_metadata`].
    pub(crate) connection_metadata: u32,
}

/// Merges the connection metadata the REQUEST offers or refers to into its metadata, and stores
/// an offered set in the connection's `store`, `None` for the ordinary HTTP mapping. It runs
/// before anything reads the metadata.
///
/// # Errors
///
/// `InvalidArgument` for an invalid offer or an unknown reference.
pub fn resolve_connection_metadata(
    store: Option<&ConnectionMetadataStore>,
    incoming: &mut IncomingRequest,
) -> Result<()> {
    incoming.connection_metadata =
        crate::connection_metadata::resolve(store, &mut incoming.request)?;
    Ok(())
}

/// Registers the datagram flow that a streaming REQUEST asks for on the connection's `demux`,
/// claiming the datagrams that overtook the REQUEST.
///
/// Calls without a flow, unary calls, and connections without datagrams (`demux` is `None`) get
/// none. Fails with `InvalidArgument` when the flow is out of range or already live on the
/// connection.
pub fn open_flow(demux: Option<&Demux>, incoming: &mut IncomingRequest) -> Result<()> {
    let request = &incoming.request;
    let flow = request.datagram_flow;
    let kind = request.rpc_kind();
    let Some(demux) = demux else {
        return Ok(());
    };
    if flow == 0 || kind == RpcKind::Unary {
        return Ok(());
    }
    if flow > varint::MAX {
        return Err(Error::from(Status::invalid_argument(
            "datagram flow is out of range",
        )));
    }
    let opened = demux
        .open_server_flow(flow, kind, &incoming.budget.accept)
        .map_err(|_| {
            Error::from(Status::invalid_argument(format!(
                "datagram flow {flow} is already in use on the connection"
            )))
        })?;
    incoming.flow = Some(opened);
    Ok(())
}

/// Reads the REQUEST frame and, for calls with a single request message, that message, bounded by
/// the server's initial request timeout.
///
/// Unknown frames are skipped. Failures here are protocol errors the caller answers with a lone
/// STATUS frame.
pub async fn read_initial_request<R>(server: &Server, recv: &mut R) -> Result<IncomingRequest>
where
    R: FrameRead,
{
    let read = read_request_and_message(server, recv);
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

async fn read_request_and_message<R>(server: &Server, recv: &mut R) -> Result<IncomingRequest>
where
    R: FrameRead,
{
    let max_frame_size = server.max_frame_size();
    let mut budget = RequestBudget::from_options(server.options());
    // Compressed request messages may use any dictionary the server holds.
    budget.accept = Accept::server(server.compression());
    let request =
        framed::read_request(recv, max_frame_size, |len| budget.charge_skipped(len)).await?;
    let compression = compression::negotiate(
        server.compression(),
        &request,
        server
            .route_message_types(&request.method)
            .map(|types| types.response),
        server.route_compress(&request.method),
    );
    let body = if request.rpc_kind().has_single_request_message() {
        let accept = budget.accept.clone();
        // The single request message is never stream-compressed.
        let frame = framed::read_request_frame(recv, max_frame_size, &accept, None, |len| {
            budget.charge_skipped(len)
        })
        .await?;
        match frame {
            Some(body) => Vec::from(body),
            None => {
                return Err(Error::from(Status::invalid_argument(
                    "request stream ended before the request message",
                )));
            }
        }
    } else {
        Vec::new()
    };
    Ok(IncomingRequest {
        request,
        body,
        budget,
        flow: None,
        compression,
        stream: ServerStream::default(),
        admitted: None,
        connection_metadata: 0,
    })
}

/// Handles one already-decoded RPC stream using a transport-neutral response writer and reader.
///
/// `stream_permit` is the connection's stream slot. It and the server-wide request slot are
/// released before the call's terminal STATUS is written or its stream is reset, never after, so
/// a client that sees the call end can start the next one without finding the slot still taken.
///
/// It is a plain function returning an `async` block rather than an `async fn`: an `async fn`
/// keeps its arguments twice in its future, as arguments and as the locals they move into, and
/// this future is moved into a task for every stream.
#[allow(clippy::too_many_lines)]
pub fn handle_stream<W, R>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    stream_permit: Permit,
    mut send: W,
    mut recv: R,
    incoming: IncomingRequest,
    mut shutdown: watch::Receiver<bool>,
) -> impl Future<Output = ()>
where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        mut budget,
        flow,
        mut compression,
        stream: _,
        admitted: _,
        connection_metadata,
    } = incoming;
    async move {
        let cancellation = CancellationToken::new();
        // A streaming call is validated and authorized first, so callers the authorizer rejects can
        // neither probe its methods nor make it reserve stream compression memory. Unary calls never
        // use stream compression, and their handlers authorize them as before.
        let admitted = if request.rpc_kind() == RpcKind::Unary {
            None
        } else {
            match server.admit(&request, &cancellation).await {
                Ok(admitted) => Some(admitted),
                Err(status) => {
                    drop(stream_permit);
                    server.record_rejected_request(&request, body.len(), &status);
                    reject_with_status(&server, &mut send, &mut recv, &request, budget, status)
                        .await;
                    return;
                }
            }
        };
        let mut stream = compression::negotiate_stream(
            &request,
            server.route_compress_stream(&request.method),
            server.stream_budget(),
            server.compression(),
        );
        if server.refuses_optimistic(&request, &stream) {
            drop(stream_permit);
            refuse(&server, &mut send, &request, body.len()).await;
            tokio::spawn(crate::request_pump::discard_request(recv));
            return;
        }
        budget.stream = stream.requests.take();
        let answer = (compression.answer.is_some() || stream.answer || connection_metadata != 0)
            .then(|| RpcAnswer {
                dictionaries: compression.answer.take().unwrap_or_default(),
                stream_compression: stream.answer,
                connection_metadata,
            });
        send.set_codec(ResponseCodec {
            encoder: MessageEncoder::new(compression.responses.clone())
                .with_stream(stream.responses.take()),
            answer,
        });
        let request_body_len = body.len();
        let mut slots = StreamSlots::new(stream_permit);
        if !slots.acquire_request(request_limit.as_ref()) {
            let status = Status::unavailable("too many concurrent RPCs");
            server.record_rejected_request(&request, request_body_len, &status);
            slots.release();
            reject_with_status(&server, &mut send, &mut recv, &request, budget, status).await;
            return;
        }

        if admitted.is_some() {
            let incoming = IncomingRequest {
                request,
                body,
                budget,
                flow,
                compression,
                stream,
                admitted,
                connection_metadata,
            };
            // Boxed, so that the futures of unary calls do not hold room for a streaming call's.
            Box::pin(handle_streaming_rpc(
                server,
                slots,
                send,
                recv,
                incoming,
                cancellation,
                shutdown,
            ))
            .await;
            return;
        }

        let method_for_failure = request.method.clone();
        let counts = RequestCounts {
            plain_compressed: budget.accept.plain_count(),
            ..RequestCounts::default()
        };
        let mut request_end = UnaryRequestEnd::new(recv, server.max_frame_size(), budget);
        let response = tokio::select! {
            biased;
            response = server.handle_request_with_cancellation(request, body, cancellation.clone(), counts) => response,
            failure = request_end.failure() => {
                server.record_active_request_failure(&method_for_failure, request_body_len, failure.status());
                if let Some(source) = failure.cancellation_source() {
                    cancellation.cancel(source);
                }
                if !failure.response_writable() {
                    slots.release();
                    send.reset();
                    request_end.settle(RequestPumpSettle::ResponseStopped);
                    return;
                }
                Err(failure.status().clone())
            },
            stopped = send.stopped() => {
                slots.release();
                cancellation.cancel(if stopped.is_ok() {
                    CancellationSource::PeerReset
                } else {
                    CancellationSource::ConnectionLost
                });
                request_end.settle(RequestPumpSettle::ResponseStopped);
                tracing::debug!("client stopped response stream before RPC completed");
                return;
            }
            changed = shutdown.changed() => {
                let _ = changed;
                cancellation.cancel(CancellationSource::ServerShutdown);
                // Never end a response without its STATUS: the client sees a cancelled call.
                slots.release();
                send.reset();
                request_end.settle(RequestPumpSettle::ServerShutdown);
                return;
            }
        };

        let mut frames = unary_response_frames(response);
        // The STATUS ends the call from the client's point of view.
        drop(slots);
        match send.write_frames(&mut frames).await {
            Ok(()) => {
                let _ = send.finish().await;
                request_end.settle(RequestPumpSettle::ResponseCommitted);
            }
            Err(error) => {
                cancel_from_transport_error(&cancellation, &error);
                request_end.settle(RequestPumpSettle::ConnectionLost);
            }
        }
    }
}

/// Frames a unary response: `[HEADERS] MESSAGE STATUS` on success, a lone STATUS on failure.
fn unary_response_frames(
    response: std::result::Result<ResponseEnvelope<Vec<u8>>, Status>,
) -> Vec<ResponseFrame> {
    match response {
        Ok(response) => {
            let (body, headers, metadata) = response.into_parts();
            let mut frames = Vec::with_capacity(3);
            if !headers.is_empty() {
                frames.push(ResponseFrame::Headers(headers));
            }
            frames.push(ResponseFrame::Message(Bytes::from(body)));
            frames.push(ResponseFrame::Status(Status::ok().with_metadata(metadata)));
            frames
        }
        Err(status) => vec![ResponseFrame::Status(status)],
    }
}

/// Rejects an already-decoded RPC because the connection stream limit was reached.
pub async fn reject_stream<W, R>(
    server: Server,
    mut send: W,
    mut recv: R,
    incoming: IncomingRequest,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        budget,
        flow: _,
        compression: _,
        stream: _,
        admitted: _,
        connection_metadata: _,
    } = incoming;
    let status = Status::unavailable("too many concurrent streams on connection");
    server.record_rejected_request(&request, body.len(), &status);
    reject_with_status(&server, &mut send, &mut recv, &request, budget, status).await;
}

/// Refuses an optimistic call whose stream-compressed requests the server does not decode, with a
/// lone STATUS, after authorizing it but before anything else runs for it: the client replays it
/// without stream compression. The caller then discards the request stream rather than stopping
/// it, since Firefox fails the unread response, STATUS included, when the request side is stopped.
async fn refuse<W>(server: &Server, send: &mut W, request: &RpcRequest, len: usize)
where
    W: ResponseWriter,
{
    let status = Status::refused_stream_compression();
    server.record_rejected_request(request, len, &status);
    if send
        .write_frames(&mut vec![ResponseFrame::Status(status)])
        .await
        .is_ok()
    {
        let _ = send.finish().await;
    }
}

/// Answers a request with a lone STATUS, then lets a request that is not left open finish.
async fn reject_with_status<W, R>(
    server: &Server,
    send: &mut W,
    recv: &mut R,
    request: &RpcRequest,
    mut budget: RequestBudget,
    status: Status,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    if send
        .write_frames(&mut vec![ResponseFrame::Status(status)])
        .await
        .is_ok()
    {
        if request.rpc_kind().has_single_request_message() {
            drain_request_end_or_stop(server, recv, &mut budget).await;
        }
        let _ = send.finish().await;
    }
}

async fn drain_request_end_or_stop<R>(server: &Server, recv: &mut R, budget: &mut RequestBudget)
where
    R: RequestPumpReader,
{
    let read = recv.read_request_end(
        server.max_frame_size(),
        budget,
        "request stream contained a frame after the request message",
    );
    let result = if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| {
                Error::from(Status::deadline_exceeded(
                    "unary request stream finish timeout",
                ))
            })
            .and_then(std::convert::identity)
    } else {
        read.await
    };
    if let Err(error) = result {
        recv.stop_trevrpc();
        tracing::debug!(%error, "failed to drain unary request stream");
    }
}

/// Frames pulled from a handler's response stream, written together.
struct ResponseBatch {
    frames: Vec<ResponseFrame>,
    /// The batch ends with the terminal STATUS.
    terminal: bool,
}

#[allow(clippy::too_many_lines)]
async fn handle_streaming_rpc<W, R>(
    server: Server,
    mut slots: StreamSlots,
    mut send: W,
    recv: R,
    incoming: IncomingRequest,
    cancellation: CancellationToken,
    mut shutdown: watch::Receiver<bool>,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        budget,
        flow,
        compression,
        stream,
        admitted,
        connection_metadata: _,
    } = incoming;
    // Calls whose requests may be stream-compressed get their ANSWER at dispatch, before any
    // request message is read, so the client learns it while it still streams. Other calls get
    // theirs with their first response frames.
    if server.answers_at_dispatch(&request.method)
        && let Err(error) = send.write_frames(&mut Vec::new()).await
    {
        cancel_from_transport_error(&cancellation, &error);
        return;
    }
    let max_frame_size = server.max_frame_size();
    let request_body_len = body.len();
    let method_for_failure = request.method.clone();
    let plain_compressed = budget.accept.plain_count();
    let (request_body, mut request_pump) = start_request_pump(
        recv,
        RequestInputKind::for_rpc_kind(request.rpc_kind()),
        max_frame_size,
        budget,
    );
    // The flow is unregistered when the call ends, however it ends.
    let (request_body, datagram_count, mut datagram_responses, _flow_guard) = match flow {
        Some(flow) => {
            let count = MessageCount::new();
            let request_body = match flow.requests {
                Some(datagrams) => MergedRequests::boxed(
                    request_body,
                    datagrams,
                    flow.guard.drain_handle(),
                    count.clone(),
                ),
                None => request_body,
            };
            let responses = server.route_unreliable(&request.method).responses.then(|| {
                ResponseDatagrams::new(flow.sender.with_compression(compression.responses.clone()))
            });
            (request_body, count, responses, Some(flow.guard))
        }
        None => (request_body, MessageCount::default(), None, None),
    };
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request_with_cancellation(
            request,
            body,
            request_body,
            admitted,
            cancellation.clone(),
            RequestCounts {
                datagrams: datagram_count,
                stream_compressed: stream.count,
                plain_compressed,
            },
        ) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&method_for_failure, request_body_len, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                slots.release();
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            crate::stream::from_iter([ResponseFrame::Status(failure.status().clone())])
        },
        stopped = send.stopped() => {
            slots.release();
            cancellation.cancel(if stopped.is_ok() {
                CancellationSource::PeerReset
            } else {
                CancellationSource::ConnectionLost
            });
            let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
            tracing::debug!("client stopped response stream before streaming RPC handler completed");
            return;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            cancellation.cancel(CancellationSource::ServerShutdown);
            // Never end a response without its STATUS: the client sees a cancelled call.
            slots.release();
            send.reset();
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    let mut batch = ResponseBatch {
        frames: Vec::with_capacity(MESSAGE_FRAME_BATCH),
        terminal: false,
    };
    loop {
        let frame = tokio::select! {
            biased;
            failure = request_pump.failure() => {
                cancellation.set_completion_code(failure.status().code());
                if let Some(source) = failure.cancellation_source() {
                    cancellation.cancel(source);
                }
                if !failure.response_writable() {
                    slots.release();
                    send.reset();
                    let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                    return;
                }
                Some(Ok(ResponseFrame::Status(failure.status().clone())))
            },
            frame = response.next() => frame,
            stopped = send.stopped() => {
                slots.release();
                cancellation.cancel(if stopped.is_ok() {
                    CancellationSource::PeerReset
                } else {
                    CancellationSource::ConnectionLost
                });
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                tracing::debug!("client stopped response stream before streaming RPC completed");
                return;
            }
            changed = shutdown.changed() => {
                let _ = changed;
                cancellation.cancel(CancellationSource::ServerShutdown);
                // Never end a response without its STATUS: the client sees a cancelled call.
                slots.release();
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
                return;
            }
        };

        let Some(frame) = frame else {
            slots.release();
            break;
        };

        batch.terminal = false;
        push_response_frame(&mut batch, frame);
        // Frames that are ready now go out in the same write; a frame that is not ready never
        // delays the ones before it.
        while !batch.terminal && batch.frames.len() < MESSAGE_FRAME_BATCH {
            match response.next().now_or_never() {
                Some(Some(frame)) => push_response_frame(&mut batch, frame),
                Some(None) | None => break,
            }
        }

        if batch.terminal {
            // The STATUS ends the call from the client's point of view.
            slots.release();
        }
        let written = match &mut datagram_responses {
            Some(datagrams) => {
                datagrams
                    .write(&mut send, &mut batch.frames, &cancellation)
                    .await
            }
            None => send.write_frames(&mut batch.frames).await,
        };
        if let Err(error) = written {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            return;
        }
        if batch.terminal {
            break;
        }
    }

    let _ = send.finish().await;
    let _ = request_pump
        .settle(RequestPumpSettle::ResponseCommitted)
        .await;
}

/// Sends a call's response messages as datagrams where they fit, and the rest on the stream.
struct ResponseDatagrams {
    sender: FlowSender,
    /// The call's HEADERS frame has been handed to the transport.
    headers_flushed: bool,
    /// The call has sent a datagram.
    sent_any: bool,
}

impl ResponseDatagrams {
    const fn new(sender: FlowSender) -> Self {
        Self {
            sender,
            headers_flushed: false,
            sent_any: false,
        }
    }

    /// Writes a batch of response frames, draining `frames`.
    ///
    /// HEADERS, even empty ones, are written and flushed before the call's first datagram.
    /// The messages go out packed into datagrams, in order; those that cannot are written to the
    /// stream with the batch's other frames, after the datagrams. The terminal STATUS is
    /// written after every datagram of its batch, and nothing follows it.
    async fn write<W: ResponseWriter>(
        &mut self,
        send: &mut W,
        frames: &mut Vec<ResponseFrame>,
        cancellation: &CancellationToken,
    ) -> Result<()> {
        let has_message = frames
            .iter()
            .any(|frame| matches!(frame, ResponseFrame::Message(_)));
        if has_message && !self.headers_flushed {
            let headers = if matches!(frames.first(), Some(ResponseFrame::Headers(_))) {
                frames.remove(0)
            } else {
                ResponseFrame::Headers(Metadata::new())
            };
            send.write_frames(&mut vec![headers]).await?;
        }
        if has_message || matches!(frames.first(), Some(ResponseFrame::Headers(_))) {
            self.headers_flushed = true;
        }

        // Only HEADERS and the terminal STATUS can accompany the messages of a batch, and they
        // stay on the stream, after the messages that could not travel as datagrams.
        let mut messages = Vec::with_capacity(frames.len());
        let mut other = Vec::new();
        for frame in frames.drain(..) {
            match frame {
                ResponseFrame::Message(body) => messages.push(body),
                frame => other.push(frame),
            }
        }
        let mut on_stream = Vec::new();
        self.sent_any |= !messages.is_empty();
        self.sender
            .send_batch(&mut messages, &mut on_stream, async {
                cancellation.cancelled().await;
            })
            .await;
        if self.sent_any && other.iter().any(ResponseFrame::is_status) {
            // The STATUS must not overtake the datagrams sent before it.
            self.sender
                .flushed(async {
                    cancellation.cancelled().await;
                })
                .await;
        }
        let mut frames_on_stream: Vec<ResponseFrame> =
            on_stream.into_iter().map(ResponseFrame::Message).collect();
        frames_on_stream.append(&mut other);
        if frames_on_stream.is_empty() {
            Ok(())
        } else {
            send.write_frames(&mut frames_on_stream).await
        }
    }
}

/// Adds a frame to a batch; a failed response stream becomes the terminal STATUS.
fn push_response_frame(batch: &mut ResponseBatch, frame: Result<ResponseFrame>) {
    let frame = frame.unwrap_or_else(|error| ResponseFrame::Status(error.into_status()));
    batch.terminal = frame.is_status();
    batch.frames.push(frame);
}

/// Answers a stream with a lone STATUS and finishes it.
pub async fn write_status<W>(mut send: W, status: Status)
where
    W: ResponseWriter,
{
    if send
        .write_frames(&mut vec![ResponseFrame::Status(status)])
        .await
        .is_ok()
    {
        let _ = send.finish().await;
    }
}

/// A reserved slot in a concurrency limit, given back when released or dropped.
#[derive(Debug)]
pub struct Permit(Option<OwnedSemaphorePermit>);

impl Permit {
    /// A permit that holds nothing, for streams admitted without a slot.
    pub const fn none() -> Self {
        Self(None)
    }

    /// Gives the slot back now. Releasing again, or dropping afterwards, does nothing.
    pub fn release(&mut self) {
        self.0 = None;
    }
}

pub fn try_acquire_permit(limit: Option<&Arc<Semaphore>>) -> Option<Permit> {
    limit.map_or(Some(Permit(None)), |semaphore| {
        semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| Permit(Some(permit)))
    })
}

/// The server capacity one RPC holds: its connection stream slot and its server-wide request slot.
struct StreamSlots {
    stream: Permit,
    request: Permit,
}

impl StreamSlots {
    const fn new(stream: Permit) -> Self {
        Self {
            stream,
            request: Permit::none(),
        }
    }

    /// Takes the server-wide request slot, or reports that the limit is reached.
    fn acquire_request(&mut self, limit: Option<&Arc<Semaphore>>) -> bool {
        try_acquire_permit(limit).is_some_and(|permit| {
            self.request = permit;
            true
        })
    }

    fn release(&mut self) {
        self.stream.release();
        self.request.release();
    }
}

/// Answers a request that could not be read with its failure status, after giving back the
/// stream slot.
pub async fn reject_unreadable<W>(
    server: &Server,
    stream_permit: &mut Permit,
    send: W,
    error: Error,
) where
    W: ResponseWriter,
{
    let status = error.into_status();
    server.record_pre_handler_failure(&status);
    stream_permit.release();
    write_status(send, status).await;
}

fn cancel_from_transport_error(cancellation: &CancellationToken, error: &Error) {
    if let Some(code) = error.transport_code() {
        cancellation.cancel(if code == Code::Cancelled {
            CancellationSource::PeerReset
        } else {
            CancellationSource::ConnectionLost
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{read_initial_request, try_acquire_permit, unary_response_frames};
    use crate::framing::{
        DEFAULT_MAX_FRAME_SIZE, encode_frame, encode_message_frame, encode_protobuf_frame,
        frame_type,
    };
    use crate::server::{Server, ServerOptions};
    use crate::test_io::Script;
    use crate::{Code, Metadata, ResponseEnvelope, ResponseFrame, RpcKind, RpcRequest, Status};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn permit_admission_preserves_unlimited_and_bounded_behavior() {
        assert!(try_acquire_permit(None).is_some());
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = try_acquire_permit(Some(&semaphore));
        assert!(permit.is_some());
        assert!(try_acquire_permit(Some(&semaphore)).is_none());
        drop(permit);
    }

    #[test]
    fn released_permit_frees_its_slot_once() {
        let semaphore = Arc::new(Semaphore::new(1));
        let mut permit = try_acquire_permit(Some(&semaphore)).expect("the slot is free");
        assert_eq!(semaphore.available_permits(), 0);

        permit.release();
        assert_eq!(semaphore.available_permits(), 1);

        // A second release, and the drop after it, must not give the slot back again.
        let held = try_acquire_permit(Some(&semaphore)).expect("the slot is free again");
        permit.release();
        drop(permit);
        assert_eq!(semaphore.available_permits(), 0);
        drop(held);
        assert_eq!(semaphore.available_permits(), 1);
    }

    fn request_frame(kind: RpcKind) -> Vec<u8> {
        encode_protobuf_frame(
            frame_type::REQUEST,
            &RpcRequest::new("/svc/m").with_kind(kind),
            DEFAULT_MAX_FRAME_SIZE,
        )
        .expect("request should encode")
    }

    fn message_frame(body: &[u8]) -> Vec<u8> {
        encode_message_frame(body, DEFAULT_MAX_FRAME_SIZE).expect("message should encode")
    }

    fn grease(payload: usize) -> Vec<u8> {
        encode_frame(0x21, &vec![0; payload], DEFAULT_MAX_FRAME_SIZE).expect("grease should encode")
    }

    async fn read(server: &Server, wire: Vec<Vec<u8>>) -> crate::Result<super::IncomingRequest> {
        read_initial_request(server, &mut Script::new(wire.concat())).await
    }

    #[tokio::test]
    async fn unary_and_server_streaming_requests_carry_one_message() {
        let server = Server::new();
        for kind in [RpcKind::Unary, RpcKind::ServerStreaming] {
            let incoming = read(&server, vec![request_frame(kind), message_frame(b"hi")])
                .await
                .expect("request should read");
            assert_eq!(incoming.request.rpc_kind(), kind);
            assert_eq!(incoming.body, b"hi");
        }
    }

    #[tokio::test]
    async fn streaming_requests_read_no_message_before_dispatch() {
        let server = Server::new();
        for kind in [RpcKind::ClientStreaming, RpcKind::BidirectionalStreaming] {
            let incoming = read(&server, vec![request_frame(kind), message_frame(b"later")])
                .await
                .expect("request should read");
            assert_eq!(incoming.body, []);
        }
    }

    #[tokio::test]
    async fn unknown_frames_are_skipped_around_the_request_and_message() {
        let server = Server::new();
        let incoming = read(
            &server,
            vec![
                grease(3),
                request_frame(RpcKind::Unary),
                grease(0),
                encode_frame(0x1234, b"x", 1024).unwrap(),
                message_frame(b"hi"),
            ],
        )
        .await
        .expect("request should read");
        assert_eq!(incoming.body, b"hi");
    }

    #[tokio::test]
    async fn unary_requests_without_a_message_are_invalid() {
        let server = Server::new();
        let ended = read(&server, vec![request_frame(RpcKind::Unary)]).await;
        let status_in_place = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                encode_protobuf_frame(
                    frame_type::STATUS,
                    &crate::RpcStatus::from(Status::ok()),
                    1024,
                )
                .unwrap(),
            ],
        )
        .await;
        for result in [ended, status_in_place] {
            let status = result
                .err()
                .expect("a missing message must be rejected")
                .into_status();
            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn misplaced_frames_before_the_request_are_invalid() {
        let server = Server::new();
        let headers = encode_frame(frame_type::HEADERS, &[], 1024).unwrap();
        for first in [headers, message_frame(b"hi")] {
            let status = read(&server, vec![first, request_frame(RpcKind::Unary)])
                .await
                .err()
                .expect("a stream must open with REQUEST")
                .into_status();
            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn oversized_request_frames_are_rejected_before_their_payload() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_frame_size(16));
        // A MESSAGE declaring 17 bytes against a 16-byte limit, with no payload.
        let status = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                vec![frame_type::MESSAGE.try_into().unwrap(), 17],
            ],
        )
        .await
        .err()
        .expect("oversized frame must be rejected")
        .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn grease_before_the_message_is_charged_to_the_body_limit() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_stream_body_size(Some(100)));
        let status = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                grease(60),
                grease(60),
                message_frame(b"hi"),
            ],
        )
        .await
        .err()
        .expect("grease must not be free")
        .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        // The request message itself is bounded by the frame size, not the stream body limit.
        let incoming = read(
            &server,
            vec![request_frame(RpcKind::Unary), message_frame(&[7; 200])],
        )
        .await
        .expect("a large unary message is fine");
        assert_eq!(incoming.body.len(), 200);
    }

    #[test]
    fn unary_responses_are_headers_message_status() {
        let headers = Metadata::from([("x-a".to_owned(), b"1".to_vec())]);
        let trailers = Metadata::from([("x-t".to_owned(), b"2".to_vec())]);
        let frames = unary_response_frames(Ok(ResponseEnvelope::new(b"reply".to_vec())
            .with_headers(headers.clone())
            .with_metadata(trailers.clone())));
        assert_eq!(
            frames,
            [
                ResponseFrame::Headers(headers),
                ResponseFrame::message(&b"reply"[..]),
                ResponseFrame::Status(Status::ok().with_metadata(trailers)),
            ]
        );

        // Without headers there is no HEADERS frame.
        let frames = unary_response_frames(Ok(ResponseEnvelope::new(Vec::new())));
        assert_eq!(
            frames,
            [
                ResponseFrame::message(Vec::new()),
                ResponseFrame::Status(Status::ok()),
            ]
        );

        // Failures are a lone STATUS.
        let frames = unary_response_frames(Err(Status::not_found("gone")));
        assert_eq!(frames, [ResponseFrame::Status(Status::not_found("gone"))]);
    }
}
