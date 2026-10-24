use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::queue;
use crate::response_state::{ResponseEvent, ResponseState, ResponseStateFailure, UnaryResponse};
use crate::sync::MutexExt as _;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{
    BoxStream, Code, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcKind, RpcRequest,
    Status, Unreliable,
};
use bytes::Bytes;
use prost::Message;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub mod call;
mod call_end;
mod channel;
mod native;
mod qmux;
mod quic;
mod race;
mod upload;

pub use call::{Call, bidirectional_streaming, client_streaming, server_streaming, unary};
pub use channel::{
    Channel, ChannelConfig, ChannelEvent, ChannelPhase, ChannelState, ExponentialBackoff,
    ReconnectBackoff,
};
pub use qmux::QmuxTransport;
pub use quic::QuicTransport;
pub use race::{TransportKind, TransportPolicy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallOptions {
    timeout: Option<Duration>,
    max_response_body_size: usize,
    max_response_messages: Option<usize>,
    max_response_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    metadata: Metadata,
    /// The directions marked unreliable, or `None` to leave them to the call; see
    /// [`Call::default_unreliable`].
    unreliable: Option<Unreliable>,
    /// The method's message types, which the call sets from [`Call::message_types`].
    message_types: Option<MessageTypes>,
}

/// The full protobuf names of a method's request and response message types.
///
/// Generated code passes them with [`Call::message_types`] and
/// [`RouteOptions::with_message_types`](crate::server::RouteOptions::with_message_types); they
/// pick the dictionaries of [compressed messages](crate::Compression).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MessageTypes {
    /// The request message type, such as `example.v1.HelloRequest`.
    pub request: &'static str,
    /// The response message type.
    pub response: &'static str,
}

impl MessageTypes {
    /// Names a method's request and response message types.
    #[must_use]
    pub const fn new(request: &'static str, response: &'static str) -> Self {
        Self { request, response }
    }
}

/// What the call layer knows about a call besides its request, for the transport that carries it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct CallInfo {
    /// The directions marked unreliable; see [`StreamingRpcTransport::streaming_call_with_info`].
    pub unreliable: Unreliable,
    /// The method's message types, when generated code passed them.
    pub message_types: Option<MessageTypes>,
}

impl CallInfo {
    /// Describes a call with these unreliable directions and message types.
    #[must_use]
    pub const fn new(unreliable: Unreliable, message_types: Option<MessageTypes>) -> Self {
        Self {
            unreliable,
            message_types,
        }
    }
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            max_response_body_size: DEFAULT_MAX_FRAME_SIZE,
            max_response_messages: None,
            max_response_stream_body_size: None,
            stream_idle_timeout: None,
            metadata: Metadata::new(),
            unreliable: None,
            message_types: None,
        }
    }
}

impl CallOptions {
    /// Creates call options with the defaults: no timeout, no metadata, a response message of at
    /// most the default frame size, and no limit on the number, total size, or spacing of the
    /// messages of a response stream.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the per-call timeout, if one is configured.
    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// Returns the maximum unary response body size in bytes.
    #[must_use]
    pub const fn max_response_body_size(&self) -> usize {
        self.max_response_body_size
    }

    /// Returns the maximum number of response messages allowed on a stream.
    #[must_use]
    pub const fn max_response_messages(&self) -> Option<usize> {
        self.max_response_messages
    }

    /// Returns the maximum response message body size allowed on a stream.
    #[must_use]
    pub const fn max_response_stream_body_size(&self) -> Option<usize> {
        self.max_response_stream_body_size
    }

    /// Returns the maximum idle time allowed while waiting for the next stream message.
    #[must_use]
    pub const fn stream_idle_timeout(&self) -> Option<Duration> {
        self.stream_idle_timeout
    }

    /// Returns the metadata sent with the request.
    #[must_use]
    pub const fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// Returns the directions marked unreliable, or `None` when the options leave them to the call
    /// (see [`Call::default_unreliable`]).
    #[must_use]
    pub const fn unreliable(&self) -> Option<Unreliable> {
        self.unreliable
    }

    /// Sets the per-call timeout.
    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Clears the per-call timeout.
    #[must_use]
    pub const fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Sets the maximum unary response body size in bytes.
    #[must_use]
    pub const fn with_max_response_body_size(mut self, max_response_body_size: usize) -> Self {
        self.max_response_body_size = max_response_body_size;
        self
    }

    /// Sets the maximum number of response messages allowed on a stream.
    #[must_use]
    pub const fn with_max_response_messages(
        mut self,
        max_response_messages: Option<usize>,
    ) -> Self {
        self.max_response_messages = max_response_messages;
        self
    }

    /// Sets the maximum response message body size allowed on a stream.
    #[must_use]
    pub const fn with_max_response_stream_body_size(
        mut self,
        max_response_stream_body_size: Option<usize>,
    ) -> Self {
        self.max_response_stream_body_size = max_response_stream_body_size;
        self
    }

    /// Sets the maximum idle time allowed while waiting for the next stream message.
    #[must_use]
    pub const fn with_stream_idle_timeout(mut self, stream_idle_timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = stream_idle_timeout;
        self
    }

    /// Marks the directions of a streaming call that are [`Unreliable`], replacing any marked
    /// already and the call's default, which generated clients take from the schema (see
    /// [`Call::default_unreliable`]). `Unreliable::NONE` keeps every message on the stream.
    ///
    /// When the connection that carries the call supports datagrams, the call gets a datagram
    /// flow, and the client sends its streamed requests as datagrams if `unreliable.requests` is
    /// set. The server decides on its own whether to send its responses as datagrams; the call
    /// receives them either way. Over transports without datagrams, and for a direction that does
    /// not stream, the setting changes nothing.
    #[must_use]
    pub const fn with_unreliable(mut self, unreliable: Unreliable) -> Self {
        self.unreliable = Some(unreliable);
        self
    }

    /// Adds request metadata after normalizing the metadata key.
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        let key = key.into();
        self.metadata
            .insert(normalize_metadata_key(&key), value.into());
        self
    }

    /// Replaces the full metadata map sent with the request.
    #[must_use]
    pub fn with_metadata_map(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[crate::async_trait]
pub trait RpcTransport: Send + Sync + 'static {
    /// Sends a unary `TrevRPC` request whose single message is `body` and returns its response.
    ///
    /// A failed call, including a non-`OK` status from the server, is the error; the status keeps
    /// its trailing metadata.
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>>;

    /// Sends a unary request like [`RpcTransport::call`], with what the call layer knows about
    /// the call.
    ///
    /// A transport that compresses messages uses `info.message_types` to pick the dictionaries
    /// of the call. The default implementation ignores `info`.
    async fn call_with_info(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        let _ = info;
        self.call(request, body).await
    }
}

/// A transport with explicit support for streaming RPCs.
#[crate::async_trait]
pub trait StreamingRpcTransport: RpcTransport {
    /// Sends a streaming `TrevRPC` request and returns the frames of its response.
    ///
    /// The returned frames are the known frames the server sent, in order, ending with the
    /// terminal [`ResponseFrame::Status`] once the stream has ended cleanly. Frames of unknown type
    /// are skipped by the transport. The call layer checks that the frames follow the response
    /// grammar.
    ///
    /// When the transport reads the terminal STATUS while the request body is still uploading, it
    /// must stop the upload and drop `request_body`.
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>>;

    /// Sends a streaming request like [`StreamingRpcTransport::streaming_call`], with what the
    /// call layer knows about the call.
    ///
    /// A transport whose connection carries datagrams gives a call with directions marked
    /// unreliable in `info.unreliable` a datagram flow (`RpcRequest::datagram_flow`), and sends
    /// request messages as datagrams if `unreliable.requests` is set. A transport that compresses
    /// messages uses `info.message_types` to pick the dictionaries of the call. These decisions
    /// belong to the connection that carries the call, not to the caller: a channel may reach a
    /// different connection after the call began. The default implementation ignores `info`, so
    /// every message stays on the stream, uncompressed.
    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        let _ = info;
        self.streaming_call(request, request_body).await
    }
}

#[crate::async_trait]
impl<T> RpcTransport for Arc<T>
where
    T: RpcTransport + ?Sized,
{
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.as_ref().call(request, body).await
    }

    async fn call_with_info(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        self.as_ref().call_with_info(request, body, info).await
    }
}

#[crate::async_trait]
impl<T> StreamingRpcTransport for Arc<T>
where
    T: StreamingRpcTransport + ?Sized,
{
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.as_ref().streaming_call(request, request_body).await
    }

    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.as_ref()
            .streaming_call_with_info(request, request_body, info)
            .await
    }
}

/// Collects a unary response from the frames of a response stream.
///
/// This is the unary half of a [`StreamingRpcTransport`] for transports that implement
/// [`RpcTransport::call`] by opening a stream: the frames must be `[HEADERS] MESSAGE STATUS` and
/// the stream must end after the status. A non-`OK` status becomes the error.
pub async fn collect_unary_response(
    mut frames: BoxStream<ResponseFrame>,
) -> Result<ResponseEnvelope<Bytes>> {
    let mut response = UnaryResponse::default();
    while let Some(frame) = frames.next().await.transpose()? {
        if response.accept(frame).map_err(Error::from)? {
            if frames.next().await.transpose()?.is_some() {
                return Err(Error::from(Status::internal(
                    "response stream continued after terminal status",
                )));
            }
            break;
        }
    }
    response.finish().map_err(Error::from)
}

/// Makes a unary call whose encoded request message is `body` and returns the decoded response
/// with the response headers and trailing metadata.
async fn unary_response<T, Res>(
    transport: &T,
    method: &str,
    body: Vec<u8>,
    options: CallOptions,
) -> Result<ResponseEnvelope<Res>>
where
    T: RpcTransport + ?Sized,
    Res: Message + Default,
{
    let CallOptions {
        timeout,
        max_response_body_size,
        max_response_messages: _,
        max_response_stream_body_size: _,
        stream_idle_timeout: _,
        metadata,
        unreliable: _,
        message_types,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;
    let timeout_nanos = timeout_nanos(timeout)?;

    let rpc_request = RpcRequest::new(method)
        .with_metadata(metadata)
        .with_timeout_nanos(timeout_nanos);
    let info = CallInfo::new(Unreliable::NONE, message_types);
    let response = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, transport.call_with_info(rpc_request, body, info))
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))??
    } else {
        transport.call_with_info(rpc_request, body, info).await?
    };

    validate_response_metadata(&response)?;

    if response.message().len() > max_response_body_size {
        return Err(Error::FrameTooLarge {
            len: response.message().len(),
            max: max_response_body_size,
        });
    }

    let (body, headers, metadata) = response.into_parts();
    let message = Res::decode(body).map_err(Error::from)?;
    Ok(ResponseEnvelope::new(message)
        .with_headers(headers)
        .with_metadata(metadata))
}

/// Starts a server-streaming call whose encoded request message is `request`.
async fn start_server_streaming<T, Res>(
    transport: &T,
    method: &str,
    request: Vec<u8>,
    options: CallOptions,
) -> Result<ResponseStream<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(method, RpcKind::ServerStreaming, options)?;
    open_response_stream(
        transport,
        prepared,
        crate::stream::from_iter([request]),
        None,
    )
    .await
}

/// Starts a client-streaming call and returns its sendable call object.
async fn start_client_streaming<T, Req, Res>(
    transport: &T,
    method: &str,
    options: CallOptions,
) -> Result<ClientStreamingCall<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(method, RpcKind::ClientStreaming, options)?;
    let (sender, requests, call_end) = request_channel(prepared.deadline);
    let response = open_response_stream(transport, prepared, requests, Some(call_end)).await?;

    Ok(ClientStreamingCall::new(sender, response))
}

/// Makes a client-streaming call that sends `requests` and returns its response with the
/// response headers and trailing metadata.
async fn client_streaming_send_all<T, Req, Res>(
    transport: &T,
    method: &str,
    requests: BoxStream<Req>,
    options: CallOptions,
) -> Result<ResponseEnvelope<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(method, RpcKind::ClientStreaming, options)?;
    let mut response =
        open_response_stream(transport, prepared, crate::stream::encode(requests), None).await?;

    read_unary_response_envelope_from_message_stream(&mut response).await
}

/// Starts a bidirectional-streaming call and returns its splittable call object.
async fn start_bidirectional_streaming<T, Req, Res>(
    transport: &T,
    method: &str,
    options: CallOptions,
) -> Result<BidirectionalCall<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(method, RpcKind::BidirectionalStreaming, options)?;
    let (sender, requests, call_end) = request_channel(prepared.deadline);
    let response = open_response_stream(transport, prepared, requests, Some(call_end)).await?;

    Ok(BidirectionalCall::new(sender, response))
}

/// Starts a bidirectional-streaming call that sends `requests` and returns its response stream.
async fn bidirectional_streaming_send_all<T, Req, Res>(
    transport: &T,
    method: &str,
    requests: BoxStream<Req>,
    options: CallOptions,
) -> Result<ResponseStream<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(method, RpcKind::BidirectionalStreaming, options)?;
    open_response_stream(transport, prepared, crate::stream::encode(requests), None).await
}

#[derive(Clone, Copy)]
enum RequestControl {
    Cancel,
}

struct RequestMessageStream {
    receiver: queue::Receiver<Vec<u8>>,
    control: mpsc::UnboundedReceiver<RequestControl>,
    cancelled: bool,
}

impl Stream for RequestMessageStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.cancelled
            && matches!(
                self.control.poll_recv(cx),
                Poll::Ready(Some(RequestControl::Cancel))
            )
        {
            self.cancelled = true;
            return Poll::Ready(Some(Err(Error::from(Status::cancelled(
                "request sender was dropped before finish",
            )))));
        }
        if self.cancelled {
            return Poll::Ready(None);
        }
        self.receiver.poll_recv(cx).map(|item| item.map(Ok))
    }
}

/// Creates the request half of a streaming call: the sender, and the stream of encoded messages
/// the transport uploads. The returned [`call_end::EndSender`] belongs to whoever reads the
/// response; it lets sends fail with the call's terminal status.
fn request_channel<T>(
    deadline: Option<Instant>,
) -> (RequestSender<T>, BoxStream<Vec<u8>>, call_end::EndSender) {
    let (sender, receiver) = queue::channel(queue::MESSAGES);
    let (control, control_rx) = mpsc::unbounded_channel();
    let (call_end, call_end_watch) = call_end::channel(deadline);
    (
        RequestSender {
            state: Arc::new(RequestSenderState {
                sender: Mutex::new(Some(sender)),
                call_end: call_end_watch,
            }),
            control,
            finished: false,
            _messages: PhantomData,
        },
        Box::pin(RequestMessageStream {
            receiver,
            control: control_rx,
            cancelled: false,
        }),
        call_end,
    )
}

struct RequestSenderState {
    sender: Mutex<Option<queue::Sender<Vec<u8>>>>,
    call_end: call_end::EndWatch,
}

/// The independently movable request half of a streaming call.
pub struct RequestSender<T> {
    state: Arc<RequestSenderState>,
    control: mpsc::UnboundedSender<RequestControl>,
    finished: bool,
    _messages: PhantomData<fn(T)>,
}

impl<T> RequestSender<T>
where
    T: Message + Send + 'static,
{
    /// Sends one request message with bounded backpressure.
    ///
    /// The message is encoded when this is called and queued for upload; a few messages are
    /// queued ahead of the upload, and the send waits while the queue is full. Fails once the
    /// server has ended the call, without waiting for a peer that no longer reads: with the
    /// server's terminal status when it is not `OK`, and with `Cancelled` when the server
    /// completed the call before the request stream finished. A send after [`Self::finish`] or
    /// on a closed stream also fails with `Cancelled`. The status is known as soon as the server
    /// sends it, unless the caller is not reading a response that has more than a couple of
    /// messages queued ahead of it; sends then fail with `Cancelled` once the upload stops.
    pub fn send(
        &self,
        request: T,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_message(&self.state, request)
    }
}

impl<T> RequestSender<T> {
    /// Gracefully finishes the request stream after all accepted messages.
    pub fn finish(mut self) -> Result<()> {
        self.finish_in_place();
        Ok(())
    }

    fn finish_in_place(&mut self) {
        self.finished = true;
        self.state.sender.lock_unpoisoned().take();
    }
}

/// The outcome of trying to queue a message without waiting.
enum Attempt {
    Queued,
    Ended(Error),
    /// The upload stopped taking messages; the response says why.
    Stopped,
    /// The queue is full; the message is handed back with a sender to wait on.
    Full(Vec<u8>, queue::Sender<Vec<u8>>),
}

impl RequestSenderState {
    #[expect(
        clippy::significant_drop_tightening,
        reason = "the sender is borrowed from the lock for the whole attempt, which saves a clone per send"
    )]
    fn try_send(&self, body: Vec<u8>) -> Attempt {
        let guard = self.sender.lock_unpoisoned();
        let Some(sender) = guard.as_ref() else {
            return Attempt::Ended(Error::from(Status::cancelled("request stream is closed")));
        };
        if let Some(error) = self.call_end.error_if_ended() {
            return Attempt::Ended(error);
        }
        let size = body.len();
        match sender.try_send(body, size) {
            Ok(()) => Attempt::Queued,
            Err(queue::TrySendError::Full(body)) => Attempt::Full(body, sender.clone()),
            Err(queue::TrySendError::Closed(_)) => Attempt::Stopped,
        }
    }
}

/// Encodes `request` and sends it. The message is encoded, and dropped, before the returned future
/// is first polled.
fn send_message<T: Message>(
    state: &Arc<RequestSenderState>,
    request: T,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    let body = request.encode_to_vec();
    drop(request);
    send_encoded(Arc::clone(state), body)
}

async fn send_encoded(state: Arc<RequestSenderState>, body: Vec<u8>) -> Result<()> {
    let (body, sender) = match state.try_send(body) {
        Attempt::Queued => return Ok(()),
        Attempt::Ended(error) => return Err(error),
        Attempt::Stopped => {
            let mut call_end = state.call_end.clone();
            call_end.upload_stopped();
            return Err(call_end.ended().await);
        }
        Attempt::Full(body, sender) => (body, sender),
    };

    // Waiting for room, the response side reads on to find a STATUS even if nobody reads it.
    let mut call_end = state.call_end.clone();
    let _waiting = call_end.waiting_send();
    let size = body.len();
    tokio::select! {
        biased;
        error = call_end.ended() => Err(error),
        sent = sender.send(body, size) => if sent.is_ok() {
            Ok(())
        } else {
            // The upload stopped: the call has ended, and the response says how.
            call_end.upload_stopped();
            Err(call_end.ended().await)
        },
    }
}

impl<T> Drop for RequestSender<T> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.control.send(RequestControl::Cancel);
        }
    }
}

pub struct ClientStreamingCall<Req, Res> {
    sender: RequestSender<Req>,
    response: ResponseStream<Res>,
}

impl<Req, Res> ClientStreamingCall<Req, Res> {
    const fn new(sender: RequestSender<Req>, response: ResponseStream<Res>) -> Self {
        Self { sender, response }
    }
}

impl<Req, Res> ClientStreamingCall<Req, Res>
where
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    /// Sends one request message; see [`RequestSender::send`] for how it fails once the server
    /// has ended the call.
    pub fn send(
        &self,
        request: Req,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_message(&self.sender.state, request)
    }

    pub fn close_send(&mut self) -> Result<()> {
        self.sender.finish_in_place();
        Ok(())
    }

    /// Waits for the server's response headers; see [`ResponseStream::headers`].
    pub async fn headers(&mut self) -> Result<&Metadata> {
        self.response.headers().await
    }

    pub async fn close_and_recv(mut self) -> Result<Res> {
        self.close_send()?;
        read_unary_response_from_message_stream(&mut self.response).await
    }

    /// Closes the request stream and returns the response with the response headers and trailing
    /// metadata.
    pub async fn close_and_recv_with_response(mut self) -> Result<ResponseEnvelope<Res>> {
        self.close_send()?;
        read_unary_response_envelope_from_message_stream(&mut self.response).await
    }
}

/// A bidirectional call whose request and response halves can move independently.
pub struct BidirectionalCall<Req, Res> {
    sender: RequestSender<Req>,
    response: ResponseStream<Res>,
}

impl<Req, Res> BidirectionalCall<Req, Res> {
    const fn new(sender: RequestSender<Req>, response: ResponseStream<Res>) -> Self {
        Self { sender, response }
    }

    /// Splits the call into independently movable request and response halves.
    #[must_use]
    pub fn split(self) -> (RequestSender<Req>, ResponseStream<Res>) {
        (self.sender, self.response)
    }
}

impl<Req, Res> BidirectionalCall<Req, Res>
where
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    /// Sends one request message; see [`RequestSender::send`] for how it fails once the server
    /// has ended the call.
    pub fn send(
        &self,
        request: Req,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_message(&self.sender.state, request)
    }

    pub async fn recv(&mut self) -> Result<Option<Res>> {
        self.response.next().await.transpose()
    }

    pub fn close_send(&mut self) -> Result<()> {
        self.sender.finish_in_place();
        Ok(())
    }

    /// Waits for the server's response headers; see [`ResponseStream::headers`].
    pub async fn headers(&mut self) -> Result<&Metadata> {
        self.response.headers().await
    }

    #[must_use]
    pub const fn terminal_status(&self) -> Option<&Status> {
        self.response.terminal_status()
    }
}

impl<Req, Res> Stream for BidirectionalCall<Req, Res>
where
    Res: Message + Default,
{
    type Item = Result<Res>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.response.poll_next_unpin(cx)
    }
}

struct PreparedStreamingCall {
    request: RpcRequest,
    deadline: Option<Instant>,
    max_response_body_size: usize,
    max_response_messages: Option<usize>,
    max_response_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    info: CallInfo,
}

fn prepare_streaming_request(
    method: &str,
    kind: RpcKind,
    options: CallOptions,
) -> Result<PreparedStreamingCall> {
    let CallOptions {
        timeout,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        metadata,
        unreliable,
        message_types,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;
    let timeout_nanos = timeout_nanos(timeout)?;
    let info = CallInfo::new(unreliable.unwrap_or_default(), message_types);

    Ok(PreparedStreamingCall {
        request: RpcRequest::new(method)
            .with_kind(kind)
            .with_metadata(metadata)
            .with_timeout_nanos(timeout_nanos),
        deadline: timeout.and_then(|timeout| Instant::now().checked_add(timeout)),
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        info,
    })
}

fn timeout_nanos(timeout: Option<Duration>) -> Result<u64> {
    let Some(timeout) = timeout else {
        return Ok(0);
    };

    let nanos = timeout.as_nanos().max(1);

    u64::try_from(nanos)
        .map_err(|_| Error::from(Status::invalid_argument("RPC timeout is too large")))
}

async fn streaming_call_with_deadline<T>(
    transport: &T,
    request: RpcRequest,
    request_body: BoxStream<Vec<u8>>,
    info: CallInfo,
    deadline: Option<Instant>,
) -> Result<BoxStream<ResponseFrame>>
where
    T: StreamingRpcTransport + ?Sized,
{
    // The common call without unreliable directions or message types takes the transport's
    // plain entry point.
    let call = async {
        if info == CallInfo::default() {
            transport.streaming_call(request, request_body).await
        } else {
            transport
                .streaming_call_with_info(request, request_body, info)
                .await
        }
    };
    match remaining_timeout(deadline)? {
        Some(timeout) => tokio::time::timeout(timeout, call)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))?,
        None => call.await,
    }
}

async fn open_response_stream<T, Res>(
    transport: &T,
    prepared: PreparedStreamingCall,
    request_body: BoxStream<Vec<u8>>,
    call_end: Option<call_end::EndSender>,
) -> Result<ResponseStream<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Res: Message + Default + Send + 'static,
{
    let PreparedStreamingCall {
        request,
        deadline,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        info,
    } = prepared;
    let response =
        streaming_call_with_deadline(transport, request, request_body, info, deadline).await?;
    let response = match call_end {
        Some(call_end) => call_end::read_response(
            response,
            call_end,
            call_end::ReadAhead::new(max_response_messages, max_response_stream_body_size),
        ),
        None => response,
    };

    Ok(ResponseStream::<Res>::new(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    ))
}

#[cfg(test)]
async fn read_unary_response_from_stream<Res>(
    response: BoxStream<ResponseFrame>,
    max_response_body_size: usize,
    max_response_messages: Option<usize>,
    max_response_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    deadline: Option<Instant>,
) -> Result<Res>
where
    Res: Message + Default + Send + 'static,
{
    let mut response = ResponseStream::<Res>::new(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    );
    read_unary_response_from_message_stream(&mut response).await
}

pub(crate) async fn read_unary_response_from_message_stream<Res>(
    response: &mut ResponseStream<Res>,
) -> Result<Res>
where
    Res: Message + Default + Send + 'static,
{
    Ok(read_unary_response_envelope_from_message_stream(response)
        .await?
        .into_parts()
        .0)
}

async fn read_unary_response_envelope_from_message_stream<Res>(
    response: &mut ResponseStream<Res>,
) -> Result<ResponseEnvelope<Res>>
where
    Res: Message + Default + Send + 'static,
{
    let mut first = None;
    let mut response_count = 0_usize;

    while let Some(message) = response.next().await {
        let message = message?;
        response_count = response_count.saturating_add(1);
        if first.is_none() {
            first = Some(message);
        }
    }

    let (Some(first), 1) = (first, response_count) else {
        return Err(Error::from(Status::internal(format!(
            "client-streaming RPC returned {response_count} response messages; expected exactly one"
        ))));
    };

    Ok(ResponseEnvelope::new(first)
        .with_headers(std::mem::take(&mut response.headers))
        .with_metadata(
            response
                .terminal_status()
                .map_or_else(Metadata::new, |status| status.metadata().clone()),
        ))
}

/// A decoded response stream that retains the response headers and the clean terminal status and
/// metadata.
pub struct ResponseStream<T> {
    inner: BoxStream<ResponseFrame>,
    max_body_size: usize,
    max_messages: Option<usize>,
    max_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    messages: usize,
    stream_body_size: usize,
    deadline: Option<Instant>,
    wait: Option<(Pin<Box<tokio::time::Sleep>>, TimeoutReason)>,
    pending_terminal: bool,
    done: bool,
    state: ResponseState,
    /// The response headers, once [`Self::headers_ready`].
    headers: Metadata,
    /// Whether the headers are known: a HEADERS frame arrived, or the first message or the status
    /// proved there are none.
    headers_ready: bool,
    /// An item read ahead while waiting for the headers, returned by the next poll.
    stashed: Option<Result<T>>,
}

impl<T> Unpin for ResponseStream<T> {}

/// What one step through the response stream produced.
enum Step<T> {
    Headers,
    Message(T),
}

impl<T> ResponseStream<T>
where
    T: Message + Default,
{
    pub(crate) fn new(
        inner: BoxStream<ResponseFrame>,
        max_body_size: usize,
        max_messages: Option<usize>,
        max_stream_body_size: Option<usize>,
        stream_idle_timeout: Option<Duration>,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            inner,
            max_body_size,
            max_messages,
            max_stream_body_size,
            stream_idle_timeout,
            messages: 0,
            stream_body_size: 0,
            deadline,
            wait: None,
            pending_terminal: false,
            done: false,
            state: ResponseState::default(),
            headers: Metadata::new(),
            headers_ready: false,
            stashed: None,
        }
    }

    /// Returns the terminal status after the status frame and clean FIN are consumed.
    #[must_use]
    pub const fn terminal_status(&self) -> Option<&Status> {
        self.state.terminal_status()
    }

    /// Returns terminal metadata after the stream reaches clean EOF.
    #[must_use]
    pub fn terminal_metadata(&self) -> Option<&Metadata> {
        self.terminal_status().map(Status::metadata)
    }

    /// Waits for the server's response headers.
    ///
    /// Resolves as soon as the first response frame arrives: the HEADERS frame, or, when the
    /// server sent none, the first message or the terminal status, in which case the headers are
    /// empty. A message read while waiting is not lost; the next poll of the stream returns it.
    /// If the call fails before any headers arrive, the error is returned here and again by the
    /// stream.
    pub async fn headers(&mut self) -> Result<&Metadata> {
        std::future::poll_fn(|cx| self.poll_headers(cx)).await?;
        Ok(&self.headers)
    }

    fn poll_headers(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        loop {
            if self.headers_ready {
                return Poll::Ready(Ok(()));
            }
            if let Some(Err(Error::Status(status))) = &self.stashed {
                return Poll::Ready(Err(Error::from(status.clone())));
            }

            match self.poll_step(cx) {
                Poll::Pending => return Poll::Pending,
                // `headers_ready` is now set.
                Poll::Ready(Some(Ok(Step::Headers))) => {}
                Poll::Ready(Some(Ok(Step::Message(message)))) => {
                    self.stashed = Some(Ok(message));
                }
                Poll::Ready(Some(Err(error))) => {
                    let status = error.into_status();
                    self.stashed = Some(Err(Error::from(status.clone())));
                    return Poll::Ready(Err(Error::from(status)));
                }
                Poll::Ready(None) => self.headers_ready = true,
            }
        }
    }

    fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<ResponseFrame>>> {
        if let Err(error) = remaining_timeout(self.deadline) {
            return Poll::Ready(Err(error));
        }

        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(frame)) => {
                self.wait = None;
                return Poll::Ready(frame.map(Some));
            }
            Poll::Ready(None) => {
                self.wait = None;
                return Poll::Ready(Ok(None));
            }
            Poll::Pending => {}
        }

        if self.wait.is_none() {
            match next_timeout(self.deadline, self.stream_idle_timeout) {
                Ok(Some((timeout, reason))) => {
                    self.wait = Some((Box::pin(tokio::time::sleep(timeout)), reason));
                }
                Ok(None) => return Poll::Pending,
                Err(error) => return Poll::Ready(Err(error)),
            }
        }

        let Some((wait, reason)) = &mut self.wait else {
            return Poll::Pending;
        };
        match wait.as_mut().poll(cx) {
            Poll::Ready(()) => Poll::Ready(Err(Error::from(reason.status()))),
            Poll::Pending => Poll::Pending,
        }
    }

    fn finish(&mut self) -> Option<Result<Step<T>>> {
        self.done = true;
        self.headers_ready = true;
        self.state
            .finish()
            .err()
            .map(|failure| Err(Error::from(failure.into_status())))
    }

    const fn fail(&mut self, error: Error) -> Poll<Option<Result<Step<T>>>> {
        self.done = true;
        Poll::Ready(Some(Err(error)))
    }

    fn fail_state(&mut self, failure: ResponseStateFailure) -> Poll<Option<Result<Step<T>>>> {
        self.fail(Error::from(failure.into_status()))
    }

    /// Reads response frames until one yields headers or a message, or the call ends.
    fn poll_step(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<Step<T>>>> {
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            let frame = match self.poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(Some(frame))) => frame,
                Poll::Ready(Ok(None)) => return Poll::Ready(self.finish()),
                Poll::Ready(Err(error)) => {
                    self.done = true;
                    if self.pending_terminal {
                        self.state.abort();
                        return Poll::Ready(Some(Err(post_terminal_error(error))));
                    }
                    return Poll::Ready(Some(Err(error)));
                }
            };

            if self.pending_terminal {
                self.state.abort();
                return self.fail(Error::from(Status::internal(
                    "response stream continued after terminal status",
                )));
            }

            if let ResponseFrame::Message(body) = &frame {
                if let Some(max) = self.max_messages
                    && self.messages >= max
                {
                    return self.fail(Error::from(Status::resource_exhausted(format!(
                        "response stream exceeded maximum of {max} messages"
                    ))));
                }

                if body.len() > self.max_body_size {
                    return self.fail(Error::FrameTooLarge {
                        len: body.len(),
                        max: self.max_body_size,
                    });
                }

                self.messages = self.messages.saturating_add(1);
                self.stream_body_size = self.stream_body_size.saturating_add(body.len());

                if let Some(max) = self.max_stream_body_size
                    && self.stream_body_size > max
                {
                    return self.fail(Error::from(Status::resource_exhausted(format!(
                        "response stream exceeded maximum body size of {max} bytes"
                    ))));
                }
            }

            match self.state.accept(frame) {
                Ok(ResponseEvent::Headers(headers)) => {
                    self.headers = headers;
                    self.headers_ready = true;
                    return Poll::Ready(Some(Ok(Step::Headers)));
                }
                Ok(ResponseEvent::Message(body)) => {
                    self.headers_ready = true;
                    let Ok(message) = T::decode(body) else {
                        return self.fail_state(ResponseStateFailure::malformed_protobuf());
                    };
                    return Poll::Ready(Some(Ok(Step::Message(message))));
                }
                Ok(ResponseEvent::Terminal) => {
                    self.headers_ready = true;
                    self.pending_terminal = true;
                }
                Err(failure) => return self.fail_state(failure),
            }
        }
    }
}

fn post_terminal_error(error: Error) -> Error {
    let malformed_trailing = match &error {
        Error::Decode(_) | Error::FrameTooLarge { .. } => true,
        Error::Status(status) => matches!(
            status.code(),
            Code::InvalidArgument | Code::ResourceExhausted | Code::Internal | Code::DataLoss
        ),
        Error::Transport(error) => error.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )
        }),
        Error::Encode(_) => false,
    };

    if malformed_trailing {
        Error::from(Status::internal(
            "response stream contained malformed trailing data after terminal status",
        ))
    } else {
        error
    }
}

impl<T> Stream for ResponseStream<T>
where
    T: Message + Default,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(item) = self.stashed.take() {
            return Poll::Ready(Some(item));
        }

        loop {
            match self.poll_step(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(Step::Headers))) => {}
                Poll::Ready(Some(Ok(Step::Message(message)))) => {
                    return Poll::Ready(Some(Ok(message)));
                }
                Poll::Ready(Some(Err(error))) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(None) => return Poll::Ready(None),
            }
        }
    }
}

#[derive(Clone, Copy)]
enum TimeoutReason {
    Deadline,
    Idle,
}

impl TimeoutReason {
    fn status(self) -> Status {
        match self {
            Self::Deadline => Status::deadline_exceeded("RPC deadline exceeded"),
            Self::Idle => Status::unavailable("response stream idle timeout"),
        }
    }
}

fn next_timeout(
    deadline: Option<Instant>,
    idle_timeout: Option<Duration>,
) -> Result<Option<(Duration, TimeoutReason)>> {
    let deadline = remaining_timeout(deadline)?.map(|timeout| (timeout, TimeoutReason::Deadline));
    let idle = idle_timeout.map(|timeout| (timeout, TimeoutReason::Idle));

    Ok(match (deadline, idle) {
        (Some(deadline), Some(idle)) if deadline.0 <= idle.0 => Some(deadline),
        (Some(_) | None, Some(idle)) => Some(idle),
        (Some(deadline), None) => Some(deadline),
        (None, None) => None,
    })
}

fn remaining_timeout(deadline: Option<Instant>) -> Result<Option<Duration>> {
    let Some(deadline) = deadline else {
        return Ok(None);
    };

    deadline
        .checked_duration_since(Instant::now())
        .map(Some)
        .ok_or_else(|| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))
}

fn validate_response_metadata<T>(response: &ResponseEnvelope<T>) -> Result<()> {
    validate_metadata(response.headers())
        .and_then(|()| validate_metadata(response.metadata()))
        .map_err(|status| {
            Error::from(Status::internal(format!(
                "invalid response metadata: {}",
                status.message()
            )))
        })
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_core::Stream;
    use futures_util::StreamExt;

    use bytes::Bytes;

    use crate::{
        Code, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcKind, RpcRequest, Status,
    };

    use std::future::IntoFuture as _;

    use super::{
        BidirectionalCall, CallOptions, ResponseStream, RpcTransport, StreamingRpcTransport,
        bidirectional_streaming, client_streaming, read_unary_response_from_stream,
        request_channel, server_streaming, unary,
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[test]
    fn call_options_normalize_metadata_keys() {
        let options = CallOptions::new().with_metadata("Authorization", b"ok".to_vec());

        assert_eq!(
            options.metadata().get("authorization").map(Vec::as_slice),
            Some(&b"ok"[..])
        );
        assert!(!options.metadata().contains_key("Authorization"));
    }

    #[tokio::test]
    async fn client_streaming_response_reads_one_message_and_status() {
        let message = TestMessage {
            value: "hello".to_owned(),
        };
        let response = crate::stream::from_iter([
            ResponseFrame::message(prost::Message::encode_to_vec(&message)),
            ResponseFrame::Status(Status::ok()),
        ]);

        let decoded = read_unary_response_from_stream::<TestMessage>(
            response,
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        )
        .await
        .expect("response should decode");

        assert_eq!(decoded, message);
    }

    #[tokio::test]
    async fn request_sender_finish_gracefully_drains_accepted_messages() {
        let (sender, mut requests, _end) = request_channel(None);
        let message = TestMessage {
            value: "accepted".to_owned(),
        };
        sender
            .send(message.clone())
            .await
            .expect("request should be accepted");
        sender.finish().expect("request sender should finish");

        assert_eq!(
            requests.next().await.transpose().unwrap(),
            Some(prost::Message::encode_to_vec(&message))
        );
        assert!(requests.next().await.is_none());
    }

    #[tokio::test]
    async fn dropping_unfinished_request_sender_emits_cancellation() {
        let (sender, mut requests, _end) = request_channel::<TestMessage>(None);
        drop(sender);

        let error = requests
            .next()
            .await
            .expect("cancellation should be emitted")
            .expect_err("unfinished drop should cancel the request stream");
        assert_eq!(error.into_status().code(), Code::Cancelled);
        assert!(requests.next().await.is_none());
    }

    #[tokio::test]
    async fn request_send_future_cannot_outlive_graceful_finish() {
        let (sender, mut requests, _end) = request_channel::<TestMessage>(None);
        let send = sender.send(TestMessage::default());
        sender.finish().expect("request sender should finish");

        assert!(
            tokio::time::timeout(Duration::from_secs(1), requests.next())
                .await
                .expect("request stream should close promptly")
                .is_none(),
            "an unpolled send future must not keep the request stream open"
        );
        let error = send
            .await
            .expect_err("a send first polled after finish must be rejected");
        assert_eq!(error.into_status().code(), Code::Cancelled);
    }

    #[tokio::test]
    async fn bidirectional_halves_move_to_independent_tasks() {
        let (sender, _requests, _end) = request_channel::<TestMessage>(None);
        let response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([ResponseFrame::Status(Status::ok())]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );
        let call = BidirectionalCall::new(sender, response);
        let (sender, mut response) = call.split();

        let sender_task = tokio::spawn(async move { sender.finish() });
        let response_task = tokio::spawn(async move {
            assert!(response.next().await.is_none());
            response.terminal_status().map(Status::code)
        });

        sender_task.await.unwrap().unwrap();
        assert_eq!(response_task.await.unwrap(), Some(Code::Ok));
    }

    /// A request and the message that travelled beside it.
    type RecordedCall = (RpcRequest, Vec<u8>);

    #[derive(Clone, Default)]
    struct RecordingTransport {
        request: Arc<Mutex<Option<RecordedCall>>>,
    }

    #[crate::async_trait]
    impl RpcTransport for RecordingTransport {
        async fn call(
            &self,
            request: RpcRequest,
            body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            *self
                .request
                .lock()
                .expect("request lock should not be poisoned") = Some((request, body));
            Ok(ResponseEnvelope::new(Bytes::from(
                prost::Message::encode_to_vec(&TestMessage {
                    value: "response".to_owned(),
                }),
            )))
        }
    }

    #[derive(Clone)]
    struct StaticResponseTransport {
        response: ResponseEnvelope<Bytes>,
    }

    #[crate::async_trait]
    impl RpcTransport for StaticResponseTransport {
        async fn call(
            &self,
            _request: RpcRequest,
            _body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            Ok(self.response.clone())
        }
    }

    #[tokio::test]
    async fn unary_calls_propagate_deadlines() {
        let transport = RecordingTransport::default();
        let request = TestMessage {
            value: "request".to_owned(),
        };

        let response = unary::<_, _, TestMessage>(
            &transport,
            "/example.Greeter/SayHello",
            &request,
            CallOptions::new().with_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("unary call should succeed");

        assert_eq!(response.value, "response");
        let (recorded, body) = transport
            .request
            .lock()
            .expect("request lock should not be poisoned")
            .clone()
            .expect("transport should receive request");
        assert_eq!(
            body,
            prost::Message::encode_to_vec(&request),
            "the request message travels beside the request"
        );
        assert_eq!(recorded.method, "/example.Greeter/SayHello");
        assert_eq!(recorded.timeout_nanos, 5_000_000_000);
    }

    #[tokio::test]
    async fn unary_response_body_limit_boundary_is_stable() {
        let body = prost::Message::encode_to_vec(&TestMessage {
            value: "response".to_owned(),
        });
        let transport = StaticResponseTransport {
            response: ResponseEnvelope::new(Bytes::from(body.clone())),
        };
        let request = TestMessage {
            value: "request".to_owned(),
        };

        let decoded = unary::<_, _, TestMessage>(
            &transport,
            "/example.Greeter/SayHello",
            &request,
            CallOptions::new().with_max_response_body_size(body.len()),
        )
        .await
        .expect("exact response body limit should pass");
        assert_eq!(decoded.value, "response");

        let error = unary::<_, _, TestMessage>(
            &transport,
            "/example.Greeter/SayHello",
            &request,
            CallOptions::new().with_max_response_body_size(body.len() - 1),
        )
        .await
        .expect_err("one byte over response body limit should fail");

        assert!(matches!(
            error,
            Error::FrameTooLarge { len, max } if len == body.len() && max == body.len() - 1
        ));
    }

    #[derive(Clone, Default)]
    struct PendingTransport;

    #[crate::async_trait]
    impl RpcTransport for PendingTransport {
        async fn call(
            &self,
            _request: RpcRequest,
            _body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            std::future::pending().await
        }
    }

    #[crate::async_trait]
    impl StreamingRpcTransport for PendingTransport {
        async fn streaming_call(
            &self,
            _request: RpcRequest,
            _request_body: crate::BoxStream<Vec<u8>>,
        ) -> Result<crate::BoxStream<ResponseFrame>> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unary_deadline_cancels_pending_transport() {
        let error = unary::<_, _, TestMessage>(
            &PendingTransport,
            "/example.Greeter/SayHello",
            &TestMessage {
                value: "request".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .await
        .expect_err("pending unary should hit deadline");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn server_streaming_deadline_cancels_pending_open() {
        let Err(error) = server_streaming::<_, _, TestMessage>(
            &PendingTransport,
            "/example.Greeter/LotsOfReplies",
            &TestMessage {
                value: "request".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .await
        else {
            panic!("pending stream open should hit deadline");
        };

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
    }

    #[derive(Clone, Default)]
    struct UploadWaitingTransport;

    #[crate::async_trait]
    impl RpcTransport for UploadWaitingTransport {
        async fn call(
            &self,
            _request: RpcRequest,
            _body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            Err(crate::Error::from(Status::unimplemented(
                "unary not implemented",
            )))
        }
    }

    #[crate::async_trait]
    impl StreamingRpcTransport for UploadWaitingTransport {
        async fn streaming_call(
            &self,
            _request: RpcRequest,
            request_body: crate::BoxStream<Vec<u8>>,
        ) -> Result<crate::BoxStream<ResponseFrame>> {
            Ok(Box::pin(request_body.map(|result| {
                result.map(|_| ResponseFrame::Status(Status::internal("unexpected request body")))
            })))
        }
    }

    struct DropTrackedPendingStream {
        dropped: Arc<AtomicBool>,
    }

    impl Stream for DropTrackedPendingStream {
        type Item = Result<TestMessage>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for DropTrackedPendingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn client_streaming_deadline_drops_pending_upload() {
        let dropped = Arc::new(AtomicBool::new(false));
        let call = client_streaming::<_, TestMessage, TestMessage>(
            &UploadWaitingTransport,
            "/example.Greeter/LotsOfGreetings",
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .send_all(Box::pin(DropTrackedPendingStream {
            dropped: Arc::clone(&dropped),
        }))
        .into_future();
        tokio::pin!(call);
        assert!(
            futures_util::poll!(&mut call).is_pending(),
            "client-streaming call should wait for the pending upload"
        );

        tokio::time::advance(Duration::from_millis(2)).await;
        let error = call.await.expect_err("pending upload should hit deadline");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[derive(Clone, Default)]
    struct CollectingStreamingTransport;

    #[crate::async_trait]
    impl RpcTransport for CollectingStreamingTransport {
        async fn call(
            &self,
            _request: RpcRequest,
            _body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            Err(crate::Error::from(Status::unimplemented(
                "unary not implemented",
            )))
        }
    }

    #[crate::async_trait]
    impl StreamingRpcTransport for CollectingStreamingTransport {
        async fn streaming_call(
            &self,
            request: RpcRequest,
            mut request_body: crate::BoxStream<Vec<u8>>,
        ) -> Result<crate::BoxStream<ResponseFrame>> {
            let mut responses = Vec::new();

            while let Some(body) = request_body.next().await {
                let body = body?;
                let message = <TestMessage as prost::Message>::decode(body.as_slice())?;
                responses.push(ResponseFrame::message(prost::Message::encode_to_vec(
                    &message,
                )));
            }

            if request.rpc_kind() == RpcKind::ClientStreaming {
                let response = TestMessage {
                    value: responses.len().to_string(),
                };
                responses = vec![ResponseFrame::message(prost::Message::encode_to_vec(
                    &response,
                ))];
            }

            responses.push(ResponseFrame::Status(Status::ok()));
            Ok(crate::stream::from_iter(responses))
        }
    }

    #[tokio::test]
    async fn client_streaming_from_stream_sends_all_requests() {
        let response = client_streaming::<_, TestMessage, TestMessage>(
            &CollectingStreamingTransport,
            "/example.Greeter/LotsOfGreetings",
            CallOptions::new(),
        )
        .send_all(crate::stream::from_iter([
            TestMessage {
                value: "one".to_owned(),
            },
            TestMessage {
                value: "two".to_owned(),
            },
        ]))
        .await
        .expect("client-streaming call should succeed");

        assert_eq!(response.value, "2");
    }

    #[tokio::test]
    async fn streaming_transport_capability_is_object_safe() {
        let concrete = CollectingStreamingTransport;
        let transport: &dyn StreamingRpcTransport = &concrete;
        let response = client_streaming::<_, TestMessage, TestMessage>(
            transport,
            "/example.Greeter/LotsOfGreetings",
            CallOptions::new(),
        )
        .send_all(crate::stream::from_iter([TestMessage {
            value: "one".to_owned(),
        }]))
        .await
        .expect("trait-object transport should be callable");
        assert_eq!(response.value, "1");

        let transport: Arc<dyn StreamingRpcTransport> = Arc::new(CollectingStreamingTransport);
        let response = client_streaming::<_, TestMessage, TestMessage>(
            &transport,
            "/example.Greeter/LotsOfGreetings",
            CallOptions::new(),
        )
        .send_all(crate::stream::from_iter([TestMessage {
            value: "two".to_owned(),
        }]))
        .await
        .expect("shared trait-object transport should be callable");
        assert_eq!(response.value, "1");
    }

    #[tokio::test]
    async fn bidirectional_streaming_from_stream_sends_all_requests() {
        let mut response = bidirectional_streaming::<_, TestMessage, TestMessage>(
            &CollectingStreamingTransport,
            "/example.Greeter/BidiHello",
            CallOptions::new(),
        )
        .send_all(crate::stream::from_iter([
            TestMessage {
                value: "one".to_owned(),
            },
            TestMessage {
                value: "two".to_owned(),
            },
        ]))
        .await
        .expect("bidi-streaming call should open");

        let mut values = Vec::new();
        while let Some(message) = response.next().await {
            values.push(message.expect("response should decode").value);
        }

        assert_eq!(values, ["one", "two"]);
    }

    #[tokio::test(start_paused = true)]
    async fn bidirectional_streaming_deadline_cancels_pending_response_read() {
        let mut responses = bidirectional_streaming::<_, TestMessage, TestMessage>(
            &UploadWaitingTransport,
            "/example.Greeter/BidiHello",
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .await
        .expect("stream should open");
        let response = responses.recv();
        tokio::pin!(response);
        assert!(
            futures_util::poll!(&mut response).is_pending(),
            "response read should be pending before the deadline"
        );

        tokio::time::advance(Duration::from_millis(2)).await;
        let error = response
            .await
            .expect_err("pending response should hit deadline");
        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn response_message_limit_returns_resource_exhausted() {
        let message = TestMessage {
            value: "hello".to_owned(),
        };
        let mut response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([
                ResponseFrame::message(prost::Message::encode_to_vec(&message)),
                ResponseFrame::message(prost::Message::encode_to_vec(&message)),
                ResponseFrame::Status(Status::ok()),
            ]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(1),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );

        assert_eq!(
            response
                .next()
                .await
                .expect("first response should exist")
                .expect("first response should decode"),
            message
        );
        let error = response
            .next()
            .await
            .expect("limit error should be emitted")
            .expect_err("second response should exceed message limit");

        assert_eq!(error.into_status().code(), Code::ResourceExhausted);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_body_limit_returns_resource_exhausted() {
        let first = TestMessage {
            value: "one".to_owned(),
        };
        let second = TestMessage {
            value: "two".to_owned(),
        };
        let mut response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([
                ResponseFrame::message(prost::Message::encode_to_vec(&first)),
                ResponseFrame::message(prost::Message::encode_to_vec(&second)),
                ResponseFrame::Status(Status::ok()),
            ]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(prost::Message::encoded_len(&first)),
            Some(Duration::from_secs(30)),
            None,
        );

        assert_eq!(
            response
                .next()
                .await
                .expect("first response should exist")
                .expect("first response should decode"),
            first
        );
        let error = response
            .next()
            .await
            .expect("limit error should be emitted")
            .expect_err("second response should exceed byte limit");

        assert_eq!(error.into_status().code(), Code::ResourceExhausted);
        assert!(response.next().await.is_none());
    }

    fn response_stream_of(
        frames: impl IntoIterator<Item = ResponseFrame, IntoIter: Send + Unpin + 'static>,
    ) -> ResponseStream<TestMessage> {
        ResponseStream::<TestMessage>::new(
            crate::stream::from_iter(frames),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        )
    }

    fn test_message(value: &str) -> ResponseFrame {
        ResponseFrame::message(prost::Message::encode_to_vec(&TestMessage {
            value: value.to_owned(),
        }))
    }

    fn header_map(key: &str, value: &[u8]) -> Metadata {
        Metadata::from([(key.to_owned(), value.to_vec())])
    }

    #[tokio::test]
    async fn second_headers_frame_is_an_internal_error() {
        let mut response = response_stream_of([
            ResponseFrame::Headers(header_map("x-a", b"1")),
            ResponseFrame::Headers(header_map("x-a", b"2")),
            ResponseFrame::Status(Status::ok()),
        ]);

        let error = response
            .next()
            .await
            .expect("the protocol error should be emitted")
            .expect_err("a second HEADERS must be rejected");

        assert_eq!(error.into_status().code(), Code::Internal);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn headers_after_a_message_are_an_internal_error() {
        let mut response = response_stream_of([
            test_message("one"),
            ResponseFrame::Headers(Metadata::new()),
            ResponseFrame::Status(Status::ok()),
        ]);

        assert_eq!(response.next().await.unwrap().unwrap().value, "one");
        let error = response
            .next()
            .await
            .expect("the protocol error should be emitted")
            .expect_err("HEADERS after MESSAGE must be rejected");

        assert_eq!(error.into_status().code(), Code::Internal);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn headers_arrive_before_any_message() {
        let mut response = response_stream_of([
            ResponseFrame::Headers(header_map("x-a", b"1")),
            test_message("one"),
            ResponseFrame::Status(Status::ok()),
        ]);

        assert_eq!(
            response.headers().await.expect("headers should resolve"),
            &header_map("x-a", b"1")
        );
        assert_eq!(response.next().await.unwrap().unwrap().value, "one");
        assert!(response.next().await.is_none());
        // Resolved headers stay available.
        assert_eq!(
            response.headers().await.expect("headers should resolve"),
            &header_map("x-a", b"1")
        );
    }

    #[tokio::test]
    async fn absent_headers_resolve_empty_at_the_first_message_without_losing_it() {
        let mut response = response_stream_of([
            test_message("one"),
            test_message("two"),
            ResponseFrame::Status(Status::ok()),
        ]);

        assert!(response.headers().await.expect("headers").is_empty());
        assert_eq!(response.next().await.unwrap().unwrap().value, "one");
        assert_eq!(response.next().await.unwrap().unwrap().value, "two");
        assert!(response.next().await.is_none());
        assert_eq!(response.terminal_status().map(Status::code), Some(Code::Ok));
    }

    #[tokio::test]
    async fn absent_headers_resolve_empty_at_a_lone_status() {
        let mut ok = response_stream_of([ResponseFrame::Status(Status::ok())]);
        assert!(ok.headers().await.expect("headers").is_empty());
        assert!(ok.next().await.is_none());

        let mut failed =
            response_stream_of([ResponseFrame::Status(Status::new(Code::NotFound, "gone"))]);
        let error = failed
            .headers()
            .await
            .expect_err("a failed call reports its error through headers");
        assert_eq!(error.into_status().code(), Code::NotFound);
        // The stream reports the same error.
        let error = failed.next().await.unwrap().expect_err("call failed");
        assert_eq!(error.into_status().code(), Code::NotFound);
        assert!(failed.next().await.is_none());
    }

    #[tokio::test]
    async fn headers_resolve_before_the_first_streamed_message_arrives() {
        let (frames, receiver) = tokio::sync::mpsc::unbounded_channel();
        let mut response = ResponseStream::<TestMessage>::new(
            Box::pin(futures_util::stream::unfold(
                receiver,
                |mut receiver| async move { receiver.recv().await.map(|frame| (Ok(frame), receiver)) },
            )),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            None,
            None,
        );

        frames
            .send(ResponseFrame::Headers(header_map("x-a", b"1")))
            .expect("receiver is alive");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), response.headers())
                .await
                .expect("headers must not wait for a message")
                .expect("headers should resolve"),
            &header_map("x-a", b"1")
        );

        frames
            .send(test_message("late"))
            .expect("receiver is alive");
        frames
            .send(ResponseFrame::Status(Status::ok()))
            .expect("receiver is alive");
        drop(frames);
        assert_eq!(response.next().await.unwrap().unwrap().value, "late");
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn headers_report_a_stream_failure_before_any_frame() {
        let mut response = ResponseStream::<TestMessage>::new(
            error_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            None,
            None,
        );

        let error = response.headers().await.expect_err("the stream failed");
        assert_eq!(error.into_status().code(), Code::InvalidArgument);
        let error = response.next().await.unwrap().expect_err("same failure");
        assert_eq!(error.into_status().code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn invalid_response_header_metadata_is_internal() {
        let mut response = response_stream_of([
            ResponseFrame::Headers(header_map("Bad Key", b"1")),
            ResponseFrame::Status(Status::ok()),
        ]);

        let error = response.headers().await.expect_err("metadata is invalid");
        assert_eq!(error.into_status().code(), Code::Internal);
    }

    #[tokio::test]
    async fn bidirectional_call_delegates_headers() {
        let (sender, _requests, _end) = request_channel::<TestMessage>(None);
        let mut call = BidirectionalCall::new(
            sender,
            response_stream_of([
                ResponseFrame::Headers(header_map("x-a", b"1")),
                ResponseFrame::Status(Status::ok()),
            ]),
        );

        assert_eq!(
            call.headers().await.expect("headers"),
            &header_map("x-a", b"1")
        );
        assert!(call.recv().await.expect("clean end").is_none());
    }

    #[tokio::test]
    async fn client_streaming_call_delegates_headers_and_fills_the_envelope() {
        let (sender, _requests, _end) = request_channel::<TestMessage>(None);
        let trailers = header_map("x-t", b"2");
        let mut call = super::ClientStreamingCall::new(
            sender,
            response_stream_of([
                ResponseFrame::Headers(header_map("x-a", b"1")),
                test_message("done"),
                ResponseFrame::Status(Status::ok().with_metadata(trailers.clone())),
            ]),
        );

        assert_eq!(
            call.headers().await.expect("headers"),
            &header_map("x-a", b"1")
        );
        let envelope = call
            .close_and_recv_with_response()
            .await
            .expect("call should complete");
        assert_eq!(envelope.message().value, "done");
        assert_eq!(envelope.headers(), &header_map("x-a", b"1"));
        assert_eq!(envelope.metadata(), &trailers);
    }

    #[tokio::test]
    async fn unary_envelope_returns_headers_and_trailing_metadata() {
        let transport = StaticResponseTransport {
            response: ResponseEnvelope::new(Bytes::from(prost::Message::encode_to_vec(
                &TestMessage {
                    value: "response".to_owned(),
                },
            )))
            .with_headers(header_map("x-a", b"1"))
            .with_metadata(header_map("x-t", b"2")),
        };

        let envelope = unary::<_, _, TestMessage>(
            &transport,
            "/example.Greeter/SayHello",
            &TestMessage::default(),
            CallOptions::new(),
        )
        .with_response()
        .await
        .expect("unary call should succeed");

        assert_eq!(envelope.message().value, "response");
        assert_eq!(envelope.headers(), &header_map("x-a", b"1"));
        assert_eq!(envelope.metadata(), &header_map("x-t", b"2"));
    }

    #[tokio::test]
    async fn unary_envelope_rejects_invalid_header_metadata() {
        let transport = StaticResponseTransport {
            response: ResponseEnvelope::new(Bytes::new()).with_headers(header_map("Bad Key", b"1")),
        };

        let error = unary::<_, _, TestMessage>(
            &transport,
            "/example.Greeter/SayHello",
            &TestMessage::default(),
            CallOptions::new(),
        )
        .with_response()
        .await
        .expect_err("invalid headers must be rejected");

        assert_eq!(error.into_status().code(), Code::Internal);
    }

    #[tokio::test]
    async fn collect_unary_response_requires_exactly_one_message_and_a_clean_end() {
        let collect = |frames: Vec<ResponseFrame>| {
            super::collect_unary_response(crate::stream::from_iter(frames))
        };

        let envelope = collect(vec![
            ResponseFrame::Headers(header_map("x-a", b"1")),
            ResponseFrame::message(&b"m"[..]),
            ResponseFrame::Status(Status::ok().with_metadata(header_map("x-t", b"2"))),
        ])
        .await
        .expect("unary response should collect");
        assert_eq!(envelope.message(), &Bytes::from_static(b"m"));
        assert_eq!(envelope.headers(), &header_map("x-a", b"1"));
        assert_eq!(envelope.metadata(), &header_map("x-t", b"2"));

        for frames in [
            vec![ResponseFrame::Status(Status::ok())],
            vec![
                ResponseFrame::message(&b"1"[..]),
                ResponseFrame::message(&b"2"[..]),
                ResponseFrame::Status(Status::ok()),
            ],
            vec![ResponseFrame::message(&b"1"[..])],
            vec![
                ResponseFrame::message(&b"1"[..]),
                ResponseFrame::Status(Status::ok()),
                ResponseFrame::message(&b"2"[..]),
            ],
        ] {
            let error = collect(frames)
                .await
                .expect_err("a malformed unary response must fail");
            assert_eq!(error.into_status().code(), Code::Internal);
        }

        let error = collect(vec![ResponseFrame::Status(Status::new(
            Code::NotFound,
            "gone",
        ))])
        .await
        .expect_err("a failed call is an error");
        assert_eq!(error.into_status().code(), Code::NotFound);
    }

    fn pending_frame_stream() -> crate::BoxStream<ResponseFrame> {
        Box::pin(futures_util::stream::pending())
    }

    fn error_frame_stream() -> crate::BoxStream<ResponseFrame> {
        Box::pin(futures_util::stream::iter([Err(Error::from(
            Status::invalid_argument("response read failed"),
        ))]))
    }

    fn terminal_then_pending_frame_stream() -> crate::BoxStream<ResponseFrame> {
        Box::pin(
            crate::stream::from_iter([ResponseFrame::Status(Status::ok())])
                .chain(futures_util::stream::pending()),
        )
    }

    fn terminal_then_decode_error_frame_stream() -> crate::BoxStream<ResponseFrame> {
        let error = <TestMessage as prost::Message>::decode(&[0x0a, 0xff][..])
            .expect_err("test payload should be malformed");
        Box::pin(
            crate::stream::from_iter([ResponseFrame::Status(Status::ok())])
                .chain(futures_util::stream::iter([Err(Error::from(error))])),
        )
    }

    #[tokio::test]
    async fn response_stream_idle_timeout_returns_unavailable() {
        let mut response = ResponseStream::<TestMessage>::new(
            pending_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_millis(1)),
            None,
        );

        let error = response
            .next()
            .await
            .expect("timeout error should be emitted")
            .expect_err("pending response should hit idle timeout");

        assert_eq!(error.into_status().code(), Code::Unavailable);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_commits_terminal_metadata_only_after_clean_eof() {
        let mut metadata = crate::Metadata::new();
        metadata.insert("x-terminal".to_owned(), b"complete".to_vec());
        let mut response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([ResponseFrame::Status(
                Status::ok().with_metadata(metadata.clone()),
            )]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );

        assert!(response.terminal_metadata().is_none());
        assert!(response.next().await.is_none());
        assert_eq!(response.terminal_status().map(Status::code), Some(Code::Ok));
        assert_eq!(response.terminal_metadata(), Some(&metadata));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_commits_remote_error_only_after_clean_eof() {
        let mut metadata = crate::Metadata::new();
        metadata.insert("x-error".to_owned(), b"denied".to_vec());
        let mut response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([ResponseFrame::Status(
                Status::new(Code::PermissionDenied, "denied").with_metadata(metadata.clone()),
            )]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );

        assert!(response.terminal_status().is_none());
        let error = response
            .next()
            .await
            .expect("remote status should be emitted")
            .expect_err("non-OK status should fail");

        assert_eq!(error.into_status().code(), Code::PermissionDenied);
        assert_eq!(
            response.terminal_status().map(Status::code),
            Some(Code::PermissionDenied)
        );
        assert_eq!(response.terminal_metadata(), Some(&metadata));
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn cancelled_post_terminal_read_does_not_commit_terminal_status() {
        let mut response = ResponseStream::<TestMessage>::new(
            terminal_then_pending_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            None,
            None,
        );

        let result = tokio::time::timeout(Duration::from_millis(1), response.next()).await;

        assert!(result.is_err());
        assert!(response.terminal_status().is_none());
        assert!(
            tokio::time::timeout(Duration::from_millis(1), response.next())
                .await
                .is_err()
        );
        assert!(response.terminal_status().is_none());
    }

    #[tokio::test]
    async fn malformed_post_terminal_read_is_classified_as_trailing_data() {
        let mut response = ResponseStream::<TestMessage>::new(
            terminal_then_decode_error_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );

        let error = response
            .next()
            .await
            .expect("trailing error should be emitted")
            .expect_err("malformed trailing data should fail")
            .into_status();

        assert_eq!(error.code(), Code::Internal);
        assert_eq!(
            error.message(),
            "response stream contained malformed trailing data after terminal status"
        );
        assert!(response.terminal_status().is_none());
        assert!(response.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn response_stream_deadline_returns_deadline_exceeded() {
        let mut response = ResponseStream::<TestMessage>::new(
            pending_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            Some(tokio::time::Instant::now() + Duration::from_millis(1)),
        );

        let error = response
            .next()
            .await
            .expect("timeout error should be emitted")
            .expect_err("pending response should hit deadline");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
        assert!(response.next().await.is_none());
    }

    #[tokio::test]
    async fn response_stream_deadline_wins_over_ready_response_error() {
        let mut response = ResponseStream::<TestMessage>::new(
            error_frame_stream(),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            Some(
                tokio::time::Instant::now()
                    .checked_sub(Duration::from_millis(1))
                    .expect("test deadline should be representable"),
            ),
        );

        let error = response
            .next()
            .await
            .expect("deadline error should be emitted")
            .expect_err("expired deadline should win over response error");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
        assert!(response.next().await.is_none());
    }
}
