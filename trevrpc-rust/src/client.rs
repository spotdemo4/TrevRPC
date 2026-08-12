use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_core::Stream;
use futures_util::StreamExt;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::response_state::{ResponseState, ResponseStateEvent};
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{
    BoxStream, Code, Error, Metadata, ResponseEnvelope, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, Status,
};
use prost::Message;
use tokio::sync::mpsc;
use tokio::time::Instant;

#[cfg(feature = "quinn")]
pub(crate) mod channel;

#[cfg(feature = "quinn")]
pub use channel::{Channel, ChannelConfig, ChannelEvent, ChannelPhase, ChannelState};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallOptions {
    timeout: Option<Duration>,
    max_response_body_size: usize,
    max_response_messages: Option<usize>,
    max_response_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    metadata: Metadata,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            max_response_body_size: DEFAULT_MAX_FRAME_SIZE,
            max_response_messages: Some(4096),
            max_response_stream_body_size: Some(16 * 1024 * 1024),
            stream_idle_timeout: Some(Duration::from_secs(30)),
            metadata: Metadata::new(),
        }
    }
}

impl CallOptions {
    /// Creates call options with the default timeout, size, stream, and metadata settings.
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
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
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
    /// Sends a unary `TrevRPC` request and returns its response.
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse>;
}

/// A transport with explicit support for streaming RPCs.
#[crate::async_trait]
pub trait StreamingRpcTransport: RpcTransport {
    /// Sends a streaming `TrevRPC` request and returns response stream frames.
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>>;
}

#[crate::async_trait]
impl<T> RpcTransport for Arc<T>
where
    T: RpcTransport + ?Sized,
{
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        self.as_ref().call(request).await
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
    ) -> Result<BoxStream<RpcStreamFrame>> {
        self.as_ref().streaming_call(request, request_body).await
    }
}

/// Calls a unary RPC and decodes the protobuf response.
pub async fn unary<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<Res>
where
    T: RpcTransport + ?Sized,
    Req: Message,
    Res: Message + Default,
{
    Ok(unary_envelope(transport, service, method, request, options)
        .await?
        .into_parts()
        .0)
}

/// Calls a unary RPC and returns the decoded response envelope.
pub async fn unary_envelope<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<ResponseEnvelope<Res>>
where
    T: RpcTransport + ?Sized,
    Req: Message,
    Res: Message + Default,
{
    let CallOptions {
        timeout,
        max_response_body_size,
        max_response_messages: _,
        max_response_stream_body_size: _,
        stream_idle_timeout: _,
        metadata,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;
    let timeout_nanos = timeout_nanos(timeout)?;

    let request = RpcRequest::new(service, method, request.encode_to_vec())
        .with_metadata(metadata)
        .with_timeout_nanos(timeout_nanos);
    let response = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, transport.call(request))
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))??
    } else {
        transport.call(request).await?
    };

    validate_response_metadata(&response)?;

    if response.body.len() > max_response_body_size {
        return Err(Error::FrameTooLarge {
            len: response.body.len(),
            max: max_response_body_size,
        });
    }

    let status = Status::from_response(&response);
    if !status.is_ok() {
        return Err(Error::from(status));
    }

    let message = Res::decode(response.body.as_slice()).map_err(Error::from)?;
    Ok(ResponseEnvelope::new(message).with_metadata(response.metadata))
}

/// Calls a server-streaming RPC and returns a terminal-aware response stream.
pub async fn server_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<ResponseStream<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(
        service,
        method,
        RpcKind::ServerStreaming,
        request.encode_to_vec(),
        options,
    )?;
    open_response_stream(transport, prepared, crate::stream::empty()).await
}

/// Starts a client-streaming RPC and returns a sendable call object.
pub async fn client_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    options: CallOptions,
) -> Result<ClientStreamingCall<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(
        service,
        method,
        RpcKind::ClientStreaming,
        Vec::new(),
        options,
    )?;
    let (sender, requests) = request_channel();
    let response =
        open_response_stream(transport, prepared, crate::stream::encode(requests)).await?;

    Ok(ClientStreamingCall::new(sender, response))
}

/// Calls a client-streaming RPC with a caller-provided request stream.
pub async fn client_streaming_from_stream<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    requests: BoxStream<Req>,
    options: CallOptions,
) -> Result<Res>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    Ok(
        client_streaming_from_stream_envelope(transport, service, method, requests, options)
            .await?
            .into_parts()
            .0,
    )
}

/// Calls a client-streaming RPC and preserves successful terminal metadata.
pub async fn client_streaming_from_stream_envelope<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    requests: BoxStream<Req>,
    options: CallOptions,
) -> Result<ResponseEnvelope<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(
        service,
        method,
        RpcKind::ClientStreaming,
        Vec::new(),
        options,
    )?;
    let mut response =
        open_response_stream(transport, prepared, crate::stream::encode(requests)).await?;

    read_unary_response_envelope_from_message_stream(&mut response).await
}

/// Starts a bidirectional-streaming RPC and returns a splittable call object.
pub async fn bidirectional_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    options: CallOptions,
) -> Result<BidirectionalCall<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(
        service,
        method,
        RpcKind::BidirectionalStreaming,
        Vec::new(),
        options,
    )?;
    let (sender, requests) = request_channel();
    let response =
        open_response_stream(transport, prepared, crate::stream::encode(requests)).await?;

    Ok(BidirectionalCall::new(sender, response))
}

/// Starts a bidirectional-streaming RPC with a caller-provided request stream.
pub async fn bidirectional_streaming_from_stream<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    requests: BoxStream<Req>,
    options: CallOptions,
) -> Result<ResponseStream<Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let prepared = prepare_streaming_request(
        service,
        method,
        RpcKind::BidirectionalStreaming,
        Vec::new(),
        options,
    )?;
    open_response_stream(transport, prepared, crate::stream::encode(requests)).await
}

#[derive(Clone, Copy)]
enum RequestControl {
    Cancel,
}

struct RequestMessageStream<T> {
    receiver: mpsc::Receiver<T>,
    control: mpsc::UnboundedReceiver<RequestControl>,
    cancelled: bool,
}

impl<T> Stream for RequestMessageStream<T> {
    type Item = Result<T>;

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

fn request_channel<T>() -> (RequestSender<T>, BoxStream<T>)
where
    T: Send + 'static,
{
    let (sender, receiver) = mpsc::channel(1);
    let (control, control_rx) = mpsc::unbounded_channel();
    (
        RequestSender {
            state: Arc::new(RequestSenderState {
                sender: Mutex::new(Some(sender)),
            }),
            control,
            finished: false,
        },
        Box::pin(RequestMessageStream {
            receiver,
            control: control_rx,
            cancelled: false,
        }),
    )
}

struct RequestSenderState<T> {
    sender: Mutex<Option<mpsc::Sender<T>>>,
}

/// The independently movable request half of a streaming call.
pub struct RequestSender<T> {
    state: Arc<RequestSenderState<T>>,
    control: mpsc::UnboundedSender<RequestControl>,
    finished: bool,
}

impl<T> RequestSender<T>
where
    T: Send + 'static,
{
    /// Sends one request message with bounded backpressure.
    pub fn send(
        &self,
        request: T,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_request(Arc::clone(&self.state), request)
    }

    /// Gracefully finishes the request stream after all accepted messages.
    pub fn finish(mut self) -> Result<()> {
        self.finish_in_place();
        Ok(())
    }

    fn finish_in_place(&mut self) {
        self.finished = true;
        self.state
            .sender
            .lock()
            .expect("request sender lock poisoned")
            .take();
    }
}

async fn send_request<T>(state: Arc<RequestSenderState<T>>, request: T) -> Result<()>
where
    T: Send + 'static,
{
    let sender = state
        .sender
        .lock()
        .expect("request sender lock poisoned")
        .clone()
        .ok_or_else(|| Error::from(Status::cancelled("request stream is closed")))?;
    sender
        .send(request)
        .await
        .map_err(|_| Error::from(Status::cancelled("request stream is closed")))
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
    Req: Send + 'static,
    Res: Message + Default + Send + 'static,
{
    pub fn send(
        &self,
        request: Req,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_request(Arc::clone(&self.sender.state), request)
    }

    pub fn close_send(&mut self) -> Result<()> {
        self.sender.finish_in_place();
        Ok(())
    }

    pub async fn close_and_recv(mut self) -> Result<Res> {
        self.close_send()?;
        read_unary_response_from_message_stream(&mut self.response).await
    }

    pub async fn close_and_recv_envelope(mut self) -> Result<ResponseEnvelope<Res>> {
        self.close_send()?;
        read_unary_response_envelope_from_message_stream(&mut self.response).await
    }
}

/// A bidirectional call whose request and response halves can move independently.
pub struct BidirectionalCall<Req, Res> {
    sender: RequestSender<Req>,
    response: ResponseStream<Res>,
}

/// Compatibility alias for the pre-Milestone 5 bidirectional call name.
#[deprecated(since = "0.1.0", note = "use BidirectionalCall")]
pub type BidirectionalStreamingCall<Req, Res> = BidirectionalCall<Req, Res>;

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
    Req: Send + 'static,
    Res: Message + Default + Send + 'static,
{
    pub fn send(
        &self,
        request: Req,
    ) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
        send_request(Arc::clone(&self.sender.state), request)
    }

    pub async fn recv(&mut self) -> Result<Option<Res>> {
        self.response.next().await.transpose()
    }

    pub fn close_send(&mut self) -> Result<()> {
        self.sender.finish_in_place();
        Ok(())
    }

    #[must_use]
    pub fn terminal_status(&self) -> Option<&Status> {
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
}

fn prepare_streaming_request(
    service: &str,
    method: &str,
    kind: RpcKind,
    body: Vec<u8>,
    options: CallOptions,
) -> Result<PreparedStreamingCall> {
    let CallOptions {
        timeout,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        metadata,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;
    let timeout_nanos = timeout_nanos(timeout)?;

    Ok(PreparedStreamingCall {
        request: RpcRequest::new(service, method, body)
            .with_kind(kind)
            .with_metadata(metadata)
            .with_timeout_nanos(timeout_nanos),
        deadline: timeout.and_then(|timeout| Instant::now().checked_add(timeout)),
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
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
    deadline: Option<Instant>,
) -> Result<BoxStream<RpcStreamFrame>>
where
    T: StreamingRpcTransport + ?Sized,
{
    match remaining_timeout(deadline)? {
        Some(timeout) => {
            tokio::time::timeout(timeout, transport.streaming_call(request, request_body))
                .await
                .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))?
        }
        None => transport.streaming_call(request, request_body).await,
    }
}

async fn open_response_stream<T, Res>(
    transport: &T,
    prepared: PreparedStreamingCall,
    request_body: BoxStream<Vec<u8>>,
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
    } = prepared;
    let response = streaming_call_with_deadline(transport, request, request_body, deadline).await?;

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
    response: BoxStream<RpcStreamFrame>,
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

    if response_count != 1 {
        return Err(Error::from(Status::internal(format!(
            "client-streaming RPC returned {response_count} response messages; expected exactly one"
        ))));
    }

    Ok(
        ResponseEnvelope::new(first.expect("one response message should have been retained"))
            .with_metadata(
                response
                    .terminal_status()
                    .map_or_else(Metadata::new, |status| status.metadata().clone()),
            ),
    )
}

/// A decoded response stream that retains the clean terminal status and metadata.
pub struct ResponseStream<T> {
    inner: BoxStream<RpcStreamFrame>,
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
    state: ResponseState<T>,
}

impl<T> Unpin for ResponseStream<T> {}

impl<T> ResponseStream<T>
where
    T: Message + Default,
{
    pub(crate) fn new(
        inner: BoxStream<RpcStreamFrame>,
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
        }
    }

    /// Returns the terminal status after the status frame and clean FIN are consumed.
    #[must_use]
    pub fn terminal_status(&self) -> Option<&Status> {
        self.state.terminal_status()
    }

    /// Returns terminal metadata after the stream reaches clean EOF.
    #[must_use]
    pub fn terminal_metadata(&self) -> Option<&Metadata> {
        self.terminal_status().map(Status::metadata)
    }

    fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<RpcStreamFrame>>> {
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

    fn finish(&mut self) -> Poll<Option<Result<T>>> {
        self.done = true;
        Poll::Ready(
            self.state
                .finish()
                .err()
                .map(|failure| Err(Error::from(failure.into_status()))),
        )
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
        if self.done {
            return Poll::Ready(None);
        }

        loop {
            let frame = match self.poll_frame(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(Some(frame))) => frame,
                Poll::Ready(Ok(None)) => return self.finish(),
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
                self.done = true;
                self.state.abort();
                return Poll::Ready(Some(Err(Error::from(Status::internal(
                    "response stream continued after terminal status",
                )))));
            }

            if frame.frame_kind() == Some(crate::RpcStreamFrameKind::Message) {
                if let Some(max) = self.max_messages
                    && self.messages >= max
                {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::from(Status::resource_exhausted(
                        format!("response stream exceeded maximum of {max} messages"),
                    )))));
                }

                if frame.body.len() > self.max_body_size {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::FrameTooLarge {
                        len: frame.body.len(),
                        max: self.max_body_size,
                    })));
                }

                self.messages = self.messages.saturating_add(1);
                self.stream_body_size = self.stream_body_size.saturating_add(frame.body.len());

                if let Some(max) = self.max_stream_body_size
                    && self.stream_body_size > max
                {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::from(Status::resource_exhausted(
                        format!("response stream exceeded maximum body size of {max} bytes"),
                    )))));
                }
            }

            match self.state.accept(&frame) {
                Ok(ResponseStateEvent::Message(message)) => return Poll::Ready(Some(Ok(message))),
                Ok(ResponseStateEvent::Terminal) => self.pending_terminal = true,
                Err(failure) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(Error::from(failure.into_status()))));
                }
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

fn validate_response_metadata(response: &RpcResponse) -> Result<()> {
    validate_metadata(&response.metadata).map_err(|status| {
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

    use crate::{Code, Error, Result, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, Status};

    use super::{
        BidirectionalCall, CallOptions, ResponseStream, RpcTransport, StreamingRpcTransport,
        bidirectional_streaming, bidirectional_streaming_from_stream, client_streaming_from_stream,
        read_unary_response_from_stream, request_channel, server_streaming, unary,
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
            RpcStreamFrame::message(prost::Message::encode_to_vec(&message)),
            RpcStreamFrame::status(Status::ok()),
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
        let (sender, mut requests) = request_channel();
        sender.send(7_u8).await.expect("request should be accepted");
        sender.finish().expect("request sender should finish");

        assert_eq!(requests.next().await.transpose().unwrap(), Some(7));
        assert!(requests.next().await.is_none());
    }

    #[tokio::test]
    async fn dropping_unfinished_request_sender_emits_cancellation() {
        let (sender, mut requests) = request_channel::<u8>();
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
        let (sender, mut requests) = request_channel::<u8>();
        let send = sender.send(7);
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
        let (sender, _requests) = request_channel::<TestMessage>();
        let response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([RpcStreamFrame::status(Status::ok())]),
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

    #[derive(Clone, Default)]
    struct RecordingTransport {
        request: Arc<Mutex<Option<RpcRequest>>>,
    }

    #[crate::async_trait]
    impl RpcTransport for RecordingTransport {
        async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
            *self
                .request
                .lock()
                .expect("request lock should not be poisoned") = Some(request);
            Ok(RpcResponse::ok(prost::Message::encode_to_vec(
                &TestMessage {
                    value: "response".to_owned(),
                },
            )))
        }
    }

    #[derive(Clone)]
    struct StaticResponseTransport {
        response: RpcResponse,
    }

    #[crate::async_trait]
    impl RpcTransport for StaticResponseTransport {
        async fn call(&self, _request: RpcRequest) -> Result<RpcResponse> {
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
            "example.Greeter",
            "SayHello",
            &request,
            CallOptions::new().with_timeout(Duration::from_secs(5)),
        )
        .await
        .expect("unary call should succeed");

        assert_eq!(response.value, "response");
        let recorded = transport
            .request
            .lock()
            .expect("request lock should not be poisoned")
            .clone()
            .expect("transport should receive request");
        assert_eq!(recorded.service, "example.Greeter");
        assert_eq!(recorded.method, "SayHello");
        assert_eq!(recorded.timeout_nanos, 5_000_000_000);
    }

    #[tokio::test]
    async fn unary_response_body_limit_boundary_is_stable() {
        let body = prost::Message::encode_to_vec(&TestMessage {
            value: "response".to_owned(),
        });
        let transport = StaticResponseTransport {
            response: RpcResponse::ok(body.clone()),
        };
        let request = TestMessage {
            value: "request".to_owned(),
        };

        let decoded = unary::<_, _, TestMessage>(
            &transport,
            "example.Greeter",
            "SayHello",
            &request,
            CallOptions::new().with_max_response_body_size(body.len()),
        )
        .await
        .expect("exact response body limit should pass");
        assert_eq!(decoded.value, "response");

        let error = unary::<_, _, TestMessage>(
            &transport,
            "example.Greeter",
            "SayHello",
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
        async fn call(&self, _request: RpcRequest) -> Result<RpcResponse> {
            std::future::pending().await
        }
    }

    #[crate::async_trait]
    impl StreamingRpcTransport for PendingTransport {
        async fn streaming_call(
            &self,
            _request: RpcRequest,
            _request_body: crate::BoxStream<Vec<u8>>,
        ) -> Result<crate::BoxStream<RpcStreamFrame>> {
            std::future::pending().await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn unary_deadline_cancels_pending_transport() {
        let error = unary::<_, _, TestMessage>(
            &PendingTransport,
            "example.Greeter",
            "SayHello",
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
            "example.Greeter",
            "LotsOfReplies",
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
        async fn call(&self, _request: RpcRequest) -> Result<RpcResponse> {
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
        ) -> Result<crate::BoxStream<RpcStreamFrame>> {
            Ok(Box::pin(request_body.map(|result| {
                result.map(|_| RpcStreamFrame::status(Status::internal("unexpected request body")))
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
        let call = client_streaming_from_stream::<_, TestMessage, TestMessage>(
            &UploadWaitingTransport,
            "example.Greeter",
            "LotsOfGreetings",
            Box::pin(DropTrackedPendingStream {
                dropped: Arc::clone(&dropped),
            }),
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        );
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
        async fn call(&self, _request: RpcRequest) -> Result<RpcResponse> {
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
        ) -> Result<crate::BoxStream<RpcStreamFrame>> {
            let mut responses = Vec::new();

            while let Some(body) = request_body.next().await {
                let body = body?;
                let message = <TestMessage as prost::Message>::decode(body.as_slice())?;
                responses.push(RpcStreamFrame::message(prost::Message::encode_to_vec(
                    &message,
                )));
            }

            if request.rpc_kind() == RpcKind::ClientStreaming {
                let response = TestMessage {
                    value: responses.len().to_string(),
                };
                responses = vec![RpcStreamFrame::message(prost::Message::encode_to_vec(
                    &response,
                ))];
            }

            responses.push(RpcStreamFrame::status(Status::ok()));
            Ok(crate::stream::from_iter(responses))
        }
    }

    #[tokio::test]
    async fn client_streaming_from_stream_sends_all_requests() {
        let response = client_streaming_from_stream::<_, TestMessage, TestMessage>(
            &CollectingStreamingTransport,
            "example.Greeter",
            "LotsOfGreetings",
            crate::stream::from_iter([
                TestMessage {
                    value: "one".to_owned(),
                },
                TestMessage {
                    value: "two".to_owned(),
                },
            ]),
            CallOptions::new(),
        )
        .await
        .expect("client-streaming call should succeed");

        assert_eq!(response.value, "2");
    }

    #[tokio::test]
    async fn streaming_transport_capability_is_object_safe() {
        let concrete = CollectingStreamingTransport;
        let transport: &dyn StreamingRpcTransport = &concrete;
        let response = client_streaming_from_stream::<_, TestMessage, TestMessage>(
            transport,
            "example.Greeter",
            "LotsOfGreetings",
            crate::stream::from_iter([TestMessage {
                value: "one".to_owned(),
            }]),
            CallOptions::new(),
        )
        .await
        .expect("trait-object transport should be callable");
        assert_eq!(response.value, "1");

        let transport: Arc<dyn StreamingRpcTransport> = Arc::new(CollectingStreamingTransport);
        let response = client_streaming_from_stream::<_, TestMessage, TestMessage>(
            &transport,
            "example.Greeter",
            "LotsOfGreetings",
            crate::stream::from_iter([TestMessage {
                value: "two".to_owned(),
            }]),
            CallOptions::new(),
        )
        .await
        .expect("shared trait-object transport should be callable");
        assert_eq!(response.value, "1");
    }

    #[tokio::test]
    async fn bidirectional_streaming_from_stream_sends_all_requests() {
        let mut response = bidirectional_streaming_from_stream::<_, TestMessage, TestMessage>(
            &CollectingStreamingTransport,
            "example.Greeter",
            "BidiHello",
            crate::stream::from_iter([
                TestMessage {
                    value: "one".to_owned(),
                },
                TestMessage {
                    value: "two".to_owned(),
                },
            ]),
            CallOptions::new(),
        )
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
            "example.Greeter",
            "BidiHello",
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
                RpcStreamFrame::message(prost::Message::encode_to_vec(&message)),
                RpcStreamFrame::message(prost::Message::encode_to_vec(&message)),
                RpcStreamFrame::status(Status::ok()),
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
                RpcStreamFrame::message(prost::Message::encode_to_vec(&first)),
                RpcStreamFrame::message(prost::Message::encode_to_vec(&second)),
                RpcStreamFrame::status(Status::ok()),
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

    #[tokio::test]
    async fn unknown_response_frame_kind_returns_invalid_argument() {
        let mut response = ResponseStream::<TestMessage>::new(
            crate::stream::from_iter([RpcStreamFrame {
                kind: 99,
                status: Code::Ok.as_u32(),
                message: String::new(),
                body: Vec::new(),
                metadata: crate::Metadata::new(),
            }]),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            None,
        );

        let error = response
            .next()
            .await
            .expect("unknown frame kind should yield an error")
            .expect_err("unknown frame kind should be invalid");

        assert_eq!(error.into_status().code(), Code::InvalidArgument);
        assert!(response.next().await.is_none());
    }

    fn pending_frame_stream() -> crate::BoxStream<RpcStreamFrame> {
        Box::pin(futures_util::stream::pending())
    }

    fn error_frame_stream() -> crate::BoxStream<RpcStreamFrame> {
        Box::pin(futures_util::stream::iter([Err(Error::from(
            Status::invalid_argument("response read failed"),
        ))]))
    }

    fn terminal_then_pending_frame_stream() -> crate::BoxStream<RpcStreamFrame> {
        Box::pin(
            crate::stream::from_iter([RpcStreamFrame::status(Status::ok())])
                .chain(futures_util::stream::pending()),
        )
    }

    fn terminal_then_decode_error_frame_stream() -> crate::BoxStream<RpcStreamFrame> {
        let error = <TestMessage as prost::Message>::decode(&[0x0a, 0xff][..])
            .expect_err("test payload should be malformed");
        Box::pin(
            crate::stream::from_iter([RpcStreamFrame::status(Status::ok())])
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
            crate::stream::from_iter([RpcStreamFrame::status(
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
            crate::stream::from_iter([RpcStreamFrame::status(
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
