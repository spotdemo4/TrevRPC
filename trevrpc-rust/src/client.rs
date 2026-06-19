use std::marker::PhantomData;
use std::time::{Duration, Instant};

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::stream::MessageStream;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{
    BoxMessageStream, Error, Metadata, Result, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame,
    RpcStreamFrameKind, Status,
};
use prost::Message;

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
            max_response_stream_body_size: Some(64 * 1024 * 1024),
            stream_idle_timeout: Some(Duration::from_secs(30)),
            metadata: Metadata::new(),
        }
    }
}

impl CallOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    #[must_use]
    pub const fn max_response_body_size(&self) -> usize {
        self.max_response_body_size
    }

    #[must_use]
    pub const fn max_response_messages(&self) -> Option<usize> {
        self.max_response_messages
    }

    #[must_use]
    pub const fn max_response_stream_body_size(&self) -> Option<usize> {
        self.max_response_stream_body_size
    }

    #[must_use]
    pub const fn stream_idle_timeout(&self) -> Option<Duration> {
        self.stream_idle_timeout
    }

    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub const fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    #[must_use]
    pub const fn with_max_response_body_size(mut self, max_response_body_size: usize) -> Self {
        self.max_response_body_size = max_response_body_size;
        self
    }

    #[must_use]
    pub const fn with_max_response_messages(
        mut self,
        max_response_messages: Option<usize>,
    ) -> Self {
        self.max_response_messages = max_response_messages;
        self
    }

    #[must_use]
    pub const fn with_max_response_stream_body_size(
        mut self,
        max_response_stream_body_size: Option<usize>,
    ) -> Self {
        self.max_response_stream_body_size = max_response_stream_body_size;
        self
    }

    #[must_use]
    pub const fn with_stream_idle_timeout(mut self, stream_idle_timeout: Option<Duration>) -> Self {
        self.stream_idle_timeout = stream_idle_timeout;
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        let key = key.into();
        self.metadata
            .insert(normalize_metadata_key(&key), value.into());
        self
    }

    #[must_use]
    pub fn with_metadata_map(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[crate::async_trait]
pub trait RpcTransport: Clone + Send + Sync + 'static {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse>;

    async fn streaming_call(
        &self,
        _request: RpcRequest,
        _request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        Err(Error::from(Status::unimplemented(
            "transport does not support streaming RPCs",
        )))
    }
}

pub async fn unary<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<Res>
where
    T: RpcTransport,
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

    Res::decode(response.body.as_slice()).map_err(Error::from)
}

pub async fn server_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<BoxMessageStream<Res>>
where
    T: RpcTransport,
    Req: Message,
    Res: Message + Default + Send + 'static,
{
    let PreparedStreamingCall {
        request,
        deadline,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
    } = prepare_streaming_request(
        service,
        method,
        RpcKind::ServerStreaming,
        request.encode_to_vec(),
        options,
    )?;
    let response =
        streaming_call_with_deadline(transport, request, crate::stream::empty(), deadline).await?;

    Ok(Box::new(ResponseMessageStream::<Res>::new(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    )))
}

pub async fn client_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    requests: BoxMessageStream<Req>,
    options: CallOptions,
) -> Result<Res>
where
    T: RpcTransport,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let PreparedStreamingCall {
        request,
        deadline,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
    } = prepare_streaming_request(
        service,
        method,
        RpcKind::ClientStreaming,
        Vec::new(),
        options,
    )?;
    let response = streaming_call_with_deadline(
        transport,
        request,
        crate::stream::encode(requests),
        deadline,
    )
    .await?;

    read_unary_response_from_stream(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    )
    .await
}

pub async fn bidirectional_streaming<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    requests: BoxMessageStream<Req>,
    options: CallOptions,
) -> Result<BoxMessageStream<Res>>
where
    T: RpcTransport,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    let PreparedStreamingCall {
        request,
        deadline,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
    } = prepare_streaming_request(
        service,
        method,
        RpcKind::BidirectionalStreaming,
        Vec::new(),
        options,
    )?;
    let response = streaming_call_with_deadline(
        transport,
        request,
        crate::stream::encode(requests),
        deadline,
    )
    .await?;

    Ok(Box::new(ResponseMessageStream::<Res>::new(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    )))
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
    request_body: BoxMessageStream<Vec<u8>>,
    deadline: Option<Instant>,
) -> Result<BoxMessageStream<RpcStreamFrame>>
where
    T: RpcTransport,
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

async fn read_unary_response_from_stream<Res>(
    response: BoxMessageStream<RpcStreamFrame>,
    max_response_body_size: usize,
    max_response_messages: Option<usize>,
    max_response_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    deadline: Option<Instant>,
) -> Result<Res>
where
    Res: Message + Default + Send + 'static,
{
    let mut response = ResponseMessageStream::<Res>::new(
        response,
        max_response_body_size,
        max_response_messages,
        max_response_stream_body_size,
        stream_idle_timeout,
        deadline,
    );
    let Some(first) = response.next().await else {
        return Err(Error::from(Status::internal(
            "response stream ended without a response message",
        )));
    };
    let first = first?;

    match response.next().await {
        Some(Ok(_)) => Err(Error::from(Status::internal(
            "client-streaming RPC returned more than one response message",
        ))),
        Some(Err(error)) => Err(error),
        None => Ok(first),
    }
}

struct ResponseMessageStream<T> {
    inner: BoxMessageStream<RpcStreamFrame>,
    max_body_size: usize,
    max_messages: Option<usize>,
    max_stream_body_size: Option<usize>,
    stream_idle_timeout: Option<Duration>,
    messages: usize,
    stream_body_size: usize,
    deadline: Option<Instant>,
    done: bool,
    _marker: PhantomData<T>,
}

impl<T> ResponseMessageStream<T> {
    const fn new(
        inner: BoxMessageStream<RpcStreamFrame>,
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
            done: false,
            _marker: PhantomData,
        }
    }
}

#[crate::async_trait]
impl<T> crate::MessageStream<T> for ResponseMessageStream<T>
where
    T: Message + Default + Send + 'static,
{
    async fn next(&mut self) -> Option<Result<T>> {
        if self.done {
            return None;
        }

        let frame =
            match next_frame_with_timeout(&mut self.inner, self.deadline, self.stream_idle_timeout)
                .await
            {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    self.done = true;
                    return Some(Err(Error::from(Status::internal(
                        "response stream ended before final status",
                    ))));
                }
                Err(error) => {
                    self.done = true;
                    return Some(Err(error));
                }
            };

        match frame.frame_kind() {
            Some(RpcStreamFrameKind::Message) => {
                if let Some(max) = self.max_messages
                    && self.messages >= max
                {
                    self.done = true;
                    return Some(Err(Error::from(Status::resource_exhausted(format!(
                        "response stream exceeded maximum of {max} messages"
                    )))));
                }

                if frame.body.len() > self.max_body_size {
                    self.done = true;
                    return Some(Err(Error::FrameTooLarge {
                        len: frame.body.len(),
                        max: self.max_body_size,
                    }));
                }

                self.messages = self.messages.saturating_add(1);
                self.stream_body_size = self.stream_body_size.saturating_add(frame.body.len());

                if let Some(max) = self.max_stream_body_size
                    && self.stream_body_size > max
                {
                    self.done = true;
                    return Some(Err(Error::from(Status::resource_exhausted(format!(
                        "response stream exceeded maximum body size of {max} bytes"
                    )))));
                }

                match T::decode(frame.body.as_slice()).map_err(Error::from) {
                    Ok(message) => Some(Ok(message)),
                    Err(error) => {
                        self.done = true;
                        Some(Err(error))
                    }
                }
            }
            Some(RpcStreamFrameKind::Status) => {
                self.done = true;
                if let Err(error) = validate_stream_status_metadata(&frame) {
                    return Some(Err(error));
                }

                let status = frame.status_value();
                if status.is_ok() {
                    None
                } else {
                    Some(Err(Error::from(status)))
                }
            }
            None => {
                self.done = true;
                Some(Err(Error::from(Status::invalid_argument(
                    "response stream contained an unknown frame kind",
                ))))
            }
        }
    }
}

async fn next_frame_with_timeout(
    stream: &mut BoxMessageStream<RpcStreamFrame>,
    deadline: Option<Instant>,
    idle_timeout: Option<Duration>,
) -> Result<Option<RpcStreamFrame>> {
    match next_timeout(deadline, idle_timeout)? {
        Some((timeout, reason)) => tokio::time::timeout(timeout, stream.next())
            .await
            .map_err(|_| Error::from(reason.status()))?
            .transpose(),
        None => stream.next().await.transpose(),
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

fn validate_stream_status_metadata(frame: &RpcStreamFrame) -> Result<()> {
    validate_metadata(&frame.metadata).map_err(|status| {
        Error::from(Status::internal(format!(
            "invalid response metadata: {}",
            status.message()
        )))
    })
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::{Code, MessageStream, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

    use super::{
        CallOptions, ResponseMessageStream, RpcTransport, bidirectional_streaming,
        client_streaming, read_unary_response_from_stream, server_streaming, unary,
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

    #[derive(Clone, Default)]
    struct PendingTransport;

    #[crate::async_trait]
    impl RpcTransport for PendingTransport {
        async fn call(&self, _request: RpcRequest) -> Result<RpcResponse> {
            std::future::pending().await
        }

        async fn streaming_call(
            &self,
            _request: RpcRequest,
            _request_body: crate::BoxMessageStream<Vec<u8>>,
        ) -> Result<crate::BoxMessageStream<RpcStreamFrame>> {
            std::future::pending().await
        }
    }

    #[tokio::test]
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

    #[tokio::test]
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

        async fn streaming_call(
            &self,
            _request: RpcRequest,
            request_body: crate::BoxMessageStream<Vec<u8>>,
        ) -> Result<crate::BoxMessageStream<RpcStreamFrame>> {
            Ok(Box::new(UploadBlockedFrameStream { request_body }))
        }
    }

    struct UploadBlockedFrameStream {
        request_body: crate::BoxMessageStream<Vec<u8>>,
    }

    #[crate::async_trait]
    impl MessageStream<RpcStreamFrame> for UploadBlockedFrameStream {
        async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
            self.request_body.next().await.map(|result| {
                result.map(|_| RpcStreamFrame::status(Status::internal("unexpected request body")))
            })
        }
    }

    struct DropTrackedPendingStream {
        dropped: Arc<AtomicBool>,
    }

    #[crate::async_trait]
    impl MessageStream<TestMessage> for DropTrackedPendingStream {
        async fn next(&mut self) -> Option<Result<TestMessage>> {
            std::future::pending().await
        }
    }

    impl Drop for DropTrackedPendingStream {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn client_streaming_deadline_drops_pending_upload() {
        let dropped = Arc::new(AtomicBool::new(false));
        let error = client_streaming::<_, _, TestMessage>(
            &UploadWaitingTransport,
            "example.Greeter",
            "LotsOfGreetings",
            Box::new(DropTrackedPendingStream {
                dropped: Arc::clone(&dropped),
            }),
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .await
        .expect_err("pending upload should hit deadline");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn bidirectional_streaming_deadline_cancels_pending_response_read() {
        let dropped = Arc::new(AtomicBool::new(false));
        let mut responses = bidirectional_streaming::<_, _, TestMessage>(
            &UploadWaitingTransport,
            "example.Greeter",
            "BidiHello",
            Box::new(DropTrackedPendingStream {
                dropped: Arc::clone(&dropped),
            }),
            CallOptions::new().with_timeout(Duration::from_millis(1)),
        )
        .await
        .expect("stream should open");

        let error = responses
            .next()
            .await
            .expect("deadline error should be emitted")
            .expect_err("pending response should hit deadline");
        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);

        drop(responses);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn response_message_limit_returns_resource_exhausted() {
        let message = TestMessage {
            value: "hello".to_owned(),
        };
        let mut response = ResponseMessageStream::<TestMessage>::new(
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
        let mut response = ResponseMessageStream::<TestMessage>::new(
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
        let mut response = ResponseMessageStream::<TestMessage>::new(
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

    struct PendingFrameStream;

    #[crate::async_trait]
    impl MessageStream<RpcStreamFrame> for PendingFrameStream {
        async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn response_stream_idle_timeout_returns_unavailable() {
        let mut response = ResponseMessageStream::<TestMessage>::new(
            Box::new(PendingFrameStream),
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
    async fn response_stream_deadline_returns_deadline_exceeded() {
        let mut response = ResponseMessageStream::<TestMessage>::new(
            Box::new(PendingFrameStream),
            crate::framing::DEFAULT_MAX_FRAME_SIZE,
            Some(4096),
            Some(64 * 1024 * 1024),
            Some(Duration::from_secs(30)),
            Some(std::time::Instant::now() + Duration::from_millis(1)),
        );

        let error = response
            .next()
            .await
            .expect("timeout error should be emitted")
            .expect_err("pending response should hit deadline");

        assert_eq!(error.into_status().code(), Code::DeadlineExceeded);
        assert!(response.next().await.is_none());
    }
}
