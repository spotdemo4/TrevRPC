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
    metadata: Metadata,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            max_response_body_size: DEFAULT_MAX_FRAME_SIZE,
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
        metadata,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;

    let request = RpcRequest::new(service, method, request.encode_to_vec()).with_metadata(metadata);
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

    read_unary_response_from_stream(response, max_response_body_size, deadline).await
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
        deadline,
    )))
}

struct PreparedStreamingCall {
    request: RpcRequest,
    deadline: Option<Instant>,
    max_response_body_size: usize,
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
        metadata,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;

    Ok(PreparedStreamingCall {
        request: RpcRequest::new(service, method, body)
            .with_kind(kind)
            .with_metadata(metadata),
        deadline: timeout.and_then(|timeout| Instant::now().checked_add(timeout)),
        max_response_body_size,
    })
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
    deadline: Option<Instant>,
) -> Result<Res>
where
    Res: Message + Default + Send + 'static,
{
    let mut response =
        ResponseMessageStream::<Res>::new(response, max_response_body_size, deadline);
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
    deadline: Option<Instant>,
    done: bool,
    _marker: PhantomData<T>,
}

impl<T> ResponseMessageStream<T> {
    const fn new(
        inner: BoxMessageStream<RpcStreamFrame>,
        max_body_size: usize,
        deadline: Option<Instant>,
    ) -> Self {
        Self {
            inner,
            max_body_size,
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

        let frame = match next_frame_with_deadline(&mut self.inner, self.deadline).await {
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
                if frame.body.len() > self.max_body_size {
                    return Some(Err(Error::FrameTooLarge {
                        len: frame.body.len(),
                        max: self.max_body_size,
                    }));
                }

                Some(T::decode(frame.body.as_slice()).map_err(Error::from))
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
                Some(Err(Error::from(Status::internal(
                    "response stream contained an unknown frame kind",
                ))))
            }
        }
    }
}

async fn next_frame_with_deadline(
    stream: &mut BoxMessageStream<RpcStreamFrame>,
    deadline: Option<Instant>,
) -> Result<Option<RpcStreamFrame>> {
    match remaining_timeout(deadline)? {
        Some(timeout) => tokio::time::timeout(timeout, stream.next())
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))?
            .transpose(),
        None => stream.next().await.transpose(),
    }
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
    use crate::{RpcStreamFrame, Status};

    use super::{CallOptions, read_unary_response_from_stream};

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
            None,
        )
        .await
        .expect("response should decode");

        assert_eq!(decoded, message);
    }
}
