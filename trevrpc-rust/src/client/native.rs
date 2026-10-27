//! Native `TrevRPC` call transport over one established multiplexed connection: QUIC or `QMux`.

use std::sync::{Arc, OnceLock};
use std::time::Instant;

use bytes::Bytes;
use futures_util::StreamExt as _;
use futures_util::future::BoxFuture;

use crate::compression::{
    ClientCall, ClientCompression, CompressionConfig, DEFAULT_CLIENT_MEMORY, MessageEncoder,
    StreamBudget, offer_stream,
};
use crate::connection_metadata::{ClientConnectionMetadata, ConnectionMetadataSource, OfferedSet};
use crate::datagram::{ClientFlow, Demux, FlowSender, MergedResponses};
use crate::framed::{self, ResponseReading};
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::response_state::UnaryResponse;
use crate::{BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest};

use super::replay::{Optimism, Replay};
use super::upload::UploadWriter;
use super::{CallInfo, RpcTransport, StreamingRpcTransport};
use crate::Status;

/// A native `TrevRPC` transport over any multiplexed connection: one RPC per bidirectional
/// stream.
#[derive(Clone)]
pub struct NativeTransport<C> {
    pub(crate) connection: C,
    pub(crate) max_frame_size: usize,
    /// The connection's datagram demultiplexer, created by the first call that needs it. It is
    /// `None` for connections without datagrams.
    datagrams: Arc<OnceLock<Option<Demux>>>,
    /// The connection's compression state: its dictionaries, if any, and compression without
    /// one.
    compression: Arc<ClientCompression>,
    /// The zstd level of outgoing messages.
    level: i32,
    /// The memory stream compression may hold, `None` when it is disabled.
    stream_budget: Option<Arc<StreamBudget>>,
    /// Whether calls on this connection stream-compress their requests optimistically.
    optimism: Arc<Optimism>,
    /// The channel's connection metadata as this connection knows it, `None` without a channel.
    connection_metadata: Option<Arc<ClientConnectionMetadata>>,
}

/// Opens the replay of a refused optimistic call with the requests to send on it.
type Retry = Box<
    dyn FnOnce(BoxStream<Vec<u8>>) -> BoxFuture<'static, Result<BoxStream<ResponseFrame>>> + Send,
>;

impl<C: MuxConnection> NativeTransport<C> {
    pub(crate) fn new(connection: C, max_frame_size: usize) -> Self {
        Self {
            connection,
            max_frame_size,
            datagrams: Arc::new(OnceLock::new()),
            compression: ClientCompression::new(CompressionConfig::plain_only()),
            level: CompressionConfig::level(None),
            stream_budget: StreamBudget::new(DEFAULT_CLIENT_MEMORY),
            optimism: Arc::new(Optimism::default()),
            connection_metadata: None,
        }
    }

    /// Gives the connection the dictionaries of `config`, which it negotiates afresh; `None`
    /// leaves it compression without a dictionary only.
    pub(crate) fn set_compression(&mut self, config: Option<Arc<CompressionConfig>>) {
        self.level = CompressionConfig::level(config.as_ref());
        self.compression =
            ClientCompression::new(config.unwrap_or_else(CompressionConfig::plain_only));
    }

    /// Makes stream compression hold memory from `budget`, which channels share between their
    /// connections; `None` disables it.
    pub(crate) fn set_stream_budget(&mut self, budget: Option<Arc<StreamBudget>>) {
        self.stream_budget = budget;
    }

    /// Makes calls carry the connection metadata of `source`, which this connection, new,
    /// offers until the server confirms it.
    pub(crate) fn set_connection_metadata(&mut self, source: Arc<ConnectionMetadataSource>) {
        self.connection_metadata = Some(ClientConnectionMetadata::new(source));
    }

    /// Fills the negotiation fields of `request` for a call described by `info`, returning the
    /// call's compression and the connection metadata set it offered.
    fn prepare(
        &self,
        request: &mut RpcRequest,
        info: &CallInfo,
    ) -> (ClientCall, Option<OfferedSet>) {
        let offered = self
            .connection_metadata
            .as_ref()
            .and_then(|connection| connection.prepare(request));
        let call = self
            .compression
            .prepare(request, info.message_types, info.compress);
        (call, offered)
    }

    fn demux(&self) -> Option<&Demux> {
        self.datagrams
            .get_or_init(|| {
                self.connection
                    .datagrams()
                    .map(|transport| Demux::client(transport, self.max_frame_size))
            })
            .as_ref()
    }

    /// Opens a streaming call. A call whose requests may be stream-compressed is optimistic while
    /// the connection is: it compresses them without waiting for the server's answer, and is
    /// replayed without stream compression if the server refuses it.
    ///
    /// Calls with a datagram flow are never optimistic, since a replay would need a second flow.
    async fn open_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        let directions = info
            .compress_stream
            .effective(request.rpc_kind())
            .without_unreliable(info.unreliable);
        let budget = self
            .stream_budget
            .as_ref()
            .filter(|_| directions.requests && !info.unreliable.any() && self.optimism.get());
        let Some(budget) = budget else {
            return self.open_stream(request, request_body, info, None).await;
        };
        let (replay, requests) = Replay::new(request_body, Arc::clone(budget));
        let transport = self.clone();
        let original = request.clone();
        let started = Instant::now();
        let retry: Retry = Box::new(move |requests| {
            Box::pin(async move { transport.replay(original, requests, info, started).await })
        });
        self.open_stream(request, requests, info, Some((replay, retry)))
            .await
    }

    /// Opens the replay of a refused optimistic call, not optimistic, with the time left of the
    /// original deadline.
    async fn replay(
        &self,
        mut request: RpcRequest,
        requests: BoxStream<Vec<u8>>,
        info: CallInfo,
        started: Instant,
    ) -> Result<BoxStream<ResponseFrame>> {
        if request.timeout_nanos != 0 {
            let elapsed = u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            request.timeout_nanos = request.timeout_nanos.saturating_sub(elapsed);
            if request.timeout_nanos == 0 {
                let status = Status::deadline_exceeded("deadline exceeded before the replay");
                return Ok(Box::pin(futures_util::stream::once(async move {
                    Ok(ResponseFrame::Status(status))
                })));
            }
        }
        self.open_stream(request, requests, info, None).await
    }

    /// Opens the call's stream, giving it a datagram flow first if it is unreliable and this
    /// connection supports datagrams. The decision is made here, on the connection that carries
    /// the call. `optimistic` is the replay of an optimistic call and how to open it again.
    async fn open_stream(
        &self,
        mut request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
        optimistic: Option<(Replay, Retry)>,
    ) -> Result<BoxStream<ResponseFrame>> {
        let (call, offered) = self.prepare(&mut request, &info);
        let directions = info
            .compress_stream
            .effective(request.rpc_kind())
            .without_unreliable(info.unreliable);
        let stream = offer_stream(
            &mut request,
            directions,
            self.stream_budget.as_ref(),
            &call.accept,
            self.level,
            optimistic.is_some(),
        );
        // A call that could not offer is not optimistic after all: it keeps no copies.
        let (replay, retry) = match optimistic {
            Some((replay, retry)) if stream.optimistic => (Some(replay), Some(retry)),
            Some((replay, _)) => {
                replay.settle();
                (None, None)
            }
            None => (None, None),
        };
        let flow = if info.unreliable.any() {
            self.demux().and_then(|demux| {
                demux.open_client_flow(request.rpc_kind(), info.unreliable, call.accept.clone())
            })
        } else {
            None
        };
        if let Some(flow) = &flow {
            request.datagram_flow = flow.flow;
        }

        let (send, recv) = self.connection.open_stream().await?;
        let max_frame_size = self.max_frame_size;
        let (requests, responses) = match flow {
            Some(ClientFlow {
                requests,
                responses,
                guard,
                ..
            }) => (
                requests.map(|sender| sender.with_compression(call.requests.clone())),
                Some((responses, guard)),
            ),
            None => (None, None),
        };
        let encoder = MessageEncoder::new(call.requests.clone()).with_stream(stream.requests);
        let upload = UploadWriter::spawn(async move {
            write_streaming_request(
                send,
                request,
                request_body,
                max_frame_size,
                requests,
                encoder,
            )
            .await
        });

        let frames = response_stream(StreamingCall {
            recv,
            upload,
            max_frame_size,
            reading: ResponseReading {
                accept: call.accept,
                learn: call.learn,
                stream: stream.responses,
                enable_requests: stream.enable_requests,
                replay,
                refused: None,
                optimism: directions.requests.then(|| Arc::clone(&self.optimism)),
                connection_metadata: offered,
                ..ResponseReading::default()
            },
            complete: false,
            retry,
        });
        Ok(match responses {
            Some((responses, guard)) => MergedResponses::boxed(frames, responses, guard),
            None => frames,
        })
    }
}

#[crate::async_trait]
impl<C: MuxConnection> RpcTransport for NativeTransport<C> {
    async fn call(&self, request: RpcRequest, body: Vec<u8>) -> Result<ResponseEnvelope<Bytes>> {
        self.call_with_info(request, body, CallInfo::default())
            .await
    }

    async fn call_with_info(
        &self,
        mut request: RpcRequest,
        body: Vec<u8>,
        info: CallInfo,
    ) -> Result<ResponseEnvelope<Bytes>> {
        let (negotiated, offered) = self.prepare(&mut request, &info);
        let (send, recv) = self.connection.open_stream().await?;
        let mut call = UnaryCall {
            send,
            recv,
            complete: false,
        };

        framed::write_unary_request(
            &mut call.send,
            &request,
            Bytes::from(body),
            self.max_frame_size,
            negotiated.requests.get().as_ref(),
        )
        .await?;
        call.send.finish_stream()?;

        let mut reading = ResponseReading {
            accept: negotiated.accept,
            learn: negotiated.learn,
            connection_metadata: offered,
            ..ResponseReading::default()
        };
        let mut response = UnaryResponse::default();
        while let Some(frame) =
            framed::read_response_frame(&mut call.recv, self.max_frame_size, &mut reading).await?
        {
            if response.accept(frame).map_err(Error::from)? {
                framed::drain_fin_after_terminal_status(
                    &mut call.recv,
                    self.max_frame_size,
                    "unary response stream",
                )
                .await?;
                break;
            }
        }
        call.complete = true;
        response.finish().map_err(Error::from)
    }
}

#[crate::async_trait]
impl<C: MuxConnection> StreamingRpcTransport for NativeTransport<C> {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.open_call(request, request_body, CallInfo::default())
            .await
    }

    async fn streaming_call_with_info(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.open_call(request, request_body, info).await
    }
}

/// Resets both halves of a unary call's stream if the call is dropped before completing.
struct UnaryCall<S: MuxSendStream, R: MuxRecvStream> {
    send: S,
    recv: R,
    complete: bool,
}

impl<S: MuxSendStream, R: MuxRecvStream> Drop for UnaryCall<S, R> {
    fn drop(&mut self) {
        if !self.complete {
            self.send.reset_stream(CANCELLED_STREAM_CODE);
            self.recv.stop_stream(CANCELLED_STREAM_CODE);
        }
    }
}

/// Resets a streaming call's request stream if the upload is dropped before it finishes.
struct UploadStream<S: MuxSendStream> {
    send: S,
    complete: bool,
}

impl<S: MuxSendStream> Drop for UploadStream<S> {
    fn drop(&mut self) {
        if !self.complete {
            self.send.reset_stream(CANCELLED_STREAM_CODE);
        }
    }
}

async fn write_streaming_request<S: MuxSendStream>(
    send: S,
    request: RpcRequest,
    mut request_body: BoxStream<Vec<u8>>,
    max_frame_size: usize,
    datagrams: Option<FlowSender>,
    mut encoder: MessageEncoder,
) -> Result<()> {
    let mut upload = UploadStream {
        send,
        complete: false,
    };
    match &datagrams {
        Some(datagrams) => {
            framed::write_request_stream_datagrams(
                &mut upload.send,
                &request,
                &mut request_body,
                max_frame_size,
                datagrams,
                &mut encoder,
            )
            .await?;
        }
        None => {
            framed::write_request_stream(
                &mut upload.send,
                &request,
                &mut request_body,
                max_frame_size,
                &mut encoder,
            )
            .await?;
        }
    }
    upload.send.finish_stream()?;
    upload.complete = true;
    Ok(())
}

/// The response half of a streaming call and the upload task feeding its request half.
struct StreamingCall<R: MuxRecvStream> {
    recv: R,
    upload: UploadWriter,
    max_frame_size: usize,
    reading: ResponseReading,
    complete: bool,
    /// How to replay the call if the server refuses it, for an optimistic call.
    retry: Option<Retry>,
}

/// Where a streaming response stands.
///
/// Boxing the call would cost every streaming call an allocation; a replayed call is rare.
#[allow(clippy::large_enum_variant)]
enum Response<R: MuxRecvStream> {
    Call(StreamingCall<R>),
    /// The server refused the optimistic call; its replay answers instead.
    Replayed(BoxStream<ResponseFrame>),
    Done,
}

impl<R: MuxRecvStream> Drop for StreamingCall<R> {
    fn drop(&mut self) {
        if !self.complete {
            self.recv.stop_stream(CANCELLED_STREAM_CODE);
        }
    }
}

/// Drives the logical lifecycle of a streaming response: upload settlement, terminal-status
/// precedence, and the clean end of the response stream.
fn response_stream<R: MuxRecvStream>(call: StreamingCall<R>) -> BoxStream<ResponseFrame> {
    Box::pin(futures_util::stream::unfold(
        Response::Call(call),
        |state| async move {
            match state {
                Response::Call(call) => next_response(call).await,
                Response::Replayed(mut frames) => {
                    let item = frames.next().await?;
                    Some((item, Response::Replayed(frames)))
                }
                Response::Done => None,
            }
        },
    ))
}

async fn next_response<R: MuxRecvStream>(
    mut call: StreamingCall<R>,
) -> Option<(Result<ResponseFrame>, Response<R>)> {
    if call.complete {
        return None;
    }

    let max_frame_size = call.max_frame_size;
    let item = match framed::read_response_frame(&mut call.recv, max_frame_size, &mut call.reading)
        .await
    {
        Ok(Some(ResponseFrame::Status(status))) => {
            if let Some(replay) = call.reading.refused.take()
                && let Some(retry) = call.retry.take()
            {
                // Nothing of the call reached application code: replay it, with the
                // requests sent so far and then the rest.
                let _ = call.upload.abort_and_settle().await;
                let requests = replay.refuse();
                drop(call);
                return match retry(requests).await {
                    Ok(mut frames) => {
                        let item = frames.next().await?;
                        Some((item, Response::Replayed(frames)))
                    }
                    Err(error) => Some((Err(error), Response::Done)),
                };
            }
            let upload_result = call.upload.abort_and_settle().await;
            let drain_result = framed::drain_fin_after_terminal_status(
                &mut call.recv,
                max_frame_size,
                "response stream",
            )
            .await;
            call.complete = true;

            if let Err(error) = drain_result {
                Err(error)
            } else if status.is_ok() {
                upload_result.map(|()| ResponseFrame::Status(status))
            } else {
                Ok(ResponseFrame::Status(status))
            }
        }
        Ok(Some(frame)) => Ok(frame),
        Ok(None) => {
            let _ = call.upload.abort_and_settle().await;
            return None;
        }
        Err(error) => {
            let _ = call.upload.abort_and_settle().await;
            call.complete = true;
            Err(error)
        }
    };

    Some((item, Response::Call(call)))
}
