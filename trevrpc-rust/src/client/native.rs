//! Native `TrevRPC` call transport over one established multiplexed connection: QUIC or `QMux`.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::compression::{ClientCall, ClientCompression, CompressionConfig, Dict};
use crate::datagram::{ClientFlow, Demux, FlowSender, MergedResponses};
use crate::framed::{self, ResponseReading};
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::response_state::UnaryResponse;
use crate::{BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest};

use super::upload::UploadWriter;
use super::{CallInfo, MessageTypes, RpcTransport, StreamingRpcTransport};

/// A native `TrevRPC` transport over any multiplexed connection: one RPC per bidirectional
/// stream.
#[derive(Clone)]
pub struct NativeTransport<C> {
    pub(crate) connection: C,
    pub(crate) max_frame_size: usize,
    /// The connection's datagram demultiplexer, created by the first call that needs it. It is
    /// `None` for connections without datagrams.
    datagrams: Arc<OnceLock<Option<Demux>>>,
    /// The connection's compression state, `None` without dictionaries.
    compression: Option<Arc<ClientCompression>>,
}

impl<C: MuxConnection> NativeTransport<C> {
    pub(crate) fn new(connection: C, max_frame_size: usize) -> Self {
        Self {
            connection,
            max_frame_size,
            datagrams: Arc::new(OnceLock::new()),
            compression: None,
        }
    }

    /// Gives the connection the dictionaries of `config`, which it negotiates afresh.
    pub(crate) fn set_compression(&mut self, config: Option<Arc<CompressionConfig>>) {
        self.compression = config.map(ClientCompression::new);
    }

    /// Fills the negotiation fields of `request` for a call whose message types are `types`.
    fn prepare(&self, request: &mut RpcRequest, types: Option<MessageTypes>) -> ClientCall {
        self.compression
            .as_ref()
            .map(|compression| compression.prepare(request, types))
            .unwrap_or_default()
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

    /// Opens the call's stream, giving it a datagram flow first if it is unreliable and this
    /// connection supports datagrams. The decision is made here, on the connection that carries
    /// the call.
    async fn open_call(
        &self,
        mut request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        info: CallInfo,
    ) -> Result<BoxStream<ResponseFrame>> {
        let call = self.prepare(&mut request, info.message_types);
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
        let compress = call.requests.clone();
        let upload = UploadWriter::spawn(async move {
            write_streaming_request(
                send,
                request,
                request_body,
                max_frame_size,
                requests,
                compress,
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
            },
            complete: false,
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
        let negotiated = self.prepare(&mut request, info.message_types);
        let (send, recv) = self.connection.open_stream().await?;
        let mut call = UnaryCall {
            send,
            recv,
            complete: false,
        };

        framed::write_request(
            &mut call.send,
            &request,
            vec![Bytes::from(body)],
            self.max_frame_size,
            negotiated.requests.as_ref(),
        )
        .await?;
        call.send.finish_stream()?;

        let mut reading = ResponseReading {
            accept: negotiated.accept,
            learn: negotiated.learn,
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
    compress: Option<Arc<Dict>>,
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
                compress.as_ref(),
            )
            .await?;
        }
        None => {
            framed::write_request_stream(
                &mut upload.send,
                &request,
                &mut request_body,
                max_frame_size,
                compress.as_ref(),
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
    Box::pin(futures_util::stream::unfold(call, |mut call| async move {
        if call.complete {
            return None;
        }

        let max_frame_size = call.max_frame_size;
        let item =
            match framed::read_response_frame(&mut call.recv, max_frame_size, &mut call.reading)
                .await
            {
                Ok(Some(ResponseFrame::Status(status))) => {
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

        Some((item, call))
    }))
}
