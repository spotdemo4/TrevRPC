//! Native `TrevRPC` call transport over one established multiplexed connection: QUIC or `QMux`.

use std::sync::{Arc, OnceLock};

use bytes::Bytes;

use crate::datagram::{ClientFlow, Demux, FlowSender, MergedResponses};
use crate::framed;
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::response_state::UnaryResponse;
use crate::{BoxStream, Error, ResponseEnvelope, ResponseFrame, Result, RpcRequest, Unreliable};

use super::upload::UploadWriter;
use super::{RpcTransport, StreamingRpcTransport};

/// A native `TrevRPC` transport over any multiplexed connection: one RPC per bidirectional
/// stream.
#[derive(Clone)]
pub struct NativeTransport<C> {
    pub(crate) connection: C,
    pub(crate) max_frame_size: usize,
    /// The connection's datagram demultiplexer, created by the first call that needs it. It is
    /// `None` for connections without datagrams.
    datagrams: Arc<OnceLock<Option<Demux>>>,
}

impl<C: MuxConnection> NativeTransport<C> {
    pub(crate) fn new(connection: C, max_frame_size: usize) -> Self {
        Self {
            connection,
            max_frame_size,
            datagrams: Arc::new(OnceLock::new()),
        }
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
        unreliable: Unreliable,
    ) -> Result<BoxStream<ResponseFrame>> {
        let flow = if unreliable.any() {
            self.demux()
                .and_then(|demux| demux.open_client_flow(request.rpc_kind(), unreliable))
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
            }) => (requests, Some((responses, guard))),
            None => (None, None),
        };
        let upload = UploadWriter::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size, requests).await
        });

        let frames = response_stream(StreamingCall {
            recv,
            upload,
            max_frame_size,
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
        )
        .await?;
        call.send.finish_stream()?;

        let mut response = UnaryResponse::default();
        while let Some(frame) =
            framed::read_response_frame(&mut call.recv, self.max_frame_size).await?
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
        self.open_call(request, request_body, Unreliable::NONE)
            .await
    }

    async fn streaming_call_unreliable(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
        unreliable: Unreliable,
    ) -> Result<BoxStream<ResponseFrame>> {
        self.open_call(request, request_body, unreliable).await
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
            )
            .await?;
        }
        None => {
            framed::write_request_stream(
                &mut upload.send,
                &request,
                &mut request_body,
                max_frame_size,
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
        let item = match framed::read_response_frame(&mut call.recv, max_frame_size).await {
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
