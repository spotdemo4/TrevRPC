//! Native `TrevRPC` call transport over one established multiplexed connection: QUIC or `QMux`.

use crate::framed;
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::{
    BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind, Status,
};

use super::upload::UploadWriter;
use super::{RpcTransport, StreamingRpcTransport};

/// A native `TrevRPC` transport over any multiplexed connection: one RPC per bidirectional
/// stream.
#[derive(Clone)]
pub(crate) struct NativeTransport<C> {
    pub(crate) connection: C,
    pub(crate) max_frame_size: usize,
}

#[crate::async_trait]
impl<C: MuxConnection> RpcTransport for NativeTransport<C> {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.connection.open_stream().await?;
        let mut call = UnaryCall {
            send,
            recv,
            complete: false,
        };

        framed::write_frame(&mut call.send, &request, self.max_frame_size).await?;
        call.send.finish_stream()?;

        let response =
            framed::read_frame::<_, RpcResponse>(&mut call.recv, self.max_frame_size).await?;
        if framed::read_raw_frame_or_eof(&mut call.recv, self.max_frame_size)
            .await?
            .is_some()
        {
            return Err(Error::from(Status::internal(
                "unary response contained more than one response frame",
            )));
        }
        call.complete = true;
        Ok(response)
    }
}

#[crate::async_trait]
impl<C: MuxConnection> StreamingRpcTransport for NativeTransport<C> {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self.connection.open_stream().await?;
        let max_frame_size = self.max_frame_size;
        let upload = UploadWriter::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(response_stream(StreamingCall {
            recv,
            upload,
            max_frame_size,
            complete: false,
        }))
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
) -> Result<()> {
    let mut upload = UploadStream {
        send,
        complete: false,
    };
    framed::write_frame(&mut upload.send, &request, max_frame_size).await?;
    framed::write_request_body_frames(&mut upload.send, &mut request_body, max_frame_size).await?;
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
fn response_stream<R: MuxRecvStream>(call: StreamingCall<R>) -> BoxStream<RpcStreamFrame> {
    Box::pin(futures_util::stream::unfold(call, |mut call| async move {
        if call.complete {
            return None;
        }

        let max_frame_size = call.max_frame_size;
        let item = match framed::read_stream_frame_or_eof(&mut call.recv, max_frame_size).await {
            Ok(Some(frame)) if frame.frame_kind() == Some(RpcStreamFrameKind::Status) => {
                let status = frame.status_value();
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
                    upload_result.map(|()| frame)
                } else {
                    Ok(frame)
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
