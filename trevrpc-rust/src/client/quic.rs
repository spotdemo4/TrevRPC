//! Native QUIC call transport over one established Quinn connection.

use crate::framed;
use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::{
    BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind, Status,
};

use super::upload::UploadWriter;
use super::{RpcTransport, StreamingRpcTransport};

/// Application error code for streams a call abandons.
const CANCELLED_STREAM_CODE: u32 = 1;

/// A native `TrevRPC` transport over one established Quinn connection.
///
/// The transport never reconnects. Most applications should use [`super::Channel`], which
/// supervises a fresh `QuicTransport` for every connection generation.
#[derive(Clone)]
pub struct QuicTransport {
    connection: quinn::Connection,
    max_frame_size: usize,
}

impl QuicTransport {
    /// Creates a transport over an established connection with the default maximum frame size.
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Sets the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    /// Returns the established connection.
    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    /// Returns the maximum `TrevRPC` frame size in bytes.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

#[crate::async_trait]
impl RpcTransport for QuicTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
        let mut call = UnaryCall {
            send,
            recv,
            complete: false,
        };

        framed::write_frame(&mut call.send, &request, self.max_frame_size).await?;
        call.send.finish().map_err(Error::transport)?;

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
impl StreamingRpcTransport for QuicTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
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
struct UnaryCall {
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    complete: bool,
}

impl Drop for UnaryCall {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.send.reset(CANCELLED_STREAM_CODE.into());
            let _ = self.recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

/// Resets a streaming call's request stream if the upload is dropped before it finishes.
struct UploadStream {
    send: quinn::SendStream,
    complete: bool,
}

impl Drop for UploadStream {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.send.reset(CANCELLED_STREAM_CODE.into());
        }
    }
}

async fn write_streaming_request(
    send: quinn::SendStream,
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
    upload.send.finish().map_err(Error::transport)?;
    upload.complete = true;
    Ok(())
}

/// The response half of a streaming call and the upload task feeding its request half.
struct StreamingCall {
    recv: quinn::RecvStream,
    upload: UploadWriter,
    max_frame_size: usize,
    complete: bool,
}

impl Drop for StreamingCall {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

/// Drives the logical lifecycle of a streaming response: upload settlement, terminal-status
/// precedence, and the clean end of the response stream.
fn response_stream(call: StreamingCall) -> BoxStream<RpcStreamFrame> {
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
