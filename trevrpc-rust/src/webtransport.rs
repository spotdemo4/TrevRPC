use bytes::Bytes;
use prost::Message;

use crate::advanced::RawWebTransport;
use crate::client::{RpcTransport, StreamingRpcTransport};
use crate::client_upload::UploadWriter;
use crate::framed::{self, FrameRead, FrameWrite, NoopFrameTrace};
use crate::{
    BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind,
};

const CANCELLED_STREAM_CODE: u32 = 1;

impl FrameWrite for web_transport_quinn::SendStream {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.write_all_chunks(chunks)
            .await
            .map_err(Error::transport)
    }
}

impl FrameRead for web_transport_quinn::RecvStream {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}

#[crate::async_trait]
impl RpcTransport for RawWebTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.session().open_bi().await.map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv);

        write_frame(streams.send_mut(), &request, self.max_frame_size()).await?;
        streams.send_mut().finish().map_err(Error::transport)?;

        let response = read_frame(streams.recv_mut(), self.max_frame_size()).await?;
        streams.complete();

        Ok(response)
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for RawWebTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self.session().open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size();
        let writer = UploadWriter::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(webtransport_response_stream(
            recv,
            writer,
            self.max_frame_size(),
        ))
    }
}

struct CancellableBiStream {
    send: Option<web_transport_quinn::SendStream>,
    recv: Option<web_transport_quinn::RecvStream>,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: web_transport_quinn::SendStream, recv: web_transport_quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }

    fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CancellableBiStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            let _ = send.reset(cancelled_stream_code());
        }

        if let Some(recv) = &mut self.recv {
            let _ = recv.stop(cancelled_stream_code());
        }
    }
}

struct CancellableSendStream {
    send: Option<web_transport_quinn::SendStream>,
    complete: bool,
}

impl CancellableSendStream {
    fn new(send: web_transport_quinn::SendStream) -> Self {
        Self {
            send: Some(send),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CancellableSendStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            let _ = send.reset(cancelled_stream_code());
        }
    }
}

struct WebTransportResponseStream {
    recv: Option<web_transport_quinn::RecvStream>,
    writer: UploadWriter,
    max_frame_size: usize,
    complete: bool,
}

impl WebTransportResponseStream {
    const fn new(
        recv: web_transport_quinn::RecvStream,
        writer: UploadWriter,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            writer,
            max_frame_size,
            complete: false,
        }
    }

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }
}

fn webtransport_response_stream(
    recv: web_transport_quinn::RecvStream,
    writer: UploadWriter,
    max_frame_size: usize,
) -> BoxStream<RpcStreamFrame> {
    Box::pin(futures_util::stream::unfold(
        WebTransportResponseStream::new(recv, writer, max_frame_size),
        |mut stream| async move {
            if stream.complete {
                return None;
            }

            let max_frame_size = stream.max_frame_size;
            let item = match read_stream_frame_or_eof(stream.recv_mut(), max_frame_size).await {
                Ok(Some(frame)) if frame.frame_kind() == Some(RpcStreamFrameKind::Status) => {
                    let status = frame.status_value();
                    let writer_result = stream.writer.abort_and_settle().await;
                    let drain_result = drain_fin_after_terminal_status(
                        stream.recv_mut(),
                        max_frame_size,
                        "response stream",
                    )
                    .await;
                    stream.complete = true;
                    if let Err(error) = drain_result {
                        Err(error)
                    } else if status.is_ok() {
                        writer_result.map(|()| frame)
                    } else {
                        Ok(frame)
                    }
                }
                Ok(Some(frame)) => Ok(frame),
                Ok(None) => {
                    let _ = stream.writer.abort_and_settle().await;
                    return None;
                }
                Err(error) => {
                    let _ = stream.writer.abort_and_settle().await;
                    stream.complete = true;
                    Err(error)
                }
            };

            Some((item, stream))
        },
    ))
}

impl Drop for WebTransportResponseStream {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            let _ = recv.stop(cancelled_stream_code());
        }
    }
}

/// Writes a length-prefixed protobuf frame to a `WebTransport` send stream.
pub async fn write_frame<M>(
    send: &mut web_transport_quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    framed::write_frame::<_, NoopFrameTrace, M>(send, message, max_frame_size).await
}

/// Reads and decodes one length-prefixed protobuf frame from a `WebTransport` receive stream.
pub async fn read_frame<M>(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<M>
where
    M: Message + Default + 'static,
{
    framed::read_frame::<_, NoopFrameTrace, M>(recv, max_frame_size).await
}

async fn read_stream_frame_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    framed::read_stream_frame_or_eof::<_, NoopFrameTrace>(recv, max_frame_size).await
}

async fn drain_fin_after_terminal_status(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
    stream_name: &'static str,
) -> Result<()> {
    framed::drain_fin_after_terminal_status::<_, NoopFrameTrace>(recv, max_frame_size, stream_name)
        .await
}

async fn write_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    write_request_body_frames(send.send_mut(), &mut request_body, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut web_transport_quinn::SendStream,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_request_body_frames::<_, NoopFrameTrace>(send, request_body, max_frame_size).await
}

fn cancelled_stream_code() -> u32 {
    CANCELLED_STREAM_CODE
}
