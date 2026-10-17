//! Helpers shared by the integration tests: raw frame IO over native QUIC and `WebTransport`
//! streams, and a test-only `WebTransport` client.
//!
//! The library itself ships no `WebTransport` or HTTP/3 client; these exist only to exercise the
//! server's `WebTransport` path.

#![allow(dead_code)]

#[macro_use]
mod ensure;

pub use ensure::AssertionFailed;

use std::future::Future;

use futures_util::StreamExt;
use prost::Message;
use tokio::task::JoinHandle;
use trevrpc::client::{RpcTransport, StreamingRpcTransport};
use trevrpc::framing::{self, DEFAULT_MAX_FRAME_SIZE};
use trevrpc::{
    BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;

/// A receive stream that can be read for raw frames.
pub trait RawRecv: Send {
    fn read_some(&mut self, buf: &mut [u8]) -> impl Future<Output = Result<Option<usize>>> + Send;
}

/// A send stream that can be written with raw frames.
pub trait RawSend: Send {
    fn write_all(&mut self, bytes: &[u8]) -> impl Future<Output = Result<()>> + Send;
}

impl RawRecv for quinn::RecvStream {
    async fn read_some(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        self.read(buf).await.map_err(Error::transport)
    }
}

impl RawSend for quinn::SendStream {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        Self::write_all(self, bytes).await.map_err(Error::transport)
    }
}

impl RawRecv for web_transport_quinn::RecvStream {
    async fn read_some(&mut self, buf: &mut [u8]) -> Result<Option<usize>> {
        self.read(buf).await.map_err(Error::transport)
    }
}

impl RawSend for web_transport_quinn::SendStream {
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        Self::write_all(self, bytes).await.map_err(Error::transport)
    }
}

fn unexpected_eof() -> Error {
    Error::transport(std::io::Error::new(
        std::io::ErrorKind::UnexpectedEof,
        "stream ended in the middle of a frame",
    ))
}

/// Fills `buf`, returning `false` on a clean end of stream before the first byte.
async fn read_exact_or_eof<R: RawRecv>(recv: &mut R, buf: &mut [u8]) -> Result<bool> {
    let total = buf.len();
    let mut rest = buf;
    while !rest.is_empty() {
        match recv.read_some(rest).await? {
            Some(read) => rest = rest.get_mut(read..).ok_or_else(unexpected_eof)?,
            None if rest.len() == total => return Ok(false),
            None => return Err(unexpected_eof()),
        }
    }
    Ok(true)
}

/// Reads one length-prefixed frame body, or `None` at a clean end of stream.
pub async fn read_raw_frame_or_eof<R: RawRecv>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<Vec<u8>>> {
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }
    let len = framing::frame_body_len(header, max_frame_size)?;
    let mut body = vec![0; len];
    if len > 0 && !read_exact_or_eof(recv, &mut body).await? {
        return Err(unexpected_eof());
    }
    Ok(Some(body))
}

/// Reads and decodes one length-prefixed protobuf frame.
pub async fn read_frame<R, M>(recv: &mut R, max_frame_size: usize) -> Result<M>
where
    R: RawRecv,
    M: Message + Default + 'static,
{
    let body = read_raw_frame_or_eof(recv, max_frame_size)
        .await?
        .ok_or_else(unexpected_eof)?;
    framing::decode_frame(&body)
}

/// Encodes and writes one length-prefixed protobuf frame.
pub async fn write_frame<S, M>(send: &mut S, message: &M, max_frame_size: usize) -> Result<()>
where
    S: RawSend,
    M: Message,
{
    send.write_all(&framing::encode_frame_with_max(message, max_frame_size)?)
        .await
}

async fn read_stream_frame_or_eof<R: RawRecv>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    read_raw_frame_or_eof(recv, max_frame_size)
        .await?
        .map(|body| framing::decode_stream_frame_body(&body))
        .transpose()
}

/// A test-only `WebTransport` client transport over one established session.
#[derive(Clone)]
pub struct WebTransportRpc {
    session: web_transport_quinn::Session,
}

impl WebTransportRpc {
    pub const fn new(session: web_transport_quinn::Session) -> Self {
        Self { session }
    }
}

/// Resets a send stream when dropped before it finished cleanly.
struct ResetOnDrop {
    stream: web_transport_quinn::SendStream,
    finished: bool,
}

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.stream.reset(CANCELLED_STREAM_CODE);
        }
    }
}

#[trevrpc::async_trait]
impl RpcTransport for WebTransportRpc {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (mut send, mut recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let result = async {
            write_frame(&mut send, &request, DEFAULT_MAX_FRAME_SIZE).await?;
            send.finish().map_err(Error::transport)?;
            read_frame::<_, RpcResponse>(&mut recv, DEFAULT_MAX_FRAME_SIZE).await
        }
        .await;
        if result.is_err() {
            let _ = send.reset(CANCELLED_STREAM_CODE);
            let _ = recv.stop(CANCELLED_STREAM_CODE);
        }
        result
    }
}

#[trevrpc::async_trait]
impl StreamingRpcTransport for WebTransportRpc {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let upload = tokio::spawn(async move {
            let mut guard = ResetOnDrop {
                stream: send,
                finished: false,
            };
            let send = &mut guard.stream;
            write_frame(send, &request, DEFAULT_MAX_FRAME_SIZE).await?;
            while let Some(body) = request_body.next().await.transpose()? {
                let frame = framing::encode_message_stream_frame(&body, DEFAULT_MAX_FRAME_SIZE)?;
                send.write_all(&frame).await.map_err(Error::transport)?;
            }
            send.finish().map_err(Error::transport)?;
            // Finished cleanly: nothing left to reset.
            guard.finished = true;
            Ok::<_, Error>(())
        });

        let state = ResponseState {
            recv: Some(recv),
            upload: Some(upload),
            complete: false,
        };
        Ok(Box::pin(futures_util::stream::unfold(
            state,
            |mut state| async move {
                if state.complete {
                    return None;
                }
                let mut recv = state.recv.take()?;
                let item = match read_stream_frame_or_eof(&mut recv, DEFAULT_MAX_FRAME_SIZE).await {
                    Ok(Some(frame)) if frame.frame_kind() == Some(RpcStreamFrameKind::Status) => {
                        state.finish();
                        let status = frame.status_value();
                        let drained =
                            read_stream_frame_or_eof(&mut recv, DEFAULT_MAX_FRAME_SIZE).await;
                        match drained {
                            Ok(Some(_)) => Err(Error::from(Status::internal(
                                "response stream continued after terminal status",
                            ))),
                            Err(error) if status.is_ok() => Err(error),
                            Ok(None) | Err(_) => Ok(frame),
                        }
                    }
                    Ok(Some(frame)) => Ok(frame),
                    Ok(None) => {
                        state.finish();
                        return None;
                    }
                    Err(error) => {
                        state.finish();
                        Err(error)
                    }
                };
                state.recv = Some(recv);
                Some((item, state))
            },
        )))
    }
}

struct ResponseState {
    recv: Option<web_transport_quinn::RecvStream>,
    upload: Option<JoinHandle<Result<()>>>,
    complete: bool,
}

impl ResponseState {
    /// Marks the response complete and abandons the upload, which the server no longer reads.
    fn finish(&mut self) {
        self.complete = true;
        if let Some(upload) = self.upload.take() {
            upload.abort();
        }
    }
}

impl Drop for ResponseState {
    fn drop(&mut self) {
        if let Some(upload) = self.upload.take() {
            upload.abort();
        }
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            let _ = recv.stop(CANCELLED_STREAM_CODE);
        }
    }
}
