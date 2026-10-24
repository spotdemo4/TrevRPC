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
use tokio::task::JoinHandle;
use trevrpc::client::{RpcTransport, StreamingRpcTransport};
use trevrpc::framing::{self, DEFAULT_MAX_FRAME_SIZE, frame_type};
use trevrpc::{
    BoxStream, Error, Metadata, ResponseEnvelope, ResponseFrame, Result, RpcHeaders, RpcRequest,
    RpcStatus, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;

/// The path of a method of the example `Greeter` service, including ones it does not define.
pub fn greeter_path(method: &str) -> String {
    format!("/example.greeter.Greeter/{method}")
}

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

fn invalid(message: &'static str) -> Error {
    Error::from(Status::invalid_argument(message))
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

/// Reads one QUIC varint a byte at a time, or `None` at a clean end of stream.
async fn read_varint<R: RawRecv>(recv: &mut R) -> Result<Option<u64>> {
    let mut first = [0_u8; 1];
    if !read_exact_or_eof(recv, &mut first).await? {
        return Ok(None);
    }
    let mut bytes = first.to_vec();
    let len = 1_usize << (first[0] >> 6);
    while bytes.len() < len {
        let mut next = [0_u8; 1];
        if !read_exact_or_eof(recv, &mut next).await? {
            return Err(unexpected_eof());
        }
        bytes.push(next[0]);
    }
    Ok(framing_varint(&bytes))
}

fn framing_varint(bytes: &[u8]) -> Option<u64> {
    trevrpc::varint::decode(bytes).map(|(value, _)| value)
}

/// A frame as it appears on the wire, of any type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub frame_type: u64,
    pub payload: Vec<u8>,
}

/// Reads one frame of any type, or `None` at a clean end of stream.
pub async fn read_any_frame<R: RawRecv>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<RawFrame>> {
    let Some(frame_type) = read_varint(recv).await? else {
        return Ok(None);
    };
    let length = read_varint(recv).await?.ok_or_else(unexpected_eof)?;
    let len = usize::try_from(length).unwrap_or(usize::MAX);
    if len > max_frame_size {
        return Err(Error::FrameTooLarge {
            len,
            max: max_frame_size,
        });
    }
    let mut payload = vec![0; len];
    if len > 0 && !read_exact_or_eof(recv, &mut payload).await? {
        return Err(unexpected_eof());
    }
    Ok(Some(RawFrame {
        frame_type,
        payload,
    }))
}

/// Splits a buffer holding whole frames into those frames, of any type.
pub fn parse_frames(mut bytes: &[u8]) -> Result<Vec<RawFrame>> {
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let (frame_type, type_len) = trevrpc::varint::decode(bytes).ok_or_else(unexpected_eof)?;
        let rest = bytes.get(type_len..).ok_or_else(unexpected_eof)?;
        let (length, length_len) = trevrpc::varint::decode(rest).ok_or_else(unexpected_eof)?;
        let rest = rest.get(length_len..).ok_or_else(unexpected_eof)?;
        let length = usize::try_from(length).map_err(|_| unexpected_eof())?;
        let payload = rest.get(..length).ok_or_else(unexpected_eof)?;
        frames.push(RawFrame {
            frame_type,
            payload: payload.to_vec(),
        });
        bytes = rest.get(length..).ok_or_else(unexpected_eof)?;
    }
    Ok(frames)
}

/// Reads the next frame of a defined type, skipping any other, or `None` at a clean end.
pub async fn read_known_frame<R: RawRecv>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<RawFrame>> {
    while let Some(frame) = read_any_frame(recv, max_frame_size).await? {
        if frame.frame_type <= frame_type::STATUS {
            return Ok(Some(frame));
        }
    }
    Ok(None)
}

/// Reads the next response frame, or `None` at a clean end of stream.
pub async fn read_response_frame<R: RawRecv>(recv: &mut R) -> Result<Option<ResponseFrame>> {
    let Some(frame) = read_known_frame(recv, DEFAULT_MAX_FRAME_SIZE).await? else {
        return Ok(None);
    };
    Ok(Some(match frame.frame_type {
        frame_type::MESSAGE => ResponseFrame::message(frame.payload),
        frame_type::HEADERS => {
            ResponseFrame::Headers(framing::decode_payload::<RpcHeaders>(&frame.payload)?.metadata)
        }
        frame_type::STATUS => ResponseFrame::Status(Status::from(framing::decode_payload::<
            RpcStatus,
        >(&frame.payload)?)),
        _ => {
            return Err(Error::from(Status::internal(
                "response stream contained a REQUEST frame",
            )));
        }
    }))
}

/// Reads the REQUEST frame that opens a request stream.
pub async fn read_request<R: RawRecv>(recv: &mut R) -> Result<RpcRequest> {
    let frame = read_known_frame(recv, DEFAULT_MAX_FRAME_SIZE)
        .await?
        .ok_or_else(unexpected_eof)?;
    if frame.frame_type != frame_type::REQUEST {
        return Err(invalid("request stream must begin with a REQUEST frame"));
    }
    framing::decode_payload(&frame.payload)
}

/// Reads the next request-stream MESSAGE frame's payload, or `None` at a clean end.
pub async fn read_message<R: RawRecv>(recv: &mut R) -> Result<Option<Vec<u8>>> {
    match read_known_frame(recv, DEFAULT_MAX_FRAME_SIZE).await? {
        None => Ok(None),
        Some(frame) if frame.frame_type == frame_type::MESSAGE => Ok(Some(frame.payload)),
        Some(_) => Err(invalid("expected a MESSAGE frame")),
    }
}

/// Reads a response that must be a lone STATUS frame, as for failures before dispatch.
pub async fn read_lone_status<R: RawRecv>(recv: &mut R) -> Result<Status> {
    match read_response_frame(recv).await? {
        Some(ResponseFrame::Status(status)) => Ok(status),
        other => Err(Error::from(Status::internal(format!(
            "expected a lone STATUS frame, got {other:?}"
        )))),
    }
}

/// A unary response read frame by frame: `[HEADERS] [MESSAGE] STATUS`.
#[derive(Debug)]
pub struct UnaryReply {
    pub headers: Metadata,
    pub messages: Vec<Vec<u8>>,
    pub status: Status,
}

/// Reads response frames through the terminal STATUS.
pub async fn read_unary_reply<R: RawRecv>(recv: &mut R) -> Result<UnaryReply> {
    let mut headers = Metadata::new();
    let mut messages = Vec::new();
    loop {
        match read_response_frame(recv).await? {
            Some(ResponseFrame::Headers(metadata)) => headers = metadata,
            Some(ResponseFrame::Message(body)) => messages.push(body.to_vec()),
            Some(ResponseFrame::Status(status)) => {
                return Ok(UnaryReply {
                    headers,
                    messages,
                    status,
                });
            }
            None => return Err(unexpected_eof()),
        }
    }
}

/// Writes raw bytes.
pub async fn write_raw<S: RawSend>(send: &mut S, bytes: &[u8]) -> Result<()> {
    send.write_all(bytes).await
}

/// Writes a REQUEST frame.
pub async fn write_request<S: RawSend>(send: &mut S, request: &RpcRequest) -> Result<()> {
    send.write_all(&framing::encode_protobuf_frame(
        frame_type::REQUEST,
        request,
        DEFAULT_MAX_FRAME_SIZE,
    )?)
    .await
}

/// Writes a MESSAGE frame.
pub async fn write_message<S: RawSend>(send: &mut S, body: &[u8]) -> Result<()> {
    send.write_all(&framing::encode_message_frame(
        body,
        DEFAULT_MAX_FRAME_SIZE,
    )?)
    .await
}

/// Writes a REQUEST frame and its first MESSAGE frame with one write.
pub async fn write_request_and_message<S: RawSend>(
    send: &mut S,
    request: &RpcRequest,
    body: &[u8],
) -> Result<()> {
    let mut bytes =
        framing::encode_protobuf_frame(frame_type::REQUEST, request, DEFAULT_MAX_FRAME_SIZE)?;
    bytes.extend(framing::encode_message_frame(body, DEFAULT_MAX_FRAME_SIZE)?);
    send.write_all(&bytes).await
}

/// Writes a STATUS frame.
pub async fn write_status<S: RawSend>(send: &mut S, status: Status) -> Result<()> {
    send.write_all(&framing::encode_protobuf_frame(
        frame_type::STATUS,
        &RpcStatus::from(status),
        DEFAULT_MAX_FRAME_SIZE,
    )?)
    .await
}

/// Writes a HEADERS frame.
pub async fn write_headers<S: RawSend>(send: &mut S, metadata: Metadata) -> Result<()> {
    send.write_all(&framing::encode_protobuf_frame(
        frame_type::HEADERS,
        &RpcHeaders {
            metadata,
            extensions: Vec::new(),
            dictionaries: Vec::new(),
        },
        DEFAULT_MAX_FRAME_SIZE,
    )?)
    .await
}

/// Writes a frame of an unknown (grease) type: type `0x1f * 3 + 0x21`.
pub async fn write_grease<S: RawSend>(send: &mut S, payload: &[u8]) -> Result<()> {
    send.write_all(&framing::encode_frame(
        0x1f * 3 + 0x21,
        payload,
        DEFAULT_MAX_FRAME_SIZE,
    )?)
    .await
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
    async fn call(
        &self,
        request: RpcRequest,
        body: Vec<u8>,
    ) -> Result<ResponseEnvelope<bytes::Bytes>> {
        let frames = self
            .streaming_call(request, trevrpc::stream::from_iter([body]))
            .await?;
        trevrpc::client::collect_unary_response(frames).await
    }
}

#[trevrpc::async_trait]
impl StreamingRpcTransport for WebTransportRpc {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<ResponseFrame>> {
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let upload = tokio::spawn(async move {
            let mut guard = ResetOnDrop {
                stream: send,
                finished: false,
            };
            let send = &mut guard.stream;
            write_request(send, &request).await?;
            while let Some(body) = request_body.next().await.transpose()? {
                write_message(send, &body).await?;
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
                let item = match read_response_frame(&mut recv).await {
                    Ok(Some(ResponseFrame::Status(status))) => {
                        state.finish();
                        let drained = read_response_frame(&mut recv).await;
                        match drained {
                            Ok(Some(_)) => Err(Error::from(Status::internal(
                                "response stream continued after terminal status",
                            ))),
                            Err(error) if status.is_ok() => Err(error),
                            Ok(None) | Err(_) => Ok(ResponseFrame::Status(status)),
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
