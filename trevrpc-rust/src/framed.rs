//! Reading and writing frames on a byte stream.
//!
//! The reader decodes the HTTP/3-style frame header (a QUIC varint type and length), checks the
//! declared length against the maximum frame size before allocating anything, and skips frames of
//! unknown type. The writer assembles whole batches of frames and writes them with one call.

use std::io;
use std::sync::Arc;

use crate::client::replay::{Optimism, Replay};
use crate::compression::{
    Accept, ClientCompression, Dict, Inflater, MessageEncoder, StreamEnable, inflate,
};
use crate::connection_metadata::OfferedSet;
use crate::framing::{
    self, MAX_FRAME_HEADER_LEN, ResponseFrame, decode_payload, frame_type, is_known_frame_type,
};
use crate::wire::{DictionaryInfo, StreamCompression};
use crate::{BoxStream, Error, Result, RpcHeaders, RpcRequest, RpcStatus, Status, varint};
use bytes::{Bytes, BytesMut};
use futures_util::{FutureExt, StreamExt};
use prost::Message;

pub const MESSAGE_FRAME_BATCH: usize = 32;

/// Bytes of an unknown frame's payload discarded per read.
const DISCARD_CHUNK: usize = 4096;

pub trait FrameWrite {
    fn write_frame_chunks(
        &mut self,
        chunks: &mut [Bytes],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub trait FrameRead: Send + Sized {
    fn read_frame_bytes(
        &mut self,
        bytes: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>>> + Send;

    /// Reads exactly `len` bytes, the payload of a frame. The stream ending first is an
    /// unexpected end.
    ///
    /// A stream that holds the bytes already can hand them over without copying.
    fn read_payload_bytes(
        &mut self,
        len: usize,
    ) -> impl std::future::Future<Output = Result<Bytes>> + Send {
        read_payload_copy(self, len)
    }
}

/// Reads `len` bytes into a new buffer.
async fn read_payload_copy<R: FrameRead>(recv: &mut R, len: usize) -> Result<Bytes> {
    let mut payload = vec![0; len];
    read_exact_body(recv, &mut payload).await?;
    Ok(Bytes::from(payload))
}

/// A byte stream that delivers its bytes in chunks.
pub trait ChunkSource: Send {
    /// The next chunk, or `None` at the end of the stream.
    fn next_chunk(&mut self) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send;

    /// Reads into `bytes`, as [`FrameRead::read_frame_bytes`] does.
    fn read_into(
        &mut self,
        bytes: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>>> + Send;
}

/// Reads at most this many bytes from the chunk buffer even when it is empty; larger reads go to
/// the stream directly.
const BUFFERED_READ_MAX: usize = 16;

/// A [`ChunkSource`] read as frames.
///
/// A frame is a few header bytes and a payload. Reading each of them from the stream costs a
/// round of locking and bookkeeping in the transport, which for small messages dominates reading
/// them. This reader instead takes whole chunks from the stream and serves headers and payloads
/// from the chunk, so one read of the stream serves as many frames as the chunk holds, and a
/// payload inside the chunk is handed out without being copied.
pub struct Chunked<S> {
    pub(crate) source: S,
    /// The part of the latest chunk that has not been read.
    pending: Bytes,
}

impl<S> Chunked<S> {
    pub const fn new(source: S) -> Self {
        Self {
            source,
            pending: Bytes::new(),
        }
    }
}

impl<S: ChunkSource> FrameRead for Chunked<S> {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        if self.pending.is_empty() {
            if bytes.len() > BUFFERED_READ_MAX {
                return self.source.read_into(bytes).await;
            }
            match self.source.next_chunk().await? {
                Some(chunk) => self.pending = chunk,
                None => return Ok(None),
            }
        }
        let len = bytes.len().min(self.pending.len());
        let (head, _) = bytes.split_at_mut(len);
        head.copy_from_slice(&self.pending.split_to(len));
        Ok(Some(len))
    }

    async fn read_payload_bytes(&mut self, len: usize) -> Result<Bytes> {
        if self.pending.len() >= len {
            return Ok(self.pending.split_to(len));
        }
        read_payload_copy(self, len).await
    }
}

// ---------------------------------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------------------------------

/// The largest MESSAGE payload that is copied next to its header. Larger payloads are written
/// as chunks of their own, so they are never copied.
const COALESCE_MAX: usize = 2048;

/// Accumulates frames as write chunks.
///
/// Frame headers, protobuf payloads and small MESSAGE payloads share one scratch allocation: the
/// transport then has one chunk to queue and packetize for a whole batch of small messages
/// instead of two per message. Larger MESSAGE payloads are chained as their own chunk.
pub struct FrameChunks {
    scratch: BytesMut,
    chunks: Vec<Bytes>,
    max_frame_size: usize,
    /// The answer to a dictionary query that the next HEADERS frame carries.
    answer: Option<Vec<DictionaryInfo>>,
    /// The answer to a stream compression offer that the next HEADERS frame carries.
    stream_answer: Option<StreamCompression>,
    /// The connection metadata set the next HEADERS frame confirms, or zero.
    connection_answer: u32,
}

impl FrameChunks {
    /// Creates a writer whose scratch buffer holds `scratch` bytes without growing.
    pub fn with_capacity(max_frame_size: usize, scratch: usize) -> Self {
        Self {
            scratch: BytesMut::with_capacity(scratch),
            chunks: Vec::new(),
            max_frame_size,
            answer: None,
            stream_answer: None,
            connection_answer: 0,
        }
    }

    /// The scratch bytes a MESSAGE frame with a payload of `len` bytes needs.
    pub const fn scratch_len(len: usize) -> usize {
        if len <= COALESCE_MAX {
            len.saturating_add(MAX_FRAME_HEADER_LEN)
        } else {
            MAX_FRAME_HEADER_LEN
        }
    }

    fn seal_scratch(&mut self) {
        if !self.scratch.is_empty() {
            self.chunks.push(self.scratch.split().freeze());
        }
    }

    pub fn protobuf<M: Message>(&mut self, frame_type: u64, message: &M) -> Result<()> {
        framing::put_protobuf_frame(&mut self.scratch, frame_type, message, self.max_frame_size)
    }

    /// Adds a message frame, which `encoder` compresses as its direction may.
    pub fn message(&mut self, body: Bytes, encoder: &mut MessageEncoder) -> Result<()> {
        // A message is checked against the frame size limit uncompressed, which limits the
        // message the receiver decompresses.
        framing::validate_payload_len(body.len(), self.max_frame_size)?;
        let (frame_type, body) = encoder.encode(body);
        framing::put_frame_header(
            &mut self.scratch,
            frame_type,
            body.len(),
            self.max_frame_size,
        )?;
        if body.len() <= COALESCE_MAX {
            self.scratch.extend_from_slice(&body);
        } else {
            self.seal_scratch();
            self.chunks.push(body);
        }
        Ok(())
    }

    pub fn response_frame(
        &mut self,
        frame: ResponseFrame,
        encoder: &mut MessageEncoder,
    ) -> Result<()> {
        match frame {
            ResponseFrame::Message(body) => self.message(body, encoder),
            ResponseFrame::Headers(metadata) => {
                let headers = RpcHeaders {
                    metadata,
                    extensions: Vec::new(),
                    dictionaries: self.answer.take().unwrap_or_default(),
                    stream_compression: self.stream_answer.take(),
                    connection_metadata: std::mem::take(&mut self.connection_answer),
                };
                self.protobuf(frame_type::HEADERS, &headers)
            }
            ResponseFrame::Status(status) => {
                self.protobuf(frame_type::STATUS, &RpcStatus::from(status))
            }
        }
    }

    pub async fn write<W: FrameWrite>(mut self, send: &mut W) -> Result<()> {
        self.seal_scratch();
        send.write_frame_chunks(&mut self.chunks).await
    }
}

/// What a HEADERS or STATUS frame is expected to need in the scratch buffer. Larger frames grow
/// it.
const FRAME_ESTIMATE: usize = 64;

/// How a server call writes its response frames.
#[derive(Debug, Default)]
pub struct ResponseCodec {
    /// How the response messages are compressed.
    pub(crate) encoder: MessageEncoder,
    /// The answer to the request's dictionary query, which the call's HEADERS carry.
    pub(crate) answer: Option<Vec<DictionaryInfo>>,
    /// The answer to the request's stream compression offer, which the call's HEADERS carry.
    pub(crate) stream_answer: Option<StreamCompression>,
    /// The connection metadata set the request offered and the server stored, which the call's
    /// HEADERS confirm; zero for none.
    pub(crate) connection_answer: u32,
}

/// Writes response frames, draining `frames`, with one write.
pub async fn write_response_frames<W: FrameWrite>(
    send: &mut W,
    frames: &mut Vec<ResponseFrame>,
    max_frame_size: usize,
    codec: &mut ResponseCodec,
) -> Result<()> {
    let scratch = frames.iter().fold(0_usize, |total, frame| {
        total.saturating_add(match frame {
            ResponseFrame::Message(body) => FrameChunks::scratch_len(body.len()),
            ResponseFrame::Headers(_) | ResponseFrame::Status(_) => FRAME_ESTIMATE,
        })
    });
    let mut chunks = FrameChunks::with_capacity(max_frame_size, scratch);
    if frames
        .iter()
        .any(|frame| matches!(frame, ResponseFrame::Headers(_)))
    {
        chunks.answer = codec.answer.take();
        chunks.stream_answer = codec.stream_answer.take();
        chunks.connection_answer = std::mem::take(&mut codec.connection_answer);
    }
    for frame in frames.drain(..) {
        chunks.response_frame(frame, &mut codec.encoder)?;
    }
    chunks.write(send).await
}

/// Writes a REQUEST frame followed by `messages` with one write, the messages encoded by
/// `encoder`.
pub async fn write_request<W: FrameWrite>(
    send: &mut W,
    request: &RpcRequest,
    messages: Vec<Bytes>,
    max_frame_size: usize,
    encoder: &mut MessageEncoder,
) -> Result<()> {
    let scratch = messages.iter().fold(
        MAX_FRAME_HEADER_LEN.saturating_add(request.encoded_len()),
        |total, message| total.saturating_add(FrameChunks::scratch_len(message.len())),
    );
    let mut chunks = FrameChunks::with_capacity(max_frame_size, scratch);
    chunks.protobuf(frame_type::REQUEST, request)?;
    for message in messages {
        chunks.message(message, encoder)?;
    }
    chunks.write(send).await
}

/// Writes a REQUEST frame and the one request message of a unary call with one write,
/// compressing the message with `compress` when set.
pub async fn write_unary_request<W: FrameWrite>(
    send: &mut W,
    request: &RpcRequest,
    message: Bytes,
    max_frame_size: usize,
    compress: Option<&Arc<Dict>>,
) -> Result<()> {
    let mut encoder = MessageEncoder::new(compress.cloned());
    write_request(send, request, vec![message], max_frame_size, &mut encoder).await
}

/// Writes `messages` as message frames with one write, draining them, encoded by `encoder`.
async fn write_message_frames<W: FrameWrite>(
    send: &mut W,
    messages: &mut Vec<Bytes>,
    max_frame_size: usize,
    encoder: &mut MessageEncoder,
) -> Result<()> {
    let scratch = messages.iter().fold(0_usize, |total, message| {
        total.saturating_add(FrameChunks::scratch_len(message.len()))
    });
    let mut chunks = FrameChunks::with_capacity(max_frame_size, scratch);
    for message in messages.drain(..) {
        chunks.message(message, encoder)?;
    }
    chunks.write(send).await
}

/// Moves the request messages that are ready right now into `batch`.
///
/// Returns whether the request stream has ended.
fn take_ready_messages(
    request_body: &mut BoxStream<Vec<u8>>,
    batch: &mut Vec<Bytes>,
) -> Result<bool> {
    while batch.len() < MESSAGE_FRAME_BATCH {
        match request_body.next().now_or_never() {
            Some(Some(Ok(body))) => batch.push(Bytes::from(body)),
            Some(Some(Err(error))) => return Err(error),
            Some(None) => return Ok(true),
            None => return Ok(false),
        }
    }
    Ok(false)
}

/// Writes a call's request half: the REQUEST frame, then every request message.
///
/// The REQUEST frame and the messages that are already available (the one message of a unary or
/// server-streaming call) share the first write. Messages are encoded by `encoder`.
pub async fn write_request_stream<W: FrameWrite>(
    send: &mut W,
    request: &RpcRequest,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
    encoder: &mut MessageEncoder,
) -> Result<()> {
    let mut batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    let mut done = take_ready_messages(request_body, &mut batch)?;
    write_request(
        send,
        request,
        std::mem::take(&mut batch),
        max_frame_size,
        encoder,
    )
    .await?;

    while !done {
        let Some(body) = request_body.next().await.transpose()? else {
            return Ok(());
        };
        batch.push(Bytes::from(body));
        done = take_ready_messages(request_body, &mut batch)?;
        write_message_frames(send, &mut batch, max_frame_size, encoder).await?;
    }
    Ok(())
}

/// Writes a call's request half like [`write_request_stream`], sending each request message as a
/// datagram when it fits.
///
/// The REQUEST frame is written alone and flushed first, as the first datagram may not precede it.
/// A message that cannot travel as a datagram is written to the stream, after the datagrams of
/// its batch.
pub async fn write_request_stream_datagrams<W: FrameWrite>(
    send: &mut W,
    request: &RpcRequest,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
    datagrams: &crate::datagram::FlowSender,
    encoder: &mut MessageEncoder,
) -> Result<()> {
    write_request(send, request, Vec::new(), max_frame_size, encoder).await?;

    let mut batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    let mut on_stream = Vec::new();
    let mut done = false;
    while !done {
        let Some(body) = request_body.next().await.transpose()? else {
            break;
        };
        batch.push(Bytes::from(body));
        done = take_ready_messages(request_body, &mut batch)?;
        // The upload is dropped to cancel the call, which also ends a send that waits.
        datagrams
            .send_batch(&mut batch, &mut on_stream, std::future::pending())
            .await;
        if !on_stream.is_empty() {
            write_message_frames(send, &mut on_stream, max_frame_size, encoder).await?;
        }
    }
    // The caller finishes the stream next, and its FIN must not overtake the datagrams.
    datagrams.flushed(std::future::pending()).await;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------------------------

/// A decoded frame header.
pub struct FrameHeader {
    pub frame_type: u64,
    pub payload_len: usize,
    /// Bytes the header occupied on the wire.
    pub header_len: usize,
}

impl FrameHeader {
    /// Bytes the whole frame occupies on the wire.
    const fn wire_len(&self) -> usize {
        self.header_len.saturating_add(self.payload_len)
    }
}

/// Reads one frame header, or `None` at a clean end of stream before it.
///
/// The declared payload length is checked against `max_frame_size` here, before any payload is
/// read or allocated. Both integers may use any valid encoding.
pub async fn read_frame_header<R: FrameRead>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<FrameHeader>> {
    // Every frame has at least two header bytes, and most have exactly two.
    let mut head = [0_u8; 2];
    if !read_exact_or_eof(recv, &mut head).await? {
        return Ok(None);
    }
    let [first, second] = head;

    let (frame_type, length, header_len) = if varint::len_from_first_byte(first) == 1 {
        let frame_type = u64::from(first);
        let (length, length_len) = if varint::len_from_first_byte(second) == 1 {
            (u64::from(second), 1)
        } else {
            varint::read_continued(recv, &[second]).await?
        };
        (frame_type, length, length_len.saturating_add(1))
    } else {
        let (frame_type, type_len) = varint::read_continued(recv, &head).await?;
        let (length, length_len) = varint::read(recv).await?.ok_or_else(unexpected_eof)?;
        (frame_type, length, type_len.saturating_add(length_len))
    };

    let payload_len = usize::try_from(length).unwrap_or(usize::MAX);
    framing::validate_payload_len(payload_len, max_frame_size)?;
    Ok(Some(FrameHeader {
        frame_type,
        payload_len,
        header_len,
    }))
}

/// Reads the next frame header of a defined type, discarding frames of any other type.
///
/// Each discarded frame's wire length is passed to `on_skipped`, which may fail the read, so a
/// caller can bound the bytes unknown frames consume.
pub async fn next_known_header<R, F>(
    recv: &mut R,
    max_frame_size: usize,
    on_skipped: &mut F,
) -> Result<Option<FrameHeader>>
where
    R: FrameRead,
    F: FnMut(usize) -> Result<()> + Send,
{
    loop {
        let Some(header) = read_frame_header(recv, max_frame_size).await? else {
            return Ok(None);
        };
        if is_known_frame_type(header.frame_type) {
            return Ok(Some(header));
        }
        discard_payload(recv, header.payload_len).await?;
        on_skipped(header.wire_len())?;
    }
}

/// Reads a frame's payload.
pub async fn read_payload<R: FrameRead>(recv: &mut R, len: usize) -> Result<Bytes> {
    if len == 0 {
        return Ok(Bytes::new());
    }

    recv.read_payload_bytes(len).await
}

/// Discards a frame's payload without buffering it.
async fn discard_payload<R: FrameRead>(recv: &mut R, len: usize) -> Result<()> {
    // Unknown frames are rare, so the buffer lives on the heap: on the stack it would sit in every
    // future that reads frames, which are copied whenever a task is spawned or moved.
    let mut buffer = vec![0_u8; len.min(DISCARD_CHUNK)];
    let mut remaining = len;
    while remaining > 0 {
        let chunk = buffer
            .get_mut(..remaining.min(DISCARD_CHUNK))
            .ok_or_else(unexpected_eof)?;
        match recv.read_frame_bytes(chunk).await? {
            Some(read) => remaining = remaining.saturating_sub(read),
            None => return Err(unexpected_eof()),
        }
    }
    Ok(())
}

/// How a client call reads its response frames.
#[derive(Debug, Default)]
pub struct ResponseReading {
    /// The dictionaries the call accepts on compressed messages.
    pub(crate) accept: Accept,
    /// The connection to teach the answer in the call's HEADERS, when the request queried.
    pub(crate) learn: Option<Arc<ClientCompression>>,
    /// The decompressing state of stream-compressed responses, when the request offered them.
    pub(crate) stream: Option<Inflater>,
    /// The request direction to enable when the call's HEADERS answer the stream compression
    /// offer.
    pub(crate) enable_requests: Option<Arc<StreamEnable>>,
    /// The requests of an optimistic call, until its HEADERS or STATUS decide it.
    pub(crate) replay: Option<Replay>,
    /// Set when the server refused the optimistic call, which the client then replays.
    pub(crate) refused: Option<Replay>,
    /// The connection to tell when HEADERS answer a stream compression offer.
    pub(crate) optimism: Option<Arc<Optimism>>,
    /// The connection metadata set the request offered, which its HEADERS may confirm.
    pub(crate) connection_metadata: Option<OfferedSet>,
}

/// Decodes a frame received on a response stream, decompressing a `ZSTD_MESSAGE` with the
/// dictionaries `reading` accepts and a `ZSTD_STREAM_MESSAGE` with its stream state, and learning
/// the answers of its HEADERS.
///
/// A REQUEST frame has no place on a response stream.
pub fn decode_response_frame(
    frame_type: u64,
    payload: Bytes,
    reading: &mut ResponseReading,
    max_frame_size: usize,
) -> Result<ResponseFrame> {
    match frame_type {
        frame_type::MESSAGE => Ok(ResponseFrame::Message(payload)),
        frame_type::ZSTD_MESSAGE => inflate(&reading.accept, &payload, max_frame_size)
            .map(ResponseFrame::Message)
            .map_err(|rejected| rejected.into_error(Status::internal)),
        frame_type::ZSTD_STREAM_MESSAGE => reading
            .stream
            .as_mut()
            .ok_or_else(|| Error::from(Status::internal(NOT_STREAM_COMPRESSED)))?
            .decode(&payload, max_frame_size)
            .map(ResponseFrame::Message)
            .map_err(|rejected| rejected.into_error(Status::internal)),
        frame_type::HEADERS => {
            let headers = decode_payload::<RpcHeaders>(&payload)?;
            if let Some(connection) = reading.learn.take() {
                connection.learn(&headers.dictionaries);
            }
            if let Some(offered) = reading.connection_metadata.take() {
                offered.learn(headers.connection_metadata);
            }
            if let Some(requests) = reading.enable_requests.take() {
                requests.enable(headers.stream_compression.as_ref());
            }
            if let Some(replay) = reading.replay.take() {
                replay.settle();
            }
            if headers.stream_compression.is_some()
                && let Some(optimism) = &reading.optimism
            {
                optimism.answered();
            }
            Ok(ResponseFrame::Headers(headers.metadata))
        }
        frame_type::STATUS => {
            let status = decode_payload::<RpcStatus>(&payload)?;
            // Only a STATUS before any HEADERS of an optimistic call can refuse it.
            if let Some(replay) = reading.replay.take() {
                if status.refused_stream_compression {
                    if let Some(optimism) = &reading.optimism {
                        optimism.refused();
                    }
                    reading.refused = Some(replay);
                } else {
                    replay.settle();
                }
            }
            Ok(ResponseFrame::Status(Status::from(status)))
        }
        _ => Err(Error::from(Status::internal(
            "response stream contained a REQUEST frame",
        ))),
    }
}

/// Reads the next frame of a response stream, skipping unknown frames, or `None` at a clean end
/// of stream.
pub async fn read_response_frame<R: FrameRead>(
    recv: &mut R,
    max_frame_size: usize,
    reading: &mut ResponseReading,
) -> Result<Option<ResponseFrame>> {
    let Some(header) = next_known_header(recv, max_frame_size, &mut |_| Ok(())).await? else {
        return Ok(None);
    };
    if header.frame_type == frame_type::REQUEST {
        return decode_response_frame(header.frame_type, Bytes::new(), reading, max_frame_size)
            .map(Some);
    }
    let payload = read_payload(recv, header.payload_len).await?;
    decode_response_frame(header.frame_type, payload, reading, max_frame_size).map(Some)
}

/// A `ZSTD_STREAM_MESSAGE` in a direction whose receiver did not enable stream compression.
const NOT_STREAM_COMPRESSED: &str =
    "stream-compressed message where stream compression is not enabled";

/// A frame that follows the REQUEST frame on a request stream.
pub enum RequestFrame {
    Message(Bytes),
    Status(Status),
}

/// Reads the REQUEST frame that opens a request stream, skipping unknown frames.
///
/// Any other known frame first is a protocol error; the stream ending first is an unexpected EOF.
pub async fn read_request<R, F>(
    recv: &mut R,
    max_frame_size: usize,
    mut on_skipped: F,
) -> Result<RpcRequest>
where
    R: FrameRead,
    F: FnMut(usize) -> Result<()> + Send,
{
    let header = next_known_header(recv, max_frame_size, &mut on_skipped)
        .await?
        .ok_or_else(unexpected_eof)?;
    if header.frame_type != frame_type::REQUEST {
        return Err(invalid_argument(
            "request stream must begin with a REQUEST frame",
        ));
    }
    let payload = read_payload(recv, header.payload_len).await?;
    decode_payload::<RpcRequest>(&payload)
}

/// Reads the next frame after REQUEST on a request stream, skipping unknown frames, or `None` at
/// a clean end of stream.
///
/// A second REQUEST or a HEADERS frame is a protocol error, rejected before its payload is read. A
/// `ZSTD_MESSAGE` is decompressed with the dictionaries `accept` holds, and a
/// `ZSTD_STREAM_MESSAGE` with `stream`, the direction's state when the server enabled it.
pub async fn read_request_frame<R, F>(
    recv: &mut R,
    max_frame_size: usize,
    accept: &Accept,
    stream: Option<&mut Inflater>,
    mut on_skipped: F,
) -> Result<Option<RequestFrame>>
where
    R: FrameRead,
    F: FnMut(usize) -> Result<()> + Send,
{
    let Some(header) = next_known_header(recv, max_frame_size, &mut on_skipped).await? else {
        return Ok(None);
    };
    match header.frame_type {
        frame_type::MESSAGE => {
            let payload = read_payload(recv, header.payload_len).await?;
            Ok(Some(RequestFrame::Message(payload)))
        }
        frame_type::ZSTD_MESSAGE => {
            let payload = read_payload(recv, header.payload_len).await?;
            let message = inflate(accept, &payload, max_frame_size)
                .map_err(|rejected| rejected.into_error(Status::invalid_argument))?;
            Ok(Some(RequestFrame::Message(message)))
        }
        frame_type::ZSTD_STREAM_MESSAGE => {
            let Some(stream) = stream else {
                return Err(Error::from(Status::invalid_argument(NOT_STREAM_COMPRESSED)));
            };
            let payload = read_payload(recv, header.payload_len).await?;
            let message = stream
                .decode(&payload, max_frame_size)
                .map_err(|rejected| rejected.into_error(Status::invalid_argument))?;
            Ok(Some(RequestFrame::Message(message)))
        }
        frame_type::STATUS => {
            let payload = read_payload(recv, header.payload_len).await?;
            let status = decode_payload::<RpcStatus>(&payload)?;
            Ok(Some(RequestFrame::Status(Status::from(status))))
        }
        frame_type::REQUEST => Err(invalid_argument(
            "request stream contained a second REQUEST",
        )),
        _ => Err(invalid_argument("request stream contained a HEADERS frame")),
    }
}

/// Reads to the end of a request stream, skipping unknown frames.
///
/// A known frame is a protocol error described by `unexpected`; it is rejected before its payload
/// is read.
pub async fn read_request_end<R, F>(
    recv: &mut R,
    max_frame_size: usize,
    mut on_skipped: F,
    unexpected: &'static str,
) -> Result<()>
where
    R: FrameRead,
    F: FnMut(usize) -> Result<()> + Send,
{
    match next_known_header(recv, max_frame_size, &mut on_skipped).await? {
        Some(_) => Err(invalid_argument(unexpected)),
        None => Ok(()),
    }
}

/// Requires the response stream to end after its terminal status, skipping unknown frames.
pub async fn drain_fin_after_terminal_status<R: FrameRead>(
    recv: &mut R,
    max_frame_size: usize,
    stream_name: &'static str,
) -> Result<()> {
    if next_known_header(recv, max_frame_size, &mut |_| Ok(()))
        .await?
        .is_some()
    {
        return Err(Error::from(Status::internal(format!(
            "{stream_name} continued after terminal status"
        ))));
    }

    Ok(())
}

fn invalid_argument(message: &'static str) -> Error {
    Error::from(Status::invalid_argument(message))
}

async fn read_exact_body<R: FrameRead>(recv: &mut R, buf: &mut [u8]) -> Result<()> {
    if read_exact_or_eof(recv, buf).await? {
        Ok(())
    } else {
        Err(unexpected_eof())
    }
}

/// Fills `buf`, returning `false` if the stream ended before the first byte.
pub async fn read_exact_or_eof<R: FrameRead>(recv: &mut R, buf: &mut [u8]) -> Result<bool> {
    let total = buf.len();
    let mut remaining = buf;

    while !remaining.is_empty() {
        match recv.read_frame_bytes(remaining).await? {
            Some(0) => {}
            Some(read) => {
                remaining = std::mem::take(&mut remaining)
                    .split_at_mut_checked(read)
                    .map_or_else(Default::default, |(_, rest)| rest);
            }
            None if remaining.len() == total => return Ok(false),
            None => return Err(unexpected_eof()),
        }
    }

    Ok(true)
}

pub fn unexpected_eof() -> Error {
    Error::transport(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "stream ended in the middle of a frame",
    ))
}

impl FrameWrite for crate::quic::QuicSendStream {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.stream
            .write_all_chunks(chunks)
            .await
            .map_err(Error::transport)
    }
}

impl ChunkSource for quinn::RecvStream {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        let chunk = self
            .read_chunk(usize::MAX, true)
            .await
            .map_err(Error::transport)?;
        Ok(chunk.map(|chunk| chunk.bytes))
    }

    async fn read_into(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::Chunked;
    use super::{
        FrameChunks, RequestFrame, ResponseCodec, ResponseReading, drain_fin_after_terminal_status,
        read_frame_header, read_request, read_request_end, read_request_frame, read_response_frame,
    };
    use crate::compression::{Accept, MessageEncoder};
    use crate::framing::{
        DEFAULT_MAX_FRAME_SIZE, ResponseFrame, encode_frame, encode_message_frame,
        encode_protobuf_frame, frame_type,
    };
    use crate::test_io::{Chunks, Script, Sink};
    use crate::{Code, Metadata, RpcHeaders, RpcRequest, RpcStatus, Status};

    fn status_frame(status: Status) -> Vec<u8> {
        encode_protobuf_frame(
            frame_type::STATUS,
            &RpcStatus::from(status),
            DEFAULT_MAX_FRAME_SIZE,
        )
        .expect("status frame should encode")
    }

    fn grease(payload: usize) -> Vec<u8> {
        encode_frame(0x21, &vec![7; payload], DEFAULT_MAX_FRAME_SIZE)
            .expect("grease frame should encode")
    }

    #[tokio::test]
    async fn writer_assembles_a_batch_into_one_write() {
        let mut headers = Metadata::new();
        headers.insert("x-a".to_owned(), b"1".to_vec());
        let mut frames = vec![
            ResponseFrame::Headers(headers.clone()),
            ResponseFrame::message(&b"hi"[..]),
            ResponseFrame::message(Bytes::new()),
            ResponseFrame::Status(Status::ok()),
        ];
        let mut sink = Sink::default();
        super::write_response_frames(
            &mut sink,
            &mut frames,
            DEFAULT_MAX_FRAME_SIZE,
            &mut ResponseCodec::default(),
        )
        .await
        .expect("frames should write");

        assert!(frames.is_empty());
        assert_eq!(sink.0.len(), 1, "one write per batch");
        let mut wire = Script::new(sink.0.remove(0));
        for expected in [
            ResponseFrame::Headers(headers),
            ResponseFrame::message(&b"hi"[..]),
            ResponseFrame::message(Bytes::new()),
            ResponseFrame::Status(Status::ok()),
        ] {
            assert_eq!(
                read_response_frame(&mut wire, 1024, &mut ResponseReading::default())
                    .await
                    .expect("frame should read"),
                Some(expected)
            );
        }
        assert!(
            read_response_frame(&mut wire, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
                .is_none()
        );
    }

    fn message_frame(body: &[u8]) -> Vec<u8> {
        encode_message_frame(body, DEFAULT_MAX_FRAME_SIZE).expect("message frame should encode")
    }

    #[tokio::test]
    async fn chunked_reader_serves_many_frames_from_one_chunk() {
        let wire = [
            message_frame(b"one"),
            message_frame(&[7; 100]),
            status_frame(Status::ok()),
        ]
        .concat();
        let mut recv = Chunked::new(Chunks::new([Bytes::from(wire)]));

        for expected in [
            ResponseFrame::message(&b"one"[..]),
            ResponseFrame::message(vec![7; 100]),
            ResponseFrame::Status(Status::ok()),
        ] {
            assert_eq!(
                read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                    .await
                    .unwrap(),
                Some(expected)
            );
        }
        assert!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
                .is_none()
        );
        // One read for the chunk, one that finds the end.
        assert_eq!(recv.source.chunk_reads, 2);
        assert_eq!(recv.source.direct_reads, 0);
    }

    #[tokio::test]
    async fn chunked_reader_hands_out_a_payload_inside_the_chunk_without_copying() {
        let wire = Bytes::from(message_frame(b"payload"));
        let memory = wire.as_ptr_range();
        let mut recv = Chunked::new(Chunks::new([wire]));

        let Some(ResponseFrame::Message(body)) =
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
        else {
            panic!("a message frame was written");
        };

        assert_eq!(&body[..], b"payload");
        assert!(
            memory.contains(&body.as_ptr()),
            "the payload shares the chunk"
        );
    }

    #[tokio::test]
    async fn chunked_reader_assembles_frames_that_span_chunks() {
        let body = vec![9_u8; 300];
        let wire = [message_frame(&body), message_frame(b"after")].concat();
        // Split inside the header, inside the payload, and between frames.
        let cuts = [1, 2, 150, 305];
        let mut chunks = Vec::new();
        let mut start = 0;
        for cut in cuts {
            chunks.push(Bytes::copy_from_slice(&wire[start..cut]));
            start = cut;
        }
        chunks.push(Bytes::copy_from_slice(&wire[start..]));
        let mut recv = Chunked::new(Chunks::new(chunks));

        assert_eq!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::message(body))
        );
        assert_eq!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::message(&b"after"[..]))
        );
        assert!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            recv.source.direct_reads > 0,
            "the payload tail is read directly"
        );
    }

    #[tokio::test]
    async fn chunked_reader_reports_a_stream_that_ends_inside_a_frame() {
        let mut wire = message_frame(&[1; 50]);
        wire.truncate(20);
        let mut recv = Chunked::new(Chunks::new([Bytes::from(wire)]));

        let error = read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
            .await
            .expect_err("a truncated frame must fail");

        assert_eq!(error.into_status().code(), Code::Unavailable);
    }

    #[test]
    fn writer_chains_large_messages_and_joins_small_ones() {
        let large = Bytes::from(vec![9_u8; super::COALESCE_MAX + 1]);
        let mut chunks = FrameChunks::with_capacity(DEFAULT_MAX_FRAME_SIZE, 0);
        chunks
            .message(Bytes::from_static(b"a"), &mut MessageEncoder::default())
            .unwrap();
        chunks
            .message(Bytes::from_static(b"b"), &mut MessageEncoder::default())
            .unwrap();
        chunks
            .message(large.clone(), &mut MessageEncoder::default())
            .unwrap();
        chunks
            .message(Bytes::from_static(b"c"), &mut MessageEncoder::default())
            .unwrap();
        chunks.seal_scratch();

        // The small frames and the large frame's header, the large payload, the last frame.
        assert_eq!(chunks.chunks.len(), 3);
        assert_eq!(
            chunks.chunks.get(1).map(|chunk| chunk.as_ptr()),
            Some(large.as_ptr()),
            "a large message is chained, not copied"
        );
    }

    #[test]
    fn writer_rejects_oversized_messages_before_writing() {
        let mut chunks = FrameChunks::with_capacity(3, 0);
        let error = chunks
            .message(Bytes::from_static(b"four"), &mut MessageEncoder::default())
            .expect_err("oversized message must fail");
        assert_eq!(error.into_status().code(), Code::ResourceExhausted);
        assert!(chunks.chunks.is_empty() && chunks.scratch.is_empty());
    }

    #[tokio::test]
    async fn request_and_first_message_share_one_write() {
        let request = RpcRequest::new("/svc/m");
        let mut sink = Sink::default();
        let mut body = crate::stream::from_iter([b"hi".to_vec()]);
        super::write_request_stream(
            &mut sink,
            &request,
            &mut body,
            DEFAULT_MAX_FRAME_SIZE,
            &mut MessageEncoder::default(),
        )
        .await
        .expect("request should write");

        assert_eq!(sink.0.len(), 1);
        let mut expected =
            encode_protobuf_frame(frame_type::REQUEST, &request, DEFAULT_MAX_FRAME_SIZE).unwrap();
        expected.extend(encode_message_frame(b"hi", DEFAULT_MAX_FRAME_SIZE).unwrap());
        assert_eq!(sink.0.remove(0), expected);
    }

    #[tokio::test]
    async fn headers_decode_with_any_varint_widths() {
        // MESSAGE with a 2-byte length, then HEADERS with a 2-byte type and 4-byte length.
        let wire = [
            &[0x00, 0x40, 0x02, b'h', b'i'][..],
            &[0x40, 0x02, 0x80, 0x00, 0x00, 0x00],
            &[0x03, 0x00],
        ]
        .concat();
        let mut recv = Script::new(wire);
        assert_eq!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::message(&b"hi"[..]))
        );
        assert_eq!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::Headers(Metadata::new()))
        );
        assert_eq!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::Status(Status::ok()))
        );
        assert!(
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unknown_and_grease_frames_are_skipped_anywhere_on_a_response() {
        let mut wire = grease(5);
        wire.extend(
            encode_protobuf_frame(
                frame_type::HEADERS,
                &RpcHeaders {
                    metadata: Metadata::new(),
                    extensions: vec![1],
                    ..RpcHeaders::default()
                },
                1024,
            )
            .unwrap(),
        );
        wire.extend(encode_frame(0x1234, b"unknown", 1024).unwrap());
        wire.extend(encode_message_frame(b"m", 1024).unwrap());
        wire.extend(grease(0));
        wire.extend(status_frame(Status::unavailable("down")));
        wire.extend(grease(9));
        let mut recv = Script::new(wire);

        let mut frames = Vec::new();
        while let Some(frame) =
            read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .unwrap()
        {
            frames.push(frame);
        }
        assert_eq!(
            frames,
            [
                ResponseFrame::Headers(Metadata::new()),
                ResponseFrame::message(&b"m"[..]),
                ResponseFrame::Status(Status::unavailable("down")),
            ]
        );
    }

    #[tokio::test]
    async fn oversized_length_is_rejected_before_the_payload_is_read() {
        for frame_type in [frame_type::MESSAGE, 0x21] {
            // Declares 17 bytes against a limit of 16 and provides none of them.
            let mut recv = Script::with_read_size(vec![u8::try_from(frame_type).unwrap(), 17], 1);
            let error = read_frame_header(&mut recv, 16)
                .await
                .err()
                .expect("oversized frame must be rejected");
            assert_eq!(error.into_status().code(), Code::ResourceExhausted);
            assert_eq!(recv.served, 2, "only the header may be consumed");
        }

        // A 62-bit length cannot even be represented as an allocation.
        let mut recv = Script::new(vec![0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        let error = read_frame_header(&mut recv, DEFAULT_MAX_FRAME_SIZE)
            .await
            .err()
            .expect("huge length must be rejected");
        assert_eq!(error.into_status().code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn a_frame_exactly_at_the_limit_is_accepted() {
        let mut recv = Script::new(encode_message_frame(&[9; 16], 16).unwrap());
        assert_eq!(
            read_response_frame(&mut recv, 16, &mut ResponseReading::default())
                .await
                .unwrap(),
            Some(ResponseFrame::message(vec![9; 16]))
        );
    }

    #[tokio::test]
    async fn stream_ending_inside_a_frame_is_unexpected_eof() {
        let whole = encode_message_frame(b"payload", 1024).unwrap();
        for cut in 1..whole.len() {
            let mut recv = Script::new(whole.get(..cut).unwrap().to_vec());
            let error = read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                .await
                .expect_err("a truncated frame must fail");
            assert_eq!(
                error.into_status().code(),
                Code::Unavailable,
                "cut at {cut}"
            );
        }
        // A cut inside a multi-byte header.
        for wire in [&[0x40][..], &[0x00, 0x40], &[0x80, 0x00, 0x00, 0x21, 0x40]] {
            let mut recv = Script::new(wire.to_vec());
            assert!(
                read_response_frame(&mut recv, 1024, &mut ResponseReading::default())
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn unknown_frames_report_their_wire_length() {
        let mut wire = grease(10);
        wire.extend(encode_frame(0x40, &[1, 2, 3], 1024).unwrap());
        wire.extend(encode_message_frame(b"m", 1024).unwrap());
        let mut recv = Script::new(wire);
        let mut skipped = Vec::new();
        let frame = read_request_frame(&mut recv, 1024, &Accept::None, None, |len| {
            skipped.push(len);
            Ok(())
        })
        .await
        .unwrap();

        // 1 type + 1 length + 10 payload, then 2 type + 1 length + 3 payload.
        assert_eq!(skipped, [12, 6]);
        assert!(matches!(frame, Some(RequestFrame::Message(body)) if body == b"m"[..]));
    }

    #[tokio::test]
    async fn skipped_frames_can_fail_the_read() {
        let mut recv = Script::new([grease(10), grease(10)].concat());
        let error = read_request_frame(&mut recv, 1024, &Accept::None, None, |_| {
            Err(Status::resource_exhausted("too much").into())
        })
        .await
        .err()
        .expect("budget failure should surface");
        assert_eq!(error.into_status().code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn request_streams_reject_misplaced_known_frames() {
        let request =
            encode_protobuf_frame(frame_type::REQUEST, &RpcRequest::new("/svc/m"), 1024).unwrap();
        let headers = encode_frame(frame_type::HEADERS, &[], 1024).unwrap();
        let message = encode_message_frame(b"m", 1024).unwrap();

        // The first known frame must be REQUEST.
        for wire in [&headers, &message, &status_frame(Status::ok())] {
            let error = read_request(&mut Script::new(wire.clone()), 1024, |_| Ok(()))
                .await
                .expect_err("non-REQUEST first frame must fail");
            assert_eq!(error.into_status().code(), Code::InvalidArgument);
        }
        assert_eq!(
            read_request(&mut Script::new(request.clone()), 1024, |_| Ok(()))
                .await
                .unwrap(),
            RpcRequest::new("/svc/m")
        );

        // Afterwards only MESSAGE and STATUS may follow.
        for wire in [&request, &headers] {
            let error = read_request_frame(
                &mut Script::new(wire.clone()),
                1024,
                &Accept::None,
                None,
                |_| Ok(()),
            )
            .await
            .err()
            .expect("REQUEST and HEADERS must be rejected");
            assert_eq!(error.into_status().code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn request_end_rejects_known_frames_without_reading_their_payload() {
        let mut recv = Script::with_read_size(
            [grease(1), vec![frame_type::MESSAGE.try_into().unwrap(), 9]].concat(),
            1,
        );
        let error = read_request_end(&mut recv, 1024, |_| Ok(()), "extra data")
            .await
            .expect_err("a known frame must be rejected");
        assert_eq!(error.into_status().message(), "extra data");
        assert_eq!(recv.served, 5, "the 9 payload bytes were never read");

        let mut recv = Script::new(grease(4));
        read_request_end(&mut recv, 1024, |_| Ok(()), "extra data")
            .await
            .expect("unknown frames are tolerated");
    }

    #[tokio::test]
    async fn only_unknown_frames_may_follow_a_terminal_status() {
        let mut recv = Script::new([grease(3), grease(0)].concat());
        drain_fin_after_terminal_status(&mut recv, 1024, "response stream")
            .await
            .expect("unknown frames after STATUS are accepted");

        let mut recv = Script::new(encode_message_frame(b"late", 1024).unwrap());
        let error = drain_fin_after_terminal_status(&mut recv, 1024, "response stream")
            .await
            .expect_err("a known frame after STATUS is a protocol error");
        assert_eq!(error.into_status().code(), Code::Internal);
    }

    #[tokio::test]
    async fn request_frames_on_a_response_stream_are_internal_errors() {
        let wire =
            encode_protobuf_frame(frame_type::REQUEST, &RpcRequest::new("/s/m"), 1024).unwrap();
        let error = read_response_frame(
            &mut Script::new(wire),
            1024,
            &mut ResponseReading::default(),
        )
        .await
        .expect_err("REQUEST has no place on a response stream");
        assert_eq!(error.into_status().code(), Code::Internal);
    }
}
