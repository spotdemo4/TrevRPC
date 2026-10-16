use std::io;

use crate::framing::{
    STREAM_FRAME_BODY_TAG, decode_frame, decode_stream_frame_body_owned, encode_frame_with_max,
    encode_message_stream_frame_prefix, frame_body_len,
};
use crate::{BoxStream, Error, Result, RpcStreamFrame, RpcStreamFrameKind, Status};
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use prost::Message;

pub(crate) const MESSAGE_FRAME_BATCH: usize = 32;

pub(crate) trait FrameWrite {
    fn write_frame_bytes(
        &mut self,
        bytes: &[u8],
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    fn write_frame_chunks(
        &mut self,
        chunks: &mut [Bytes],
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

pub(crate) trait FrameRead {
    fn read_frame_bytes(
        &mut self,
        bytes: &mut [u8],
    ) -> impl std::future::Future<Output = Result<Option<usize>>> + Send;
}

pub(crate) async fn write_frame<W, M>(
    send: &mut W,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    W: FrameWrite,
    M: Message,
{
    let frame = encode_frame_with_max(message, max_frame_size)?;
    send.write_frame_bytes(&frame).await
}

pub(crate) async fn write_stream_frame<W>(
    send: &mut W,
    frame: RpcStreamFrame,
    max_frame_size: usize,
) -> Result<()>
where
    W: FrameWrite,
{
    if is_plain_message_frame(&frame) {
        write_message_stream_frame::<W>(send, frame.body, max_frame_size).await
    } else {
        write_frame::<W, _>(send, &frame, max_frame_size).await
    }
}

pub(crate) async fn write_message_stream_frame<W>(
    send: &mut W,
    body: Vec<u8>,
    max_frame_size: usize,
) -> Result<()>
where
    W: FrameWrite,
{
    let mut prefix = Vec::new();
    encode_message_stream_frame_prefix(body.len(), max_frame_size, &mut prefix)?;
    if body.is_empty() {
        return send.write_frame_bytes(&prefix).await;
    }

    send.write_frame_chunks(&mut [Bytes::from(prefix), Bytes::from(body)])
        .await
}

pub(crate) async fn write_message_stream_frames<W>(
    send: &mut W,
    bodies: &mut Vec<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()>
where
    W: FrameWrite,
{
    let mut chunks = Vec::with_capacity(bodies.len().saturating_mul(2));
    let mut prefix = Vec::new();
    for body in bodies.drain(..) {
        encode_message_stream_frame_prefix(body.len(), max_frame_size, &mut prefix)?;
        chunks.push(Bytes::copy_from_slice(&prefix));
        if !body.is_empty() {
            chunks.push(Bytes::from(body));
        }
    }

    send.write_frame_chunks(&mut chunks).await
}

pub(crate) async fn write_request_body_frames<W>(
    send: &mut W,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()>
where
    W: FrameWrite,
{
    let mut batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        batch.clear();
        let Some(body) = request_body.next().await.transpose()? else {
            return Ok(());
        };
        batch.push(body);

        let mut done = false;
        while batch.len() < MESSAGE_FRAME_BATCH {
            match request_body.next().now_or_never() {
                Some(Some(Ok(body))) => batch.push(body),
                Some(Some(Err(error))) => return Err(error),
                Some(None) => {
                    done = true;
                    break;
                }
                None => break,
            }
        }

        write_message_stream_frames::<W>(send, &mut batch, max_frame_size).await?;
        if done {
            return Ok(());
        }
    }
}

pub(crate) fn is_plain_message_frame(frame: &RpcStreamFrame) -> bool {
    frame.frame_kind() == Some(RpcStreamFrameKind::Message)
        && frame.status == crate::Code::Ok.as_u32()
        && frame.message.is_empty()
        && frame.metadata.is_empty()
}

pub(crate) async fn read_frame<R, M>(recv: &mut R, max_frame_size: usize) -> Result<M>
where
    R: FrameRead,
    M: Message + Default + 'static,
{
    let body = read_raw_frame_or_eof(recv, max_frame_size)
        .await?
        .ok_or_else(unexpected_eof)?;

    decode_frame(&body)
}

pub(crate) async fn read_raw_frame_or_eof<R>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<Vec<u8>>>
where
    R: FrameRead,
{
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }

    let len = frame_body_len(header, max_frame_size)?;
    read_body(recv, len).await.map(Some)
}

pub(crate) async fn read_stream_frame_or_eof<R>(
    recv: &mut R,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>>
where
    R: FrameRead,
{
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }

    let len = frame_body_len(header, max_frame_size)?;
    if len == 0 {
        let frame = RpcStreamFrame::message(Vec::new());
        return Ok(Some(frame));
    }

    let mut prefix = [0_u8; 11];
    read_exact_body(recv, &mut prefix[..1]).await?;
    if prefix[0] != STREAM_FRAME_BODY_TAG {
        let body = read_body_with_prefix(recv, len, &prefix[..1]).await?;
        let frame = decode_stream_frame_body_owned(body)?;
        return Ok(Some(frame));
    }

    let mut value = 0_u64;
    let mut prefix_len = 1;
    for shift in (0..64).step_by(7) {
        if prefix_len == len {
            let body = prefix[..prefix_len].to_vec();
            return decode_stream_frame_body_owned(body).map(Some);
        }

        read_exact_body(recv, &mut prefix[prefix_len..=prefix_len]).await?;
        let byte = prefix[prefix_len];
        prefix_len += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            let body_len = usize::try_from(value).map_err(|_| {
                Error::from(Status::invalid_argument(
                    "stream frame field length exceeded supported range",
                ))
            })?;
            if prefix_len.checked_add(body_len) == Some(len) {
                let frame = RpcStreamFrame::message(read_body(recv, body_len).await?);
                return Ok(Some(frame));
            }

            let body = read_body_with_prefix(recv, len, &prefix[..prefix_len]).await?;
            let frame = decode_stream_frame_body_owned(body)?;
            return Ok(Some(frame));
        }
    }

    let body = read_body_with_prefix(recv, len, &prefix[..prefix_len]).await?;
    let frame = decode_stream_frame_body_owned(body)?;
    Ok(Some(frame))
}

pub(crate) async fn drain_fin_after_terminal_status<R>(
    recv: &mut R,
    max_frame_size: usize,
    stream_name: &'static str,
) -> Result<()>
where
    R: FrameRead,
{
    if read_stream_frame_or_eof::<R>(recv, max_frame_size)
        .await?
        .is_some()
    {
        return Err(Error::from(Status::internal(format!(
            "{stream_name} continued after terminal status"
        ))));
    }

    Ok(())
}

pub(crate) async fn drain_unary_request_end<R>(recv: &mut R) -> Result<()>
where
    R: FrameRead,
{
    let mut buf = [0; 1024];
    loop {
        match recv.read_frame_bytes(&mut buf).await? {
            Some(0) => {}
            Some(_) => {
                return Err(Error::from(Status::invalid_argument(
                    "unary request stream contained data after the initial request frame",
                )));
            }
            None => return Ok(()),
        }
    }
}

async fn read_body<R>(recv: &mut R, len: usize) -> Result<Vec<u8>>
where
    R: FrameRead,
{
    if len == 0 {
        return Ok(Vec::new());
    }

    let mut body = vec![0; len];
    read_exact_body(recv, &mut body).await?;

    Ok(body)
}

async fn read_body_with_prefix<R>(recv: &mut R, len: usize, prefix: &[u8]) -> Result<Vec<u8>>
where
    R: FrameRead,
{
    let remaining = len.checked_sub(prefix.len()).ok_or_else(|| {
        Error::from(Status::invalid_argument(
            "stream frame prefix exceeded frame length",
        ))
    })?;
    let mut body = Vec::with_capacity(len);
    body.extend_from_slice(prefix);
    body.resize(len, 0);
    read_exact_body(recv, &mut body[prefix.len()..prefix.len() + remaining]).await?;

    Ok(body)
}

async fn read_exact_body<R>(recv: &mut R, buf: &mut [u8]) -> Result<()>
where
    R: FrameRead,
{
    let mut offset = 0;
    while offset < buf.len() {
        match recv.read_frame_bytes(&mut buf[offset..]).await? {
            Some(0) => {}
            Some(read) => offset += read,
            None => return Err(unexpected_eof()),
        }
    }

    Ok(())
}

async fn read_exact_or_eof<R>(recv: &mut R, buf: &mut [u8]) -> Result<bool>
where
    R: FrameRead,
{
    let mut offset = 0;

    while offset < buf.len() {
        match recv.read_frame_bytes(&mut buf[offset..]).await? {
            Some(0) => {}
            Some(read) => offset += read,
            None if offset == 0 => return Ok(false),
            None => return Err(unexpected_eof()),
        }
    }

    Ok(true)
}

fn unexpected_eof() -> Error {
    Error::transport(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "stream ended in the middle of a frame",
    ))
}

impl FrameWrite for quinn::SendStream {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.write_all_chunks(chunks)
            .await
            .map_err(Error::transport)
    }
}

impl FrameRead for quinn::RecvStream {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}
