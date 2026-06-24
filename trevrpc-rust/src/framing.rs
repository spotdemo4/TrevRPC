use prost::Message;

use crate::{Code, Error, Metadata, Result, RpcStreamFrame, RpcStreamFrameKind, Status};

const FRAME_HEADER_LEN: usize = 4;
const STREAM_FRAME_BODY_TAG: u8 = 4 << 3 | 2;

pub const DEFAULT_MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

/// Encodes a protobuf message with the default `TrevRPC` frame size limit.
pub fn encode_frame<M>(message: &M) -> Result<Vec<u8>>
where
    M: Message,
{
    encode_frame_with_max(message, DEFAULT_MAX_FRAME_SIZE)
}

/// Encodes a protobuf message into a length-prefixed `TrevRPC` frame.
pub fn encode_frame_with_max<M>(message: &M, max_frame_size: usize) -> Result<Vec<u8>>
where
    M: Message,
{
    let encoded_len = message.encoded_len();

    if encoded_len > max_frame_size {
        return Err(Error::FrameTooLarge {
            len: encoded_len,
            max: max_frame_size,
        });
    }

    let frame_len = u32::try_from(encoded_len).map_err(|_| Error::FrameTooLarge {
        len: encoded_len,
        max: max_frame_size,
    })?;

    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + encoded_len);
    frame.extend_from_slice(&frame_len.to_be_bytes());
    message.encode(&mut frame)?;

    Ok(frame)
}

/// Encodes one stream message frame carrying an already-encoded protobuf body.
pub fn encode_message_stream_frame(body: &[u8], max_frame_size: usize) -> Result<Vec<u8>> {
    let body_len = message_stream_frame_body_len(body.len());
    check_frame_body_len(body_len, max_frame_size)?;

    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + body_len);
    frame.extend_from_slice(&frame_len(body_len, max_frame_size)?.to_be_bytes());
    append_message_stream_frame_body(&mut frame, body);

    Ok(frame)
}

/// Encodes multiple stream message frames into one contiguous write buffer.
pub fn encode_message_stream_frames(bodies: &[Vec<u8>], max_frame_size: usize) -> Result<Vec<u8>> {
    let mut total_len = 0_usize;
    for body in bodies {
        let body_len = message_stream_frame_body_len(body.len());
        check_frame_body_len(body_len, max_frame_size)?;
        total_len = total_len.saturating_add(FRAME_HEADER_LEN + body_len);
    }

    let mut frames = Vec::with_capacity(total_len);
    for body in bodies {
        let body_len = message_stream_frame_body_len(body.len());
        frames.extend_from_slice(&frame_len(body_len, max_frame_size)?.to_be_bytes());
        append_message_stream_frame_body(&mut frames, body);
    }

    Ok(frames)
}

/// Decodes a streaming RPC frame body using a fast path for common message frames.
pub fn decode_stream_frame_body(body: &[u8]) -> Result<RpcStreamFrame> {
    if body.is_empty() {
        return Ok(RpcStreamFrame::message(Vec::new()));
    }

    let mut offset = 0;
    let mut kind = RpcStreamFrameKind::Message as i32;
    let mut status = Code::Ok.as_u32();
    let mut message = String::new();
    let mut frame_body = Vec::new();
    let metadata = Metadata::new();

    while offset < body.len() {
        let tag = consume_varint(body, &mut offset)?;
        if tag == 0 {
            return Err(invalid_stream_frame("invalid stream frame field tag"));
        }

        let field = tag >> 3;
        let wire_type = tag & 0x7;
        match field {
            1 => {
                require_wire_type(wire_type, 0, "invalid stream frame kind wire type")?;
                kind = i32::try_from(consume_varint(body, &mut offset)?).map_err(|_| {
                    invalid_stream_frame("stream frame kind exceeded supported range")
                })?;
            }
            2 => {
                require_wire_type(wire_type, 0, "invalid stream frame status wire type")?;
                status = u32::try_from(consume_varint(body, &mut offset)?).map_err(|_| {
                    invalid_stream_frame("stream frame status exceeded supported range")
                })?;
            }
            3 => {
                require_wire_type(wire_type, 2, "invalid stream frame message wire type")?;
                let value = consume_length_delimited(body, &mut offset)?;
                message = String::from_utf8(value.to_vec()).map_err(|_| {
                    invalid_stream_frame("stream frame message was not valid UTF-8")
                })?;
            }
            4 => {
                require_wire_type(wire_type, 2, "invalid stream frame body wire type")?;
                frame_body = consume_length_delimited(body, &mut offset)?.to_vec();
            }
            5 => return decode_frame::<RpcStreamFrame>(body),
            _ => skip_proto_field(body, &mut offset, wire_type)?,
        }
    }

    if RpcStreamFrameKind::try_from(kind).is_err() {
        return Err(invalid_stream_frame(
            "stream frame contained an unknown frame kind",
        ));
    }

    Ok(RpcStreamFrame {
        kind,
        status,
        message,
        body: frame_body,
        metadata,
    })
}

/// Decodes a protobuf message from a `TrevRPC` frame body.
pub fn decode_frame<M>(body: &[u8]) -> Result<M>
where
    M: Message + Default,
{
    M::decode(body).map_err(Error::from)
}

fn check_frame_body_len(len: usize, max_frame_size: usize) -> Result<()> {
    if len > max_frame_size {
        return Err(Error::FrameTooLarge {
            len,
            max: max_frame_size,
        });
    }

    Ok(())
}

fn frame_len(len: usize, max_frame_size: usize) -> Result<u32> {
    u32::try_from(len).map_err(|_| Error::FrameTooLarge {
        len,
        max: max_frame_size,
    })
}

fn message_stream_frame_body_len(body_len: usize) -> usize {
    if body_len == 0 {
        return 0;
    }

    1 + varint_len_usize(body_len) + body_len
}

fn append_message_stream_frame_body(frame: &mut Vec<u8>, body: &[u8]) {
    if body.is_empty() {
        return;
    }

    frame.push(STREAM_FRAME_BODY_TAG);
    append_varint_usize(frame, body.len());
    frame.extend_from_slice(body);
}

fn append_varint_usize(data: &mut Vec<u8>, mut value: usize) {
    while value >= 0x80 {
        let byte = u8::try_from(value & 0x7f).expect("masked varint byte should fit in u8");
        data.push(byte | 0x80);
        value >>= 7;
    }
    data.push(u8::try_from(value).expect("terminal varint byte should fit in u8"));
}

fn varint_len_usize(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len
}

fn consume_varint(data: &[u8], offset: &mut usize) -> Result<u64> {
    let mut value = 0_u64;
    for shift in (0..64).step_by(7) {
        let Some(byte) = data.get(*offset).copied() else {
            return Err(invalid_stream_frame("truncated stream frame varint"));
        };
        *offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte < 0x80 {
            return Ok(value);
        }
    }

    Err(invalid_stream_frame("stream frame varint exceeded 64 bits"))
}

fn require_wire_type(actual: u64, expected: u64, message: &'static str) -> Result<()> {
    if actual != expected {
        return Err(invalid_stream_frame(message));
    }

    Ok(())
}

fn consume_length_delimited<'a>(data: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = usize::try_from(consume_varint(data, offset)?)
        .map_err(|_| invalid_stream_frame("stream frame field length exceeded supported range"))?;
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_stream_frame("stream frame field length overflowed"))?;
    if end > data.len() {
        return Err(invalid_stream_frame("truncated stream frame field"));
    }

    let start = *offset;
    *offset = end;
    Ok(&data[start..end])
}

fn skip_proto_field(data: &[u8], offset: &mut usize, wire_type: u64) -> Result<()> {
    match wire_type {
        0 => {
            let _ = consume_varint(data, offset)?;
            Ok(())
        }
        1 => skip_fixed(data, offset, 8),
        2 => {
            let _ = consume_length_delimited(data, offset)?;
            Ok(())
        }
        5 => skip_fixed(data, offset, 4),
        _ => Err(invalid_stream_frame(
            "stream frame contained an unsupported wire type",
        )),
    }
}

fn skip_fixed(data: &[u8], offset: &mut usize, len: usize) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| invalid_stream_frame("stream frame fixed field length overflowed"))?;
    if end > data.len() {
        return Err(invalid_stream_frame("truncated stream frame fixed field"));
    }
    *offset = end;
    Ok(())
}

fn invalid_stream_frame(message: &'static str) -> Error {
    Error::from(Status::invalid_argument(message))
}

/// Decodes and validates the body length stored in a `TrevRPC` frame header.
pub fn frame_body_len(header: [u8; FRAME_HEADER_LEN], max_frame_size: usize) -> Result<usize> {
    let len = usize::try_from(u32::from_be_bytes(header)).map_err(|_| Error::FrameTooLarge {
        len: usize::MAX,
        max: max_frame_size,
    })?;

    if len > max_frame_size {
        return Err(Error::FrameTooLarge {
            len,
            max: max_frame_size,
        });
    }

    Ok(len)
}

#[cfg(test)]
mod tests {
    use crate::{Code, RpcRequest, RpcStreamFrame, Status};

    use super::{
        decode_frame, decode_stream_frame_body, encode_frame, encode_message_stream_frame,
        encode_message_stream_frames, frame_body_len,
    };

    #[test]
    fn round_trips_a_frame() {
        let request = RpcRequest::new("hello.Greeter", "SayHello", b"trev".to_vec());
        let frame = encode_frame(&request).expect("request should encode");
        let header = [frame[0], frame[1], frame[2], frame[3]];
        let len = frame_body_len(header, 1024).expect("frame length should decode");
        let decoded = decode_frame::<RpcRequest>(&frame[4..]).expect("request should decode");

        assert_eq!(len, frame.len() - 4);
        assert_eq!(decoded.service, request.service);
        assert_eq!(decoded.method, request.method);
        assert_eq!(decoded.body, request.body);
        assert_eq!(decoded.metadata, request.metadata);
    }

    #[test]
    fn frame_body_len_boundary_cases_are_stable() {
        for length in [0_u32, 1, 15, 16] {
            let decoded =
                frame_body_len(length.to_be_bytes(), 16).expect("in-range length should decode");
            assert_eq!(decoded, length as usize);
        }

        for length in [17_u32, u32::MAX] {
            let error = frame_body_len(length.to_be_bytes(), 16)
                .expect_err("oversized length should be rejected before body allocation");
            assert_eq!(error.into_status().code(), Code::ResourceExhausted);
        }
    }

    #[test]
    fn malformed_protobuf_frame_bodies_are_invalid_argument() {
        for body in [&[0xff][..], &[0xff, 0xff], &[0x0a, 0xff]] {
            let status = decode_frame::<RpcRequest>(body)
                .expect_err("malformed protobuf should fail")
                .into_status();

            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[test]
    fn stream_message_fast_path_matches_protobuf_encoding() {
        let body = b"hello".to_vec();
        let fast = encode_message_stream_frame(&body, 1024).expect("frame should encode");
        let generic = encode_frame(&RpcStreamFrame::message(body.clone()))
            .expect("generic frame should encode");

        assert_eq!(fast, generic);

        let decoded = decode_stream_frame_body(&fast[4..]).expect("frame should decode");
        assert_eq!(
            decoded.frame_kind(),
            Some(crate::RpcStreamFrameKind::Message)
        );
        assert_eq!(decoded.body, body);
    }

    #[test]
    fn empty_stream_message_fast_path_matches_protobuf_encoding() {
        let fast = encode_message_stream_frame(&[], 1024).expect("frame should encode");
        let generic = encode_frame(&RpcStreamFrame::message(Vec::new()))
            .expect("generic frame should encode");

        assert_eq!(fast, generic);

        let decoded = decode_stream_frame_body(&fast[4..]).expect("frame should decode");
        assert_eq!(
            decoded.frame_kind(),
            Some(crate::RpcStreamFrameKind::Message)
        );
        assert!(decoded.body.is_empty());
    }

    #[test]
    fn batched_stream_message_fast_path_concatenates_frames() {
        let bodies = vec![b"left".to_vec(), b"right".to_vec()];
        let batched = encode_message_stream_frames(&bodies, 1024).expect("frames should encode");
        let mut expected = Vec::new();
        for body in &bodies {
            expected.extend(
                encode_message_stream_frame(body, 1024).expect("single frame should encode"),
            );
        }

        assert_eq!(batched, expected);
    }

    #[test]
    fn stream_frame_fast_decoder_accepts_status_frames() {
        let frame = RpcStreamFrame::status(Status::unavailable("later"));
        let encoded = encode_frame(&frame).expect("status should encode");
        let decoded = decode_stream_frame_body(&encoded[4..]).expect("status should decode");

        assert_eq!(decoded.frame_kind(), frame.frame_kind());
        assert_eq!(decoded.status, frame.status);
        assert_eq!(decoded.message, frame.message);
    }
}
