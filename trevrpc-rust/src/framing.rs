//! Frame encoding and the logical frames of a call.
//!
//! Every frame is a QUIC variable-length integer type, a QUIC variable-length integer payload
//! length, and the payload. See the wire format documentation for the grammar.

use std::any::TypeId;

use bytes::{BufMut, Bytes};
use prost::Message;

use crate::{Error, Metadata, Result, RpcHeaders, RpcRequest, RpcStatus, Status, varint};

/// The default maximum frame payload size, 4 MiB.
pub const DEFAULT_MAX_FRAME_SIZE: usize = 4 * 1024 * 1024;

/// The longest frame header: a type and a length, each an eight-byte variable-length integer.
pub const MAX_FRAME_HEADER_LEN: usize = 2 * varint::MAX_LEN;

/// Frame types defined by wire version 1. Every other type is skipped by receivers.
pub mod frame_type {
    /// A raw protobuf-encoded application message.
    pub const MESSAGE: u64 = 0;
    /// An `RpcRequest`.
    pub const REQUEST: u64 = 1;
    /// An `RpcHeaders`.
    pub const HEADERS: u64 = 2;
    /// An `RpcStatus`.
    pub const STATUS: u64 = 3;
}

/// Returns whether `frame_type` is defined by wire version 1.
#[must_use]
pub const fn is_known_frame_type(frame_type: u64) -> bool {
    frame_type <= frame_type::STATUS
}

/// A frame of a response stream, the currency between the transports and the call layer.
///
/// A well-formed response is `[Headers] Message* Status`: see the wire format documentation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResponseFrame {
    /// Response header metadata, sent at most once and before anything else.
    Headers(Metadata),
    /// One raw protobuf-encoded response message.
    Message(Bytes),
    /// The terminal status and trailing metadata.
    Status(Status),
}

impl ResponseFrame {
    /// Creates a message frame.
    #[must_use]
    pub fn message(body: impl Into<Bytes>) -> Self {
        Self::Message(body.into())
    }

    /// Returns whether this is the terminal frame.
    #[must_use]
    pub const fn is_status(&self) -> bool {
        matches!(self, Self::Status(_))
    }
}

/// Appends a frame header for a payload of `payload_len` bytes.
///
/// Fails with [`Error::FrameTooLarge`] when the payload exceeds `max_frame_size`.
pub fn put_frame_header(
    out: &mut impl BufMut,
    frame_type: u64,
    payload_len: usize,
    max_frame_size: usize,
) -> Result<()> {
    validate_payload_len(payload_len, max_frame_size)?;
    varint::push(out, frame_type)?;
    varint::push(out, u64::try_from(payload_len).unwrap_or(u64::MAX))
}

/// Appends a complete frame whose payload is the protobuf encoding of `message`.
pub fn put_protobuf_frame<M>(
    out: &mut impl BufMut,
    frame_type: u64,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    put_frame_header(out, frame_type, message.encoded_len(), max_frame_size)?;
    message.encode(out)?;
    Ok(())
}

/// Encodes a complete frame around `payload`.
pub fn encode_frame(frame_type: u64, payload: &[u8], max_frame_size: usize) -> Result<Vec<u8>> {
    let mut frame = Vec::with_capacity(MAX_FRAME_HEADER_LEN.saturating_add(payload.len()));
    put_frame_header(&mut frame, frame_type, payload.len(), max_frame_size)?;
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Encodes a complete MESSAGE frame around an already-encoded protobuf message.
pub fn encode_message_frame(body: &[u8], max_frame_size: usize) -> Result<Vec<u8>> {
    encode_frame(frame_type::MESSAGE, body, max_frame_size)
}

/// Encodes a complete frame whose payload is the protobuf encoding of `message`.
pub fn encode_protobuf_frame<M>(
    frame_type: u64,
    message: &M,
    max_frame_size: usize,
) -> Result<Vec<u8>>
where
    M: Message,
{
    let mut frame = Vec::with_capacity(MAX_FRAME_HEADER_LEN.saturating_add(message.encoded_len()));
    put_protobuf_frame(&mut frame, frame_type, message, max_frame_size)?;
    Ok(frame)
}

/// Decodes a protobuf frame payload, rejecting wire-type mismatches in known fields first.
pub fn decode_payload<M>(payload: &[u8]) -> Result<M>
where
    M: Message + Default + 'static,
{
    preflight_known_wire::<M>(payload)?;
    M::decode(payload).map_err(Error::from)
}

/// Checks a payload length against the maximum frame size.
pub const fn validate_payload_len(len: usize, max_frame_size: usize) -> Result<()> {
    if len > max_frame_size {
        return Err(Error::FrameTooLarge {
            len,
            max: max_frame_size,
        });
    }

    Ok(())
}

fn preflight_known_wire<M: 'static>(body: &[u8]) -> Result<()> {
    // Field 8 of `RpcRequest` and field 2 of `RpcHeaders` are repeated integers, which may be
    // packed (length-delimited) or not, so they are not pinned to one wire type here. Fields 2, 3
    // and 6 of `RpcRequest` are reserved and skipped like any unknown field.
    let (fields, metadata_field): (&[(u64, u64)], Option<u64>) =
        if TypeId::of::<M>() == TypeId::of::<RpcRequest>() {
            (&[(1, 2), (4, 2), (5, 0), (7, 0), (9, 0)], Some(4))
        } else if TypeId::of::<M>() == TypeId::of::<RpcHeaders>() {
            (&[(1, 2)], Some(1))
        } else if TypeId::of::<M>() == TypeId::of::<RpcStatus>() {
            (&[(1, 0), (2, 2), (3, 2)], Some(3))
        } else {
            return Ok(());
        };
    preflight_wire_message(body, fields, metadata_field)
}

fn preflight_wire_message(
    data: &[u8],
    fields: &[(u64, u64)],
    metadata_field: Option<u64>,
) -> Result<()> {
    let mut offset = 0;
    while offset < data.len() {
        let tag = consume_proto_varint(data, &mut offset)?;
        if tag == 0 {
            return Err(malformed_protobuf("invalid protobuf field tag"));
        }
        let field = tag >> 3;
        let wire_type = tag & 7;
        if let Some((_, expected)) = fields.iter().find(|(known, _)| *known == field)
            && wire_type != *expected
        {
            return Err(malformed_protobuf(
                "known protobuf field used the wrong wire type",
            ));
        }
        if metadata_field == Some(field) {
            let entry = consume_proto_bytes(data, &mut offset)?;
            preflight_wire_message(entry, &[(1, 2), (2, 2)], None)?;
        } else {
            skip_proto_value(data, &mut offset, field, wire_type)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum VarintDecodeError {
    Truncated,
    Overflow,
}

fn consume_u64_varint(
    data: &[u8],
    offset: &mut usize,
) -> std::result::Result<u64, VarintDecodeError> {
    let mut value = 0_u64;
    for index in 0_u32..10 {
        let Some(byte) = data.get(*offset).copied() else {
            return Err(VarintDecodeError::Truncated);
        };
        *offset = offset.saturating_add(1);

        if index == 9 && byte > 1 {
            return Err(VarintDecodeError::Overflow);
        }
        value |= u64::from(byte & 0x7f).wrapping_shl(index.saturating_mul(7));
        if byte < 0x80 {
            return Ok(value);
        }
    }

    Err(VarintDecodeError::Overflow)
}

fn consume_proto_varint(data: &[u8], offset: &mut usize) -> Result<u64> {
    consume_u64_varint(data, offset).map_err(|error| match error {
        VarintDecodeError::Truncated => malformed_protobuf("truncated protobuf varint"),
        VarintDecodeError::Overflow => malformed_protobuf("protobuf varint exceeded 64 bits"),
    })
}

fn consume_proto_bytes<'a>(data: &'a [u8], offset: &mut usize) -> Result<&'a [u8]> {
    let len = usize::try_from(consume_proto_varint(data, offset)?)
        .map_err(|_| malformed_protobuf("protobuf field length exceeded supported range"))?;
    let start = *offset;
    let end = start
        .checked_add(len)
        .ok_or_else(|| malformed_protobuf("protobuf field length overflowed"))?;
    let bytes = data
        .get(start..end)
        .ok_or_else(|| malformed_protobuf("truncated protobuf field"))?;
    *offset = end;
    Ok(bytes)
}

fn skip_proto_value(data: &[u8], offset: &mut usize, field: u64, wire_type: u64) -> Result<()> {
    match wire_type {
        0 => consume_proto_varint(data, offset).map(|_| ()),
        1 => skip_proto_fixed(data, offset, 8),
        2 => consume_proto_bytes(data, offset).map(|_| ()),
        3 => loop {
            let tag = consume_proto_varint(data, offset)?;
            if tag == 0 {
                return Err(malformed_protobuf("invalid protobuf group tag"));
            }
            let nested_field = tag >> 3;
            let nested_wire_type = tag & 7;
            if nested_wire_type == 4 {
                if nested_field != field {
                    return Err(malformed_protobuf("mismatched protobuf end group"));
                }
                break Ok(());
            }
            skip_proto_value(data, offset, nested_field, nested_wire_type)?;
        },
        4 => Err(malformed_protobuf("unexpected protobuf end group")),
        5 => skip_proto_fixed(data, offset, 4),
        _ => Err(malformed_protobuf("unsupported protobuf wire type")),
    }
}

fn skip_proto_fixed(data: &[u8], offset: &mut usize, len: usize) -> Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| malformed_protobuf("protobuf fixed field length overflowed"))?;
    if end > data.len() {
        return Err(malformed_protobuf("truncated protobuf fixed field"));
    }
    *offset = end;
    Ok(())
}

fn malformed_protobuf(message: &'static str) -> Error {
    Error::from(Status::invalid_argument(message))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::{
        DEFAULT_MAX_FRAME_SIZE, MAX_FRAME_HEADER_LEN, ResponseFrame, decode_payload, encode_frame,
        encode_message_frame, encode_protobuf_frame, frame_type, is_known_frame_type,
        put_frame_header, validate_payload_len,
    };
    use crate::{Code, Metadata, RpcHeaders, RpcKind, RpcRequest, RpcStatus, Status};

    #[test]
    fn frame_headers_use_minimal_varints() {
        let frame = encode_message_frame(b"hi", 1024).expect("frame should encode");
        assert_eq!(frame, [0x00, 0x02, b'h', b'i']);

        let payload = vec![0; 64];
        let frame = encode_frame(frame_type::MESSAGE, &payload, 1024).expect("frame should encode");
        assert_eq!(frame.get(..3), Some(&[0x00, 0x40, 0x40][..]));
        assert_eq!(frame.len(), 3 + 64);

        let frame = encode_frame(0x40, &[], 1024).expect("grease frame should encode");
        assert_eq!(frame, [0x40, 0x40, 0x00]);

        let mut header = BytesMut::new();
        put_frame_header(&mut header, 0x3fff_ffff, 0x4000, DEFAULT_MAX_FRAME_SIZE)
            .expect("wide header should encode");
        assert_eq!(header.len(), 4 + 4);
        assert!(header.len() <= MAX_FRAME_HEADER_LEN);
    }

    #[test]
    fn frame_size_limit_is_inclusive_and_checked_before_encoding() {
        assert!(encode_message_frame(&[0; 16], 16).is_ok());
        let error = encode_message_frame(&[0; 17], 16).expect_err("one over must fail");
        assert_eq!(error.into_status().code(), Code::ResourceExhausted);

        let mut header = BytesMut::new();
        assert!(put_frame_header(&mut header, frame_type::MESSAGE, 17, 16).is_err());
        assert!(header.is_empty(), "nothing is written for a rejected frame");

        assert!(validate_payload_len(0, 0).is_ok());
        assert!(validate_payload_len(1, 0).is_err());
    }

    #[test]
    fn protobuf_frames_wrap_the_encoded_message() {
        let status = RpcStatus::from(Status::unavailable("later"));
        let frame = encode_protobuf_frame(frame_type::STATUS, &status, 1024)
            .expect("status frame should encode");
        let payload = prost::Message::encode_to_vec(&status);
        assert_eq!(frame.first(), Some(&0x03));
        assert_eq!(
            frame.get(1),
            Some(&u8::try_from(payload.len()).expect("small payload"))
        );
        assert_eq!(frame.get(2..), Some(payload.as_slice()));
    }

    #[test]
    fn known_frame_types_are_exactly_the_defined_ones() {
        for known in 0..=3 {
            assert!(is_known_frame_type(known));
        }
        for unknown in [4, 0x21, 0x40, 0x5f, u64::MAX] {
            assert!(!is_known_frame_type(unknown));
        }
        // The grease types 0x1f * N + 0x21 are all unknown.
        for n in 0..1000_u64 {
            assert!(!is_known_frame_type(0x1f * n + 0x21));
        }
    }

    #[test]
    fn response_frames_expose_their_kind() {
        assert!(ResponseFrame::Status(Status::ok()).is_status());
        assert!(!ResponseFrame::Headers(Metadata::new()).is_status());
        assert!(!ResponseFrame::message(&b"m"[..]).is_status());
    }

    #[test]
    fn malformed_protobuf_payloads_are_invalid_argument() {
        for payload in [&[0xff][..], &[0xff, 0xff], &[0x0a, 0xff]] {
            let status = decode_payload::<RpcRequest>(payload)
                .expect_err("malformed protobuf should fail")
                .into_status();

            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[test]
    fn known_fields_with_the_wrong_wire_type_are_rejected() {
        // RpcRequest.method (field 1) as a varint.
        let status = decode_payload::<RpcRequest>(&[0x08, 0x01])
            .expect_err("wrong wire type should fail")
            .into_status();
        assert_eq!(status.code(), Code::InvalidArgument);

        // RpcRequest.datagram_flow (field 9) as bytes.
        let status = decode_payload::<RpcRequest>(&[0x4a, 0x01, 0x01])
            .expect_err("wrong wire type should fail")
            .into_status();
        assert_eq!(status.code(), Code::InvalidArgument);

        // RpcStatus.code (field 1) as bytes, RpcHeaders.metadata (field 1) as a varint.
        assert!(decode_payload::<RpcStatus>(&[0x0a, 0x00]).is_err());
        assert!(decode_payload::<RpcHeaders>(&[0x08, 0x01]).is_err());
    }

    #[test]
    fn the_reserved_fields_and_unknown_fields_are_ignored() {
        // Field 2 (formerly the method name, here with the wrong wire type), field 3 (formerly the
        // request body), field 6 (formerly the wire version), and field 99 are skipped.
        let mut payload = vec![0x10, 0x07, 0x1a, 0x02, b'x', b'y', 0x30, 0x01];
        payload.extend([0x9a, 0x06, 0x01, 0x00]);
        payload.extend([0x0a, 0x04, b'/', b's', b'/', b'm']);
        let request = decode_payload::<RpcRequest>(&payload).expect("request should decode");
        assert_eq!(request.method, "/s/m");
        assert_eq!(request.rpc_kind(), RpcKind::Unary);
    }

    #[test]
    fn extensions_decode_packed_or_unpacked_and_are_ignored() {
        let packed = decode_payload::<RpcHeaders>(&[0x12, 0x02, 0x01, 0xac])
            .expect_err("a truncated packed varint is malformed");
        assert_eq!(packed.into_status().code(), Code::InvalidArgument);

        let packed = decode_payload::<RpcHeaders>(&[0x12, 0x03, 0x01, 0xac, 0x02])
            .expect("packed extensions should decode");
        assert_eq!(packed.extensions, [1, 300]);
        let unpacked = decode_payload::<RpcHeaders>(&[0x10, 0x01, 0x10, 0xac, 0x02])
            .expect("unpacked extensions should decode");
        assert_eq!(unpacked.extensions, [1, 300]);
        let request = decode_payload::<RpcRequest>(&[0x42, 0x03, 0x01, 0xac, 0x02])
            .expect("request extensions should decode");
        assert_eq!(request.extensions, [1, 300]);
    }

    #[test]
    fn metadata_maps_are_structurally_validated() {
        // A metadata entry whose value uses the wrong wire type.
        let status = decode_payload::<RpcStatus>(&[0x1a, 0x04, 0x0a, 0x01, b'k', 0x10])
            .expect_err("malformed metadata entry should fail")
            .into_status();
        assert_eq!(status.code(), Code::InvalidArgument);
    }
}
