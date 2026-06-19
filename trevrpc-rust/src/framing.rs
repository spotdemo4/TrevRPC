use prost::Message;

use crate::{Error, Result};

const FRAME_HEADER_LEN: usize = 4;

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

/// Decodes a protobuf message from a `TrevRPC` frame body.
pub fn decode_frame<M>(body: &[u8]) -> Result<M>
where
    M: Message + Default,
{
    M::decode(body).map_err(Error::from)
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
    use crate::{Code, RpcRequest};

    use super::{decode_frame, encode_frame, frame_body_len};

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
}
