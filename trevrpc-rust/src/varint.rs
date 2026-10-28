//! QUIC variable-length integers (RFC 9000, section 16).
//!
//! The two most significant bits of the first byte give the encoded length (1, 2, 4, or 8 bytes);
//! the remaining bits hold the value in network byte order.

use bytes::BufMut;

use crate::framed::{FrameRead, read_exact_or_eof, unexpected_eof};
use crate::{Result, Status};

/// The largest value a QUIC variable-length integer carries.
pub const MAX: u64 = (1 << 62) - 1;

/// The longest encoding of a QUIC variable-length integer.
pub const MAX_LEN: usize = 8;

/// Returns the length in bytes of the minimal encoding of `value`.
///
/// Values above [`MAX`] have no encoding; they report the longest length.
#[must_use]
pub const fn encoded_len(value: u64) -> usize {
    if value < 1 << 6 {
        1
    } else if value < 1 << 14 {
        2
    } else if value < 1 << 30 {
        4
    } else {
        MAX_LEN
    }
}

/// Returns the length in bytes of the encoding that starts with `first`.
#[must_use]
pub const fn len_from_first_byte(first: u8) -> usize {
    match first >> 6 {
        0 => 1,
        1 => 2,
        2 => 4,
        _ => MAX_LEN,
    }
}

/// Appends the minimal encoding of `value`.
///
/// Fails when `value` exceeds [`MAX`], in which case nothing is written.
pub fn push(out: &mut impl BufMut, value: u64) -> Result<()> {
    let overflow = || crate::Error::from(Status::internal("varint exceeds 2^62 - 1"));
    match encoded_len(value) {
        1 => out.put_u8(u8::try_from(value).map_err(|_| overflow())?),
        2 => out.put_u16(u16::try_from(value).map_err(|_| overflow())? | 0x4000),
        4 => out.put_u32(u32::try_from(value).map_err(|_| overflow())? | 0x8000_0000),
        _ if value <= MAX => out.put_u64(value | 0xc000_0000_0000_0000),
        _ => return Err(overflow()),
    }
    Ok(())
}

/// Decodes the variable-length integer at the start of `buf`.
///
/// Returns the value and the number of bytes it occupied, or `None` when `buf` ends inside the
/// integer. Any valid encoding is accepted, including non-minimal ones.
#[must_use]
pub fn decode(buf: &[u8]) -> Option<(u64, usize)> {
    let first = *buf.first()?;
    let len = len_from_first_byte(first);
    let bytes = buf.get(..len)?;
    let mut value = u64::from(first & 0x3f);
    for byte in bytes.get(1..)? {
        value = value.wrapping_shl(8) | u64::from(*byte);
    }
    Some((value, len))
}

/// Reads one variable-length integer, returning `None` at a clean end of stream before its
/// first byte.
///
/// A stream that ends inside the integer is an unexpected-EOF transport error.
pub async fn read<R: FrameRead>(recv: &mut R) -> Result<Option<(u64, usize)>> {
    let mut first = [0_u8; 1];
    if !read_exact_or_eof(recv, &mut first).await? {
        return Ok(None);
    }
    read_continued(recv, &first).await.map(Some)
}

/// Completes a variable-length integer whose first bytes are already in `prefix`.
///
/// Returns the value and the total number of bytes it occupied. `prefix` must not be longer than
/// the integer; a stream that ends before the integer is complete is an unexpected-EOF transport
/// error.
pub async fn read_continued<R: FrameRead>(recv: &mut R, prefix: &[u8]) -> Result<(u64, usize)> {
    let invalid = || crate::Error::from(Status::internal("invalid varint prefix"));
    let first = *prefix.first().ok_or_else(invalid)?;
    let len = len_from_first_byte(first);
    let mut buf = [0_u8; MAX_LEN];
    buf.get_mut(..prefix.len())
        .ok_or_else(invalid)?
        .copy_from_slice(prefix);
    if prefix.len() < len {
        let rest = buf.get_mut(prefix.len()..len).ok_or_else(invalid)?;
        if !read_exact_or_eof(recv, rest).await? {
            return Err(unexpected_eof());
        }
    }
    decode(&buf).ok_or_else(invalid)
}

#[cfg(test)]
#[allow(clippy::unused_async_trait_impl)]
mod tests {
    use super::{MAX, decode, encoded_len, len_from_first_byte, push, read};
    use crate::Code;
    use crate::framed::FrameRead;

    /// An in-memory stream that yields `data` one byte at a time.
    struct Trickle<'a>(&'a [u8]);

    impl FrameRead for Trickle<'_> {
        async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> crate::Result<Option<usize>> {
            let Some((first, rest)) = self.0.split_first() else {
                return Ok(None);
            };
            let Some(slot) = bytes.first_mut() else {
                return Ok(Some(0));
            };
            *slot = *first;
            self.0 = rest;
            Ok(Some(1))
        }
    }

    fn encode(value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        push(&mut out, value).expect("value should encode");
        out
    }

    #[test]
    fn encodes_minimally_and_matches_rfc_9000_examples() {
        assert_eq!(encode(0), [0x00]);
        assert_eq!(encode(63), [0x3f]);
        assert_eq!(encode(64), [0x40, 0x40]);
        assert_eq!(encode(15_293), [0x7b, 0xbd]);
        assert_eq!(encode(16_383), [0x7f, 0xff]);
        assert_eq!(encode(16_384), [0x80, 0x00, 0x40, 0x00]);
        assert_eq!(encode(494_878_333), [0x9d, 0x7f, 0x3e, 0x7d]);
        assert_eq!(encode((1 << 30) - 1), [0xbf, 0xff, 0xff, 0xff]);
        assert_eq!(
            encode(1 << 30),
            [0xc0, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            encode(151_288_809_941_952_652),
            [0xc2, 0x19, 0x7c, 0x5e, 0xff, 0x14, 0xe8, 0x8c]
        );
        assert_eq!(encode(MAX).len(), 8);
        for value in [0, 63, 64, 16_383, 16_384, (1 << 30) - 1, 1 << 30, MAX] {
            assert_eq!(encode(value).len(), encoded_len(value));
        }
    }

    #[test]
    fn rejects_values_above_the_maximum() {
        let mut out = Vec::new();
        assert!(push(&mut out, MAX + 1).is_err());
        assert_eq!(out, []);
    }

    #[test]
    fn decodes_minimal_and_non_minimal_encodings() {
        for value in [0, 1, 63, 64, 16_383, 16_384, 1 << 29, 1 << 30, MAX] {
            let encoded = encode(value);
            assert_eq!(decode(&encoded), Some((value, encoded.len())));
        }
        // 37 in every width.
        assert_eq!(decode(&[0x25]), Some((37, 1)));
        assert_eq!(decode(&[0x40, 0x25]), Some((37, 2)));
        assert_eq!(decode(&[0x80, 0x00, 0x00, 0x25]), Some((37, 4)));
        assert_eq!(
            decode(&[0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x25, 0xff]),
            Some((37, 8))
        );
        assert_eq!(len_from_first_byte(0x7f), 2);
    }

    #[test]
    fn decode_reports_truncation() {
        assert_eq!(decode(&[]), None);
        assert_eq!(decode(&[0x40]), None);
        assert_eq!(decode(&[0x80, 0x00, 0x00]), None);
        assert_eq!(decode(&[0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]), None);
    }

    #[tokio::test]
    async fn reads_incrementally_and_reports_clean_end() {
        for value in [0, 63, 64, 16_383, 1 << 20, MAX] {
            let encoded = encode(value);
            let mut stream = Trickle(&encoded);
            let read = read(&mut stream).await.expect("varint should read");
            assert_eq!(read, Some((value, encoded.len())));
            assert!(
                super::read(&mut stream)
                    .await
                    .expect("clean end should not fail")
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn mid_varint_end_is_unexpected_eof() {
        for truncated in [&[0x40][..], &[0x80, 0x00, 0x00], &[0xc0, 0x00]] {
            let error = read(&mut Trickle(truncated))
                .await
                .expect_err("a truncated varint must fail");
            assert_eq!(error.into_status().code(), Code::Unavailable);
        }
    }
}
