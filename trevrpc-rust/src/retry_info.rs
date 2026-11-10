//! The standard `google.rpc.RetryInfo` status detail, which tells a client when to retry.
//!
//! `TrevRPC` encodes and decodes the two small messages itself, so that it does not depend on
//! googleapis; the encoding is the one every protobuf library produces.

use std::time::Duration;

use prost::Message;

/// The full name of `google.rpc.RetryInfo`, the type of the detail.
pub(crate) const RETRY_INFO_TYPE: &str = "google.rpc.RetryInfo";

/// `google.rpc.RetryInfo`.
#[derive(Clone, PartialEq, Message)]
struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    retry_delay: Option<ProtoDuration>,
}

/// `google.protobuf.Duration`.
#[derive(Clone, PartialEq, Message)]
struct ProtoDuration {
    #[prost(int64, tag = "1")]
    seconds: i64,
    #[prost(int32, tag = "2")]
    nanos: i32,
}

/// Encodes a `RetryInfo` that tells the client to wait `delay`. A delay too long for a
/// `google.protobuf.Duration` is the longest one.
pub(crate) fn encode(delay: Duration) -> Vec<u8> {
    RetryInfo {
        retry_delay: Some(ProtoDuration {
            seconds: i64::try_from(delay.as_secs()).unwrap_or(i64::MAX),
            // `subsec_nanos` is below a billion.
            nanos: i32::try_from(delay.subsec_nanos()).unwrap_or_default(),
        }),
    }
    .encode_to_vec()
}

/// Decodes a `RetryInfo`. `None` when the value is malformed, has no `retry_delay`, or holds a
/// delay that is not a valid non-negative `Duration`: its nanos must be below a second. Fields
/// the message does not know are skipped, and a repeated field takes its last value, as protobuf
/// decoders do.
pub(crate) fn decode(value: &[u8]) -> Option<Duration> {
    let delay = RetryInfo::decode(value).ok()?.retry_delay?;
    let seconds = u64::try_from(delay.seconds).ok()?;
    let nanos = u32::try_from(delay.nanos).ok()?;
    (nanos < 1_000_000_000).then(|| Duration::new(seconds, nanos))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{decode, encode};

    fn unhex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    #[test]
    fn golden_encodings_match_every_runtime() {
        // RetryInfo{retry_delay: Duration{seconds: 1, nanos: 2}} is the same bytes in every
        // runtime's tests.
        for (delay, expected) in [
            (Duration::new(1, 2), "0a0408011002"),
            (Duration::from_secs(30), "0a02081e"),
            (Duration::from_millis(500), "0a061080cab5ee01"),
            (Duration::from_millis(1500), "0a0808011080cab5ee01"),
            (Duration::ZERO, "0a00"),
        ] {
            assert_eq!(encode(delay), unhex(expected), "{delay:?}");
            assert_eq!(decode(&unhex(expected)), Some(delay), "{expected}");
        }
    }

    #[test]
    fn a_delay_too_long_for_a_duration_is_the_longest_one() {
        let longest = Duration::new(i64::MAX.unsigned_abs(), 999_999_999);
        assert_eq!(decode(&encode(Duration::MAX)), Some(longest));
    }

    #[test]
    fn decoding_follows_protobuf_rules() {
        for (hex, expected) in [
            // Unknown fields are skipped, at either level.
            ("1a036162630a04080110022000", Some(Duration::new(1, 2))),
            ("0a061a0008011002", Some(Duration::new(1, 2))),
            // The last value wins.
            ("0a02080a0a02080b", Some(Duration::from_secs(11))),
            // No delay, a malformed one, or an invalid one.
            ("", None),
            ("1a03616263", None),
            ("0a", None),
            ("0a040801", None),
            ("0a0108", None),
            ("0801", None),
            ("0a050d00000000", None),
            ("0a0b08ffffffffffffffffff01", None),
            ("0a0d080110ffffffffffffffffff01", None),
            ("0a08080110 8094ebdc03", None),
        ] {
            assert_eq!(decode(&unhex(&hex.replace(' ', ""))), expected, "{hex}");
        }
    }
}
