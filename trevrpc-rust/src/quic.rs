//! Quinn helpers shared by the native client and the server.

/// Bytes of the length prefix that precede every `TrevRPC` frame.
const FRAME_HEADER_LEN: u64 = 4;

/// The stream receive window that fits one maximum-size frame and its length prefix.
pub(crate) fn frame_receive_window(max_frame_size: usize) -> u64 {
    saturating_usize_to_u64(max_frame_size).saturating_add(FRAME_HEADER_LEN)
}

pub(crate) fn saturating_usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

pub(crate) fn varint(value: u64) -> quinn::VarInt {
    quinn::VarInt::from_u64(value).unwrap_or(quinn::VarInt::MAX)
}

/// Returns the ALPN protocol the TLS handshake negotiated for `connection`.
pub(crate) fn negotiated_protocol(connection: &quinn::Connection) -> Option<Vec<u8>> {
    connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|data| data.protocol)
}
