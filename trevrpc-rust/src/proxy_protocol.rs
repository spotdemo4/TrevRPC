//! The PROXY protocol, versions 1 and 2 (<https://www.haproxy.org/download/3.0/doc/proxy-protocol.txt>),
//! with which a load balancer passes on the address of the client it forwards. The TCP listener
//! reads it at the start of the connections from the trusted proxies of
//! [`TcpFallback`](crate::tcp::TcpFallback), and the socket of a
//! [`QuicEndpoint`](crate::udp::QuicEndpoint) at the start of every datagram from its trusted
//! proxies; both require it from them.

use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf as _, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, ReadBuf};

/// The first bytes of a version 2 header.
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";
/// The fixed part of a version 2 header: the signature, the version and command, the address
/// family and transport, and the length of the rest.
const V2_HEADER_LEN: usize = 16;
/// The first bytes of a version 1 header.
const V1_PREFIX: &[u8] = b"PROXY ";
/// The longest version 1 header, CRLF included.
const V1_MAX_LEN: usize = 107;
/// How much is read at a time while looking for the end of a header.
const READ_CHUNK: usize = 256;

/// The transport a version 2 header's address family names, which must be the one it arrived on
/// for its addresses to be the client's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Transport {
    Stream,
    Datagram,
}

impl Transport {
    /// The transport's code in the low four bits of the address family byte.
    const fn code(self) -> u8 {
        match self {
            Self::Stream => 1,
            Self::Datagram => 2,
        }
    }
}

/// The result of parsing the start of a connection.
#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    /// The bytes are a valid start of a header, which is at least this long.
    Incomplete { at_least: usize },
    /// A whole header of `len` bytes. `source` is the client's address, or `None` when the
    /// header does not carry one (version 2 `LOCAL`, version 1 `UNKNOWN`, or an address family
    /// other than the transport's over IPv4 or IPv6) and the proxy's own address stands for it.
    Complete {
        source: Option<SocketAddr>,
        len: usize,
    },
}

/// Reads the PROXY header at the start of `stream`.
///
/// Returns the client's address the header carries, `None` when it carries none, and what was
/// read past the header, which belongs to the protocol that follows.
///
/// # Errors
///
/// `InvalidData` when the stream does not start with a valid header, and the read's error or
/// `UnexpectedEof` when it fails or ends first.
pub(crate) async fn read_header<S>(stream: &mut S) -> io::Result<(Option<SocketAddr>, BytesMut)>
where
    S: AsyncRead + Unpin,
{
    let mut buf = BytesMut::new();
    loop {
        match parse(&buf).map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))? {
            Parsed::Complete { source, len } => {
                buf.advance(len);
                return Ok((source, buf));
            }
            Parsed::Incomplete { at_least } => {
                buf.reserve(at_least.saturating_sub(buf.len()).max(READ_CHUNK));
                if stream.read_buf(&mut buf).await? == 0 {
                    return Err(io::ErrorKind::UnexpectedEof.into());
                }
            }
        }
    }
}

/// Parses the version 2 header that must start `datagram`, which arrived over UDP.
///
/// Returns the client's address the header carries, `None` when it carries none, and the
/// header's length.
pub(crate) fn parse_datagram(datagram: &[u8]) -> Result<(Option<SocketAddr>, usize), &'static str> {
    if !datagram.starts_with(V2_SIGNATURE) {
        return Err("datagram from a trusted proxy does not start with a PROXY v2 header");
    }
    match parse_v2(datagram, Transport::Datagram)? {
        Parsed::Complete { source, len } => Ok((source, len)),
        Parsed::Incomplete { .. } => Err("PROXY v2 header is longer than its datagram"),
    }
}

/// Parses the header at the start of `buf`, the start of a TCP connection.
fn parse(buf: &[u8]) -> Result<Parsed, &'static str> {
    if buf.starts_with(V2_SIGNATURE) {
        return parse_v2(buf, Transport::Stream);
    }
    if buf.starts_with(V1_PREFIX) {
        return parse_v1(buf);
    }
    if V2_SIGNATURE.starts_with(buf) || V1_PREFIX.starts_with(buf) {
        return Ok(Parsed::Incomplete {
            at_least: buf.len().saturating_add(1),
        });
    }
    Err("connection from a trusted proxy does not start with a PROXY header")
}

/// Parses a version 1 header: `PROXY TCP4|TCP6 <source> <destination> <source port>
/// <destination port>\r\n`, or `PROXY UNKNOWN ...\r\n`.
fn parse_v1(buf: &[u8]) -> Result<Parsed, &'static str> {
    let searched = buf.get(..V1_MAX_LEN).unwrap_or(buf);
    let Some(end) = searched.windows(2).position(|pair| pair == b"\r\n") else {
        if buf.len() >= V1_MAX_LEN {
            return Err("PROXY v1 header is too long");
        }
        return Ok(Parsed::Incomplete {
            at_least: buf.len().saturating_add(1),
        });
    };
    let line = searched
        .get(..end)
        .and_then(|line| std::str::from_utf8(line).ok())
        .ok_or("PROXY v1 header is not ASCII")?;
    let mut fields = line.split(' ').skip(1);
    let source = match fields.next() {
        Some("UNKNOWN") => None,
        Some("TCP4") => Some(parse_v1_addresses::<Ipv4Addr>(fields)?),
        Some("TCP6") => Some(parse_v1_addresses::<Ipv6Addr>(fields)?),
        _ => return Err("PROXY v1 header has an unknown protocol"),
    };
    Ok(Parsed::Complete {
        source,
        len: end.saturating_add(2),
    })
}

/// Parses the four address fields of a version 1 `TCP4` or `TCP6` header and returns the source.
fn parse_v1_addresses<'a, A>(
    mut fields: impl Iterator<Item = &'a str>,
) -> Result<SocketAddr, &'static str>
where
    A: std::str::FromStr + Into<std::net::IpAddr>,
{
    let mut address = || {
        fields
            .next()
            .and_then(|field| field.parse::<A>().ok())
            .ok_or("PROXY v1 header has an invalid address")
    };
    let source = address()?;
    address()?;
    let mut port = || {
        fields
            .next()
            // Decimal, without a sign or leading zeros.
            .filter(|field| {
                !field.is_empty()
                    && field.bytes().all(|byte| byte.is_ascii_digit())
                    && (field.len() == 1 || !field.starts_with('0'))
            })
            .and_then(|field| field.parse::<u16>().ok())
            .ok_or("PROXY v1 header has an invalid port")
    };
    let source_port = port()?;
    port()?;
    if fields.next().is_some() {
        return Err("PROXY v1 header has extra fields");
    }
    Ok(SocketAddr::new(source.into(), source_port))
}

/// Parses a version 2 header that arrived over `transport`. Its TLVs are skipped.
fn parse_v2(buf: &[u8], transport: Transport) -> Result<Parsed, &'static str> {
    let Some(&[version_command, family, len_high, len_low]) = buf.get(12..V2_HEADER_LEN) else {
        return Ok(Parsed::Incomplete {
            at_least: V2_HEADER_LEN,
        });
    };
    if version_command >> 4 != 2 {
        return Err("PROXY v2 header has an unknown version");
    }
    let local = match version_command & 0x0f {
        0 => true,
        1 => false,
        _ => return Err("PROXY v2 header has an unknown command"),
    };
    let len = V2_HEADER_LEN.saturating_add(usize::from(u16::from_be_bytes([len_high, len_low])));
    let Some(addresses) = buf.get(V2_HEADER_LEN..len) else {
        return Ok(Parsed::Incomplete { at_least: len });
    };
    if local {
        return Ok(Parsed::Complete { source: None, len });
    }
    if family & 0x0f != transport.code() {
        // Another transport, or unspecified: the proxy's own address stands for the client.
        return Ok(Parsed::Complete { source: None, len });
    }
    let source = match family >> 4 {
        // IPv4: source and destination addresses, then ports.
        1 => {
            let Some(&[a, b, c, d, _, _, _, _, port_high, port_low, _, _]) = addresses.get(..12)
            else {
                return Err("PROXY v2 IPv4 address block is too short");
            };
            Some(SocketAddr::new(
                Ipv4Addr::new(a, b, c, d).into(),
                u16::from_be_bytes([port_high, port_low]),
            ))
        }
        // IPv6.
        2 => {
            let Some(block) = addresses.get(..36) else {
                return Err("PROXY v2 IPv6 address block is too short");
            };
            let (source, ports) = block.split_at(16);
            let (Ok(source), Some(&[port_high, port_low])) =
                (<[u8; 16]>::try_from(source), ports.get(16..18))
            else {
                return Err("PROXY v2 IPv6 address block is too short");
            };
            Some(SocketAddr::new(
                Ipv6Addr::from(source).into(),
                u16::from_be_bytes([port_high, port_low]),
            ))
        }
        // Unix sockets or unspecified: the proxy's own address stands for the client.
        _ => None,
    };
    Ok(Parsed::Complete { source, len })
}

/// A stream that first yields the bytes read ahead of it.
pub(crate) struct Rewound<S> {
    prefix: BytesMut,
    inner: S,
}

impl<S> Rewound<S> {
    pub(crate) const fn new(prefix: BytesMut, inner: S) -> Self {
        Self { prefix, inner }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for Rewound<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.prefix.is_empty() {
            return Pin::new(&mut self.inner).poll_read(context, buf);
        }
        let len = self.prefix.len().min(buf.remaining());
        buf.put_slice(&self.prefix.split_to(len));
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Rewound<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(context, buf)
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write_vectored(context, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }

    fn poll_flush(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(context)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(context)
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use tokio::io::AsyncReadExt as _;

    use super::{Parsed, Rewound, V2_SIGNATURE, parse, parse_datagram, read_header};

    fn addr(text: &str) -> SocketAddr {
        text.parse().unwrap()
    }

    fn complete(buf: &[u8]) -> (Option<SocketAddr>, usize) {
        match parse(buf) {
            Ok(Parsed::Complete { source, len }) => (source, len),
            other => panic!("{other:?} for {buf:?}"),
        }
    }

    fn v2(command: u8, family: u8, rest: &[u8]) -> Vec<u8> {
        let mut header = V2_SIGNATURE.to_vec();
        header.push(0x20 | command);
        header.push(family);
        header.extend_from_slice(&u16::try_from(rest.len()).unwrap().to_be_bytes());
        header.extend_from_slice(rest);
        header
    }

    fn v2_ipv4_block() -> Vec<u8> {
        let mut block = vec![203, 0, 113, 7, 10, 0, 0, 1];
        block.extend_from_slice(&51_000_u16.to_be_bytes());
        block.extend_from_slice(&443_u16.to_be_bytes());
        block
    }

    #[test]
    fn v1_tcp4_tcp6_and_unknown() {
        let line = b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443\r\n";
        assert_eq!(
            complete(line),
            (Some(addr("203.0.113.7:51000")), line.len())
        );
        let line = b"PROXY TCP6 2001:db8::7 2001:db8::1 51000 443\r\n";
        assert_eq!(
            complete(line),
            (Some(addr("[2001:db8::7]:51000")), line.len())
        );
        let line = b"PROXY UNKNOWN ffff:f...f:ffff ffff:f...f:ffff 65535 65535\r\n";
        assert_eq!(complete(line), (None, line.len()));
        assert_eq!(complete(b"PROXY UNKNOWN\r\n"), (None, 15));
    }

    #[test]
    fn v1_rejects_malformed_lines() {
        for line in [
            &b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443\n\r\n"[..],
            b"PROXY TCP4 2001:db8::7 10.0.0.1 51000 443\r\n",
            b"PROXY TCP6 203.0.113.7 2001:db8::1 51000 443\r\n",
            b"PROXY TCP4 203.0.113.256 10.0.0.1 51000 443\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 65536 443\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 +5100 443\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 051000 443\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 51000\r\n",
            b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443 extra\r\n",
            b"PROXY TCP4  203.0.113.7 10.0.0.1 51000 443\r\n",
            b"PROXY UDP4 203.0.113.7 10.0.0.1 51000 443\r\n",
        ] {
            assert!(parse(line).is_err(), "{:?}", String::from_utf8_lossy(line));
        }
    }

    #[test]
    fn v1_without_crlf_is_incomplete_until_too_long() {
        let partial = b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443";
        assert!(matches!(parse(partial), Ok(Parsed::Incomplete { .. })));
        let mut long = b"PROXY UNKNOWN ".to_vec();
        long.resize(107, b'x');
        assert!(parse(&long).is_err());
        // A CRLF at bytes 106 and 107 still fits.
        long.truncate(105);
        long.extend_from_slice(b"\r\n");
        assert_eq!(complete(&long), (None, 107));
    }

    #[test]
    fn v2_proxy_ipv4_and_ipv6() {
        let header = v2(1, 0x11, &v2_ipv4_block());
        assert_eq!(complete(&header), (Some(addr("203.0.113.7:51000")), 28));

        let mut block = Vec::new();
        block.extend_from_slice(
            &"2001:db8::7"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .octets(),
        );
        block.extend_from_slice(
            &"2001:db8::1"
                .parse::<std::net::Ipv6Addr>()
                .unwrap()
                .octets(),
        );
        block.extend_from_slice(&51_000_u16.to_be_bytes());
        block.extend_from_slice(&443_u16.to_be_bytes());
        let header = v2(1, 0x21, &block);
        assert_eq!(complete(&header), (Some(addr("[2001:db8::7]:51000")), 52));
    }

    #[test]
    fn v2_skips_tlvs() {
        let mut rest = v2_ipv4_block();
        // PP2_TYPE_AUTHORITY "example.com", then PP2_TYPE_NOOP.
        rest.extend_from_slice(&[0x02, 0, 11]);
        rest.extend_from_slice(b"example.com");
        rest.extend_from_slice(&[0x04, 0, 2, 0, 0]);
        let header = v2(1, 0x11, &rest);
        assert_eq!(
            complete(&header),
            (Some(addr("203.0.113.7:51000")), header.len())
        );
    }

    #[test]
    fn v2_local_and_other_families_carry_no_address() {
        assert_eq!(complete(&v2(0, 0x11, &v2_ipv4_block())), (None, 28));
        assert_eq!(complete(&v2(0, 0x00, &[])), (None, 16));
        assert_eq!(complete(&v2(1, 0x00, &[])), (None, 16));
        // UDP over IPv4, and Unix stream sockets.
        assert_eq!(complete(&v2(1, 0x12, &v2_ipv4_block())), (None, 28));
        assert_eq!(complete(&v2(1, 0x31, &[0; 216])), (None, 232));
    }

    #[test]
    fn v2_rejects_bad_version_command_and_short_blocks() {
        let mut header = v2(1, 0x11, &v2_ipv4_block());
        header[12] = 0x11;
        assert!(parse(&header).is_err());
        assert!(parse(&v2(2, 0x11, &v2_ipv4_block())).is_err());
        assert!(parse(&v2(1, 0x11, &[0; 11])).is_err());
        assert!(parse(&v2(1, 0x21, &[0; 35])).is_err());
    }

    #[test]
    fn v2_is_incomplete_until_its_length_arrives() {
        let header = v2(1, 0x11, &v2_ipv4_block());
        assert_eq!(
            parse(&header[..10]),
            Ok(Parsed::Incomplete { at_least: 11 })
        );
        assert_eq!(
            parse(&header[..20]),
            Ok(Parsed::Incomplete { at_least: 28 })
        );
    }

    #[test]
    fn anything_else_is_rejected() {
        // A TLS ClientHello.
        assert!(parse(&[0x16, 0x03, 0x01, 0x02, 0x00]).is_err());
        assert!(parse(b"GET / HTTP/1.1\r\n").is_err());
        assert!(matches!(parse(b"PRO"), Ok(Parsed::Incomplete { .. })));
        assert!(matches!(parse(b""), Ok(Parsed::Incomplete { .. })));
    }

    #[test]
    fn datagrams_carry_udp_addresses() {
        let mut datagram = v2(1, 0x12, &v2_ipv4_block());
        datagram.extend_from_slice(b"quic");
        assert_eq!(
            parse_datagram(&datagram),
            Ok((Some(addr("203.0.113.7:51000")), 28))
        );
        let mut block = vec![0; 36];
        block[15] = 7;
        block[32..34].copy_from_slice(&51_000_u16.to_be_bytes());
        assert_eq!(
            parse_datagram(&v2(1, 0x22, &block)),
            Ok((Some(addr("[::7]:51000")), 52))
        );
        // TCP families, LOCAL, and extensions.
        assert_eq!(
            parse_datagram(&v2(1, 0x11, &v2_ipv4_block())),
            Ok((None, 28))
        );
        assert_eq!(parse_datagram(&v2(0, 0x00, &[])), Ok((None, 16)));
        let mut rest = v2_ipv4_block();
        rest.extend_from_slice(&[0x04, 0, 2, 0, 0]);
        assert_eq!(
            parse_datagram(&v2(1, 0x12, &rest)),
            Ok((Some(addr("203.0.113.7:51000")), 33))
        );
        // And the TCP listener ignores UDP families, as before.
        assert_eq!(complete(&v2(1, 0x12, &v2_ipv4_block())), (None, 28));
    }

    #[test]
    fn datagrams_must_hold_a_whole_v2_header() {
        let header = v2(1, 0x12, &v2_ipv4_block());
        assert!(parse_datagram(&header[..27]).is_err());
        assert!(parse_datagram(&header[..10]).is_err());
        assert!(parse_datagram(b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443\r\n").is_err());
        assert!(parse_datagram(&[0xc0, 0, 0, 0, 1]).is_err());
        assert!(parse_datagram(&v2(1, 0x12, &[0; 11])).is_err());
        assert!(parse_datagram(&v2(2, 0x12, &v2_ipv4_block())).is_err());
    }

    #[tokio::test]
    async fn read_header_keeps_what_follows() {
        let mut input = b"PROXY TCP4 203.0.113.7 10.0.0.1 51000 443\r\n".to_vec();
        input.extend_from_slice(b"\x16\x03\x01client hello");
        let mut stream = &input[..];
        let (source, rest) = read_header(&mut stream).await.unwrap();
        assert_eq!(source, Some(addr("203.0.113.7:51000")));
        let mut rewound = Rewound::new(rest, stream);
        let mut after = Vec::new();
        rewound.read_to_end(&mut after).await.unwrap();
        assert_eq!(after, b"\x16\x03\x01client hello");

        let mut input = v2(1, 0x11, &v2_ipv4_block());
        input.extend_from_slice(b"after");
        let (reader, mut writer) = tokio::io::duplex(8);
        let writing = tokio::spawn(async move {
            tokio::io::AsyncWriteExt::write_all(&mut writer, &input)
                .await
                .unwrap();
        });
        let mut reader = reader;
        let (source, rest) = read_header(&mut reader).await.unwrap();
        assert_eq!(source, Some(addr("203.0.113.7:51000")));
        writing.await.unwrap();
        let mut after = Vec::new();
        Rewound::new(rest, reader)
            .read_to_end(&mut after)
            .await
            .unwrap();
        assert_eq!(after, b"after");
    }

    #[tokio::test]
    async fn read_header_fails_on_early_end() {
        let mut stream = &b"PROXY TCP4 203.0.113.7"[..];
        let error = read_header(&mut stream).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    }
}
