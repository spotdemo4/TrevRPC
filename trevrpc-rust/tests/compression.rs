//! Compressed messages end to end: the frames a Rust server and a Rust client put on the wire,
//! and compressed calls over QUIC and `QMux` over TLS.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::StreamExt;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::client::{
    ChannelConfig, MessageTypes, QuicTransport, StreamingRpcTransport, TransportPolicy,
};
use trevrpc::framing::{decode_payload, encode_message_frame, encode_protobuf_frame, frame_type};
use trevrpc::server::{RouteOptions, Server, ServerOptions};
use trevrpc::tcp::TcpFallback;
use trevrpc::wire::DictionaryInfo;
use trevrpc::{Compression, Dictionary, RpcHeaders, RpcKind, RpcRequest, RpcStatus};

#[macro_use]
mod support;

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const UNARY: &str = "/test.Compressed/Echo";
const BIDI: &str = "/test.Compressed/Chat";
/// The type the test dictionary compresses: its samples are the JSON documents [`sample`] writes.
const MESSAGE_TYPE: &str = "test.Echo";

#[derive(Clone, PartialEq, Eq, prost::Message)]
struct Echo {
    #[prost(string, tag = "1")]
    value: String,
}

fn sample(i: u32) -> Echo {
    Echo {
        value: format!(
            r#"{{"service":"search","region":"us-east-1","status":404,"latency_ms":{}}}"#,
            100_u32.saturating_add(i)
        ),
    }
}

fn vector(name: &str) -> TestResult<Vec<u8>> {
    let line = include_str!("../../testdata/zstd-golden-vectors.txt")
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{name} = ")))
        .ok_or_else(|| format!("no vector {name}"))?;
    line.as_bytes()
        .chunks(2)
        .map(|pair| Ok(u8::from_str_radix(std::str::from_utf8(pair)?, 16)?))
        .collect()
}

fn dictionary_info() -> TestResult<DictionaryInfo> {
    Ok(DictionaryInfo {
        id: 1,
        fingerprint: vector("dict.d1.fingerprint")?,
    })
}

fn compression() -> TestResult<Compression> {
    Ok(Compression::new().with_dictionary(
        Dictionary::new(vector("dict.d1.dictionary")?).with_messages([MESSAGE_TYPE]),
    ))
}

struct RunningServer {
    udp: SocketAddr,
    tcp: SocketAddr,
    cert: CertificateDer<'static>,
    shutdown: Option<oneshot::Sender<()>>,
    task: JoinHandle<trevrpc::Result<()>>,
}

impl RunningServer {
    async fn stop(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        tokio::time::timeout(TEST_TIMEOUT, &mut self.task).await???;
        Ok(())
    }
}

fn self_signed() -> TestResult<(CertificateDer<'static>, quinn::rustls::ServerConfig)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key)?;
    Ok((cert_der, tls))
}

/// Serves an echo service whose routes name their message types, over QUIC and TCP.
async fn spawn_server(compression: Compression) -> TestResult<RunningServer> {
    let (cert, tls) = self_signed()?;
    let mut server = Server::new();
    server.set_options(ServerOptions::new().with_compression(compression));
    let options = RouteOptions::new().with_message_types(MESSAGE_TYPE, MESSAGE_TYPE);
    server.route_with_options(UNARY, options, |_context, body| async move { Ok(body) });
    server.route_streaming_with_options(
        BIDI,
        RpcKind::BidirectionalStreaming,
        options,
        |_context, _body, requests| async move { Ok(requests) },
    );
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )?;
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await?;
    let udp = endpoint.local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = TcpFallback {
        listener,
        tls: server.tcp_tls_config(tls),
    };
    let (shutdown, stopped) = oneshot::channel();
    let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));
    Ok(RunningServer {
        udp,
        tcp,
        cert,
        shutdown: Some(shutdown),
        task,
    })
}

fn client_tls(cert: &CertificateDer<'static>) -> TestResult<quinn::rustls::ClientConfig> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert.clone())?;
    Ok(quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth())
}

async fn quic_connection(
    addr: SocketAddr,
    cert: &CertificateDer<'static>,
) -> TestResult<(quinn::Endpoint, quinn::Connection)> {
    let mut tls = client_tls(cert)?;
    tls.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let config = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)?,
    ));
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config);
    let connection = endpoint.connect(addr, "localhost")?.await?;
    Ok((endpoint, connection))
}

/// A frame as it crossed the wire.
struct RawFrame {
    frame_type: u64,
    payload: Vec<u8>,
}

fn parse_frames(mut bytes: &[u8]) -> TestResult<Vec<RawFrame>> {
    let truncated = || "truncated frame";
    let mut frames = Vec::new();
    while !bytes.is_empty() {
        let (frame_type, type_len) = trevrpc::varint::decode(bytes).ok_or_else(truncated)?;
        let rest = bytes.get(type_len..).ok_or_else(truncated)?;
        let (len, len_len) = trevrpc::varint::decode(rest).ok_or_else(truncated)?;
        let rest = rest.get(len_len..).ok_or_else(truncated)?;
        let (payload, after) = rest
            .split_at_checked(usize::try_from(len)?)
            .ok_or_else(truncated)?;
        frames.push(RawFrame {
            frame_type,
            payload: payload.to_vec(),
        });
        bytes = after;
    }
    Ok(frames)
}

fn types(frames: &[RawFrame]) -> Vec<u64> {
    frames.iter().map(|frame| frame.frame_type).collect()
}

/// A request that queries the server and accepts dictionary 1, as a client's first call does.
fn querying_request(method: &str) -> TestResult<RpcRequest> {
    let mut request = RpcRequest::new(method);
    request.dictionary_query = vec![dictionary_info()?];
    request.accept_dictionaries = vec![1];
    Ok(request)
}

#[tokio::test]
async fn the_server_answers_and_compresses_its_responses() -> TestResult {
    let server = spawn_server(compression()?).await?;
    let (_endpoint, connection) = quic_connection(server.udp, &server.cert).await?;

    let (mut send, mut recv) = connection.open_bi().await?;
    let mut bytes = encode_protobuf_frame(frame_type::REQUEST, &querying_request(UNARY)?, 1 << 20)?;
    bytes.extend(encode_message_frame(
        &prost::Message::encode_to_vec(&sample(1)),
        1 << 20,
    )?);
    send.write_all(&bytes).await?;
    send.finish()?;
    let response = tokio::time::timeout(TEST_TIMEOUT, recv.read_to_end(1 << 20)).await??;
    let frames = parse_frames(&response)?;
    ensure_eq!(
        types(&frames),
        [
            frame_type::HEADERS,
            frame_type::ZSTD_MESSAGE,
            frame_type::STATUS
        ]
    );
    let headers: RpcHeaders = decode_payload(&frames[0].payload)?;
    ensure_eq!(headers.dictionaries, [dictionary_info()?]);

    // A wrong fingerprint gets an empty answer and an uncompressed response.
    let mut request = querying_request(UNARY)?;
    request.dictionary_query[0].fingerprint = b"12345678".to_vec();
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut bytes = encode_protobuf_frame(frame_type::REQUEST, &request, 1 << 20)?;
    bytes.extend(encode_message_frame(
        &prost::Message::encode_to_vec(&sample(1)),
        1 << 20,
    )?);
    send.write_all(&bytes).await?;
    send.finish()?;
    let response = tokio::time::timeout(TEST_TIMEOUT, recv.read_to_end(1 << 20)).await??;
    let frames = parse_frames(&response)?;
    ensure_eq!(
        types(&frames),
        [frame_type::HEADERS, frame_type::MESSAGE, frame_type::STATUS]
    );
    let headers: RpcHeaders = decode_payload(&frames[0].payload)?;
    ensure!(headers.dictionaries.is_empty());

    // A compressed request with a dictionary the server lacks is InvalidArgument.
    let (mut send, mut recv) = connection.open_bi().await?;
    let mut bytes = encode_protobuf_frame(frame_type::REQUEST, &RpcRequest::new(UNARY), 1 << 20)?;
    let payload = vector("bad.unknown_dictionary.payload")?;
    bytes.push(u8::try_from(frame_type::ZSTD_MESSAGE)?);
    bytes.push(u8::try_from(payload.len())?);
    bytes.extend(payload);
    send.write_all(&bytes).await?;
    send.finish()?;
    let response = tokio::time::timeout(TEST_TIMEOUT, recv.read_to_end(1 << 20)).await??;
    let frames = parse_frames(&response)?;
    let status: RpcStatus = decode_payload(&frames.last().unwrap().payload)?;
    ensure_eq!(
        status.code,
        trevrpc::Code::InvalidArgument.as_u32(),
        "{status:?}"
    );

    drop(connection);
    server.stop().await
}

/// The client of a channel with compression: over QUIC or over `QMux` over TLS.
async fn compressed_channel(
    server: &RunningServer,
    policy: TransportPolicy,
) -> TestResult<(quinn::Endpoint, trevrpc::client::Channel)> {
    let mut tls = client_tls(&server.cert)?;
    tls.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let config = ChannelConfig::new()
        .with_compression(compression()?)
        .with_tcp_fallback(client_tls(&server.cert)?)
        .with_tcp_addr(server.tcp)
        .with_transport_policy(policy);
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config.quic_config(client_tls(&server.cert)?)?);
    let channel = trevrpc::client::Channel::connect_with_config(
        endpoint.clone(),
        server.udp,
        "localhost",
        config,
    )
    .await?;
    Ok((endpoint, channel))
}

#[tokio::test]
async fn compressed_calls_work_over_quic_and_qmux() -> TestResult {
    let server = spawn_server(compression()?).await?;
    for policy in [TransportPolicy::QuicOnly, TransportPolicy::TcpOnly] {
        let (_endpoint, channel) = compressed_channel(&server, policy).await?;
        for i in 0..2 {
            let reply: Echo = trevrpc::client::unary(
                &channel,
                UNARY,
                &sample(i),
                trevrpc::client::CallOptions::new(),
            )
            .message_types(MESSAGE_TYPE, MESSAGE_TYPE)
            .await?;
            ensure!(reply == sample(i), "{policy:?}: {reply:?}");
        }
        let chat = trevrpc::client::bidirectional_streaming::<_, Echo, Echo>(
            &channel,
            BIDI,
            trevrpc::client::CallOptions::new(),
        )
        .message_types(MESSAGE_TYPE, MESSAGE_TYPE)
        .await?;
        let (sender, mut replies) = chat.split();
        for i in 0..3 {
            sender.send(sample(i)).await?;
        }
        sender.finish()?;
        for i in 0..3 {
            let reply = tokio::time::timeout(TEST_TIMEOUT, replies.next())
                .await?
                .unwrap()?;
            ensure!(reply == sample(i), "{policy:?}: {reply:?}");
        }
        ensure!(replies.next().await.is_none());
    }
    server.stop().await
}

#[tokio::test]
async fn invalid_compression_fails_at_startup() -> TestResult {
    let mut server = Server::new();
    server.set_options(ServerOptions::new().with_compression(
        Compression::new().with_dictionary(Dictionary::new(b"not a dictionary".to_vec())),
    ));
    let (_, tls) = self_signed()?;
    ensure!(server.quic_config(tls).is_err());
    let config = ChannelConfig::new()
        .with_compression(Compression::new().with_dictionary(Dictionary::new(Vec::new())));
    let endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let result = trevrpc::client::Channel::connect_with_config(
        endpoint,
        SocketAddr::from(([127, 0, 0, 1], 9)),
        "localhost",
        config,
    )
    .await;
    ensure!(result.is_err());
    Ok(())
}

/// The frames of one call a fake server received.
type Received = Vec<RawFrame>;

/// Decompresses a `ZSTD_MESSAGE` payload with the test dictionary, as libzstd does.
fn decompress(payload: &[u8]) -> TestResult<Vec<u8>> {
    use zstd_safe::{DCtx, DParameter, FrameFormat};
    let zstd = |code| zstd_safe::get_error_name(code).to_owned();
    let mut context = DCtx::create();
    context
        .set_parameter(DParameter::Format(FrameFormat::Magicless))
        .map_err(zstd)?;
    context
        .load_dictionary(&vector("dict.d1.dictionary")?)
        .map_err(zstd)?;
    let mut message = Vec::with_capacity(1 << 16);
    context.decompress(&mut message, payload).map_err(zstd)?;
    Ok(message)
}

/// The fake server's response to the request frames of one call: HEADERS carrying `answer`, an
/// uncompressed echo of each request message, and an OK STATUS.
fn fake_response(frames: &[RawFrame], answer: &[DictionaryInfo]) -> TestResult<Vec<u8>> {
    let headers = RpcHeaders {
        dictionaries: answer.to_vec(),
        ..RpcHeaders::default()
    };
    let mut response = encode_protobuf_frame(frame_type::HEADERS, &headers, 1 << 20)?;
    for frame in frames {
        let message = match frame.frame_type {
            frame_type::MESSAGE => frame.payload.clone(),
            frame_type::ZSTD_MESSAGE => decompress(&frame.payload)?,
            _ => continue,
        };
        response.extend(encode_message_frame(&message, 1 << 20)?);
    }
    response.extend([u8::try_from(frame_type::STATUS)?, 0]);
    Ok(response)
}

/// Accepts one connection and answers every call with HEADERS carrying `answer`, an echo of each
/// request message, and STATUS, recording what each call sent.
fn fake_server(
    answer: Vec<DictionaryInfo>,
) -> TestResult<(
    SocketAddr,
    CertificateDer<'static>,
    tokio::sync::mpsc::UnboundedReceiver<Received>,
)> {
    let (cert, mut tls) = self_signed()?;
    tls.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let config = quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(tls)?,
    ));
    let endpoint = quinn::Endpoint::server(config, SocketAddr::from(([127, 0, 0, 1], 0)))?;
    let addr = endpoint.local_addr()?;
    let (calls, received) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let Some(incoming) = endpoint.accept().await else {
            return;
        };
        let Ok(connection) = incoming.await else {
            return;
        };
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let Ok(request) = recv.read_to_end(1 << 20).await else {
                return;
            };
            let Ok(frames) = parse_frames(&request) else {
                return;
            };
            let Ok(response) = fake_response(&frames, &answer) else {
                return;
            };
            let _ = send.write_all(&response).await;
            let _ = send.finish();
            let _ = calls.send(frames);
        }
    });
    Ok((addr, cert, received))
}

#[tokio::test]
async fn the_client_queries_then_compresses_its_requests() -> TestResult {
    let (addr, cert, mut received) = fake_server(vec![dictionary_info()?])?;
    let (_endpoint, connection) = quic_connection(addr, &cert).await?;
    let transport = QuicTransport::new(connection).with_compression(&compression()?)?;
    let call = |i| {
        let transport = transport.clone();
        async move {
            let reply: Echo = trevrpc::client::unary(
                &transport,
                UNARY,
                &sample(i),
                trevrpc::client::CallOptions::new(),
            )
            .message_types(MESSAGE_TYPE, MESSAGE_TYPE)
            .await?;
            ensure_eq!(reply, sample(i));
            TestResult::Ok(())
        }
    };

    call(0).await?;
    let first = received.recv().await.unwrap();
    ensure_eq!(types(&first), [frame_type::REQUEST, frame_type::MESSAGE]);
    let request: RpcRequest = decode_payload(&first[0].payload)?;
    ensure_eq!(request.dictionary_query, [dictionary_info()?]);
    ensure_eq!(request.accept_dictionaries, [1]);

    call(1).await?;
    let second = received.recv().await.unwrap();
    ensure_eq!(
        types(&second),
        [frame_type::REQUEST, frame_type::ZSTD_MESSAGE]
    );
    let request: RpcRequest = decode_payload(&second[0].payload)?;
    ensure!(request.dictionary_query.is_empty());
    ensure_eq!(request.accept_dictionaries, [1]);

    // Streaming calls compress too, once the connection has its answer.
    let mut frames = transport
        .streaming_call_with_info(
            RpcRequest::new(BIDI).with_kind(RpcKind::BidirectionalStreaming),
            trevrpc::stream::from_iter((0..3).map(|i| prost::Message::encode_to_vec(&sample(i)))),
            trevrpc::client::CallInfo::new(
                trevrpc::Unreliable::NONE,
                Some(MessageTypes::new(MESSAGE_TYPE, MESSAGE_TYPE)),
            ),
        )
        .await?;
    while frames.next().await.transpose()?.is_some() {}
    let streamed = received.recv().await.unwrap();
    ensure_eq!(
        types(&streamed),
        [
            frame_type::REQUEST,
            frame_type::ZSTD_MESSAGE,
            frame_type::ZSTD_MESSAGE,
            frame_type::ZSTD_MESSAGE
        ]
    );
    Ok(())
}
