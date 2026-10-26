//! The `server` subcommand: `Echo` over native QUIC, HTTP/3 and `WebTransport` on one endpoint.

use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};

use futures_util::StreamExt;
use futures_util::future::{Either, ready};
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::AsyncReadExt;
use tower_http::services::ServeDir;
use trevrpc::fallback::HttpFallback;
use trevrpc::server::{
    RequestContext, RouteOptions, Server, ServerOptions, WebTransportAdmissionRequest,
};
use trevrpc::tcp::TcpFallback;
use trevrpc::{BoxStream, Code, Error, RpcKind, Status};

use crate::proto::{
    self, CompressedEcho, Echo, EchoRequest, EchoResponse, PlainMessage, UnreliableEcho,
};
use crate::{BoxError, Flags, connect};

const SUFFIX_METADATA_KEY: &str = "interop-suffix";
const HEADER_METADATA_KEY: &str = "interop-header";
const TRAILER_METADATA_KEY: &str = "interop-trailer";

struct EchoService;

/// Answers the `interop-header` request metadata.
///
/// When the request carries the key with value `v`, the response headers are
/// `{interop-header: v}` and the `OK` status carries trailing metadata `{interop-trailer: v}`.
/// Without it the response has neither.
fn answer_header(context: &RequestContext) -> Result<(), Status> {
    let Some(value) = context.metadata().get(HEADER_METADATA_KEY) else {
        return Ok(());
    };
    context.set_response_header(HEADER_METADATA_KEY, value.clone())?;
    context.set_response_metadata(TRAILER_METADATA_KEY, value.clone())
}

/// The status a request asks to fail with, if any.
fn requested_failure(request: &EchoRequest) -> Option<Status> {
    (request.fail_code != 0).then(|| {
        Status::new(
            Code::from_u32(request.fail_code),
            request.fail_message.clone(),
        )
    })
}

#[trevrpc::async_trait]
impl Echo for EchoService {
    async fn unary(
        &self,
        context: RequestContext,
        request: EchoRequest,
    ) -> Result<EchoResponse, Status> {
        if let Some(status) = requested_failure(&request) {
            return Err(status);
        }
        answer_header(&context)?;
        let suffix = context
            .metadata()
            .get(SUFFIX_METADATA_KEY)
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .unwrap_or_default();
        Ok(EchoResponse {
            message: request.message + &suffix,
            index: 0,
        })
    }

    async fn client_stream(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<EchoResponse, Status> {
        let response = concatenate(requests).await?;
        answer_header(&context)?;
        Ok(response)
    }

    async fn server_stream(
        &self,
        context: RequestContext,
        request: EchoRequest,
    ) -> Result<BoxStream<EchoResponse>, Status> {
        let failure = requested_failure(&request);
        let EchoRequest { message, count, .. } = request;
        let responses = (0..count)
            .map(move |index| {
                Ok(EchoResponse {
                    message: message.clone(),
                    index,
                })
            })
            .chain(failure.map(|status| Err(Error::from(status))));
        answer_header(&context)?;
        Ok(Box::pin(futures_util::stream::iter(responses)))
    }

    async fn bidi(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<BoxStream<EchoResponse>, Status> {
        answer_header(&context)?;
        Ok(Box::pin(echo_each(requests)))
    }
}

/// `Echo.ClientStream`: the requests' messages concatenated, with their count as the index, or
/// the status of the first request that asks to fail.
async fn concatenate(mut requests: BoxStream<EchoRequest>) -> Result<EchoResponse, Status> {
    let mut message = String::new();
    let mut count = 0_u32;
    while let Some(request) = requests.next().await {
        let request = request?;
        if let Some(status) = requested_failure(&request) {
            return Err(status);
        }
        message.push_str(&request.message);
        count = count.saturating_add(1);
    }
    Ok(EchoResponse {
        message,
        index: count,
    })
}

/// `Echo.Bidi`: echoes each request as it arrives, with its index, until one asks to fail.
fn echo_each(
    requests: BoxStream<EchoRequest>,
) -> impl futures_util::Stream<Item = Result<EchoResponse, Error>> + Send {
    // `(requests, next index, finished)`: a failing request ends the stream with its status.
    futures_util::stream::unfold(
        (requests, 0_u32, false),
        |(mut requests, index, finished)| async move {
            if finished {
                return None;
            }
            match requests.next().await? {
                Err(error) => Some((Err(error), (requests, index, true))),
                Ok(request) => match requested_failure(&request) {
                    Some(status) => Some((Err(Error::from(status)), (requests, index, true))),
                    None => Some((
                        Ok(EchoResponse {
                            message: request.message,
                            index,
                        }),
                        (requests, index.saturating_add(1), false),
                    )),
                },
            }
        },
    )
}

const STREAM_COMPRESSED_RECEIVED_METADATA_KEY: &str = "interop-stream-compressed-received";
const PLAIN_COMPRESSED_RECEIVED_METADATA_KEY: &str = "interop-plain-compressed-received";

struct CompressedEchoService;

#[trevrpc::async_trait]
impl CompressedEcho for CompressedEchoService {
    /// `Echo.Bidi`, whose trailing metadata counts the requests that arrived stream-compressed,
    /// known once the request stream has ended. Its HEADERS go out at dispatch, so it sets no
    /// response headers.
    async fn bidi(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<BoxStream<EchoResponse>, Status> {
        let report = futures_util::stream::once(async move {
            context.set_response_metadata(
                STREAM_COMPRESSED_RECEIVED_METADATA_KEY,
                context.stream_compressed_requests_received().to_string(),
            )
        })
        .filter_map(|reported| async move {
            reported.err().map(|status| Err(Error::from(status)))
        });
        Ok(Box::pin(echo_each(requests).chain(report)))
    }

    /// `Echo.ClientStream`, whose trailing metadata counts the requests that arrived
    /// stream-compressed, as `bidi` does.
    async fn client_stream(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<EchoResponse, Status> {
        let response = concatenate(requests).await?;
        context.set_response_metadata(
            STREAM_COMPRESSED_RECEIVED_METADATA_KEY,
            context.stream_compressed_requests_received().to_string(),
        )?;
        Ok(response)
    }

    /// `Echo.Unary`, for the load benchmarks' unary scenario without a dictionary.
    async fn unary(
        &self,
        context: RequestContext,
        request: EchoRequest,
    ) -> Result<EchoResponse, Status> {
        EchoService.unary(context, request).await
    }

    /// Echoes the text, with trailing metadata `1` if the request arrived compressed without a
    /// dictionary and `0` otherwise.
    async fn plain(
        &self,
        context: RequestContext,
        request: PlainMessage,
    ) -> Result<PlainMessage, Status> {
        context.set_response_metadata(
            PLAIN_COMPRESSED_RECEIVED_METADATA_KEY,
            context.plain_compressed_requests_received().to_string(),
        )?;
        Ok(request)
    }
}

/// Registers `RefusingEcho.ClientStream` as `Echo.ClientStream`, by hand and without stream
/// compression, as a server built from a schema without `compress_stream` would: the server
/// refuses every optimistic call to it, and clients replay them.
fn register_refusing_echo(server: &mut Server) {
    server.route_streaming_with_options(
        proto::refusing_echo_paths::CLIENT_STREAM,
        RpcKind::ClientStreaming,
        RouteOptions::new().with_message_types(
            "trevrpc.interop.v1.EchoRequest",
            "trevrpc.interop.v1.EchoResponse",
        ),
        |_context, _body, requests| async move {
            let response = concatenate(trevrpc::stream::decode::<EchoRequest>(requests)).await?;
            Ok(trevrpc::stream::encode(trevrpc::stream::from_iter([
                response,
            ])))
        },
    );
}

const RECEIVED_METADATA_KEY: &str = "interop-received";
const DATAGRAMS_RECEIVED_METADATA_KEY: &str = "interop-datagrams-received";

struct UnreliableEchoService;

#[trevrpc::async_trait]
impl UnreliableEcho for UnreliableEchoService {
    /// Sends `count` responses for each request, in order, as it arrives. The trailing metadata
    /// counts the requests, and how many of them arrived as datagrams, so it is set once the
    /// request stream has ended.
    async fn bidi(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<BoxStream<EchoResponse>, Status> {
        answer_header(&context)?;
        // `(requests, received, context)`: the context is gone once the stream has failed or
        // reported its counts.
        let responses = futures_util::stream::unfold(
            (requests, 0_u64, Some(context)),
            |(mut requests, received, context)| async move {
                let context = context?;
                match requests.next().await {
                    Some(Ok(EchoRequest { message, count, .. })) => {
                        let echoes = (0..count).map(move |index| {
                            Ok(EchoResponse {
                                message: message.clone(),
                                index,
                            })
                        });
                        Some((
                            Either::Left(futures_util::stream::iter(echoes)),
                            (requests, received.saturating_add(1), Some(context)),
                        ))
                    }
                    Some(Err(error)) => Some((
                        Either::Right(futures_util::stream::once(ready(Err(error)))),
                        (requests, received, None),
                    )),
                    None => report_received(&context, received).err().map(|status| {
                        (
                            Either::Right(futures_util::stream::once(ready(Err(Error::from(
                                status,
                            ))))),
                            (requests, received, None),
                        )
                    }),
                }
            },
        )
        .flatten();
        Ok(Box::pin(responses))
    }
}

/// Sets the trailing metadata of an `UnreliableEcho.Bidi` call whose request stream has ended.
fn report_received(context: &RequestContext, received: u64) -> Result<(), Status> {
    context.set_response_metadata(RECEIVED_METADATA_KEY, received.to_string())?;
    context.set_response_metadata(
        DATAGRAMS_RECEIVED_METADATA_KEY,
        context.datagram_requests_received().to_string(),
    )
}

/// Browsers connect from arbitrary origins, so any session on the shared path is admitted.
const fn admit_webtransport(_request: &WebTransportAdmissionRequest<'_>) -> bool {
    true
}

pub async fn run(flags: &Flags) -> Result<(), BoxError> {
    let host: IpAddr = flags.get("host").unwrap_or("127.0.0.1").parse()?;
    let port: u16 = flags.get("port").unwrap_or("0").parse()?;
    let certs =
        CertificateDer::pem_file_iter(flags.require("cert")?)?.collect::<Result<Vec<_>, _>>()?;
    let key = PrivateKeyDer::from_pem_file(flags.require("key")?)?;

    // No `tracing` subscriber is installed, so the per-RPC logs are not emitted and cannot skew
    // the load benchmarks.
    // `--web-root` serves a directory, such as the browser interop page, through the HTTP
    // fallback on the same ports.
    let mut server = Server::new();
    server.set_options(
        ServerOptions::new()
            .with_compression(connect::compression(flags)?)
            .with_http_enabled(true)
            .with_webtransport_enabled(true)
            .with_webtransport_admission(Some(admit_webtransport))
            .with_http_fallback(
                flags
                    .get("web-root")
                    .map(|root| HttpFallback::new(ServeDir::new(root))),
            ),
    );
    proto::register_echo(&mut server, EchoService);
    proto::register_unreliable_echo(&mut server, UnreliableEchoService);
    proto::register_compressed_echo(&mut server, CompressedEchoService);
    register_refusing_echo(&mut server);

    let tls = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::new(host, port),
    )?;
    let udp = endpoint.local_addr()?.port();
    let listener = tokio::net::TcpListener::bind(SocketAddr::new(host, port)).await?;
    let tcp = listener.local_addr()?.port();
    let fallback = TcpFallback {
        listener,
        tls: server.tcp_tls_config(tls),
    };

    let serving = tokio::spawn(server.serve_with_tcp(endpoint, fallback, shutdown_requested()));

    // One UDP socket serves the QUIC protocols and one TCP socket the fallbacks.
    {
        let mut stdout = std::io::stdout().lock();
        writeln!(
            stdout,
            r#"{{"native":{udp},"http3":{udp},"webtransport":{udp},"qmux":{tcp},"http2":{tcp},"websocket":{tcp}}}"#
        )?;
        stdout.flush()?;
    }

    serving.await??;
    Ok(())
}

/// Completes when stdin reaches EOF or the process receives `SIGTERM` or `SIGINT`.
async fn shutdown_requested() {
    let stdin_closed = async {
        let mut stdin = tokio::io::stdin();
        let mut buffer = [0_u8; 256];
        while matches!(stdin.read(&mut buffer).await, Ok(read) if read > 0) {}
    };
    tokio::select! {
        () = stdin_closed => eprintln!("stdin closed; shutting down"),
        () = termination_signal() => eprintln!("signal received; shutting down"),
    }
}

#[cfg(unix)]
async fn termination_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    match (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) {
        (Ok(mut terminate), Ok(mut interrupt)) => {
            tokio::select! {
                _ = terminate.recv() => {}
                _ = interrupt.recv() => {}
            }
        }
        _ => std::future::pending().await,
    }
}

#[cfg(not(unix))]
async fn termination_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
