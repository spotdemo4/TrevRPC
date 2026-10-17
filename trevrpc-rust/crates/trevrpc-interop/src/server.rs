//! The `server` subcommand: `Echo` over native QUIC, HTTP/3 and `WebTransport` on one endpoint.

use std::io::Write as _;
use std::net::{IpAddr, SocketAddr};

use futures_util::StreamExt;
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::AsyncReadExt;
use trevrpc::server::{RequestContext, Server, ServerOptions, WebTransportAdmissionRequest};
use trevrpc::tcp::TcpFallback;
use trevrpc::{BoxStream, Code, Error, Metadata, ResponseEnvelope, Status};

use crate::proto::{self, Echo, EchoRequest, EchoResponse};
use crate::{BoxError, Flags};

const SUFFIX_METADATA_KEY: &str = "interop-suffix";
const HEADER_METADATA_KEY: &str = "interop-header";
const TRAILER_METADATA_KEY: &str = "interop-trailer";

struct EchoService;

/// Wraps a response in an envelope that honors the `interop-header` request metadata.
///
/// When the request carries the key with value `v`, the response headers are
/// `{interop-header: v}` and the `OK` status carries trailing metadata `{interop-trailer: v}`.
/// Without it the envelope has neither.
fn envelope<T>(context: &RequestContext, message: T) -> ResponseEnvelope<T> {
    let Some(value) = context.metadata().get(HEADER_METADATA_KEY) else {
        return ResponseEnvelope::new(message);
    };
    ResponseEnvelope::new(message)
        .with_headers(Metadata::from([(
            HEADER_METADATA_KEY.to_owned(),
            value.clone(),
        )]))
        .with_metadata(Metadata::from([(
            TRAILER_METADATA_KEY.to_owned(),
            value.clone(),
        )]))
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
    ) -> Result<ResponseEnvelope<EchoResponse>, Status> {
        if let Some(status) = requested_failure(&request) {
            return Err(status);
        }
        let suffix = context
            .metadata()
            .get(SUFFIX_METADATA_KEY)
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .unwrap_or_default();
        Ok(envelope(
            &context,
            EchoResponse {
                message: request.message + &suffix,
                index: 0,
            },
        ))
    }

    async fn client_stream(
        &self,
        context: RequestContext,
        mut requests: BoxStream<EchoRequest>,
    ) -> Result<ResponseEnvelope<EchoResponse>, Status> {
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
        Ok(envelope(
            &context,
            EchoResponse {
                message,
                index: count,
            },
        ))
    }

    async fn server_stream(
        &self,
        context: RequestContext,
        request: EchoRequest,
    ) -> Result<ResponseEnvelope<BoxStream<EchoResponse>>, Status> {
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
        Ok(envelope(
            &context,
            Box::pin(futures_util::stream::iter(responses)),
        ))
    }

    async fn bidi(
        &self,
        context: RequestContext,
        requests: BoxStream<EchoRequest>,
    ) -> Result<ResponseEnvelope<BoxStream<EchoResponse>>, Status> {
        // `(requests, next index, finished)`: a failing request ends the stream with its status.
        let responses = futures_util::stream::unfold(
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
        );
        Ok(envelope(&context, Box::pin(responses)))
    }
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

    let mut server = Server::new();
    server.set_options(
        ServerOptions::new()
            .with_http_enabled(true)
            .with_webtransport_enabled(true)
            .with_webtransport_admission(Some(admit_webtransport)),
    );
    proto::register_echo(&mut server, EchoService);

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
