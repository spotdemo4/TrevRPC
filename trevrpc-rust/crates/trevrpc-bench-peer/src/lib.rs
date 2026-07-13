#![forbid(unsafe_code)]

mod config;
mod histogram;
mod proto;
mod workload;

use std::error::Error;
use std::fmt;
use std::fs;
use std::io::{self, Write};
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process;
use std::sync::Arc;
use std::time::{Duration, Instant};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Barrier, watch};
use tokio::task::JoinHandle;

use config::{
    ClientConfig, Command, MAX_ENCODED_FRAME_BYTES, MAX_MESSAGES_PER_STREAM, ServerConfig,
};
use histogram::{HistogramBucket, LogLinearHistogram};
use workload::{BenchmarkServiceImpl, MessageCounts, Workload, WorkloadConfig};

const SCHEMA_VERSION: u8 = 1;
const PEER: &str = "rust";
const SERVER_MAX_STREAMS: usize = 1024;
const SERVER_MAX_REQUESTS: usize = 4096;

type PeerResult<T = ()> = Result<T, PeerError>;

#[derive(Debug)]
pub struct PeerError {
    phase: &'static str,
    code: &'static str,
    message: String,
}

impl PeerError {
    fn new(phase: &'static str, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            phase,
            code,
            message: message.into(),
        }
    }

    fn wrap(phase: &'static str, code: &'static str, error: impl fmt::Display) -> Self {
        Self::new(phase, code, error.to_string())
    }
}

impl fmt::Display for PeerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

impl Error for PeerError {}

pub async fn run(args: impl IntoIterator<Item = String>) -> PeerResult {
    let command = config::parse(args)
        .map_err(|error| PeerError::wrap("config", "invalid_argument", error))?;
    match command {
        Command::Capabilities => emit(&CapabilitiesEvent {
            schema_version: SCHEMA_VERSION,
            event: "capabilities",
            peer: PEER,
            roles: ["client", "server"],
            rpc_kinds: ["unary", "client_stream", "server_stream", "bidi"],
            transports: ["native_quic"],
            histogram: "log_linear_v1",
        }),
        Command::Server(config) => run_server(config).await,
        Command::Client(config) => run_client(config).await,
    }
}

pub fn emit_error(error: &PeerError) {
    let event = ErrorEvent {
        schema_version: SCHEMA_VERSION,
        event: "error",
        peer: PEER,
        phase: error.phase,
        code: error.code,
        message: &error.message,
    };
    if let Err(emit_error) = emit(&event) {
        eprintln!("failed to emit error event: {emit_error}");
    }
}

async fn run_server(config: ServerConfig) -> PeerResult {
    let mut server = trevrpc::server::Server::new();
    server.set_options(
        trevrpc::server::ServerOptions::new()
            .with_max_frame_size(MAX_ENCODED_FRAME_BYTES)
            .with_max_concurrent_connections(Some(8))
            .with_max_concurrent_streams_per_connection(Some(SERVER_MAX_STREAMS))
            .with_max_concurrent_requests(Some(SERVER_MAX_REQUESTS))
            .with_max_stream_messages(Some(MAX_MESSAGES_PER_STREAM as usize))
            .with_max_stream_body_size(None),
    );
    proto::register_benchmark_service(&mut server, BenchmarkServiceImpl);

    let certificate_pem = fs::read(&config.certificate)
        .map_err(|error| PeerError::wrap("server", "certificate_read_failed", error))?;
    let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| PeerError::wrap("server", "certificate_parse_failed", error))?;
    if certificates.is_empty() {
        return Err(PeerError::new(
            "server",
            "certificate_parse_failed",
            "certificate file contained no certificates",
        ));
    }
    let private_key_pem = fs::read(&config.private_key)
        .map_err(|error| PeerError::wrap("server", "private_key_read_failed", error))?;
    let private_key = PrivateKeyDer::from_pem_slice(&private_key_pem)
        .map_err(|error| PeerError::wrap("server", "private_key_parse_failed", error))?;
    let mut crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, private_key)
        .map_err(|error| PeerError::wrap("server", "tls_config_failed", error))?;
    crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(
        QuicServerConfig::try_from(crypto)
            .map_err(|error| PeerError::wrap("server", "tls_config_failed", error))?,
    ));
    trevrpc::quinn::configure_server_config(
        &mut server_config,
        server.options(),
        trevrpc::quinn::TransportMode::Native,
    );
    let server_transport = Arc::get_mut(&mut server_config.transport).ok_or_else(|| {
        PeerError::new(
            "config",
            "transport_config_failed",
            "benchmark server transport configuration is unexpectedly shared",
        )
    })?;
    configure_transport_lifetime(server_transport)?;
    let endpoint = quinn::Endpoint::server(server_config, config.listen)
        .map_err(|error| PeerError::wrap("server", "listen_failed", error))?;
    let address = endpoint
        .local_addr()
        .map_err(|error| PeerError::wrap("server", "listen_failed", error))?;
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let mut server_task = tokio::spawn(async move {
        server
            .serve_quinn_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    emit(&ReadyEvent {
        schema_version: SCHEMA_VERSION,
        event: "ready",
        peer: PEER,
        address: address.to_string(),
        pid: process::id(),
    })?;

    let control = tokio::select! {
        control = wait_for_server_control() => control,
        result = &mut server_task => {
            return finish_server_task(result, false);
        }
    };
    let _ = shutdown_tx.send(());
    let server_result = server_task.await;
    let protocol_shutdown = match control {
        Ok(protocol_shutdown) => protocol_shutdown,
        Err(error) => {
            let _ = finish_server_task(server_result, false);
            return Err(error);
        }
    };
    finish_server_task(server_result, protocol_shutdown)
}

fn finish_server_task(
    result: Result<trevrpc::Result<()>, tokio::task::JoinError>,
    emit_stopped: bool,
) -> PeerResult {
    result
        .map_err(|error| PeerError::wrap("server", "server_task_failed", error))?
        .map_err(|error| PeerError::wrap("server", "serve_failed", error))?;
    if emit_stopped {
        emit(&StoppedEvent {
            schema_version: SCHEMA_VERSION,
            event: "stopped",
            peer: PEER,
        })?;
    }
    Ok(())
}

async fn wait_for_server_control() -> PeerResult<bool> {
    let shutdown = read_expected_command("SHUTDOWN");
    tokio::pin!(shutdown);

    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .map_err(|error| PeerError::wrap("control", "signal_setup_failed", error))?;
        tokio::select! {
            result = &mut shutdown => result.map(|()| true),
            result = tokio::signal::ctrl_c() => {
                result.map_err(|error| PeerError::wrap("control", "signal_failed", error))?;
                Ok(false)
            }
            _ = terminate.recv() => Ok(false),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            result = &mut shutdown => result.map(|()| true),
            result = tokio::signal::ctrl_c() => {
                result.map_err(|error| PeerError::wrap("control", "signal_failed", error))?;
                Ok(false)
            }
        }
    }
}

async fn run_client(config: ClientConfig) -> PeerResult {
    let connected = connect_client(&config).await?;
    connected
        .workload
        .execute()
        .await
        .map_err(|error| PeerError::wrap("validate", "rpc_failed", error))?;

    if config.warmup_ms > 0 {
        let warmup = PreparedPhase::new(
            connected.workload.clone(),
            config.concurrency,
            Duration::from_millis(config.warmup_ms),
            false,
        )
        .await?
        .start()
        .await?;
        if warmup.failed != 0 {
            return Err(PeerError::new(
                "warmup",
                "rpc_failed",
                format!("warmup recorded {} failed operations", warmup.failed),
            ));
        }
    }

    let measured = PreparedPhase::new(
        connected.workload.clone(),
        config.concurrency,
        Duration::from_millis(config.measurement_ms),
        true,
    )
    .await?;
    emit(&ArmedEvent {
        schema_version: SCHEMA_VERSION,
        event: "armed",
        peer: PEER,
        pid: process::id(),
    })?;
    read_expected_command("START").await?;
    let measured = measured.start().await?;

    if measured.histogram.count() != measured.completed {
        return Err(PeerError::new(
            "measure",
            "histogram_count_mismatch",
            format!(
                "histogram count {} did not match {} completed operations",
                measured.histogram.count(),
                measured.completed
            ),
        ));
    }
    if measured.failed != 0 {
        return Err(PeerError::new(
            "measure",
            "rpc_failed",
            format!("measurement recorded {} failed operations", measured.failed),
        ));
    }
    let admission = Duration::from_millis(config.measurement_ms);
    emit(&SampleEvent {
        schema_version: SCHEMA_VERSION,
        event: "sample",
        peer: PEER,
        rpc_kind: config.rpc.as_str(),
        admission_ns: admission.as_nanos().to_string(),
        elapsed_ns: measured.elapsed.as_nanos().to_string(),
        drain_ns: measured
            .elapsed
            .saturating_sub(admission)
            .as_nanos()
            .to_string(),
        completed: measured.completed.to_string(),
        failed: measured.failed.to_string(),
        request_messages: measured.messages.request.to_string(),
        response_messages: measured.messages.response.to_string(),
        histogram: measured.histogram.into_buckets(),
    })?;

    connected
        .connection
        .close(0_u32.into(), b"benchmark complete");
    connected.endpoint.wait_idle().await;
    Ok(())
}

struct ConnectedClient {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    workload: Workload,
}

async fn connect_client(config: &ClientConfig) -> PeerResult<ConnectedClient> {
    let certificate_pem = fs::read(&config.certificate)
        .map_err(|error| PeerError::wrap("connect", "certificate_read_failed", error))?;
    let certificates = CertificateDer::pem_slice_iter(&certificate_pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| PeerError::wrap("connect", "certificate_parse_failed", error))?;
    if certificates.is_empty() {
        return Err(PeerError::new(
            "connect",
            "certificate_parse_failed",
            "CA file contained no certificates",
        ));
    }
    let mut roots = quinn::rustls::RootCertStore::empty();
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| PeerError::wrap("connect", "certificate_rejected", error))?;
    }
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let local_address = if config.address.is_ipv4() {
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
    } else {
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
    };
    let mut endpoint = quinn::Endpoint::client(local_address)
        .map_err(|error| PeerError::wrap("connect", "endpoint_failed", error))?;
    let mut client_config = quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(crypto)
            .map_err(|error| PeerError::wrap("connect", "tls_config_failed", error))?,
    ));
    let mut client_transport = quinn::TransportConfig::default();
    trevrpc::quinn::apply_transport_limits(
        &mut client_transport,
        trevrpc::quinn::client_transport_limits(
            MAX_ENCODED_FRAME_BYTES,
            trevrpc::quinn::TransportMode::Native,
        ),
    );
    configure_transport_lifetime(&mut client_transport)?;
    client_config.transport_config(Arc::new(client_transport));
    endpoint.set_default_client_config(client_config);
    let connection = endpoint
        .connect(config.address, "localhost")
        .map_err(|error| PeerError::wrap("connect", "connect_start_failed", error))?
        .await
        .map_err(|error| PeerError::wrap("connect", "tls_connect_failed", error))?;
    let workload = Workload::new(
        trevrpc::advanced::RawQuinnTransport::new(connection.clone())
            .with_max_frame_size(MAX_ENCODED_FRAME_BYTES),
        WorkloadConfig {
            rpc: config.rpc,
            request_bytes: config.request_bytes,
            response_bytes: config.response_bytes,
            messages_per_stream: config.messages_per_stream,
        },
    );
    Ok(ConnectedClient {
        endpoint,
        connection,
        workload,
    })
}

fn configure_transport_lifetime(transport: &mut quinn::TransportConfig) -> PeerResult {
    let idle_timeout = Duration::from_mins(10)
        .try_into()
        .map_err(|error| PeerError::wrap("config", "transport_config_failed", error))?;
    transport.max_idle_timeout(Some(idle_timeout));
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    Ok(())
}

struct PreparedPhase {
    start: watch::Sender<Option<Instant>>,
    tasks: Vec<JoinHandle<LaneResult>>,
}

impl PreparedPhase {
    async fn new(
        workload: Workload,
        concurrency: usize,
        duration: Duration,
        record_latency: bool,
    ) -> PeerResult<Self> {
        let barrier = Arc::new(Barrier::new(concurrency + 1));
        let (start, start_receiver) = watch::channel(None::<Instant>);
        let mut tasks = Vec::with_capacity(concurrency);
        for _ in 0..concurrency {
            let workload = workload.clone();
            let barrier = Arc::clone(&barrier);
            let mut start_receiver = start_receiver.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                if start_receiver.changed().await.is_err() {
                    return LaneResult {
                        failed: 1,
                        ..LaneResult::default()
                    };
                }
                let Some(phase_start) = *start_receiver.borrow() else {
                    return LaneResult {
                        failed: 1,
                        ..LaneResult::default()
                    };
                };
                run_lane(&workload, phase_start, duration, record_latency).await
            }));
        }
        barrier.wait().await;
        Ok(Self { start, tasks })
    }

    async fn start(self) -> PeerResult<PhaseResult> {
        let phase_start = Instant::now();
        self.start
            .send(Some(phase_start))
            .map_err(|error| PeerError::wrap("measure", "lane_start_failed", error))?;
        let mut aggregate = LaneResult::default();
        for task in self.tasks {
            let lane = task
                .await
                .map_err(|error| PeerError::wrap("measure", "lane_task_failed", error))?;
            aggregate.merge(lane);
        }
        Ok(PhaseResult {
            elapsed: phase_start.elapsed(),
            completed: aggregate.completed,
            failed: aggregate.failed,
            messages: aggregate.messages,
            histogram: aggregate.histogram,
        })
    }
}

#[derive(Debug, Default)]
struct LaneResult {
    completed: u64,
    failed: u64,
    messages: MessageCounts,
    histogram: LogLinearHistogram,
}

impl LaneResult {
    fn merge(&mut self, other: Self) {
        self.completed = self.completed.saturating_add(other.completed);
        self.failed = self.failed.saturating_add(other.failed);
        self.messages.request = self.messages.request.saturating_add(other.messages.request);
        self.messages.response = self
            .messages
            .response
            .saturating_add(other.messages.response);
        self.histogram.merge(other.histogram);
    }
}

struct PhaseResult {
    elapsed: Duration,
    completed: u64,
    failed: u64,
    messages: MessageCounts,
    histogram: LogLinearHistogram,
}

async fn run_lane(
    workload: &Workload,
    phase_start: Instant,
    duration: Duration,
    record_latency: bool,
) -> LaneResult {
    let deadline = phase_start + duration;
    let mut result = LaneResult::default();
    loop {
        let operation_start = Instant::now();
        if operation_start >= deadline {
            break;
        }
        match workload.execute().await {
            Ok(messages) => {
                result.completed = result.completed.saturating_add(1);
                result.messages.request = result.messages.request.saturating_add(messages.request);
                result.messages.response =
                    result.messages.response.saturating_add(messages.response);
                if record_latency {
                    let latency = u64::try_from(operation_start.elapsed().as_nanos())
                        .unwrap_or(u64::MAX)
                        .max(1);
                    result.histogram.record(latency);
                }
            }
            Err(error) => {
                eprintln!("benchmark operation failed: {error}");
                result.failed = result.failed.saturating_add(1);
                break;
            }
        }
    }
    result
}

async fn read_expected_command(expected: &'static str) -> PeerResult {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let line = lines
        .next_line()
        .await
        .map_err(|error| PeerError::wrap("control", "stdin_read_failed", error))?
        .ok_or_else(|| PeerError::new("control", "stdin_closed", "standard input closed"))?;
    if line == expected {
        Ok(())
    } else {
        Err(PeerError::new(
            "control",
            "unexpected_command",
            format!("expected {expected}, received {line:?}"),
        ))
    }
}

fn emit(event: &impl Serialize) -> PeerResult {
    let stdout = io::stdout();
    let mut stdout = stdout.lock();
    serde_json::to_writer(&mut stdout, event)
        .map_err(|error| PeerError::wrap("protocol", "json_encode_failed", error))?;
    stdout
        .write_all(b"\n")
        .and_then(|()| stdout.flush())
        .map_err(|error| PeerError::wrap("protocol", "stdout_write_failed", error))
}

#[derive(Serialize)]
struct CapabilitiesEvent {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
    roles: [&'static str; 2],
    rpc_kinds: [&'static str; 4],
    transports: [&'static str; 1],
    histogram: &'static str,
}

#[derive(Serialize)]
struct ReadyEvent {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
    address: String,
    pid: u32,
}

#[derive(Serialize)]
struct ArmedEvent {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
    pid: u32,
}

#[derive(Serialize)]
struct StoppedEvent {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
}

#[derive(Serialize)]
struct SampleEvent {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
    rpc_kind: &'static str,
    admission_ns: String,
    elapsed_ns: String,
    drain_ns: String,
    completed: String,
    failed: String,
    request_messages: String,
    response_messages: String,
    histogram: Vec<HistogramBucket>,
}

#[derive(Serialize)]
struct ErrorEvent<'a> {
    schema_version: u8,
    event: &'static str,
    peer: &'static str,
    phase: &'static str,
    code: &'static str,
    message: &'a str,
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;

    use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
    use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, IsCa, KeyPair};
    use tokio::sync::oneshot;

    use crate::PreparedPhase;
    use crate::config::RpcKind;
    use crate::workload::{BenchmarkServiceImpl, MessageCounts, Workload, WorkloadConfig};

    type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

    #[tokio::test]
    #[allow(clippy::too_many_lines)]
    async fn private_ca_quinn_round_trips_all_rpc_kinds() -> TestResult {
        let mut ca_params = CertificateParams::default();
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate()?)?;
        let server_key = KeyPair::generate()?;
        let certificate =
            CertificateParams::new(vec!["localhost".to_owned()])?.signed_by(&server_key, &ca)?;
        let certificate_der = CertificateDer::from(certificate);
        let private_key = PrivatePkcs8KeyDer::from(server_key.serialize_der());

        let mut server = trevrpc::server::Server::new();
        server.set_options(
            trevrpc::server::ServerOptions::new()
                .with_graceful_shutdown_timeout(Some(Duration::from_secs(1))),
        );
        crate::proto::register_benchmark_service(&mut server, BenchmarkServiceImpl);
        let mut server_crypto = quinn::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![certificate_der.clone()],
                PrivateKeyDer::from(private_key),
            )?;
        server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
        let mut server_config =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
        trevrpc::quinn::configure_server_config(
            &mut server_config,
            server.options(),
            trevrpc::quinn::TransportMode::Native,
        );
        let server_endpoint =
            quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let server_address = server_endpoint.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let server_task = tokio::spawn(async move {
            server
                .serve_quinn_with_shutdown(server_endpoint, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut roots = quinn::rustls::RootCertStore::empty();
        roots.add(ca.der().clone())?;
        let mut client_crypto = quinn::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
        let mut client_endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
        let mut client_config =
            quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
        trevrpc::quinn::configure_client_config(
            &mut client_config,
            trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
            trevrpc::quinn::TransportMode::Native,
        );
        client_endpoint.set_default_client_config(client_config);
        let connection = client_endpoint
            .connect(server_address, "localhost")?
            .await?;
        let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());

        for (rpc, expected) in [
            (
                RpcKind::Unary,
                MessageCounts {
                    request: 1,
                    response: 1,
                },
            ),
            (
                RpcKind::ClientStream,
                MessageCounts {
                    request: 4,
                    response: 1,
                },
            ),
            (
                RpcKind::ServerStream,
                MessageCounts {
                    request: 1,
                    response: 4,
                },
            ),
            (
                RpcKind::Bidi,
                MessageCounts {
                    request: 4,
                    response: 4,
                },
            ),
        ] {
            let workload = Workload::new(
                transport.clone(),
                WorkloadConfig {
                    rpc,
                    request_bytes: 17,
                    response_bytes: 23,
                    messages_per_stream: 4,
                },
            );
            assert_eq!(workload.execute().await?, expected);
        }

        let measured_workload = Workload::new(
            transport,
            WorkloadConfig {
                rpc: RpcKind::Unary,
                request_bytes: 17,
                response_bytes: 23,
                messages_per_stream: 4,
            },
        );
        let admission = Duration::from_millis(20);
        let measured = PreparedPhase::new(measured_workload, 4, admission, true)
            .await?
            .start()
            .await?;
        assert!(measured.completed > 0);
        assert_eq!(measured.failed, 0);
        assert_eq!(measured.histogram.count(), measured.completed);
        assert_eq!(measured.messages.request, measured.completed);
        assert_eq!(measured.messages.response, measured.completed);
        assert!(measured.elapsed >= admission);

        connection.close(0_u32.into(), b"test complete");
        client_endpoint.wait_idle().await;
        let _ = shutdown_tx.send(());
        tokio::time::timeout(Duration::from_secs(2), server_task).await???;
        Ok(())
    }
}
