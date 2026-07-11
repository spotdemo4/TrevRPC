use std::fs;
use std::net::{Ipv6Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use prost::Message;
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::sync::{Barrier, oneshot, watch};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig,
};
use trevrpc_rust_matrix::config::{BenchmarkIdentity, ClientConfig, ServerConfig, Stack};
use trevrpc_rust_matrix::control::{self, ControlClient};
use trevrpc_rust_matrix::events::{HistogramBucket, ReadyEvent, SampleResult};
use trevrpc_rust_matrix::fixtures::{tiny_request, tiny_response};
use trevrpc_rust_matrix::metrics::ProcessSnapshot;
use trevrpc_rust_matrix::proto::benchmark_service_client::BenchmarkServiceClient;
use trevrpc_rust_matrix::proto::benchmark_service_server::{
    BenchmarkService, BenchmarkServiceServer,
};
use trevrpc_rust_matrix::proto::{TinyRequest, TinyResponse};
use trevrpc_rust_matrix::{BoxError, METHOD_NAME, SCHEMA_VERSION, SERVICE_NAME, TINY_VALUE};

#[derive(Clone, Copy, Debug, Default)]
struct Service;

#[tonic::async_trait]
impl BenchmarkService for Service {
    async fn tiny_unary(
        &self,
        request: tonic::Request<TinyRequest>,
    ) -> Result<tonic::Response<TinyResponse>, tonic::Status> {
        Ok(tonic::Response::new(tiny_response(request.into_inner())))
    }
}

#[derive(Clone)]
enum MatrixClient {
    Trevrpc {
        transport: trevrpc::advanced::RawQuinnTransport,
        _endpoint: quinn::Endpoint,
        _connection: quinn::Connection,
    },
    Grpc(BenchmarkServiceClient<Channel>),
}

impl MatrixClient {
    async fn call(&mut self) -> Result<(), BoxError> {
        let request = tiny_request();
        let response = match self {
            Self::Trevrpc { transport, .. } => {
                trevrpc::client::unary::<_, TinyRequest, TinyResponse>(
                    transport,
                    SERVICE_NAME,
                    METHOD_NAME,
                    &request,
                    trevrpc::client::CallOptions::new(),
                )
                .await?
            }
            Self::Grpc(client) => client.tiny_unary(request).await?.into_inner(),
        };
        if response.message != TINY_VALUE {
            return Err(format!("unexpected response payload: {:?}", response.message).into());
        }
        Ok(())
    }
}

#[derive(Debug)]
struct LaneResult {
    completed: u64,
    failed: u64,
    histogram: Histogram<u64>,
}

type ServerTask = (
    String,
    oneshot::Sender<()>,
    tokio::task::JoinHandle<Result<(), String>>,
);

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    match std::env::args().nth(1).as_deref() {
        Some("server") => run_server(ServerConfig::parse()?).await,
        Some("client") => run_client(ClientConfig::parse()?).await,
        Some("check-client") => check_client(ClientConfig::parse()?).await,
        Some("hash-config") => {
            println!("{}", BenchmarkIdentity::parse()?.hash());
            Ok(())
        }
        _ => Err(
            "usage: trevrpc-rust-matrix-peer server|client|check-client|hash-config [options]"
                .into(),
        ),
    }
}

async fn run_server(config: ServerConfig) -> Result<(), BoxError> {
    let control_listener = control::bind(&config.control)?;
    let (address, shutdown, task) = match config.stack {
        Stack::TrevrpcQuinn => start_trevrpc_server(&config)?,
        Stack::GrpcTonicGenerated => start_grpc_server(&config)?,
    };

    let ready = ReadyEvent {
        schema_version: SCHEMA_VERSION,
        event: "ready".to_owned(),
        run_id: config.run_id.clone(),
        config_hash: config.config_hash.clone(),
        stack: config.stack,
        address,
        control_path: config.control.display().to_string(),
        pid: std::process::id(),
    };
    write_ready(&config.ready_file, &ready)?;

    let control_result =
        control::serve_one(control_listener, &config.run_id, &config.config_hash).await;
    let _ = shutdown.send(());
    let server_result = task.await?;
    control_result?;
    server_result.map_err(Into::into)
}

fn start_trevrpc_server(config: &ServerConfig) -> Result<ServerTask, BoxError> {
    let options = trevrpc::server::ServerOptions::new()
        .with_max_concurrent_connections(Some(8))
        .with_max_concurrent_streams_per_connection(Some(256))
        .with_max_concurrent_requests(Some(512));
    let mut server = trevrpc::server::Server::new();
    server.set_options(options);
    server.route(SERVICE_NAME, METHOD_NAME, |body| async move {
        let request = TinyRequest::decode(body.as_slice())?;
        Ok(tiny_response(request).encode_to_vec())
    });

    let certificate = CertificateDer::from_pem_slice(&fs::read(&config.certificate)?)?;
    let private_key = PrivateKeyDer::from_pem_slice(&fs::read(&config.private_key)?)?;
    let mut crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], private_key)?;
    crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let mut endpoint_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
    trevrpc::quinn::configure_server_config(
        &mut endpoint_config,
        server.options(),
        trevrpc::quinn::TransportMode::Native,
    );
    let endpoint = quinn::Endpoint::server(endpoint_config, config.listen.parse()?)?;
    let address = endpoint.local_addr()?.to_string();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_quinn_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|error| error.to_string())
    });
    Ok((address, shutdown_tx, task))
}

fn start_grpc_server(config: &ServerConfig) -> Result<ServerTask, BoxError> {
    let certificate = fs::read(&config.certificate)?;
    let private_key = fs::read(&config.private_key)?;
    let tls = ServerTlsConfig::new().identity(Identity::from_pem(certificate, private_key));
    let incoming =
        TcpIncoming::bind(config.listen.parse::<SocketAddr>()?)?.with_nodelay(Some(true));
    let address = incoming.local_addr()?.to_string();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .tls_config(tls)
            .map_err(|error| error.to_string())?
            .add_service(BenchmarkServiceServer::new(Service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(|error| error.to_string())
    });
    Ok((address, shutdown_tx, task))
}

async fn run_client(config: ClientConfig) -> Result<(), BoxError> {
    let client = connect_client(&config).await?;
    let mut validation_client = client.clone();
    validation_client.call().await?;
    if config.warmup_ms > 0 {
        let (warmup, _) = run_phase(
            client.clone(),
            config.concurrency,
            Duration::from_millis(config.warmup_ms),
            false,
        )
        .await?;
        if warmup.failed != 0 {
            return Err(format!("warmup recorded {} failures", warmup.failed).into());
        }
    }

    let mut control = ControlClient::connect(&config.control, &config.run_id).await?;
    control.begin(&config.config_hash).await?;
    let client_start = ProcessSnapshot::capture()?;
    let (measured, measured_elapsed) = run_phase(
        client,
        config.concurrency,
        Duration::from_millis(config.measurement_ms),
        true,
    )
    .await?;
    let client_metrics = client_start.delta(ProcessSnapshot::capture()?);
    let server_metrics = control.end(measured.completed, measured.failed).await?;

    let admission_ns = config.measurement_ms.saturating_mul(1_000_000);
    let elapsed_ns = u64::try_from(measured_elapsed.as_nanos())?;
    let drain_ns = elapsed_ns.saturating_sub(admission_ns);
    let request_bytes = tiny_request().encoded_len();
    let response_bytes = tiny_response(tiny_request()).encoded_len();
    let throughput_per_s = measured.completed as f64 / (config.measurement_ms as f64 / 1000.0);
    let result = SampleResult {
        schema_version: SCHEMA_VERSION,
        event: "sample".to_owned(),
        run_id: config.run_id,
        config_hash: config.config_hash,
        source_commit: std::env::var("TREVRPC_BENCH_SOURCE_COMMIT")
            .unwrap_or_else(|_| "unknown".to_owned()),
        artifact_sha256: std::env::var("TREVRPC_BENCH_ARTIFACT_SHA256")
            .unwrap_or_else(|_| "unknown".to_owned()),
        stack: config.stack,
        application_encoding: "protobuf".to_owned(),
        workload: "tiny".to_owned(),
        operation: "unary_closed_loop".to_owned(),
        repetition: config.repetition,
        concurrency: config.concurrency,
        connections: 1,
        warmup_ms: config.warmup_ms,
        measurement_ms: config.measurement_ms,
        elapsed_ns,
        drain_ns,
        completed: measured.completed,
        failed: measured.failed,
        throughput_per_s,
        latency_p50_ns: measured.histogram.value_at_quantile(0.50),
        latency_p90_ns: measured.histogram.value_at_quantile(0.90),
        latency_p95_ns: measured.histogram.value_at_quantile(0.95),
        latency_p99_ns: measured.histogram.value_at_quantile(0.99),
        latency_p999_ns: measured.histogram.value_at_quantile(0.999),
        latency_max_ns: measured.histogram.max(),
        application_request_bytes: request_bytes,
        application_response_bytes: response_bytes,
        transport_security_mode: "encrypted".to_owned(),
        certificate_verification_mode: "private-ca-verified".to_owned(),
        batching_policy: "one-request-per-lane".to_owned(),
        network_profile: "loopback".to_owned(),
        client: client_metrics,
        server: server_metrics,
        histogram: histogram_buckets(&measured.histogram),
    };
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}

async fn check_client(config: ClientConfig) -> Result<(), BoxError> {
    let mut client = connect_client(&config).await?;
    client.call().await?;
    let mut control = ControlClient::connect(&config.control, &config.run_id).await?;
    control.begin(&config.config_hash).await?;
    let server = control.end(1, 0).await?;
    println!(
        "{}",
        serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "event": "self_test",
            "run_id": config.run_id,
            "stack": config.stack,
            "server_cpu_ns": server.cpu_ns,
        })
    );
    Ok(())
}

async fn connect_client(config: &ClientConfig) -> Result<MatrixClient, BoxError> {
    match config.stack {
        Stack::TrevrpcQuinn => {
            let certificate = CertificateDer::from_pem_slice(&fs::read(&config.certificate)?)?;
            let mut roots = quinn::rustls::RootCertStore::empty();
            roots.add(certificate)?;
            let mut crypto = quinn::rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
            let mut endpoint =
                quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
            let mut client_config =
                quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
            trevrpc::quinn::configure_client_config(
                &mut client_config,
                trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
                trevrpc::quinn::TransportMode::Native,
            );
            endpoint.set_default_client_config(client_config);
            let connection = endpoint
                .connect(config.address.parse()?, "localhost")?
                .await?;
            let transport = trevrpc::advanced::RawQuinnTransport::new(connection.clone());
            Ok(MatrixClient::Trevrpc {
                transport,
                _endpoint: endpoint,
                _connection: connection,
            })
        }
        Stack::GrpcTonicGenerated => {
            let tls = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(fs::read(&config.certificate)?))
                .domain_name("localhost");
            let endpoint = Endpoint::from_shared(format!("https://{}", config.address))?
                .tls_config(tls)?
                .tcp_nodelay(true);
            Ok(MatrixClient::Grpc(
                BenchmarkServiceClient::connect(endpoint).await?,
            ))
        }
    }
}

async fn run_phase(
    client: MatrixClient,
    concurrency: usize,
    duration: Duration,
    record_latency: bool,
) -> Result<(LaneResult, Duration), BoxError> {
    let barrier = Arc::new(Barrier::new(concurrency + 1));
    let (start_sender, start_receiver) = watch::channel(None::<Instant>);
    let mut tasks = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let mut lane_client = client.clone();
        let lane_barrier = Arc::clone(&barrier);
        let mut lane_start = start_receiver.clone();
        tasks.push(tokio::spawn(async move {
            lane_barrier.wait().await;
            lane_start.changed().await?;
            let start = lane_start
                .borrow()
                .as_ref()
                .copied()
                .ok_or("measurement start was not set")?;
            let deadline = start + duration;
            let mut completed = 0_u64;
            let mut failed = 0_u64;
            let mut histogram = Histogram::<u64>::new_with_bounds(1, 60_000_000_000, 3)?;
            while Instant::now() < deadline {
                let operation_start = Instant::now();
                match lane_client.call().await {
                    Ok(()) => {
                        completed += 1;
                        if record_latency {
                            let latency = u64::try_from(operation_start.elapsed().as_nanos())?;
                            histogram.record(latency.max(1))?;
                        }
                    }
                    Err(error) => {
                        eprintln!("benchmark operation failed: {error}");
                        failed += 1;
                        break;
                    }
                }
            }
            Ok::<_, BoxError>(LaneResult {
                completed,
                failed,
                histogram,
            })
        }));
    }
    barrier.wait().await;
    let phase_start = Instant::now();
    start_sender.send(Some(phase_start))?;

    let mut result = LaneResult {
        completed: 0,
        failed: 0,
        histogram: Histogram::new_with_bounds(1, 60_000_000_000, 3)?,
    };
    for task in tasks {
        let lane = task.await??;
        result.completed += lane.completed;
        result.failed += lane.failed;
        result.histogram.add(&lane.histogram)?;
    }
    let elapsed = phase_start.elapsed();
    Ok((result, elapsed))
}

fn histogram_buckets(histogram: &Histogram<u64>) -> Vec<HistogramBucket> {
    histogram
        .iter_recorded()
        .map(|value| HistogramBucket {
            highest_equivalent_ns: value.value_iterated_to(),
            count: value.count_since_last_iteration(),
        })
        .collect()
}

fn write_ready(path: &Path, event: &ReadyEvent) -> Result<(), BoxError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("tmp");
    fs::write(&temporary, serde_json::to_vec(event)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}
