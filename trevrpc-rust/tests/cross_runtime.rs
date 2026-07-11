#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::io::{BufRead, BufReader};
use std::net::{Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::thread::JoinHandle as ThreadJoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::pem::PemObject;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::client::CallOptions;
use trevrpc::server::{MetadataValueAuthorizer, Server, ServerOptions};
use trevrpc::{Code, MessageStream, RpcResponse, Status};

#[path = "../examples/shared/greeter.rs"]
mod greeter;

const AUTH_TOKEN: &str = "cross-runtime-token";
const TEST_TIMEOUT: Duration = Duration::from_secs(10);

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

struct CrossRuntimeGreeter;

#[trevrpc::async_trait]
impl greeter::Greeter for CrossRuntimeGreeter {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        Ok(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
    }

    async fn lots_of_replies(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, Status> {
        Ok(trevrpc::stream::from_iter([
            greeter::HelloReply {
                message: format!("hello, {}", request.name),
            },
            greeter::HelloReply {
                message: format!("goodbye, {}", request.name),
            },
        ]))
    }

    async fn lots_of_greetings(
        &self,
        mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, Status> {
        let mut names = Vec::new();
        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        Ok(greeter::HelloReply {
            message: names.join(","),
        })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, Status> {
        Ok(Box::new(EchoReplies { requests }))
    }
}

struct EchoReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl MessageStream<greeter::HelloReply> for EchoReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: format!("echo, {}", request.name),
            })
        })
    }
}

#[derive(Clone, Copy)]
enum ExternalRuntime {
    Go,
    Kotlin,
}

impl ExternalRuntime {
    const fn name(self) -> &'static str {
        match self {
            Self::Go => "Go",
            Self::Kotlin => "Kotlin",
        }
    }

    const fn writes_certificate_line(self) -> bool {
        matches!(self, Self::Kotlin)
    }

    const fn slug(self) -> &'static str {
        match self {
            Self::Go => "go",
            Self::Kotlin => "kotlin",
        }
    }
}

struct ProcessServer {
    child: Child,
    addr: SocketAddr,
    stdout_drain: Option<ThreadJoinHandle<std::io::Result<u64>>>,
    stopped: bool,
}

impl ProcessServer {
    fn shutdown(mut self) -> TestResult {
        self.stop()
    }

    fn stop(&mut self) -> TestResult {
        if self.stopped {
            return Ok(());
        }
        if self.child.try_wait()?.is_none() {
            self.child.kill()?;
        }
        self.child.wait()?;
        if let Some(stdout_drain) = self.stdout_drain.take() {
            stdout_drain
                .join()
                .map_err(|_| "server stdout drain thread panicked")??;
        }
        self.stopped = true;
        Ok(())
    }
}

impl Drop for ProcessServer {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}

struct RustServer {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<trevrpc::Result<()>>>,
}

impl RustServer {
    async fn shutdown(mut self) -> TestResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(TEST_TIMEOUT, task).await???;
        }

        Ok(())
    }
}

impl Drop for RustServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
        self.endpoint.close(0_u32.into(), b"test shutdown");
    }
}

#[tokio::test]
async fn rust_client_talks_to_go_server_over_quic() -> TestResult {
    let Some(go_binary) = go_binary() else {
        return Ok(());
    };
    rust_client_talks_to_external_server(go_binary, ExternalRuntime::Go).await
}

#[tokio::test]
async fn go_client_talks_to_rust_server_over_quic() -> TestResult {
    let Some(go_binary) = go_binary() else {
        return Ok(());
    };
    external_client_talks_to_rust_server(go_binary, ExternalRuntime::Go).await
}

#[tokio::test]
async fn rust_client_talks_to_kotlin_server_over_quic() -> TestResult {
    let Some(kotlin_binary) = kotlin_binary() else {
        return Ok(());
    };
    rust_client_talks_to_external_server(kotlin_binary, ExternalRuntime::Kotlin).await
}

#[tokio::test]
async fn kotlin_client_talks_to_rust_server_over_quic() -> TestResult {
    let Some(kotlin_binary) = kotlin_binary() else {
        return Ok(());
    };
    external_client_talks_to_rust_server(kotlin_binary, ExternalRuntime::Kotlin).await
}

#[tokio::test]
#[ignore = "scheduled/manual cross-runtime lifecycle stress"]
async fn cross_runtime_lifecycle_stress() -> TestResult {
    let binaries = [
        (go_binary(), ExternalRuntime::Go),
        (kotlin_binary(), ExternalRuntime::Kotlin),
    ];
    if binaries.iter().all(|(binary, _)| binary.is_none()) {
        return Ok(());
    }
    let iterations = lifecycle_iterations()?;

    for (binary, runtime) in binaries {
        if let Some(binary) = binary {
            stress_external_runtime_lifecycle(binary, runtime, iterations).await?;
        }
    }
    Ok(())
}

fn go_binary() -> Option<PathBuf> {
    std::env::var_os("TREVRPC_XRUNTIME_GO").map(PathBuf::from)
}

fn kotlin_binary() -> Option<PathBuf> {
    std::env::var_os("TREVRPC_XRUNTIME_KOTLIN").map(PathBuf::from)
}

async fn rust_client_talks_to_external_server(
    binary: PathBuf,
    runtime: ExternalRuntime,
) -> TestResult {
    let cert_path = temp_cert_path(&format!("{}-server", runtime.slug()));
    let server = spawn_external_server(&binary, &cert_path, runtime)?;
    let result: TestResult = async {
        let (endpoint, connection, client) = connect_rust_client(server.addr, &cert_path).await?;

        exercise_rust_client(&client).await?;
        expect_rust_auth_failure(&client).await;
        expect_rust_protocol_error(&connection).await?;

        connection.close(0_u32.into(), b"test complete");
        endpoint.wait_idle().await;
        Ok(())
    }
    .await;
    let shutdown = server.shutdown();
    let _ = std::fs::remove_file(cert_path);
    result?;
    shutdown
}

async fn external_client_talks_to_rust_server(
    binary: PathBuf,
    runtime: ExternalRuntime,
) -> TestResult {
    let cert_path = temp_cert_path(&format!("rust-server-for-{}", runtime.slug()));
    let rust_server = spawn_rust_server(&cert_path)?;
    let result = run_external_client(
        binary,
        runtime,
        "client",
        rust_server.addr,
        &cert_path,
        None,
    )
    .await;
    let shutdown = rust_server.shutdown().await;
    let _ = std::fs::remove_file(cert_path);
    result?;
    shutdown
}

async fn stress_external_runtime_lifecycle(
    binary: PathBuf,
    runtime: ExternalRuntime,
    iterations: usize,
) -> TestResult {
    let external_cert_path = temp_cert_path(&format!("{}-lifecycle-server", runtime.slug()));
    let external_server = spawn_external_server(&binary, &external_cert_path, runtime)?;
    let rust_client_result: TestResult = async {
        for iteration in 0..iterations {
            let (endpoint, connection, client) =
                connect_rust_client(external_server.addr, &external_cert_path)
                    .await
                    .map_err(|error| {
                        format!(
                            "Rust client -> {} lifecycle iteration {} connect: {error}",
                            runtime.name(),
                            iteration + 1
                        )
                    })?;
            stress_rust_client_lifecycle(&client)
                .await
                .map_err(|error| {
                    format!(
                        "Rust client -> {} lifecycle iteration {} failed: {error}",
                        runtime.name(),
                        iteration + 1
                    )
                })?;
            connection.close(0_u32.into(), b"lifecycle iteration complete");
            endpoint.wait_idle().await;
        }
        Ok(())
    }
    .await;
    let external_shutdown = external_server.shutdown();
    let _ = std::fs::remove_file(external_cert_path);
    rust_client_result?;
    external_shutdown?;

    let rust_cert_path = temp_cert_path(&format!("rust-lifecycle-server-for-{}", runtime.slug()));
    let rust_server = spawn_rust_server(&rust_cert_path)?;
    let external_client_result = run_external_client(
        binary,
        runtime,
        "lifecycle-client",
        rust_server.addr,
        &rust_cert_path,
        Some(iterations),
    )
    .await;
    let rust_shutdown = rust_server.shutdown().await;
    let _ = std::fs::remove_file(rust_cert_path);
    external_client_result?;
    rust_shutdown
}

async fn run_external_client(
    binary: PathBuf,
    runtime: ExternalRuntime,
    mode: &'static str,
    addr: SocketAddr,
    cert_path: &Path,
    iterations: Option<usize>,
) -> TestResult {
    let cert_path = cert_path.to_owned();
    let output = tokio::task::spawn_blocking(move || {
        let mut command = Command::new(binary);
        command
            .arg("-mode")
            .arg(mode)
            .arg("-addr")
            .arg(addr.to_string())
            .arg("-cert")
            .arg(cert_path)
            .arg("-token")
            .arg(AUTH_TOKEN);
        if let Some(iterations) = iterations {
            command.arg("-iterations").arg(iterations.to_string());
        }
        command.output()
    })
    .await??;

    if !output.status.success() {
        return Err(format!(
            "{} {mode} failed with status {}\nstdout:\n{}\nstderr:\n{}",
            runtime.name(),
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    Ok(())
}

fn lifecycle_iterations() -> TestResult<usize> {
    let value =
        std::env::var("TREVRPC_XRUNTIME_LIFECYCLE_ITERATIONS").unwrap_or_else(|_| "25".to_owned());
    let iterations = value.parse::<usize>()?;
    if iterations == 0 {
        return Err("TREVRPC_XRUNTIME_LIFECYCLE_ITERATIONS must be positive".into());
    }

    Ok(iterations)
}

fn temp_cert_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trevrpc-cross-runtime-{name}-{unique}.pem"))
}

fn spawn_external_server(
    binary: &Path,
    cert_path: &Path,
    runtime: ExternalRuntime,
) -> TestResult<ProcessServer> {
    let mut child = Command::new(binary)
        .arg("-mode")
        .arg("server")
        .arg("-addr")
        .arg("127.0.0.1:0")
        .arg("-cert")
        .arg(cert_path)
        .arg("-token")
        .arg(AUTH_TOKEN)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()?;

    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill();
        let _ = child.wait();
        return Err(format!("{} server stdout should be piped", runtime.name()).into());
    };
    let mut stdout = BufReader::new(stdout);
    let startup = (|| -> TestResult<SocketAddr> {
        let mut ready = String::new();
        stdout.read_line(&mut ready)?;
        let Some(addr) = ready.strip_prefix("READY ") else {
            return Err(
                format!("unexpected {} server ready line: {ready:?}", runtime.name()).into(),
            );
        };
        let addr = addr.trim().parse::<SocketAddr>()?;

        if runtime.writes_certificate_line() {
            let mut certificate = String::new();
            stdout.read_line(&mut certificate)?;
            let expected = format!("certificate written to {}", cert_path.display());
            if certificate.trim() != expected {
                return Err(format!(
                    "unexpected {} server certificate line: {certificate:?}; expected {expected:?}",
                    runtime.name()
                )
                .into());
            }
        }

        Ok(addr)
    })();
    let addr = match startup {
        Ok(addr) => addr,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }
    };
    let stdout_drain = std::thread::spawn(move || std::io::copy(&mut stdout, &mut std::io::sink()));

    Ok(ProcessServer {
        child,
        addr,
        stdout_drain: Some(stdout_drain),
        stopped: false,
    })
}

async fn connect_rust_client(
    addr: SocketAddr,
    cert_path: &Path,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
)> {
    let endpoint = make_client_endpoint(cert_path)?;
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let client = greeter::GreeterClient::new(trevrpc::advanced::RawQuinnTransport::new(
        connection.clone(),
    ));

    Ok((endpoint, connection, client))
}

fn make_client_endpoint(cert_path: &Path) -> TestResult<quinn::Endpoint> {
    let cert_der = CertificateDer::from_pem_slice(&std::fs::read(cert_path)?)?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;

    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)))?;
    let mut client_config =
        quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(client_crypto)?));
    trevrpc::quinn::configure_client_config(
        &mut client_config,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
        trevrpc::quinn::TransportMode::Native,
    );
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn spawn_rust_server(cert_path: &Path) -> TestResult<RustServer> {
    let identity = make_identity()?;
    write_certificate(&identity, cert_path)?;
    let mut server = Server::new();
    let options = ServerOptions::new()
        .with_max_concurrent_connections(Some(64))
        .with_max_concurrent_streams_per_connection(Some(64))
        .with_max_concurrent_requests(Some(128));
    server.set_options(options);
    server.set_authorizer(MetadataValueAuthorizer::bearer(AUTH_TOKEN));
    greeter::register_greeter(&mut server, CrossRuntimeGreeter);
    let endpoint = make_server_endpoint(&identity, server.options())?;
    let addr = endpoint.local_addr()?;
    let endpoint_for_drop = endpoint.clone();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        server
            .serve_quinn_with_shutdown(endpoint, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    Ok(RustServer {
        endpoint: endpoint_for_drop,
        addr,
        shutdown: Some(shutdown_tx),
        task: Some(task),
    })
}

fn make_identity() -> TestResult<rcgen::CertifiedKey<rcgen::KeyPair>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut params =
        rcgen::CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    params.not_before = not_before;
    params.not_after = not_before + time::Duration::hours(25);
    let cert = params.self_signed(&signing_key)?;

    Ok(rcgen::CertifiedKey { cert, signing_key })
}

fn write_certificate(identity: &rcgen::CertifiedKey<rcgen::KeyPair>, path: &Path) -> TestResult {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, identity.cert.pem())?;
    Ok(())
}

fn make_server_endpoint(
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    options: &ServerOptions,
) -> TestResult<quinn::Endpoint> {
    let cert_der = identity.cert.der().clone();
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(
        &mut server_config,
        options,
        trevrpc::quinn::TransportMode::Native,
    );

    Ok(quinn::Endpoint::server(
        server_config,
        SocketAddr::from(([127, 0, 0, 1], 0)),
    )?)
}

fn call_options() -> CallOptions {
    CallOptions::new()
        .with_timeout(Duration::from_secs(5))
        .with_metadata("authorization", format!("Bearer {AUTH_TOKEN}").into_bytes())
}

async fn exercise_rust_client(
    client: &greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
) -> TestResult {
    let response = client
        .say_hello(
            greeter::HelloRequest {
                name: "unary".to_owned(),
            },
            call_options(),
        )
        .await?;
    assert_eq!(response.message, "hello, unary");

    let replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "server".to_owned(),
            },
            call_options(),
        )
        .await?;
    assert_stream(replies, ["hello, server", "goodbye, server"]).await?;

    let mut greetings = client.lots_of_greetings(call_options()).await?;
    for name in ["left", "right"] {
        greetings
            .send(greeter::HelloRequest {
                name: name.to_owned(),
            })
            .await?;
    }
    let summary = greetings.close_and_recv().await?;
    assert_eq!(summary.message, "left,right");

    let mut replies = client.bidi_hello(call_options()).await?;
    for name in ["one", "two"] {
        replies
            .send(greeter::HelloRequest {
                name: name.to_owned(),
            })
            .await?;
        let reply = replies
            .recv()
            .await?
            .expect("bidi stream should yield reply");
        assert_eq!(reply.message, format!("echo, {name}"));
    }
    replies.close_send()?;
    assert!(replies.recv().await?.is_none());
    Ok(())
}

async fn stress_rust_client_lifecycle(
    client: &greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
) -> TestResult {
    let response = client
        .say_hello(
            greeter::HelloRequest {
                name: "lifecycle-unary".to_owned(),
            },
            call_options(),
        )
        .await?;
    assert_eq!(response.message, "hello, lifecycle-unary");

    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: "lifecycle-server-stream".to_owned(),
            },
            call_options(),
        )
        .await?;
    let reply = replies
        .next()
        .await
        .expect("server stream should yield first response")?;
    assert_eq!(reply.message, "hello, lifecycle-server-stream");
    drop(replies);

    let mut greetings = client.lots_of_greetings(call_options()).await?;
    greetings
        .send(greeter::HelloRequest {
            name: "cancelled-client-stream".to_owned(),
        })
        .await?;
    drop(greetings);

    let mut replies = client.bidi_hello(call_options()).await?;
    replies
        .send(greeter::HelloRequest {
            name: "cancelled-bidi".to_owned(),
        })
        .await?;
    let reply = replies
        .recv()
        .await?
        .expect("bidi stream should yield first response");
    assert_eq!(reply.message, "echo, cancelled-bidi");
    drop(replies);

    tokio::time::sleep(Duration::from_millis(5)).await;
    Ok(())
}

async fn assert_stream<const N: usize>(
    mut stream: trevrpc::BoxMessageStream<greeter::HelloReply>,
    expected: [&str; N],
) -> TestResult {
    for expected in expected {
        let reply = stream
            .next()
            .await
            .expect("stream should yield expected item")?;
        assert_eq!(reply.message, expected);
    }
    assert!(stream.next().await.is_none());

    Ok(())
}

async fn expect_rust_auth_failure(
    client: &greeter::GreeterClient<trevrpc::advanced::RawQuinnTransport>,
) {
    let error = client
        .say_hello(
            greeter::HelloRequest {
                name: "unauthenticated".to_owned(),
            },
            CallOptions::new().with_timeout(Duration::from_secs(5)),
        )
        .await
        .expect_err("missing auth should fail");
    assert_eq!(error.into_status().code(), Code::Unauthenticated);
}

async fn expect_rust_protocol_error(connection: &quinn::Connection) -> TestResult {
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&2_u32.to_be_bytes()).await?;
    send.write_all(&[0xff, 0xff]).await?;
    send.finish()?;

    let response = trevrpc::quinn::read_frame::<RpcResponse>(
        &mut recv,
        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
    )
    .await?;
    assert_eq!(Code::from_u32(response.status), Code::InvalidArgument);

    Ok(())
}
