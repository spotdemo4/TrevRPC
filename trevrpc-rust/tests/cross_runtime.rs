#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::error::Error;
use std::io::{BufRead, BufReader};
use std::net::{Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
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

struct GoServer {
    child: Child,
    addr: SocketAddr,
}

impl Drop for GoServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
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
    let cert_path = temp_cert_path("go-server");
    let go_server = spawn_go_server(&go_binary, &cert_path)?;
    let (endpoint, connection, client) = connect_rust_client(go_server.addr, &cert_path).await?;

    exercise_rust_client(&client).await?;
    expect_rust_auth_failure(&client).await;
    expect_rust_protocol_error(&connection).await?;

    connection.close(0_u32.into(), b"test complete");
    endpoint.wait_idle().await;
    drop(go_server);
    let _ = std::fs::remove_file(cert_path);
    Ok(())
}

#[tokio::test]
async fn go_client_talks_to_rust_server_over_quic() -> TestResult {
    let Some(go_binary) = go_binary() else {
        return Ok(());
    };
    let cert_path = temp_cert_path("rust-server");
    let rust_server = spawn_rust_server(&cert_path)?;
    let addr = rust_server.addr;
    let output = tokio::task::spawn_blocking(move || {
        Command::new(go_binary)
            .arg("-mode")
            .arg("client")
            .arg("-addr")
            .arg(addr.to_string())
            .arg("-cert")
            .arg(cert_path)
            .arg("-token")
            .arg(AUTH_TOKEN)
            .output()
    })
    .await??;

    if !output.status.success() {
        return Err(format!(
            "Go client failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }

    rust_server.shutdown().await
}

fn go_binary() -> Option<PathBuf> {
    std::env::var_os("TREVRPC_XRUNTIME_GO").map(PathBuf::from)
}

fn temp_cert_path(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("trevrpc-cross-runtime-{name}-{unique}.pem"))
}

fn spawn_go_server(go_binary: &Path, cert_path: &Path) -> TestResult<GoServer> {
    let mut child = Command::new(go_binary)
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

    let stdout = child
        .stdout
        .take()
        .ok_or("Go server stdout should be piped")?;
    let mut ready = String::new();
    BufReader::new(stdout).read_line(&mut ready)?;
    let Some(addr) = ready.strip_prefix("READY ") else {
        return Err(format!("unexpected Go server ready line: {ready:?}").into());
    };
    let addr = addr.trim().parse::<SocketAddr>()?;

    Ok(GoServer { child, addr })
}

async fn connect_rust_client(
    addr: SocketAddr,
    cert_path: &Path,
) -> TestResult<(
    quinn::Endpoint,
    quinn::Connection,
    greeter::GreeterClient<trevrpc::quinn::Client>,
)> {
    let endpoint = make_client_endpoint(cert_path)?;
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let client = greeter::GreeterClient::new(trevrpc::quinn::Client::new(connection.clone()));

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
        false,
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
    trevrpc::quinn::configure_server_config(&mut server_config, options, false);

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
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
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

async fn expect_rust_auth_failure(client: &greeter::GreeterClient<trevrpc::quinn::Client>) {
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
