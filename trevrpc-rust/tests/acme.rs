//! Certificates from ACME: `rustls-acme` gets a certificate from Pebble, an ACME test server, which
//! validates TLS-ALPN-01 on the TCP listener, and the server presents it over QUIC and `QMux` over
//! TLS.

use std::fs::{self, File};
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::greeter;
use crate::support::{self, TestResult};
use futures_util::StreamExt;
use quinn::rustls::pki_types::pem::PemObject as _;
use quinn::rustls::pki_types::{CertificateDer, ServerName};
use rustls_acme::{AcmeConfig, EventOk};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::oneshot;
use trevrpc::Status;
use trevrpc::client::{Channel, ChannelConfig, TransportKind, TransportPolicy};
use trevrpc::server::{RequestContext, Server, ServerOptions};
use trevrpc::tcp::TcpFallback;

/// The name Pebble issues the certificate for. Its mock DNS server resolves every name to
/// 127.0.0.1.
const DOMAIN: &str = "trevrpc.test";
const TEST_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds getting the certificate.
const ISSUE_TIMEOUT: Duration = Duration::from_secs(60);
/// Pebble's answer when rustls-acme asks it to validate a challenge it has already validated.
/// rustls-acme 0.15.4 does that when it finds the authorization pending a second after asking,
/// and Pebble finishes validating before the second request: on a loaded machine both can take
/// that long. The order fails, and rustls-acme orders again, which then succeeds.
const CHALLENGE_ALREADY_VALID: &str = "Cannot update challenge with status valid";
/// Bounds starting Pebble or its mock DNS server.
const START_TIMEOUT: Duration = Duration::from_secs(10);

struct Greeter;

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, Status> {
        Ok(greeter::HelloReply {
            message: format!("hello, {}", request.name),
        })
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        _request: greeter::HelloRequest,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("not under test"))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<greeter::HelloReply, Status> {
        Err(Status::unimplemented("not under test"))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("not under test"))
    }
}

#[tokio::test]
async fn rustls_acme_answers_tls_alpn_01_on_the_tcp_listener() -> TestResult {
    // Pebble validates on the TCP listener's port, so it is bound first.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let pebble = Pebble::start(listener.local_addr()?.port()).await?;
    let pebble_tls = Arc::new(support::client_tls(&pebble.listener_cert, &[b"http/1.1"])?);
    let mut state = AcmeConfig::new_with_client_config([DOMAIN], pebble_tls)
        .directory(&pebble.directory)
        .state();
    // The resolver presents the challenge certificate to acme-tls/1 handshakes, which the
    // configuration offers, and the certificate it got to the rest.
    let tls = Arc::unwrap_or_clone(state.challenge_rustls_config());

    let mut server = Server::new();
    server.set_options(
        ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(500))),
    );
    greeter::register_greeter(&mut server, Greeter);
    let endpoint = quinn::Endpoint::server(
        server.quic_config(tls.clone())?,
        SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
    )?;
    let udp = endpoint.local_addr()?;
    let tcp = listener.local_addr()?;
    let fallback = TcpFallback::new(listener, server.tcp_tls_config(tls));
    let (shutdown, stopped) = oneshot::channel::<()>();
    let serving = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
        let _ = stopped.await;
    }));

    // The state orders the certificate as soon as it is polled, and keeps it renewed.
    let (deployed, issued) = oneshot::channel();
    let ordering = tokio::spawn(async move {
        let mut deployed = Some(deployed);
        while let Some(event) = state.next().await {
            let event = match event {
                Ok(EventOk::DeployedNewCert) => Ok(()),
                Ok(_) => continue,
                Err(error) => {
                    let error = format!("{error:?}");
                    if error.contains(CHALLENGE_ALREADY_VALID) {
                        // The state orders again after a second.
                        eprintln!("ordering again after a challenge race: {error}");
                        continue;
                    }
                    Err(error)
                }
            };
            if let Some(deployed) = deployed.take() {
                let _ = deployed.send(event);
            }
        }
    });
    within!(ISSUE_TIMEOUT, issued)???;

    for policy in [TransportPolicy::QuicOnly, TransportPolicy::TcpOnly] {
        let roots = pebble.roots.clone();
        let config = ChannelConfig::new()
            .with_transport_policy(policy)
            .with_tcp_fallback(client_tls_with_roots(roots.clone(), trevrpc::QMUX_ALPN))
            .with_tcp_addr(tcp);
        let mut endpoint = quinn::Endpoint::client(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))?;
        endpoint.set_default_client_config(
            config.quic_config(client_tls_with_roots(roots, trevrpc::ALPN))?,
        );
        let channel = within!(
            TEST_TIMEOUT,
            Channel::connect_with_config(endpoint, udp, DOMAIN, config),
        )??;
        let expected = match policy {
            TransportPolicy::TcpOnly => TransportKind::Qmux,
            _ => TransportKind::Quic,
        };
        ensure_eq!(channel.transport(), Some(expected));
        let reply = within!(
            TEST_TIMEOUT,
            greeter::GreeterClient::new(channel.clone()).say_hello(&greeter::HelloRequest {
                name: "acme".to_owned(),
            }),
        )??;
        ensure_eq!(reply.message, "hello, acme");
        channel.close();
    }

    ordering.abort();
    let _ = shutdown.send(());
    within!(TEST_TIMEOUT, serving)???;
    Ok(())
}

/// A client configuration that trusts `roots` and offers `alpn`.
fn client_tls_with_roots(
    roots: quinn::rustls::RootCertStore,
    alpn: &[u8],
) -> quinn::rustls::ClientConfig {
    let mut tls = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    tls
}

/// A running Pebble and its mock DNS server, which are killed when it is dropped.
struct Pebble {
    /// The URL of the ACME directory.
    directory: String,
    /// The self-signed certificate of the ACME and management interfaces, for 127.0.0.1.
    listener_cert: CertificateDer<'static>,
    /// Trusts the certificates Pebble issues.
    roots: quinn::rustls::RootCertStore,
    _processes: Vec<KillOnDrop>,
    _dir: TempDir,
}

impl Pebble {
    /// Starts Pebble, which validates TLS-ALPN-01 challenges on `validation_port` of 127.0.0.1.
    /// It binds ports that another process may take first, so it retries with fresh ones.
    async fn start(validation_port: u16) -> TestResult<Self> {
        let dir = TempDir::new()?;
        let cert = rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_owned()])?;
        let cert_file = dir.path().join("cert.pem");
        let key_file = dir.path().join("key.pem");
        fs::write(&cert_file, cert.cert.pem())?;
        fs::write(&key_file, cert.signing_key.serialize_pem())?;
        let listener_cert = CertificateDer::from(cert.cert);

        let mut last_error = None;
        for attempt in 0..3 {
            let started = try_start(
                &dir,
                attempt,
                &cert_file,
                &key_file,
                validation_port,
                &listener_cert,
            )
            .await;
            match started {
                Ok((directory, roots, processes)) => {
                    return Ok(Self {
                        directory,
                        listener_cert,
                        roots,
                        _processes: processes,
                        _dir: dir,
                    });
                }
                Err(error) => {
                    eprintln!("starting Pebble: {error}");
                    last_error = Some(error);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| "Pebble did not start".into()))
    }
}

/// One attempt of [`Pebble::start`]: the directory URL, the roots of the certificates Pebble
/// issues, and the running processes.
async fn try_start(
    dir: &TempDir,
    attempt: usize,
    cert_file: &Path,
    key_file: &Path,
    validation_port: u16,
    listener_cert: &CertificateDer<'static>,
) -> TestResult<(String, quinn::rustls::RootCertStore, Vec<KillOnDrop>)> {
    let dns_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, free_udp_port()?));
    let acme_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, free_tcp_port()?));
    let management_addr = SocketAddr::from((Ipv4Addr::LOCALHOST, free_tcp_port()?));

    let mut dns = KillOnDrop::spawn(
        dir,
        &format!("pebble-challtestsrv-{attempt}"),
        Command::new("pebble-challtestsrv").args([
            "-dns01",
            &dns_addr.to_string(),
            "-defaultIPv4",
            "127.0.0.1",
            // Pebble validates the first address, which would be an IPv6 one.
            "-defaultIPv6",
            "",
            "-http01",
            "",
            "-https01",
            "",
            "-tlsalpn01",
            "",
            "-doh",
            "",
            "-management",
            "127.0.0.1:0",
        ]),
    )?;
    dns.wait_until(|| resolves_to_loopback(dns_addr)).await?;

    let config_file = dir.path().join(format!("pebble-config-{attempt}.json"));
    fs::write(
        &config_file,
        format!(
            r#"{{"pebble": {{
                "listenAddress": "{acme_addr}",
                "managementListenAddress": "{management_addr}",
                "certificate": "{}",
                "privateKey": "{}",
                "httpPort": 5002,
                "tlsPort": {validation_port},
                "ocspResponderURL": "",
                "externalAccountBindingRequired": false,
                "retryAfter": {{"authz": 1, "order": 1}}
            }}}}"#,
            cert_file.display(),
            key_file.display(),
        ),
    )?;
    let mut pebble = KillOnDrop::spawn(
        dir,
        &format!("pebble-{attempt}"),
        Command::new("pebble")
            .arg("-config")
            .arg(&config_file)
            .args(["-dnsserver", &dns_addr.to_string()])
            // Validate at once, and never reject a nonce to test client retries.
            .env("PEBBLE_VA_NOSLEEP", "1")
            .env("PEBBLE_WFE_NONCEREJECT", "0"),
    )?;
    let tls = Arc::new(support::client_tls(listener_cert, &[b"http/1.1"])?);
    pebble
        .wait_until(|| https_get(acme_addr, "/dir", tls.clone()))
        .await?;
    let root = https_get(management_addr, "/roots/0", tls).await?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(&root) {
        roots.add(cert?)?;
    }
    ensure!(!roots.is_empty(), "no certificate in Pebble's root");
    Ok((format!("https://{acme_addr}/dir"), roots, vec![dns, pebble]))
}

fn free_udp_port() -> TestResult<u16> {
    Ok(std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port())
}

fn free_tcp_port() -> TestResult<u16> {
    Ok(std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?
        .local_addr()?
        .port())
}

/// Asks the DNS server at `addr` for the A record of [`DOMAIN`], which must be 127.0.0.1 only.
async fn resolves_to_loopback(addr: SocketAddr) -> TestResult {
    let mut name = Vec::new();
    for label in DOMAIN.split('.') {
        name.push(u8::try_from(label.len())?);
        name.extend_from_slice(label.as_bytes());
    }
    name.push(0);
    // One question, recursion desired: the name, type A, class IN.
    let mut query = vec![0x12, 0x34, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0];
    query.extend_from_slice(&name);
    query.extend_from_slice(&[0, 1, 0, 1]);
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    socket.send_to(&query, addr).await?;
    let mut response = [0; 512];
    let len = within!(Duration::from_secs(1), socket.recv(&mut response))??;
    let response = response.get(..len).ok_or("oversized DNS response")?;
    ensure!(
        response.get(6..8) == Some(&[0, 1][..]),
        "DNS response {response:?} has not one answer"
    );
    // The answer follows the question, which the response repeats. Its name is either the same
    // name or a pointer to it.
    let answer = response
        .get(query.len()..)
        .ok_or("truncated DNS response")?;
    let name_len = if answer.first().is_some_and(|byte| byte & 0xc0 == 0xc0) {
        2
    } else {
        name.len()
    };
    // Type A, class IN, a TTL, and four bytes of address.
    let Some(&[0, 1, 0, 1, _, _, _, _, 0, 4, a, b, c, d, ..]) = answer.get(name_len..) else {
        return Err(format!("DNS answer {answer:?} is not an A record").into());
    };
    ensure_eq!([a, b, c, d], [127, 0, 0, 1]);
    Ok(())
}

/// The body of a successful HTTP/1.0 GET of `path` from `addr`, over TLS for 127.0.0.1.
async fn https_get(
    addr: SocketAddr,
    path: &str,
    tls: Arc<quinn::rustls::ClientConfig>,
) -> TestResult<Vec<u8>> {
    let stream = TcpStream::connect(addr).await?;
    let mut stream = tokio_rustls::TlsConnector::from(tls)
        .connect(ServerName::try_from("127.0.0.1")?, stream)
        .await?;
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nHost: {addr}\r\n\r\n").as_bytes())
        .await?;
    let mut response = Vec::new();
    // Pebble may close without close_notify; the response is complete by then.
    let _ = stream.read_to_end(&mut response).await;
    let head_end = response
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or("incomplete HTTP response")?;
    ensure!(
        response.starts_with(b"HTTP/1.0 200") || response.starts_with(b"HTTP/1.1 200"),
        "GET {path}: {}",
        String::from_utf8_lossy(&response)
    );
    Ok(response.split_off(head_end.saturating_add(4)))
}

/// A child process that is killed when this is dropped, whose output goes to a file.
struct KillOnDrop {
    name: String,
    child: Child,
    log: PathBuf,
}

impl KillOnDrop {
    fn spawn(dir: &TempDir, name: &str, command: &mut Command) -> TestResult<Self> {
        let log = dir.path().join(format!("{name}.log"));
        let output = File::create(&log)?;
        let child = command
            .stdin(Stdio::null())
            .stdout(output.try_clone()?)
            .stderr(output)
            .spawn()
            .map_err(|error| format!("start {name} (it is in the dev shell): {error}"))?;
        Ok(Self {
            name: name.to_owned(),
            child,
            log,
        })
    }

    /// Polls `ready` until it succeeds and returns its value, failing once the process exits or
    /// after [`START_TIMEOUT`].
    async fn wait_until<T, F, Fut>(&mut self, mut ready: F) -> TestResult<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = TestResult<T>>,
    {
        let started = Instant::now();
        loop {
            let error = match ready().await {
                Ok(value) => return Ok(value),
                Err(error) => error,
            };
            let log = fs::read_to_string(&self.log).unwrap_or_default();
            if let Some(status) = self.child.try_wait()? {
                return Err(format!("{} exited with {status}:\n{log}", self.name).into());
            }
            if started.elapsed() > START_TIMEOUT {
                return Err(format!("{} not ready: {error}\n{log}", self.name).into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A temporary directory removed when dropped.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> std::io::Result<Self> {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "trevrpc-acme-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path)?;
        Ok(Self(path))
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
