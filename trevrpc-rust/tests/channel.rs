use std::error::Error;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::task::JoinHandle;
use trevrpc::advanced::ChannelOperations;
use trevrpc::advanced::{ChannelConfigOperations, ReconnectBackoff};
use trevrpc::client::{Channel, ChannelConfig, ChannelEvent, ChannelPhase, RpcTransport};
use trevrpc::quinn::TransportMode;
use trevrpc::{Code, RpcRequest, RpcResponse, Status};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_FRAME_SIZE: usize = trevrpc::framing::DEFAULT_MAX_FRAME_SIZE;

type TestResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

struct FixedBackoff(Duration);

impl ReconnectBackoff for FixedBackoff {
    fn delay(&self, _attempt: u32) -> Duration {
        self.0
    }
}

struct RawServer {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    accepted_connections: Arc<AtomicUsize>,
    received_requests: Arc<AtomicUsize>,
    task: JoinHandle<()>,
}

impl RawServer {
    fn spawn(fail_first_request: bool) -> TestResult<Self> {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let cert_der = CertificateDer::from(cert.cert);
        let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
        let mut crypto = quinn::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
        crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
        let mut server_config =
            quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(crypto)?));
        trevrpc::quinn::configure_server_config(
            &mut server_config,
            &trevrpc::server::ServerOptions::new(),
            TransportMode::Native,
        );
        let endpoint =
            quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?;
        let addr = endpoint.local_addr()?;
        let accepted_connections = Arc::new(AtomicUsize::new(0));
        let received_requests = Arc::new(AtomicUsize::new(0));
        let fail_first_request = Arc::new(AtomicBool::new(fail_first_request));
        let server_endpoint = endpoint.clone();
        let task = tokio::spawn(run_raw_server(
            server_endpoint,
            Arc::clone(&accepted_connections),
            Arc::clone(&received_requests),
            fail_first_request,
        ));

        Ok(Self {
            endpoint,
            addr,
            cert: cert_der,
            accepted_connections,
            received_requests,
            task,
        })
    }

    async fn shutdown(self) -> TestResult {
        self.endpoint.close(0_u32.into(), b"test complete");
        tokio::time::timeout(TEST_TIMEOUT, self.task).await??;
        Ok(())
    }
}

async fn run_raw_server(
    endpoint: quinn::Endpoint,
    accepted_connections: Arc<AtomicUsize>,
    received_requests: Arc<AtomicUsize>,
    fail_first_request: Arc<AtomicBool>,
) {
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connection) = incoming.await else {
            continue;
        };
        accepted_connections.fetch_add(1, Ordering::SeqCst);
        let received_requests = Arc::clone(&received_requests);
        let fail_first_request = Arc::clone(&fail_first_request);
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                let Ok(request) =
                    trevrpc::quinn::read_frame::<RpcRequest>(&mut recv, MAX_FRAME_SIZE).await
                else {
                    break;
                };
                received_requests.fetch_add(1, Ordering::SeqCst);
                if fail_first_request.swap(false, Ordering::SeqCst) {
                    connection.close(1_u32.into(), b"drop first generation");
                    break;
                }

                let response = RpcResponse::ok(request.body);
                if trevrpc::quinn::write_frame(&mut send, &response, MAX_FRAME_SIZE)
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = send.finish();
            }
        });
    }
}

fn make_client_endpoint(cert: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert)?;
    let mut crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];
    let mut config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    trevrpc::quinn::configure_client_config(&mut config, MAX_FRAME_SIZE, TransportMode::Native);
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    endpoint.set_default_client_config(config);
    Ok(endpoint)
}

async fn connect_channel(server: &RawServer, delay: Duration) -> TestResult<Channel> {
    let endpoint = make_client_endpoint(server.cert.clone())?;
    Ok(Channel::connect_with_config(
        endpoint,
        server.addr,
        "localhost",
        ChannelConfig::new().with_reconnect_backoff(FixedBackoff(delay)),
    )
    .await?)
}

async fn wait_for_phase(client: &Channel, phase: ChannelPhase) -> TestResult {
    let mut states = client.subscribe_state();
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if states.borrow_and_update().phase() == phase {
                return;
            }
            states
                .changed()
                .await
                .expect("channel state sender should remain open");
        }
    })
    .await?;
    Ok(())
}

fn request(body: &[u8]) -> RpcRequest {
    RpcRequest::new("test.Service", "Call", body.to_vec())
}

fn assert_unavailable(error: trevrpc::Error) {
    let status = Status::from(error);
    assert_eq!(status.code(), Code::Unavailable);
}

#[tokio::test]
async fn lost_generation_is_not_replayed_and_future_calls_recover() -> TestResult {
    let server = RawServer::spawn(true)?;
    let client = connect_channel(&server, Duration::from_millis(150)).await?;
    let mut events = client.subscribe_events();
    assert_eq!(client.state().generation(), 1);

    let first_error = client
        .call(request(b"first"))
        .await
        .expect_err("the first generation should be closed in flight");
    assert_unavailable(first_error);
    wait_for_phase(&client, ChannelPhase::Reconnecting).await?;

    let reconnecting_error = tokio::time::timeout(
        Duration::from_millis(20),
        client.call(request(b"must fail fast")),
    )
    .await?
    .expect_err("calls during reconnection must fail");
    assert_unavailable(reconnecting_error);
    assert_eq!(server.received_requests.load(Ordering::SeqCst), 1);

    assert_eq!(
        tokio::time::timeout(TEST_TIMEOUT, client.wait_until_ready()).await??,
        2
    );
    assert_eq!(
        events.recv().await?,
        ChannelEvent::ConnectionLost { generation: 1 }
    );
    assert_eq!(
        events.recv().await?,
        ChannelEvent::ReconnectAttempt {
            generation: 1,
            attempt: 1,
        }
    );
    assert_eq!(events.recv().await?, ChannelEvent::Ready { generation: 2 });
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert_eq!(server.received_requests.load(Ordering::SeqCst), 1);

    let response = client.call(request(b"future")).await?;
    assert_eq!(response.body, b"future");
    assert_eq!(server.received_requests.load(Ordering::SeqCst), 2);
    assert_eq!(server.accepted_connections.load(Ordering::SeqCst), 2);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn close_stops_reconnect_and_rebind_preserves_generation() -> TestResult {
    let server = RawServer::spawn(false)?;
    let client = connect_channel(&server, Duration::from_millis(40)).await?;
    let mut events = client.subscribe_events();
    let generation = client.state().generation();
    assert!(client.is_ready());

    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    client.rebind_quinn(socket)?;
    assert_eq!(client.state().generation(), generation);

    client.close();
    assert_eq!(events.recv().await?, ChannelEvent::Closed { generation });
    assert_eq!(client.state().phase(), ChannelPhase::Closed);
    assert_eq!(client.state().generation(), generation);
    assert_unavailable(
        client
            .call(request(b"closed"))
            .await
            .expect_err("closed client calls must fail"),
    );
    assert_unavailable(
        client
            .wait_until_ready()
            .await
            .expect_err("closed client cannot become ready"),
    );

    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(server.accepted_connections.load(Ordering::SeqCst), 1);
    server.shutdown().await
}
