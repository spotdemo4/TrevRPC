use std::error::Error;
use std::net::{SocketAddr, UdpSocket};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::task::JoinHandle;
use trevrpc::client::{
    Channel, ChannelConfig, ChannelEvent, ChannelPhase, ReconnectBackoff, RpcTransport,
};
use trevrpc::{Code, RpcRequest, Status};

#[macro_use]
mod support;

const TEST_TIMEOUT: Duration = Duration::from_secs(3);

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
        Self::spawn_with_delay(fail_first_request, Duration::ZERO)
    }

    /// Spawns a server that answers each request after `delay`.
    fn spawn_with_delay(fail_first_request: bool, delay: Duration) -> TestResult<Self> {
        let (endpoint, cert_der) = server_endpoint()?;
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
            delay,
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

/// A QUIC server endpoint with a new self-signed certificate.
fn server_endpoint() -> TestResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    let server_config = trevrpc::server::Server::new().quic_config(crypto)?;
    let endpoint = quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?;
    Ok((endpoint, cert_der))
}

async fn run_raw_server(
    endpoint: quinn::Endpoint,
    accepted_connections: Arc<AtomicUsize>,
    received_requests: Arc<AtomicUsize>,
    fail_first_request: Arc<AtomicBool>,
    delay: Duration,
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
                let Ok(_request) = support::read_request(&mut recv).await else {
                    break;
                };
                let Ok(Some(body)) = support::read_message(&mut recv).await else {
                    break;
                };
                received_requests.fetch_add(1, Ordering::SeqCst);
                if fail_first_request.swap(false, Ordering::SeqCst) {
                    connection.close(1_u32.into(), b"drop first generation");
                    break;
                }

                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    if support::write_message(&mut send, &body).await.is_ok()
                        && support::write_status(&mut send, Status::ok()).await.is_ok()
                    {
                        let _ = send.finish();
                    }
                });
            }
        });
    }
}

fn make_client_endpoint(cert: CertificateDer<'static>) -> TestResult<quinn::Endpoint> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert)?;
    let crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let config = ChannelConfig::new().quic_config(crypto)?;
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
                return Ok::<_, tokio::sync::watch::error::RecvError>(());
            }
            states.changed().await?;
        }
    })
    .await??;
    Ok(())
}

fn request() -> RpcRequest {
    RpcRequest::new("/test.Service/Call")
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
    ensure_eq!(client.state().generation(), 1);

    let first_error = client
        .call(request(), b"first".to_vec())
        .await
        .expect_err("the first generation should be closed in flight");
    assert_unavailable(first_error);
    wait_for_phase(&client, ChannelPhase::Reconnecting).await?;

    let reconnecting_error = tokio::time::timeout(
        Duration::from_millis(20),
        client.call(request(), b"must fail fast".to_vec()),
    )
    .await?
    .expect_err("calls during reconnection must fail");
    assert_unavailable(reconnecting_error);
    ensure_eq!(server.received_requests.load(Ordering::SeqCst), 1);

    ensure_eq!(
        tokio::time::timeout(TEST_TIMEOUT, client.wait_until_ready()).await??,
        2
    );
    ensure_eq!(
        events.recv().await?,
        ChannelEvent::ConnectionLost { generation: 1 }
    );
    ensure_eq!(
        events.recv().await?,
        ChannelEvent::ReconnectAttempt {
            generation: 1,
            attempt: 1,
        }
    );
    ensure_eq!(
        events.recv().await?,
        ChannelEvent::Ready {
            generation: 2,
            transport: trevrpc::client::TransportKind::Quic
        }
    );
    tokio::time::sleep(Duration::from_millis(25)).await;
    ensure_eq!(server.received_requests.load(Ordering::SeqCst), 1);

    let response = client.call(request(), b"future".to_vec()).await?;
    ensure_eq!(response.message(), &bytes::Bytes::from_static(b"future"));
    ensure_eq!(server.received_requests.load(Ordering::SeqCst), 2);
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 2);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn close_stops_reconnect_and_rebind_preserves_generation() -> TestResult {
    let server = RawServer::spawn(false)?;
    let client = connect_channel(&server, Duration::from_millis(40)).await?;
    let mut events = client.subscribe_events();
    let generation = client.state().generation();
    ensure!(client.is_ready());

    let socket = UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], 0)))?;
    client.rebind(socket)?;
    ensure_eq!(client.state().generation(), generation);

    client.close();
    ensure_eq!(events.recv().await?, ChannelEvent::Closed { generation });
    ensure_eq!(client.state().phase(), ChannelPhase::Closed);
    ensure_eq!(client.state().generation(), generation);
    assert_unavailable(
        client
            .call(request(), b"closed".to_vec())
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
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 1);
    server.shutdown().await
}

async fn connect_pooled(server: &RawServer, config: ChannelConfig) -> TestResult<Channel> {
    let endpoint = make_client_endpoint(server.cert.clone())?;
    Ok(Channel::connect_with_config(endpoint, server.addr, "localhost", config).await?)
}

/// Makes `count` calls at once and checks that every one succeeds.
async fn concurrent_calls(client: &Channel, count: usize) -> TestResult {
    let calls: Vec<_> = (0..count)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.call(request(), b"pool".to_vec()).await })
        })
        .collect();
    for call in calls {
        let response = tokio::time::timeout(TEST_TIMEOUT, call).await???;
        ensure_eq!(response.message().as_ref(), b"pool");
    }
    Ok(())
}

/// Spreads calls at any call rate, so that tests reach it with a few slow calls.
const fn fast(config: ChannelConfig) -> ChannelConfig {
    config.with_quic_connection_call_rate(1)
}

/// Keeps making bursts of calls until the channel spreads them over `count` connections. Each
/// burst that finds every connection busy dials one more.
async fn grow_to(client: &Channel, count: usize) -> TestResult {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while client.quic_connections().len() < count {
            concurrent_calls(client, 8).await?;
        }
        Ok::<_, Box<dyn Error + Send + Sync>>(())
    })
    .await??;
    Ok(())
}

async fn wait_for_connections(client: &Channel, count: usize) -> TestResult {
    tokio::time::timeout(TEST_TIMEOUT, async {
        while client.quic_connections().len() != count {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn busy_calls_spread_over_added_connections_up_to_the_limit() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(100))?;
    let client = connect_pooled(
        &server,
        fast(ChannelConfig::new().with_max_quic_connections(3)),
    )
    .await?;

    grow_to(&client, 3).await?;
    concurrent_calls(&client, 12).await?;
    ensure_eq!(client.quic_connections().len(), 3);
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 3);
    ensure_eq!(client.state().generation(), 1);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn moving_bytes_fast_spreads_calls_at_any_call_rate() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(100))?;
    let config = ChannelConfig::new()
        .with_max_quic_connections(2)
        .with_quic_connection_call_rate(u64::MAX)
        .with_quic_connection_byte_rate(1);
    let client = connect_pooled(&server, config).await?;

    grow_to(&client, 2).await?;
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 2);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn slow_calls_keep_one_connection_however_many_are_in_flight() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(100))?;
    let client = connect_pooled(&server, ChannelConfig::new().with_max_quic_connections(3)).await?;

    // Eight calls in flight at a time, but fewer than 100 starting per second.
    for _ in 0..5 {
        concurrent_calls(&client, 8).await?;
    }
    ensure_eq!(client.quic_connections().len(), 1);
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 1);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn one_connection_when_the_limit_is_one() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(50))?;
    let client = connect_pooled(
        &server,
        fast(ChannelConfig::new()).with_max_quic_connections(1),
    )
    .await?;

    concurrent_calls(&client, 8).await?;
    ensure_eq!(client.quic_connections().len(), 1);
    ensure_eq!(server.accepted_connections.load(Ordering::SeqCst), 1);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn added_connections_close_once_idle() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(100))?;
    let config = fast(ChannelConfig::new())
        .with_max_quic_connections(3)
        .with_quic_connection_idle_timeout(Duration::from_millis(200));
    let client = connect_pooled(&server, config).await?;

    grow_to(&client, 3).await?;
    wait_for_connections(&client, 1).await?;
    // The generation's own connection stays, and calls keep working.
    concurrent_calls(&client, 1).await?;
    ensure_eq!(client.state().generation(), 1);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn the_minimum_connections_are_open_from_the_start_and_share_every_call() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(100))?;
    // Slow calls at a low rate, which alone would keep one connection, and an idle timeout the
    // minimum connections outlast.
    let config = ChannelConfig::new()
        .with_min_quic_connections(3)
        .with_max_quic_connections(2)
        .with_quic_connection_idle_timeout(Duration::from_millis(100));
    let client = connect_pooled(&server, config).await?;
    ensure_eq!(client.quic_connections().len(), 3);
    // The server finishes its handshakes one at a time, after the client.
    tokio::time::timeout(TEST_TIMEOUT, async {
        while server.accepted_connections.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    // Six calls opened at once go two to a connection.
    let calls: Vec<_> = (0..6)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.call(request(), b"pool".to_vec()).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(30)).await;
    let busy: Vec<u64> = client
        .quic_connections()
        .iter()
        .map(|connection| connection.stats().frame_tx.stream)
        .collect();
    for call in calls {
        tokio::time::timeout(TEST_TIMEOUT, call).await???;
    }
    ensure!(
        busy.iter().all(|&frames| frames > 0),
        "calls did not reach every connection: {busy:?}"
    );

    tokio::time::sleep(Duration::from_millis(400)).await;
    ensure_eq!(client.quic_connections().len(), 3);

    // A lost one is dialed again by the next call.
    client.quic_connections()[2].close(0_u32.into(), b"test");
    wait_for_connections(&client, 2).await?;
    concurrent_calls(&client, 1).await?;
    wait_for_connections(&client, 3).await?;
    ensure_eq!(client.state().generation(), 1);

    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn losing_the_first_connection_drains_the_added_ones() -> TestResult {
    let server = RawServer::spawn_with_delay(false, Duration::from_millis(300))?;
    let config = fast(ChannelConfig::new())
        .with_max_quic_connections(2)
        .with_reconnect_backoff(FixedBackoff(Duration::from_millis(50)));
    let client = connect_pooled(&server, config).await?;
    grow_to(&client, 2).await?;
    let connections = client.quic_connections();
    let (primary, added) = (connections[0].clone(), connections[1].clone());

    // Busy both connections, then lose the first: calls on the added one still finish.
    let calls: Vec<_> = (0..4)
        .map(|_| {
            let client = client.clone();
            tokio::spawn(async move { client.call(request(), b"pool".to_vec()).await })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(100)).await;
    primary.close(0_u32.into(), b"test");
    let mut succeeded = 0;
    for call in calls {
        if tokio::time::timeout(TEST_TIMEOUT, call).await??.is_ok() {
            succeeded += 1;
        }
    }
    ensure!(succeeded >= 1, "no call on the added connection finished");
    ensure!(
        succeeded < 4,
        "every call finished despite the lost connection"
    );
    tokio::time::timeout(TEST_TIMEOUT, added.closed()).await?;

    wait_for_phase(&client, ChannelPhase::Ready).await?;
    concurrent_calls(&client, 1).await?;
    ensure_eq!(client.state().generation(), 2);

    client.close();
    server.shutdown().await
}

/// One REQUEST's connection metadata as a server saw it: the connection it came on, numbered in
/// order of their first request, the set's ID, and whether it offered the set.
type SeenConnectionMetadata = (usize, u32, bool);

/// A server that records the connection metadata of every request and confirms every offer,
/// answering each request after `delay`.
struct ConnectionMetadataServer {
    endpoint: quinn::Endpoint,
    addr: SocketAddr,
    cert: CertificateDer<'static>,
    seen: Arc<Mutex<Vec<SeenConnectionMetadata>>>,
    connections: Arc<Mutex<Vec<quinn::Connection>>>,
    task: JoinHandle<()>,
}

impl ConnectionMetadataServer {
    fn spawn(delay: Duration) -> TestResult<Self> {
        let (endpoint, cert) = server_endpoint()?;
        let addr = endpoint.local_addr()?;
        let seen = Arc::<Mutex<Vec<SeenConnectionMetadata>>>::default();
        let connections = Arc::<Mutex<Vec<quinn::Connection>>>::default();
        let task = tokio::spawn(run_connection_metadata_server(
            endpoint.clone(),
            Arc::clone(&seen),
            Arc::clone(&connections),
            delay,
        ));
        Ok(Self {
            endpoint,
            addr,
            cert,
            seen,
            connections,
            task,
        })
    }

    fn seen(&self) -> Vec<SeenConnectionMetadata> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    async fn shutdown(self) -> TestResult {
        self.endpoint.close(0_u32.into(), b"test complete");
        tokio::time::timeout(TEST_TIMEOUT, self.task).await??;
        Ok(())
    }
}

async fn run_connection_metadata_server(
    endpoint: quinn::Endpoint,
    seen: Arc<Mutex<Vec<SeenConnectionMetadata>>>,
    connections: Arc<Mutex<Vec<quinn::Connection>>>,
    delay: Duration,
) {
    while let Some(incoming) = endpoint.accept().await {
        let Ok(connection) = incoming.await else {
            continue;
        };
        let seen = Arc::clone(&seen);
        let connections = Arc::clone(&connections);
        tokio::spawn(async move {
            while let Ok((mut send, mut recv)) = connection.accept_bi().await {
                let Ok(request) = support::read_request(&mut recv).await else {
                    break;
                };
                let Ok(Some(body)) = support::read_message(&mut recv).await else {
                    break;
                };
                let index = connection_index(&connections, &connection);
                let confirmed = request.connection_metadata.map_or(0, |set| {
                    let offered = !set.metadata.is_empty();
                    seen.lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .push((index, set.id, offered));
                    if offered { set.id } else { 0 }
                });
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    let answer = trevrpc::RpcAnswer {
                        connection_metadata: confirmed,
                        ..trevrpc::RpcAnswer::default()
                    };
                    let Ok(answer) = trevrpc::framing::encode_protobuf_frame(
                        trevrpc::framing::frame_type::ANSWER,
                        &answer,
                        trevrpc::framing::DEFAULT_MAX_FRAME_SIZE,
                    ) else {
                        return;
                    };
                    if support::write_raw(&mut send, &answer).await.is_ok()
                        && support::write_message(&mut send, &body).await.is_ok()
                        && support::write_status(&mut send, Status::ok()).await.is_ok()
                    {
                        let _ = send.finish();
                    }
                });
            }
        });
    }
}

/// The number of `connection` among `connections`, which it joins if it is new.
fn connection_index(
    connections: &Mutex<Vec<quinn::Connection>>,
    connection: &quinn::Connection,
) -> usize {
    let mut connections = connections.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(index) = connections
        .iter()
        .position(|known| known.stable_id() == connection.stable_id())
    {
        return index;
    }
    connections.push(connection.clone());
    connections.len().saturating_sub(1)
}

fn suffix(value: &str) -> trevrpc::Metadata {
    trevrpc::Metadata::from([("suffix".to_owned(), value.as_bytes().to_vec())])
}

async fn connect_with_connection_metadata(
    server: &ConnectionMetadataServer,
    config: ChannelConfig,
) -> TestResult<Channel> {
    let endpoint = make_client_endpoint(server.cert.clone())?;
    Ok(tokio::time::timeout(
        TEST_TIMEOUT,
        Channel::connect_with_config(endpoint, server.addr, "localhost", config),
    )
    .await??)
}

async fn sequential_calls(client: &Channel, count: usize) -> TestResult {
    for _ in 0..count {
        tokio::time::timeout(TEST_TIMEOUT, client.call(request(), b"set".to_vec())).await??;
    }
    Ok(())
}

#[tokio::test]
async fn connection_metadata_is_offered_until_each_connection_confirms_it() -> TestResult {
    let server = ConnectionMetadataServer::spawn(Duration::ZERO)?;
    let client = connect_with_connection_metadata(
        &server,
        ChannelConfig::new()
            .with_reconnect_backoff(FixedBackoff(Duration::from_millis(20)))
            .with_connection_metadata(suffix("!")),
    )
    .await?;
    sequential_calls(&client, 3).await?;
    client.set_connection_metadata(suffix("#"))?;
    sequential_calls(&client, 2).await?;

    // The next generation's connection starts unconfirmed.
    server
        .connections
        .lock()
        .unwrap_or_else(PoisonError::into_inner)[0]
        .close(0_u32.into(), b"test");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while !(client.is_ready() && client.state().generation() == 2) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    sequential_calls(&client, 2).await?;

    // Removing it stops sending it.
    client.set_connection_metadata(trevrpc::Metadata::new())?;
    sequential_calls(&client, 1).await?;

    ensure_eq!(
        server.seen(),
        [
            (0, 1, true),
            (0, 1, false),
            (0, 1, false),
            (0, 2, true),
            (0, 2, false),
            (1, 2, true),
            (1, 2, false),
        ]
    );
    client.close();
    server.shutdown().await
}

#[tokio::test]
async fn every_pooled_connection_offers_connection_metadata_before_referring_to_it() -> TestResult {
    let server = ConnectionMetadataServer::spawn(Duration::from_millis(50))?;
    let client = connect_with_connection_metadata(
        &server,
        ChannelConfig::new()
            .with_min_quic_connections(2)
            .with_connection_metadata(suffix("!")),
    )
    .await?;
    for _ in 0..2 {
        concurrent_calls(&client, 6).await?;
    }

    let seen = server.seen();
    let connections: std::collections::BTreeSet<usize> =
        seen.iter().map(|&(connection, _, _)| connection).collect();
    ensure_eq!(connections.len(), 2, "{seen:?}");
    for connection in connections {
        let first = seen
            .iter()
            .find(|&&(seen_on, _, _)| seen_on == connection)
            .copied();
        ensure_eq!(first, Some((connection, 1, true)), "{seen:?}");
    }
    ensure!(
        seen.iter().any(|&(_, _, offered)| !offered),
        "no call referred to the confirmed set: {seen:?}"
    );
    client.close();
    server.shutdown().await
}
