//! Setup shared by the loopback benchmarks: a server on loopback ports, a connected client over
//! native QUIC or `QMux` over TLS, and the four call shapes against either of two services.
//!
//! The `Raw` service registers closures with [`Server::route`] and [`Server::route_streaming`]
//! that never decode a message, which isolates the runtime's own overhead. The `Greeter` service
//! is the generated one from `examples/shared/greeter.rs`, which adds protobuf decoding and
//! encoding and the generated dispatch.
//!
//! No `tracing` subscriber is ever installed. The server logs every RPC at `info` level, and
//! without a subscriber those events cost one disabled-callsite check.

use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

use futures_util::StreamExt as _;
use prost::Message as _;
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::net::TcpListener;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use trevrpc::client::{
    BidirectionalCall, CallOptions, ChannelConfig, QmuxTransport, QuicTransport,
    StreamingRpcTransport,
};
use trevrpc::server::{RequestContext, Server, ServerOptions};
use trevrpc::tcp::TcpFallback;
use trevrpc::{BoxStream, RpcKind, Status};

#[allow(dead_code)]
#[path = "../../examples/shared/greeter.rs"]
pub mod greeter;

/// Messages per streaming call, as in the load benchmarks' default.
pub const MESSAGES: usize = 100;

const RAW_UNARY: &str = "/bench.Raw/Unary";
const RAW_SERVER_STREAM: &str = "/bench.Raw/ServerStream";
const RAW_CLIENT_STREAM: &str = "/bench.Raw/ClientStream";
const RAW_BIDI: &str = "/bench.Raw/Bidi";

/// Which transport a benchmark runs over.
#[derive(Clone, Copy, Debug)]
pub enum Transport {
    Quic,
    Qmux,
}

/// Which service answers the calls.
#[derive(Clone, Copy, Debug)]
pub enum Variant {
    Raw,
    Greeter,
}

/// A message whose single field is opaque bytes: the payload of the `Raw` service.
#[derive(Clone, PartialEq, Eq, prost::Message)]
pub struct Blob {
    #[prost(bytes = "vec", tag = "1")]
    pub data: Vec<u8>,
}

// ---------------------------------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------------------------------

fn register_raw(server: &mut Server) {
    // Unary and bidi echo the bytes they receive, undecoded.
    server.route(RAW_UNARY, |body| async move { Ok(body) });
    server.route_streaming(
        RAW_SERVER_STREAM,
        RpcKind::ServerStreaming,
        |body, _requests| async move {
            Ok(trevrpc::stream::from_iter(
                (0..MESSAGES).map(move |_| body.clone()),
            ))
        },
    );
    server.route_streaming(
        RAW_CLIENT_STREAM,
        RpcKind::ClientStreaming,
        |_body, mut requests| async move {
            let mut count = 0_usize;
            while let Some(request) = requests.next().await {
                request?;
                count += 1;
            }
            let reply = Blob {
                data: count.to_le_bytes().to_vec(),
            };
            Ok(trevrpc::stream::from_iter([reply.encode_to_vec()]))
        },
    );
    server.route_streaming(
        RAW_BIDI,
        RpcKind::BidirectionalStreaming,
        |_body, requests| async move { Ok(requests) },
    );
}

struct BenchGreeter;

const fn reply(message: String) -> greeter::HelloReply {
    greeter::HelloReply { message }
}

#[trevrpc::async_trait]
impl greeter::Greeter for BenchGreeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, Status> {
        Ok(reply(request.name))
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<BoxStream<greeter::HelloReply>, Status> {
        Ok(trevrpc::stream::from_iter(
            (0..MESSAGES).map(move |_| reply(request.name.clone())),
        ))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        mut requests: BoxStream<greeter::HelloRequest>,
    ) -> Result<greeter::HelloReply, Status> {
        let mut count = 0_usize;
        while let Some(request) = requests.next().await {
            request?;
            count += 1;
        }
        Ok(reply(count.to_string()))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        requests: BoxStream<greeter::HelloRequest>,
    ) -> Result<BoxStream<greeter::HelloReply>, Status> {
        Ok(Box::pin(
            requests.map(|request| request.map(|request| reply(request.name))),
        ))
    }
}

// ---------------------------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------------------------

/// A connected client, whichever transport it speaks.
pub enum Link {
    Quic {
        endpoint: quinn::Endpoint,
        transport: QuicTransport,
    },
    Qmux(QmuxTransport),
}

/// A tokio runtime, a server running on it, and a client connected to that server.
///
/// Everything is built outside the timed region. Dropping the fixture closes the connection and
/// shuts the server down.
pub struct Fixture {
    pub runtime: Runtime,
    pub link: Link,
    shutdown: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<trevrpc::Result<()>>>,
}

impl Fixture {
    pub fn start(transport: Transport, variant: Variant) -> Self {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let (link, shutdown, server) = runtime.block_on(Self::connect(transport, variant));
        Self {
            runtime,
            link,
            shutdown: Some(shutdown),
            server: Some(server),
        }
    }

    async fn connect(
        transport: Transport,
        variant: Variant,
    ) -> (Link, oneshot::Sender<()>, JoinHandle<trevrpc::Result<()>>) {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let cert_der = CertificateDer::from(cert.cert);
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()));
        let server_tls = quinn::rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key)
            .unwrap();

        let mut server = Server::new();
        server.set_options(
            ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(200))),
        );
        match variant {
            Variant::Raw => register_raw(&mut server),
            Variant::Greeter => greeter::register_greeter(&mut server, BenchGreeter),
        }
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let endpoint =
            quinn::Endpoint::server(server.quic_config(server_tls.clone()).unwrap(), loopback)
                .unwrap();
        let listener = TcpListener::bind(loopback).await.unwrap();
        let udp = endpoint.local_addr().unwrap();
        let tcp = listener.local_addr().unwrap();
        let fallback = TcpFallback {
            listener,
            tls: server.tcp_tls_config(server_tls),
        };
        let (shutdown, stopped) = oneshot::channel();
        let task = tokio::spawn(server.serve_with_tcp(endpoint, fallback, async move {
            let _ = stopped.await;
        }));

        let mut roots = quinn::rustls::RootCertStore::empty();
        roots.add(cert_der).unwrap();
        let client_tls = quinn::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let link = match transport {
            Transport::Quic => {
                let mut endpoint = quinn::Endpoint::client(loopback).unwrap();
                endpoint.set_default_client_config(
                    ChannelConfig::new().quic_config(client_tls).unwrap(),
                );
                let connection = endpoint.connect(udp, "localhost").unwrap().await.unwrap();
                Link::Quic {
                    endpoint,
                    transport: QuicTransport::new(connection),
                }
            }
            Transport::Qmux => Link::Qmux(
                QmuxTransport::connect(tcp, "localhost", client_tls)
                    .await
                    .unwrap(),
            ),
        };
        (link, shutdown, task)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Closing a `QMux` session needs a runtime context.
        let _guard = self.runtime.enter();
        match &self.link {
            Link::Quic { endpoint, .. } => endpoint.close(0_u8.into(), b"done"),
            Link::Qmux(transport) => transport.close(),
        }
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(server) = self.server.take() {
            let _ = self
                .runtime
                .block_on(tokio::time::timeout(Duration::from_secs(5), server));
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Calls
// ---------------------------------------------------------------------------------------------

/// Call options for a benchmark: no limits that a long-lived stream would eventually hit.
fn options() -> CallOptions {
    CallOptions::new()
        .with_max_response_messages(None)
        .with_max_response_stream_body_size(None)
        .with_stream_idle_timeout(None)
}

/// A client of one of the two services, issuing the four call shapes with a fixed payload.
pub enum Client<T> {
    Raw {
        transport: T,
        request: Blob,
    },
    Greeter {
        client: greeter::GreeterClient<T>,
        request: greeter::HelloRequest,
    },
}

impl<T> Client<T>
where
    T: StreamingRpcTransport,
{
    /// A client over `transport` whose requests carry `size` bytes.
    pub fn new(transport: T, variant: Variant, size: usize) -> Self {
        match variant {
            Variant::Raw => Self::Raw {
                transport,
                request: Blob {
                    data: vec![0xa5; size],
                },
            },
            Variant::Greeter => Self::Greeter {
                client: greeter::GreeterClient::new(transport),
                request: greeter::HelloRequest {
                    name: "x".repeat(size),
                },
            },
        }
    }

    /// One unary round trip.
    pub async fn unary(&self) {
        match self {
            Self::Raw { transport, request } => {
                let reply: Blob = trevrpc::client::unary(transport, RAW_UNARY, request, options())
                    .await
                    .unwrap();
                assert_eq!(reply.data.len(), request.data.len());
            }
            Self::Greeter { client, request } => {
                let reply = client
                    .say_hello(request)
                    .options(|_| options())
                    .await
                    .unwrap();
                assert_eq!(reply.message.len(), request.name.len());
            }
        }
    }

    /// One server-streaming call, reading all [`MESSAGES`] responses.
    pub async fn server_stream(&self) {
        let mut received = 0_usize;
        match self {
            Self::Raw { transport, request } => {
                let mut responses = trevrpc::client::server_streaming::<_, _, Blob>(
                    transport,
                    RAW_SERVER_STREAM,
                    request,
                    options(),
                )
                .await
                .unwrap();
                while let Some(response) = responses.next().await {
                    response.unwrap();
                    received += 1;
                }
            }
            Self::Greeter { client, request } => {
                let mut responses = client
                    .lots_of_replies(request)
                    .options(|_| options())
                    .await
                    .unwrap();
                while let Some(response) = responses.next().await {
                    response.unwrap();
                    received += 1;
                }
            }
        }
        assert_eq!(received, MESSAGES);
    }

    /// One client-streaming call, sending [`MESSAGES`] requests and awaiting the count reply.
    pub async fn client_stream(&self) {
        let sent = match self {
            Self::Raw { transport, request } => {
                let call = trevrpc::client::client_streaming::<_, Blob, Blob>(
                    transport,
                    RAW_CLIENT_STREAM,
                    options(),
                )
                .await
                .unwrap();
                for _ in 0..MESSAGES {
                    call.send(request.clone()).await.unwrap();
                }
                let reply = call.close_and_recv().await.unwrap();
                usize::from_le_bytes(reply.data.try_into().unwrap())
            }
            Self::Greeter { client, request } => {
                let call = client
                    .lots_of_greetings()
                    .options(|_| options())
                    .await
                    .unwrap();
                for _ in 0..MESSAGES {
                    call.send(request.clone()).await.unwrap();
                }
                call.close_and_recv()
                    .await
                    .unwrap()
                    .message
                    .parse()
                    .unwrap()
            }
        };
        assert_eq!(sent, MESSAGES);
    }

    /// Opens a bidirectional stream that [`Bidi::round_trip`] reuses.
    pub async fn open_bidi(&self) -> Bidi {
        match self {
            Self::Raw { transport, request } => Bidi::Raw {
                call: trevrpc::client::bidirectional_streaming(transport, RAW_BIDI, options())
                    .await
                    .unwrap(),
                request: request.clone(),
            },
            Self::Greeter { client, request } => Bidi::Greeter {
                call: client.bidi_hello().options(|_| options()).await.unwrap(),
                request: request.clone(),
            },
        }
    }
}

/// An open bidirectional stream.
pub enum Bidi {
    Raw {
        call: BidirectionalCall<Blob, Blob>,
        request: Blob,
    },
    Greeter {
        call: BidirectionalCall<greeter::HelloRequest, greeter::HelloReply>,
        request: greeter::HelloRequest,
    },
}

impl Bidi {
    /// Sends one request and reads its echo.
    pub async fn round_trip(&mut self) {
        match self {
            Self::Raw { call, request } => {
                call.send(request.clone()).await.unwrap();
                assert!(call.recv().await.unwrap().is_some());
            }
            Self::Greeter { call, request } => {
                call.send(request.clone()).await.unwrap();
                assert!(call.recv().await.unwrap().is_some());
            }
        }
    }
}
