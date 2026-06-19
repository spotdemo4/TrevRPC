#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::convert::Infallible;
use std::error::Error;
use std::hint::black_box;
use std::net::SocketAddr;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::body::Body as TonicBody;
use tonic::codegen::{Body, BoxFuture, Bytes, Service, StdError, http};
use tonic::transport::{Channel, Endpoint, Server as GrpcServer};

#[path = "../examples/shared/greeter.rs"]
#[allow(dead_code)]
mod greeter;

const BENCH_REQUEST_NAME: &str = "benchmark";
const GRPC_SERVICE: &str = "example.greeter.Greeter";
const GRPC_SAY_HELLO_PATH: &str = "/example.greeter.Greeter/SayHello";
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Copy, Debug)]
struct BenchGreeter;

#[trevrpc::async_trait]
impl greeter::Greeter for BenchGreeter {
    async fn say_hello(
        &self,
        request: greeter::HelloRequest,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        Ok(greeter::HelloReply {
            message: request.name,
        })
    }

    async fn lots_of_replies(
        &self,
        _request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Err(trevrpc::Status::unimplemented(
            "benchmark service only implements unary RPCs",
        ))
    }

    async fn lots_of_greetings(
        &self,
        _requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        Err(trevrpc::Status::unimplemented(
            "benchmark service only implements unary RPCs",
        ))
    }

    async fn bidi_hello(
        &self,
        _requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Err(trevrpc::Status::unimplemented(
            "benchmark service only implements unary RPCs",
        ))
    }
}

#[tonic::async_trait]
trait GrpcGreeter: Send + Sync + 'static {
    async fn say_hello(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status>;
}

#[tonic::async_trait]
impl GrpcGreeter for BenchGreeter {
    async fn say_hello(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status> {
        Ok(tonic::Response::new(greeter::HelloReply {
            message: request.into_inner().name,
        }))
    }
}

#[derive(Debug)]
struct GrpcGreeterServer<T> {
    inner: Arc<T>,
}

impl<T> GrpcGreeterServer<T> {
    fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }
}

impl<T, B> Service<http::Request<B>> for GrpcGreeterServer<T>
where
    T: GrpcGreeter,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    type Response = http::Response<TonicBody>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        match req.uri().path() {
            GRPC_SAY_HELLO_PATH => {
                struct SayHelloSvc<T: GrpcGreeter>(Arc<T>);

                impl<T> tonic::server::UnaryService<greeter::HelloRequest> for SayHelloSvc<T>
                where
                    T: GrpcGreeter,
                {
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    type Response = greeter::HelloReply;

                    fn call(
                        &mut self,
                        request: tonic::Request<greeter::HelloRequest>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { GrpcGreeter::say_hello(&*inner, request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.unary(SayHelloSvc(inner), req).await)
                })
            }
            _ => Box::pin(async move {
                let mut response = http::Response::new(TonicBody::default());
                let headers = response.headers_mut();
                headers.insert(
                    tonic::Status::GRPC_STATUS,
                    (tonic::Code::Unimplemented as i32).into(),
                );
                headers.insert(
                    http::header::CONTENT_TYPE,
                    tonic::metadata::GRPC_CONTENT_TYPE,
                );
                Ok(response)
            }),
        }
    }
}

impl<T> Clone for GrpcGreeterServer<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> tonic::server::NamedService for GrpcGreeterServer<T> {
    const NAME: &'static str = GRPC_SERVICE;
}

#[derive(Clone)]
struct GrpcGreeterClient<T> {
    inner: tonic::client::Grpc<T>,
}

impl GrpcGreeterClient<Channel> {
    async fn connect(addr: SocketAddr) -> Result<Self, tonic::transport::Error> {
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?;
        Ok(Self::new(endpoint.connect().await?))
    }
}

impl<T> GrpcGreeterClient<T>
where
    T: tonic::client::GrpcService<TonicBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    fn new(inner: T) -> Self {
        Self {
            inner: tonic::client::Grpc::new(inner),
        }
    }

    async fn say_hello(
        &mut self,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;

        let mut request = tonic::Request::new(request);
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new(GRPC_SERVICE, "SayHello"));

        let path = http::uri::PathAndQuery::from_static(GRPC_SAY_HELLO_PATH);
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .unary(request, path, codec)
            .await
            .map(tonic::Response::into_inner)
    }
}

struct BenchmarkState {
    trevrpc: RunningTrevRpc,
    grpc: RunningGrpc,
}

impl BenchmarkState {
    async fn start() -> BenchResult<Self> {
        let trevrpc = RunningTrevRpc::start().await?;
        let grpc = RunningGrpc::start().await?;

        assert_eq!(
            trevrpc_unary_call(&trevrpc.client).await?,
            BENCH_REQUEST_NAME
        );
        let mut grpc_client = grpc.client.clone();
        assert_eq!(grpc_unary_call(&mut grpc_client).await?, BENCH_REQUEST_NAME);

        Ok(Self { trevrpc, grpc })
    }

    async fn shutdown(self) -> BenchResult {
        self.trevrpc.shutdown().await?;
        self.grpc.shutdown().await
    }
}

struct RunningTrevRpc {
    endpoint: quinn::Endpoint,
    connection: quinn::Connection,
    client: greeter::GreeterClient<trevrpc::quinn::Client>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<trevrpc::Result<()>>>,
}

impl RunningTrevRpc {
    async fn start() -> BenchResult<Self> {
        let mut server = trevrpc::server::Server::new();
        server.set_options(benchmark_server_options());
        greeter::register_greeter(&mut server, BenchGreeter);

        let (server_endpoint, cert_der) = make_trevrpc_server_endpoint(server.options())?;
        let addr = server_endpoint.local_addr()?;
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            server
                .serve_quinn_with_shutdown(server_endpoint, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let endpoint = make_trevrpc_client_endpoint(cert_der)?;
        let connection = endpoint.connect(addr, "localhost")?.await?;
        let client = greeter::GreeterClient::new(trevrpc::quinn::Client::new(connection.clone()));

        Ok(Self {
            endpoint,
            connection,
            client,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    async fn shutdown(mut self) -> BenchResult {
        self.connection.close(0_u32.into(), b"benchmark complete");
        tokio::time::timeout(SHUTDOWN_TIMEOUT, self.endpoint.wait_idle()).await?;

        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await???;
        }

        Ok(())
    }
}

impl Drop for RunningTrevRpc {
    fn drop(&mut self) {
        self.connection.close(0_u32.into(), b"benchmark dropped");
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct RunningGrpc {
    client: GrpcGreeterClient<Channel>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl RunningGrpc {
    async fn start() -> BenchResult<Self> {
        let listener = std::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?;
        listener.set_nonblocking(true)?;
        let addr = listener.local_addr()?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            GrpcServer::builder()
                .add_service(GrpcGreeterServer::new(BenchGreeter))
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let client = GrpcGreeterClient::connect(addr).await?;

        Ok(Self {
            client,
            shutdown: Some(shutdown_tx),
            task: Some(task),
        })
    }

    async fn shutdown(mut self) -> BenchResult {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            tokio::time::timeout(SHUTDOWN_TIMEOUT, task).await???;
        }

        Ok(())
    }
}

impl Drop for RunningGrpc {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn rpc_comparison(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("benchmark runtime should start");
    let state = runtime
        .block_on(BenchmarkState::start())
        .expect("benchmark servers should start");

    let trevrpc_client = state.trevrpc.client.clone();
    let grpc_client = state.grpc.client.clone();
    let mut group = c.benchmark_group("unary_round_trip");

    group.bench_function("trevrpc_quinn", |b| {
        let client = trevrpc_client.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let client = client.clone();
            async move {
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(
                        trevrpc_unary_call(&client)
                            .await
                            .expect("TrevRPC unary call"),
                    );
                }
                start.elapsed()
            }
        });
    });

    group.bench_function("grpc_tonic", |b| {
        let client = grpc_client.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let mut client = client.clone();
            async move {
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(grpc_unary_call(&mut client).await.expect("gRPC unary call"));
                }
                start.elapsed()
            }
        });
    });

    group.finish();
    runtime
        .block_on(state.shutdown())
        .expect("benchmark servers should shut down cleanly");
}

async fn trevrpc_unary_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult<String> {
    let response = client
        .say_hello(
            greeter::HelloRequest {
                name: BENCH_REQUEST_NAME.to_owned(),
            },
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    Ok(response.message)
}

async fn grpc_unary_call(client: &mut GrpcGreeterClient<Channel>) -> BenchResult<String> {
    let response = client
        .say_hello(greeter::HelloRequest {
            name: BENCH_REQUEST_NAME.to_owned(),
        })
        .await?;
    Ok(response.message)
}

fn benchmark_server_options() -> trevrpc::server::ServerOptions {
    trevrpc::server::ServerOptions::new()
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
        .with_max_concurrent_streams_per_connection(Some(512))
        .with_max_concurrent_requests(Some(1024))
}

fn make_trevrpc_server_endpoint(
    options: &trevrpc::server::ServerOptions,
) -> BenchResult<(quinn::Endpoint, CertificateDer<'static>)> {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_der = CertificateDer::from(cert.cert);
    let key_der = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], PrivateKeyDer::from(key_der))?;
    server_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(&mut server_config, options, false);

    Ok((
        quinn::Endpoint::server(server_config, SocketAddr::from(([127, 0, 0, 1], 0)))?,
        cert_der,
    ))
}

fn make_trevrpc_client_endpoint(cert_der: CertificateDer<'static>) -> BenchResult<quinn::Endpoint> {
    let mut roots = quinn::rustls::RootCertStore::empty();
    roots.add(cert_der)?;

    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_crypto.alpn_protocols = vec![trevrpc::ALPN.to_vec()];

    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))?;
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

criterion_group!(benches, rpc_comparison);
criterion_main!(benches);
