#![allow(clippy::missing_errors_doc, clippy::module_name_repetitions)]

use std::convert::Infallible;
use std::error::Error;
use std::hint::black_box;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use criterion::{Criterion, criterion_group, criterion_main};
use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as TonicBody;
use tonic::codegen::{Body, BoxFuture, Bytes, Service, StdError, http};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Server as GrpcServer};

#[path = "../examples/shared/greeter.rs"]
#[allow(dead_code)]
mod greeter;

const BENCH_REQUEST_NAME: &str = "benchmark";
const STREAM_MESSAGE_COUNT: usize = 16;
const GRPC_SERVICE: &str = "example.greeter.Greeter";
const GRPC_SAY_HELLO_PATH: &str = "/example.greeter.Greeter/SayHello";
const GRPC_LOTS_OF_REPLIES_PATH: &str = "/example.greeter.Greeter/LotsOfReplies";
const GRPC_LOTS_OF_GREETINGS_PATH: &str = "/example.greeter.Greeter/LotsOfGreetings";
const GRPC_BIDI_HELLO_PATH: &str = "/example.greeter.Greeter/BidiHello";
const BENCHMARK_QUIC_IDLE_TIMEOUT: Duration = Duration::from_secs(600);
const BENCHMARK_QUIC_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(5);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type GrpcReplyStream =
    Pin<Box<dyn Stream<Item = Result<greeter::HelloReply, tonic::Status>> + Send + 'static>>;

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
        request: greeter::HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(trevrpc::stream::from_iter(server_stream_replies(
            &request.name,
        )))
    }

    async fn lots_of_greetings(
        &self,
        mut requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<greeter::HelloReply, trevrpc::Status> {
        let mut count = 0;
        while let Some(request) = requests.next().await {
            request?;
            count += 1;
        }

        Ok(greeter::HelloReply {
            message: stream_count_message(count),
        })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(Box::new(TrevRpcBidiReplies { requests }))
    }
}

struct TrevRpcBidiReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<greeter::HelloReply> for TrevRpcBidiReplies {
    async fn next(&mut self) -> Option<trevrpc::Result<greeter::HelloReply>> {
        self.requests.next().await.map(|request| {
            request.map(|request| greeter::HelloReply {
                message: request.name,
            })
        })
    }
}

#[tonic::async_trait]
trait GrpcGreeter: Send + Sync + 'static {
    type BidiHelloStream: Stream<Item = Result<greeter::HelloReply, tonic::Status>> + Send + 'static;
    type LotsOfRepliesStream: Stream<Item = Result<greeter::HelloReply, tonic::Status>>
        + Send
        + 'static;

    async fn say_hello(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status>;

    async fn lots_of_replies(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<Self::LotsOfRepliesStream>, tonic::Status>;

    async fn lots_of_greetings(
        &self,
        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status>;

    async fn bidi_hello(
        &self,
        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
    ) -> Result<tonic::Response<Self::BidiHelloStream>, tonic::Status>;
}

#[tonic::async_trait]
impl GrpcGreeter for BenchGreeter {
    type BidiHelloStream = GrpcReplyStream;
    type LotsOfRepliesStream = GrpcReplyStream;

    async fn say_hello(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status> {
        Ok(tonic::Response::new(greeter::HelloReply {
            message: request.into_inner().name,
        }))
    }

    async fn lots_of_replies(
        &self,
        request: tonic::Request<greeter::HelloRequest>,
    ) -> Result<tonic::Response<Self::LotsOfRepliesStream>, tonic::Status> {
        let replies = server_stream_replies(&request.into_inner().name)
            .into_iter()
            .map(Ok::<_, tonic::Status>);

        Ok(tonic::Response::new(Box::pin(tokio_stream::iter(replies))))
    }

    async fn lots_of_greetings(
        &self,
        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
    ) -> Result<tonic::Response<greeter::HelloReply>, tonic::Status> {
        let mut requests = request.into_inner();
        let mut count = 0;
        while let Some(_request) = requests.message().await? {
            count += 1;
        }

        Ok(tonic::Response::new(greeter::HelloReply {
            message: stream_count_message(count),
        }))
    }

    async fn bidi_hello(
        &self,
        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
    ) -> Result<tonic::Response<Self::BidiHelloStream>, tonic::Status> {
        let mut requests = request.into_inner();
        let (sender, receiver) = mpsc::channel(1);
        tokio::spawn(async move {
            loop {
                match requests.message().await {
                    Ok(Some(request)) => {
                        if sender
                            .send(Ok(greeter::HelloReply {
                                message: request.name,
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(status) => {
                        let _ = sender.send(Err(status)).await;
                        break;
                    }
                }
            }
        });

        Ok(tonic::Response::new(Box::pin(ReceiverStream::new(
            receiver,
        ))))
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

    #[allow(clippy::too_many_lines)]
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
            GRPC_LOTS_OF_REPLIES_PATH => {
                struct LotsOfRepliesSvc<T: GrpcGreeter>(Arc<T>);

                impl<T> tonic::server::ServerStreamingService<greeter::HelloRequest> for LotsOfRepliesSvc<T>
                where
                    T: GrpcGreeter,
                {
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    type Response = greeter::HelloReply;
                    type ResponseStream = T::LotsOfRepliesStream;

                    fn call(
                        &mut self,
                        request: tonic::Request<greeter::HelloRequest>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(
                            async move { GrpcGreeter::lots_of_replies(&*inner, request).await },
                        )
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.server_streaming(LotsOfRepliesSvc(inner), req).await)
                })
            }
            GRPC_LOTS_OF_GREETINGS_PATH => {
                struct LotsOfGreetingsSvc<T: GrpcGreeter>(Arc<T>);

                impl<T> tonic::server::ClientStreamingService<greeter::HelloRequest> for LotsOfGreetingsSvc<T>
                where
                    T: GrpcGreeter,
                {
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    type Response = greeter::HelloReply;

                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(
                            async move { GrpcGreeter::lots_of_greetings(&*inner, request).await },
                        )
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.client_streaming(LotsOfGreetingsSvc(inner), req).await)
                })
            }
            GRPC_BIDI_HELLO_PATH => {
                struct BidiHelloSvc<T: GrpcGreeter>(Arc<T>);

                impl<T> tonic::server::StreamingService<greeter::HelloRequest> for BidiHelloSvc<T>
                where
                    T: GrpcGreeter,
                {
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    type Response = greeter::HelloReply;
                    type ResponseStream = T::BidiHelloStream;

                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<greeter::HelloRequest>>,
                    ) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        Box::pin(async move { GrpcGreeter::bidi_hello(&*inner, request).await })
                    }
                }

                let inner = Arc::clone(&self.inner);
                Box::pin(async move {
                    let codec = tonic_prost::ProstCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec);
                    Ok(grpc.streaming(BidiHelloSvc(inner), req).await)
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
        let endpoint = Endpoint::from_shared(format!("http://{addr}"))?.tcp_nodelay(true);
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

    async fn lots_of_replies(
        &mut self,
        request: greeter::HelloRequest,
    ) -> Result<tonic::Streaming<greeter::HelloReply>, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;

        let mut request = tonic::Request::new(request);
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new(GRPC_SERVICE, "LotsOfReplies"));

        let path = http::uri::PathAndQuery::from_static(GRPC_LOTS_OF_REPLIES_PATH);
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .server_streaming(request, path, codec)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn lots_of_greetings(&mut self) -> Result<greeter::HelloReply, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;

        let mut request = tonic::Request::new(tokio_stream::iter(benchmark_requests()));
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new(GRPC_SERVICE, "LotsOfGreetings"));

        let path = http::uri::PathAndQuery::from_static(GRPC_LOTS_OF_GREETINGS_PATH);
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .client_streaming(request, path, codec)
            .await
            .map(tonic::Response::into_inner)
    }

    async fn bidi_hello(&mut self) -> Result<tonic::Streaming<greeter::HelloReply>, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;

        let mut request = tonic::Request::new(tokio_stream::iter(benchmark_requests()));
        request
            .extensions_mut()
            .insert(tonic::GrpcMethod::new(GRPC_SERVICE, "BidiHello"));

        let path = http::uri::PathAndQuery::from_static(GRPC_BIDI_HELLO_PATH);
        let codec = tonic_prost::ProstCodec::default();
        self.inner
            .streaming(request, path, codec)
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
        assert_eq!(
            trevrpc_server_streaming_call(&trevrpc.client).await?,
            STREAM_MESSAGE_COUNT
        );
        assert_eq!(
            trevrpc_client_streaming_call(&trevrpc.client).await?,
            STREAM_MESSAGE_COUNT
        );
        assert_eq!(
            trevrpc_bidi_streaming_call(&trevrpc.client).await?,
            STREAM_MESSAGE_COUNT
        );

        let mut grpc_client = grpc.client.clone();
        assert_eq!(grpc_unary_call(&mut grpc_client).await?, BENCH_REQUEST_NAME);
        assert_eq!(
            grpc_server_streaming_call(&mut grpc_client).await?,
            STREAM_MESSAGE_COUNT
        );
        assert_eq!(
            grpc_client_streaming_call(&mut grpc_client).await?,
            STREAM_MESSAGE_COUNT
        );
        assert_eq!(
            grpc_bidi_streaming_call(&mut grpc_client).await?,
            STREAM_MESSAGE_COUNT
        );

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
        let incoming =
            TcpIncoming::bind(SocketAddr::from(([127, 0, 0, 1], 0)))?.with_nodelay(Some(true));
        let addr = incoming.local_addr()?;
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

#[allow(clippy::too_many_lines)]
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

    let mut group = c.benchmark_group(format!("server_stream_{STREAM_MESSAGE_COUNT}_messages"));

    group.bench_function("trevrpc_quinn", |b| {
        let client = trevrpc_client.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let client = client.clone();
            async move {
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(
                        trevrpc_server_streaming_call(&client)
                            .await
                            .expect("TrevRPC server-streaming call"),
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
                    black_box(
                        grpc_server_streaming_call(&mut client)
                            .await
                            .expect("gRPC server-streaming call"),
                    );
                }
                start.elapsed()
            }
        });
    });

    group.finish();

    let mut group = c.benchmark_group(format!("client_stream_{STREAM_MESSAGE_COUNT}_messages"));

    group.bench_function("trevrpc_quinn", |b| {
        let client = trevrpc_client.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let client = client.clone();
            async move {
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(
                        trevrpc_client_streaming_call(&client)
                            .await
                            .expect("TrevRPC client-streaming call"),
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
                    black_box(
                        grpc_client_streaming_call(&mut client)
                            .await
                            .expect("gRPC client-streaming call"),
                    );
                }
                start.elapsed()
            }
        });
    });

    group.finish();

    let mut group = c.benchmark_group(format!("bidi_stream_{STREAM_MESSAGE_COUNT}_messages"));

    group.bench_function("trevrpc_quinn", |b| {
        let client = trevrpc_client.clone();
        b.to_async(&runtime).iter_custom(move |iters| {
            let client = client.clone();
            async move {
                let start = Instant::now();
                for _ in 0..iters {
                    black_box(
                        trevrpc_bidi_streaming_call(&client)
                            .await
                            .expect("TrevRPC bidi-streaming call"),
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
                    black_box(
                        grpc_bidi_streaming_call(&mut client)
                            .await
                            .expect("gRPC bidi-streaming call"),
                    );
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

async fn trevrpc_server_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult<usize> {
    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: BENCH_REQUEST_NAME.to_owned(),
            },
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    let mut count = 0;

    while let Some(reply) = replies.next().await {
        let _reply = reply?;
        count += 1;
    }

    Ok(count)
}

async fn grpc_server_streaming_call(client: &mut GrpcGreeterClient<Channel>) -> BenchResult<usize> {
    let mut replies = client
        .lots_of_replies(greeter::HelloRequest {
            name: BENCH_REQUEST_NAME.to_owned(),
        })
        .await?;
    let mut count = 0;

    while let Some(_reply) = replies.message().await? {
        count += 1;
    }

    Ok(count)
}

async fn trevrpc_client_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult<usize> {
    let mut call = client
        .lots_of_greetings(trevrpc::client::CallOptions::new())
        .await?;

    for request in benchmark_requests() {
        call.send(request).await?;
    }

    let response = call.close_and_recv().await?;
    parse_stream_count(&response.message)
}

async fn grpc_client_streaming_call(client: &mut GrpcGreeterClient<Channel>) -> BenchResult<usize> {
    let response = client.lots_of_greetings().await?;
    parse_stream_count(&response.message)
}

async fn trevrpc_bidi_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult<usize> {
    let mut call = client
        .bidi_hello(trevrpc::client::CallOptions::new())
        .await?;

    for request in benchmark_requests() {
        call.send(request).await?;
    }
    call.close_send()?;

    let mut count = 0;
    while let Some(_reply) = call.recv().await? {
        count += 1;
    }

    Ok(count)
}

async fn grpc_bidi_streaming_call(client: &mut GrpcGreeterClient<Channel>) -> BenchResult<usize> {
    let mut replies = client.bidi_hello().await?;
    let mut count = 0;

    while let Some(_reply) = replies.message().await? {
        count += 1;
    }

    Ok(count)
}

fn benchmark_requests() -> impl Iterator<Item = greeter::HelloRequest> {
    (0..STREAM_MESSAGE_COUNT).map(|index| greeter::HelloRequest {
        name: format!("{BENCH_REQUEST_NAME}-{index}"),
    })
}

fn server_stream_replies(name: &str) -> Vec<greeter::HelloReply> {
    (0..STREAM_MESSAGE_COUNT)
        .map(|index| greeter::HelloReply {
            message: format!("{name}-{index}"),
        })
        .collect()
}

fn stream_count_message(count: usize) -> String {
    count.to_string()
}

fn parse_stream_count(message: &str) -> BenchResult<usize> {
    Ok(message.parse()?)
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
    server_config.transport_config(benchmark_transport_config(
        trevrpc::quinn::transport_limits_from_server_options(options, false),
    )?);

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
    client_config.transport_config(benchmark_transport_config(
        trevrpc::quinn::client_transport_limits(trevrpc::framing::DEFAULT_MAX_FRAME_SIZE, false),
    )?);
    endpoint.set_default_client_config(client_config);

    Ok(endpoint)
}

fn benchmark_transport_config(
    limits: trevrpc::quinn::TransportLimits,
) -> BenchResult<Arc<quinn::TransportConfig>> {
    let mut transport = quinn::TransportConfig::default();
    trevrpc::quinn::apply_transport_limits(&mut transport, limits);
    transport.max_idle_timeout(Some(BENCHMARK_QUIC_IDLE_TIMEOUT.try_into()?));
    transport.keep_alive_interval(Some(BENCHMARK_QUIC_KEEP_ALIVE_INTERVAL));

    Ok(Arc::new(transport))
}

criterion_group!(benches, rpc_comparison);
criterion_main!(benches);
