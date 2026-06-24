use std::convert::Infallible;
use std::error::Error;
use std::fs;
use std::future::Future;
use std::net::{Ipv6Addr, SocketAddr};
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::rustls;
use quinn::rustls::pki_types::{
    CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime,
};
use tokio::sync::mpsc;
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::body::Body as TonicBody;
use tonic::codegen::{Body, BoxFuture, Bytes, Service, StdError, http};
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Endpoint, Server as GrpcServer};

#[allow(dead_code)]
#[path = "shared/greeter.rs"]
mod greeter;

const REQUEST_NAME: &str = "TrevRPC benchmark";
const LATENCY_STREAM_MESSAGE_COUNT: usize = 1;
const GRPC_SERVICE: &str = "example.greeter.Greeter";
const GRPC_SAY_HELLO_PATH: &str = "/example.greeter.Greeter/SayHello";
const GRPC_LOTS_OF_REPLIES_PATH: &str = "/example.greeter.Greeter/LotsOfReplies";
const GRPC_LOTS_OF_GREETINGS_PATH: &str = "/example.greeter.Greeter/LotsOfGreetings";
const GRPC_BIDI_HELLO_PATH: &str = "/example.greeter.Greeter/BidiHello";

type BenchResult<T = ()> = Result<T, Box<dyn Error + Send + Sync>>;
type GrpcReplyStream =
    Pin<Box<dyn Stream<Item = Result<greeter::HelloReply, tonic::Status>> + Send + 'static>>;

struct SplitGreeter;

#[tokio::main]
async fn main() -> BenchResult {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("client") => {
            let addr = required_arg(&mut args, "addr")?.parse::<SocketAddr>()?;
            let cert = required_arg(&mut args, "cert")?;
            let iterations = required_arg(&mut args, "iterations")?.parse::<u32>()?;
            run_client(addr, Path::new(&cert), iterations).await
        }
        Some("server") => {
            let addr = args
                .next()
                .unwrap_or_else(|| "127.0.0.1:0".to_owned())
                .parse::<SocketAddr>()?;
            run_server(addr).await
        }
        Some("webtransport-server") => {
            let addr = required_arg(&mut args, "addr")?.parse::<SocketAddr>()?;
            let cert = required_arg(&mut args, "cert")?;
            let origin = required_arg(&mut args, "origin")?;
            run_webtransport_server(addr, Path::new(&cert), origin).await
        }
        Some("grpc-client") => {
            let addr = required_arg(&mut args, "addr")?.parse::<SocketAddr>()?;
            let iterations = required_arg(&mut args, "iterations")?.parse::<u32>()?;
            run_grpc_client(addr, iterations).await
        }
        Some("grpc-server") => {
            let addr = args
                .next()
                .unwrap_or_else(|| "127.0.0.1:0".to_owned())
                .parse::<SocketAddr>()?;
            run_grpc_server(addr).await
        }
        _ => Err(
            "usage: rpc_split_bench client <addr> <cert> <iterations> | server [addr] | webtransport-server <addr> <cert> <origin> | grpc-client <addr> <iterations> | grpc-server [addr]"
                .into(),
        ),
    }
}

async fn run_client(addr: SocketAddr, cert_path: &Path, iterations: u32) -> BenchResult {
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let endpoint = make_client_endpoint(cert_path)?;
    let connection = endpoint.connect(addr, "localhost")?.await?;
    let client = greeter::GreeterClient::new(trevrpc::quinn::Client::new(connection.clone()));

    warm_client(&client).await?;
    run_latency_case("unary_latency", iterations, || trevrpc_unary_call(&client)).await?;
    run_latency_case("server_stream_latency", iterations, || {
        trevrpc_server_streaming_call(&client, LATENCY_STREAM_MESSAGE_COUNT)
    })
    .await?;
    run_message_throughput_case("server_stream_throughput", iterations, || {
        trevrpc_server_streaming_call(&client, usize::try_from(iterations).unwrap())
    })
    .await?;
    run_latency_case("client_stream_latency", iterations, || {
        trevrpc_client_streaming_call(&client, LATENCY_STREAM_MESSAGE_COUNT)
    })
    .await?;
    run_message_throughput_case("client_stream_throughput", iterations, || {
        trevrpc_client_streaming_call(&client, usize::try_from(iterations).unwrap())
    })
    .await?;
    run_latency_case("bidi_stream_latency", iterations, || {
        trevrpc_bidi_streaming_call(&client, LATENCY_STREAM_MESSAGE_COUNT)
    })
    .await?;
    run_message_throughput_case("bidi_stream_throughput", iterations, || {
        trevrpc_bidi_streaming_call(&client, usize::try_from(iterations).unwrap())
    })
    .await?;

    connection.close(0_u32.into(), b"split benchmark complete");
    endpoint.wait_idle().await;
    Ok(())
}

async fn run_server(addr: SocketAddr) -> BenchResult {
    let mut server = trevrpc::server::Server::new();
    server.set_options(benchmark_server_options());
    greeter::register_greeter(&mut server, SplitGreeter);
    let endpoint = make_server_endpoint(addr, server.options())?;
    let local_addr = endpoint.local_addr()?;

    println!("PORT {}", local_addr.port());
    server
        .serve_quinn_with_shutdown(endpoint, shutdown_signal())
        .await?;
    Ok(())
}

async fn run_webtransport_server(
    addr: SocketAddr,
    cert_path: &Path,
    origin: String,
) -> BenchResult {
    let mut server = trevrpc::server::Server::new();
    server.set_options(benchmark_webtransport_server_options(origin));
    greeter::register_greeter(&mut server, SplitGreeter);
    let endpoint = make_webtransport_server_endpoint(addr, server.options(), cert_path)?;
    let local_addr = endpoint.local_addr()?;

    println!("PORT {}", local_addr.port());
    println!("CERT {}", cert_path.display());
    server
        .serve_quinn_and_webtransport_with_shutdown(endpoint, shutdown_signal())
        .await?;
    Ok(())
}

async fn run_grpc_client(addr: SocketAddr, iterations: u32) -> BenchResult {
    if iterations == 0 {
        return Err("iterations must be positive".into());
    }
    let mut client = GrpcGreeterClient::connect(addr).await?;

    warm_grpc_client(&mut client).await?;
    let throughput_messages = usize::try_from(iterations)?;

    let start = Instant::now();
    for _ in 0..iterations {
        grpc_unary_call(&mut client).await?;
    }
    print_latency_result("unary_latency", iterations, start.elapsed());

    let start = Instant::now();
    for _ in 0..iterations {
        grpc_server_streaming_call(&mut client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    }
    print_latency_result("server_stream_latency", iterations, start.elapsed());

    let start = Instant::now();
    grpc_server_streaming_call(&mut client, throughput_messages).await?;
    print_message_throughput_result("server_stream_throughput", iterations, start.elapsed());

    let start = Instant::now();
    for _ in 0..iterations {
        grpc_client_streaming_call(&mut client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    }
    print_latency_result("client_stream_latency", iterations, start.elapsed());

    let start = Instant::now();
    grpc_client_streaming_call(&mut client, throughput_messages).await?;
    print_message_throughput_result("client_stream_throughput", iterations, start.elapsed());

    let start = Instant::now();
    for _ in 0..iterations {
        grpc_bidi_streaming_call(&mut client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    }
    print_latency_result("bidi_stream_latency", iterations, start.elapsed());

    let start = Instant::now();
    grpc_bidi_streaming_call(&mut client, throughput_messages).await?;
    print_message_throughput_result("bidi_stream_throughput", iterations, start.elapsed());

    Ok(())
}

async fn run_grpc_server(addr: SocketAddr) -> BenchResult {
    let incoming = TcpIncoming::bind(addr)?.with_nodelay(Some(true));
    let local_addr = incoming.local_addr()?;
    println!("PORT {}", local_addr.port());
    GrpcServer::builder()
        .add_service(GrpcGreeterServer::new(SplitGreeter))
        .serve_with_incoming_shutdown(incoming, shutdown_signal())
        .await?;
    Ok(())
}

async fn warm_client(client: &greeter::GreeterClient<trevrpc::quinn::Client>) -> BenchResult {
    trevrpc_unary_call(client).await?;
    trevrpc_server_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    trevrpc_client_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    trevrpc_bidi_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    Ok(())
}

async fn warm_grpc_client(client: &mut GrpcGreeterClient<Channel>) -> BenchResult {
    grpc_unary_call(client).await?;
    grpc_server_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    grpc_client_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    grpc_bidi_streaming_call(client, LATENCY_STREAM_MESSAGE_COUNT).await?;
    Ok(())
}

async fn run_latency_case<F, Fut>(name: &str, iterations: u32, mut call: F) -> BenchResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = BenchResult>,
{
    let start = Instant::now();
    for _ in 0..iterations {
        call().await?;
    }
    let elapsed = start.elapsed();
    print_latency_result(name, iterations, elapsed);
    Ok(())
}

async fn run_message_throughput_case<F, Fut>(name: &str, messages: u32, mut call: F) -> BenchResult
where
    F: FnMut() -> Fut,
    Fut: Future<Output = BenchResult>,
{
    let start = Instant::now();
    call().await?;
    let elapsed = start.elapsed();
    print_message_throughput_result(name, messages, elapsed);
    Ok(())
}

fn print_latency_result(name: &str, iterations: u32, elapsed: Duration) {
    let latency_us = elapsed.as_secs_f64() * 1_000_000.0 / f64::from(iterations);
    println!(
        "{name}: {latency_us:.3} us/op ({iterations} iterations in {:.3}s)",
        elapsed.as_secs_f64()
    );
}

fn print_message_throughput_result(name: &str, messages: u32, elapsed: Duration) {
    let messages_per_second = f64::from(messages) / elapsed.as_secs_f64();
    println!(
        "{name}: {messages_per_second:.0} messages/s ({messages} messages in {:.3}s)",
        elapsed.as_secs_f64()
    );
}

async fn trevrpc_unary_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
) -> BenchResult {
    let response = client
        .say_hello(
            greeter::HelloRequest {
                name: REQUEST_NAME.to_owned(),
            },
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    if response.message != REQUEST_NAME {
        return Err(format!("unary response = {:?}", response.message).into());
    }
    Ok(())
}

async fn trevrpc_server_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
    message_count: usize,
) -> BenchResult {
    let mut replies = client
        .lots_of_replies(
            greeter::HelloRequest {
                name: message_count.to_string(),
            },
            trevrpc::client::CallOptions::new().with_max_response_messages(Some(message_count)),
        )
        .await?;
    let mut count = 0;
    while let Some(reply) = replies.next().await {
        if reply?.message != "server stream" {
            return Err("unexpected server-stream response".into());
        }
        count += 1;
    }
    if count != message_count {
        return Err(format!("server stream count = {count}, want {message_count}").into());
    }
    Ok(())
}

async fn trevrpc_client_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
    message_count: usize,
) -> BenchResult {
    let response = client
        .lots_of_greetings_from_stream(
            trevrpc::stream::from_iter(benchmark_requests(message_count)),
            trevrpc::client::CallOptions::new(),
        )
        .await?;
    let expected = format!("streamed {message_count} greetings");
    if response.message != expected {
        return Err(format!("client stream response = {:?}", response.message).into());
    }
    Ok(())
}

async fn trevrpc_bidi_streaming_call(
    client: &greeter::GreeterClient<trevrpc::quinn::Client>,
    message_count: usize,
) -> BenchResult {
    let mut replies = client
        .bidi_hello_from_stream(
            trevrpc::stream::from_iter(benchmark_requests(message_count)),
            trevrpc::client::CallOptions::new().with_max_response_messages(Some(message_count)),
        )
        .await?;
    let mut count = 0;
    while let Some(reply) = replies.next().await.transpose()? {
        if reply.message != REQUEST_NAME {
            return Err("unexpected bidi response".into());
        }
        count += 1;
    }
    if count != message_count {
        return Err(format!("bidi stream count = {count}, want {message_count}").into());
    }
    Ok(())
}

async fn grpc_unary_call(client: &mut GrpcGreeterClient<Channel>) -> BenchResult {
    let response = client
        .say_hello(greeter::HelloRequest {
            name: REQUEST_NAME.to_owned(),
        })
        .await?;
    if response.message != REQUEST_NAME {
        return Err(format!("gRPC unary response = {:?}", response.message).into());
    }
    Ok(())
}

async fn grpc_server_streaming_call(
    client: &mut GrpcGreeterClient<Channel>,
    message_count: usize,
) -> BenchResult {
    let mut replies = client
        .lots_of_replies(greeter::HelloRequest {
            name: message_count.to_string(),
        })
        .await?;
    let mut count = 0;
    while let Some(reply) = replies.message().await? {
        if reply.message != "server stream" {
            return Err("unexpected gRPC server-stream response".into());
        }
        count += 1;
    }
    if count != message_count {
        return Err(format!("gRPC server stream count = {count}, want {message_count}").into());
    }
    Ok(())
}

async fn grpc_client_streaming_call(
    client: &mut GrpcGreeterClient<Channel>,
    message_count: usize,
) -> BenchResult {
    let response = client.lots_of_greetings(message_count).await?;
    let expected = format!("streamed {message_count} greetings");
    if response.message != expected {
        return Err(format!("gRPC client stream response = {:?}", response.message).into());
    }
    Ok(())
}

async fn grpc_bidi_streaming_call(
    client: &mut GrpcGreeterClient<Channel>,
    message_count: usize,
) -> BenchResult {
    let mut replies = client.bidi_hello(message_count).await?;
    let mut count = 0;
    while let Some(reply) = replies.message().await? {
        if reply.message != REQUEST_NAME {
            return Err("unexpected gRPC bidi response".into());
        }
        count += 1;
    }
    if count != message_count {
        return Err(format!("gRPC bidi stream count = {count}, want {message_count}").into());
    }
    Ok(())
}

fn benchmark_requests(message_count: usize) -> impl Iterator<Item = greeter::HelloRequest> {
    (0..message_count).map(|_| greeter::HelloRequest {
        name: REQUEST_NAME.to_owned(),
    })
}

#[trevrpc::async_trait]
impl greeter::Greeter for SplitGreeter {
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
        let count = message_count_from_name(&request.name);
        Ok(trevrpc::stream::from_iter((0..count).map(|_| {
            greeter::HelloReply {
                message: "server stream".to_owned(),
            }
        })))
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
            message: format!("streamed {count} greetings"),
        })
    }

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<greeter::HelloReply>, trevrpc::Status> {
        Ok(Box::new(BidiReplies { requests }))
    }
}

fn message_count_from_name(name: &str) -> usize {
    name.parse::<usize>()
        .ok()
        .filter(|count| *count > 0)
        .unwrap_or(LATENCY_STREAM_MESSAGE_COUNT)
}

struct BidiReplies {
    requests: trevrpc::BoxMessageStream<greeter::HelloRequest>,
}

#[trevrpc::async_trait]
impl trevrpc::MessageStream<greeter::HelloReply> for BidiReplies {
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
impl GrpcGreeter for SplitGreeter {
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
        let count = message_count_from_name(&request.into_inner().name);
        let replies = (0..count).map(|_| {
            Ok::<_, tonic::Status>(greeter::HelloReply {
                message: "server stream".to_owned(),
            })
        });
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
            message: format!("streamed {count} greetings"),
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

    async fn lots_of_greetings(
        &mut self,
        message_count: usize,
    ) -> Result<greeter::HelloReply, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;
        let mut request =
            tonic::Request::new(tokio_stream::iter(benchmark_requests(message_count)));
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

    async fn bidi_hello(
        &mut self,
        message_count: usize,
    ) -> Result<tonic::Streaming<greeter::HelloReply>, tonic::Status> {
        self.inner.ready().await.map_err(|error| {
            tonic::Status::unknown(format!("gRPC service was not ready: {}", error.into()))
        })?;
        let mut request =
            tonic::Request::new(tokio_stream::iter(benchmark_requests(message_count)));
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

fn benchmark_server_options() -> trevrpc::server::ServerOptions {
    trevrpc::server::ServerOptions::new()
        .with_graceful_shutdown_timeout(Some(Duration::from_millis(200)))
        .with_max_concurrent_connections(Some(512))
        .with_max_concurrent_streams_per_connection(Some(128))
        .with_max_concurrent_requests(Some(1024))
        .with_max_stream_messages(None)
        .with_max_stream_body_size(None)
}

fn benchmark_webtransport_server_options(origin: String) -> trevrpc::server::ServerOptions {
    let origin: &'static str = Box::leak(origin.into_boxed_str());
    let origins: &'static [&'static str] = Box::leak(vec![origin].into_boxed_slice());
    benchmark_server_options()
        .with_max_concurrent_streams_per_connection(Some(65_535))
        .with_webtransport_allowed_origins(origins)
}

fn make_server_endpoint(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
) -> BenchResult<quinn::Endpoint> {
    let identity = make_identity()?;
    make_server_endpoint_with_identity(
        addr,
        options,
        &identity,
        vec![trevrpc::ALPN.to_vec()],
        false,
    )
}

fn make_webtransport_server_endpoint(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
    cert_path: &Path,
) -> BenchResult<quinn::Endpoint> {
    let identity = make_identity()?;
    write_certificate(&identity, cert_path)?;
    make_server_endpoint_with_identity(
        addr,
        options,
        &identity,
        vec![
            trevrpc::ALPN.to_vec(),
            web_transport_quinn::ALPN.as_bytes().to_vec(),
        ],
        true,
    )
}

fn make_identity() -> BenchResult<rcgen::CertifiedKey<rcgen::KeyPair>> {
    let signing_key = rcgen::KeyPair::generate()?;
    let not_before = time::OffsetDateTime::now_utc() - time::Duration::hours(1);
    let mut params =
        rcgen::CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])?;
    params.not_before = not_before;
    params.not_after = not_before + time::Duration::hours(25);
    let cert = params.self_signed(&signing_key)?;

    Ok(rcgen::CertifiedKey { cert, signing_key })
}

fn make_server_endpoint_with_identity(
    addr: SocketAddr,
    options: &trevrpc::server::ServerOptions,
    identity: &rcgen::CertifiedKey<rcgen::KeyPair>,
    alpn_protocols: Vec<Vec<u8>>,
    enable_webtransport: bool,
) -> BenchResult<quinn::Endpoint> {
    let key_der = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());

    let mut server_crypto = quinn::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![identity.cert.der().clone()],
            PrivateKeyDer::from(key_der),
        )?;
    server_crypto.alpn_protocols = alpn_protocols;

    let mut server_config =
        quinn::ServerConfig::with_crypto(Arc::new(QuicServerConfig::try_from(server_crypto)?));
    trevrpc::quinn::configure_server_config(&mut server_config, options, enable_webtransport);
    Ok(quinn::Endpoint::server(server_config, addr)?)
}

fn write_certificate(identity: &rcgen::CertifiedKey<rcgen::KeyPair>, path: &Path) -> BenchResult {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, identity.cert.pem())?;
    Ok(())
}

fn make_client_endpoint(cert_path: &Path) -> BenchResult<quinn::Endpoint> {
    let _ = cert_path;
    let mut client_crypto = quinn::rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(SkipServerVerification::new())
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

#[derive(Debug)]
struct SkipServerVerification(Arc<rustls::crypto::CryptoProvider>);

impl SkipServerVerification {
    fn new() -> Arc<Self> {
        Arc::new(Self(Arc::new(rustls::crypto::ring::default_provider())))
    }
}

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = signal.recv() => {}
            }
            return;
        }
    }

    let _ = tokio::signal::ctrl_c().await;
}

fn required_arg(args: &mut impl Iterator<Item = String>, name: &str) -> BenchResult<String> {
    args.next()
        .ok_or_else(|| format!("missing {name} argument").into())
}
