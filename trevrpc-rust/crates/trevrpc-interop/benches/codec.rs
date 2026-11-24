//! Microbenchmarks of the codecs on the interop `EchoRequest`, at the load benchmarks' small and
//! large message sizes: decoding a received body into a `prost` message, an owned `buffa` message
//! and a `buffa` view, as a route does, and encoding the owned messages, as a client does.
//!
//! `unary` times whole `Echo.Unary` round trips over native QUIC on loopback, with each codec on
//! both ends, as the generated code of each uses it: the codec's share of a call.
//!
//! Run with `cargo bench -p trevrpc-interop --bench codec`; add `-- --test` to run each benchmark
//! once.

// Benchmark setup fails loudly: a panic here is the intended way to stop a broken benchmark.
// The benchmark modules import the shared helpers wholesale.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::wildcard_imports)]

use std::future::Future;
use std::time::Duration;

use divan::{Bencher, black_box};
use tokio::runtime::{Builder, Runtime};
use trevrpc::client::{Channel, ChannelConfig};
use trevrpc::codec::{Buffa, BuffaView, BuffaViewHandle, Decode as _, Encode as _, Prost};
use trevrpc::server::{RequestContext, Server, ServerOptions};
use trevrpc::{BoxStream, Bytes, Status};

#[path = "../src/proto.rs"]
mod proto;

use proto::buffa as pb;

#[allow(dead_code)]
#[path = "../../../tests/support/serving.rs"]
mod serving;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// Message sizes: the load benchmarks' small and large sizes.
const SIZES: [usize; 2] = [64, 4096];

fn main() {
    divan::main();
}

fn prost_request(size: usize) -> proto::EchoRequest {
    proto::EchoRequest {
        message: "x".repeat(size),
        count: 3,
        ..Default::default()
    }
}

fn buffa_request(size: usize) -> pb::EchoRequest {
    pb::EchoRequest {
        message: "x".repeat(size),
        count: 3,
        ..Default::default()
    }
}

/// The body of a request with a message of `size` bytes, as a route receives it.
fn body(size: usize) -> Bytes {
    Bytes::from(Prost.encode(&prost_request(size)).unwrap())
}

mod decode {
    use super::*;

    #[divan::bench(args = SIZES)]
    fn prost(bencher: Bencher, size: usize) {
        let body = body(size);
        bencher
            .with_inputs(|| body.clone())
            .bench_values(|body| -> proto::EchoRequest { Prost.decode(body).unwrap() });
    }

    #[divan::bench(args = SIZES)]
    fn buffa(bencher: Bencher, size: usize) {
        let body = body(size);
        bencher
            .with_inputs(|| body.clone())
            .bench_values(|body| -> pb::EchoRequest { Buffa.decode(body).unwrap() });
    }

    /// Decodes the view and reads its message, which the view reads from the body in place.
    #[divan::bench(args = SIZES)]
    fn buffa_view(bencher: Bencher, size: usize) {
        let body = body(size);
        let codec = BuffaView::<pb::EchoRequest>::new();
        bencher.with_inputs(|| body.clone()).bench_values(|body| {
            let view: BuffaViewHandle<pb::EchoRequest> = codec.decode(body).unwrap();
            black_box(view.message().len());
            view
        });
    }
}

mod encode {
    use super::*;

    #[divan::bench(args = SIZES)]
    fn prost(bencher: Bencher, size: usize) {
        let request = prost_request(size);
        bencher.bench(|| Prost.encode(black_box(&request)).unwrap());
    }

    #[divan::bench(args = SIZES)]
    fn buffa(bencher: Bencher, size: usize) {
        let request = buffa_request(size);
        bencher.bench(|| Buffa.encode(black_box(&request)).unwrap());
    }
}

/// An error for the call shapes the round-trip benchmarks do not make.
fn not_benchmarked() -> Status {
    Status::unimplemented("not benchmarked")
}

/// Implements `Echo.Unary` as `$unary` and leaves the other methods unimplemented, for the
/// service trait `$trait` of `$request` and `$response` messages.
macro_rules! unary_echo {
    ($service:ident: $trait:path, $request:ty, $response:ty, $unary:expr) => {
        struct $service;

        #[trevrpc::async_trait]
        impl $trait for $service {
            async fn unary(
                &self,
                _context: RequestContext,
                request: $request,
            ) -> Result<$response, Status> {
                Ok($unary(request))
            }

            async fn client_stream(
                &self,
                _context: RequestContext,
                _requests: BoxStream<$request>,
            ) -> Result<$response, Status> {
                Err(not_benchmarked())
            }

            async fn server_stream(
                &self,
                _context: RequestContext,
                _request: $request,
            ) -> Result<BoxStream<$response>, Status> {
                Err(not_benchmarked())
            }

            async fn bidi(
                &self,
                _context: RequestContext,
                _requests: BoxStream<$request>,
            ) -> Result<BoxStream<$response>, Status> {
                Err(not_benchmarked())
            }
        }
    };
}

fn prost_echo(request: proto::EchoRequest) -> proto::EchoResponse {
    proto::EchoResponse {
        message: request.message,
        index: 0,
    }
}

fn buffa_echo(request: pb::EchoRequest) -> pb::EchoResponse {
    pb::EchoResponse {
        message: request.message,
        ..Default::default()
    }
}

/// Reads the request's message from its body, and copies it only into the response.
#[expect(
    clippy::needless_pass_by_value,
    reason = "the handler owns the request, as every handler does"
)]
fn view_echo(request: BuffaViewHandle<pb::EchoRequest>) -> pb::EchoResponse {
    pb::EchoResponse {
        message: request.message().to_owned(),
        ..Default::default()
    }
}

unary_echo!(ProstEcho: proto::Echo, proto::EchoRequest, proto::EchoResponse, prost_echo);
unary_echo!(BuffaEcho: pb::Echo, pb::EchoRequest, pb::EchoResponse, buffa_echo);
unary_echo!(
    ViewEcho: pb::views::Echo,
    BuffaViewHandle<pb::EchoRequest>,
    pb::EchoResponse,
    view_echo
);

/// A runtime, a server on loopback that `register` set up, and a channel connected to it.
struct Fixture {
    runtime: Runtime,
    channel: Channel,
    server: Option<serving::Serving>,
    _endpoint: quinn::Endpoint,
}

impl Fixture {
    fn start(register: impl FnOnce(&mut Server)) -> Self {
        let runtime = Builder::new_multi_thread().enable_all().build().unwrap();
        let mut server = Server::new();
        server.set_options(
            ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(200))),
        );
        register(&mut server);
        let (server, channel, endpoint) = runtime.block_on(async {
            let server = serving::serve(server, serving::self_signed().unwrap())
                .await
                .unwrap();
            let mut roots = quinn::rustls::RootCertStore::empty();
            roots.add(server.cert.clone()).unwrap();
            let tls = quinn::rustls::ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let config = ChannelConfig::new();
            let mut endpoint =
                trevrpc::udp::client_endpoint((std::net::Ipv4Addr::LOCALHOST, 0).into()).unwrap();
            endpoint.set_default_client_config(config.quic_config(tls).unwrap());
            let channel =
                Channel::connect_with_config(endpoint.clone(), server.udp, "localhost", config)
                    .await
                    .unwrap();
            (server, channel, endpoint)
        });
        Self {
            runtime,
            channel,
            server: Some(server),
            _endpoint: endpoint,
        }
    }

    /// Times `call`, one round trip, on the fixture's runtime.
    fn bench<F: Future>(&self, bencher: Bencher, call: impl Fn() -> F) {
        bencher.bench_local(|| self.runtime.block_on(call()));
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.channel.close();
        if let Some(server) = self.server.take() {
            self.runtime
                .block_on(server.stop(Duration::from_secs(5)))
                .unwrap();
        }
    }
}

mod unary {
    use super::*;

    #[divan::bench(args = SIZES)]
    fn prost(bencher: Bencher, size: usize) {
        let fixture = Fixture::start(|server| proto::register_echo(server, ProstEcho));
        let client = proto::EchoClient::new(fixture.channel.clone());
        let request = prost_request(size);
        fixture.bench(bencher, || async {
            assert_eq!(client.unary(&request).await.unwrap().message.len(), size);
        });
    }

    #[divan::bench(args = SIZES)]
    fn buffa(bencher: Bencher, size: usize) {
        let fixture = Fixture::start(|server| pb::register_echo(server, BuffaEcho));
        let client = pb::EchoClient::new(fixture.channel.clone());
        let request = buffa_request(size);
        fixture.bench(bencher, || async {
            assert_eq!(client.unary(&request).await.unwrap().message.len(), size);
        });
    }

    #[divan::bench(args = SIZES)]
    fn buffa_view(bencher: Bencher, size: usize) {
        let fixture = Fixture::start(|server| pb::views::register_echo(server, ViewEcho));
        let client = pb::views::EchoClient::new(fixture.channel.clone());
        let request = buffa_request(size);
        fixture.bench(bencher, || async {
            assert_eq!(client.unary(&request).await.unwrap().message().len(), size);
        });
    }
}
