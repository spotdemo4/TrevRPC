//! Loopback benchmarks of the four call shapes over native QUIC and `QMux` over TLS.
//!
//! Each benchmark builds its own tokio runtime, server and connection outside the timed region,
//! then times `block_on` of one operation: a unary round trip, one server-streaming call, one
//! client-streaming call (both of [`support::MESSAGES`] messages), or one ping-pong round trip on
//! a bidirectional stream that stays open across iterations. The `raw` service isolates runtime
//! overhead; the `greeter` service adds the generated protobuf codec and dispatch.
//!
//! Run with `cargo bench --bench loopback`; add `-- --test` to run each benchmark once.

// Benchmark setup fails loudly: a panic here is the intended way to stop a broken benchmark.
// The generated per-transport modules import the shared helpers wholesale.
#![allow(
    clippy::wildcard_imports,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::as_conversions
)]

mod support;

use divan::Bencher;
use divan::counter::{BytesCount, ItemsCount};
use support::{Client, Fixture, Link, MESSAGES, Transport, Variant};
use tokio::runtime::Runtime;
use trevrpc::client::StreamingRpcTransport;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// Payload sizes: a small message, a typical one, and one that spans many QUIC packets.
const SIZES: [usize; 3] = [16, 1024, 64 * 1024];

fn main() {
    divan::main();
}

#[derive(Clone, Copy)]
enum Shape {
    Unary,
    ServerStream,
    ClientStream,
    Bidi,
}

/// Starts a fixture and times `shape` over it.
fn run(bencher: Bencher, transport: Transport, variant: Variant, shape: Shape, size: usize) {
    let fixture = Fixture::start(transport, variant);
    match &fixture.link {
        Link::Quic {
            transport: quic, ..
        } => drive(
            bencher,
            &fixture.runtime,
            &Client::new(quic.clone(), variant, size),
            shape,
            size,
        ),
        Link::Qmux(qmux) => drive(
            bencher,
            &fixture.runtime,
            &Client::new(qmux.clone(), variant, size),
            shape,
            size,
        ),
    }
}

fn drive<T>(bencher: Bencher, runtime: &Runtime, client: &Client<T>, shape: Shape, size: usize)
where
    T: StreamingRpcTransport,
{
    // A streaming call moves `MESSAGES` messages; the counters report per-message throughput.
    match shape {
        Shape::Unary => bencher
            .counter(ItemsCount::new(1_usize))
            .counter(BytesCount::new(size))
            .bench_local(|| runtime.block_on(client.unary())),
        Shape::ServerStream => bencher
            .counter(ItemsCount::new(MESSAGES))
            .counter(BytesCount::new(size * MESSAGES))
            .bench_local(|| runtime.block_on(client.server_stream())),
        Shape::ClientStream => bencher
            .counter(ItemsCount::new(MESSAGES))
            .counter(BytesCount::new(size * MESSAGES))
            .bench_local(|| runtime.block_on(client.client_stream())),
        Shape::Bidi => {
            let mut stream = runtime.block_on(client.open_bidi());
            bencher
                .counter(ItemsCount::new(1_usize))
                .counter(BytesCount::new(size))
                .bench_local(|| runtime.block_on(stream.round_trip()));
        }
    }
}

/// Registers the four call shapes for one transport and service.
macro_rules! shapes {
    ($transport:expr, $variant:expr) => {
        #[divan::bench(args = SIZES)]
        fn unary(bencher: Bencher, size: usize) {
            run(bencher, $transport, $variant, Shape::Unary, size);
        }

        #[divan::bench(args = SIZES)]
        fn server_stream(bencher: Bencher, size: usize) {
            run(bencher, $transport, $variant, Shape::ServerStream, size);
        }

        #[divan::bench(args = SIZES)]
        fn client_stream(bencher: Bencher, size: usize) {
            run(bencher, $transport, $variant, Shape::ClientStream, size);
        }

        #[divan::bench(args = SIZES)]
        fn bidi(bencher: Bencher, size: usize) {
            run(bencher, $transport, $variant, Shape::Bidi, size);
        }
    };
}

mod quic {
    use super::*;

    mod raw {
        use super::*;
        shapes!(Transport::Quic, Variant::Raw);
    }

    mod greeter {
        use super::*;
        shapes!(Transport::Quic, Variant::Greeter);
    }
}

mod qmux {
    use super::*;

    mod raw {
        use super::*;
        shapes!(Transport::Qmux, Variant::Raw);
    }

    mod greeter {
        use super::*;
        shapes!(Transport::Qmux, Variant::Greeter);
    }
}
