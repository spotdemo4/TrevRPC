//! Microbenchmarks of frame encoding, payload decoding and varints through the public API.
//!
//! Run with `cargo bench --bench frames`; add `-- --test` to run each benchmark once.

// Benchmark setup fails loudly: a panic here is the intended way to stop a broken benchmark.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::as_conversions
)]

use divan::counter::{BytesCount, ItemsCount};
use divan::{Bencher, black_box};
use trevrpc::framing::{self, DEFAULT_MAX_FRAME_SIZE, frame_type};
use trevrpc::varint;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// Payload sizes: a small message, a typical one, and one that spans many QUIC packets.
const SIZES: [usize; 3] = [16, 1024, 64 * 1024];

/// A protobuf message whose encoded size tracks its payload.
#[derive(Clone, PartialEq, prost::Message)]
struct Payload {
    #[prost(bytes = "vec", tag = "1")]
    data: Vec<u8>,
}

impl Payload {
    fn of_size(size: usize) -> Self {
        Self {
            data: vec![0xa5; size],
        }
    }
}

fn main() {
    divan::main();
}

#[divan::bench(args = SIZES)]
fn encode_message_frame(bencher: Bencher, size: usize) {
    let body = vec![0xa5_u8; size];
    bencher
        .counter(BytesCount::new(size))
        .bench(|| framing::encode_message_frame(black_box(&body), DEFAULT_MAX_FRAME_SIZE));
}

#[divan::bench(args = SIZES)]
fn encode_protobuf_frame(bencher: Bencher, size: usize) {
    let message = Payload::of_size(size);
    bencher.counter(BytesCount::new(size)).bench(|| {
        framing::encode_protobuf_frame(
            frame_type::MESSAGE,
            black_box(&message),
            DEFAULT_MAX_FRAME_SIZE,
        )
    });
}

#[divan::bench(args = SIZES)]
fn decode_payload(bencher: Bencher, size: usize) {
    let payload = prost::Message::encode_to_vec(&Payload::of_size(size));
    bencher
        .counter(BytesCount::new(size))
        .bench(|| framing::decode_payload::<Payload>(black_box(&payload)));
}

/// Values that encode in one, two, four and eight bytes.
const VARINTS: [u64; 4] = [16, 1024, 64 * 1024, 1 << 40];

#[divan::bench(args = VARINTS)]
fn varint_push(bencher: Bencher, value: u64) {
    let mut out = Vec::with_capacity(varint::MAX_LEN);
    bencher.counter(ItemsCount::new(1_usize)).bench_local(|| {
        out.clear();
        varint::push(&mut out, black_box(value))
    });
}

#[divan::bench(args = VARINTS)]
fn varint_decode(bencher: Bencher, value: u64) {
    let mut encoded = Vec::new();
    varint::push(&mut encoded, value).unwrap();
    bencher
        .counter(ItemsCount::new(1_usize))
        .bench(|| varint::decode(black_box(&encoded)));
}
