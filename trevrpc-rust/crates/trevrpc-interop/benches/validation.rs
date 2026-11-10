//! Microbenchmarks of `trevrpc-protovalidate`'s check of the request the validation scenarios
//! send (see `bench/README.md`), against the rules of `StandardBenchRequest` and of
//! `CelBenchRequest`. A validated route decodes the request into its prost type, then validates
//! its encoding: `MessageValidator::validate` decodes it a second time, into a `DynamicMessage`,
//! and evaluates the rules on that. `decode` and `decode_dynamic` time the two decodings alone,
//! for scale.
//!
//! Run with `cargo bench -p trevrpc-interop --bench validation`; add `-- --test` to run each
//! benchmark once.

// Benchmark setup fails loudly: a panic here is the intended way to stop a broken benchmark.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use divan::{Bencher, black_box};
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage};
use trevrpc_protovalidate::Validator;

#[path = "../src/proto.rs"]
mod proto;
#[allow(dead_code, unreachable_pub)]
#[path = "../src/validation.rs"]
mod validation;

use proto::{BenchRequest, DESCRIPTOR_SET};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// Message sizes: the load benchmarks' small and large sizes.
const SIZES: [usize; 2] = [64, 4096];

fn main() {
    divan::main();
}

/// The encoding of the request with a message of `size` bytes.
fn encoded(size: usize) -> Vec<u8> {
    validation::bench_request("x".repeat(size)).encode_to_vec()
}

/// Times the check of the request against the rules of the message type `full_name`.
fn validate(bencher: Bencher, full_name: &'static str, size: usize) {
    let validate = Validator::from_descriptor_set(DESCRIPTOR_SET).message(full_name);
    let encoded = encoded(size);
    validate.validate(&encoded).unwrap();
    bencher.bench_local(|| validate.validate(black_box(&encoded)));
}

#[divan::bench(args = SIZES)]
fn validate_standard(bencher: Bencher, size: usize) {
    validate(bencher, "trevrpc.interop.v1.StandardBenchRequest", size);
}

#[divan::bench(args = SIZES)]
fn validate_cel(bencher: Bencher, size: usize) {
    validate(bencher, "trevrpc.interop.v1.CelBenchRequest", size);
}

#[divan::bench(args = SIZES)]
fn decode(bencher: Bencher, size: usize) {
    let encoded = encoded(size);
    bencher.bench_local(|| BenchRequest::decode(black_box(encoded.as_slice())).unwrap());
}

#[divan::bench(args = SIZES)]
fn decode_dynamic(bencher: Bencher, size: usize) {
    let descriptor = DescriptorPool::decode(DESCRIPTOR_SET)
        .unwrap()
        .get_message_by_name("trevrpc.interop.v1.StandardBenchRequest")
        .unwrap();
    let encoded = encoded(size);
    bencher.bench_local(|| {
        DynamicMessage::decode(descriptor.clone(), black_box(encoded.as_slice())).unwrap()
    });
}
