#![allow(dead_code)]

include!(concat!(env!("OUT_DIR"), "/trevrpc.benchmark.v1.rs"));
include!(concat!(env!("OUT_DIR"), "/trevrpc.benchmark.v1.trevrpc.rs"));

#[allow(
    clippy::default_trait_access,
    clippy::doc_markdown,
    clippy::too_many_lines
)]
pub(crate) mod grpc {
    include!(concat!(env!("OUT_DIR"), "/tonic/trevrpc.benchmark.v1.rs"));
}
