//! Messages and the `Echo`, `UnreliableEcho`, `CompressedEcho`, `RefusingEcho`, `ValidatedEcho`
//! and `ValidationBench` services generated from `interop/proto`.

#![allow(dead_code, clippy::pedantic)]

include!(concat!(env!("OUT_DIR"), "/trevrpc.interop.v1.rs"));
include!(concat!(env!("OUT_DIR"), "/trevrpc.interop.v1.trevrpc.rs"));

/// The `buf.validate` messages, for the `buf.validate.Violations` status detail.
#[allow(clippy::enum_variant_names)]
pub mod validate {
    include!(concat!(env!("OUT_DIR"), "/buf.validate.rs"));
}

/// The encoded descriptors of the interop protos and every file they import, from which the
/// validation tests and microbenchmarks build their validator.
pub const DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/interop.fds"));
