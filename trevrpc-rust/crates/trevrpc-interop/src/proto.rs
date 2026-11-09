//! Messages and the `Echo`, `UnreliableEcho`, `CompressedEcho`, `RefusingEcho` and
//! `ValidatedEcho` services generated from `interop/proto`.

#![allow(dead_code, clippy::pedantic)]

include!(concat!(env!("OUT_DIR"), "/trevrpc.interop.v1.rs"));
include!(concat!(env!("OUT_DIR"), "/trevrpc.interop.v1.trevrpc.rs"));

/// The `buf.validate` messages, for the `buf.validate.Violations` status detail.
#[allow(clippy::enum_variant_names)]
pub mod validate {
    include!(concat!(env!("OUT_DIR"), "/buf.validate.rs"));
}
