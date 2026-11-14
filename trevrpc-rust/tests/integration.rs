//! The integration tests of the trevrpc crate, built as one test binary: each file is a module, so
//! the tests link once and run in one process instead of one binary per file.

#[allow(dead_code)]
#[path = "../examples/shared/greeter.rs"]
mod greeter;
#[macro_use]
mod support;

mod acme;
mod call_termination;
mod channel;
mod compression;
mod datagrams;
mod quinn_integration;
mod rate_limit;
mod remote_addr;
mod tcp_integration;
