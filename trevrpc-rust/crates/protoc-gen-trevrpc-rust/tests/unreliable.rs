//! The `(trevrpc.method)` option: how the generator reads it from descriptors, emits it, and
//! rejects it on methods whose direction does not stream.

// Helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(clippy::expect_used)]

use std::path::Path;

use prost::Message;
use prost_types::compiler::CodeGeneratorResponse;
use protox::Compiler;
use protox::file::{
    ChainFileResolver, File, FileResolver, GoogleFileResolver, IncludeFileResolver,
};

/// Serves one in-memory proto file, `test.proto`.
struct Source(String);

impl FileResolver for Source {
    fn open_file(&self, name: &str) -> Result<File, protox::Error> {
        if name == "test.proto" {
            File::from_source(name, &self.0)
        } else {
            Err(protox::Error::file_not_found(name))
        }
    }
}

/// Compiles `source` with `trevrpc/options.proto` and the well-known types importable, and runs
/// the plugin on the result, as `protoc` would hand it over.
fn generate(source: &str) -> CodeGeneratorResponse {
    let proto_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../proto");
    let mut resolver = ChainFileResolver::new();
    resolver.add(Source(source.to_owned()));
    resolver.add(IncludeFileResolver::new(proto_dir));
    resolver.add(GoogleFileResolver::new());
    let mut compiler = Compiler::with_file_resolver(resolver);
    compiler.include_imports(true);
    compiler
        .open_file("test.proto")
        .expect("proto should compile");

    let request = protoc_gen_trevrpc_rust::code_generator_request(
        &compiler.encode_file_descriptor_set(),
        &["test.proto"],
        "runtime_path=::trevrpc",
    )
    .expect("descriptor set should decode");
    let mut output = Vec::new();
    protoc_gen_trevrpc_rust::run_plugin(request.as_slice(), &mut output)
        .expect("plugin should run");
    CodeGeneratorResponse::decode(output.as_slice()).expect("response should decode")
}

fn generated(source: &str) -> String {
    let response = generate(source);
    assert_eq!(response.error, None);
    response
        .file
        .into_iter()
        .next()
        .and_then(|file| file.content)
        .expect("plugin should generate content")
}

fn service(methods: &str) -> String {
    format!(
        r#"syntax = "proto3";
package demo.v1;
import "trevrpc/options.proto";
message Position {{ int32 x = 1; }}
service Telemetry {{
{methods}
}}
"#
    )
}

#[test]
fn the_option_is_read_from_descriptors_and_emitted_for_clients_and_servers() {
    let content = generated(&service(
        r"
        rpc Track(stream Position) returns (stream Position) {
            option (trevrpc.method) = { unreliable_requests: true unreliable_responses: true };
        }
        rpc Upload(stream Position) returns (Position) {
            option (trevrpc.method).unreliable_requests = true;
        }
        rpc Watch(Position) returns (stream Position) {
            option (trevrpc.method) = { unreliable_responses: true };
        }
        rpc Reliable(stream Position) returns (stream Position);
        ",
    ));

    // Clients apply the marks on top of the caller's options.
    assert!(content.contains(
        "bidirectional_streaming(&self.transport, telemetry_paths::TRACK, options.with_unreliable(::trevrpc::Unreliable::BOTH)).await"
    ), "{content}");
    assert!(content.contains(
        "bidirectional_streaming_from_stream(&self.transport, telemetry_paths::TRACK, requests, options.with_unreliable(::trevrpc::Unreliable::BOTH)).await"
    ));
    assert!(content.contains(
        "client_streaming(&self.transport, telemetry_paths::UPLOAD, options.with_unreliable(::trevrpc::Unreliable::REQUESTS)).await"
    ));
    assert!(content.contains(
        "server_streaming(&self.transport, telemetry_paths::WATCH, &request, options.with_unreliable(::trevrpc::Unreliable::RESPONSES)).await"
    ));
    assert!(content.contains(
        "bidirectional_streaming(&self.transport, telemetry_paths::RELIABLE, options).await"
    ));

    // Servers register the marked routes with route options.
    assert!(content.contains(
        "server.route_streaming_envelope_with_options(telemetry_paths::TRACK, ::trevrpc::RpcKind::BidirectionalStreaming, ::trevrpc::server::RouteOptions::new().with_unreliable(::trevrpc::Unreliable::BOTH), move |context,"
    ));
    assert!(content.contains(
        "server.route_streaming_envelope_with_options(telemetry_paths::UPLOAD, ::trevrpc::RpcKind::ClientStreaming, ::trevrpc::server::RouteOptions::new().with_unreliable(::trevrpc::Unreliable::REQUESTS), move |context,"
    ));
    assert!(content.contains(
        "server.route_streaming_envelope_with_context(telemetry_paths::RELIABLE, ::trevrpc::RpcKind::BidirectionalStreaming, move |context,"
    ));
}

#[test]
fn methods_without_the_option_generate_exactly_what_they_did_before() {
    let marked = generated(&service(
        "rpc Track(stream Position) returns (stream Position) { option (trevrpc.method) = { unreliable_requests: true }; }",
    ));
    let plain = generated(&service(
        "rpc Track(stream Position) returns (stream Position);",
    ));

    assert!(marked.contains("with_unreliable"));
    assert!(!plain.contains("with_unreliable"));
    assert!(!plain.contains("Unreliable"));
    assert!(!plain.contains("RouteOptions"));
}

#[test]
fn requests_cannot_be_unreliable_on_methods_that_do_not_stream_them() {
    for method in [
        "rpc Call(Position) returns (Position) { option (trevrpc.method) = { unreliable_requests: true }; }",
        "rpc Watch(Position) returns (stream Position) { option (trevrpc.method) = { unreliable_requests: true }; }",
    ] {
        let response = generate(&service(method));
        let name = method.split(['(', ' ']).nth(1).expect("method has a name");
        assert_eq!(
            response.error.as_deref(),
            Some(
                format!(
                    "demo.v1.Telemetry.{name}: unreliable_requests requires a client-streaming or bidirectional method"
                )
                .as_str()
            )
        );
        assert!(response.file.is_empty());
    }
}

#[test]
fn responses_cannot_be_unreliable_on_methods_that_do_not_stream_them() {
    for method in [
        "rpc Call(Position) returns (Position) { option (trevrpc.method) = { unreliable_responses: true }; }",
        "rpc Upload(stream Position) returns (Position) { option (trevrpc.method) = { unreliable_responses: true }; }",
    ] {
        let response = generate(&service(method));
        let name = method.split(['(', ' ']).nth(1).expect("method has a name");
        assert_eq!(
            response.error.as_deref(),
            Some(
                format!(
                    "demo.v1.Telemetry.{name}: unreliable_responses requires a server-streaming or bidirectional method"
                )
                .as_str()
            )
        );
    }
}

#[test]
fn false_flags_are_not_marks() {
    let content = generated(&service(
        "rpc Call(Position) returns (Position) { option (trevrpc.method) = { unreliable_requests: false }; }",
    ));
    assert!(!content.contains("Unreliable"));
}
