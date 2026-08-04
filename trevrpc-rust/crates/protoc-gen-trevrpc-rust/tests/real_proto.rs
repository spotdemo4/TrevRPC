use std::error::Error;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use prost::Message;
use prost_types::compiler::{CodeGeneratorRequest, CodeGeneratorResponse};

#[test]
fn generates_services_from_a_real_proto_descriptor() -> Result<(), Box<dyn Error>> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let proto_dir = manifest_dir.join("tests/proto");
    let descriptor_set = protox::compile(["greeter.proto"], [&proto_dir])?;
    let request = CodeGeneratorRequest {
        file_to_generate: vec!["greeter.proto".to_owned()],
        parameter: Some("runtime_path=::trevrpc".to_owned()),
        proto_file: descriptor_set.file,
        compiler_version: None,
    };
    let mut input = Vec::new();
    request.encode(&mut input)?;
    let mut output = Vec::new();

    protoc_gen_trevrpc_rust::run_plugin(input.as_slice(), &mut output)?;

    let response = CodeGeneratorResponse::decode(output.as_slice())?;
    assert_eq!(response.error, None);
    assert_eq!(response.file.len(), 1);
    assert_eq!(
        response.file[0].name.as_deref(),
        Some("example.greeter.trevrpc.rs")
    );

    let content = response.file[0]
        .content
        .as_deref()
        .expect("plugin should generate content");
    assert!(content.contains("pub trait Greeter"));
    assert!(content.contains("pub struct GreeterClient<T>"));
    assert!(content.contains("async fn say_hello"));
    assert!(content.contains("async fn lots_of_replies"));
    assert!(content.contains("async fn lots_of_greetings"));
    assert!(content.contains("async fn bidi_hello"));
    assert!(content.contains("options: ::trevrpc::client::CallOptions"));
    assert!(content.contains("::trevrpc::client::server_streaming"));
    assert!(content.contains("::trevrpc::client::client_streaming"));
    assert!(content.contains("::trevrpc::client::client_streaming_from_stream"));
    assert!(content.contains("::trevrpc::client::bidirectional_streaming"));
    assert!(content.contains("::trevrpc::client::bidirectional_streaming_from_stream"));
    assert!(content.contains("server.route_streaming"));
    assert!(content.contains("::trevrpc::RpcKind::ServerStreaming"));
    assert!(content.contains("::trevrpc::RpcKind::ClientStreaming"));
    assert!(content.contains("::trevrpc::RpcKind::BidirectionalStreaming"));

    let buf_config = include_str!("proto/buf.gen.yaml");
    assert!(buf_config.contains("protoc-gen-trevrpc-rust"));

    let checked_in =
        std::fs::read_to_string(manifest_dir.join("../../examples/shared/greeter.rs"))?;
    let formatted = rustfmt_generated(content)?;
    assert!(
        checked_in.ends_with(&formatted),
        "checked-in greeter bindings have drifted from generator output"
    );

    compile_generated_service(content)?;

    Ok(())
}

fn rustfmt_generated(content: &str) -> Result<String, Box<dyn Error>> {
    let rustfmt = std::env::var_os("RUSTFMT").unwrap_or_else(|| "rustfmt".into());
    let mut child = Command::new(rustfmt)
        .args(["--edition", "2024", "--emit", "stdout"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    child
        .stdin
        .take()
        .expect("rustfmt stdin should be piped")
        .write_all(content.as_bytes())?;
    let output = child.wait_with_output()?;
    assert!(
        output.status.success(),
        "rustfmt should format generated code"
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[allow(clippy::too_many_lines)]
fn compile_generated_service(content: &str) -> Result<(), Box<dyn Error>> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir.join("../..").canonicalize()?;
    let temp_dir =
        std::env::temp_dir().join(format!("trevrpc-generated-compile-{}", std::process::id()));

    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }

    std::fs::create_dir_all(temp_dir.join("src"))?;
    std::fs::write(temp_dir.join("src/generated.rs"), content)?;
    std::fs::write(
        temp_dir.join("Cargo.toml"),
        format!(
            r#"[package]
name = "trevrpc-generated-compile"
version = "0.1.0"
edition = "2024"

[dependencies]
futures-util = "0.3"
prost = "0.14.4"
trevrpc = {{ path = "{}" }}
"#,
            workspace_root.display()
        ),
    )?;
    std::fs::write(
        temp_dir.join("src/lib.rs"),
        r#"use futures_util::StreamExt;

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: String,
}

include!("generated.rs");

struct Service;

#[trevrpc::async_trait]
impl Greeter for Service {
    async fn say_hello(
        &self,
        _context: trevrpc::server::RequestContext,
        request: HelloRequest,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<HelloReply>, trevrpc::Status> {
        Ok(trevrpc::ResponseEnvelope::new(HelloReply { message: request.name }))
    }

    async fn lots_of_replies(
        &self,
        _context: trevrpc::server::RequestContext,
        request: HelloRequest,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<HelloReply>>, trevrpc::Status> {
        Ok(trevrpc::ResponseEnvelope::new(trevrpc::stream::from_iter([HelloReply {
            message: request.name,
        }])))
    }

    async fn lots_of_greetings(
        &self,
        _context: trevrpc::server::RequestContext,
        mut requests: trevrpc::BoxStream<HelloRequest>,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<HelloReply>, trevrpc::Status> {
        let mut names = Vec::new();

        while let Some(request) = requests.next().await {
            names.push(request?.name);
        }

        Ok(trevrpc::ResponseEnvelope::new(HelloReply {
            message: names.join(","),
        }))
    }

    async fn bidi_hello(
        &self,
        _context: trevrpc::server::RequestContext,
        requests: trevrpc::BoxStream<HelloRequest>,
    ) -> core::result::Result<trevrpc::ResponseEnvelope<trevrpc::BoxStream<HelloReply>>, trevrpc::Status> {
        Ok(trevrpc::ResponseEnvelope::new(Box::pin(requests.map(|request| {
            request.map(|request| HelloReply {
                message: request.name,
            })
        }))))
    }
}

pub fn register(server: &mut trevrpc::server::Server) {
    register_greeter(server, Service);
}
"#,
    )?;

    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let status = Command::new(cargo)
        .arg("check")
        .arg("--quiet")
        .arg("--offline")
        .current_dir(&temp_dir)
        .env("CARGO_TARGET_DIR", temp_dir.join("target"))
        .status()?;

    assert!(status.success(), "generated service crate should compile");
    std::fs::remove_dir_all(temp_dir)?;

    Ok(())
}
