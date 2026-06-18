use std::error::Error;
use std::path::Path;

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

    protoc_gen_trevrpc::run_plugin(input.as_slice(), &mut output)?;

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
    assert!(content.contains("options: ::trevrpc::client::CallOptions"));

    let buf_config = include_str!("proto/buf.gen.yaml");
    assert!(buf_config.contains("protoc-gen-trevrpc"));

    Ok(())
}
