use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use prost::Message;
use prost_types::compiler::{CodeGeneratorRequest, CodeGeneratorResponse};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("missing manifest dir")?);
    let canonical_proto = manifest_dir.join("../../../bench/proto/benchmark.proto");
    let packaged_proto = manifest_dir.join("proto/benchmark.proto");
    println!("cargo:rerun-if-changed={}", canonical_proto.display());
    println!("cargo:rerun-if-changed={}", packaged_proto.display());

    let proto = if canonical_proto.exists() {
        if fs::read(&canonical_proto)? != fs::read(&packaged_proto)? {
            return Err(
                "packaged benchmark.proto is out of sync with bench/proto/benchmark.proto".into(),
            );
        }
        canonical_proto
    } else {
        packaged_proto
    };
    let proto_dir = proto.parent().ok_or("benchmark.proto has no parent")?;

    let descriptor = protox::compile(["benchmark.proto"], [proto_dir])?;
    prost_build::Config::new().compile_fds(descriptor.clone())?;

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);

    let request = CodeGeneratorRequest {
        file_to_generate: vec!["benchmark.proto".to_owned()],
        parameter: Some("runtime_path=::trevrpc".to_owned()),
        proto_file: descriptor.file,
        compiler_version: None,
    };
    let mut input = Vec::new();
    request.encode(&mut input)?;
    let mut output = Vec::new();
    protoc_gen_trevrpc_rust::run_plugin(input.as_slice(), &mut output)?;
    let response = CodeGeneratorResponse::decode(output.as_slice())?;
    if let Some(error) = response.error {
        return Err(error.into());
    }
    let generated = response
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some("trevrpc.benchmark.v1.trevrpc.rs"))
        .and_then(|file| file.content)
        .ok_or("TrevRPC generator did not produce the benchmark service")?;
    fs::write(out_dir.join("trevrpc.benchmark.v1.trevrpc.rs"), generated)?;

    Ok(())
}
