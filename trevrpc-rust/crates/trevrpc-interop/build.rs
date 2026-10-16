use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use prost::Message;
use prost_types::compiler::{CodeGeneratorRequest, CodeGeneratorResponse};

/// The single source of truth for the interop service, shared by every runtime.
const PROTO_DIR: &str = "../../../interop/proto";
const PROTO_FILE: &str = "interop.proto";
const GENERATED_SERVICE: &str = "trevrpc.interop.v1.trevrpc.rs";

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("missing manifest dir")?);
    let proto_dir = manifest_dir.join(PROTO_DIR);
    println!(
        "cargo:rerun-if-changed={}",
        proto_dir.join(PROTO_FILE).display()
    );

    let descriptor = protox::compile([PROTO_FILE], [&proto_dir])?;
    prost_build::Config::new().compile_fds(descriptor.clone())?;

    let request = CodeGeneratorRequest {
        file_to_generate: vec![PROTO_FILE.to_owned()],
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
        .find(|file| file.name.as_deref() == Some(GENERATED_SERVICE))
        .and_then(|file| file.content)
        .ok_or("TrevRPC generator did not produce the interop service")?;
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
    fs::write(out_dir.join(GENERATED_SERVICE), generated)?;

    Ok(())
}
