use std::env;
use std::error::Error;
use std::fs;
use std::path::PathBuf;

use prost::Message;
use prost_types::compiler::CodeGeneratorResponse;

/// The interop services, shared by every runtime. `interop.proto` has the reliable `Echo`,
/// `unreliable.proto` the `UnreliableEcho` service with datagram messages, and `compressed.proto`
/// the `CompressedEcho` service with stream compression.
const INTEROP_PROTO_DIR: &str = "../../../interop/proto";
const INTEROP_PROTOS: [&str; 3] = ["interop.proto", "unreliable.proto", "compressed.proto"];
/// The directory holding `trevrpc/options.proto`, which the other files import.
const TREVRPC_PROTO_DIR: &str = "../../../proto";
const OPTIONS_PROTO: &str = "trevrpc/options.proto";
const GENERATED_SERVICE: &str = "trevrpc.interop.v1.trevrpc.rs";

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir =
        PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").ok_or("missing manifest dir")?);
    let interop_dir = manifest_dir.join(INTEROP_PROTO_DIR);
    let trevrpc_dir = manifest_dir.join(TREVRPC_PROTO_DIR);
    for proto in INTEROP_PROTOS {
        println!(
            "cargo:rerun-if-changed={}",
            interop_dir.join(proto).display()
        );
    }
    println!(
        "cargo:rerun-if-changed={}",
        trevrpc_dir.join(OPTIONS_PROTO).display()
    );

    // The generator needs every imported file to resolve types.
    let mut compiler = protox::Compiler::new([&interop_dir, &trevrpc_dir])?;
    compiler.include_imports(true);
    compiler.open_files(INTEROP_PROTOS)?;

    prost_build::Config::new().compile_fds(compiler.file_descriptor_set())?;

    // The `(trevrpc.method)` options are extensions that `prost_types` drops, so the plugin gets
    // the encoded descriptors, which keep them.
    let request = protoc_gen_trevrpc_rust::code_generator_request(
        &compiler.encode_file_descriptor_set(),
        &INTEROP_PROTOS,
        "runtime_path=::trevrpc",
    )?;
    let mut output = Vec::new();
    protoc_gen_trevrpc_rust::run_plugin(request.as_slice(), &mut output)?;
    let response = CodeGeneratorResponse::decode(output.as_slice())?;
    if let Some(error) = response.error {
        return Err(error.into());
    }
    let generated = response
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(GENERATED_SERVICE))
        .and_then(|file| file.content)
        .ok_or("TrevRPC generator did not produce the interop services")?;
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
    fs::write(out_dir.join(GENERATED_SERVICE), generated)?;

    Ok(())
}
