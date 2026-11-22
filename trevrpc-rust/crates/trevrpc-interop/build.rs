use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;
use prost_types::compiler::CodeGeneratorResponse;

/// The interop services, shared by every runtime. `interop.proto` has the reliable `Echo`,
/// `unreliable.proto` the `UnreliableEcho` service with datagram messages, `compressed.proto`
/// the `CompressedEcho` service with stream compression and compression without a dictionary,
/// `validated.proto` the `ValidatedEcho` service, whose requests carry `buf.validate` rules
/// from `buf/validate/validate.proto`, vendored in the same directory, and
/// `validation_bench.proto` the `ValidationBench` service of the validation benchmarks.
const INTEROP_PROTO_DIR: &str = "../../../interop/proto";
const INTEROP_PROTOS: [&str; 5] = [
    "interop.proto",
    "unreliable.proto",
    "compressed.proto",
    "validated.proto",
    "validation_bench.proto",
];
/// The directory holding `trevrpc/options.proto`, which the other files import.
const TREVRPC_PROTO_DIR: &str = "../../../proto";
const OPTIONS_PROTO: &str = "trevrpc/options.proto";
const GENERATED_SERVICE: &str = "trevrpc.interop.v1.trevrpc.rs";
/// The `Echo` service again, carrying the `buffa` messages of `interop.proto`, which the buffa
/// codec tests serve and call.
const BUFFA_PROTO: &str = "interop.proto";
const BUFFA_SERVICE_SUFFIX: &str = ".buffa.trevrpc.rs";
/// The `Echo` service of the buffa messages once more, decoding them as zero-copy views.
const BUFFA_VIEWS_SERVICE_SUFFIX: &str = ".buffa-views.trevrpc.rs";
/// The directory under `OUT_DIR` of the `buffa` messages, one file per package.
const BUFFA_DIR: &str = "buffa";
/// The encoded descriptors of the interop protos and their imports, from which the validation
/// microbenchmarks build their validator.
const DESCRIPTOR_SET: &str = "interop.fds";

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
    let descriptor_set = compiler.encode_file_descriptor_set();
    let request = protoc_gen_trevrpc_rust::code_generator_request(
        &descriptor_set,
        &INTEROP_PROTOS,
        "runtime_path=::trevrpc,validate=true",
    )?;
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").ok_or("missing OUT_DIR")?);
    fs::write(
        out_dir.join(GENERATED_SERVICE),
        generate_services(&request, GENERATED_SERVICE)?,
    )?;

    for (suffix, views) in [
        (BUFFA_SERVICE_SUFFIX, false),
        (BUFFA_VIEWS_SERVICE_SUFFIX, true),
    ] {
        let name = format!("trevrpc.interop.v1{suffix}");
        let request = protoc_gen_trevrpc_rust::code_generator_request(
            &descriptor_set,
            &[BUFFA_PROTO],
            &format!("runtime_path=::trevrpc,codec=buffa,views={views},file_suffix={suffix}"),
        )?;
        fs::write(out_dir.join(&name), generate_services(&request, &name)?)?;
    }
    generate_buffa_messages(&descriptor_set, &out_dir.join(BUFFA_DIR))?;

    fs::write(out_dir.join(DESCRIPTOR_SET), descriptor_set)?;

    Ok(())
}

/// Runs the `TrevRPC` generator on the encoded `request` and returns the file named `name`.
fn generate_services(request: &[u8], name: &str) -> Result<String, Box<dyn Error>> {
    let mut output = Vec::new();
    protoc_gen_trevrpc_rust::run_plugin(request, &mut output)?;
    let response = CodeGeneratorResponse::decode(output.as_slice())?;
    if let Some(error) = response.error {
        return Err(error.into());
    }
    response
        .file
        .into_iter()
        .find(|file| file.name.as_deref() == Some(name))
        .and_then(|file| file.content)
        .ok_or_else(|| format!("TrevRPC generator did not produce {name}").into())
}

/// Generates the `buffa` messages of `interop.proto` into `dir`, one file per package.
fn generate_buffa_messages(descriptor_set: &[u8], dir: &Path) -> Result<(), Box<dyn Error>> {
    use buffa::Message as _;

    let set = buffa_descriptor::generated::descriptor::FileDescriptorSet::decode_from_slice(
        descriptor_set,
    )?;
    let mut config = buffa_codegen::CodeGenConfig::default();
    config.file_per_package = true;
    fs::create_dir_all(dir)?;
    for file in buffa_codegen::generate(&set.file, &[BUFFA_PROTO.to_owned()], &config)? {
        fs::write(dir.join(file.name), file.content)?;
    }
    Ok(())
}
