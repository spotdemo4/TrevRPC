//! The validator of the tests' messages, shared by the test binaries.

use std::path::Path;

use trevrpc_protovalidate::Validator;

/// A validator for `tests/proto/rules.proto` and `tests/proto/unpackaged.proto`.
#[allow(clippy::unwrap_used)] // A test helper fails by panicking, like the tests themselves.
pub fn validator() -> Validator {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut compiler = protox::Compiler::new([
        manifest_dir.join("tests/proto"),
        manifest_dir.join("../../../interop/proto"),
    ])
    .unwrap();
    compiler.include_imports(true);
    compiler.open_file("rules.proto").unwrap();
    compiler.open_file("unpackaged.proto").unwrap();
    Validator::from_descriptor_set(&compiler.encode_file_descriptor_set())
}
