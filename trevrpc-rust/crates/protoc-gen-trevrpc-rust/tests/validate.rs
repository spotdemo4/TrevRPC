//! The `validate=true` option: which methods check their requests against `buf.validate` rules,
//! and the descriptors the generated validator holds.

// Helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::arithmetic_side_effects
)]

use std::path::Path;

use prost::Message;
use prost_types::FileDescriptorSet;
use prost_types::compiler::CodeGeneratorResponse;

/// Generates `tests/proto/validated.proto` with `validate=true`, from the encoded descriptors
/// that keep its `buf.validate` options.
fn generate(parameter: &str) -> String {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut compiler = protox::Compiler::new([
        manifest_dir.join("tests/proto"),
        manifest_dir.join("../../../interop/proto"),
    ])
    .unwrap();
    compiler.include_imports(true);
    compiler.open_file("validated.proto").unwrap();
    let request = protoc_gen_trevrpc_rust::code_generator_request(
        &compiler.encode_file_descriptor_set(),
        &["validated.proto"],
        parameter,
    )
    .unwrap();
    let mut output = Vec::new();
    protoc_gen_trevrpc_rust::run_plugin(request.as_slice(), &mut output).unwrap();
    let response = CodeGeneratorResponse::decode(output.as_slice()).unwrap();
    assert_eq!(response.error, None);
    response.file[0].content.clone().unwrap()
}

/// The route registration of the method whose path constant is `path`, from its validator to the
/// end of its handler.
fn registration<'a>(content: &'a str, path: &str) -> &'a str {
    let route = content
        .find(&format!("_with_options(checked_paths::{path},"))
        .unwrap_or_else(|| panic!("no registration of {path}"));
    let start = content[..route].rfind("\n    {\n").unwrap();
    let end = content[start..].find("\n    }\n").unwrap();
    &content[start..start + end]
}

#[test]
fn validated_methods_check_requests_that_carry_rules() {
    let content = generate("validate=true");

    assert!(content.contains("static TREVRPC_VALIDATOR"));
    assert!(content.contains("let validator = &*TREVRPC_VALIDATOR;"));
    assert!(content.contains("validator.message(\"example.validated.Request\")"));
    assert!(content.contains("validator.message(\"example.validated.Wrapper\")"));
    assert!(registration(&content, "UNARY").contains("validate.validate(&body)?;"));
    assert!(registration(&content, "SERVER_STREAM").contains("validate.validate(&body)?;"));
    for path in ["CLIENT_STREAM", "BIDI"] {
        assert!(registration(&content, path).contains(
            "::trevrpc::stream::decode_validated::<Request, _>(request_stream, move |body| validate.validate(body))"
        ));
    }
    let unchecked = registration(&content, "UNCHECKED");
    assert!(
        !unchecked.contains("validator.message") && !unchecked.contains("validate.validate"),
        "{unchecked}"
    );
}

/// A route whose request type has no rules costs nothing with the option: its registration is
/// the same code as without it.
#[test]
fn routes_without_rules_are_generated_as_without_the_option() {
    let validated = generate("validate=true");
    let plain = generate("validate=false");
    assert_eq!(
        registration(&validated, "UNCHECKED"),
        registration(&plain, "UNCHECKED")
    );
}

#[test]
fn the_validator_holds_the_files_of_the_validated_types_after_their_imports() {
    let content = generate("validate=true");
    let start = content.find("from_descriptor_set(b\"").unwrap() + "from_descriptor_set(b\"".len();
    let end = start + content[start..].find("\")").unwrap();
    let set = FileDescriptorSet::decode(unescape(&content[start..end]).as_slice()).unwrap();
    let names: Vec<_> = set
        .file
        .iter()
        .filter_map(|file| file.name.as_deref())
        .collect();

    let position = |name| names.iter().position(|file| *file == name).unwrap();
    assert!(position("google/protobuf/descriptor.proto") < position("buf/validate/validate.proto"));
    assert!(position("buf/validate/validate.proto") < position("validated.proto"));
    assert_eq!(names.last(), Some(&"validated.proto"));
}

#[test]
fn without_the_option_nothing_is_validated() {
    for parameter in ["", "validate=false"] {
        let content = generate(parameter);
        assert!(!content.contains("trevrpc_protovalidate"), "{parameter}");
        assert!(!content.contains("decode_validated"), "{parameter}");
    }
}

/// Reverses the generator's byte string escaping: `\xNN`, and line breaks after a backslash.
fn unescape(literal: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut rest = literal.as_bytes();
    while let Some((&byte, tail)) = rest.split_first() {
        rest = tail;
        if byte != b'\\' {
            bytes.push(byte);
            continue;
        }
        match rest.split_first() {
            Some((b'x', tail)) => {
                let hex = std::str::from_utf8(&tail[..2]).unwrap();
                bytes.push(u8::from_str_radix(hex, 16).unwrap());
                rest = &tail[2..];
            }
            Some((b'\n', tail)) => {
                rest = tail.trim_ascii_start();
            }
            _ => panic!("unexpected escape"),
        }
    }
    bytes
}
