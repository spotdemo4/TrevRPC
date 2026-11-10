//! Checking encoded requests against `buf.validate` rules, standard and CEL, and failing at
//! registration on rules that cannot be enforced.

// Helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

use std::path::Path;

use prost::Message;
use prost_protovalidate::types::Violations;
use trevrpc::Code;
use trevrpc_protovalidate::{MAX_DETAIL_VIOLATIONS, VIOLATIONS_TYPE, Validator};

#[derive(Clone, PartialEq, prost::Message)]
struct Request {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(message, optional, tag = "2")]
    inner: Option<Inner>,
}

#[derive(Clone, PartialEq, prost::Message)]
struct Inner {
    #[prost(string, tag = "1")]
    label: String,
}

fn validator() -> Validator {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut compiler = protox::Compiler::new([
        manifest_dir.join("tests/proto"),
        manifest_dir.join("../../../interop/proto"),
    ])
    .unwrap();
    compiler.include_imports(true);
    compiler.open_file("rules.proto").unwrap();
    Validator::from_descriptor_set(&compiler.encode_file_descriptor_set())
}

fn request(name: &str, label: &str) -> Vec<u8> {
    Request {
        name: name.to_owned(),
        inner: Some(Inner {
            label: label.to_owned(),
        }),
    }
    .encode_to_vec()
}

#[test]
fn valid_requests_pass() {
    let validate = validator().message("test.rules.Request");
    assert_eq!(validate.validate(&request("alice", "a")), Ok(()));
}

#[test]
fn broken_rules_fail_with_violations() {
    let validate = validator().message("test.rules.Request");

    let status = validate
        .validate(&request("al", "label"))
        .expect_err("both standard rules are broken");
    assert_eq!(status.code(), Code::InvalidArgument);
    let detail = status
        .find_detail(VIOLATIONS_TYPE)
        .expect("violations detail");
    let violations = detail.decode::<Violations>().unwrap();
    let mut rules: Vec<_> = violations
        .violations
        .iter()
        .map(|violation| violation.rule_id().to_owned())
        .collect();
    rules.sort();
    assert_eq!(rules, ["string.max_len", "string.min_len"]);
}

#[test]
fn cel_rules_are_enforced() {
    let validate = validator().message("test.rules.Request");

    let status = validate
        .validate(&request("same", "same"))
        .expect_err("the message rule is broken");
    let violations = status
        .find_detail(VIOLATIONS_TYPE)
        .expect("violations detail")
        .decode::<Violations>()
        .unwrap();
    assert_eq!(violations.violations.len(), 1);
    assert_eq!(violations.violations[0].rule_id(), "name_not_inner");
}

#[test]
#[should_panic(expected = "cannot validate test.rules.BadRule")]
fn rules_that_cannot_compile_fail_at_registration() {
    let _ = validator().message("test.rules.BadRule");
}

#[test]
#[should_panic(expected = "cannot validate test.rules.Missing")]
fn unknown_types_fail_at_registration() {
    let _ = validator().message("test.rules.Missing");
}

#[derive(Clone, PartialEq, prost::Message)]
struct Items {
    #[prost(string, repeated, tag = "1")]
    items: Vec<String>,
}

#[test]
fn many_violations_fail_with_a_status_that_fits_a_frame() {
    let validate = validator().message("test.rules.Items");
    let items = Items {
        items: vec![String::new(); 5000],
    }
    .encode_to_vec();

    let status = validate
        .validate(&items)
        .expect_err("every item breaks its rule");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.message().len() < 200, "{}", status.message());
    assert!(
        status.message().ends_with("(and 4999 more)"),
        "{}",
        status.message()
    );
    let violations = status
        .find_detail(VIOLATIONS_TYPE)
        .expect("violations detail")
        .decode::<Violations>()
        .unwrap();
    assert_eq!(violations.violations.len(), MAX_DETAIL_VIOLATIONS);
}

#[test]
fn validate_message_checks_a_message_as_the_server_does() {
    let validate = validator().message("test.rules.Request");
    let valid = Request {
        name: "alice".to_owned(),
        inner: Some(Inner {
            label: "a".to_owned(),
        }),
    };
    assert_eq!(validate.validate_message(&valid), Ok(()));

    let status = validate
        .validate_message(&Request {
            name: "al".to_owned(),
            ..valid
        })
        .expect_err("the name is too short");
    assert_eq!(status.code(), Code::InvalidArgument);
    assert!(status.find_detail(VIOLATIONS_TYPE).is_some());
}

#[test]
fn request_checks_fail_with_the_status_the_server_sends() {
    let validate = validator().message("test.rules.Request");
    let check = validate.request_check();

    check.check(&request("alice", "a")).unwrap();
    let error = check
        .check(&request("al", "label"))
        .expect_err("both standard rules are broken");
    assert_eq!(
        error.into_status(),
        validate.validate(&request("al", "label")).unwrap_err()
    );
}
