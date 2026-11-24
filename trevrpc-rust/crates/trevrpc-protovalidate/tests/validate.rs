//! Checking encoded requests against `buf.validate` rules, standard and CEL, and failing at
//! registration on rules that cannot be enforced.

// Helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::indexing_slicing
)]

mod common;

use common::validator;
use prost::Message;
use prost_protovalidate::types::Violations;
use trevrpc::Code;
use trevrpc_protovalidate::{MAX_DETAIL_VIOLATIONS, VIOLATIONS_TYPE};

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
    let violations = detail.decode::<Violations>(&trevrpc::codec::Prost).unwrap();
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
        .decode::<Violations>(&trevrpc::codec::Prost)
        .unwrap();
    assert_eq!(violations.violations.len(), 1);
    assert_eq!(violations.violations[0].rule_id(), "name_not_inner");
}

#[test]
#[should_panic(expected = "cannot validate test.rules.BadRule")]
fn rules_that_cannot_compile_fail_at_registration() {
    let _ = validator().message("test.rules.BadRule");
}

/// prost-protovalidate reports a rule that names a field the message lacks only when it evaluates
/// it, so it registers and fails every call.
#[test]
fn rules_naming_a_missing_field_fail_every_call() {
    let validate = validator().message("test.rules.MissingField");
    assert_eq!(
        validate
            .validate(&[])
            .expect_err("the rule cannot be evaluated")
            .code(),
        Code::Internal
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct Divisor {
    #[prost(int32, tag = "1")]
    count: i32,
}

/// A rule that cannot be evaluated for the empty message's default values registers, and fails
/// only the messages it cannot evaluate.
#[test]
fn rules_that_fail_only_for_some_values_register() {
    let validate = validator().message("test.rules.Divisor");
    let count = |count: i32| validate.validate_message(&Divisor { count });
    assert_eq!(count(5), Ok(()));
    assert_eq!(
        count(200).expect_err("broken rule").code(),
        Code::InvalidArgument
    );
    assert_eq!(
        count(0).expect_err("rule that cannot be evaluated").code(),
        Code::Internal
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct Named {
    #[prost(string, tag = "1")]
    name: String,
}

/// Nested messages and messages of files without a package register by their full names.
#[test]
fn nested_and_unpackaged_types_register() {
    for full_name in ["test.rules.Outer.Inner", "Unpackaged"] {
        let validate = validator().message(full_name);
        assert_eq!(
            validate.validate_message(&Named {
                name: "x".to_owned()
            }),
            Ok(()),
            "{full_name}"
        );
        assert_eq!(
            validate
                .validate_message(&Named::default())
                .expect_err("the name is empty")
                .code(),
            Code::InvalidArgument,
            "{full_name}"
        );
    }
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
        .decode::<Violations>(&trevrpc::codec::Prost)
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
