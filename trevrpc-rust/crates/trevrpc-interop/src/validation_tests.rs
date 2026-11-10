//! Checks that the request of the validation scenarios keeps the rules of `ValidationBench`'s
//! request types; it is apart from `validation`, which the validation microbenchmarks include.

use prost::Message as _;
use trevrpc::Error;
use trevrpc_protovalidate::Validator;

use crate::client::expect_violation;
use crate::proto::{BenchRequest, DESCRIPTOR_SET, EchoResponse};
use crate::validation::bench_request;

fn validate(
    validator: &Validator,
    name: &'static str,
    request: &BenchRequest,
) -> Result<EchoResponse, Error> {
    validator
        .message(name)
        .validate(&request.encode_to_vec())
        .map(|()| EchoResponse::default())
        .map_err(Error::from)
}

/// The request the validation scenarios send passes the rules of every request type, and
/// each CEL rule really runs.
#[test]
fn the_bench_request_keeps_every_rule() {
    let validator = Validator::from_descriptor_set(DESCRIPTOR_SET);
    let request = bench_request("hello".to_owned());
    for name in [
        "trevrpc.interop.v1.StandardBenchRequest",
        "trevrpc.interop.v1.CelBenchRequest",
    ] {
        assert!(validate(&validator, name, &request).is_ok(), "{name}");
    }
    let broken: [(&str, &str, BenchRequest); 3] = [
        (
            "window.ordered",
            "",
            BenchRequest {
                end_ms: request.start_ms,
                ..request.clone()
            },
        ),
        (
            "tags.not_region",
            "",
            BenchRequest {
                tags: vec![request.region.clone()],
                ..request.clone()
            },
        ),
        (
            "string.email",
            "email",
            BenchRequest {
                email: "alice".to_owned(),
                ..request
            },
        ),
    ];
    for (rule, field, request) in broken {
        let outcome = validate(&validator, "trevrpc.interop.v1.CelBenchRequest", &request);
        if let Err(error) = expect_violation(rule, outcome, rule, field) {
            panic!("{error}");
        }
    }
}
