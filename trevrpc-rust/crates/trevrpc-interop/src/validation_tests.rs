//! Checks that the request of the validation scenarios keeps the rules of `ValidationBench`'s
//! request types; it is apart from `validation`, which the validation microbenchmarks include.

use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures_util::StreamExt as _;
use prost::Message as _;
use trevrpc::client::{RpcTransport, StreamingRpcTransport};
use trevrpc::{BoxStream, Code, Error, ResponseEnvelope, ResponseFrame, RpcRequest};
use trevrpc_protovalidate::Validator;

use crate::client::expect_violation;
use crate::proto::validate::Violations;
use crate::proto::{
    BenchRequest, DESCRIPTOR_SET, EchoResponse, ValidatedEchoClient, ValidatedRequest,
    validated_echo_paths,
};
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

/// A transport that records what the clients send it, and never answers a streaming call.
#[derive(Default)]
struct RecordingTransport {
    calls: Mutex<Vec<String>>,
    uploads: Mutex<Vec<BoxStream<Vec<u8>>>>,
}

impl RecordingTransport {
    fn calls(&self) -> Vec<String> {
        self.calls.lock().unwrap().clone()
    }

    /// The messages the clients uploaded on their first streaming call, once its request stream
    /// has finished.
    async fn uploaded(&self) -> Vec<String> {
        let upload = self.uploads.lock().unwrap().remove(0);
        upload
            .map(|body| {
                ValidatedRequest::decode(body.unwrap().as_slice())
                    .unwrap()
                    .message
            })
            .collect()
            .await
    }
}

#[trevrpc::async_trait]
impl RpcTransport for RecordingTransport {
    async fn call(
        &self,
        request: RpcRequest,
        _body: Vec<u8>,
    ) -> trevrpc::Result<ResponseEnvelope<Bytes>> {
        self.calls.lock().unwrap().push(request.method);
        Ok(ResponseEnvelope::new(Bytes::new()))
    }
}

#[trevrpc::async_trait]
impl StreamingRpcTransport for RecordingTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> trevrpc::Result<BoxStream<ResponseFrame>> {
        self.calls.lock().unwrap().push(request.method);
        self.uploads.lock().unwrap().push(request_body);
        Ok(Box::pin(futures_util::stream::pending()))
    }
}

fn request(message: &str) -> ValidatedRequest {
    ValidatedRequest {
        message: message.to_owned(),
    }
}

/// Checks that `outcome` is the local rejection of an empty message: `InvalidArgument` with the
/// `string.min_len` violation.
fn expect_empty_message_rejected<T>(outcome: trevrpc::Result<T>) {
    let Err(error) = outcome else {
        panic!("the request breaks its rules");
    };
    let status = error.into_status();
    assert_eq!(status.code(), Code::InvalidArgument);
    let violations = status
        .find_detail(trevrpc_protovalidate::VIOLATIONS_TYPE)
        .expect("violations detail")
        .decode::<Violations>()
        .unwrap();
    assert_eq!(violations.violations.len(), 1);
    assert_eq!(violations.violations[0].rule_id(), "string.min_len");
}

/// A client with validation fails requests that break their rules locally, and the transport never
/// sees them.
#[tokio::test]
async fn a_validating_client_rejects_invalid_unary_requests_without_sending_them() {
    let transport = Arc::new(RecordingTransport::default());
    let client = ValidatedEchoClient::new(Arc::clone(&transport)).with_validation();

    expect_empty_message_rejected(client.unary(&request("")).await);
    let status = client
        .unary(&request("forbidden"))
        .await
        .expect_err("the message rule is broken")
        .into_status();
    assert_eq!(status.code(), Code::InvalidArgument);
    assert_eq!(transport.calls(), Vec::<String>::new());

    client.unary(&request("hello")).await.unwrap();
    assert_eq!(transport.calls(), [validated_echo_paths::UNARY]);
}

/// A client without validation sends the same request, which is left to the server.
#[tokio::test]
async fn a_client_without_validation_sends_invalid_requests() {
    let transport = Arc::new(RecordingTransport::default());
    let client = ValidatedEchoClient::new(Arc::clone(&transport));

    client.unary(&request("")).await.unwrap();
    assert_eq!(transport.calls(), [validated_echo_paths::UNARY]);
}

/// A streamed message that breaks the rules fails its send and is not uploaded, and the call goes
/// on with the next message.
#[tokio::test]
async fn a_validating_client_rejects_invalid_streamed_messages_without_sending_them() {
    let transport = Arc::new(RecordingTransport::default());
    let client = ValidatedEchoClient::new(Arc::clone(&transport)).with_validation();

    let mut call = client.client_stream().await.unwrap();
    call.send(request("a")).await.unwrap();
    expect_empty_message_rejected(call.send(request("")).await);
    call.send(request("b")).await.unwrap();
    call.close_send().unwrap();

    assert_eq!(transport.uploaded().await, ["a", "b"]);
}
