use std::error::Error;
use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use trevrpc::MessageStream;
use trevrpc::advanced::RawQuinnTransport;
use trevrpc::client::CallOptions;
use trevrpc::server::RequestContext;

use crate::config::{
    MAX_APPLICATION_PAYLOAD_BYTES, MAX_ENCODED_FRAME_BYTES, MAX_MESSAGES_PER_STREAM, RpcKind,
};
use crate::proto::{
    BenchmarkRequest, BenchmarkResponse, BenchmarkService, BenchmarkServiceClient,
    BenchmarkSummary, StreamRequest,
};

pub(crate) type BoxError = Box<dyn Error + Send + Sync>;

pub(crate) trait BenchmarkWorkload: Clone + Send + Sync + 'static {
    fn execute(&self) -> impl Future<Output = Result<MessageCounts, BoxError>> + Send;
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct MessageCounts {
    pub(crate) request: u64,
    pub(crate) response: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct WorkloadConfig {
    pub(crate) rpc: RpcKind,
    pub(crate) request_bytes: usize,
    pub(crate) response_bytes: u32,
    pub(crate) messages_per_stream: u32,
}

#[derive(Clone)]
pub(crate) struct Workload {
    client: BenchmarkServiceClient<RawQuinnTransport>,
    config: WorkloadConfig,
    next_sequence: Arc<AtomicU64>,
}

impl Workload {
    pub(crate) fn new(transport: RawQuinnTransport, config: WorkloadConfig) -> Self {
        Self {
            client: BenchmarkServiceClient::with_default_call_options(transport, call_options()),
            config,
            next_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) async fn execute(&self) -> Result<MessageCounts, BoxError> {
        match self.config.rpc {
            RpcKind::Unary => self.unary().await,
            RpcKind::ClientStream => self.client_stream().await,
            RpcKind::ServerStream => self.server_stream().await,
            RpcKind::Bidi => self.bidi().await,
        }
    }

    async fn unary(&self) -> Result<MessageCounts, BoxError> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let response = self
            .client
            .unary(BenchmarkRequest {
                sequence,
                payload: vec![0; self.config.request_bytes],
                response_bytes: self.config.response_bytes,
            })
            .await?;
        validate_response(&response, sequence, self.config.response_bytes)?;
        Ok(MessageCounts {
            request: 1,
            response: 1,
        })
    }

    async fn client_stream(&self) -> Result<MessageCounts, BoxError> {
        let message_count = self.config.messages_per_stream;
        let first_sequence = self
            .next_sequence
            .fetch_add(u64::from(message_count), Ordering::Relaxed);
        let request_bytes = self.config.request_bytes;
        let response_bytes = self.config.response_bytes;
        let requests =
            trevrpc::stream::from_iter((0..message_count).map(move |offset| BenchmarkRequest {
                sequence: first_sequence.wrapping_add(u64::from(offset)),
                payload: vec![0; request_bytes],
                response_bytes,
            }));
        let summary = self.client.client_stream_from_stream(requests).await?;
        let expected_payload_bytes = u64::from(message_count)
            .checked_mul(u64::try_from(request_bytes)?)
            .ok_or_else(|| io::Error::other("request payload byte count overflowed"))?;
        if summary.message_count != u64::from(message_count)
            || summary.payload_bytes != expected_payload_bytes
        {
            return Err(io::Error::other(format!(
                "unexpected client-stream summary: count={}, bytes={}",
                summary.message_count, summary.payload_bytes
            ))
            .into());
        }
        Ok(MessageCounts {
            request: u64::from(message_count),
            response: 1,
        })
    }

    async fn server_stream(&self) -> Result<MessageCounts, BoxError> {
        let message_count = self.config.messages_per_stream;
        let mut responses = self
            .client
            .server_stream(StreamRequest {
                message_count,
                payload: vec![0; self.config.request_bytes],
                response_bytes: self.config.response_bytes,
            })
            .await?;
        let mut received = 0_u32;
        while let Some(response) = responses.next().await {
            let response = response?;
            validate_response(&response, u64::from(received), self.config.response_bytes)?;
            received = received
                .checked_add(1)
                .ok_or_else(|| io::Error::other("server-stream response count overflowed"))?;
        }
        if received != message_count {
            return Err(io::Error::other(format!(
                "server stream returned {received} messages, expected {message_count}"
            ))
            .into());
        }
        Ok(MessageCounts {
            request: 1,
            response: u64::from(message_count),
        })
    }

    async fn bidi(&self) -> Result<MessageCounts, BoxError> {
        let message_count = self.config.messages_per_stream;
        let first_sequence = self
            .next_sequence
            .fetch_add(u64::from(message_count), Ordering::Relaxed);
        let request_bytes = self.config.request_bytes;
        let response_bytes = self.config.response_bytes;
        let requests =
            trevrpc::stream::from_iter((0..message_count).map(move |offset| BenchmarkRequest {
                sequence: first_sequence.wrapping_add(u64::from(offset)),
                payload: vec![0; request_bytes],
                response_bytes,
            }));
        let mut responses = self.client.bidi_from_stream(requests).await?;
        let mut received = 0_u32;
        while let Some(response) = responses.next().await {
            let response = response?;
            validate_response(
                &response,
                first_sequence.wrapping_add(u64::from(received)),
                self.config.response_bytes,
            )?;
            received = received
                .checked_add(1)
                .ok_or_else(|| io::Error::other("bidi response count overflowed"))?;
        }
        if received != message_count {
            return Err(io::Error::other(format!(
                "bidi stream returned {received} messages, expected {message_count}"
            ))
            .into());
        }
        Ok(MessageCounts {
            request: u64::from(message_count),
            response: u64::from(message_count),
        })
    }
}

impl BenchmarkWorkload for Workload {
    fn execute(&self) -> impl Future<Output = Result<MessageCounts, BoxError>> + Send {
        Self::execute(self)
    }
}

fn call_options() -> CallOptions {
    CallOptions::new()
        .with_max_response_body_size(MAX_ENCODED_FRAME_BYTES)
        .with_max_response_messages(None)
        .with_max_response_stream_body_size(None)
}

pub(crate) fn validate_response(
    response: &BenchmarkResponse,
    expected_sequence: u64,
    expected_bytes: u32,
) -> Result<(), io::Error> {
    if response.sequence != expected_sequence {
        return Err(io::Error::other(format!(
            "response sequence was {}, expected {expected_sequence}",
            response.sequence
        )));
    }
    if response.payload.len() != expected_bytes as usize {
        return Err(io::Error::other(format!(
            "response payload was {} bytes, expected {expected_bytes}",
            response.payload.len()
        )));
    }
    if response.payload.iter().any(|byte| *byte != 0) {
        return Err(io::Error::other("response payload contained non-zero data"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BenchmarkServiceImpl;

#[trevrpc::async_trait]
impl BenchmarkService for BenchmarkServiceImpl {
    async fn unary(
        &self,
        _context: RequestContext,
        request: BenchmarkRequest,
    ) -> Result<BenchmarkResponse, trevrpc::Status> {
        response_for(&request)
    }

    async fn client_stream(
        &self,
        _context: RequestContext,
        mut requests: trevrpc::BoxMessageStream<BenchmarkRequest>,
    ) -> Result<BenchmarkSummary, trevrpc::Status> {
        let mut message_count = 0_u64;
        let mut payload_bytes = 0_u64;
        while let Some(request) = requests.next().await {
            let request = request.map_err(trevrpc::Error::into_status)?;
            checked_request_payload(&request.payload)?;
            message_count = message_count
                .checked_add(1)
                .ok_or_else(|| trevrpc::Status::resource_exhausted("message count overflowed"))?;
            if message_count > u64::from(MAX_MESSAGES_PER_STREAM) {
                return Err(trevrpc::Status::resource_exhausted(
                    "message count exceeded the benchmark peer limit",
                ));
            }
            payload_bytes = payload_bytes
                .checked_add(u64::try_from(request.payload.len()).map_err(|_| {
                    trevrpc::Status::resource_exhausted("payload byte count overflowed")
                })?)
                .ok_or_else(|| {
                    trevrpc::Status::resource_exhausted("payload byte count overflowed")
                })?;
        }
        Ok(BenchmarkSummary {
            message_count,
            payload_bytes,
        })
    }

    async fn server_stream(
        &self,
        _context: RequestContext,
        request: StreamRequest,
    ) -> Result<trevrpc::BoxMessageStream<BenchmarkResponse>, trevrpc::Status> {
        checked_request_payload(&request.payload)?;
        let response_bytes = checked_response_bytes(request.response_bytes)?;
        if request.message_count == 0 || request.message_count > MAX_MESSAGES_PER_STREAM {
            return Err(trevrpc::Status::invalid_argument(
                "message_count is outside the benchmark peer limit",
            ));
        }
        Ok(trevrpc::stream::from_iter((0..request.message_count).map(
            move |sequence| BenchmarkResponse {
                sequence: u64::from(sequence),
                payload: vec![0; response_bytes],
            },
        )))
    }

    async fn bidi(
        &self,
        _context: RequestContext,
        requests: trevrpc::BoxMessageStream<BenchmarkRequest>,
    ) -> Result<trevrpc::BoxMessageStream<BenchmarkResponse>, trevrpc::Status> {
        Ok(Box::new(BidiResponses {
            requests,
            received: 0,
        }))
    }
}

struct BidiResponses {
    requests: trevrpc::BoxMessageStream<BenchmarkRequest>,
    received: u32,
}

#[trevrpc::async_trait]
impl MessageStream<BenchmarkResponse> for BidiResponses {
    async fn next(&mut self) -> Option<trevrpc::Result<BenchmarkResponse>> {
        let request = self.requests.next().await?;
        self.received = match self.received.checked_add(1) {
            Some(received) if received <= MAX_MESSAGES_PER_STREAM => received,
            _ => {
                return Some(Err(trevrpc::Status::resource_exhausted(
                    "message count exceeded the benchmark peer limit",
                )
                .into()));
            }
        };
        Some(request.and_then(|request| response_for(&request).map_err(Into::into)))
    }
}

fn response_for(request: &BenchmarkRequest) -> Result<BenchmarkResponse, trevrpc::Status> {
    checked_request_payload(&request.payload)?;
    let response_bytes = checked_response_bytes(request.response_bytes)?;
    Ok(BenchmarkResponse {
        sequence: request.sequence,
        payload: vec![0; response_bytes],
    })
}

fn checked_request_payload(payload: &[u8]) -> Result<(), trevrpc::Status> {
    if payload.len() > MAX_APPLICATION_PAYLOAD_BYTES {
        return Err(trevrpc::Status::invalid_argument(
            "request payload is outside the benchmark peer limit",
        ));
    }
    Ok(())
}

fn checked_response_bytes(response_bytes: u32) -> Result<usize, trevrpc::Status> {
    let response_bytes = response_bytes as usize;
    if response_bytes > MAX_APPLICATION_PAYLOAD_BYTES {
        return Err(trevrpc::Status::invalid_argument(
            "response_bytes is outside the benchmark peer limit",
        ));
    }
    Ok(response_bytes)
}

#[cfg(test)]
mod tests {
    use super::BenchmarkServiceImpl;
    use crate::config::MAX_APPLICATION_PAYLOAD_BYTES;
    use crate::proto::{BenchmarkRequest, BenchmarkService, StreamRequest};

    #[tokio::test]
    async fn native_service_rejects_oversized_request_payloads() {
        let status = BenchmarkServiceImpl
            .unary(request_context(), oversized_request())
            .await
            .expect_err("oversized unary payload unexpectedly succeeded");
        assert_oversized_status(&status);

        let status = BenchmarkServiceImpl
            .client_stream(
                request_context(),
                trevrpc::stream::from_iter([oversized_request()]),
            )
            .await
            .expect_err("oversized client-stream payload unexpectedly succeeded");
        assert_oversized_status(&status);

        let Err(status) = BenchmarkServiceImpl
            .server_stream(
                request_context(),
                StreamRequest {
                    message_count: 1,
                    payload: oversized_payload(),
                    response_bytes: 0,
                },
            )
            .await
        else {
            panic!("oversized server-stream payload unexpectedly succeeded");
        };
        assert_oversized_status(&status);

        let mut responses = BenchmarkServiceImpl
            .bidi(
                request_context(),
                trevrpc::stream::from_iter([oversized_request()]),
            )
            .await
            .expect("bidi response stream should be created");
        let error = responses
            .next()
            .await
            .expect("bidi response stream ended before rejecting the payload")
            .expect_err("oversized bidi payload unexpectedly succeeded");
        assert_oversized_status(&error.into_status());
    }

    fn request_context() -> trevrpc::server::RequestContext {
        trevrpc::server::RequestContext::new(
            &trevrpc::RpcRequest::new("benchmark", "test", Vec::new()),
            None,
        )
    }

    fn oversized_request() -> BenchmarkRequest {
        BenchmarkRequest {
            sequence: 0,
            payload: oversized_payload(),
            response_bytes: 0,
        }
    }

    fn oversized_payload() -> Vec<u8> {
        vec![0; MAX_APPLICATION_PAYLOAD_BYTES + 1]
    }

    fn assert_oversized_status(status: &trevrpc::Status) {
        assert_eq!(status.code(), trevrpc::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "request payload is outside the benchmark peer limit"
        );
    }
}
