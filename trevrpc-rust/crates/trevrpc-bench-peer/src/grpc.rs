use std::future::Future;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

use crate::config::{
    MAX_APPLICATION_PAYLOAD_BYTES, MAX_ENCODED_FRAME_BYTES, MAX_MESSAGES_PER_STREAM, RpcKind,
};
use crate::proto::grpc::benchmark_service_client::BenchmarkServiceClient;
use crate::proto::grpc::benchmark_service_server::{BenchmarkService, BenchmarkServiceServer};
use crate::proto::{BenchmarkRequest, BenchmarkResponse, BenchmarkSummary, StreamRequest};
use crate::workload::{
    BenchmarkWorkload, BoxError, MessageCounts, WorkloadConfig, validate_response,
};

const STREAM_BUFFER: usize = 16;

#[derive(Clone)]
pub(crate) struct GrpcWorkload {
    client: BenchmarkServiceClient<Channel>,
    config: WorkloadConfig,
    next_sequence: Arc<AtomicU64>,
}

impl GrpcWorkload {
    pub(crate) fn new(channel: Channel, config: WorkloadConfig) -> Self {
        Self {
            client: BenchmarkServiceClient::new(channel)
                .max_decoding_message_size(MAX_ENCODED_FRAME_BYTES)
                .max_encoding_message_size(MAX_ENCODED_FRAME_BYTES),
            config,
            next_sequence: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn execute(&self) -> Result<MessageCounts, BoxError> {
        match self.config.rpc {
            RpcKind::Unary => self.unary().await,
            RpcKind::ClientStream => self.client_stream().await,
            RpcKind::ServerStream => self.server_stream().await,
            RpcKind::Bidi => self.bidi().await,
        }
    }

    async fn unary(&self) -> Result<MessageCounts, BoxError> {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let mut client = self.client.clone();
        let response = client
            .unary(BenchmarkRequest {
                sequence,
                payload: vec![0; self.config.request_bytes],
                response_bytes: self.config.response_bytes,
            })
            .await?
            .into_inner();
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
        let requests = tokio_stream::iter((0..message_count).map(move |offset| BenchmarkRequest {
            sequence: first_sequence.wrapping_add(u64::from(offset)),
            payload: vec![0; request_bytes],
            response_bytes,
        }));
        let mut client = self.client.clone();
        let summary = client.client_stream(requests).await?.into_inner();
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
        let mut client = self.client.clone();
        let mut responses = client
            .server_stream(StreamRequest {
                message_count,
                payload: vec![0; self.config.request_bytes],
                response_bytes: self.config.response_bytes,
            })
            .await?
            .into_inner();
        let mut received = 0_u32;
        while let Some(response) = responses.message().await? {
            validate_response(&response, u64::from(received), self.config.response_bytes)?;
            received = received
                .checked_add(1)
                .ok_or_else(|| io::Error::other("server-stream response count overflowed"))?;
        }
        validate_message_count("server stream", received, message_count)?;
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
        let (requests_tx, requests_rx) = mpsc::channel(STREAM_BUFFER);
        let request_task = tokio::spawn(async move {
            for offset in 0..message_count {
                requests_tx
                    .send(BenchmarkRequest {
                        sequence: first_sequence.wrapping_add(u64::from(offset)),
                        payload: vec![0; request_bytes],
                        response_bytes,
                    })
                    .await
                    .map_err(|_| io::Error::other("bidi request stream closed early"))?;
            }
            Ok::<(), io::Error>(())
        });

        let mut client = self.client.clone();
        let mut responses = client
            .bidi(ReceiverStream::new(requests_rx))
            .await?
            .into_inner();
        let mut received = 0_u32;
        while let Some(response) = responses.message().await? {
            validate_response(
                &response,
                first_sequence.wrapping_add(u64::from(received)),
                self.config.response_bytes,
            )?;
            received = received
                .checked_add(1)
                .ok_or_else(|| io::Error::other("bidi response count overflowed"))?;
        }
        request_task.await??;
        validate_message_count("bidi stream", received, message_count)?;
        Ok(MessageCounts {
            request: u64::from(message_count),
            response: u64::from(message_count),
        })
    }
}

impl BenchmarkWorkload for GrpcWorkload {
    fn execute(&self) -> impl Future<Output = Result<MessageCounts, BoxError>> + Send {
        Self::execute(self)
    }
}

fn validate_message_count(name: &str, actual: u32, expected: u32) -> Result<(), io::Error> {
    if actual == expected {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "{name} returned {actual} messages, expected {expected}"
        )))
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GrpcBenchmarkService;

type ResponseStream = ReceiverStream<Result<BenchmarkResponse, Status>>;

#[tonic::async_trait]
impl BenchmarkService for GrpcBenchmarkService {
    async fn unary(
        &self,
        request: Request<BenchmarkRequest>,
    ) -> Result<Response<BenchmarkResponse>, Status> {
        response_for(&request.into_inner()).map(Response::new)
    }

    async fn client_stream(
        &self,
        request: Request<tonic::Streaming<BenchmarkRequest>>,
    ) -> Result<Response<BenchmarkSummary>, Status> {
        let mut requests = request.into_inner();
        let mut message_count = 0_u64;
        let mut payload_bytes = 0_u64;
        while let Some(request) = requests.message().await? {
            checked_request_payload(&request.payload)?;
            message_count = message_count
                .checked_add(1)
                .ok_or_else(|| Status::resource_exhausted("message count overflowed"))?;
            if message_count > u64::from(MAX_MESSAGES_PER_STREAM) {
                return Err(Status::resource_exhausted(
                    "message count exceeded the benchmark peer limit",
                ));
            }
            payload_bytes = payload_bytes
                .checked_add(
                    u64::try_from(request.payload.len())
                        .map_err(|_| Status::resource_exhausted("payload byte count overflowed"))?,
                )
                .ok_or_else(|| Status::resource_exhausted("payload byte count overflowed"))?;
        }
        Ok(Response::new(BenchmarkSummary {
            message_count,
            payload_bytes,
        }))
    }

    type ServerStreamStream = ResponseStream;

    async fn server_stream(
        &self,
        request: Request<StreamRequest>,
    ) -> Result<Response<Self::ServerStreamStream>, Status> {
        let request = request.into_inner();
        checked_request_payload(&request.payload)?;
        let response_bytes = checked_response_bytes(request.response_bytes)?;
        if request.message_count == 0 || request.message_count > MAX_MESSAGES_PER_STREAM {
            return Err(Status::invalid_argument(
                "message_count is outside the benchmark peer limit",
            ));
        }
        let (responses_tx, responses_rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            for sequence in 0..request.message_count {
                if responses_tx
                    .send(Ok(BenchmarkResponse {
                        sequence: u64::from(sequence),
                        payload: vec![0; response_bytes],
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(responses_rx)))
    }

    type BidiStream = ResponseStream;

    async fn bidi(
        &self,
        request: Request<tonic::Streaming<BenchmarkRequest>>,
    ) -> Result<Response<Self::BidiStream>, Status> {
        let mut requests = request.into_inner();
        let (responses_tx, responses_rx) = mpsc::channel(STREAM_BUFFER);
        tokio::spawn(async move {
            let mut received = 0_u32;
            loop {
                let response = match requests.message().await {
                    Ok(Some(request)) => {
                        received = match received.checked_add(1) {
                            Some(received) if received <= MAX_MESSAGES_PER_STREAM => received,
                            _ => {
                                let _ = responses_tx
                                    .send(Err(Status::resource_exhausted(
                                        "message count exceeded the benchmark peer limit",
                                    )))
                                    .await;
                                return;
                            }
                        };
                        response_for(&request)
                    }
                    Ok(None) => return,
                    Err(status) => Err(status),
                };
                if responses_tx.send(response).await.is_err() {
                    return;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(responses_rx)))
    }
}

pub(crate) fn benchmark_service() -> BenchmarkServiceServer<GrpcBenchmarkService> {
    BenchmarkServiceServer::new(GrpcBenchmarkService)
        .max_decoding_message_size(MAX_ENCODED_FRAME_BYTES)
        .max_encoding_message_size(MAX_ENCODED_FRAME_BYTES)
}

fn response_for(request: &BenchmarkRequest) -> Result<BenchmarkResponse, Status> {
    checked_request_payload(&request.payload)?;
    let response_bytes = checked_response_bytes(request.response_bytes)?;
    Ok(BenchmarkResponse {
        sequence: request.sequence,
        payload: vec![0; response_bytes],
    })
}

fn checked_request_payload(payload: &[u8]) -> Result<(), Status> {
    if payload.len() > MAX_APPLICATION_PAYLOAD_BYTES {
        return Err(Status::invalid_argument(
            "request payload is outside the benchmark peer limit",
        ));
    }
    Ok(())
}

fn checked_response_bytes(response_bytes: u32) -> Result<usize, Status> {
    let response_bytes = response_bytes as usize;
    if response_bytes > MAX_APPLICATION_PAYLOAD_BYTES {
        return Err(Status::invalid_argument(
            "response_bytes is outside the benchmark peer limit",
        ));
    }
    Ok(response_bytes)
}
