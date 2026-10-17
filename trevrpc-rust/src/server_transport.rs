use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::framed::{self, FrameRead, MESSAGE_FRAME_BATCH};
use crate::request_pump::{
    RequestInputKind, RequestPumpReader, RequestPumpSettle, start_request_pump,
};
use crate::server::{CancellationSource, CancellationToken, RequestBudget, Server};
use crate::{Code, Error, ResponseEnvelope, ResponseFrame, Result, RpcKind, RpcRequest, Status};

/// Sends response frames without imposing a transport framing format.
///
/// A transport adapter owns encoding, length prefixes, tracing, and the concrete stream
/// cancellation operations. The server driver only deals in logical response frames.
pub trait ResponseWriter: Send + 'static {
    /// Writes `frames` in order, as one write where the transport allows, draining the vector.
    fn write_frames(
        &mut self,
        frames: &mut Vec<ResponseFrame>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn stopped(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn reset(&mut self);

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// A request whose REQUEST frame, and the single message of a unary or server-streaming call,
/// have been read.
pub struct IncomingRequest {
    pub(crate) request: RpcRequest,
    /// The request message of a unary or server-streaming call, empty for the other kinds.
    pub(crate) body: Vec<u8>,
    /// What the rest of the request stream may still send.
    pub(crate) budget: RequestBudget,
}

/// Reads the REQUEST frame and, for calls with a single request message, that message, bounded by
/// the server's initial request timeout.
///
/// Unknown frames are skipped. Failures here are protocol errors the caller answers with a lone
/// STATUS frame.
pub async fn read_initial_request<R>(server: &Server, recv: &mut R) -> Result<IncomingRequest>
where
    R: FrameRead,
{
    let read = read_request_and_message(server, recv);
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

async fn read_request_and_message<R>(server: &Server, recv: &mut R) -> Result<IncomingRequest>
where
    R: FrameRead,
{
    let max_frame_size = server.max_frame_size();
    let mut budget = RequestBudget::from_options(server.options());
    let request =
        framed::read_request(recv, max_frame_size, |len| budget.charge_skipped(len)).await?;
    let body = if request.rpc_kind().has_single_request_message() {
        let frame =
            framed::read_request_frame(recv, max_frame_size, |len| budget.charge_skipped(len))
                .await?;
        match frame {
            Some(framed::RequestFrame::Message(body)) => Vec::from(body),
            Some(framed::RequestFrame::Status(_)) => {
                return Err(Error::from(Status::invalid_argument(
                    "request stream carried a STATUS frame instead of the request message",
                )));
            }
            None => {
                return Err(Error::from(Status::invalid_argument(
                    "request stream ended before the request message",
                )));
            }
        }
    } else {
        Vec::new()
    };
    Ok(IncomingRequest {
        request,
        body,
        budget,
    })
}

/// Handles one already-decoded RPC stream using a transport-neutral response writer and reader.
pub async fn handle_stream<W, R>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: W,
    mut recv: R,
    incoming: IncomingRequest,
    mut shutdown: watch::Receiver<bool>,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        budget,
    } = incoming;
    let request_body_len = body.len();
    let Some(request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, request_body_len, &status);
        reject_with_status(&server, &mut send, &mut recv, &request, budget, status).await;
        return;
    };

    let cancellation = CancellationToken::new();
    if request.rpc_kind() != RpcKind::Unary {
        let incoming = IncomingRequest {
            request,
            body,
            budget,
        };
        handle_streaming_rpc(server, send, recv, incoming, cancellation, shutdown).await;
        return;
    }

    let request_for_failure = request.clone();
    let (_request_body, mut request_pump) = start_request_pump(
        recv,
        RequestInputKind::Unary,
        server.max_frame_size(),
        budget,
    );
    let response = tokio::select! {
        biased;
        response = server.handle_request_with_cancellation(request, body, cancellation.clone()) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, request_body_len, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            Err(failure.status().clone())
        },
        stopped = send.stopped() => {
            cancellation.cancel(if stopped.is_ok() {
                CancellationSource::PeerReset
            } else {
                CancellationSource::ConnectionLost
            });
            let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
            tracing::debug!("client stopped response stream before RPC completed");
            return;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            cancellation.cancel(CancellationSource::ServerShutdown);
            // Never end a response without its STATUS: the client sees a cancelled call.
            send.reset();
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    let mut frames = unary_response_frames(response);
    match send.write_frames(&mut frames).await {
        Ok(()) => {
            let _ = send.finish().await;
            drop(request_permit);
            let _ = request_pump
                .settle(RequestPumpSettle::ResponseCommitted)
                .await;
        }
        Err(error) => {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
        }
    }
}

/// Frames a unary response: `[HEADERS] MESSAGE STATUS` on success, a lone STATUS on failure.
fn unary_response_frames(
    response: std::result::Result<ResponseEnvelope<Vec<u8>>, Status>,
) -> Vec<ResponseFrame> {
    match response {
        Ok(response) => {
            let (body, headers, metadata) = response.into_parts();
            let mut frames = Vec::with_capacity(3);
            if !headers.is_empty() {
                frames.push(ResponseFrame::Headers(headers));
            }
            frames.push(ResponseFrame::Message(Bytes::from(body)));
            frames.push(ResponseFrame::Status(Status::ok().with_metadata(metadata)));
            frames
        }
        Err(status) => vec![ResponseFrame::Status(status)],
    }
}

/// Rejects an already-decoded RPC because the connection stream limit was reached.
pub async fn reject_stream<W, R>(
    server: Server,
    mut send: W,
    mut recv: R,
    incoming: IncomingRequest,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        budget,
    } = incoming;
    let status = Status::unavailable("too many concurrent streams on connection");
    server.record_rejected_request(&request, body.len(), &status);
    reject_with_status(&server, &mut send, &mut recv, &request, budget, status).await;
}

/// Answers a request with a lone STATUS, then lets a request that is not left open finish.
async fn reject_with_status<W, R>(
    server: &Server,
    send: &mut W,
    recv: &mut R,
    request: &RpcRequest,
    mut budget: RequestBudget,
    status: Status,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    if send
        .write_frames(&mut vec![ResponseFrame::Status(status)])
        .await
        .is_ok()
    {
        if request.rpc_kind().has_single_request_message() {
            drain_request_end_or_stop(server, recv, &mut budget).await;
        }
        let _ = send.finish().await;
    }
}

async fn drain_request_end_or_stop<R>(server: &Server, recv: &mut R, budget: &mut RequestBudget)
where
    R: RequestPumpReader,
{
    let read = recv.read_request_end(
        server.max_frame_size(),
        budget,
        "request stream contained a frame after the request message",
    );
    let result = if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| {
                Error::from(Status::deadline_exceeded(
                    "unary request stream finish timeout",
                ))
            })
            .and_then(std::convert::identity)
    } else {
        read.await
    };
    if let Err(error) = result {
        recv.stop_trevrpc();
        tracing::debug!(%error, "failed to drain unary request stream");
    }
}

/// Frames pulled from a handler's response stream, written together.
struct ResponseBatch {
    frames: Vec<ResponseFrame>,
    /// The batch ends with the terminal STATUS.
    terminal: bool,
}

#[allow(clippy::too_many_lines)]
async fn handle_streaming_rpc<W, R>(
    server: Server,
    mut send: W,
    recv: R,
    incoming: IncomingRequest,
    cancellation: CancellationToken,
    mut shutdown: watch::Receiver<bool>,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let IncomingRequest {
        request,
        body,
        budget,
    } = incoming;
    let max_frame_size = server.max_frame_size();
    let request_body_len = body.len();
    let request_for_failure = request.clone();
    let (request_body, mut request_pump) = start_request_pump(
        recv,
        RequestInputKind::for_rpc_kind(request.rpc_kind()),
        max_frame_size,
        budget,
    );
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request_with_cancellation(
            request,
            body,
            request_body,
            cancellation.clone(),
        ) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, request_body_len, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            crate::stream::from_iter([ResponseFrame::Status(failure.status().clone())])
        },
        stopped = send.stopped() => {
            cancellation.cancel(if stopped.is_ok() {
                CancellationSource::PeerReset
            } else {
                CancellationSource::ConnectionLost
            });
            let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
            tracing::debug!("client stopped response stream before streaming RPC handler completed");
            return;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            cancellation.cancel(CancellationSource::ServerShutdown);
            // Never end a response without its STATUS: the client sees a cancelled call.
            send.reset();
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    loop {
        let frame = tokio::select! {
            biased;
            failure = request_pump.failure() => {
                cancellation.set_completion_code(failure.status().code());
                if let Some(source) = failure.cancellation_source() {
                    cancellation.cancel(source);
                }
                if !failure.response_writable() {
                    send.reset();
                    let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                    return;
                }
                Some(Ok(ResponseFrame::Status(failure.status().clone())))
            },
            frame = response.next() => frame,
            stopped = send.stopped() => {
                cancellation.cancel(if stopped.is_ok() {
                    CancellationSource::PeerReset
                } else {
                    CancellationSource::ConnectionLost
                });
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                tracing::debug!("client stopped response stream before streaming RPC completed");
                return;
            }
            changed = shutdown.changed() => {
                let _ = changed;
                cancellation.cancel(CancellationSource::ServerShutdown);
                // Never end a response without its STATUS: the client sees a cancelled call.
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
                return;
            }
        };

        let Some(frame) = frame else {
            break;
        };

        let mut batch = ResponseBatch {
            frames: Vec::with_capacity(MESSAGE_FRAME_BATCH),
            terminal: false,
        };
        push_response_frame(&mut batch, frame);
        // Frames that are ready now go out in the same write; a frame that is not ready never
        // delays the ones before it.
        while !batch.terminal && batch.frames.len() < MESSAGE_FRAME_BATCH {
            match response.next().now_or_never() {
                Some(Some(frame)) => push_response_frame(&mut batch, frame),
                Some(None) | None => break,
            }
        }

        if let Err(error) = send.write_frames(&mut batch.frames).await {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            return;
        }
        if batch.terminal {
            break;
        }
    }

    let _ = send.finish().await;
    let _ = request_pump
        .settle(RequestPumpSettle::ResponseCommitted)
        .await;
}

/// Adds a frame to a batch; a failed response stream becomes the terminal STATUS.
fn push_response_frame(batch: &mut ResponseBatch, frame: Result<ResponseFrame>) {
    let frame = frame.unwrap_or_else(|error| ResponseFrame::Status(error.into_status()));
    batch.terminal = frame.is_status();
    batch.frames.push(frame);
}

/// Answers a stream with a lone STATUS and finishes it.
pub async fn write_status<W>(mut send: W, status: Status)
where
    W: ResponseWriter,
{
    if send
        .write_frames(&mut vec![ResponseFrame::Status(status)])
        .await
        .is_ok()
    {
        let _ = send.finish().await;
    }
}

#[allow(dead_code)]
pub struct Permit(Option<OwnedSemaphorePermit>);

pub fn try_acquire_permit(limit: Option<&Arc<Semaphore>>) -> Option<Permit> {
    limit.map_or(Some(Permit(None)), |semaphore| {
        semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| Permit(Some(permit)))
    })
}

fn cancel_from_transport_error(cancellation: &CancellationToken, error: &Error) {
    if let Some(code) = error.transport_code() {
        cancellation.cancel(if code == Code::Cancelled {
            CancellationSource::PeerReset
        } else {
            CancellationSource::ConnectionLost
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{read_initial_request, try_acquire_permit, unary_response_frames};
    use crate::framing::{
        DEFAULT_MAX_FRAME_SIZE, encode_frame, encode_message_frame, encode_protobuf_frame,
        frame_type,
    };
    use crate::server::{Server, ServerOptions};
    use crate::test_io::Script;
    use crate::{Code, Metadata, ResponseEnvelope, ResponseFrame, RpcKind, RpcRequest, Status};
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn permit_admission_preserves_unlimited_and_bounded_behavior() {
        assert!(try_acquire_permit(None).is_some());
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = try_acquire_permit(Some(&semaphore));
        assert!(permit.is_some());
        assert!(try_acquire_permit(Some(&semaphore)).is_none());
        drop(permit);
    }

    fn request_frame(kind: RpcKind) -> Vec<u8> {
        encode_protobuf_frame(
            frame_type::REQUEST,
            &RpcRequest::new("svc", "m").with_kind(kind),
            DEFAULT_MAX_FRAME_SIZE,
        )
        .expect("request should encode")
    }

    fn message_frame(body: &[u8]) -> Vec<u8> {
        encode_message_frame(body, DEFAULT_MAX_FRAME_SIZE).expect("message should encode")
    }

    fn grease(payload: usize) -> Vec<u8> {
        encode_frame(0x21, &vec![0; payload], DEFAULT_MAX_FRAME_SIZE).expect("grease should encode")
    }

    async fn read(server: &Server, wire: Vec<Vec<u8>>) -> crate::Result<super::IncomingRequest> {
        read_initial_request(server, &mut Script::new(wire.concat())).await
    }

    #[tokio::test]
    async fn unary_and_server_streaming_requests_carry_one_message() {
        let server = Server::new();
        for kind in [RpcKind::Unary, RpcKind::ServerStreaming] {
            let incoming = read(&server, vec![request_frame(kind), message_frame(b"hi")])
                .await
                .expect("request should read");
            assert_eq!(incoming.request.rpc_kind(), kind);
            assert_eq!(incoming.body, b"hi");
        }
    }

    #[tokio::test]
    async fn streaming_requests_read_no_message_before_dispatch() {
        let server = Server::new();
        for kind in [RpcKind::ClientStreaming, RpcKind::BidirectionalStreaming] {
            let incoming = read(&server, vec![request_frame(kind), message_frame(b"later")])
                .await
                .expect("request should read");
            assert!(incoming.body.is_empty());
        }
    }

    #[tokio::test]
    async fn unknown_frames_are_skipped_around_the_request_and_message() {
        let server = Server::new();
        let incoming = read(
            &server,
            vec![
                grease(3),
                request_frame(RpcKind::Unary),
                grease(0),
                encode_frame(0x1234, b"x", 1024).unwrap(),
                message_frame(b"hi"),
            ],
        )
        .await
        .expect("request should read");
        assert_eq!(incoming.body, b"hi");
    }

    #[tokio::test]
    async fn unary_requests_without_a_message_are_invalid() {
        let server = Server::new();
        let ended = read(&server, vec![request_frame(RpcKind::Unary)]).await;
        let status_in_place = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                encode_protobuf_frame(
                    frame_type::STATUS,
                    &crate::RpcStatus::from(Status::ok()),
                    1024,
                )
                .unwrap(),
            ],
        )
        .await;
        for result in [ended, status_in_place] {
            let status = result
                .err()
                .expect("a missing message must be rejected")
                .into_status();
            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn misplaced_frames_before_the_request_are_invalid() {
        let server = Server::new();
        let headers = encode_frame(frame_type::HEADERS, &[], 1024).unwrap();
        for first in [headers, message_frame(b"hi")] {
            let status = read(&server, vec![first, request_frame(RpcKind::Unary)])
                .await
                .err()
                .expect("a stream must open with REQUEST")
                .into_status();
            assert_eq!(status.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn oversized_request_frames_are_rejected_before_their_payload() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_frame_size(16));
        // A MESSAGE declaring 17 bytes against a 16-byte limit, with no payload.
        let status = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                vec![frame_type::MESSAGE.try_into().unwrap(), 17],
            ],
        )
        .await
        .err()
        .expect("oversized frame must be rejected")
        .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn grease_before_the_message_is_charged_to_the_body_limit() {
        let mut server = Server::new();
        server.set_options(ServerOptions::new().with_max_stream_body_size(Some(100)));
        let status = read(
            &server,
            vec![
                request_frame(RpcKind::Unary),
                grease(60),
                grease(60),
                message_frame(b"hi"),
            ],
        )
        .await
        .err()
        .expect("grease must not be free")
        .into_status();
        assert_eq!(status.code(), Code::ResourceExhausted);

        // The request message itself is bounded by the frame size, not the stream body limit.
        let incoming = read(
            &server,
            vec![request_frame(RpcKind::Unary), message_frame(&[7; 200])],
        )
        .await
        .expect("a large unary message is fine");
        assert_eq!(incoming.body.len(), 200);
    }

    #[test]
    fn unary_responses_are_headers_message_status() {
        let headers = Metadata::from([("x-a".to_owned(), b"1".to_vec())]);
        let trailers = Metadata::from([("x-t".to_owned(), b"2".to_vec())]);
        let frames = unary_response_frames(Ok(ResponseEnvelope::new(b"reply".to_vec())
            .with_headers(headers.clone())
            .with_metadata(trailers.clone())));
        assert_eq!(
            frames,
            [
                ResponseFrame::Headers(headers),
                ResponseFrame::message(&b"reply"[..]),
                ResponseFrame::Status(Status::ok().with_metadata(trailers)),
            ]
        );

        // Without headers there is no HEADERS frame.
        let frames = unary_response_frames(Ok(ResponseEnvelope::new(Vec::new())));
        assert_eq!(
            frames,
            [
                ResponseFrame::message(Vec::new()),
                ResponseFrame::Status(Status::ok()),
            ]
        );

        // Failures are a lone STATUS.
        let frames = unary_response_frames(Err(Status::not_found("gone")));
        assert_eq!(frames, [ResponseFrame::Status(Status::not_found("gone"))]);
    }
}
