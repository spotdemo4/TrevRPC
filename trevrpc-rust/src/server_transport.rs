use std::future::Future;
use std::sync::Arc;

use futures_util::{FutureExt, StreamExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};

use crate::framed::{self, FrameRead, MESSAGE_FRAME_BATCH, is_plain_message_frame};
use crate::request_pump::{
    RequestInputKind, RequestPumpReader, RequestPumpSettle, start_request_pump,
};
use crate::server::{CancellationSource, CancellationToken, Server};
use crate::{
    Code, Error, Result, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind,
    Status,
};

/// Sends complete `TrevRPC` response messages without imposing a transport framing format.
///
/// A transport adapter owns encoding, length prefixes, tracing, and the concrete stream
/// cancellation operations. The server driver only deals in logical RPC responses and frames.
pub(crate) trait ResponseWriter: Send + 'static {
    fn write_response(&mut self, response: &RpcResponse)
    -> impl Future<Output = Result<()>> + Send;

    fn write_message_stream_frames(
        &mut self,
        bodies: &mut Vec<Vec<u8>>,
    ) -> impl Future<Output = Result<()>> + Send;

    fn write_stream_frame(
        &mut self,
        frame: RpcStreamFrame,
    ) -> impl Future<Output = Result<()>> + Send;

    fn stopped(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn reset(&mut self);

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Reads the initial request frame, bounded by the server's initial request timeout.
pub(crate) async fn read_initial_request<R>(server: &Server, recv: &mut R) -> Result<RpcRequest>
where
    R: FrameRead,
{
    let read = framed::read_frame::<_, RpcRequest>(recv, server.max_frame_size());
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

/// Handles one already-decoded RPC stream using a transport-neutral response writer and reader.
pub(crate) async fn handle_stream<W, R>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: W,
    mut recv: R,
    request: RpcRequest,
    mut shutdown: watch::Receiver<bool>,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let Some(request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, &status);
        if write_rpc_status(&mut send, &request, status).await {
            if request.rpc_kind() == RpcKind::Unary {
                drain_unary_request_end_or_stop(&server, &mut recv).await;
            }
            let _ = send.finish().await;
        }
        return;
    };

    let cancellation = CancellationToken::new();
    if request.rpc_kind() != RpcKind::Unary {
        handle_streaming_rpc(server, send, recv, request, cancellation, shutdown).await;
        return;
    }

    let request_for_failure = request.clone();
    let (_request_body, mut request_pump) =
        start_request_pump(recv, RequestInputKind::Unary, server.max_frame_size());
    let response = tokio::select! {
        biased;
        response = server.handle_request_with_cancellation(request, cancellation.clone()) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            failure.status().clone().into_response(Vec::new())
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
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    match send.write_response(&response).await {
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

/// Rejects an already-decoded RPC because the connection stream limit was reached.
pub(crate) async fn reject_stream<W, R>(
    server: Server,
    mut send: W,
    mut recv: R,
    request: RpcRequest,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let status = Status::unavailable("too many concurrent streams on connection");
    server.record_rejected_request(&request, &status);
    if write_rpc_status(&mut send, &request, status).await {
        if request.rpc_kind() == RpcKind::Unary {
            drain_unary_request_end_or_stop(&server, &mut recv).await;
        }
        let _ = send.finish().await;
    }
}

async fn drain_unary_request_end_or_stop<R>(server: &Server, recv: &mut R)
where
    R: RequestPumpReader,
{
    let read = recv.read_unary_end();
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

#[allow(clippy::too_many_lines)]
async fn handle_streaming_rpc<W, R>(
    server: Server,
    mut send: W,
    recv: R,
    request: RpcRequest,
    cancellation: CancellationToken,
    mut shutdown: watch::Receiver<bool>,
) where
    W: ResponseWriter,
    R: RequestPumpReader,
{
    let max_frame_size = server.max_frame_size();
    let request_for_failure = request.clone();
    let (request_body, mut request_pump) = start_request_pump(
        recv,
        RequestInputKind::for_rpc_kind(
            request.rpc_kind(),
            server.options().max_stream_messages(),
            server.options().max_stream_body_size(),
        ),
        max_frame_size,
    );
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request_with_cancellation(
            request,
            request_body,
            cancellation.clone(),
        ) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                send.reset();
                let _ = request_pump.settle(RequestPumpSettle::ResponseStopped).await;
                return;
            }
            crate::stream::from_iter([RpcStreamFrame::status(failure.status().clone())])
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
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    let mut message_batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
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
                Some(Ok(RpcStreamFrame::status(failure.status().clone())))
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
                let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
                return;
            }
        };

        let Some(frame) = frame else {
            break;
        };

        let frame = match frame {
            Ok(frame) => frame,
            Err(error) => RpcStreamFrame::status(error.into_status()),
        };

        if is_plain_message_frame(&frame) {
            message_batch.push(frame.body);
            let mut next_frame = None;
            while message_batch.len() < MESSAGE_FRAME_BATCH {
                match response.next().now_or_never() {
                    Some(Some(Ok(frame))) if is_plain_message_frame(&frame) => {
                        message_batch.push(frame.body);
                    }
                    Some(Some(Ok(frame))) => {
                        next_frame = Some(frame);
                        break;
                    }
                    Some(Some(Err(error))) => {
                        next_frame = Some(RpcStreamFrame::status(error.into_status()));
                        break;
                    }
                    Some(None) | None => break,
                }
            }

            if let Err(error) = send.write_message_stream_frames(&mut message_batch).await {
                cancel_from_transport_error(&cancellation, &error);
                let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
                return;
            }

            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if let Err(error) = send.write_stream_frame(frame).await {
                cancel_from_transport_error(&cancellation, &error);
                let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
                return;
            }
            if is_status {
                break;
            }
            continue;
        }

        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
        if let Err(error) = send.write_stream_frame(frame).await {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            return;
        }

        if is_status {
            break;
        }
    }

    let _ = send.finish().await;
    let _ = request_pump
        .settle(RequestPumpSettle::ResponseCommitted)
        .await;
}

async fn write_rpc_status<W>(send: &mut W, request: &RpcRequest, status: Status) -> bool
where
    W: ResponseWriter,
{
    let result = if request.rpc_kind() == RpcKind::Unary {
        send.write_response(&status.into_response(Vec::new())).await
    } else {
        send.write_stream_frame(RpcStreamFrame::status(status))
            .await
    };
    result.is_ok()
}

pub(crate) async fn write_status<W>(mut send: W, status: Status)
where
    W: ResponseWriter,
{
    let response = status.into_response(Vec::new());

    if send.write_response(&response).await.is_ok() {
        let _ = send.finish().await;
    }
}

#[allow(dead_code)]
pub(crate) struct Permit(Option<OwnedSemaphorePermit>);

pub(crate) fn try_acquire_permit(limit: Option<&Arc<Semaphore>>) -> Option<Permit> {
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
    use super::try_acquire_permit;
    use std::sync::Arc;
    use tokio::sync::Semaphore;

    #[test]
    fn permit_admission_preserves_unlimited_and_bounded_behavior() {
        assert!(try_acquire_permit(None).is_some());
        let semaphore = Arc::new(Semaphore::new(1));
        let permit = try_acquire_permit(Some(&semaphore));
        assert!(permit.is_some());
        assert!(try_acquire_permit(Some(&semaphore)).is_none());
    }
}
