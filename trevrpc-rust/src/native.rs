//! Server side of native `TrevRPC` over QUIC (ALPN `trevrpc/1`): one RPC per bidirectional stream.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::framed::{self, FrameRead};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::serve::{drain_streams, log_task_error};
use crate::server::Server;
use crate::server_transport::{self, ResponseWriter, try_acquire_permit};
use crate::{Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

/// Application error code for streams the server abandons.
const CANCELLED_STREAM_CODE: u32 = 1;

struct QuinnResponseWriter {
    send: quinn::SendStream,
    max_frame_size: usize,
}

impl QuinnResponseWriter {
    const fn new(send: quinn::SendStream, max_frame_size: usize) -> Self {
        Self {
            send,
            max_frame_size,
        }
    }
}

impl ResponseWriter for QuinnResponseWriter {
    async fn write_response(&mut self, response: &RpcResponse) -> Result<()> {
        framed::write_frame(&mut self.send, response, self.max_frame_size).await
    }

    async fn write_message_stream_frames(&mut self, bodies: &mut Vec<Vec<u8>>) -> Result<()> {
        framed::write_message_stream_frames(&mut self.send, bodies, self.max_frame_size).await
    }

    async fn write_stream_frame(&mut self, frame: RpcStreamFrame) -> Result<()> {
        framed::write_stream_frame(&mut self.send, frame, self.max_frame_size).await
    }

    async fn stopped(&mut self) -> Result<()> {
        self.send
            .stopped()
            .await
            .map(|_| ())
            .map_err(Error::transport)
    }

    fn reset(&mut self) {
        let _ = self.send.reset(CANCELLED_STREAM_CODE.into());
    }

    async fn finish(&mut self) -> Result<()> {
        self.send.finish().map_err(Error::transport)
    }
}

/// How often a backpressured request stream with unread buffered data is re-checked for a reset.
const RESET_PROBE_INTERVAL: Duration = Duration::from_millis(250);

struct QuinnPumpReader {
    recv: quinn::RecvStream,
    connection: quinn::Connection,
}

impl QuinnPumpReader {
    const fn new(recv: quinn::RecvStream, connection: quinn::Connection) -> Self {
        Self { recv, connection }
    }
}

/// Waits for the peer to reset `recv` without consuming any of its data, or never if it cannot
/// tell.
///
/// This deliberately avoids `quinn::RecvStream::received_reset`. Quinn 0.11.12 registers the
/// stream's blocked-reader waker from it and clears that waker only on the next stream event, a
/// blocked read, or a drop before EOF. When the whole stream has already arrived, nothing clears
/// it: reading to EOF then leaves a stale waker, which trips the `debug_assert!` in
/// `RecvStream::drop` and, in release builds, keeps the pump task allocated until the
/// connection closes. Fixed on quinn's main branch by commit cdd7940 ("cleanup stale wakers in
/// `blocked_readers`"), which no release includes yet.
///
/// A zero-length chunk read is the one public probe that only registers a waker when the stream
/// is genuinely blocked, and such a waker is cleared by the data or FIN that unblocks it. It
/// returns a reset at once, and an empty chunk when unread data is buffered. A reset discards
/// buffered data, so a buffered stream has not been reset yet, but it cannot wake this probe when
/// it is. That case is re-checked on a timer instead.
async fn peer_reset(recv: &mut quinn::RecvStream) -> Option<RequestTransportEvent> {
    loop {
        match recv.read_chunk(0, true).await {
            Ok(Some(_)) => tokio::time::sleep(RESET_PROBE_INTERVAL).await,
            Err(quinn::ReadError::Reset(code)) => {
                return Some(RequestTransportEvent::PeerReset(Status::cancelled(
                    format!(
                        "request stream reset by peer with code {}",
                        code.into_inner()
                    ),
                )));
            }
            Err(
                error @ (quinn::ReadError::ConnectionLost(_) | quinn::ReadError::ZeroRttRejected),
            ) => {
                return Some(RequestTransportEvent::connection_lost(error));
            }
            // The request ended cleanly, was stopped locally, or cannot be probed: the reads that
            // follow report whatever remains.
            Ok(None)
            | Err(quinn::ReadError::ClosedStream | quinn::ReadError::IllegalOrderedRead) => {
                return std::future::pending().await;
            }
        }
    }
}

impl FrameRead for QuinnPumpReader {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.recv.read_frame_bytes(bytes).await
    }
}

impl RequestPumpReader for QuinnPumpReader {
    fn stop_trevrpc(&mut self) {
        let _ = self.recv.stop(CANCELLED_STREAM_CODE.into());
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        framed::drain_unary_request_end(&mut self.recv).await
    }

    async fn read_stream_frame_or_eof(
        &mut self,
        max_frame_size: usize,
    ) -> Result<Option<RpcStreamFrame>> {
        framed::read_stream_frame_or_eof(&mut self.recv, max_frame_size).await
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        tokio::select! {
            biased;
            event = peer_reset(&mut self.recv) => event,
            closed = self.connection.closed() => Some(RequestTransportEvent::connection_lost(closed)),
        }
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Serves one native connection until it closes or the server shuts down.
pub(crate) async fn handle_connection(
    server: Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        if *shutdown.borrow() {
            break;
        }
        tokio::select! {
            accepted = connection.accept_bi(), if !*shutdown.borrow() => {
                let Ok((send, recv)) = accepted else {
                    break;
                };

                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    stream_tasks.spawn(reject_stream(
                        server.clone(),
                        connection.clone(),
                        send,
                        recv,
                    ));
                    continue;
                };

                let server = server.clone();
                let stream_connection = connection.clone();
                let request_limit = request_limit.clone();
                let shutdown = shutdown.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_stream(server, stream_connection, request_limit, send, recv, shutdown)
                        .await;
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                log_task_error(result, "native stream task failed");
            }
        }
    }

    drain_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
        &connection,
    )
    .await;

    if *shutdown.borrow() {
        connection.close(0_u32.into(), b"server drained connection");
    }
}

/// Reads the initial request frame, replying with its failure status when it is unusable.
async fn read_request(
    server: &Server,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
) -> Option<(QuinnResponseWriter, QuinnPumpReader, RpcRequest)> {
    let max_frame_size = server.max_frame_size();
    let mut recv = QuinnPumpReader::new(recv, connection);
    match server_transport::read_initial_request(server, &mut recv.recv).await {
        Ok(request) => Some((
            QuinnResponseWriter::new(send, max_frame_size),
            recv,
            request,
        )),
        Err(error) => {
            recv.stop_trevrpc();
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(QuinnResponseWriter::new(send, max_frame_size), status)
                .await;
            None
        }
    }
}

async fn handle_stream(
    server: Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
    shutdown: watch::Receiver<bool>,
) {
    if let Some((send, recv, request)) = read_request(&server, connection, send, recv).await {
        server_transport::handle_stream(server, request_limit, send, recv, request, shutdown).await;
    }
}

async fn reject_stream(
    server: Server,
    connection: quinn::Connection,
    send: quinn::SendStream,
    recv: quinn::RecvStream,
) {
    if let Some((send, recv, request)) = read_request(&server, connection, send, recv).await {
        server_transport::reject_stream(server, send, recv, request).await;
    }
}
