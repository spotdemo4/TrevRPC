//! Native `TrevRPC` (ALPN `trevrpc/1` over QUIC, and `QMux`): one RPC per bidirectional stream.

use std::sync::Arc;

use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::framed::{self, FrameRead};
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::serve::{drain_streams, log_task_error};
use crate::server::Server;
use crate::server_transport::{self, ResponseWriter, try_acquire_permit};
use crate::{Result, RpcRequest, RpcResponse, RpcStreamFrame};

struct NativeResponseWriter<S> {
    send: S,
    max_frame_size: usize,
}

impl<S: MuxSendStream> NativeResponseWriter<S> {
    const fn new(send: S, max_frame_size: usize) -> Self {
        Self {
            send,
            max_frame_size,
        }
    }
}

impl<S: MuxSendStream> ResponseWriter for NativeResponseWriter<S> {
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
        self.send.peer_stopped().await
    }

    fn reset(&mut self) {
        self.send.reset_stream(CANCELLED_STREAM_CODE);
    }

    async fn finish(&mut self) -> Result<()> {
        self.send.finish_stream()
    }
}

struct NativePumpReader<C: MuxConnection> {
    recv: C::RecvStream,
    connection: C,
}

impl<C: MuxConnection> NativePumpReader<C> {
    const fn new(recv: C::RecvStream, connection: C) -> Self {
        Self { recv, connection }
    }
}

impl<C: MuxConnection> FrameRead for NativePumpReader<C> {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.recv.read_frame_bytes(bytes).await
    }
}

impl<C: MuxConnection> RequestPumpReader for NativePumpReader<C> {
    fn stop_trevrpc(&mut self) {
        self.recv.stop_stream(CANCELLED_STREAM_CODE);
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
            event = self.recv.peer_reset() => event,
            error = self.connection.connection_lost() => {
                Some(RequestTransportEvent::ConnectionLost(error.into_status()))
            }
        }
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Serves one native connection until it closes or the server shuts down.
pub(crate) async fn handle_connection<C: MuxConnection>(
    server: Server,
    connection: C,
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
            accepted = connection.accept_stream(), if !*shutdown.borrow() => {
                let Some((send, recv)) = accepted else {
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
        || connection.close_connection(b"server stream drain timed out"),
    )
    .await;

    if *shutdown.borrow() {
        connection.close_connection(b"server drained connection");
    }
}

/// Reads the initial request frame, replying with its failure status when it is unusable.
async fn read_request<C: MuxConnection>(
    server: &Server,
    connection: C,
    send: C::SendStream,
    recv: C::RecvStream,
) -> Option<(
    NativeResponseWriter<C::SendStream>,
    NativePumpReader<C>,
    RpcRequest,
)> {
    let max_frame_size = server.max_frame_size();
    let mut recv = NativePumpReader::new(recv, connection);
    match server_transport::read_initial_request(server, &mut recv.recv).await {
        Ok(request) => Some((
            NativeResponseWriter::new(send, max_frame_size),
            recv,
            request,
        )),
        Err(error) => {
            recv.stop_trevrpc();
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(NativeResponseWriter::new(send, max_frame_size), status)
                .await;
            None
        }
    }
}

async fn handle_stream<C: MuxConnection>(
    server: Server,
    connection: C,
    request_limit: Option<Arc<Semaphore>>,
    send: C::SendStream,
    recv: C::RecvStream,
    shutdown: watch::Receiver<bool>,
) {
    if let Some((send, recv, request)) = read_request(&server, connection, send, recv).await {
        server_transport::handle_stream(server, request_limit, send, recv, request, shutdown).await;
    }
}

async fn reject_stream<C: MuxConnection>(
    server: Server,
    connection: C,
    send: C::SendStream,
    recv: C::RecvStream,
) {
    if let Some((send, recv, request)) = read_request(&server, connection, send, recv).await {
        server_transport::reject_stream(server, send, recv, request).await;
    }
}
