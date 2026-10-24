//! Native `TrevRPC` (ALPN `trevrpc/1` over QUIC, and `QMux`): one RPC per bidirectional stream.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;

use crate::datagram::Demux;
use crate::framed::{self, FrameRead, ResponseCodec};
use crate::mux::{CANCELLED_STREAM_CODE, MuxConnection, MuxRecvStream, MuxSendStream};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::serve::{drain_streams, log_task_error};
use crate::server::Server;
use crate::server_transport::{
    self, IncomingRequest, Permit, ResponseWriter, open_flow, reject_unreadable, try_acquire_permit,
};
use crate::{ResponseFrame, Result};

struct NativeResponseWriter<S> {
    send: S,
    max_frame_size: usize,
    codec: ResponseCodec,
}

impl<S: MuxSendStream> NativeResponseWriter<S> {
    fn new(send: S, max_frame_size: usize) -> Self {
        Self {
            send,
            max_frame_size,
            codec: ResponseCodec::default(),
        }
    }
}

impl<S: MuxSendStream> ResponseWriter for NativeResponseWriter<S> {
    async fn write_frames(&mut self, frames: &mut Vec<ResponseFrame>) -> Result<()> {
        framed::write_response_frames(&mut self.send, frames, self.max_frame_size, &mut self.codec)
            .await
    }

    fn set_codec(&mut self, codec: ResponseCodec) {
        self.codec = codec;
    }

    async fn stopped(&mut self) -> Result<()> {
        self.send.peer_stopped().await
    }

    fn reset(&mut self) {
        self.send.reset_stream(CANCELLED_STREAM_CODE);
    }

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(self.send.finish_stream())
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

    async fn read_payload_bytes(&mut self, len: usize) -> Result<Bytes> {
        self.recv.read_payload_bytes(len).await
    }
}

impl<C: MuxConnection> RequestPumpReader for NativePumpReader<C> {
    fn stop_trevrpc(&mut self) {
        self.recv.stop_stream(CANCELLED_STREAM_CODE);
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

    fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send {
        std::future::ready(Ok(()))
    }
}

/// Serves one native connection until it closes or the server shuts down.
pub async fn handle_connection<C: MuxConnection>(
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
    let demux = connection.datagrams().map(|transport| {
        Arc::new(Demux::server(
            transport,
            server.options().max_concurrent_streams_per_connection(),
            server.max_frame_size(),
        ))
    });

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
                stream_tasks.spawn(handle_stream(
                    server,
                    stream_connection,
                    demux.clone(),
                    request_limit,
                    stream_permit,
                    send,
                    recv,
                    shutdown,
                ));
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

/// Reads the initial request frame and opens its datagram flow on `demux`, replying with the
/// failure status when either fails. The stream slot is released before that reply.
async fn read_request<C: MuxConnection>(
    server: &Server,
    stream_permit: &mut Permit,
    connection: C,
    demux: Option<&Demux>,
    send: C::SendStream,
    recv: C::RecvStream,
) -> Option<(
    NativeResponseWriter<C::SendStream>,
    NativePumpReader<C>,
    IncomingRequest,
)> {
    let max_frame_size = server.max_frame_size();
    let mut recv = NativePumpReader::new(recv, connection);
    let incoming = server_transport::read_initial_request(server, &mut recv.recv)
        .await
        .and_then(|mut incoming| open_flow(demux, &mut incoming).map(|()| incoming));
    match incoming {
        Ok(incoming) => Some((
            NativeResponseWriter::new(send, max_frame_size),
            recv,
            incoming,
        )),
        Err(error) => {
            recv.stop_trevrpc();
            reject_unreadable(
                server,
                stream_permit,
                NativeResponseWriter::new(send, max_frame_size),
                error,
            )
            .await;
            None
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_stream<C: MuxConnection>(
    server: Server,
    connection: C,
    demux: Option<Arc<Demux>>,
    request_limit: Option<Arc<Semaphore>>,
    mut stream_permit: Permit,
    send: C::SendStream,
    recv: C::RecvStream,
    shutdown: watch::Receiver<bool>,
) {
    if let Some((send, recv, incoming)) = read_request(
        &server,
        &mut stream_permit,
        connection,
        demux.as_deref(),
        send,
        recv,
    )
    .await
    {
        server_transport::handle_stream(
            server,
            request_limit,
            stream_permit,
            send,
            recv,
            incoming,
            shutdown,
        )
        .await;
    }
}

async fn reject_stream<C: MuxConnection>(
    server: Server,
    connection: C,
    send: C::SendStream,
    recv: C::RecvStream,
) {
    let mut no_permit = Permit::none();
    if let Some((send, recv, incoming)) =
        read_request(&server, &mut no_permit, connection, None, send, recv).await
    {
        server_transport::reject_stream(server, send, recv, incoming).await;
    }
}
