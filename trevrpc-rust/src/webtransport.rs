use std::future::{Future, pending};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;

use crate::advanced::RawWebTransport;
use crate::client::{RpcTransport, StreamingRpcTransport};
use crate::client_upload::UploadWriter;
use crate::framed::{self, FrameRead, FrameWrite, MESSAGE_FRAME_BATCH, NoopFrameTrace};
use crate::request_pump::{
    RequestInputKind, RequestPumpReader, RequestPumpSettle, RequestTransportEvent,
    start_request_pump,
};
use crate::server::{CancellationSource, CancellationToken};
use crate::{
    BoxStream, Error, Result, RpcKind, RpcRequest, RpcResponse, RpcStreamFrame, RpcStreamFrameKind,
    Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;

type ServerEndpoint = web_transport_quinn::Server;

impl FrameWrite for web_transport_quinn::SendStream {
    async fn write_frame_bytes(&mut self, bytes: &[u8]) -> Result<()> {
        self.write_all(bytes).await.map_err(Error::transport)
    }

    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        self.write_all_chunks(chunks)
            .await
            .map_err(Error::transport)
    }
}

impl FrameRead for web_transport_quinn::RecvStream {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.read(bytes).await.map_err(Error::transport)
    }
}

struct WebTransportPumpReader(web_transport_quinn::RecvStream);

impl FrameRead for WebTransportPumpReader {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        self.0.read(bytes).await.map_err(Error::transport)
    }
}

impl RequestPumpReader for WebTransportPumpReader {
    fn stop_trevrpc(&mut self) {
        let _ = self.0.stop(cancelled_stream_code());
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        match self.0.received_reset().await {
            Ok(Some(code)) => Some(RequestTransportEvent::PeerReset(Status::cancelled(
                format!("request stream reset by peer with code {code}"),
            ))),
            Ok(None) => std::future::pending().await,
            Err(error) => Some(RequestTransportEvent::ConnectionLost(
                Error::transport(error).into_status(),
            )),
        }
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

#[crate::async_trait]
impl RpcTransport for RawWebTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.session().open_bi().await.map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv);

        write_frame(streams.send_mut(), &request, self.max_frame_size()).await?;
        streams.send_mut().finish().map_err(Error::transport)?;

        let response = read_frame(streams.recv_mut(), self.max_frame_size()).await?;
        streams.complete();

        Ok(response)
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for RawWebTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let (send, recv) = self.session().open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size();
        let writer = UploadWriter::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(webtransport_response_stream(
            recv,
            writer,
            self.max_frame_size(),
        ))
    }
}

struct CancellableBiStream {
    send: Option<web_transport_quinn::SendStream>,
    recv: Option<web_transport_quinn::RecvStream>,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: web_transport_quinn::SendStream, recv: web_transport_quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }

    fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CancellableBiStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            let _ = send.reset(cancelled_stream_code());
        }

        if let Some(recv) = &mut self.recv {
            let _ = recv.stop(cancelled_stream_code());
        }
    }
}

struct CancellableSendStream {
    send: Option<web_transport_quinn::SendStream>,
    complete: bool,
}

impl CancellableSendStream {
    fn new(send: web_transport_quinn::SendStream) -> Self {
        Self {
            send: Some(send),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut web_transport_quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn complete(mut self) {
        self.complete = true;
    }
}

impl Drop for CancellableSendStream {
    fn drop(&mut self) {
        if self.complete {
            return;
        }

        if let Some(send) = &mut self.send {
            let _ = send.reset(cancelled_stream_code());
        }
    }
}

struct WebTransportResponseStream {
    recv: Option<web_transport_quinn::RecvStream>,
    writer: UploadWriter,
    max_frame_size: usize,
    complete: bool,
}

impl WebTransportResponseStream {
    const fn new(
        recv: web_transport_quinn::RecvStream,
        writer: UploadWriter,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            writer,
            max_frame_size,
            complete: false,
        }
    }

    fn recv_mut(&mut self) -> &mut web_transport_quinn::RecvStream {
        self.recv
            .as_mut()
            .expect("recv stream should be present until completion")
    }
}

fn webtransport_response_stream(
    recv: web_transport_quinn::RecvStream,
    writer: UploadWriter,
    max_frame_size: usize,
) -> BoxStream<RpcStreamFrame> {
    Box::pin(futures_util::stream::unfold(
        WebTransportResponseStream::new(recv, writer, max_frame_size),
        |mut stream| async move {
            if stream.complete {
                return None;
            }

            let max_frame_size = stream.max_frame_size;
            let item = match read_stream_frame_or_eof(stream.recv_mut(), max_frame_size).await {
                Ok(Some(frame)) if frame.frame_kind() == Some(RpcStreamFrameKind::Status) => {
                    let status = frame.status_value();
                    let writer_result = stream.writer.abort_and_settle().await;
                    let drain_result = drain_fin_after_terminal_status(
                        stream.recv_mut(),
                        max_frame_size,
                        "response stream",
                    )
                    .await;
                    stream.complete = true;
                    if let Err(error) = drain_result {
                        Err(error)
                    } else if status.is_ok() {
                        writer_result.map(|()| frame)
                    } else {
                        Ok(frame)
                    }
                }
                Ok(Some(frame)) => Ok(frame),
                Ok(None) => {
                    let _ = stream.writer.abort_and_settle().await;
                    return None;
                }
                Err(error) => {
                    let _ = stream.writer.abort_and_settle().await;
                    stream.complete = true;
                    Err(error)
                }
            };

            Some((item, stream))
        },
    ))
}

impl Drop for WebTransportResponseStream {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            let _ = recv.stop(cancelled_stream_code());
        }
    }
}

/// Writes a length-prefixed protobuf frame to a `WebTransport` send stream.
pub async fn write_frame<M>(
    send: &mut web_transport_quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    framed::write_frame::<_, NoopFrameTrace, M>(send, message, max_frame_size).await
}

async fn write_message_stream_frames(
    send: &mut web_transport_quinn::SendStream,
    bodies: &mut Vec<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_message_stream_frames::<_, NoopFrameTrace>(send, bodies, max_frame_size).await
}

async fn write_stream_frame(
    send: &mut web_transport_quinn::SendStream,
    frame: RpcStreamFrame,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_stream_frame::<_, NoopFrameTrace>(send, frame, max_frame_size).await
}

fn is_plain_message_frame(frame: &RpcStreamFrame) -> bool {
    framed::is_plain_message_frame(frame)
}

/// Reads and decodes one length-prefixed protobuf frame from a `WebTransport` receive stream.
pub async fn read_frame<M>(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<M>
where
    M: Message + Default + 'static,
{
    framed::read_frame::<_, NoopFrameTrace, M>(recv, max_frame_size).await
}

async fn read_stream_frame_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    framed::read_stream_frame_or_eof::<_, NoopFrameTrace>(recv, max_frame_size).await
}

async fn drain_fin_after_terminal_status(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
    stream_name: &'static str,
) -> Result<()> {
    framed::drain_fin_after_terminal_status::<_, NoopFrameTrace>(recv, max_frame_size, stream_name)
        .await
}

async fn write_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    write_request_body_frames(send.send_mut(), &mut request_body, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut web_transport_quinn::SendStream,
    request_body: &mut BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    framed::write_request_body_frames::<_, NoopFrameTrace>(send, request_body, max_frame_size).await
}

impl crate::server::Server {
    /// Serves `TrevRPC` over a `WebTransport` endpoint until the endpoint stops accepting requests.
    pub async fn serve_webtransport(self, endpoint: ServerEndpoint) -> Result<()> {
        self.serve_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

    /// Serves `TrevRPC` over a `WebTransport` endpoint until the shutdown future completes.
    pub async fn serve_webtransport_with_shutdown<S>(
        self,
        mut endpoint: ServerEndpoint,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        let connection_limit = self
            .options()
            .max_concurrent_connections()
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let request_limit = self
            .options()
            .max_concurrent_requests()
            .map(|limit| Arc::new(Semaphore::new(limit)));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut connection_tasks = JoinSet::new();

        tokio::pin!(shutdown);

        loop {
            tokio::select! {
                request = endpoint.accept() => {
                    let Some(request) = request else {
                        break;
                    };

                    if let Some(status) = validate_request(&self, &request) {
                        let _ = request.reject(status).await;
                        continue;
                    }

                    let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                        let _ = request.reject(web_transport_quinn::http::StatusCode::SERVICE_UNAVAILABLE).await;
                        continue;
                    };

                    let server = self.clone();
                    let request_limit = request_limit.clone();
                    let shutdown = shutdown_rx.clone();
                    connection_tasks.spawn(async move {
                        let _connection_permit = connection_permit;
                        if let Ok(session) = request.ok().await
                        {
                            handle_session(server, session, request_limit, shutdown).await;
                        }
                    });
                }
                () = &mut shutdown => {
                    let _ = shutdown_tx.send(true);
                    break;
                }
                result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    if let Some(Err(error)) = result {
                        let _ = &error;
                        #[cfg(feature = "tracing")]
                        tracing::warn!(%error, "WebTransport connection task failed");
                    }
                }
            }
        }

        let _ = shutdown_tx.send(true);
        drain_connections(
            &mut connection_tasks,
            self.options().graceful_shutdown_timeout(),
            endpoint,
        )
        .await;

        Ok(())
    }
}

pub(crate) fn validate_request(
    server: &crate::server::Server,
    request: &web_transport_quinn::Request,
) -> Option<web_transport_quinn::http::StatusCode> {
    let origin = request
        .headers
        .get(web_transport_quinn::http::header::ORIGIN)
        .and_then(|origin| origin.to_str().ok());
    let authority = request.url.host_str().map(|host| {
        request
            .url
            .port()
            .map_or_else(|| host.to_owned(), |port| format!("{host}:{port}"))
    });
    let headers = request
        .headers
        .iter()
        .map(|(name, value)| crate::server::AdmissionHeader {
            name: name.as_str(),
            value: value.as_bytes(),
        })
        .collect::<Vec<_>>();
    validate_admission(
        server,
        request.url.path(),
        authority.as_deref(),
        origin,
        request.url.scheme() == "https",
        &headers,
    )
}

pub(crate) fn validate_admission(
    server: &crate::server::Server,
    path: &str,
    authority: Option<&str>,
    origin: Option<&str>,
    secure: bool,
    headers: &[crate::server::AdmissionHeader<'_>],
) -> Option<web_transport_quinn::http::StatusCode> {
    let options = server.options();
    if let Some(admission) = options.webtransport_admission() {
        let admission_request = crate::server::WebTransportAdmissionRequest {
            path,
            authority,
            origin,
            secure,
            headers,
        };
        return (!admission(&admission_request))
            .then_some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }
    if path != options.webtransport_path() {
        return Some(web_transport_quinn::http::StatusCode::NOT_FOUND);
    }
    let allowed_authorities = options.webtransport_allowed_authorities();
    if !allowed_authorities.is_empty()
        && !authority.is_some_and(|authority| {
            allowed_authorities
                .iter()
                .any(|allowed| allowed == authority || allowed == authority_host(authority))
        })
    {
        return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }
    if origin.is_some_and(|origin| {
        !options
            .webtransport_allowed_origins()
            .iter()
            .any(|allowed| allowed == origin)
    }) {
        return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }
    None
}

fn authority_host(authority: &str) -> &str {
    if let Some(authority) = authority.strip_prefix('[')
        && let Some((host, _)) = authority.split_once(']')
    {
        return host;
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

pub(crate) async fn handle_session(
    server: crate::server::Server,
    session: web_transport_quinn::Session,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = session.accept_bi(), if !*shutdown.borrow() => {
                let Ok((send, recv)) = accepted else {
                    break;
                };

                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    write_status(
                        send,
                        Status::unavailable("too many concurrent streams on WebTransport session"),
                        server.max_frame_size(),
                    )
                    .await;
                    continue;
                };

                let server = server.clone();
                let request_limit = request_limit.clone();
                let stream_shutdown = shutdown.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_stream(server, request_limit, send, recv, stream_shutdown).await;
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "WebTransport stream task failed");
                }
            }
        }
    }

    drain_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
        &session,
    )
    .await;

    if *shutdown.borrow() {
        session.close(
            cancelled_stream_code(),
            b"server drained WebTransport connection",
        );
    }
}

async fn handle_stream(
    server: crate::server::Server,
    request_limit: Option<Arc<Semaphore>>,
    mut send: web_transport_quinn::SendStream,
    mut recv: web_transport_quinn::RecvStream,
    mut shutdown: watch::Receiver<bool>,
) {
    let request = match read_initial_request(&server, &mut recv).await {
        Ok(request) => request,
        Err(error) => {
            let _ = recv.stop(cancelled_stream_code());
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            write_status(send, status, server.max_frame_size()).await;
            return;
        }
    };

    let Some(_request_permit) = try_acquire_permit(request_limit.as_ref()) else {
        let status = Status::unavailable("too many concurrent RPCs");
        server.record_rejected_request(&request, &status);
        write_rpc_status(send, &request, status, server.max_frame_size()).await;
        return;
    };

    let cancellation = CancellationToken::new();
    if request.rpc_kind() != RpcKind::Unary {
        handle_streaming_rpc(server, send, recv, request, cancellation, shutdown).await;
        return;
    }

    let request_for_failure = request.clone();
    let (_request_body, mut request_pump) = start_request_pump::<_, NoopFrameTrace>(
        WebTransportPumpReader(recv),
        RequestInputKind::Unary,
        server.max_frame_size(),
    );
    let response = tokio::select! {
        biased;
        response = server.handle_request_with_cancellation(request, cancellation.clone()) => response,
        failure = request_pump.failure() => {
            server.record_active_request_failure(&request_for_failure, failure.status());
            if let Some(source) = failure.cancellation_source() {
                cancellation.cancel(source);
            }
            if !failure.response_writable() {
                let _ = send.reset(cancelled_stream_code());
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
            #[cfg(feature = "tracing")]
            tracing::debug!(?stopped, "client stopped WebTransport response stream before RPC completed");
            return;
        }
        changed = shutdown.changed() => {
            let _ = changed;
            cancellation.cancel(CancellationSource::ServerShutdown);
            let _ = request_pump.settle(RequestPumpSettle::ServerShutdown).await;
            return;
        }
    };

    match write_frame(&mut send, &response, server.max_frame_size()).await {
        Ok(()) => {
            let _ = request_pump
                .settle(RequestPumpSettle::ResponseCommitted)
                .await;
            let _ = send.finish();
        }
        Err(error) => {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
        }
    }
}

async fn read_initial_request(
    server: &crate::server::Server,
    recv: &mut web_transport_quinn::RecvStream,
) -> Result<RpcRequest> {
    let read = read_frame::<RpcRequest>(recv, server.max_frame_size());
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

fn cancel_from_transport_error(cancellation: &CancellationToken, error: &Error) {
    if let Some(code) = error.transport_code() {
        cancellation.cancel(if code == crate::Code::Cancelled {
            CancellationSource::PeerReset
        } else {
            CancellationSource::ConnectionLost
        });
    }
}

#[allow(clippy::too_many_lines)]
async fn handle_streaming_rpc(
    server: crate::server::Server,
    mut send: web_transport_quinn::SendStream,
    recv: web_transport_quinn::RecvStream,
    request: RpcRequest,
    cancellation: CancellationToken,
    mut shutdown: watch::Receiver<bool>,
) {
    let max_frame_size = server.max_frame_size();
    let request_for_failure = request.clone();
    let (request_body, mut request_pump) = start_request_pump::<_, NoopFrameTrace>(
        WebTransportPumpReader(recv),
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
                let _ = send.reset(cancelled_stream_code());
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
            #[cfg(feature = "tracing")]
            tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC handler completed");
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
                    let _ = send.reset(cancelled_stream_code());
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
                #[cfg(feature = "tracing")]
                tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC completed");
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

            if let Err(error) =
                write_message_stream_frames(&mut send, &mut message_batch, max_frame_size).await
            {
                cancel_from_transport_error(&cancellation, &error);
                let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
                return;
            }

            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if let Err(error) = write_stream_frame(&mut send, frame, max_frame_size).await {
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

        if let Err(error) = write_stream_frame(&mut send, frame, max_frame_size).await {
            cancel_from_transport_error(&cancellation, &error);
            let _ = request_pump.settle(RequestPumpSettle::ConnectionLost).await;
            return;
        }

        if is_status {
            break;
        }
    }

    let _ = request_pump
        .settle(RequestPumpSettle::ResponseCommitted)
        .await;
    let _ = send.finish();
}

async fn write_rpc_status(
    mut send: web_transport_quinn::SendStream,
    request: &RpcRequest,
    status: Status,
    max_frame_size: usize,
) {
    let result = if request.rpc_kind() == RpcKind::Unary {
        write_frame(&mut send, &status.into_response(Vec::new()), max_frame_size).await
    } else {
        write_frame(&mut send, &RpcStreamFrame::status(status), max_frame_size).await
    };

    if result.is_ok() {
        let _ = send.finish();
    }
}

#[allow(dead_code)]
struct Permit(Option<OwnedSemaphorePermit>);

fn try_acquire_permit(limit: Option<&Arc<Semaphore>>) -> Option<Permit> {
    limit.map_or(Some(Permit(None)), |semaphore| {
        semaphore
            .clone()
            .try_acquire_owned()
            .ok()
            .map(|permit| Permit(Some(permit)))
    })
}

async fn write_status(
    mut send: web_transport_quinn::SendStream,
    status: Status,
    max_frame_size: usize,
) {
    let response = status.into_response(Vec::new());

    if write_frame(&mut send, &response, max_frame_size)
        .await
        .is_ok()
    {
        let _ = send.finish();
    }
}

async fn drain_streams(
    stream_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    session: &web_transport_quinn::Session,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_stream_tasks(stream_tasks))
            .await
            .is_err()
        {
            session.close(
                cancelled_stream_code(),
                b"server WebTransport stream drain timed out",
            );
            stream_tasks.abort_all();
            while stream_tasks.join_next().await.is_some() {}
        }
    } else {
        drain_stream_tasks(stream_tasks).await;
    }
}

async fn drain_stream_tasks(stream_tasks: &mut JoinSet<()>) {
    while let Some(result) = stream_tasks.join_next().await {
        if let Err(error) = result {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::warn!(%error, "WebTransport stream task failed while draining");
        }
    }
}

async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    endpoint: ServerEndpoint,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_connection_tasks(connection_tasks))
            .await
            .is_err()
        {
            endpoint.close(0_u32.into(), b"server graceful shutdown timed out");
            connection_tasks.abort_all();
            while connection_tasks.join_next().await.is_some() {}
        } else {
            endpoint.close(0_u32.into(), b"server shutdown complete");
        }
    } else {
        drain_connection_tasks(connection_tasks).await;
        endpoint.close(0_u32.into(), b"server shutdown complete");
    }
}

async fn drain_connection_tasks(connection_tasks: &mut JoinSet<()>) {
    while let Some(result) = connection_tasks.join_next().await {
        if let Err(error) = result {
            let _ = &error;
            #[cfg(feature = "tracing")]
            tracing::warn!(%error, "WebTransport connection task failed while draining");
        }
    }
}

fn cancelled_stream_code() -> u32 {
    CANCELLED_STREAM_CODE
}
