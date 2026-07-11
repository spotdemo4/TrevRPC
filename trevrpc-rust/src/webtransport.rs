use std::future::{Future, pending};
use std::sync::Arc;

use bytes::Bytes;
use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::advanced::RawWebTransport;
use crate::client::RpcTransport;
use crate::framed::{self, FrameRead, FrameWrite, MESSAGE_FRAME_BATCH, NoopFrameTrace};
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, RpcStreamFrameKind, Status,
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

    async fn read_exact_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.read_exact(bytes).await.map_err(Error::transport)
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

    async fn streaming_call(
        &self,
        request: RpcRequest,
        mut request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        let (send, recv) = self.session().open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size();

        if request_body.is_non_blocking() {
            match request_body.next().await {
                None => {
                    write_empty_streaming_request(send, request, max_frame_size).await?;
                    return Ok(Box::new(WebTransportResponseStream::new(
                        recv,
                        None,
                        self.max_frame_size(),
                    )));
                }
                Some(first) => {
                    request_body = crate::stream::prefixed(first, request_body);
                }
            }
        }

        let write_task = tokio::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(Box::new(WebTransportResponseStream::new(
            recv,
            Some(write_task),
            self.max_frame_size(),
        )))
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
    write_task: Option<JoinHandle<Result<()>>>,
    max_frame_size: usize,
    complete: bool,
}

impl WebTransportResponseStream {
    const fn new(
        recv: web_transport_quinn::RecvStream,
        write_task: Option<JoinHandle<Result<()>>>,
        max_frame_size: usize,
    ) -> Self {
        Self {
            recv: Some(recv),
            write_task,
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

#[crate::async_trait]
impl MessageStream<RpcStreamFrame> for WebTransportResponseStream {
    async fn next(&mut self) -> Option<Result<RpcStreamFrame>> {
        if self.complete {
            return None;
        }

        let max_frame_size = self.max_frame_size;
        match read_stream_frame_or_eof(self.recv_mut(), max_frame_size).await {
            Ok(Some(frame)) => {
                if frame.frame_kind() == Some(RpcStreamFrameKind::Status) {
                    self.complete = true;
                    if frame.status_value().is_ok() {
                        if let Err(error) = self.stop_writer(true).await {
                            return Some(Err(error));
                        }
                    } else {
                        self.ignore_writer_error();
                    }
                }
                Some(Ok(frame))
            }
            Ok(None) => {
                self.complete = true;
                if let Err(error) = self.stop_writer(false).await {
                    return Some(Err(error));
                }
                None
            }
            Err(error) => {
                self.complete = true;
                let _ = self.stop_writer(true).await;
                Some(Err(error))
            }
        }
    }
}

impl WebTransportResponseStream {
    async fn stop_writer(&mut self, ignore_cancelled: bool) -> Result<()> {
        let Some(write_task) = self.write_task.take() else {
            return Ok(());
        };

        if !write_task.is_finished() {
            write_task.abort();
        }

        match write_task.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) if ignore_cancelled && is_cancelled_error(&error) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(error) if ignore_cancelled && error.is_cancelled() => Ok(()),
            Err(error) => Err(Error::transport(error)),
        }
    }

    fn ignore_writer_error(&mut self) {
        if let Some(write_task) = self.write_task.take()
            && !write_task.is_finished()
        {
            write_task.abort();
        }
    }
}

fn is_cancelled_error(error: &Error) -> bool {
    match error {
        Error::Status(status) => status.code() == crate::Code::Cancelled,
        Error::Transport(_) | Error::Encode(_) | Error::Decode(_) | Error::FrameTooLarge { .. } => {
            false
        }
    }
}

impl Drop for WebTransportResponseStream {
    fn drop(&mut self) {
        if !self.complete
            && let Some(recv) = &mut self.recv
        {
            let _ = recv.stop(cancelled_stream_code());
        }

        if let Some(write_task) = &self.write_task {
            write_task.abort();
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
    M: Message + Default,
{
    framed::read_frame::<_, NoopFrameTrace, M>(recv, max_frame_size).await
}

async fn read_stream_frame_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<RpcStreamFrame>> {
    framed::read_stream_frame_or_eof::<_, NoopFrameTrace>(recv, max_frame_size).await
}

async fn write_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxMessageStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    write_request_body_frames(send.send_mut(), &mut request_body, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_empty_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

async fn write_request_body_frames(
    send: &mut web_transport_quinn::SendStream,
    request_body: &mut BoxMessageStream<Vec<u8>>,
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
            &endpoint,
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
            allowed_authorities.contains(&authority)
                || allowed_authorities.contains(&authority_host(authority))
        })
    {
        return Some(web_transport_quinn::http::StatusCode::FORBIDDEN);
    }
    if origin.is_some_and(|origin| !options.webtransport_allowed_origins().contains(&origin)) {
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
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_stream(server, request_limit, send, recv).await;
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

    if request.rpc_kind() != RpcKind::Unary {
        handle_streaming_rpc(server, send, recv, request).await;
        return;
    }

    let response = tokio::select! {
        biased;
        response = server.handle_request(request) => response,
        stopped = send.stopped() => {
            let _ = &stopped;
            #[cfg(feature = "tracing")]
            tracing::debug!(?stopped, "client stopped WebTransport response stream before RPC completed");
            return;
        }
    };

    if write_frame(&mut send, &response, server.max_frame_size())
        .await
        .is_ok()
    {
        if let Err(error) = read_unary_request_end(&server, &mut recv).await {
            let _ = &error;
            let _ = recv.stop(cancelled_stream_code());
            #[cfg(feature = "tracing")]
            tracing::debug!(%error, "failed to drain WebTransport unary request stream");
        }
        let _ = send.finish();
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

async fn read_unary_request_end(
    server: &crate::server::Server,
    recv: &mut web_transport_quinn::RecvStream,
) -> Result<()> {
    let read = drain_unary_request_end(recv);
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read).await.map_err(|_| {
            Error::from(Status::deadline_exceeded(
                "unary request stream finish timeout",
            ))
        })?
    } else {
        read.await
    }
}

async fn drain_unary_request_end(recv: &mut web_transport_quinn::RecvStream) -> Result<()> {
    let mut buf = [0; 1024];
    loop {
        match recv.read(&mut buf).await.map_err(Error::transport)? {
            Some(0) => {}
            Some(_) => {
                return Err(Error::from(Status::invalid_argument(
                    "unary request stream contained data after the initial request frame",
                )));
            }
            None => return Ok(()),
        }
    }
}

async fn handle_streaming_rpc(
    server: crate::server::Server,
    mut send: web_transport_quinn::SendStream,
    recv: web_transport_quinn::RecvStream,
    request: RpcRequest,
) {
    let max_frame_size = server.max_frame_size();
    let request_body = Box::new(WebTransportRequestStream::new(recv, max_frame_size));
    let mut response = tokio::select! {
        biased;
        response = server.handle_streaming_request(request, request_body) => response,
        stopped = send.stopped() => {
            let _ = &stopped;
            #[cfg(feature = "tracing")]
            tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC handler completed");
            return;
        }
    };

    let response_is_non_blocking = response.is_non_blocking();
    let mut message_batch = Vec::with_capacity(MESSAGE_FRAME_BATCH);
    loop {
        let frame = tokio::select! {
            biased;
            frame = response.next() => frame,
            stopped = send.stopped() => {
                let _ = &stopped;
                #[cfg(feature = "tracing")]
                tracing::debug!(?stopped, "client stopped WebTransport response stream before streaming RPC completed");
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

        if response_is_non_blocking && is_plain_message_frame(&frame) {
            message_batch.push(frame.body);
            let mut next_frame = None;
            while message_batch.len() < MESSAGE_FRAME_BATCH {
                match response.next().await {
                    Some(Ok(frame)) if is_plain_message_frame(&frame) => {
                        message_batch.push(frame.body);
                    }
                    Some(Ok(frame)) => {
                        next_frame = Some(frame);
                        break;
                    }
                    Some(Err(error)) => {
                        next_frame = Some(RpcStreamFrame::status(error.into_status()));
                        break;
                    }
                    None => break,
                }
            }

            if write_message_stream_frames(&mut send, &mut message_batch, max_frame_size)
                .await
                .is_err()
            {
                return;
            }

            let Some(frame) = next_frame else {
                continue;
            };
            let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);
            if write_stream_frame(&mut send, frame, max_frame_size)
                .await
                .is_err()
            {
                return;
            }
            if is_status {
                break;
            }
            continue;
        }

        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);

        if write_stream_frame(&mut send, frame, max_frame_size)
            .await
            .is_err()
        {
            return;
        }

        if is_status {
            break;
        }
    }

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

struct WebTransportRequestStream {
    recv: web_transport_quinn::RecvStream,
    max_frame_size: usize,
    done: bool,
}

impl WebTransportRequestStream {
    const fn new(recv: web_transport_quinn::RecvStream, max_frame_size: usize) -> Self {
        Self {
            recv,
            max_frame_size,
            done: false,
        }
    }
}

impl Drop for WebTransportRequestStream {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.recv.stop(cancelled_stream_code());
        }
    }
}

#[crate::async_trait]
impl MessageStream<Vec<u8>> for WebTransportRequestStream {
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done {
            return None;
        }

        match read_stream_frame_or_eof(&mut self.recv, self.max_frame_size).await {
            Ok(Some(frame)) => match frame.frame_kind() {
                Some(RpcStreamFrameKind::Message) => Some(Ok(frame.body)),
                Some(RpcStreamFrameKind::Status) => {
                    self.done = true;
                    let status = frame.status_value();
                    if status.is_ok() {
                        None
                    } else {
                        Some(Err(Error::from(status)))
                    }
                }
                None => {
                    self.done = true;
                    Some(Err(Error::from(Status::invalid_argument(
                        "request stream contained an unknown frame kind",
                    ))))
                }
            },
            Ok(None) => {
                self.done = true;
                None
            }
            Err(error) => {
                self.done = true;
                Some(Err(error))
            }
        }
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
    endpoint: &ServerEndpoint,
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
