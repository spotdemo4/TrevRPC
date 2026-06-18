use std::future::{Future, pending};
use std::io;
use std::sync::Arc;

use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::RpcTransport;
use crate::framing::{DEFAULT_MAX_FRAME_SIZE, decode_frame, encode_frame_with_max, frame_body_len};
use crate::{
    BoxMessageStream, Error, MessageStream, Result, RpcKind, RpcRequest, RpcResponse,
    RpcStreamFrame, RpcStreamFrameKind, Status,
};

const CANCELLED_STREAM_CODE: u32 = 1;

type ServerEndpoint = web_transport_quinn::Server;

#[derive(Clone)]
pub struct WebTransportTransport {
    session: web_transport_quinn::Session,
    max_frame_size: usize,
}

impl WebTransportTransport {
    #[must_use]
    pub const fn new(session: web_transport_quinn::Session) -> Self {
        Self {
            session,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    #[must_use]
    pub const fn session(&self) -> &web_transport_quinn::Session {
        &self.session
    }

    #[must_use]
    pub const fn connection(&self) -> &web_transport_quinn::Session {
        &self.session
    }

    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

#[crate::async_trait]
impl RpcTransport for WebTransportTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv);

        write_frame(streams.send_mut(), &request, self.max_frame_size).await?;
        streams.send_mut().finish().map_err(Error::transport)?;

        let response = read_frame(streams.recv_mut(), self.max_frame_size).await?;
        streams.complete();

        Ok(response)
    }

    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxMessageStream<Vec<u8>>,
    ) -> Result<BoxMessageStream<RpcStreamFrame>> {
        let (send, recv) = self.session.open_bi().await.map_err(Error::transport)?;
        let max_frame_size = self.max_frame_size;
        let write_task = tokio::spawn(async move {
            write_streaming_request(send, request, request_body, max_frame_size).await
        });

        Ok(Box::new(WebTransportResponseStream::new(
            recv,
            write_task,
            self.max_frame_size,
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
    write_task: JoinHandle<Result<()>>,
    max_frame_size: usize,
    complete: bool,
}

impl WebTransportResponseStream {
    const fn new(
        recv: web_transport_quinn::RecvStream,
        write_task: JoinHandle<Result<()>>,
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
        match read_frame_or_eof::<RpcStreamFrame>(self.recv_mut(), max_frame_size).await {
            Ok(Some(frame)) => {
                if frame.frame_kind() == Some(RpcStreamFrameKind::Status) {
                    self.complete = true;
                }
                Some(Ok(frame))
            }
            Ok(None) => {
                self.complete = true;
                None
            }
            Err(error) => {
                self.complete = true;
                Some(Err(error))
            }
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

        self.write_task.abort();
    }
}

pub async fn write_frame<M>(
    send: &mut web_transport_quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    let frame = encode_frame_with_max(message, max_frame_size)?;
    send.write_all(&frame).await.map_err(Error::transport)
}

pub async fn read_frame<M>(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<M>
where
    M: Message + Default,
{
    let mut header = [0; 4];
    recv.read_exact(&mut header)
        .await
        .map_err(Error::transport)?;

    let len = frame_body_len(header, max_frame_size)?;
    let mut body = vec![0; len];
    recv.read_exact(&mut body).await.map_err(Error::transport)?;

    decode_frame(&body)
}

async fn read_frame_or_eof<M>(
    recv: &mut web_transport_quinn::RecvStream,
    max_frame_size: usize,
) -> Result<Option<M>>
where
    M: Message + Default,
{
    let mut header = [0; 4];
    if !read_exact_or_eof(recv, &mut header).await? {
        return Ok(None);
    }

    let len = frame_body_len(header, max_frame_size)?;
    let mut body = vec![0; len];
    read_exact_or_unexpected_eof(recv, &mut body).await?;

    decode_frame(&body).map(Some)
}

async fn read_exact_or_eof(
    recv: &mut web_transport_quinn::RecvStream,
    buf: &mut [u8],
) -> Result<bool> {
    let mut offset = 0;

    while offset < buf.len() {
        match recv
            .read(&mut buf[offset..])
            .await
            .map_err(Error::transport)?
        {
            Some(0) => {}
            Some(read) => offset += read,
            None if offset == 0 => return Ok(false),
            None => return Err(unexpected_eof()),
        }
    }

    Ok(true)
}

async fn read_exact_or_unexpected_eof(
    recv: &mut web_transport_quinn::RecvStream,
    buf: &mut [u8],
) -> Result<()> {
    if read_exact_or_eof(recv, buf).await? {
        Ok(())
    } else {
        Err(unexpected_eof())
    }
}

fn unexpected_eof() -> Error {
    Error::transport(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "stream ended in the middle of a frame",
    ))
}

async fn write_streaming_request(
    send: web_transport_quinn::SendStream,
    request: RpcRequest,
    mut request_body: BoxMessageStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let mut send = CancellableSendStream::new(send);
    write_frame(send.send_mut(), &request, max_frame_size).await?;

    while let Some(body) = request_body.next().await.transpose()? {
        write_frame(
            send.send_mut(),
            &RpcStreamFrame::message(body),
            max_frame_size,
        )
        .await?;
    }

    send.send_mut().finish().map_err(Error::transport)?;
    send.complete();

    Ok(())
}

impl crate::server::Server {
    pub async fn serve_webtransport(self, endpoint: ServerEndpoint) -> Result<()> {
        self.serve_webtransport_with_shutdown(endpoint, pending::<()>())
            .await
    }

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

                let Some(request_permit) = try_acquire_permit(request_limit.as_ref()) else {
                    write_status(
                        send,
                        Status::unavailable("too many concurrent RPCs"),
                        server.max_frame_size(),
                    )
                    .await;
                    continue;
                };

                let server = server.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    let _request_permit = request_permit;
                    handle_stream(server, send, recv).await;
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
    mut send: web_transport_quinn::SendStream,
    mut recv: web_transport_quinn::RecvStream,
) {
    let request = match read_frame::<RpcRequest>(&mut recv, server.max_frame_size()).await {
        Ok(request) => request,
        Err(error) => {
            write_status(
                send,
                Status::internal(error.to_string()),
                server.max_frame_size(),
            )
            .await;
            return;
        }
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
        let _ = send.finish();
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
    let mut response = server.handle_streaming_request(request, request_body).await;

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
        let is_status = frame.frame_kind() == Some(RpcStreamFrameKind::Status);

        if write_frame(&mut send, &frame, max_frame_size)
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

#[crate::async_trait]
impl MessageStream<Vec<u8>> for WebTransportRequestStream {
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        if self.done {
            return None;
        }

        match read_frame_or_eof::<RpcStreamFrame>(&mut self.recv, self.max_frame_size).await {
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
                    Some(Err(Error::from(Status::internal(
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
