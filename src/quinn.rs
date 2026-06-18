use std::future::{Future, pending};
use std::sync::Arc;

use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinSet;

use crate::client::RpcTransport;
use crate::framing::{DEFAULT_MAX_FRAME_SIZE, decode_frame, encode_frame_with_max, frame_body_len};
use crate::{Error, Result, RpcRequest, RpcResponse, Status};

const CANCELLED_STREAM_CODE: u32 = 1;

#[derive(Clone)]
pub struct QuinnTransport {
    connection: quinn::Connection,
    max_frame_size: usize,
}

impl QuinnTransport {
    #[must_use]
    pub const fn new(connection: quinn::Connection) -> Self {
        Self {
            connection,
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
        }
    }

    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }

    #[must_use]
    pub const fn connection(&self) -> &quinn::Connection {
        &self.connection
    }

    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }
}

#[crate::async_trait]
impl RpcTransport for QuinnTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let (send, recv) = self.connection.open_bi().await.map_err(Error::transport)?;
        let mut streams = CancellableBiStream::new(send, recv);

        write_frame(streams.send_mut(), &request, self.max_frame_size).await?;
        streams.send_mut().finish().map_err(Error::transport)?;

        let response = read_frame(streams.recv_mut(), self.max_frame_size).await?;
        streams.complete();

        Ok(response)
    }
}

struct CancellableBiStream {
    send: Option<quinn::SendStream>,
    recv: Option<quinn::RecvStream>,
    complete: bool,
}

impl CancellableBiStream {
    fn new(send: quinn::SendStream, recv: quinn::RecvStream) -> Self {
        Self {
            send: Some(send),
            recv: Some(recv),
            complete: false,
        }
    }

    fn send_mut(&mut self) -> &mut quinn::SendStream {
        self.send
            .as_mut()
            .expect("send stream should be present until completion")
    }

    fn recv_mut(&mut self) -> &mut quinn::RecvStream {
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
            let _ = send.reset(CANCELLED_STREAM_CODE.into());
        }

        if let Some(recv) = &mut self.recv {
            let _ = recv.stop(CANCELLED_STREAM_CODE.into());
        }
    }
}

pub async fn write_frame<M>(
    send: &mut quinn::SendStream,
    message: &M,
    max_frame_size: usize,
) -> Result<()>
where
    M: Message,
{
    let frame = encode_frame_with_max(message, max_frame_size)?;
    send.write_all(&frame).await.map_err(Error::transport)
}

pub async fn read_frame<M>(recv: &mut quinn::RecvStream, max_frame_size: usize) -> Result<M>
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

impl crate::server::Server {
    pub async fn serve_quinn(self, endpoint: quinn::Endpoint) -> Result<()> {
        self.serve_quinn_with_shutdown(endpoint, pending::<()>())
            .await
    }

    pub async fn serve_quinn_with_shutdown<S>(
        self,
        endpoint: quinn::Endpoint,
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
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else {
                        break;
                    };

                    let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                        incoming.refuse();
                        continue;
                    };

                    let server = self.clone();
                    let request_limit = request_limit.clone();
                    let shutdown = shutdown_rx.clone();
                    connection_tasks.spawn(async move {
                        if let Ok(connection) = incoming.await {
                            handle_connection(server, connection, request_limit, connection_permit, shutdown).await;
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
                        tracing::warn!(%error, "connection task failed");
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

async fn handle_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    _connection_permit: Permit,
    mut shutdown: watch::Receiver<bool>,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = connection.accept_bi(), if !*shutdown.borrow() => {
                let Ok((send, recv)) = accepted else {
                    break;
                };

                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    write_status(
                        send,
                        Status::unavailable("too many concurrent streams on connection"),
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
                    tracing::warn!(%error, "stream task failed");
                }
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

async fn handle_stream(
    server: crate::server::Server,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    let response = match read_frame::<RpcRequest>(&mut recv, server.max_frame_size()).await {
        Ok(request) => {
            tokio::select! {
                biased;
                response = server.handle_request(request) => response,
                stopped = send.stopped() => {
                    let _ = &stopped;
                    #[cfg(feature = "tracing")]
                    tracing::debug!("client stopped response stream before RPC completed");
                    return;
                }
            }
        }
        Err(error) => Status::internal(error.to_string()).into_response(Vec::new()),
    };

    if write_frame(&mut send, &response, server.max_frame_size())
        .await
        .is_ok()
    {
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

async fn write_status(mut send: quinn::SendStream, status: Status, max_frame_size: usize) {
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
    connection: &quinn::Connection,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_stream_tasks(stream_tasks))
            .await
            .is_err()
        {
            connection.close(0_u32.into(), b"server stream drain timed out");
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
            tracing::warn!(%error, "stream task failed while draining");
        }
    }
}

async fn drain_connections(
    connection_tasks: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
    endpoint: &quinn::Endpoint,
) {
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain_connection_tasks(connection_tasks, endpoint))
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
        drain_connection_tasks(connection_tasks, endpoint).await;
        endpoint.close(0_u32.into(), b"server shutdown complete");
    }
}

async fn drain_connection_tasks(connection_tasks: &mut JoinSet<()>, endpoint: &quinn::Endpoint) {
    let mut accepting = true;

    while !connection_tasks.is_empty() {
        tokio::select! {
            incoming = endpoint.accept(), if accepting => {
                if let Some(incoming) = incoming {
                    incoming.refuse();
                } else {
                    accepting = false;
                }
            }
            result = connection_tasks.join_next() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "connection task failed while draining");
                }
            }
        }
    }
}
