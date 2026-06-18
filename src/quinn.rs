use std::future::{Future, pending};
use std::sync::Arc;

use prost::Message;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::client::RpcTransport;
use crate::framing::{DEFAULT_MAX_FRAME_SIZE, decode_frame, encode_frame_with_max, frame_body_len};
use crate::{Error, Result, RpcRequest, RpcResponse, Status};

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
        let (mut send, mut recv) = self.connection.open_bi().await.map_err(Error::transport)?;

        write_frame(&mut send, &request, self.max_frame_size).await?;
        send.finish().map_err(Error::transport)?;

        read_frame(&mut recv, self.max_frame_size).await
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
                    tokio::spawn(async move {
                        if let Ok(connection) = incoming.await {
                            handle_connection(server, connection, request_limit, connection_permit).await;
                        }
                    });
                }
                () = &mut shutdown => {
                    endpoint.close(0_u32.into(), b"server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

async fn handle_connection(
    server: crate::server::Server,
    connection: quinn::Connection,
    request_limit: Option<Arc<Semaphore>>,
    _connection_permit: Permit,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));

    while let Ok((send, recv)) = connection.accept_bi().await {
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
        tokio::spawn(async move {
            let _stream_permit = stream_permit;
            let _request_permit = request_permit;
            handle_stream(server, send, recv).await;
        });
    }
}

async fn handle_stream(
    server: crate::server::Server,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
) {
    let response = match read_frame::<RpcRequest>(&mut recv, server.max_frame_size()).await {
        Ok(request) => server.handle_request(request).await,
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
