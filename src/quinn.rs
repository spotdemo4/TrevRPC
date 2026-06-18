use prost::Message;

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
        while let Some(incoming) = endpoint.accept().await {
            let server = self.clone();
            tokio::spawn(async move {
                if let Ok(connection) = incoming.await {
                    handle_connection(server, connection).await;
                }
            });
        }

        Ok(())
    }
}

async fn handle_connection(server: crate::server::Server, connection: quinn::Connection) {
    while let Ok((send, recv)) = connection.accept_bi().await {
        let server = server.clone();
        tokio::spawn(async move {
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
