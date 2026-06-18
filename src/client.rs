use crate::{Error, Result, RpcRequest, RpcResponse, Status};
use prost::Message;

#[crate::async_trait]
pub trait RpcTransport: Clone + Send + Sync + 'static {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse>;
}

pub async fn unary<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
) -> Result<Res>
where
    T: RpcTransport,
    Req: Message,
    Res: Message + Default,
{
    let response = transport
        .call(RpcRequest::new(service, method, request.encode_to_vec()))
        .await?;

    let status = Status::from_response(&response);
    if !status.is_ok() {
        return Err(Error::from(status));
    }

    Res::decode(response.body.as_slice()).map_err(Error::from)
}
