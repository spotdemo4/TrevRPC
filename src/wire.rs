#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcRequest {
    #[prost(string, tag = "1")]
    pub service: String,
    #[prost(string, tag = "2")]
    pub method: String,
    #[prost(bytes = "vec", tag = "3")]
    pub body: Vec<u8>,
}

impl RpcRequest {
    #[must_use]
    pub fn new(service: impl Into<String>, method: impl Into<String>, body: Vec<u8>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
            body,
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct RpcResponse {
    #[prost(uint32, tag = "1")]
    pub status: u32,
    #[prost(string, tag = "2")]
    pub message: String,
    #[prost(bytes = "vec", tag = "3")]
    pub body: Vec<u8>,
}

impl RpcResponse {
    #[must_use]
    pub fn ok(body: Vec<u8>) -> Self {
        crate::Status::ok().into_response(body)
    }
}
