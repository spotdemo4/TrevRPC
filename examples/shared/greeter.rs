use std::sync::Arc;

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloRequest {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct HelloReply {
    #[prost(string, tag = "1")]
    pub message: String,
}

#[allow(clippy::missing_errors_doc)]
#[trevrpc::async_trait]
pub trait Greeter: Send + Sync + 'static {
    async fn say_hello(
        &self,
        request: HelloRequest,
    ) -> core::result::Result<HelloReply, trevrpc::Status>;
}

#[derive(Clone)]
pub struct GreeterClient<T> {
    transport: T,
}

impl<T> GreeterClient<T> {
    pub const SERVICE: &'static str = "example.greeter.Greeter";

    #[must_use]
    pub const fn new(transport: T) -> Self {
        Self { transport }
    }

    pub async fn say_hello(
        &self,
        request: HelloRequest,
        options: trevrpc::client::CallOptions,
    ) -> trevrpc::Result<HelloReply>
    where
        T: trevrpc::client::RpcTransport,
    {
        trevrpc::client::unary(
            &self.transport,
            Self::SERVICE,
            "SayHello",
            &request,
            options,
        )
        .await
    }
}

pub fn register_greeter<S>(server: &mut trevrpc::server::Server, service: S)
where
    S: Greeter,
{
    let service = Arc::new(service);

    {
        let service = Arc::clone(&service);
        server.route(GreeterClient::<()>::SERVICE, "SayHello", move |body| {
            let service = Arc::clone(&service);
            async move {
                let request = <HelloRequest as prost::Message>::decode(body.as_slice())?;
                let response = service.say_hello(request).await?;
                Ok(prost::Message::encode_to_vec(&response))
            }
        });
    }
}
