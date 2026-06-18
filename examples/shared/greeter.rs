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

    async fn lots_of_replies(
        &self,
        request: HelloRequest,
    ) -> core::result::Result<trevrpc::BoxMessageStream<HelloReply>, trevrpc::Status>;

    async fn lots_of_greetings(
        &self,
        requests: trevrpc::BoxMessageStream<HelloRequest>,
    ) -> core::result::Result<HelloReply, trevrpc::Status>;

    async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<HelloRequest>,
    ) -> core::result::Result<trevrpc::BoxMessageStream<HelloReply>, trevrpc::Status>;
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

    pub async fn lots_of_replies(
        &self,
        request: HelloRequest,
        options: trevrpc::client::CallOptions,
    ) -> trevrpc::Result<trevrpc::BoxMessageStream<HelloReply>>
    where
        T: trevrpc::client::RpcTransport,
    {
        trevrpc::client::server_streaming(
            &self.transport,
            Self::SERVICE,
            "LotsOfReplies",
            &request,
            options,
        )
        .await
    }

    pub async fn lots_of_greetings(
        &self,
        requests: trevrpc::BoxMessageStream<HelloRequest>,
        options: trevrpc::client::CallOptions,
    ) -> trevrpc::Result<HelloReply>
    where
        T: trevrpc::client::RpcTransport,
    {
        trevrpc::client::client_streaming(
            &self.transport,
            Self::SERVICE,
            "LotsOfGreetings",
            requests,
            options,
        )
        .await
    }

    pub async fn bidi_hello(
        &self,
        requests: trevrpc::BoxMessageStream<HelloRequest>,
        options: trevrpc::client::CallOptions,
    ) -> trevrpc::Result<trevrpc::BoxMessageStream<HelloReply>>
    where
        T: trevrpc::client::RpcTransport,
    {
        trevrpc::client::bidirectional_streaming(
            &self.transport,
            Self::SERVICE,
            "BidiHello",
            requests,
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

    {
        let service = Arc::clone(&service);
        server.route_streaming(
            GreeterClient::<()>::SERVICE,
            "LotsOfReplies",
            trevrpc::RpcKind::ServerStreaming,
            move |body, _request_stream| {
                let service = Arc::clone(&service);
                async move {
                    let request = <HelloRequest as prost::Message>::decode(body.as_slice())?;
                    let responses = service.lots_of_replies(request).await?;
                    Ok(trevrpc::stream::encode(responses))
                }
            },
        );
    }

    {
        let service = Arc::clone(&service);
        server.route_streaming(
            GreeterClient::<()>::SERVICE,
            "LotsOfGreetings",
            trevrpc::RpcKind::ClientStreaming,
            move |_body, request_stream| {
                let service = Arc::clone(&service);
                async move {
                    let requests = trevrpc::stream::decode::<HelloRequest>(request_stream);
                    let response = service.lots_of_greetings(requests).await?;
                    Ok(trevrpc::stream::encode(trevrpc::stream::from_iter([
                        response,
                    ])))
                }
            },
        );
    }

    {
        let service = Arc::clone(&service);
        server.route_streaming(
            GreeterClient::<()>::SERVICE,
            "BidiHello",
            trevrpc::RpcKind::BidirectionalStreaming,
            move |_body, request_stream| {
                let service = Arc::clone(&service);
                async move {
                    let requests = trevrpc::stream::decode::<HelloRequest>(request_stream);
                    let responses = service.bidi_hello(requests).await?;
                    Ok(trevrpc::stream::encode(responses))
                }
            },
        );
    }
}
