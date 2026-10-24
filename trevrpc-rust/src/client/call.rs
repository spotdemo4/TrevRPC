//! Calls that have not been made yet: what generated clients and the call functions return.
//!
//! A [`Call`] holds the transport, the method path, the call's [`CallOptions`], and what the call
//! sends. Awaiting it makes the call. Before that, its options can be changed for this call only,
//! on top of the options it started with (a generated client's default call options), and its
//! shape can change: [`Call::with_response`] also returns the response headers and trailing
//! metadata, and [`Call::send_all`] sends an existing stream of requests.
//!
//! ```ignore
//! let reply = client.say_hello(&request).await?;
//! let reply = client.say_hello(&request).timeout(Duration::from_secs(1)).await?;
//! let envelope = client.say_hello(&request).with_response().await?;
//! let reply = client.lots_of_greetings().send_all(requests).await?;
//! ```

use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::time::Duration;

use futures_core::Stream;
use prost::Message;

use super::{
    BidirectionalCall, CallOptions, ClientStreamingCall, MessageTypes, ResponseStream,
    RpcTransport, StreamingRpcTransport,
};
use crate::{BoxStream, CompressStream, ResponseEnvelope, Result, Unreliable};

/// The future of an awaited [`Call`].
pub type CallFuture<'a, T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'a>>;

/// A call to make. Awaiting it makes the call; see the [module documentation](self).
#[must_use = "a call does nothing until it is awaited"]
pub struct Call<'a, T: ?Sized, S> {
    transport: &'a T,
    method: &'a str,
    options: CallOptions,
    /// The directions marked unreliable when the options leave them to the call.
    default_unreliable: Unreliable,
    /// The method's message types, from generated code.
    message_types: Option<MessageTypes>,
    /// The directions that may use stream compression, from generated code.
    compress_stream: CompressStream,
    shape: S,
}

/// What a [`Call`] is made with, once awaited.
struct Parts<'a, T: ?Sized, S> {
    transport: &'a T,
    method: &'a str,
    options: CallOptions,
    shape: S,
}

impl<'a, T: ?Sized, S> Call<'a, T, S> {
    const fn new(transport: &'a T, method: &'a str, options: CallOptions, shape: S) -> Self {
        Self {
            transport,
            method,
            options,
            default_unreliable: Unreliable::NONE,
            message_types: None,
            compress_stream: CompressStream::NONE,
            shape,
        }
    }

    fn reshape<U>(self, reshape: impl FnOnce(S) -> U) -> Call<'a, T, U> {
        Call {
            transport: self.transport,
            method: self.method,
            options: self.options,
            default_unreliable: self.default_unreliable,
            message_types: self.message_types,
            compress_stream: self.compress_stream,
            shape: reshape(self.shape),
        }
    }

    /// Splits the call into what it is made with: its options get the default unreliable
    /// directions unless they mark some.
    fn into_parts(self) -> Parts<'a, T, S> {
        let mut options = self.options;
        options.unreliable.get_or_insert(self.default_unreliable);
        options.message_types = self.message_types;
        options.compress_stream = self.compress_stream;
        Parts {
            transport: self.transport,
            method: self.method,
            options,
            shape: self.shape,
        }
    }

    /// Changes the options of this call. `configure` receives the options so far, those the call
    /// started with plus earlier changes, and returns the options to make the call with.
    pub fn options(mut self, configure: impl FnOnce(CallOptions) -> CallOptions) -> Self {
        self.options = configure(self.options);
        self
    }

    /// Sets the timeout of this call; see [`CallOptions::with_timeout`].
    pub fn timeout(self, timeout: Duration) -> Self {
        self.options(|options| options.with_timeout(timeout))
    }

    /// Adds request metadata to this call; see [`CallOptions::with_metadata`].
    pub fn metadata(self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.options(|options| options.with_metadata(key, value))
    }

    /// Marks the directions of a streaming call that are [`Unreliable`] unless its options mark
    /// them with [`CallOptions::with_unreliable`]. Generated clients pass the schema's marks, so
    /// the client's default options and the call's own options override the schema.
    pub const fn default_unreliable(mut self, unreliable: Unreliable) -> Self {
        self.default_unreliable = unreliable;
        self
    }

    /// Names the method's request and response message types, which pick the dictionaries of
    /// [compressed messages](crate::Compression). Generated clients pass them.
    pub const fn message_types(mut self, request: &'static str, response: &'static str) -> Self {
        self.message_types = Some(MessageTypes::new(request, response));
        self
    }

    /// Lets the directions `directions` sets compress their messages with one zstd context per
    /// call; see [`CompressStream`]. Generated clients pass the schema's `compress_stream`.
    pub const fn compress_stream(mut self, directions: CompressStream) -> Self {
        self.compress_stream = directions;
        self
    }

    /// Returns the options of the call so far.
    #[must_use]
    pub const fn call_options(&self) -> &CallOptions {
        &self.options
    }
}

/// The shape of a unary call: awaiting it returns the response message.
pub struct Unary<Req, Res> {
    /// The encoded request message.
    body: Vec<u8>,
    messages: PhantomData<fn(Req) -> Res>,
}

/// The shape of a server-streaming call: awaiting it returns the response stream.
pub struct ServerStreaming<Req, Res> {
    /// The encoded request message.
    body: Vec<u8>,
    messages: PhantomData<fn(Req) -> Res>,
}

/// The shape of a client-streaming call: awaiting it returns a [`ClientStreamingCall`] to send
/// the requests on.
pub struct ClientStreaming<Req, Res> {
    messages: PhantomData<fn(Req) -> Res>,
}

/// The shape of a bidirectional-streaming call: awaiting it returns a [`BidirectionalCall`].
pub struct BidirectionalStreaming<Req, Res> {
    messages: PhantomData<fn(Req) -> Res>,
}

/// The shape of a call that sends an existing stream of requests; see [`Call::send_all`].
pub struct SendAll<S: StreamsRequests> {
    requests: BoxStream<S::Request>,
    shape: PhantomData<S>,
}

/// The shape of a call that also returns the response headers and trailing metadata; see
/// [`Call::with_response`].
pub struct WithResponse<S>(S);

/// A call shape whose requests are a stream.
pub trait StreamsRequests: sealed::Sealed {
    /// The request message.
    type Request;
}

impl<Req, Res> StreamsRequests for ClientStreaming<Req, Res> {
    type Request = Req;
}

impl<Req, Res> StreamsRequests for BidirectionalStreaming<Req, Res> {
    type Request = Req;
}

mod sealed {
    pub trait Sealed {}

    impl<Req, Res> Sealed for super::ClientStreaming<Req, Res> {}
    impl<Req, Res> Sealed for super::BidirectionalStreaming<Req, Res> {}
}

/// Prepares a unary call of `method`, the path `/package.Service/Method`, made with `options`.
/// The request is encoded at once.
pub fn unary<'a, T, Req, Res>(
    transport: &'a T,
    method: &'a str,
    request: &Req,
    options: CallOptions,
) -> Call<'a, T, Unary<Req, Res>>
where
    T: ?Sized,
    Req: Message,
{
    Call::new(
        transport,
        method,
        options,
        Unary {
            body: request.encode_to_vec(),
            messages: PhantomData,
        },
    )
}

/// Prepares a server-streaming call of `method`, made with `options`. The request is encoded at
/// once.
pub fn server_streaming<'a, T, Req, Res>(
    transport: &'a T,
    method: &'a str,
    request: &Req,
    options: CallOptions,
) -> Call<'a, T, ServerStreaming<Req, Res>>
where
    T: ?Sized,
    Req: Message,
{
    Call::new(
        transport,
        method,
        options,
        ServerStreaming {
            body: request.encode_to_vec(),
            messages: PhantomData,
        },
    )
}

/// Prepares a client-streaming call of `method`, made with `options`.
pub const fn client_streaming<'a, T, Req, Res>(
    transport: &'a T,
    method: &'a str,
    options: CallOptions,
) -> Call<'a, T, ClientStreaming<Req, Res>>
where
    T: ?Sized,
{
    Call::new(
        transport,
        method,
        options,
        ClientStreaming {
            messages: PhantomData,
        },
    )
}

/// Prepares a bidirectional-streaming call of `method`, made with `options`.
pub const fn bidirectional_streaming<'a, T, Req, Res>(
    transport: &'a T,
    method: &'a str,
    options: CallOptions,
) -> Call<'a, T, BidirectionalStreaming<Req, Res>>
where
    T: ?Sized,
{
    Call::new(
        transport,
        method,
        options,
        BidirectionalStreaming {
            messages: PhantomData,
        },
    )
}

impl<'a, T: ?Sized, Req, Res> Call<'a, T, Unary<Req, Res>> {
    /// Makes the call return the response headers and trailing metadata with the message.
    pub fn with_response(self) -> Call<'a, T, WithResponse<Unary<Req, Res>>> {
        self.reshape(WithResponse)
    }
}

impl<'a, T: ?Sized, S: StreamsRequests> Call<'a, T, S> {
    /// Makes the call send every message of `requests` and then finish the request stream.
    ///
    /// A client-streaming call then returns its response message, and a bidirectional call its
    /// response stream. A request stream that fails cancels the call.
    pub fn send_all<R>(self, requests: R) -> Call<'a, T, SendAll<S>>
    where
        R: Stream<Item = Result<S::Request>> + Send + 'static,
    {
        self.reshape(|_| SendAll {
            requests: Box::pin(requests),
            shape: PhantomData,
        })
    }
}

impl<'a, T: ?Sized, Req, Res> Call<'a, T, SendAll<ClientStreaming<Req, Res>>> {
    /// Makes the call return the response headers and trailing metadata with the message.
    pub fn with_response(self) -> Call<'a, T, WithResponse<SendAll<ClientStreaming<Req, Res>>>> {
        self.reshape(WithResponse)
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, Unary<Req, Res>>
where
    T: RpcTransport + ?Sized,
    Res: Message + Default + 'a,
{
    type Output = Result<Res>;
    type IntoFuture = CallFuture<'a, Res>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        let response = super::unary_response(transport, method, shape.body, options);
        Box::pin(async move { Ok(response.await?.into_parts().0) })
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, WithResponse<Unary<Req, Res>>>
where
    T: RpcTransport + ?Sized,
    Res: Message + Default + 'a,
{
    type Output = Result<ResponseEnvelope<Res>>;
    type IntoFuture = CallFuture<'a, ResponseEnvelope<Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        Box::pin(super::unary_response(
            transport,
            method,
            shape.0.body,
            options,
        ))
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, ServerStreaming<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<ResponseStream<Res>>;
    type IntoFuture = CallFuture<'a, ResponseStream<Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        Box::pin(super::start_server_streaming(
            transport, method, shape.body, options,
        ))
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, ClientStreaming<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<ClientStreamingCall<Req, Res>>;
    type IntoFuture = CallFuture<'a, ClientStreamingCall<Req, Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            ..
        } = self.into_parts();
        Box::pin(super::start_client_streaming(transport, method, options))
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, SendAll<ClientStreaming<Req, Res>>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<Res>;
    type IntoFuture = CallFuture<'a, Res>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        let response = super::client_streaming_send_all(transport, method, shape.requests, options);
        Box::pin(async move { Ok(response.await?.into_parts().0) })
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, WithResponse<SendAll<ClientStreaming<Req, Res>>>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<ResponseEnvelope<Res>>;
    type IntoFuture = CallFuture<'a, ResponseEnvelope<Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        Box::pin(super::client_streaming_send_all(
            transport,
            method,
            shape.0.requests,
            options,
        ))
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, BidirectionalStreaming<Req, Res>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<BidirectionalCall<Req, Res>>;
    type IntoFuture = CallFuture<'a, BidirectionalCall<Req, Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            ..
        } = self.into_parts();
        Box::pin(super::start_bidirectional_streaming(
            transport, method, options,
        ))
    }
}

impl<'a, T, Req, Res> IntoFuture for Call<'a, T, SendAll<BidirectionalStreaming<Req, Res>>>
where
    T: StreamingRpcTransport + ?Sized,
    Req: Message + Send + 'static,
    Res: Message + Default + Send + 'static,
{
    type Output = Result<ResponseStream<Res>>;
    type IntoFuture = CallFuture<'a, ResponseStream<Res>>;

    fn into_future(self) -> Self::IntoFuture {
        let Parts {
            transport,
            method,
            options,
            shape,
        } = self.into_parts();
        Box::pin(super::bidirectional_streaming_send_all(
            transport,
            method,
            shape.requests,
            options,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;

    use super::{CallOptions, bidirectional_streaming, unary};
    use crate::client::{CallInfo, MessageTypes, RpcTransport, StreamingRpcTransport};
    use crate::{
        BoxStream, ResponseEnvelope, ResponseFrame, Result, RpcRequest, Status, Unreliable,
    };

    #[derive(Clone, PartialEq, prost::Message)]
    struct Echo {
        #[prost(string, tag = "1")]
        value: String,
    }

    /// Records the request and the call information of the last call, and ends every streaming
    /// call at once with an `OK` status.
    #[derive(Default)]
    struct Recorder {
        last: Arc<Mutex<Option<(RpcRequest, CallInfo)>>>,
    }

    impl Recorder {
        fn last(&self) -> (RpcRequest, CallInfo) {
            self.last
                .lock()
                .expect("lock should not be poisoned")
                .take()
                .expect("a call should be recorded")
        }
    }

    #[crate::async_trait]
    impl RpcTransport for Recorder {
        async fn call(
            &self,
            request: RpcRequest,
            body: Vec<u8>,
        ) -> Result<ResponseEnvelope<Bytes>> {
            self.call_with_info(request, body, CallInfo::default())
                .await
        }

        async fn call_with_info(
            &self,
            request: RpcRequest,
            body: Vec<u8>,
            info: CallInfo,
        ) -> Result<ResponseEnvelope<Bytes>> {
            *self.last.lock().expect("lock should not be poisoned") = Some((request, info));
            Ok(ResponseEnvelope::new(Bytes::from(body)))
        }
    }

    #[crate::async_trait]
    impl StreamingRpcTransport for Recorder {
        async fn streaming_call(
            &self,
            request: RpcRequest,
            request_body: BoxStream<Vec<u8>>,
        ) -> Result<BoxStream<ResponseFrame>> {
            self.streaming_call_with_info(request, request_body, CallInfo::default())
                .await
        }

        async fn streaming_call_with_info(
            &self,
            request: RpcRequest,
            _request_body: BoxStream<Vec<u8>>,
            info: CallInfo,
        ) -> Result<BoxStream<ResponseFrame>> {
            *self.last.lock().expect("lock should not be poisoned") = Some((request, info));
            Ok(crate::stream::from_iter([ResponseFrame::Status(
                Status::ok(),
            )]))
        }
    }

    #[tokio::test]
    async fn per_call_options_build_on_the_options_the_call_started_with() {
        let transport = Recorder::default();
        let defaults = CallOptions::new().with_metadata("token", "secret");
        let request = Echo {
            value: "hi".to_owned(),
        };

        let reply: Echo = unary(&transport, "/test.Echo/Say", &request, defaults)
            .metadata("trace", "1")
            .timeout(std::time::Duration::from_secs(5))
            .await
            .expect("the call should succeed");

        assert_eq!(reply, request);
        let (sent, _) = transport.last();
        assert_eq!(sent.metadata.get("token"), Some(&b"secret".to_vec()));
        assert_eq!(sent.metadata.get("trace"), Some(&b"1".to_vec()));
        assert_ne!(sent.timeout_nanos, 0);
    }

    #[tokio::test]
    async fn options_override_the_default_unreliable_directions() {
        let transport = Recorder::default();
        for (options, expected) in [
            (CallOptions::new(), Unreliable::BOTH),
            (
                CallOptions::new().with_unreliable(Unreliable::NONE),
                Unreliable::NONE,
            ),
            (
                CallOptions::new().with_unreliable(Unreliable::REQUESTS),
                Unreliable::REQUESTS,
            ),
        ] {
            let _call =
                bidirectional_streaming::<_, Echo, Echo>(&transport, "/test.Echo/Chat", options)
                    .default_unreliable(Unreliable::BOTH)
                    .await
                    .expect("the call should open");
            assert_eq!(transport.last().1.unreliable, expected);
        }
    }

    #[tokio::test]
    async fn generated_message_types_reach_the_transport() {
        let transport = Recorder::default();
        let request = Echo {
            value: "hi".to_owned(),
        };
        let _: Echo = unary(&transport, "/test.Echo/Say", &request, CallOptions::new())
            .message_types("test.Echo", "test.Echo")
            .await
            .unwrap();
        assert_eq!(
            transport.last().1.message_types,
            Some(MessageTypes::new("test.Echo", "test.Echo"))
        );

        let _ = bidirectional_streaming::<_, Echo, Echo>(
            &transport,
            "/test.Echo/Chat",
            CallOptions::new(),
        )
        .message_types("test.Echo", "test.Echo")
        .default_unreliable(Unreliable::BOTH)
        .await
        .unwrap();
        let (_, info) = transport.last();
        assert_eq!(info.unreliable, Unreliable::BOTH);
        assert_eq!(
            info.message_types,
            Some(MessageTypes::new("test.Echo", "test.Echo"))
        );
    }
}
