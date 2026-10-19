//! The service for HTTP requests that `TrevRPC` does not claim, such as the static content of a
//! web application, served on the same endpoint and TCP listener as `TrevRPC`.

use std::fmt;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use bytes::{Buf as _, Bytes};
use http::{HeaderMap, HeaderName, HeaderValue, Request, Response, StatusCode, header};
use http_body::{Body, Frame, SizeHint};
use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt as _, Empty};

use crate::http3::H3RecvStream;
use crate::rpc_body::BodySink;
use crate::{Error, Result};

/// The error a fallback service or its response body fails with.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// The body of every response the HTTP bindings write.
pub(crate) type ResponseBody = UnsyncBoxBody<Bytes, BoxError>;

type ResponseFuture =
    Pin<Box<dyn Future<Output = std::result::Result<Response<ResponseBody>, BoxError>> + Send>>;

/// The object-safe form of a fallback service.
trait Fallback: Send + Sync + 'static {
    fn serve(&self, request: Request<FallbackBody>) -> ResponseFuture;
}

impl<S, B> Fallback for S
where
    S: tower_service::Service<Request<FallbackBody>, Response = Response<B>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError>,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<BoxError>,
{
    fn serve(&self, request: Request<FallbackBody>) -> ResponseFuture {
        // Each request drives its own clone, so a service that is not ready blocks only it.
        let mut service = self.clone();
        Box::pin(async move {
            poll_fn(|context| service.poll_ready(context))
                .await
                .map_err(Into::<BoxError>::into)?;
            let response = tower_service::Service::call(&mut service, request)
                .await
                .map_err(Into::<BoxError>::into)?;
            Ok(response.map(|body| body.map_err(Into::into).boxed_unsync()))
        })
    }
}

/// A service for the HTTP requests `TrevRPC` does not claim; see
/// [`crate::server::ServerOptions::with_http_fallback`].
///
/// Any [`tower_service::Service`] over [`FallbackBody`] requests works, such as an `axum` router
/// or `tower_http::services::ServeDir`.
#[derive(Clone)]
pub struct HttpFallback(Arc<dyn Fallback>);

impl HttpFallback {
    /// Wraps `service`. It is cloned for every request.
    pub fn new<S, B>(service: S) -> Self
    where
        S: tower_service::Service<Request<FallbackBody>, Response = Response<B>>
            + Clone
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
        S::Error: Into<BoxError>,
        B: Body<Data = Bytes> + Send + 'static,
        B::Error: Into<BoxError>,
    {
        Self(Arc::new(service))
    }

    /// Serves `request`, answering 500 when the service fails. HTTP/3 is advertised through
    /// `alt_svc`, when given, unless the service set `Alt-Svc` itself.
    pub(crate) async fn call(
        &self,
        request: Request<FallbackBody>,
        alt_svc: Option<HeaderValue>,
    ) -> Response<ResponseBody> {
        let mut response = match self.0.serve(request).await {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(%error, "HTTP fallback failed");
                empty_response(StatusCode::INTERNAL_SERVER_ERROR)
            }
        };
        if let Some(alt_svc) = alt_svc {
            response
                .headers_mut()
                .entry(header::ALT_SVC)
                .or_insert(alt_svc);
        }
        response
    }
}

impl fmt::Debug for HttpFallback {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HttpFallback(..)")
    }
}

/// The head of a fallback response on an HTTP/2 or HTTP/3 stream, without the connection-specific
/// headers those versions forbid (RFC 9113 section 8.2.2, RFC 9114 section 4.2).
pub(crate) fn multiplexed_head(parts: http::response::Parts) -> Response<()> {
    let mut head = Response::from_parts(parts, ());
    let headers = head.headers_mut();
    for name in [
        header::CONNECTION,
        header::TRANSFER_ENCODING,
        header::UPGRADE,
        header::TE,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
    ] {
        headers.remove(name);
    }
    head
}

/// A response with no body.
pub(crate) fn empty_response(status: StatusCode) -> Response<ResponseBody> {
    let mut response = Response::new(Empty::new().map_err(|never| match never {}).boxed_unsync());
    *response.status_mut() = status;
    response
}

/// The request body a fallback service receives, whichever HTTP version carried the request.
pub struct FallbackBody(Kind);

enum Kind {
    Http1(hyper::body::Incoming),
    Http2 {
        body: h2::RecvStream,
        data_done: bool,
    },
    Http3 {
        body: H3RecvStream,
        data_done: bool,
    },
}

impl FallbackBody {
    pub(crate) const fn http1(body: hyper::body::Incoming) -> Self {
        Self(Kind::Http1(body))
    }

    pub(crate) const fn http2(body: h2::RecvStream) -> Self {
        Self(Kind::Http2 {
            body,
            data_done: false,
        })
    }

    pub(crate) const fn http3(body: H3RecvStream) -> Self {
        Self(Kind::Http3 {
            body,
            data_done: false,
        })
    }
}

impl Body for FallbackBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>>>> {
        match &mut self.get_mut().0 {
            Kind::Http1(body) => Pin::new(body).poll_frame(context).map_err(Error::transport),
            Kind::Http2 { body, data_done } => {
                if !*data_done {
                    match ready!(body.poll_data(context)) {
                        Some(Ok(data)) => {
                            // The chunk is handed to the service, so its credit returns at once.
                            let _ = body.flow_control().release_capacity(data.len());
                            return Poll::Ready(Some(Ok(Frame::data(data))));
                        }
                        Some(Err(error)) => return Poll::Ready(Some(Err(Error::transport(error)))),
                        None => *data_done = true,
                    }
                }
                Poll::Ready(
                    ready!(body.poll_trailers(context))
                        .map_err(Error::transport)
                        .transpose()
                        .map(|trailers| trailers.map(Frame::trailers)),
                )
            }
            Kind::Http3 { body, data_done } => {
                if !*data_done {
                    match ready!(body.poll_recv_data(context)) {
                        Ok(Some(mut data)) => {
                            let data = data.copy_to_bytes(data.remaining());
                            return Poll::Ready(Some(Ok(Frame::data(data))));
                        }
                        Ok(None) => *data_done = true,
                        Err(error) => return Poll::Ready(Some(Err(Error::transport(error)))),
                    }
                }
                Poll::Ready(
                    ready!(body.poll_recv_trailers(context))
                        .map_err(Error::transport)
                        .transpose()
                        .map(|trailers| trailers.map(Frame::trailers)),
                )
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.0 {
            Kind::Http1(body) => body.is_end_stream(),
            Kind::Http2 { body, .. } => body.is_end_stream(),
            Kind::Http3 { .. } => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        match &self.0 {
            Kind::Http1(body) => body.size_hint(),
            Kind::Http2 { .. } | Kind::Http3 { .. } => SizeHint::default(),
        }
    }
}

impl fmt::Debug for FallbackBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let version = match self.0 {
            Kind::Http1(_) => "HTTP/1.1",
            Kind::Http2 { .. } => "HTTP/2",
            Kind::Http3 { .. } => "HTTP/3",
        };
        f.debug_tuple("FallbackBody").field(&version).finish()
    }
}

/// The send half of an HTTP/2 or HTTP/3 request stream that a fallback response body is written
/// to. HTTP/1.1 responses are written by hyper.
pub(crate) trait ResponseSink: BodySink {
    /// Sends trailers, which end the response.
    fn send_trailers(&mut self, trailers: HeaderMap) -> impl Future<Output = Result<()>> + Send;
}

/// Writes a fallback response body to `sink` and ends the stream. The stream is reset when the
/// body fails, and the body is dropped as soon as the peer stops reading or the connection is lost.
pub(crate) async fn write_body<K: ResponseSink>(body: ResponseBody, mut sink: K) {
    if write_frames(body, &mut sink).await.is_err() {
        sink.reset();
    }
}

async fn write_frames<K: ResponseSink>(mut body: ResponseBody, sink: &mut K) -> Result<()> {
    loop {
        let frame = tokio::select! {
            frame = body.frame() => frame,
            result = sink.stopped() => {
                result?;
                return Err(Error::from(crate::Status::cancelled("peer stopped reading the response")));
            }
        };
        let Some(frame) = frame else {
            return sink.finish().await;
        };
        match frame.map_err(Error::Transport)?.into_data() {
            Ok(data) => sink.write(data).await?,
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    return sink.send_trailers(trailers).await;
                }
            }
        }
    }
}
