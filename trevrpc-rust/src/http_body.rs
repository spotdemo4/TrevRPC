//! RPC streams carried in HTTP request and response bodies: HTTP/3 and HTTP/2 requests, and
//! `WebTransport` streams, which share the same body-oriented libraries.

use std::future::Future;
use std::sync::Arc;

use bytes::{Buf, BufMut as _, Bytes};
use tokio::sync::{Semaphore, watch};

use crate::datagram::Demux;
use crate::framed::{self, FrameRead, FrameWrite};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::server::Server;
use crate::server_transport::{
    self, IncomingRequest, Permit, ResponseWriter, open_flow, reject_unreadable,
};
use crate::{ResponseFrame, Result};

/// The send half of one HTTP request or `WebTransport` stream.
pub trait BodySink: Send + 'static {
    fn write(&mut self, chunk: &[u8]) -> impl Future<Output = Result<()>> + Send;

    fn finish(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn reset(&mut self);

    /// Resolves `Ok` when the peer stops reading the response, and with an error when the
    /// connection is lost first.
    fn stopped(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// The receive half of one HTTP request or `WebTransport` stream.
pub trait BodySource: Send + 'static {
    fn next_chunk(&mut self) -> impl Future<Output = Result<Option<Bytes>>> + Send;

    /// Validates anything that follows the request body, such as HTTP trailers.
    fn validate_end(&mut self) -> impl Future<Output = Result<()>> + Send;

    /// Asks the peer to stop sending the request body, if the transport can say so.
    fn stop(&mut self);

    /// Resolves when the transport ends the request without consuming its data, or never if it
    /// cannot tell. It must be cancel-safe.
    fn transport_event(&mut self) -> impl Future<Output = Option<RequestTransportEvent>> + Send;
}

/// Adapts a body's send half to the server's response writer.
struct BodyWriter<S> {
    sink: S,
    max_frame_size: usize,
}

impl<S: BodySink> FrameWrite for BodyWriter<S> {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.sink.write(chunk).await?;
        }
        Ok(())
    }
}

impl<S: BodySink> ResponseWriter for BodyWriter<S> {
    async fn write_frames(&mut self, frames: &mut Vec<ResponseFrame>) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        framed::write_response_frames(self, frames, max_frame_size).await
    }

    async fn stopped(&mut self) -> Result<()> {
        self.sink.stopped().await
    }

    fn reset(&mut self) {
        self.sink.reset();
    }

    async fn finish(&mut self) -> Result<()> {
        self.sink.finish().await
    }
}

/// Adapts a body's receive half to the server's request reader.
struct BodyReader<S> {
    source: S,
    chunk: Bytes,
}

impl<S: BodySource> FrameRead for BodyReader<S> {
    async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
        while self.chunk.is_empty() {
            let Some(chunk) = self.source.next_chunk().await? else {
                return Ok(None);
            };
            self.chunk = chunk;
        }
        let len = bytes.len().min(self.chunk.len());
        let mut dest = bytes;
        dest.put((&mut self.chunk).take(len));
        Ok(Some(len))
    }
}

impl<S: BodySource> RequestPumpReader for BodyReader<S> {
    fn stop_trevrpc(&mut self) {
        self.source.stop();
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        self.source.transport_event().await
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        self.source.validate_end().await
    }
}

/// Reads the initial request frame and opens its datagram flow on `demux`, replying with the
/// failure status when either fails. The stream slot is released before that reply.
async fn read_request<K, S>(
    server: &Server,
    stream_permit: &mut Permit,
    demux: Option<&Demux>,
    sink: K,
    source: S,
) -> Option<(BodyWriter<K>, BodyReader<S>, IncomingRequest)>
where
    K: BodySink,
    S: BodySource,
{
    let writer = BodyWriter {
        sink,
        max_frame_size: server.max_frame_size(),
    };
    let mut reader = BodyReader {
        source,
        chunk: Bytes::new(),
    };
    let incoming = server_transport::read_initial_request(server, &mut reader)
        .await
        .and_then(|mut incoming| open_flow(demux, &mut incoming).map(|()| incoming));
    match incoming {
        Ok(incoming) => Some((writer, reader, incoming)),
        Err(error) => {
            reject_unreadable(server, stream_permit, writer, error).await;
            None
        }
    }
}

/// Serves one RPC carried by a body, holding `stream_permit` until its STATUS is written.
///
/// `demux` carries the datagrams of the `WebTransport` session the body belongs to, if any.
pub async fn handle_rpc_stream<K, S>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    mut stream_permit: Permit,
    demux: Option<Arc<Demux>>,
    sink: K,
    source: S,
    shutdown: watch::Receiver<bool>,
) where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, incoming)) =
        read_request(&server, &mut stream_permit, demux.as_deref(), sink, source).await
    {
        server_transport::handle_stream(
            server,
            request_limit,
            stream_permit,
            writer,
            reader,
            incoming,
            shutdown,
        )
        .await;
    }
}

/// Answers one RPC carried by a body with the over-limit status.
pub async fn reject_rpc_stream<K, S>(server: Server, sink: K, source: S)
where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, incoming)) =
        read_request(&server, &mut Permit::none(), None, sink, source).await
    {
        server_transport::reject_stream(server, writer, reader, incoming).await;
    }
}
