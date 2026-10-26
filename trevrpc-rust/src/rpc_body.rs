//! RPC streams carried in HTTP request and response bodies: HTTP/3 and HTTP/2 requests, and
//! `WebTransport` streams, which share the same body-oriented libraries.

use std::future::Future;
use std::sync::Arc;

use bytes::{Buf, BufMut as _, Bytes};
use tokio::sync::{Semaphore, watch};

use crate::connection_metadata::ConnectionMetadataStore;
use crate::datagram::Demux;
use crate::framed::{self, FrameRead, FrameWrite, ResponseCodec};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::server::Server;
use crate::server_transport::{
    self, IncomingRequest, Permit, ResponseWriter, open_flow, reject_unreadable,
    resolve_connection_metadata,
};
use crate::{ResponseFrame, Result};

/// What the RPC streams of one `WebTransport` session share. Ordinary HTTP requests have none:
/// they use offered connection metadata for their own call only.
#[derive(Default)]
pub struct WebTransportSessionState {
    /// The session's datagrams, when it has them.
    pub demux: Option<Arc<Demux>>,
    pub connection_metadata: ConnectionMetadataStore,
}

/// The send half of one HTTP request or `WebTransport` stream.
pub trait BodySink: Send + 'static {
    fn write(&mut self, chunk: Bytes) -> impl Future<Output = Result<()>> + Send;

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
    codec: ResponseCodec,
}

impl<S: BodySink> FrameWrite for BodyWriter<S> {
    async fn write_frame_chunks(&mut self, chunks: &mut [Bytes]) -> Result<()> {
        for chunk in chunks {
            self.sink.write(std::mem::take(chunk)).await?;
        }
        Ok(())
    }
}

impl<S: BodySink> ResponseWriter for BodyWriter<S> {
    async fn write_frames(&mut self, frames: &mut Vec<ResponseFrame>) -> Result<()> {
        let max_frame_size = self.max_frame_size;
        // The writer is the frames' destination too, so the codec is moved out meanwhile.
        let mut codec = std::mem::take(&mut self.codec);
        let written = framed::write_response_frames(self, frames, max_frame_size, &mut codec).await;
        self.codec = codec;
        written
    }

    fn set_codec(&mut self, codec: ResponseCodec) {
        self.codec = codec;
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

/// Reads the initial request frame, resolves its connection metadata and opens its datagram flow
/// in `session`, replying with the failure status when any of them fails. The stream slot is
/// released before that reply.
async fn read_request<K, S>(
    server: &Server,
    stream_permit: &mut Permit,
    session: Option<&WebTransportSessionState>,
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
        codec: ResponseCodec::default(),
    };
    let mut reader = BodyReader {
        source,
        chunk: Bytes::new(),
    };
    let incoming = server_transport::read_initial_request(server, &mut reader)
        .await
        .and_then(|mut incoming| {
            resolve_connection_metadata(
                session.map(|session| &session.connection_metadata),
                &mut incoming,
            )?;
            open_flow(
                session.and_then(|session| session.demux.as_deref()),
                &mut incoming,
            )?;
            Ok(incoming)
        });
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
/// `session` is the `WebTransport` session the body belongs to, if any.
pub async fn handle_rpc_stream<K, S>(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    mut stream_permit: Permit,
    session: Option<Arc<WebTransportSessionState>>,
    sink: K,
    source: S,
    shutdown: watch::Receiver<bool>,
) where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, incoming)) = read_request(
        &server,
        &mut stream_permit,
        session.as_deref(),
        sink,
        source,
    )
    .await
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
pub async fn reject_rpc_stream<K, S>(
    server: Server,
    session: Option<Arc<WebTransportSessionState>>,
    sink: K,
    source: S,
) where
    K: BodySink,
    S: BodySource,
{
    if let Some((writer, reader, incoming)) = read_request(
        &server,
        &mut Permit::none(),
        session.as_deref(),
        sink,
        source,
    )
    .await
    {
        server_transport::reject_stream(server, writer, reader, incoming).await;
    }
}
