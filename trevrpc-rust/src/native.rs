use std::future::{Future, pending};
use std::io;
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use trevrpc_native::{Admission, RecvHalf, SendHalf};
pub use trevrpc_native::{
    CloseReason, Connection, EndpointConfig, ListenerEvent, MsQuicConfig, NativeTransport,
    Protocol, TransportConfig,
};

use crate::client::{RpcTransport, StreamingRpcTransport};
use crate::client_transport::{ClientResponseBackend, read_unary_response, streaming_response};
use crate::client_upload::UploadWriter;
use crate::framing::{
    decode_frame, decode_stream_frame_body_owned, encode_frame_body_with_max,
    encode_stream_frame_body_with_max,
};
use crate::request_pump::{RequestPumpReader, RequestTransportEvent};
use crate::server::Server;
use crate::server_transport::{self, ResponseWriter, try_acquire_permit};
use crate::{BoxStream, Error, Result, RpcRequest, RpcResponse, RpcStreamFrame, Status};

fn native_frame_size(max_frame_size: usize) -> u64 {
    u64::try_from(max_frame_size).unwrap_or(u64::MAX)
}

pub(crate) fn validate_native_frame_capacity(
    max_frame_size: usize,
    max_receive_owned_bytes: u64,
) -> Result<()> {
    if native_frame_size(max_frame_size) > max_receive_owned_bytes {
        return Err(Status::invalid_argument(
            "native maximum frame size exceeds the transport receive-owned byte capacity",
        )
        .into());
    }
    Ok(())
}

/// Configuration used to establish one native C transport connection.
#[derive(Clone, Debug)]
pub struct NativeClientConfig {
    transport: TransportConfig,
    provider: MsQuicConfig,
    endpoint: EndpointConfig,
    max_frame_size: usize,
}

impl NativeClientConfig {
    /// Creates a native client configuration for an endpoint.
    #[must_use]
    pub fn new(endpoint: EndpointConfig) -> Self {
        Self {
            transport: TransportConfig::default(),
            provider: MsQuicConfig::default(),
            endpoint,
            max_frame_size: crate::framing::DEFAULT_MAX_FRAME_SIZE,
        }
    }

    /// Replaces the native transport capacity configuration.
    #[must_use]
    pub fn with_transport_config(mut self, transport: TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    /// Replaces the native provider configuration.
    #[must_use]
    pub fn with_provider_config(mut self, provider: MsQuicConfig) -> Self {
        self.provider = provider;
        self
    }

    /// Sets the maximum encoded `TrevRPC` frame-body size.
    #[must_use]
    pub const fn with_max_frame_size(mut self, max_frame_size: usize) -> Self {
        self.max_frame_size = max_frame_size;
        self
    }
}

/// Configuration for a native C transport server.
#[derive(Clone, Debug)]
pub struct NativeServerConfig {
    transport: TransportConfig,
    provider: MsQuicConfig,
    endpoint: EndpointConfig,
}

impl NativeServerConfig {
    /// Creates a native server configuration for one listener endpoint.
    #[must_use]
    pub fn new(endpoint: EndpointConfig) -> Self {
        Self {
            transport: TransportConfig::default(),
            provider: MsQuicConfig::default(),
            endpoint,
        }
    }

    /// Replaces the native transport capacity configuration.
    #[must_use]
    pub fn with_transport_config(mut self, transport: TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    /// Replaces the native provider configuration.
    #[must_use]
    pub fn with_provider_config(mut self, provider: MsQuicConfig) -> Self {
        self.provider = provider;
        self
    }
}

/// A transport-neutral RPC adapter backed by the native C Transport ABI.
#[derive(Clone)]
pub struct RawNativeTransport {
    runtime: NativeTransport,
    connection: Connection,
    max_frame_size: usize,
}

impl RawNativeTransport {
    /// Establishes a native connection and waits for its complete handshake.
    pub async fn connect(config: NativeClientConfig) -> Result<Self> {
        let NativeClientConfig {
            transport,
            provider,
            mut endpoint,
            max_frame_size,
        } = config;
        validate_native_frame_capacity(max_frame_size, transport.max_receive_owned_bytes)?;
        endpoint.max_frame_size = native_frame_size(max_frame_size);
        let runtime = NativeTransport::new_with_provider(transport, provider)
            .await
            .map_err(Error::transport)?;
        let connection = match runtime.dial(endpoint).await {
            Ok(connection) => connection,
            Err(error) => {
                let _ = runtime.close().await;
                return Err(Error::transport(error));
            }
        };
        Ok(Self::from_connection(runtime, connection, max_frame_size))
    }

    /// Creates a raw transport over a connection owned by an existing native runtime.
    pub(crate) fn from_connection(
        runtime: NativeTransport,
        connection: Connection,
        max_frame_size: usize,
    ) -> Self {
        Self {
            runtime,
            connection,
            max_frame_size,
        }
    }

    /// Returns the maximum encoded frame-body size.
    #[must_use]
    pub const fn max_frame_size(&self) -> usize {
        self.max_frame_size
    }

    /// Returns a clone of the established low-level connection.
    #[must_use]
    pub fn connection(&self) -> Connection {
        self.connection.clone()
    }

    /// Closes the connection and its owning native transport.
    pub async fn close(&self) -> Result<()> {
        let connection = self.connection.close(0).await;
        let transport = self.runtime.close().await;
        connection.map_err(Error::transport)?;
        transport.map_err(Error::transport)
    }
}

#[crate::async_trait]
impl RpcTransport for RawNativeTransport {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse> {
        let stream = self.connection.open_bi().await.map_err(Error::transport)?;
        let (send, receive) = stream.split();
        let body = encode_frame_body_with_max(&request, self.max_frame_size)?;
        send.send(body).await.map_err(Error::transport)?;
        send.finish().await.map_err(Error::transport)?;

        read_unary_response(NativeResponseBackend::new(
            receive,
            NativeResponseLease {
                _runtime: self.runtime.clone(),
                _connection: self.connection.clone(),
            },
            self.max_frame_size,
        ))
        .await
    }
}

#[crate::async_trait]
impl StreamingRpcTransport for RawNativeTransport {
    async fn streaming_call(
        &self,
        request: RpcRequest,
        request_body: BoxStream<Vec<u8>>,
    ) -> Result<BoxStream<RpcStreamFrame>> {
        let stream = self.connection.open_bi().await.map_err(Error::transport)?;
        let (send, receive) = stream.split();
        let max_frame_size = self.max_frame_size;
        let writer = UploadWriter::spawn(write_streaming_request(
            send,
            request,
            request_body,
            max_frame_size,
        ));
        Ok(streaming_response(NativeResponseBackend::with_writer(
            receive,
            writer,
            NativeResponseLease {
                _runtime: self.runtime.clone(),
                _connection: self.connection.clone(),
            },
            max_frame_size,
        )))
    }
}

struct NativeResponseLease {
    _runtime: NativeTransport,
    _connection: Connection,
}

struct NativeResponseBackend {
    receive: RecvHalf,
    writer: Option<UploadWriter>,
    max_frame_size: usize,
    _lease: NativeResponseLease,
}

impl NativeResponseBackend {
    const fn new(receive: RecvHalf, lease: NativeResponseLease, max_frame_size: usize) -> Self {
        Self {
            receive,
            writer: None,
            max_frame_size,
            _lease: lease,
        }
    }

    const fn with_writer(
        receive: RecvHalf,
        writer: UploadWriter,
        lease: NativeResponseLease,
        max_frame_size: usize,
    ) -> Self {
        Self {
            receive,
            writer: Some(writer),
            max_frame_size,
            _lease: lease,
        }
    }
}

impl ClientResponseBackend for NativeResponseBackend {
    async fn read_unary_response(&mut self) -> Result<RpcResponse> {
        let body = self
            .receive
            .receive()
            .await
            .map_err(Error::transport)?
            .ok_or_else(|| {
                unexpected_eof("native unary response ended before its response frame")
            })?;
        if body.as_bytes().len() > self.max_frame_size {
            return Err(Error::FrameTooLarge {
                len: body.as_bytes().len(),
                max: self.max_frame_size,
            });
        }
        decode_frame(body.as_bytes())
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        match self.receive.receive().await.map_err(Error::transport)? {
            None => Ok(()),
            Some(_) => Err(protocol_error(
                "native unary response contained more than one response frame",
            )),
        }
    }

    async fn read_stream_frame(&mut self) -> Result<Option<RpcStreamFrame>> {
        let Some(body) = self.receive.receive().await.map_err(Error::transport)? else {
            return Ok(None);
        };
        if body.as_bytes().len() > self.max_frame_size {
            return Err(Error::FrameTooLarge {
                len: body.as_bytes().len(),
                max: self.max_frame_size,
            });
        }
        decode_stream_frame_body_owned(body.into_bytes()).map(Some)
    }

    async fn abort_upload_and_settle(&mut self) -> Result<()> {
        match &mut self.writer {
            Some(writer) => writer.abort_and_settle().await,
            None => Ok(()),
        }
    }

    async fn drain_after_terminal_status(&mut self) -> Result<()> {
        match self.receive.receive().await.map_err(Error::transport)? {
            None => Ok(()),
            Some(_) => Err(protocol_error(
                "native response stream contained a frame after terminal status",
            )),
        }
    }

    fn mark_complete(&mut self) {}
}

async fn write_streaming_request(
    send: SendHalf,
    request: RpcRequest,
    mut request_body: BoxStream<Vec<u8>>,
    max_frame_size: usize,
) -> Result<()> {
    let request = encode_frame_body_with_max(&request, max_frame_size)?;
    send.send(request).await.map_err(Error::transport)?;

    while let Some(body) = request_body.next().await {
        let frame = RpcStreamFrame::message(body?);
        let body = encode_stream_frame_body_with_max(&frame, max_frame_size)?;
        send.send(body).await.map_err(Error::transport)?;
    }
    send.finish().await.map_err(Error::transport)
}

fn unexpected_eof(message: &'static str) -> Error {
    Error::transport(io::Error::new(io::ErrorKind::UnexpectedEof, message))
}

fn protocol_error(message: &'static str) -> Error {
    Error::transport(io::Error::new(io::ErrorKind::InvalidData, message))
}

struct NativeResponseWriter {
    send: Option<SendHalf>,
    max_frame_size: usize,
}

impl NativeResponseWriter {
    const fn new(send: SendHalf, max_frame_size: usize) -> Self {
        Self {
            send: Some(send),
            max_frame_size,
        }
    }

    fn send(&self) -> Result<&SendHalf> {
        self.send.as_ref().ok_or_else(|| {
            Error::transport(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "native response stream is no longer writable",
            ))
        })
    }
}

impl ResponseWriter for NativeResponseWriter {
    async fn write_response(&mut self, response: &RpcResponse) -> Result<()> {
        let body = encode_frame_body_with_max(response, self.max_frame_size)?;
        self.send()?.send(body).await.map_err(Error::transport)
    }

    async fn write_message_stream_frames(&mut self, bodies: &mut Vec<Vec<u8>>) -> Result<()> {
        let send = self.send()?;
        for body in bodies.drain(..) {
            let frame = RpcStreamFrame::message(body);
            let encoded = encode_stream_frame_body_with_max(&frame, self.max_frame_size)?;
            send.send(encoded).await.map_err(Error::transport)?;
        }
        Ok(())
    }

    async fn write_stream_frame(&mut self, frame: RpcStreamFrame) -> Result<()> {
        let body = encode_stream_frame_body_with_max(&frame, self.max_frame_size)?;
        self.send()?.send(body).await.map_err(Error::transport)
    }

    async fn stopped(&mut self) -> Result<()> {
        let Some(send) = &self.send else {
            return pending().await;
        };
        match send.stopped().await.map_err(Error::transport)? {
            CloseReason::Peer { .. } => Ok(()),
            reason => Err(native_closed_error(reason)),
        }
    }

    fn reset(&mut self) {
        // Dropping an unfinished native send half schedules the provider's reset and
        // closes the logical stream without blocking this synchronous trait method.
        self.send.take();
    }

    async fn finish(&mut self, _detail: &'static str) -> Result<()> {
        self.send()?.finish().await.map_err(Error::transport)
    }
}

struct NativePumpReader {
    receive: Option<RecvHalf>,
    receive_end_observed: bool,
}

impl NativePumpReader {
    const fn new(receive: RecvHalf) -> Self {
        Self {
            receive: Some(receive),
            receive_end_observed: false,
        }
    }

    fn receive(&self) -> Result<&RecvHalf> {
        self.receive.as_ref().ok_or_else(|| {
            Error::transport(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "native request stream is no longer readable",
            ))
        })
    }
}

impl RequestPumpReader for NativePumpReader {
    fn stop_trevrpc(&mut self) {
        // Dropping an unfinished native receive half schedules STOP_SENDING through
        // the safe native transport driver's cleanup path.
        self.receive.take();
    }

    async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
        let Some(receive) = &self.receive else {
            return pending().await;
        };
        if self.receive_end_observed {
            return Some(map_native_stream_close(receive.closed().await));
        }
        tokio::select! {
            biased;
            ended = receive.ended() => {
                self.receive_end_observed = true;
                match ended {
                    Ok(()) => None,
                    Err(error) if error.is_peer_reset() => {
                        Some(RequestTransportEvent::PeerReset(Status::cancelled(
                            "native request stream reset by peer",
                        )))
                    }
                    Err(error) => Some(RequestTransportEvent::ConnectionLost(
                        Error::transport(error).into_status(),
                    )),
                }
            }
            closed = receive.closed() => Some(map_native_stream_close(closed)),
        }
    }

    async fn read_unary_end(&mut self) -> Result<()> {
        match self.receive()?.receive().await.map_err(Error::transport)? {
            None => Ok(()),
            Some(_) => Err(Error::from(Status::invalid_argument(
                "unary request stream contained data after the initial request frame",
            ))),
        }
    }

    async fn read_stream_frame_or_eof(
        &mut self,
        max_frame_size: usize,
    ) -> Result<Option<RpcStreamFrame>> {
        let Some(body) = self.receive()?.receive().await.map_err(Error::transport)? else {
            return Ok(None);
        };
        if body.as_bytes().len() > max_frame_size {
            return Err(Error::FrameTooLarge {
                len: body.as_bytes().len(),
                max: max_frame_size,
            });
        }
        decode_stream_frame_body_owned(body.into_bytes()).map(Some)
    }

    async fn validate_transport_end(&mut self) -> Result<()> {
        Ok(())
    }
}

fn map_native_stream_close(
    closed: std::result::Result<CloseReason, trevrpc_native::NativeError>,
) -> RequestTransportEvent {
    match closed {
        Ok(CloseReason::Peer {
            peer_reset: true, ..
        }) => RequestTransportEvent::PeerReset(Status::cancelled(
            "native request stream reset by peer",
        )),
        Ok(_) => RequestTransportEvent::ConnectionLost(Status::unavailable(
            "native request stream closed",
        )),
        Err(error) => RequestTransportEvent::ConnectionLost(Error::transport(error).into_status()),
    }
}

fn native_closed_error(reason: CloseReason) -> Error {
    let kind = match reason {
        CloseReason::Peer {
            peer_reset: true, ..
        } => io::ErrorKind::ConnectionReset,
        CloseReason::Peer { .. } | CloseReason::Clean | CloseReason::Local { .. } => {
            io::ErrorKind::BrokenPipe
        }
        CloseReason::Transport { .. } | CloseReason::Failed { .. } => {
            io::ErrorKind::ConnectionAborted
        }
    };
    Error::transport(io::Error::new(
        kind,
        format!("native stream closed: {reason:?}"),
    ))
}

impl Server {
    /// Serves native `TrevRPC` streams using the C transport ABI.
    pub async fn serve_native(self, config: NativeServerConfig) -> Result<()> {
        self.serve_native_with_shutdown(config, pending::<()>())
            .await
    }

    /// Serves native `TrevRPC` streams until the supplied shutdown future completes.
    pub async fn serve_native_with_shutdown<S>(
        self,
        config: NativeServerConfig,
        shutdown: S,
    ) -> Result<()>
    where
        S: Future<Output = ()> + Send,
    {
        let NativeServerConfig {
            transport,
            provider,
            mut endpoint,
        } = config;
        validate_native_frame_capacity(self.max_frame_size(), transport.max_receive_owned_bytes)?;
        configure_native_server_endpoint(&self, &mut endpoint)?;
        let runtime = NativeTransport::new_with_provider(transport, provider)
            .await
            .map_err(Error::transport)?;
        let result = serve_native_on(self, runtime.clone(), endpoint, shutdown).await;
        let close = runtime.close().await.map_err(Error::transport);
        result.and(close)
    }
}

fn configure_native_server_endpoint(server: &Server, endpoint: &mut EndpointConfig) -> Result<()> {
    endpoint.max_frame_size = native_frame_size(server.max_frame_size());
    match endpoint.protocol {
        Protocol::Native | Protocol::Auto => {}
        Protocol::Http3 => {
            endpoint.path = Some(server.options().http3_path().to_owned());
            endpoint.defer_admission = true;
        }
        Protocol::WebTransport => {
            endpoint.path = Some(server.options().webtransport_path().to_owned());
            endpoint.defer_admission = true;
        }
        Protocol::Multiplexed => {
            if server.options().http3_path() != server.options().webtransport_path() {
                return Err(Status::invalid_argument(
                    "native multiplexed listeners require matching HTTP/3 and WebTransport paths",
                )
                .into());
            }
            endpoint.path = Some(server.options().http3_path().to_owned());
            endpoint.defer_admission = true;
        }
    }
    Ok(())
}

async fn serve_native_on<S>(
    server: Server,
    runtime: NativeTransport,
    endpoint: EndpointConfig,
    shutdown: S,
) -> Result<()>
where
    S: Future<Output = ()> + Send,
{
    let listener = runtime.listen(endpoint).await.map_err(Error::transport)?;
    let connection_limit = server
        .options()
        .max_concurrent_connections()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let request_limit = server
        .options()
        .max_concurrent_requests()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut connection_tasks = JoinSet::new();
    tokio::pin!(shutdown);

    let result = loop {
        tokio::select! {
            event = listener.accept() => {
                let event = event.map_err(Error::transport)?;
                let Some(event) = event else { break Ok(()); };
                match event {
                    ListenerEvent::Connection(connection) => {
                        let Some(connection_permit) = try_acquire_permit(connection_limit.as_ref()) else {
                            let _ = connection.close(0).await;
                            continue;
                        };
                        let server = server.clone();
                        let request_limit = request_limit.clone();
                        let shutdown = shutdown_rx.clone();
                        connection_tasks.spawn(async move {
                            let _connection_permit = connection_permit;
                            handle_native_connection(server, connection, request_limit, shutdown).await;
                        });
                    }
                    ListenerEvent::Http3Admission(admission) => {
                        handle_http3_admission(&server, admission).await;
                    }
                    ListenerEvent::WebTransportAdmission(admission) => {
                        handle_webtransport_admission(&server, admission).await;
                    }
                }
            }
            () = &mut shutdown => {
                let _ = shutdown_tx.send(true);
                break Ok(());
            }
            result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "native connection task failed");
                }
            }
        }
    };

    let _ = shutdown_tx.send(true);
    let _ = listener.close().await;
    drain_native_connections(
        &mut connection_tasks,
        server.options().graceful_shutdown_timeout(),
    )
    .await;
    result
}

async fn handle_native_connection(
    server: Server,
    connection: Connection,
    request_limit: Option<Arc<Semaphore>>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stream_limit = server
        .options()
        .max_concurrent_streams_per_connection()
        .map(|limit| Arc::new(Semaphore::new(limit)));
    let mut stream_tasks = JoinSet::new();

    loop {
        tokio::select! {
            accepted = connection.accept_bi(), if !*shutdown.borrow() => {
                let Ok(Some(stream)) = accepted else { break; };
                let Some(stream_permit) = try_acquire_permit(stream_limit.as_ref()) else {
                    let server = server.clone();
                    stream_tasks.spawn(handle_native_rejected_stream(server, stream));
                    continue;
                };
                let server = server.clone();
                let request_limit = request_limit.clone();
                let stream_shutdown = shutdown.clone();
                stream_tasks.spawn(async move {
                    let _stream_permit = stream_permit;
                    handle_native_stream(server, request_limit, stream, stream_shutdown).await;
                });
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            _ = connection.closed() => {
                break;
            }
            result = stream_tasks.join_next(), if !stream_tasks.is_empty() => {
                if let Some(Err(error)) = result {
                    let _ = &error;
                    #[cfg(feature = "tracing")]
                    tracing::warn!(%error, "native stream task failed");
                }
            }
        }
    }

    drain_native_streams(
        &mut stream_tasks,
        server.options().graceful_shutdown_timeout(),
    )
    .await;
    if *shutdown.borrow() {
        let _ = connection.close(0).await;
    }
}

async fn handle_native_stream(
    server: Server,
    request_limit: Option<Arc<Semaphore>>,
    stream: trevrpc_native::BiStream,
    shutdown: watch::Receiver<bool>,
) {
    let max_frame_size = server.max_frame_size();
    let (send, recv) = stream.split();
    let request = match read_native_initial_request(&server, &recv).await {
        Ok(request) => request,
        Err(error) => {
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(NativeResponseWriter::new(send, max_frame_size), status)
                .await;
            return;
        }
    };
    server_transport::handle_stream(
        server,
        request_limit,
        NativeResponseWriter::new(send, max_frame_size),
        NativePumpReader::new(recv),
        request,
        shutdown,
    )
    .await;
}

async fn handle_native_rejected_stream(server: Server, stream: trevrpc_native::BiStream) {
    let max_frame_size = server.max_frame_size();
    let (send, recv) = stream.split();
    let request = match read_native_initial_request(&server, &recv).await {
        Ok(request) => request,
        Err(error) => {
            let status = error.into_status();
            server.record_pre_handler_failure(&status);
            server_transport::write_status(NativeResponseWriter::new(send, max_frame_size), status)
                .await;
            return;
        }
    };
    server_transport::reject_stream(
        server,
        NativeResponseWriter::new(send, max_frame_size),
        NativePumpReader::new(recv),
        request,
    )
    .await;
}

async fn read_native_initial_request(server: &Server, receive: &RecvHalf) -> Result<RpcRequest> {
    let read = async {
        let body = receive
            .receive()
            .await
            .map_err(Error::transport)?
            .ok_or_else(|| unexpected_eof("native stream ended before its request frame"))?;
        if body.as_bytes().len() > server.max_frame_size() {
            return Err(Error::FrameTooLarge {
                len: body.as_bytes().len(),
                max: server.max_frame_size(),
            });
        }
        decode_frame(body.as_bytes())
    };
    if let Some(timeout) = server.options().initial_request_timeout() {
        tokio::time::timeout(timeout, read)
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("initial request frame timeout")))?
    } else {
        read.await
    }
}

async fn drain_native_streams(streams: &mut JoinSet<()>, timeout: Option<std::time::Duration>) {
    let drain = async {
        while let Some(result) = streams.join_next().await {
            if let Err(error) = result {
                let _ = &error;
                #[cfg(feature = "tracing")]
                tracing::warn!(%error, "native stream task failed while draining");
            }
        }
    };
    if let Some(timeout) = timeout {
        if tokio::time::timeout(timeout, drain).await.is_err() {
            streams.abort_all();
            while streams.join_next().await.is_some() {}
        }
    } else {
        drain.await;
    }
}

async fn drain_native_connections(
    connections: &mut JoinSet<()>,
    timeout: Option<std::time::Duration>,
) {
    drain_native_streams(connections, timeout).await;
}

async fn handle_webtransport_admission(server: &Server, admission: Admission) {
    let status = webtransport_admission_status(server, admission.request());
    let result = match status {
        Some(status) => admission.reject(status).await,
        None => admission.accept().await,
    };
    if let Err(error) = result {
        let _ = &error;
        #[cfg(feature = "tracing")]
        tracing::debug!(%error, "native WebTransport admission response failed");
    }
}

fn webtransport_admission_status(
    server: &Server,
    request: &trevrpc_native::AdmissionRequest,
) -> Option<u16> {
    let Ok(path) = std::str::from_utf8(&request.path) else {
        return Some(400);
    };
    let authority = if request.authority.is_empty() {
        None
    } else {
        match std::str::from_utf8(&request.authority) {
            Ok(authority) => Some(authority),
            Err(_) => return Some(400),
        }
    };
    let origin = if request.origin.is_empty() {
        None
    } else {
        match std::str::from_utf8(&request.origin) {
            Ok(origin) => Some(origin),
            Err(_) => return Some(400),
        }
    };
    let Some(headers) = native_admission_headers(&request.headers) else {
        return Some(400);
    };
    if let Some(admission) = server.options().webtransport_admission() {
        let accepted = admission(&crate::server::WebTransportAdmissionRequest {
            path,
            authority,
            origin,
            secure: true,
            headers: &headers,
        });
        return (!accepted).then_some(403);
    }
    if path != server.options().webtransport_path() {
        return Some(404);
    }
    if !server
        .options()
        .webtransport_allowed_authorities()
        .is_empty()
        && !authority.is_some_and(|value| {
            server
                .options()
                .webtransport_allowed_authorities()
                .iter()
                .any(|allowed| allowed == value || allowed == authority_host(value))
        })
    {
        return Some(403);
    }
    if origin.is_some_and(|value| {
        !server
            .options()
            .webtransport_allowed_origins()
            .iter()
            .any(|allowed| allowed == value)
    }) {
        return Some(403);
    }
    None
}

async fn handle_http3_admission(server: &Server, admission: Admission) {
    let status = http3_admission_status(server, admission.request());
    let result = match status {
        Some(status) => admission.reject(status).await,
        None => admission.accept().await,
    };
    if let Err(error) = result {
        let _ = &error;
        #[cfg(feature = "tracing")]
        tracing::debug!(%error, "native HTTP/3 admission response failed");
    }
}

fn http3_admission_status(
    server: &Server,
    request: &trevrpc_native::AdmissionRequest,
) -> Option<u16> {
    use http::{Method, Request, Uri, header};

    if !server.options().http3_enabled() {
        return Some(404);
    }
    let Ok(method) = Method::from_bytes(&request.method) else {
        return Some(400);
    };
    let Ok(path) = std::str::from_utf8(&request.path) else {
        return Some(400);
    };
    let uri = if request.authority.is_empty() {
        path.to_owned()
    } else {
        let Ok(authority) = std::str::from_utf8(&request.authority) else {
            return Some(400);
        };
        format!("https://{authority}{path}")
    };
    let Ok(uri) = uri.parse::<Uri>() else {
        return Some(400);
    };
    if method != Method::POST {
        return Some(405);
    }
    let mut builder = Request::builder().method(method.clone()).uri(uri);
    let mut headers = http::HeaderMap::new();
    for (name, value) in &request.headers {
        let Ok(name) = header::HeaderName::from_bytes(name) else {
            return Some(400);
        };
        let Ok(value) = header::HeaderValue::from_bytes(value) else {
            return Some(400);
        };
        headers.append(name, value);
    }
    let request_headers = headers.clone();
    let Some(target) = builder.headers_mut() else {
        return Some(400);
    };
    *target = headers;
    let Ok(request) = builder.body(()) else {
        return Some(400);
    };
    if request.uri().path() != server.options().http3_path() {
        return Some(404);
    }
    let content_type = request_headers.get_all(header::CONTENT_TYPE);
    let mut values = content_type.iter();
    if values.next().is_none() || values.next().is_some() {
        return Some(415);
    }
    if !content_type.iter().next().is_some_and(|value| {
        value
            .as_bytes()
            .eq_ignore_ascii_case(b"application/trevrpc")
    }) {
        return Some(415);
    }
    let header_values = request_headers_to_vec(&request_headers);
    let Some(headers) = native_admission_headers(&header_values) else {
        return Some(400);
    };
    if let Some(admission) = server.options().http3_admission() {
        let accepted = admission(&crate::server::Http3AdmissionRequest {
            request: &request,
            path: request.uri().path(),
            authority: request.uri().authority().map(http::uri::Authority::as_str),
            secure: true,
            headers: &headers,
        });
        return (!accepted).then_some(403);
    }
    None
}

fn request_headers_to_vec(headers: &http::HeaderMap) -> Vec<(Vec<u8>, Vec<u8>)> {
    headers
        .iter()
        .map(|(name, value)| (name.as_str().as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect()
}

fn native_admission_headers(
    headers: &[(Vec<u8>, Vec<u8>)],
) -> Option<Vec<crate::server::AdmissionHeader<'_>>> {
    headers
        .iter()
        .map(|(name, value)| {
            Some(crate::server::AdmissionHeader {
                name: std::str::from_utf8(name).ok()?,
                value,
            })
        })
        .collect()
}

fn authority_host(authority: &str) -> &str {
    if let Some(authority) = authority.strip_prefix('[')
        && let Some((host, _)) = authority.split_once(']')
    {
        return host;
    }
    authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host)
}

#[cfg(test)]
mod tests {
    use super::{
        Server, authority_host, configure_native_server_endpoint, validate_native_frame_capacity,
        webtransport_admission_status,
    };
    use trevrpc_native::{AdmissionRequest, EndpointConfig, Protocol, TransportConfig};

    fn admission(path: &[u8]) -> AdmissionRequest {
        AdmissionRequest {
            protocol: Protocol::WebTransport,
            headers: Vec::new(),
            method: b"CONNECT".to_vec(),
            path: path.to_vec(),
            authority: b"rpc.example:443".to_vec(),
            origin: Vec::new(),
        }
    }

    #[test]
    fn native_admission_uses_safe_webtransport_path_rules() {
        let server = Server::new();
        assert_eq!(
            webtransport_admission_status(&server, &admission(b"/wrong")),
            Some(404)
        );
        assert_eq!(
            webtransport_admission_status(&server, &admission(b"/trevrpc")),
            None
        );
    }

    #[test]
    fn native_admission_rejects_invalid_utf8_without_accepting_it() {
        let server = Server::new();
        let request = admission(b"/trevrpc");
        let mut invalid = request;
        invalid.path = vec![0xff];
        assert_eq!(webtransport_admission_status(&server, &invalid), Some(400));
    }

    #[test]
    fn native_authority_matching_handles_ipv6_and_ports() {
        assert_eq!(authority_host("rpc.example:443"), "rpc.example");
        assert_eq!(authority_host("[::1]:443"), "::1");
        assert_eq!(authority_host("rpc.example"), "rpc.example");
    }

    #[test]
    fn native_server_synchronizes_protocol_paths_and_admission() {
        let server = Server::new();
        let mut endpoint = EndpointConfig {
            protocol: Protocol::Http3,
            ..EndpointConfig::default()
        };

        configure_native_server_endpoint(&server, &mut endpoint).unwrap();

        assert_eq!(endpoint.path.as_deref(), Some("/trevrpc"));
        assert!(endpoint.defer_admission);
        assert_eq!(endpoint.max_frame_size, server.max_frame_size() as u64);

        let mut native_endpoint = EndpointConfig::default();
        configure_native_server_endpoint(&server, &mut native_endpoint).unwrap();
        assert!(!native_endpoint.defer_admission);
    }

    #[test]
    fn native_frame_limit_must_fit_transport_receive_capacity() {
        let status = validate_native_frame_capacity(1025, 1024)
            .expect_err("one complete native frame must fit the transport receive budget")
            .into_status();

        assert_eq!(status.code(), crate::Code::InvalidArgument);
        validate_native_frame_capacity(1024, 1024).unwrap();
    }

    #[test]
    fn native_multiplexed_server_rejects_distinct_protocol_paths() {
        let mut server = Server::new();
        server.set_http3_path("/http3");
        server.set_webtransport_path("/webtransport");
        let mut endpoint = EndpointConfig {
            protocol: Protocol::Multiplexed,
            ..EndpointConfig::default()
        };

        let status = configure_native_server_endpoint(&server, &mut endpoint)
            .expect_err("one C listener cannot enforce two protocol paths")
            .into_status();

        assert_eq!(status.code(), crate::Code::InvalidArgument);
    }
}
