use std::future::Future;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::framed::{self, FrameRead, FrameTrace};
use crate::server::{CancellationSource, check_stream_message_body_limits};
use crate::{BoxStream, Code, Error, Result, RpcStreamFrameKind, Status};

pub(crate) const REQUEST_BODY_CHANNEL_CAPACITY: usize = 1;

#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestInputKind {
    Unary,
    Streaming {
        max_messages: Option<usize>,
        max_body_size: Option<usize>,
    },
}

impl RequestInputKind {
    pub(crate) const fn for_rpc_kind(
        rpc_kind: crate::RpcKind,
        max_messages: Option<usize>,
        max_body_size: Option<usize>,
    ) -> Self {
        match rpc_kind {
            crate::RpcKind::Unary | crate::RpcKind::ServerStreaming => Self::Unary,
            crate::RpcKind::ClientStreaming | crate::RpcKind::BidirectionalStreaming => {
                Self::Streaming {
                    max_messages,
                    max_body_size,
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RequestPumpFailure {
    Protocol(Status),
    PeerStatus(Status),
    PeerReset(Status),
    ConnectionLost(Status),
}

impl RequestPumpFailure {
    pub(crate) const fn status(&self) -> &Status {
        match self {
            Self::Protocol(status)
            | Self::PeerStatus(status)
            | Self::PeerReset(status)
            | Self::ConnectionLost(status) => status,
        }
    }

    pub(crate) const fn cancellation_source(&self) -> Option<CancellationSource> {
        match self {
            Self::PeerStatus(_) | Self::PeerReset(_) => Some(CancellationSource::PeerReset),
            Self::ConnectionLost(_) => Some(CancellationSource::ConnectionLost),
            Self::Protocol(_) => None,
        }
    }

    pub(crate) const fn response_writable(&self) -> bool {
        matches!(self, Self::Protocol(_))
    }
}

#[derive(Clone, Debug)]
pub(crate) enum RequestPumpState {
    Receiving,
    DrainingTerminalFin,
    Failed(RequestPumpFailure),
    Settled(RequestPumpOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RequestPumpOutcome {
    CleanFin,
    LocallyStopped,
    PeerReset,
    ConnectionLost,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum RequestPumpSettle {
    ResponseCommitted,
    ResponseStopped,
    ServerShutdown,
    ConnectionLost,
}

#[derive(Clone, Debug)]
pub(crate) enum RequestTransportEvent {
    PeerReset(Status),
    ConnectionLost(Status),
}

pub(crate) trait RequestPumpReader: FrameRead + Send + 'static {
    fn stop_trevrpc(&mut self);

    fn backpressure_event(&mut self) -> impl Future<Output = Option<RequestTransportEvent>> + Send;

    fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send;
}

struct PumpReaderGuard<R: RequestPumpReader> {
    reader: R,
    settled: bool,
}

impl<R: RequestPumpReader> PumpReaderGuard<R> {
    const fn new(reader: R) -> Self {
        Self {
            reader,
            settled: false,
        }
    }
}

impl<R: RequestPumpReader> Drop for PumpReaderGuard<R> {
    fn drop(&mut self) {
        if !self.settled {
            self.reader.stop_trevrpc();
        }
    }
}

pub(crate) struct RequestPump {
    state: watch::Receiver<RequestPumpState>,
    settle: watch::Sender<Option<RequestPumpSettle>>,
    task: Option<JoinHandle<RequestPumpOutcome>>,
}

impl RequestPump {
    pub(crate) async fn failure(&mut self) -> RequestPumpFailure {
        loop {
            let settled = match &*self.state.borrow_and_update() {
                RequestPumpState::Failed(failure) => return failure.clone(),
                RequestPumpState::Settled(outcome) => {
                    let _ = outcome;
                    true
                }
                RequestPumpState::Receiving | RequestPumpState::DrainingTerminalFin => false,
            };
            if self.state.changed().await.is_err() {
                if settled {
                    std::future::pending::<()>().await;
                }
                return RequestPumpFailure::Protocol(Status::internal(
                    "request pump task terminated unexpectedly",
                ));
            }
        }
    }

    pub(crate) async fn settle(mut self, reason: RequestPumpSettle) -> RequestPumpOutcome {
        let _ = self.settle.send(Some(reason));
        let Some(task) = self.task.take() else {
            return RequestPumpOutcome::LocallyStopped;
        };
        match task.await {
            Ok(outcome) => outcome,
            Err(_) => RequestPumpOutcome::LocallyStopped,
        }
    }
}

impl Drop for RequestPump {
    fn drop(&mut self) {
        let _ = self.settle.send(Some(RequestPumpSettle::ResponseStopped));
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

pub(crate) fn start_request_pump<R, T>(
    reader: R,
    input_kind: RequestInputKind,
    max_frame_size: usize,
) -> (BoxStream<Vec<u8>>, RequestPump)
where
    R: RequestPumpReader,
    T: FrameTrace + Send + 'static,
{
    let (body_tx, body_rx) = mpsc::channel(REQUEST_BODY_CHANNEL_CAPACITY);
    let (state_tx, state_rx) = watch::channel(RequestPumpState::Receiving);
    let (settle_tx, settle_rx) = watch::channel(None);
    let task = tokio::spawn(run_request_pump::<R, T>(
        reader,
        input_kind,
        max_frame_size,
        body_tx,
        state_tx,
        settle_rx,
    ));
    let body = Box::pin(futures_util::stream::unfold(
        body_rx,
        |mut receiver| async move { receiver.recv().await.map(|item| (item, receiver)) },
    ));
    (
        body,
        RequestPump {
            state: state_rx,
            settle: settle_tx,
            task: Some(task),
        },
    )
}

async fn run_request_pump<R, T>(
    reader: R,
    input_kind: RequestInputKind,
    max_frame_size: usize,
    body_tx: mpsc::Sender<Result<Vec<u8>>>,
    state_tx: watch::Sender<RequestPumpState>,
    mut settle_rx: watch::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
    T: FrameTrace,
{
    let mut reader = PumpReaderGuard::new(reader);
    let outcome = match input_kind {
        RequestInputKind::Unary => {
            run_unary_pump::<R>(&mut reader, &state_tx, &mut settle_rx).await
        }
        RequestInputKind::Streaming {
            max_messages,
            max_body_size,
        } => {
            run_streaming_pump::<R, T>(
                &mut reader,
                max_frame_size,
                max_messages,
                max_body_size,
                &body_tx,
                &state_tx,
                &mut settle_rx,
            )
            .await
        }
    };
    let _ = state_tx.send(RequestPumpState::Settled(outcome));
    outcome
}

async fn run_unary_pump<R>(
    reader: &mut PumpReaderGuard<R>,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut watch::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    let mut byte = [0_u8; 1];
    loop {
        let read = tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                let reason = settle_rx
                    .borrow_and_update()
                    .unwrap_or(RequestPumpSettle::ResponseStopped);
                if matches!(reason, RequestPumpSettle::ResponseCommitted) {
                    continue;
                }
                return settle_reader(reader, reason);
            }
            read = reader.reader.read_frame_bytes(&mut byte) => read,
        };
        match read {
            Ok(None) => {
                if let Err(error) = reader.reader.validate_transport_end().await {
                    return fail_and_wait(reader, classify_error(error), state_tx, settle_rx, None)
                        .await;
                }
                reader.settled = true;
                return RequestPumpOutcome::CleanFin;
            }
            Ok(Some(0)) => {}
            Ok(Some(_)) => {
                let failure = RequestPumpFailure::Protocol(Status::invalid_argument(
                    "unary request stream contained data after the initial request frame",
                ));
                return fail_and_wait(reader, failure, state_tx, settle_rx, None).await;
            }
            Err(error) => {
                return fail_and_wait(reader, classify_error(error), state_tx, settle_rx, None)
                    .await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_streaming_pump<R, T>(
    reader: &mut PumpReaderGuard<R>,
    max_frame_size: usize,
    max_messages: Option<usize>,
    max_body_size: Option<usize>,
    body_tx: &mpsc::Sender<Result<Vec<u8>>>,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut watch::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
    T: FrameTrace,
{
    let mut messages = 0_usize;
    let mut body_size = 0_usize;
    loop {
        let permit = tokio::select! {
            biased;
            event = reader.reader.backpressure_event() => {
                match event {
                    Some(RequestTransportEvent::PeerReset(status)) => {
                        reader.settled = true;
                        return fail_and_wait(
                            reader,
                            RequestPumpFailure::PeerReset(status),
                            state_tx,
                            settle_rx,
                            None,
                        ).await;
                    }
                    Some(RequestTransportEvent::ConnectionLost(status)) => {
                        reader.settled = true;
                        return fail_and_wait(
                            reader,
                            RequestPumpFailure::ConnectionLost(status),
                            state_tx,
                            settle_rx,
                            None,
                        ).await;
                    }
                    None => continue,
                }
            }
            changed = settle_rx.changed() => {
                let _ = changed;
                return settle_reader(reader, settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped));
            }
            permit = body_tx.reserve() => {
                if let Ok(permit) = permit {
                    permit
                } else {
                    reader.reader.stop_trevrpc();
                    reader.settled = true;
                    return RequestPumpOutcome::LocallyStopped;
                }
            }
        };

        let frame = tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                drop(permit);
                return settle_reader(reader, settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped));
            }
            frame = framed::read_stream_frame_or_eof::<_, T>(&mut reader.reader, max_frame_size) => frame,
        };

        match frame {
            Ok(Some(frame)) => match frame.frame_kind() {
                Some(RpcStreamFrameKind::Message) => {
                    if let Err(error) = check_stream_message_body_limits(
                        "request",
                        max_messages,
                        max_body_size,
                        &mut messages,
                        &mut body_size,
                        frame.body.len(),
                    ) {
                        let failure = RequestPumpFailure::Protocol(error.into_status());
                        return fail_and_wait(reader, failure, state_tx, settle_rx, Some(permit))
                            .await;
                    }
                    permit.send(Ok(frame.body));
                }
                Some(RpcStreamFrameKind::Status) => {
                    drop(permit);
                    let status = frame.status_value();
                    let _ = state_tx.send(RequestPumpState::DrainingTerminalFin);
                    match drain_terminal::<R, T>(reader, max_frame_size, settle_rx).await {
                        Ok(RequestPumpOutcome::CleanFin) if status.is_ok() => {
                            reader.settled = true;
                            return RequestPumpOutcome::CleanFin;
                        }
                        Ok(RequestPumpOutcome::CleanFin) => {
                            reader.settled = true;
                            return fail_and_wait(
                                reader,
                                RequestPumpFailure::PeerStatus(status),
                                state_tx,
                                settle_rx,
                                None,
                            )
                            .await;
                        }
                        Ok(outcome) => return outcome,
                        Err(error) => {
                            return fail_and_wait(
                                reader,
                                classify_error(error),
                                state_tx,
                                settle_rx,
                                None,
                            )
                            .await;
                        }
                    }
                }
                None => {
                    let failure = RequestPumpFailure::Protocol(Status::invalid_argument(
                        "request stream contained an unknown frame kind",
                    ));
                    return fail_and_wait(reader, failure, state_tx, settle_rx, Some(permit)).await;
                }
            },
            Ok(None) => {
                drop(permit);
                if let Err(error) = reader.reader.validate_transport_end().await {
                    return fail_and_wait(reader, classify_error(error), state_tx, settle_rx, None)
                        .await;
                }
                reader.settled = true;
                return RequestPumpOutcome::CleanFin;
            }
            Err(error) => {
                return fail_and_wait(
                    reader,
                    classify_error(error),
                    state_tx,
                    settle_rx,
                    Some(permit),
                )
                .await;
            }
        }
    }
}

async fn drain_terminal<R, T>(
    reader: &mut PumpReaderGuard<R>,
    max_frame_size: usize,
    settle_rx: &mut watch::Receiver<Option<RequestPumpSettle>>,
) -> Result<RequestPumpOutcome>
where
    R: RequestPumpReader,
    T: FrameTrace,
{
    loop {
        let frame = tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                match settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped) {
                    RequestPumpSettle::ResponseCommitted => continue,
                    reason => return Ok(settle_reader(reader, reason)),
                }
            }
            frame = framed::read_stream_frame_or_eof::<_, T>(&mut reader.reader, max_frame_size) => frame?,
        };
        if frame.is_some() {
            return Err(Error::from(Status::internal(
                "request stream continued after terminal status",
            )));
        }
        reader.reader.validate_transport_end().await?;
        return Ok(RequestPumpOutcome::CleanFin);
    }
}

async fn fail_and_wait<R>(
    reader: &mut PumpReaderGuard<R>,
    failure: RequestPumpFailure,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut watch::Receiver<Option<RequestPumpSettle>>,
    permit: Option<mpsc::Permit<'_, Result<Vec<u8>>>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    let outcome = match &failure {
        RequestPumpFailure::PeerStatus(_) | RequestPumpFailure::PeerReset(_) => {
            RequestPumpOutcome::PeerReset
        }
        RequestPumpFailure::ConnectionLost(_) => RequestPumpOutcome::ConnectionLost,
        RequestPumpFailure::Protocol(_) => RequestPumpOutcome::LocallyStopped,
    };
    let _ = state_tx.send(RequestPumpState::Failed(failure.clone()));
    if let Some(permit) = permit {
        permit.send(Err(Error::from(failure.status().clone())));
    }
    if let Some(reason) = *settle_rx.borrow_and_update() {
        if !matches!(failure, RequestPumpFailure::Protocol(_)) {
            reader.settled = true;
            return outcome;
        }
        return settle_reader(reader, reason);
    }
    if !matches!(failure, RequestPumpFailure::Protocol(_)) {
        reader.settled = true;
        loop {
            if settle_rx.changed().await.is_err() {
                return outcome;
            }
            if settle_rx.borrow_and_update().is_some() {
                return outcome;
            }
        }
    }
    loop {
        if settle_rx.changed().await.is_err() {
            return settle_reader(reader, RequestPumpSettle::ResponseStopped);
        }
        if let Some(reason) = *settle_rx.borrow_and_update() {
            return settle_reader(reader, reason);
        }
    }
}

fn settle_reader<R>(
    reader: &mut PumpReaderGuard<R>,
    reason: RequestPumpSettle,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    match reason {
        RequestPumpSettle::ConnectionLost => {
            reader.settled = true;
            RequestPumpOutcome::ConnectionLost
        }
        RequestPumpSettle::ResponseCommitted
        | RequestPumpSettle::ResponseStopped
        | RequestPumpSettle::ServerShutdown => {
            reader.settled = true;
            reader.reader.stop_trevrpc();
            RequestPumpOutcome::LocallyStopped
        }
    }
}

#[derive(Clone, Copy)]
enum TransportFailureKind {
    PeerReset,
    ConnectionLost,
    Protocol,
}

fn classify_error(error: Error) -> RequestPumpFailure {
    let failure_kind = transport_failure_kind(&error);
    let status = error.into_status();
    match failure_kind {
        Some(TransportFailureKind::PeerReset) => RequestPumpFailure::PeerReset(status),
        Some(TransportFailureKind::Protocol) => RequestPumpFailure::Protocol(status),
        None if status.code() == Code::Cancelled => RequestPumpFailure::PeerReset(status),
        Some(TransportFailureKind::ConnectionLost) | None => {
            RequestPumpFailure::ConnectionLost(status)
        }
    }
}

fn transport_failure_kind(error: &Error) -> Option<TransportFailureKind> {
    let Error::Transport(error) = error else {
        return Some(TransportFailureKind::Protocol);
    };

    #[cfg(feature = "quinn")]
    if let Some(error) = error.downcast_ref::<quinn::ReadError>() {
        return Some(match error {
            quinn::ReadError::Reset(_) => TransportFailureKind::PeerReset,
            quinn::ReadError::ConnectionLost(_) | quinn::ReadError::ZeroRttRejected => {
                TransportFailureKind::ConnectionLost
            }
            quinn::ReadError::ClosedStream | quinn::ReadError::IllegalOrderedRead => {
                TransportFailureKind::Protocol
            }
        });
    }

    #[cfg(feature = "webtransport-client")]
    if let Some(error) = error.downcast_ref::<web_transport_quinn::ReadError>() {
        return Some(match error {
            web_transport_quinn::ReadError::Reset(_)
            | web_transport_quinn::ReadError::InvalidReset(_) => TransportFailureKind::PeerReset,
            web_transport_quinn::ReadError::SessionError(_) => TransportFailureKind::ConnectionLost,
            web_transport_quinn::ReadError::ClosedStream
            | web_transport_quinn::ReadError::IllegalOrderedRead => TransportFailureKind::Protocol,
        });
    }

    #[cfg(feature = "http3")]
    if let Some(error) = error.downcast_ref::<h3::error::StreamError>() {
        return Some(match error {
            h3::error::StreamError::RemoteTerminate { .. } => TransportFailureKind::PeerReset,
            h3::error::StreamError::StreamError { .. }
            | h3::error::StreamError::HeaderTooBig { .. } => TransportFailureKind::Protocol,
            _ => TransportFailureKind::ConnectionLost,
        });
    }

    #[cfg(feature = "http3")]
    if let Some(error) = error.downcast_ref::<h3::quic::StreamErrorIncoming>() {
        return Some(match error {
            h3::quic::StreamErrorIncoming::StreamTerminated { .. } => {
                TransportFailureKind::PeerReset
            }
            h3::quic::StreamErrorIncoming::ConnectionErrorIncoming { .. }
            | h3::quic::StreamErrorIncoming::Unknown(_) => TransportFailureKind::ConnectionLost,
        });
    }

    error
        .downcast_ref::<std::io::Error>()
        .map(|_| TransportFailureKind::ConnectionLost)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::mpsc;

    use crate::framed::{FrameRead, NoopFrameTrace};
    use crate::{Code, Error, Result, RpcStreamFrame, Status};

    use super::{
        RequestInputKind, RequestPumpFailure, RequestPumpReader, RequestPumpSettle,
        RequestPumpState, RequestTransportEvent, classify_error, start_request_pump,
    };

    struct TestReader {
        stream: DuplexStream,
        events: mpsc::UnboundedReceiver<RequestTransportEvent>,
        stops: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
        read_started: Arc<AtomicBool>,
        end_error: Option<Status>,
    }

    impl FrameRead for TestReader {
        async fn read_frame_bytes(&mut self, bytes: &mut [u8]) -> Result<Option<usize>> {
            self.read_started.store(true, Ordering::SeqCst);
            let read = self.stream.read(bytes).await.map_err(Error::transport)?;
            self.reads.fetch_add(read, Ordering::SeqCst);
            Ok((read != 0).then_some(read))
        }
    }

    impl RequestPumpReader for TestReader {
        fn stop_trevrpc(&mut self) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }

        async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
            self.events.recv().await
        }

        async fn validate_transport_end(&mut self) -> Result<()> {
            self.end_error
                .take()
                .map_or(Ok(()), |status| Err(Error::from(status)))
        }
    }

    struct PanicReader {
        stops: Arc<AtomicUsize>,
    }

    impl FrameRead for PanicReader {
        async fn read_frame_bytes(&mut self, _bytes: &mut [u8]) -> Result<Option<usize>> {
            panic!("request reader panic");
        }
    }

    impl RequestPumpReader for PanicReader {
        fn stop_trevrpc(&mut self) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }

        async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
            std::future::pending().await
        }

        async fn validate_transport_end(&mut self) -> Result<()> {
            Ok(())
        }
    }

    struct TestPumpIo {
        writer: DuplexStream,
        reader: TestReader,
        events: mpsc::UnboundedSender<RequestTransportEvent>,
        stops: Arc<AtomicUsize>,
        reads: Arc<AtomicUsize>,
        read_started: Arc<AtomicBool>,
    }

    fn test_io(end_error: Option<Status>) -> TestPumpIo {
        let (writer, stream) = tokio::io::duplex(4096);
        let (events, event_rx) = mpsc::unbounded_channel();
        let stops = Arc::new(AtomicUsize::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let read_started = Arc::new(AtomicBool::new(false));
        TestPumpIo {
            writer,
            reader: TestReader {
                stream,
                events: event_rx,
                stops: Arc::clone(&stops),
                reads: Arc::clone(&reads),
                read_started: Arc::clone(&read_started),
                end_error,
            },
            events,
            stops,
            reads,
            read_started,
        }
    }

    async fn wait_for(counter: &AtomicUsize, value: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while counter.load(Ordering::SeqCst) < value {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("instrumented read should make progress");
    }

    fn message(body: &[u8]) -> Vec<u8> {
        crate::framing::encode_message_stream_frame(body, crate::framing::DEFAULT_MAX_FRAME_SIZE)
            .expect("message frame should encode")
    }

    fn streaming() -> RequestInputKind {
        RequestInputKind::Streaming {
            max_messages: None,
            max_body_size: None,
        }
    }

    #[tokio::test]
    async fn request_pump_reserves_before_reading() {
        let mut io = test_io(None);
        let first = message(b"first");
        let second = message(b"second");
        io.writer
            .write_all(&[first.clone(), second.clone()].concat())
            .await
            .expect("frames should be written");
        let (mut body, pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);

        wait_for(&io.reads, first.len()).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(io.reads.load(Ordering::SeqCst), first.len());
        assert_eq!(body.next().await.unwrap().unwrap(), b"first");
        wait_for(&io.reads, first.len() + second.len()).await;

        drop(pump);
    }

    #[tokio::test]
    async fn request_pump_never_queues_more_than_one_body() {
        let mut io = test_io(None);
        let first = message(b"one");
        let second = message(b"two");
        io.writer
            .write_all(&[first.clone(), second.clone()].concat())
            .await
            .unwrap();
        let (mut body, pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);

        wait_for(&io.reads, first.len()).await;
        assert_eq!(io.reads.load(Ordering::SeqCst), first.len());
        assert_eq!(body.next().await.unwrap().unwrap(), b"one");
        wait_for(&io.reads, first.len() + second.len()).await;
        assert_eq!(body.next().await.unwrap().unwrap(), b"two");

        drop(pump);
    }

    #[tokio::test]
    async fn request_reset_during_reserve_cancels_without_body_poll() {
        let mut io = test_io(None);
        let first = message(b"queued");
        io.writer.write_all(&first).await.unwrap();
        let (mut body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        wait_for(&io.reads, first.len()).await;
        io.events
            .send(RequestTransportEvent::PeerReset(Status::cancelled("reset")))
            .unwrap();

        let failure = tokio::time::timeout(Duration::from_secs(1), pump.failure())
            .await
            .expect("reset should wake control");
        assert!(matches!(failure, RequestPumpFailure::PeerReset(_)));
        assert_eq!(
            failure.cancellation_source(),
            Some(crate::server::CancellationSource::PeerReset)
        );
        assert_eq!(body.next().await.unwrap().unwrap(), b"queued");
        let _ = pump.settle(RequestPumpSettle::ResponseStopped).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn connection_loss_during_reserve_is_observable() {
        let mut io = test_io(None);
        let first = message(b"queued");
        io.writer.write_all(&first).await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        wait_for(&io.reads, first.len()).await;
        io.events
            .send(RequestTransportEvent::ConnectionLost(Status::unavailable(
                "connection lost",
            )))
            .unwrap();

        let failure = pump.failure().await;
        assert!(matches!(failure, RequestPumpFailure::ConnectionLost(_)));
        assert_eq!(
            failure.cancellation_source(),
            Some(crate::server::CancellationSource::ConnectionLost)
        );
        let _ = pump.settle(RequestPumpSettle::ConnectionLost).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[cfg(feature = "webtransport-client")]
    #[test]
    fn webtransport_session_close_is_connection_loss_not_peer_reset() {
        let session_error = web_transport_quinn::SessionError::WebTransportError(
            web_transport_quinn::WebTransportError::Closed(7, "session closed".to_owned()),
        );
        let failure = classify_error(Error::transport(
            web_transport_quinn::ReadError::SessionError(session_error),
        ));

        assert!(matches!(failure, RequestPumpFailure::ConnectionLost(_)));
        assert_eq!(
            failure.cancellation_source(),
            Some(crate::server::CancellationSource::ConnectionLost)
        );
    }

    #[cfg(feature = "quinn")]
    #[test]
    fn locally_closed_quinn_connection_is_not_a_peer_reset() {
        let failure = classify_error(Error::transport(quinn::ReadError::ConnectionLost(
            quinn::ConnectionError::LocallyClosed,
        )));

        assert!(matches!(failure, RequestPumpFailure::ConnectionLost(_)));
        assert_eq!(
            failure.cancellation_source(),
            Some(crate::server::CancellationSource::ConnectionLost)
        );
    }

    #[tokio::test]
    async fn observed_limit_error_wakes_control_before_body_poll() {
        let mut io = test_io(None);
        io.writer.write_all(&message(b"over limit")).await.unwrap();
        let (_body, mut pump) = start_request_pump::<_, NoopFrameTrace>(
            io.reader,
            RequestInputKind::Streaming {
                max_messages: Some(0),
                max_body_size: None,
            },
            1024,
        );

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn malformed_frame_wakes_control_before_body_poll() {
        let mut io = test_io(None);
        io.writer.write_all(&[0, 0, 0, 1, 0xff]).await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn terminal_status_settlement_waits_for_clean_fin() {
        let mut io = test_io(None);
        let terminal = crate::framing::encode_frame(&RpcStreamFrame::status(Status::ok())).unwrap();
        io.writer.write_all(&terminal).await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(*pump.state.borrow(), RequestPumpState::DrainingTerminalFin) {
                    break;
                }
                pump.state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let mut settlement = tokio::spawn(pump.settle(RequestPumpSettle::ResponseCommitted));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut settlement)
                .await
                .is_err()
        );

        io.writer.shutdown().await.unwrap();
        assert_eq!(
            settlement.await.unwrap(),
            super::RequestPumpOutcome::CleanFin
        );
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn frame_after_terminal_status_reports_internal() {
        let mut io = test_io(None);
        let terminal = crate::framing::encode_frame(&RpcStreamFrame::status(Status::ok())).unwrap();
        io.writer
            .write_all(&[terminal, message(b"trailing")].concat())
            .await
            .unwrap();
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::Internal);
        assert_eq!(
            failure.status().message(),
            "request stream continued after terminal status"
        );
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn response_commit_stops_nonterminal_request_with_code_one() {
        let io = test_io(None);
        let (_body, pump) = start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;

        assert_eq!(outcome, super::RequestPumpOutcome::LocallyStopped);
        assert_eq!(io.stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn pump_drop_stops_request_with_code_one() {
        let io = test_io(None);
        let (_body, pump) = start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !io.read_started.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(pump);
        tokio::time::timeout(Duration::from_secs(1), async {
            while io.stops.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn request_pump_task_panic_wakes_control_and_settles() {
        let stops = Arc::new(AtomicUsize::new(0));
        let (_body, mut pump) = start_request_pump::<_, NoopFrameTrace>(
            PanicReader {
                stops: Arc::clone(&stops),
            },
            streaming(),
            1024,
        );

        let failure = tokio::time::timeout(Duration::from_secs(1), pump.failure())
            .await
            .expect("pump task failure should wake control");
        assert_eq!(failure.status().code(), Code::Internal);
        assert_eq!(
            failure.status().message(),
            "request pump task terminated unexpectedly"
        );
        let outcome = pump.settle(RequestPumpSettle::ResponseStopped).await;

        assert_eq!(outcome, super::RequestPumpOutcome::LocallyStopped);
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn clean_fin_never_sends_stop_sending() {
        let mut io = test_io(None);
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if matches!(
                    *pump.state.borrow(),
                    RequestPumpState::Settled(super::RequestPumpOutcome::CleanFin)
                ) {
                    break;
                }
                pump.state.changed().await.unwrap();
            }
        })
        .await
        .unwrap();
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;

        assert_eq!(outcome, super::RequestPumpOutcome::CleanFin);
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn http3_request_trailers_are_rejected() {
        let mut io = test_io(Some(Status::invalid_argument(
            "HTTP/3 request trailers are not supported",
        )));
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) =
            start_request_pump::<_, NoopFrameTrace>(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        assert_eq!(
            failure.status().message(),
            "HTTP/3 request trailers are not supported"
        );
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }
}
