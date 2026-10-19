use std::future::{Future, pending, poll_fn};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::framed::{self, FrameRead, RequestFrame};
use crate::server::{CancellationSource, RequestBudget};
use crate::transport_error::{self, TransportFailureKind};
use crate::{BoxStream, Code, Error, Result, Status};
use crate::{latest, queue};

/// How long, and how many bytes, the server keeps discarding the request side of a call it has
/// already answered before giving up and stopping it.
const DISCARD_TIMEOUT: Duration = Duration::from_secs(1);
const DISCARD_LIMIT: usize = 1 << 20;

/// Why a call's request half is still being read.
#[derive(Clone, Copy, Debug)]
pub enum RequestInputKind {
    /// The single request message was read before dispatch; only the end of the stream is left.
    Unary,
    /// Request messages stream to the handler as they arrive.
    Streaming,
}

impl RequestInputKind {
    pub(crate) const fn for_rpc_kind(rpc_kind: crate::RpcKind) -> Self {
        if rpc_kind.has_single_request_message() {
            Self::Unary
        } else {
            Self::Streaming
        }
    }
}

/// The error for a known frame after the single request message of a unary-input call.
const UNARY_TRAILING_FRAME: &str = "request stream contained a frame after the request message";
/// The error for a known frame after a request's terminal STATUS.
const TERMINAL_TRAILING_FRAME: &str = "request stream continued after terminal status";

#[derive(Clone, Debug)]
pub enum RequestPumpFailure {
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
pub enum RequestPumpState {
    Receiving,
    DrainingTerminalFin,
    Failed(RequestPumpFailure),
    Settled(RequestPumpOutcome),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestPumpOutcome {
    CleanFin,
    LocallyStopped,
    PeerReset,
    ConnectionLost,
}

#[derive(Clone, Copy, Debug)]
pub enum RequestPumpSettle {
    ResponseCommitted,
    ResponseStopped,
    ServerShutdown,
    ConnectionLost,
}

/// A transport-level end of the request stream, observed while the pump waits on the consumer.
#[derive(Clone, Debug)]
pub enum RequestTransportEvent {
    PeerReset(Status),
    ConnectionLost(Status),
}

impl RequestTransportEvent {
    /// The event for a QUIC connection that ended while a request was still in flight.
    pub(crate) fn connection_lost(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::ConnectionLost(Error::transport(error).into_status())
    }

    /// The event for a peer that reset the request stream with `code`.
    pub(crate) fn peer_reset(code: impl std::fmt::Display) -> Self {
        Self::PeerReset(Status::cancelled(format!(
            "request stream reset by peer with code {code}"
        )))
    }
}

/// How often a reset probe that cannot be woken by a reset, because unread request data is
/// buffered ahead of it, is retried.
pub const RESET_PROBE_INTERVAL: Duration = Duration::from_millis(250);

/// Waits for the peer to reset a request stream through a `received_reset`-style poll, which
/// resolves with the reset code, with `None` once the stream can no longer be reset, or with an
/// error when the connection is lost.
///
/// h3's poll stays pending without registering a wakeup while unread data is buffered, because
/// Quinn's own `received_reset` leaves a stale waker (see `peer_reset` in `quic.rs`), so it is
/// retried every [`RESET_PROBE_INTERVAL`]. It is cancel-safe whenever the poll is.
pub async fn poll_peer_reset<E>(
    mut poll: impl FnMut(&mut Context<'_>) -> Poll<std::result::Result<Option<u64>, E>>,
) -> Option<RequestTransportEvent>
where
    E: std::error::Error + Send + Sync + 'static,
{
    loop {
        match tokio::time::timeout(RESET_PROBE_INTERVAL, poll_fn(&mut poll)).await {
            Err(_) => {}
            Ok(Ok(Some(code))) => return Some(RequestTransportEvent::peer_reset(code)),
            // The request ended or was stopped locally: the reads that follow report it.
            Ok(Ok(None)) => return pending().await,
            Ok(Err(error)) => return Some(RequestTransportEvent::connection_lost(error)),
        }
    }
}

pub trait RequestPumpReader: FrameRead + Send + Sized + 'static {
    fn stop_trevrpc(&mut self);

    /// Resolves when the transport ends the request stream, or never if it cannot tell.
    ///
    /// The pump polls this only while the consumer is not taking bodies, the one time it is not
    /// itself reading the stream. It is dropped without resolving whenever the consumer catches
    /// up, so it must be cancel-safe and must leave the stream readable and free of stale
    /// registrations.
    fn backpressure_event(&mut self) -> impl Future<Output = Option<RequestTransportEvent>> + Send;

    /// Reads the next frame after REQUEST, charging unknown frames to `budget`.
    fn read_request_frame(
        &mut self,
        max_frame_size: usize,
        budget: &mut RequestBudget,
    ) -> impl Future<Output = Result<Option<RequestFrame>>> + Send {
        framed::read_request_frame(self, max_frame_size, |len| budget.charge_skipped(len))
    }

    /// Reads to the end of the request stream, failing on any known frame with `unexpected`.
    fn read_request_end(
        &mut self,
        max_frame_size: usize,
        budget: &mut RequestBudget,
        unexpected: &'static str,
    ) -> impl Future<Output = Result<()>> + Send {
        framed::read_request_end(
            self,
            max_frame_size,
            |len| budget.charge_skipped(len),
            unexpected,
        )
    }

    fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send;
}

/// Owns the request reader while the pump runs and decides its fate when the pump ends.
struct PumpReaderGuard<R: RequestPumpReader> {
    reader: Option<R>,
    /// The reader needs no `STOP_SENDING`: it finished, or the peer or connection ended it.
    settled: bool,
    /// The call was answered while the request side may still be open.
    discard: bool,
}

impl<R: RequestPumpReader> PumpReaderGuard<R> {
    const fn new(reader: R) -> Self {
        Self {
            reader: Some(reader),
            settled: false,
            discard: false,
        }
    }

    #[expect(
        clippy::expect_used,
        reason = "`reader` is only taken in `drop`, so it is present for the guard's whole life"
    )]
    const fn reader(&mut self) -> &mut R {
        self.reader
            .as_mut()
            .expect("request reader is present until the guard drops")
    }
}

impl<R: RequestPumpReader> Drop for PumpReaderGuard<R> {
    fn drop(&mut self) {
        let Some(mut reader) = self.reader.take() else {
            return;
        };
        if self.discard {
            // Peers such as Firefox tear down the whole stream, unread response included, when
            // the request side is stopped. Let the request finish on its own, detached from the
            // handler so it holds no handler resources.
            tokio::spawn(discard_request(reader));
        } else if !self.settled {
            reader.stop_trevrpc();
        }
    }
}

/// Discards the rest of an answered call's request until its EOF or reset, stopping it only if
/// the peer keeps sending for longer than `DISCARD_TIMEOUT` or past `DISCARD_LIMIT` bytes.
///
/// The task ends within `DISCARD_TIMEOUT`, and sooner when the connection closes, so it cannot
/// outlive a shutdown by more than that.
async fn discard_request<R: RequestPumpReader>(mut reader: R) {
    // On the heap, so the spawned task does not carry the buffer in its state.
    let mut buffer = vec![0_u8; 4096];
    let finished = tokio::time::timeout(DISCARD_TIMEOUT, async {
        let mut discarded = 0_usize;
        loop {
            match reader.read_frame_bytes(&mut buffer).await {
                Ok(Some(read)) => {
                    discarded = discarded.saturating_add(read);
                    if discarded > DISCARD_LIMIT {
                        return false;
                    }
                }
                // End of stream, a peer reset, or a lost connection: nothing is left to stop.
                Ok(None) | Err(_) => return true,
            }
        }
    })
    .await
    .unwrap_or(false);
    if !finished {
        reader.stop_trevrpc();
    }
}

pub struct RequestPump {
    state: watch::Receiver<RequestPumpState>,
    settle: latest::Sender<Option<RequestPumpSettle>>,
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
        self.settle.send(Some(reason));
        let Some(task) = self.task.take() else {
            return RequestPumpOutcome::LocallyStopped;
        };
        task.await.unwrap_or(RequestPumpOutcome::LocallyStopped)
    }
}

impl Drop for RequestPump {
    fn drop(&mut self) {
        self.settle.send(Some(RequestPumpSettle::ResponseStopped));
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

pub fn start_request_pump<R>(
    reader: R,
    input_kind: RequestInputKind,
    max_frame_size: usize,
    budget: RequestBudget,
) -> (BoxStream<Vec<u8>>, RequestPump)
where
    R: RequestPumpReader,
{
    let (body_tx, body_rx) = queue::channel(queue::MESSAGES);
    let (state_tx, state_rx) = watch::channel(RequestPumpState::Receiving);
    let (settle_tx, settle_rx) = latest::channel(None);
    let task = tokio::spawn(run_request_pump::<R>(
        reader,
        input_kind,
        max_frame_size,
        budget,
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

async fn run_request_pump<R>(
    reader: R,
    input_kind: RequestInputKind,
    max_frame_size: usize,
    budget: RequestBudget,
    body_tx: queue::Sender<Result<Vec<u8>>>,
    state_tx: watch::Sender<RequestPumpState>,
    mut settle_rx: latest::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    let mut reader = PumpReaderGuard::new(reader);
    let outcome = match input_kind {
        RequestInputKind::Unary => {
            run_unary_pump::<R>(
                &mut reader,
                max_frame_size,
                budget,
                &state_tx,
                &mut settle_rx,
            )
            .await
        }
        RequestInputKind::Streaming => {
            run_streaming_pump::<R>(
                &mut reader,
                max_frame_size,
                budget,
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
    max_frame_size: usize,
    mut budget: RequestBudget,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut latest::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    let mut read = Box::pin(reader.reader().read_request_end(
        max_frame_size,
        &mut budget,
        UNARY_TRAILING_FRAME,
    ));
    let result = tokio::select! {
        biased;
        changed = settle_rx.changed() => {
            let _ = changed;
            let reason = settle_rx
                .borrow_and_update()
                .unwrap_or(RequestPumpSettle::ResponseStopped);
            // The logical read only validates the request before the response exists. Once the
            // response is committed, the rest of the request goes to the bounded drain.
            drop(read);
            return settle_reader(reader, reason);
        }
        result = &mut read => result,
    };
    drop(read);
    match result {
        Ok(()) => {
            if let Err(error) = reader.reader().validate_transport_end().await {
                return fail_and_wait(reader, classify_error(error), state_tx, settle_rx, None)
                    .await;
            }
            reader.settled = true;
            RequestPumpOutcome::CleanFin
        }
        Err(error) => fail_and_wait(reader, classify_error(error), state_tx, settle_rx, None).await,
    }
}

#[allow(clippy::too_many_lines)]
async fn run_streaming_pump<R>(
    reader: &mut PumpReaderGuard<R>,
    max_frame_size: usize,
    mut budget: RequestBudget,
    body_tx: &queue::Sender<Result<Vec<u8>>>,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut latest::Receiver<Option<RequestPumpSettle>>,
) -> RequestPumpOutcome
where
    R: RequestPumpReader,
{
    loop {
        // The pump reads the stream only once the consumer has room for the next body. Transport
        // events are watched solely while it waits for that room, so a consumer that keeps up never
        // touches the watcher. A settled call outranks a closed consumer, which must not stop a
        // request that is still being drained.
        let permit = tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                return settle_reader(reader, settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped));
            }
            permit = body_tx.reserve() => {
                if let Ok(permit) = permit {
                    permit
                } else {
                    reader.reader().stop_trevrpc();
                    reader.settled = true;
                    return RequestPumpOutcome::LocallyStopped;
                }
            }
            event = reader.reader().backpressure_event() => {
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
        };

        let frame = tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                return settle_reader(reader, settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped));
            }
            frame = reader.reader().read_request_frame(max_frame_size, &mut budget) => frame,
        };

        match frame {
            Ok(Some(RequestFrame::Message(body))) => {
                if let Err(error) = budget.charge_message(body.len()) {
                    let failure = RequestPumpFailure::Protocol(error.into_status());
                    return fail_and_wait(reader, failure, state_tx, settle_rx, Some(permit)).await;
                }
                let size = body.len();
                permit.send(Ok(Vec::from(body)), size);
            }
            Ok(Some(RequestFrame::Status(status))) => {
                let _ = state_tx.send(RequestPumpState::DrainingTerminalFin);
                match drain_terminal::<R>(reader, max_frame_size, &mut budget, settle_rx).await {
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
            Ok(None) => {
                if let Err(error) = reader.reader().validate_transport_end().await {
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

/// Waits for the clean end of a request stream whose terminal STATUS has been read.
///
/// The read stays pinned while the response is committed, so no partially read frame is lost.
async fn drain_terminal<R>(
    reader: &mut PumpReaderGuard<R>,
    max_frame_size: usize,
    budget: &mut RequestBudget,
    settle_rx: &mut latest::Receiver<Option<RequestPumpSettle>>,
) -> Result<RequestPumpOutcome>
where
    R: RequestPumpReader,
{
    let mut read = Box::pin(reader.reader().read_request_end(
        max_frame_size,
        budget,
        TERMINAL_TRAILING_FRAME,
    ));
    loop {
        tokio::select! {
            biased;
            changed = settle_rx.changed() => {
                let _ = changed;
                match settle_rx.borrow_and_update().unwrap_or(RequestPumpSettle::ResponseStopped) {
                    RequestPumpSettle::ResponseCommitted => {}
                    reason => {
                        drop(read);
                        return Ok(settle_reader(reader, reason));
                    }
                }
            }
            result = &mut read => {
                drop(read);
                result?;
                reader.reader().validate_transport_end().await?;
                return Ok(RequestPumpOutcome::CleanFin);
            }
        }
    }
}

async fn fail_and_wait<R>(
    reader: &mut PumpReaderGuard<R>,
    failure: RequestPumpFailure,
    state_tx: &watch::Sender<RequestPumpState>,
    settle_rx: &mut latest::Receiver<Option<RequestPumpSettle>>,
    permit: Option<queue::Permit<'_, Result<Vec<u8>>>>,
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
        permit.send(Err(Error::from(failure.status().clone())), 0);
    }
    let settled = settle_rx.borrow_and_update();
    if let Some(reason) = settled {
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
        let settled = settle_rx.borrow_and_update();
        if let Some(reason) = settled {
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
        RequestPumpSettle::ResponseCommitted => {
            reader.settled = true;
            reader.discard = true;
            RequestPumpOutcome::LocallyStopped
        }
        RequestPumpSettle::ResponseStopped | RequestPumpSettle::ServerShutdown => {
            reader.settled = true;
            reader.reader().stop_trevrpc();
            RequestPumpOutcome::LocallyStopped
        }
    }
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
    transport_error::classify(error.as_ref()).kind
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use futures_util::StreamExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};
    use tokio::sync::{mpsc, oneshot};

    use crate::framed::FrameRead;
    use crate::framing::{
        DEFAULT_MAX_FRAME_SIZE, encode_frame, encode_message_frame, encode_protobuf_frame,
        frame_type,
    };
    use crate::queue;
    use crate::server::RequestBudget;
    use crate::{Code, Error, Result, RpcRequest, RpcStatus, Status};

    use super::{
        RequestInputKind, RequestPumpFailure, RequestPumpReader, RequestPumpSettle,
        RequestPumpState, RequestTransportEvent, classify_error, start_request_pump as start_pump,
    };

    /// Starts a pump with no request limits.
    fn start_request_pump<R: RequestPumpReader>(
        reader: R,
        kind: RequestInputKind,
        max_frame_size: usize,
    ) -> (crate::BoxStream<Vec<u8>>, super::RequestPump) {
        start_pump(reader, kind, max_frame_size, RequestBudget::new(None, None))
    }

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

        fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send {
            std::future::ready(
                self.end_error
                    .take()
                    .map_or(Ok(()), |status| Err(Error::from(status))),
            )
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

        fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send {
            std::future::ready(Ok(()))
        }
    }

    struct TrackedUnaryRead {
        completion: oneshot::Receiver<()>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for TrackedUnaryRead {
        type Output = Result<()>;

        fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.completion).poll(context) {
                Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
                Poll::Ready(Err(_)) => Poll::Ready(Err(Error::from(Status::cancelled(
                    "tracked unary read was cancelled",
                )))),
                Poll::Pending => Poll::Pending,
            }
        }
    }

    impl Drop for TrackedUnaryRead {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct TrackedUnaryReader {
        completion: Option<oneshot::Receiver<()>>,
        starts: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
    }

    impl FrameRead for TrackedUnaryReader {
        /// The raw drain read: the client never sends or finishes.
        async fn read_frame_bytes(&mut self, _bytes: &mut [u8]) -> Result<Option<usize>> {
            std::future::pending().await
        }
    }

    impl RequestPumpReader for TrackedUnaryReader {
        fn stop_trevrpc(&mut self) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }

        fn read_request_end(
            &mut self,
            _max_frame_size: usize,
            _budget: &mut RequestBudget,
            _unexpected: &'static str,
        ) -> impl Future<Output = Result<()>> + Send {
            self.starts.fetch_add(1, Ordering::SeqCst);
            TrackedUnaryRead {
                completion: self
                    .completion
                    .take()
                    .expect("unary read future should start once"),
                drops: Arc::clone(&self.drops),
            }
        }

        async fn backpressure_event(&mut self) -> Option<RequestTransportEvent> {
            std::future::pending().await
        }

        fn validate_transport_end(&mut self) -> impl Future<Output = Result<()>> + Send {
            std::future::ready(Ok(()))
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
        test_io_with_capacity(end_error, 4096)
    }

    fn test_io_with_capacity(end_error: Option<Status>, capacity: usize) -> TestPumpIo {
        let (writer, stream) = tokio::io::duplex(capacity);
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
        encode_message_frame(body, DEFAULT_MAX_FRAME_SIZE).expect("message frame should encode")
    }

    fn status_frame(status: Status) -> Vec<u8> {
        encode_protobuf_frame(
            frame_type::STATUS,
            &RpcStatus::from(status),
            DEFAULT_MAX_FRAME_SIZE,
        )
        .expect("status frame should encode")
    }

    /// A frame of an unknown (grease) type with `payload` bytes.
    fn grease(payload: usize) -> Vec<u8> {
        encode_frame(0x21, &vec![0; payload], DEFAULT_MAX_FRAME_SIZE)
            .expect("grease frame should encode")
    }

    #[tokio::test(start_paused = true)]
    async fn unary_response_commit_hands_open_request_to_bounded_drain() {
        // Keep the sender alive: the client never finishes its request.
        let (_completion_tx, completion_rx) = oneshot::channel();
        let starts = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let reader = TrackedUnaryReader {
            completion: Some(completion_rx),
            starts: Arc::clone(&starts),
            drops: Arc::clone(&drops),
            stops: Arc::clone(&stops),
        };
        let (_body, pump) = start_request_pump(reader, RequestInputKind::Unary, 1024);

        wait_for(&starts, 1).await;
        // Settling returns at once instead of waiting for the client's EOF.
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        assert_eq!(outcome, super::RequestPumpOutcome::LocallyStopped);
        assert_eq!(
            starts.load(Ordering::SeqCst),
            1,
            "the read is not restarted"
        );
        assert_eq!(drops.load(Ordering::SeqCst), 1);

        // The detached drain stops the request only at its time bound.
        tokio::time::sleep(super::DISCARD_TIMEOUT / 2).await;
        assert_eq!(stops.load(Ordering::SeqCst), 0);
        tokio::time::sleep(super::DISCARD_TIMEOUT).await;
        assert_eq!(stops.load(Ordering::SeqCst), 1);
        assert_eq!(starts.load(Ordering::SeqCst), 1);
    }

    fn streaming() -> RequestInputKind {
        RequestInputKind::Streaming
    }

    /// The frames that fill a request body queue by count.
    fn queue_filling_frames() -> Vec<u8> {
        message(b"queued").repeat(queue::MESSAGES.values)
    }

    #[tokio::test]
    async fn request_pump_reads_only_while_the_queue_has_room() {
        let mut io = test_io(None);
        let queued = queue_filling_frames();
        let extra = message(b"extra");
        io.writer
            .write_all(&[queued.clone(), extra.clone()].concat())
            .await
            .expect("frames should be written");
        let (mut body, pump) = start_request_pump(io.reader, streaming(), 1024);

        wait_for(&io.reads, queued.len()).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(io.reads.load(Ordering::SeqCst), queued.len());
        assert_eq!(body.next().await.unwrap().unwrap(), b"queued");
        wait_for(&io.reads, queued.len() + extra.len()).await;

        drop(pump);
    }

    #[tokio::test]
    async fn request_pump_queues_at_most_the_byte_limit_and_one_body() {
        let body_len = queue::MESSAGES.bytes * 2 / 3;
        let large = message(&vec![7; body_len]);
        let mut io = test_io_with_capacity(None, 4 * large.len());
        io.writer
            .write_all(&[large.clone(), large.clone(), large.clone()].concat())
            .await
            .expect("frames should be written");
        let (mut body, pump) = start_request_pump(io.reader, streaming(), usize::MAX);

        // The second body crosses the byte limit, so the third is not read.
        wait_for(&io.reads, 2 * large.len()).await;
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(io.reads.load(Ordering::SeqCst), 2 * large.len());
        assert_eq!(body.next().await.unwrap().unwrap().len(), body_len);
        wait_for(&io.reads, 3 * large.len()).await;
        assert_eq!(body.next().await.unwrap().unwrap().len(), body_len);
        assert_eq!(body.next().await.unwrap().unwrap().len(), body_len);

        drop(pump);
    }

    #[tokio::test]
    async fn request_reset_during_reserve_cancels_without_body_poll() {
        let mut io = test_io(None);
        let queued = queue_filling_frames();
        io.writer.write_all(&queued).await.unwrap();
        let (mut body, mut pump) = start_request_pump(io.reader, streaming(), 1024);
        wait_for(&io.reads, queued.len()).await;
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
        let queued = queue_filling_frames();
        io.writer.write_all(&queued).await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);
        wait_for(&io.reads, queued.len()).await;
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

    #[tokio::test(start_paused = true)]
    async fn observed_limit_error_wakes_control_before_body_poll() {
        let mut io = test_io(None);
        io.writer.write_all(&message(b"over limit")).await.unwrap();
        let (_body, mut pump) = start_pump(
            io.reader,
            RequestInputKind::Streaming,
            1024,
            RequestBudget::new(Some(0), None),
        );

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        // The request side stays open, so it is discarded and then stopped at the time bound.
        tokio::time::sleep(super::DISCARD_TIMEOUT * 2).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn malformed_frame_wakes_control_before_body_poll() {
        let mut io = test_io(None);
        // A STATUS frame whose payload is a truncated protobuf varint.
        io.writer.write_all(&[0x03, 0x01, 0xff]).await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn terminal_status_settlement_waits_for_clean_fin() {
        let mut io = test_io(None);
        let terminal = status_frame(Status::ok());
        io.writer.write_all(&terminal).await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);
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
    async fn frame_after_terminal_status_reports_invalid_argument() {
        let mut io = test_io(None);
        let terminal = status_frame(Status::ok());
        io.writer
            .write_all(&[terminal, message(b"trailing")].concat())
            .await
            .unwrap();
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        assert_eq!(
            failure.status().message(),
            "request stream continued after terminal status"
        );
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test(start_paused = true)]
    async fn response_commit_discards_open_request_then_stops_at_time_bound() {
        let io = test_io(None);
        let (_body, pump) = start_request_pump(io.reader, streaming(), 1024);
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;

        assert_eq!(outcome, super::RequestPumpOutcome::LocallyStopped);
        // Settling does not stop the request side immediately...
        tokio::time::sleep(super::DISCARD_TIMEOUT / 2).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
        // ...only the time bound does.
        tokio::time::sleep(super::DISCARD_TIMEOUT).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn response_commit_lets_open_request_finish_without_stop() {
        let mut io = test_io(None);
        let (_body, pump) = start_request_pump(io.reader, streaming(), 1024);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;

        io.writer.write_all(&[7; 100]).await.unwrap();
        drop(io.writer);
        wait_for(&io.reads, 100).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn response_commit_discard_stops_at_byte_bound() {
        let mut io = test_io(None);
        let (_body, pump) = start_request_pump(io.reader, streaming(), 1024);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;

        let chunk = [0_u8; 4096];
        let mut sent = 0;
        while sent <= super::DISCARD_LIMIT {
            io.writer.write_all(&chunk).await.unwrap();
            sent += chunk.len();
        }
        wait_for(&io.stops, 1).await;
        assert!(io.reads.load(Ordering::SeqCst) <= super::DISCARD_LIMIT + chunk.len());
    }

    #[tokio::test]
    async fn pump_drop_stops_request_with_code_one() {
        let io = test_io(None);
        let (_body, pump) = start_request_pump(io.reader, streaming(), 1024);
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
        let (_body, mut pump) = start_request_pump(
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
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);
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
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        assert_eq!(
            failure.status().message(),
            "HTTP/3 request trailers are not supported"
        );
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    /// Waits until the pump has read the request stream to a clean end.
    async fn wait_for_clean_fin(pump: &mut super::RequestPump) {
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
        .expect("the request should end cleanly");
    }

    async fn collect_bodies(body: &mut crate::BoxStream<Vec<u8>>) -> Vec<Vec<u8>> {
        let mut bodies = Vec::new();
        while let Some(item) = body.next().await {
            bodies.push(item.expect("request body should not fail"));
        }
        bodies
    }

    #[tokio::test]
    async fn unknown_frames_are_skipped_between_request_messages() {
        let mut io = test_io(None);
        let wire = [
            grease(5),
            message(b"a"),
            grease(0),
            encode_frame(0x1234, b"unknown", 1024).unwrap(),
            message(b"b"),
            grease(9),
        ]
        .concat();
        io.writer.write_all(&wire).await.unwrap();
        io.writer.shutdown().await.unwrap();
        let (mut body, pump) = start_request_pump(io.reader, streaming(), 1024);

        assert_eq!(
            collect_bodies(&mut body).await,
            [b"a".to_vec(), b"b".to_vec()]
        );
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        assert_eq!(outcome, super::RequestPumpOutcome::CleanFin);
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unknown_frames_are_skipped_after_the_terminal_status() {
        let mut io = test_io(None);
        io.writer
            .write_all(&[status_frame(Status::ok()), grease(3)].concat())
            .await
            .unwrap();
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        wait_for_clean_fin(&mut pump).await;
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        assert_eq!(outcome, super::RequestPumpOutcome::CleanFin);
        assert_eq!(io.stops.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn grease_is_charged_to_the_stream_body_limit() {
        let mut io = test_io(None);
        // Four 32-byte frames against a 100-byte limit; no message is ever sent.
        io.writer
            .write_all(&[grease(30), grease(30), grease(30), grease(30)].concat())
            .await
            .unwrap();
        let (_body, mut pump) = start_pump(
            io.reader,
            streaming(),
            1024,
            RequestBudget::new(None, Some(100)),
        );

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        assert!(matches!(failure, RequestPumpFailure::Protocol(_)));
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn grease_and_messages_share_the_stream_body_limit() {
        let mut io = test_io(None);
        // 20 message bytes plus 2 x 42 grease bytes pass 100; the grease alone does not.
        io.writer
            .write_all(&[message(&[1; 20]), grease(40), grease(40)].concat())
            .await
            .unwrap();
        let (mut body, mut pump) = start_pump(
            io.reader,
            streaming(),
            1024,
            RequestBudget::new(None, Some(100)),
        );

        assert_eq!(body.next().await.unwrap().unwrap(), [1; 20]);
        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn grease_cannot_hold_a_unary_request_stream_open() {
        let mut io = test_io(None);
        io.writer
            .write_all(&[grease(30), grease(30), grease(30), grease(30)].concat())
            .await
            .unwrap();
        let (_body, mut pump) = start_pump(
            io.reader,
            RequestInputKind::Unary,
            1024,
            RequestBudget::new(None, Some(100)),
        );

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn unary_pump_accepts_unknown_frames_until_fin() {
        let mut io = test_io(None);
        io.writer.write_all(&grease(3)).await.unwrap();
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, RequestInputKind::Unary, 1024);

        wait_for_clean_fin(&mut pump).await;
        let outcome = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        assert_eq!(outcome, super::RequestPumpOutcome::CleanFin);
    }

    #[tokio::test]
    async fn unary_pump_rejects_a_second_message_without_reading_it() {
        let mut io = test_io(None);
        // The header declares 1000 payload bytes that never arrive.
        io.writer.write_all(&[0x00, 0x43, 0xe8]).await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, RequestInputKind::Unary, 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::InvalidArgument);
        assert!(matches!(failure, RequestPumpFailure::Protocol(_)));
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn request_streams_reject_headers_and_a_second_request() {
        let headers = encode_frame(frame_type::HEADERS, &[], 1024).unwrap();
        let request = encode_protobuf_frame(
            frame_type::REQUEST,
            &RpcRequest::new("/svc/m"),
            DEFAULT_MAX_FRAME_SIZE,
        )
        .unwrap();
        for wire in [headers, request] {
            let mut io = test_io(None);
            io.writer.write_all(&wire).await.unwrap();
            let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

            let failure = pump.failure().await;
            assert_eq!(failure.status().code(), Code::InvalidArgument);
            let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
        }
    }

    #[tokio::test]
    async fn oversized_unknown_frame_is_rejected_before_its_payload() {
        let mut io = test_io(None);
        // Grease declaring 2000 payload bytes against a 1024-byte limit; none follow.
        io.writer.write_all(&[0x21, 0x47, 0xd0]).await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::ResourceExhausted);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }

    #[tokio::test]
    async fn stream_ending_inside_a_frame_header_is_a_connection_failure() {
        let mut io = test_io(None);
        io.writer.write_all(&[0x00]).await.unwrap();
        io.writer.shutdown().await.unwrap();
        let (_body, mut pump) = start_request_pump(io.reader, streaming(), 1024);

        let failure = pump.failure().await;
        assert_eq!(failure.status().code(), Code::Unavailable);
        let _ = pump.settle(RequestPumpSettle::ResponseCommitted).await;
    }
}
