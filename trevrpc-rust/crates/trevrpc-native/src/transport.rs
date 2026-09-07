use std::collections::{HashMap, HashSet, VecDeque};
use std::os::fd::{AsRawFd, RawFd};
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, Weak};
use std::time::Duration;

use futures_util::future::select_all;
use tokio::io::Interest;
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::{Mutex, MutexGuard, mpsc, oneshot, watch};
use tokio::time;

use crate::ffi::{self, EventInfo, EventKind, Handle, ObjectKind, RawEvent, RawTransport};
use crate::{
    CloseReason, EndpointConfig, MsQuicConfig, NativeError, Protocol, Result, TransportConfig,
};

const EVENT_BATCH: usize = 64;
const IDLE_POLL: Duration = Duration::from_mins(1);
const RELEASE_RETRY: Duration = Duration::from_millis(10);
const DROP_ABORT_CODE: u64 = 1;
const FINALIZER_WORKERS: usize = 2;

struct FinalizerJob {
    raw: RawTransport,
    admissions: Vec<RawEvent>,
    handles: HashSet<(Handle, ObjectKind)>,
    failure_reporter: watch::Sender<Option<Result<()>>>,
}

static FINALIZER: OnceLock<std::result::Result<std::sync::mpsc::Sender<FinalizerJob>, String>> =
    OnceLock::new();

fn finalizer_sender() -> Result<&'static std::sync::mpsc::Sender<FinalizerJob>> {
    FINALIZER
        .get_or_init(|| {
            let (sender, receiver) = std::sync::mpsc::channel::<FinalizerJob>();
            let receiver = Arc::new(StdMutex::new(receiver));
            let mut started = 0;
            let mut last_error = None;
            for index in 0..FINALIZER_WORKERS {
                let receiver = receiver.clone();
                match std::thread::Builder::new()
                    .name(format!("trevrpc-native-finalizer-{index}"))
                    .spawn(move || {
                        loop {
                            let job = receiver
                                .lock()
                                .expect("native finalizer receiver lock poisoned")
                                .recv();
                            let Ok(job) = job else {
                                break;
                            };
                            run_finalizer_job(job);
                        }
                    }) {
                    Ok(_) => started += 1,
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
            if started == 0 {
                Err(last_error.unwrap_or_else(|| "no finalizer workers started".to_owned()))
            } else {
                Ok(sender)
            }
        })
        .as_ref()
        .map_err(|error| {
            NativeError::message(format!("native finalizer thread could not start: {error}"))
        })
}

fn dispatch_finalizer_job(job: FinalizerJob) {
    let job = Arc::new(StdMutex::new(Some(job)));
    let worker_job = job.clone();
    if std::thread::Builder::new()
        .name("trevrpc-native-cleanup".to_owned())
        .spawn(move || {
            let job = worker_job
                .lock()
                .expect("native finalizer job lock poisoned")
                .take()
                .expect("native finalizer job already claimed");
            run_finalizer_job(job);
        })
        .is_err()
        && let Some(job) = job
            .lock()
            .expect("native finalizer job lock poisoned")
            .take()
    {
        run_finalizer_job(job);
    }
}

fn run_finalizer_job(job: FinalizerJob) {
    let _ = emergency_cleanup(
        job.raw,
        job.admissions,
        job.handles,
        Some(&job.failure_reporter),
    );
}

fn enqueue_finalizer(job: FinalizerJob) {
    match finalizer_sender() {
        Ok(sender) => {
            if let Err(error) = sender.send(job) {
                dispatch_finalizer_job(error.0);
            }
        }
        Err(_) => dispatch_finalizer_job(job),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportState {
    Running,
    Stopping,
    Stopped,
    Unknown(u32),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Diagnostics {
    pub state: TransportState,
    pub terminal_status: i32,
    pub queue_depth: u32,
    pub ordinary_queue_depth: u32,
    pub live_listeners: u64,
    pub live_connections: u64,
    pub live_streams: u64,
    pub pending_send_count: u64,
    pub pending_send_bytes: u64,
    pub receive_owned_count: u64,
    pub receive_owned_bytes: u64,
    pub events_enqueued: u64,
    pub events_dequeued: u64,
    pub events_rejected: u64,
    pub provider_error_code: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmissionRequest {
    pub protocol: Protocol,
    pub headers: Vec<(Vec<u8>, Vec<u8>)>,
    pub method: Vec<u8>,
    pub path: Vec<u8>,
    pub authority: Vec<u8>,
    pub origin: Vec<u8>,
}

pub enum ListenerEvent {
    Connection(Connection),
    Http3Admission(Admission),
    WebTransportAdmission(Admission),
}

struct DeliveryReceiver<T> {
    items: mpsc::Receiver<T>,
    rejections: watch::Receiver<u64>,
    reported_rejections: u64,
}

impl<T> DeliveryReceiver<T> {
    async fn receive(&mut self, item: &'static str) -> Result<Option<T>> {
        loop {
            let rejections = *self.rejections.borrow_and_update();
            if rejections != self.reported_rejections {
                let count = rejections.wrapping_sub(self.reported_rejections);
                self.reported_rejections = rejections;
                return Err(NativeError::message(format!(
                    "native {item} delivery queue rejected {count} incoming item(s)"
                )));
            }
            tokio::select! {
                biased;
                changed = self.rejections.changed() => {
                    if changed.is_err() {
                        return Ok(self.items.recv().await);
                    }
                }
                item = self.items.recv() => return Ok(item),
            }
        }
    }
}

fn record_delivery_rejection(rejections: &watch::Sender<u64>) {
    rejections.send_modify(|count| *count = count.wrapping_add(1));
}

struct OperationId {
    value: u64,
    active: Arc<StdMutex<HashSet<u64>>>,
}

impl OperationId {
    const fn get(&self) -> u64 {
        self.value
    }
}

impl Drop for OperationId {
    fn drop(&mut self) {
        self.active
            .lock()
            .expect("operation ID registry lock poisoned")
            .remove(&self.value);
    }
}

#[derive(Clone)]
struct Shared {
    commands: mpsc::Sender<Command>,
    cleanup: mpsc::UnboundedSender<Cleanup>,
    operations: Arc<AtomicU64>,
    active_operations: Arc<StdMutex<HashSet<u64>>>,
    accepting_commands: Arc<AtomicBool>,
    command_reservations: Arc<AtomicU64>,
    shutdown_started: watch::Sender<bool>,
}

struct CommandReservation {
    count: Arc<AtomicU64>,
}

impl CommandReservation {
    fn new(count: Arc<AtomicU64>) -> Self {
        count.fetch_add(1, Ordering::AcqRel);
        Self { count }
    }
}

impl Drop for CommandReservation {
    fn drop(&mut self) {
        self.count.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Shared {
    async fn send(&self, command: Command) -> Result<()> {
        if !self.accepting_commands.load(Ordering::Acquire) {
            return Err(NativeError::message("native transport is shutting down"));
        }
        let _reservation = CommandReservation::new(self.command_reservations.clone());
        if !self.accepting_commands.load(Ordering::Acquire) {
            return Err(NativeError::message("native transport is shutting down"));
        }
        let permit = self
            .commands
            .reserve()
            .await
            .map_err(|_| NativeError::message("native owner task stopped"))?;
        if !self.accepting_commands.load(Ordering::Acquire) {
            return Err(NativeError::message("native transport is shutting down"));
        }
        permit.send(command);
        Ok(())
    }

    fn mark_shutdown_started(&self) {
        self.accepting_commands.store(false, Ordering::Release);
        self.shutdown_started.send_replace(true);
    }

    fn begin_shutdown(&self) {
        self.mark_shutdown_started();
        self.cleanup(Cleanup::Shutdown);
    }

    fn cleanup(&self, cleanup: Cleanup) {
        let _ = self.cleanup.send(cleanup);
    }

    fn reliable_cleanup(&self, cleanup: Cleanup) -> Result<()> {
        self.cleanup
            .send(cleanup)
            .map_err(|_| NativeError::message("native owner task stopped"))
    }

    fn next_operation(&self) -> OperationId {
        loop {
            let value = self.operations.fetch_add(1, Ordering::Relaxed);
            if value == 0 {
                continue;
            }
            let mut active = self
                .active_operations
                .lock()
                .expect("operation ID registry lock poisoned");
            if active.insert(value) {
                drop(active);
                return OperationId {
                    value,
                    active: self.active_operations.clone(),
                };
            }
        }
    }
}

struct TransportInner {
    shared: Shared,
    terminal: watch::Receiver<Option<Result<()>>>,
    max_receive_owned_bytes: u64,
}

impl Drop for TransportInner {
    fn drop(&mut self) {
        self.shared.begin_shutdown();
    }
}

#[derive(Clone)]
pub struct NativeTransport {
    inner: Arc<TransportInner>,
}

impl NativeTransport {
    pub async fn new(config: TransportConfig) -> Result<Self> {
        Self::new_with_provider(config, MsQuicConfig::default()).await
    }

    pub async fn new_with_provider(
        config: TransportConfig,
        provider_config: MsQuicConfig,
    ) -> Result<Self> {
        config.validate()?;
        let raw = RawTransport::create(&config, &provider_config)?;
        Self::start(config, raw).await
    }

    #[cfg(feature = "testing")]
    pub(crate) async fn new_testing(
        config: TransportConfig,
    ) -> Result<(Self, ffi::TestingControl)> {
        config.validate()?;
        let (raw, control) = RawTransport::create_testing(&config)?;
        Self::start(config, raw)
            .await
            .map(|transport| (transport, control))
    }

    async fn start(config: TransportConfig, raw: RawTransport) -> Result<Self> {
        let _ = finalizer_sender()?;
        let delivery_capacity = config.delivery_capacity;
        let (command_tx, command_rx) = mpsc::channel(config.command_capacity);
        let (cleanup_tx, cleanup_rx) = mpsc::unbounded_channel();
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let (shutdown_started, _) = watch::channel(false);
        let shared = Shared {
            commands: command_tx,
            cleanup: cleanup_tx,
            operations: Arc::new(AtomicU64::new(1)),
            active_operations: Arc::new(StdMutex::new(HashSet::new())),
            accepting_commands: Arc::new(AtomicBool::new(true)),
            command_reservations: Arc::new(AtomicU64::new(0)),
            shutdown_started,
        };
        let inner = Arc::new(TransportInner {
            shared: shared.clone(),
            terminal: terminal_rx,
            max_receive_owned_bytes: config.max_receive_owned_bytes,
        });
        let (ready_tx, ready_rx) = oneshot::channel();
        tokio::spawn(owner_loop(
            shared,
            terminal_tx,
            raw,
            delivery_capacity,
            command_rx,
            cleanup_rx,
            ready_tx,
        ));
        ready_rx
            .await
            .map_err(|_| NativeError::message("native owner task failed during startup"))??;
        Ok(Self { inner })
    }

    pub async fn listen(&self, config: EndpointConfig) -> Result<Listener> {
        let (reply, wait) = oneshot::channel();
        self.inner
            .shared
            .send(Command::Listen { config, reply })
            .await?;
        receive_reply(wait).await
    }

    pub async fn dial(&self, config: EndpointConfig) -> Result<Connection> {
        let (reply, wait) = oneshot::channel();
        self.inner
            .shared
            .send(Command::Dial { config, reply })
            .await?;
        receive_reply(wait).await
    }

    pub async fn diagnostics(&self) -> Result<Diagnostics> {
        let (reply, wait) = oneshot::channel();
        self.inner
            .shared
            .send(Command::Diagnostics { reply })
            .await?;
        receive_reply(wait).await
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.inner.shared.accepting_commands.load(Ordering::Acquire)
    }

    pub async fn shutdown_started(&self) {
        let mut shutdown = self.inner.shared.shutdown_started.subscribe();
        while !*shutdown.borrow_and_update() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    }

    #[must_use]
    pub fn max_receive_owned_bytes(&self) -> u64 {
        self.inner.max_receive_owned_bytes
    }

    pub async fn close(&self) -> Result<()> {
        if let Some(result) = self.inner.terminal.borrow().clone() {
            return result;
        }
        self.inner.shared.begin_shutdown();
        let mut terminal = self.inner.terminal.clone();
        loop {
            if let Some(result) = terminal.borrow_and_update().clone() {
                return result;
            }
            terminal
                .changed()
                .await
                .map_err(|_| NativeError::message("native owner stopped without a result"))?;
        }
    }
}

struct ObjectState {
    shared: Shared,
    handle: Handle,
    kind: ObjectKind,
    terminal: watch::Receiver<Option<CloseReason>>,
    send_stopped: Option<watch::Receiver<Option<CloseReason>>>,
    receive_ended: Option<watch::Receiver<Option<Result<()>>>>,
    protocol: Option<Arc<OnceLock<Protocol>>>,
    receive_cache: Option<Arc<StdMutex<Option<RetiredReceive>>>>,
}

impl ObjectState {
    fn public_handle(&self) -> (u64, u32, u32) {
        (
            self.handle.owner(),
            self.handle.slot(),
            self.handle.generation(),
        )
    }

    async fn closed(&self) -> Result<CloseReason> {
        let mut terminal = self.terminal.clone();
        loop {
            if let Some(reason) = *terminal.borrow_and_update() {
                return Ok(reason);
            }
            terminal.changed().await.map_err(|_| {
                NativeError::message("native object closed without a terminal event")
            })?;
        }
    }

    async fn send_stopped(&self) -> Result<CloseReason> {
        let mut stopped = self
            .send_stopped
            .as_ref()
            .ok_or_else(|| NativeError::message("native object has no send direction"))?
            .clone();
        loop {
            if let Some(reason) = *stopped.borrow_and_update() {
                return Ok(reason);
            }
            stopped.changed().await.map_err(|_| {
                NativeError::message("native send direction stopped without a terminal event")
            })?;
        }
    }

    async fn receive_ended(&self) -> Result<()> {
        let mut ended = self
            .receive_ended
            .as_ref()
            .ok_or_else(|| NativeError::message("native object has no receive direction"))?
            .clone();
        loop {
            if let Some(result) = ended.borrow_and_update().clone() {
                return result;
            }
            ended.changed().await.map_err(|_| {
                NativeError::message("native receive direction ended without a terminal event")
            })?;
        }
    }
}

impl Drop for ObjectState {
    fn drop(&mut self) {
        self.shared.cleanup(Cleanup::ObjectDropped {
            handle: self.handle,
            kind: self.kind,
        });
    }
}

pub struct Listener {
    state: Arc<ObjectState>,
    events: Mutex<DeliveryReceiver<ListenerEvent>>,
}

impl Listener {
    pub async fn port(&self) -> Result<u16> {
        let (reply, wait) = oneshot::channel();
        self.state
            .shared
            .send(Command::ListenerPort {
                handle: self.state.handle,
                reply,
            })
            .await?;
        receive_reply(wait).await
    }

    pub async fn accept(&self) -> Result<Option<ListenerEvent>> {
        self.events.lock().await.receive("listener").await
    }

    pub async fn close(&self) -> Result<CloseReason> {
        start_close(&self.state, 0).await?;
        self.state.closed().await
    }

    pub async fn closed(&self) -> Result<CloseReason> {
        self.state.closed().await
    }

    #[must_use]
    pub fn handle(&self) -> (u64, u32, u32) {
        self.state.public_handle()
    }
}

#[derive(Clone)]
pub struct Connection {
    state: Arc<ObjectState>,
    streams: Arc<Mutex<DeliveryReceiver<BiStream>>>,
}

impl Connection {
    pub async fn open_bi(&self) -> Result<BiStream> {
        let operation = self.state.shared.next_operation();
        let (reply, wait) = oneshot::channel();
        self.state
            .shared
            .send(Command::OpenBi {
                connection: self.state.clone(),
                operation,
                reply,
            })
            .await?;
        receive_reply(wait).await
    }

    pub async fn accept_bi(&self) -> Result<Option<BiStream>> {
        self.streams.lock().await.receive("stream").await
    }

    #[must_use]
    pub fn protocol(&self) -> Option<Protocol> {
        self.state
            .protocol
            .as_ref()
            .and_then(|protocol| protocol.get().copied())
    }

    pub async fn close(&self, application_code: u64) -> Result<CloseReason> {
        start_close(&self.state, application_code).await?;
        self.state.closed().await
    }

    pub async fn closed(&self) -> Result<CloseReason> {
        self.state.closed().await
    }

    #[must_use]
    pub fn handle(&self) -> (u64, u32, u32) {
        self.state.public_handle()
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum SendDirectionState {
    Open,
    Finished,
    Aborted,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ReceiveDirectionState {
    Open,
    Ended,
    Aborted,
}

struct StreamDirections {
    send: Mutex<SendDirectionState>,
    receive: Mutex<ReceiveDirectionState>,
    closing: AtomicBool,
}

struct SendOperationGuard<'a> {
    state: MutexGuard<'a, SendDirectionState>,
    shared: Shared,
    stream: Handle,
    whole_stream: bool,
    armed: bool,
}

impl SendOperationGuard<'_> {
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for SendOperationGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        *self.state = SendDirectionState::Aborted;
        self.shared.cleanup(Cleanup::StreamHalfDropped {
            stream: self.stream,
            direction: Direction::Send,
            abort: true,
        });
        if self.whole_stream {
            self.shared.cleanup(Cleanup::StreamHalfDropped {
                stream: self.stream,
                direction: Direction::Receive,
                abort: true,
            });
        }
    }
}

struct ReceiveCancellationGuard {
    shared: Shared,
    stream: Handle,
    armed: bool,
}

impl Drop for ReceiveCancellationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.cleanup(Cleanup::StreamHalfDropped {
                stream: self.stream,
                direction: Direction::Receive,
                abort: true,
            });
        }
    }
}

pub struct BiStream {
    state: Arc<ObjectState>,
    directions: Arc<StreamDirections>,
    send_done: AtomicBool,
    receive_done: AtomicBool,
    disarmed: bool,
}

impl BiStream {
    fn new(state: Arc<ObjectState>) -> Self {
        Self {
            state,
            directions: Arc::new(StreamDirections {
                send: Mutex::new(SendDirectionState::Open),
                receive: Mutex::new(ReceiveDirectionState::Open),
                closing: AtomicBool::new(false),
            }),
            send_done: AtomicBool::new(false),
            receive_done: AtomicBool::new(false),
            disarmed: false,
        }
    }

    pub fn split(mut self) -> (SendHalf, RecvHalf) {
        self.disarmed = true;
        (
            SendHalf {
                state: self.state.clone(),
                directions: self.directions.clone(),
                done: AtomicBool::new(self.send_done.load(Ordering::Acquire)),
            },
            RecvHalf {
                state: self.state.clone(),
                directions: self.directions.clone(),
                done: AtomicBool::new(self.receive_done.load(Ordering::Acquire)),
            },
        )
    }

    pub async fn send(&self, body: impl AsRef<[u8]>) -> Result<()> {
        send_body(&self.state, &self.directions, body.as_ref()).await
    }

    pub async fn receive(&self) -> Result<Option<OwnedReceive>> {
        let result = receive_body_guarded(&self.state, &self.directions).await;
        if receive_result_is_terminal(&result) {
            self.receive_done.store(true, Ordering::Release);
        }
        result
    }

    pub async fn finish_send(&self) -> Result<()> {
        finish_send(&self.state, &self.directions, &self.send_done).await?;
        self.send_done.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn abort(&self, application_code: u64) -> Result<()> {
        abort_stream(&self.state, &self.directions, application_code).await?;
        self.send_done.store(true, Ordering::Release);
        self.receive_done.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn send_stopped(&self) -> Result<CloseReason> {
        let reason = self.state.send_stopped().await?;
        self.send_done.store(true, Ordering::Release);
        Ok(reason)
    }

    pub async fn receive_ended(&self) -> Result<()> {
        let result = self.state.receive_ended().await;
        self.receive_done.store(true, Ordering::Release);
        result
    }

    pub async fn close(mut self) -> Result<CloseReason> {
        let send = self.directions.send.lock().await;
        if *send != SendDirectionState::Open || self.directions.closing.swap(true, Ordering::AcqRel)
        {
            return Err(NativeError::message("native stream is not open"));
        }
        let mut cancel = SendOperationGuard {
            state: send,
            shared: self.state.shared.clone(),
            stream: self.state.handle,
            whole_stream: true,
            armed: true,
        };
        let mut receive = if let Ok(receive) = self.directions.receive.try_lock() {
            receive
        } else {
            if let Err(error) = abort_half(&self.state, Direction::Receive, DROP_ABORT_CODE).await {
                self.directions.closing.store(false, Ordering::Release);
                cancel.disarm();
                return Err(error);
            }
            self.directions.receive.lock().await
        };
        if let Err(error) = start_close(&self.state, 0).await {
            self.directions.closing.store(false, Ordering::Release);
            cancel.disarm();
            return Err(error);
        }
        *cancel.state = SendDirectionState::Aborted;
        *receive = ReceiveDirectionState::Aborted;
        cancel.disarm();
        self.disarmed = true;
        self.state.closed().await
    }

    pub async fn closed(&self) -> Result<CloseReason> {
        self.state.closed().await
    }

    #[must_use]
    pub fn handle(&self) -> (u64, u32, u32) {
        self.state.public_handle()
    }
}

impl Drop for BiStream {
    fn drop(&mut self) {
        if self.disarmed {
            return;
        }
        self.state.shared.cleanup(Cleanup::StreamHalfDropped {
            stream: self.state.handle,
            direction: Direction::Send,
            abort: !self.send_done.load(Ordering::Acquire),
        });
        self.state.shared.cleanup(Cleanup::StreamHalfDropped {
            stream: self.state.handle,
            direction: Direction::Receive,
            abort: !self.receive_done.load(Ordering::Acquire),
        });
    }
}

pub struct SendHalf {
    state: Arc<ObjectState>,
    directions: Arc<StreamDirections>,
    done: AtomicBool,
}

impl SendHalf {
    pub async fn send(&self, body: impl AsRef<[u8]>) -> Result<()> {
        send_body(&self.state, &self.directions, body.as_ref()).await
    }

    pub async fn finish(&self) -> Result<()> {
        finish_send(&self.state, &self.directions, &self.done).await?;
        self.done.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn abort(&self, application_code: u64) -> Result<()> {
        abort_half_guarded(
            &self.state,
            &self.directions,
            Direction::Send,
            application_code,
        )
        .await?;
        self.done.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn stopped(&self) -> Result<CloseReason> {
        let reason = self.state.send_stopped().await?;
        self.done.store(true, Ordering::Release);
        Ok(reason)
    }

    pub async fn closed(&self) -> Result<CloseReason> {
        self.state.closed().await
    }
}

impl Drop for SendHalf {
    fn drop(&mut self) {
        self.state.shared.cleanup(Cleanup::StreamHalfDropped {
            stream: self.state.handle,
            direction: Direction::Send,
            abort: !self.done.load(Ordering::Acquire),
        });
    }
}

pub struct RecvHalf {
    state: Arc<ObjectState>,
    directions: Arc<StreamDirections>,
    done: AtomicBool,
}

impl RecvHalf {
    pub async fn receive(&self) -> Result<Option<OwnedReceive>> {
        let result = receive_body_guarded(&self.state, &self.directions).await;
        if receive_result_is_terminal(&result) {
            self.done.store(true, Ordering::Release);
        }
        result
    }

    pub async fn abort(&self, application_code: u64) -> Result<()> {
        abort_half_guarded(
            &self.state,
            &self.directions,
            Direction::Receive,
            application_code,
        )
        .await?;
        self.done.store(true, Ordering::Release);
        Ok(())
    }

    pub async fn ended(&self) -> Result<()> {
        let result = self.state.receive_ended().await;
        self.done.store(true, Ordering::Release);
        result
    }

    pub async fn closed(&self) -> Result<CloseReason> {
        self.state.closed().await
    }
}

impl Drop for RecvHalf {
    fn drop(&mut self) {
        self.state.shared.cleanup(Cleanup::StreamHalfDropped {
            stream: self.state.handle,
            direction: Direction::Receive,
            abort: !self.done.load(Ordering::Acquire),
        });
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedReceive(Vec<u8>);

impl OwnedReceive {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

impl AsRef<[u8]> for OwnedReceive {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl std::ops::Deref for OwnedReceive {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_bytes()
    }
}

const ADMISSION_PENDING: u8 = 0;
const ADMISSION_RESPONDING: u8 = 1;
const ADMISSION_ANSWERED: u8 = 2;

#[derive(Clone)]
pub struct Admission {
    inner: Arc<AdmissionInner>,
}

struct AdmissionInner {
    shared: Shared,
    id: u64,
    request: AdmissionRequest,
    state: AtomicU8,
}

struct AdmissionResponseGuard<'a> {
    state: &'a AtomicU8,
    complete: bool,
}

impl Drop for AdmissionResponseGuard<'_> {
    fn drop(&mut self) {
        if !self.complete {
            let _ = self.state.compare_exchange(
                ADMISSION_RESPONDING,
                ADMISSION_PENDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}

impl Admission {
    #[must_use]
    pub fn request(&self) -> &AdmissionRequest {
        &self.inner.request
    }

    pub async fn accept(&self) -> Result<()> {
        self.respond(200).await
    }

    pub async fn reject(&self, status: u16) -> Result<()> {
        self.respond(status).await
    }

    /// Attempts the one permitted admission decision.
    ///
    /// A valid status consumes the decision once it is queued to the native owner. A nonzero
    /// result can report response-delivery failure, but the decision must not be retried.
    pub async fn respond(&self, status: u16) -> Result<()> {
        if status != 200 && !(400..=599).contains(&status) {
            return Err(NativeError::message(
                "admission status must be 200 or between 400 and 599",
            ));
        }
        if self
            .inner
            .state
            .compare_exchange(
                ADMISSION_PENDING,
                ADMISSION_RESPONDING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(NativeError::message(
                "admission is already answered or a response is in progress",
            ));
        }
        let mut guard = AdmissionResponseGuard {
            state: &self.inner.state,
            complete: false,
        };
        let (reply, wait) = oneshot::channel();
        self.inner
            .shared
            .reliable_cleanup(Cleanup::RespondAdmission {
                id: self.inner.id,
                status,
                reply: Some(reply),
            })?;
        self.inner
            .state
            .store(ADMISSION_ANSWERED, Ordering::Release);
        guard.complete = true;
        receive_reply(wait).await
    }
}

impl Drop for AdmissionInner {
    fn drop(&mut self) {
        if self.state.swap(ADMISSION_ANSWERED, Ordering::AcqRel) != ADMISSION_ANSWERED {
            self.shared.cleanup(Cleanup::FailAdmission(self.id));
        }
    }
}

async fn receive_reply<T>(wait: oneshot::Receiver<Result<T>>) -> Result<T> {
    wait.await
        .map_err(|_| NativeError::message("native owner task stopped"))?
}

async fn start_close(state: &Arc<ObjectState>, application_code: u64) -> Result<()> {
    if state.terminal.borrow().is_some() {
        return Ok(());
    }
    let (reply, wait) = oneshot::channel();
    state
        .shared
        .send(Command::StartClose {
            handle: state.handle,
            kind: state.kind,
            application_code,
            reply,
        })
        .await?;
    match receive_reply(wait).await {
        Ok(()) => Ok(()),
        Err(_) if state.terminal.borrow().is_some() => Ok(()),
        Err(error) => Err(error),
    }
}

async fn send_body(
    state: &Arc<ObjectState>,
    directions: &Arc<StreamDirections>,
    body: &[u8],
) -> Result<()> {
    let send = directions.send.lock().await;
    if *send != SendDirectionState::Open || directions.closing.load(Ordering::Acquire) {
        return Err(NativeError::message(
            "native stream send direction is not open",
        ));
    }
    let mut guard = SendOperationGuard {
        state: send,
        shared: state.shared.clone(),
        stream: state.handle,
        whole_stream: false,
        armed: true,
    };
    let operation = state.shared.next_operation();
    let (reply, wait) = oneshot::channel();
    if let Err(error) = state
        .shared
        .send(Command::Send {
            stream: state.handle,
            operation,
            body: body.to_vec(),
            reply,
        })
        .await
    {
        guard.disarm();
        return Err(error);
    }
    match receive_reply(wait).await {
        Ok(()) => {
            guard.disarm();
            Ok(())
        }
        Err(error) => {
            guard.disarm();
            Err(error)
        }
    }
}

async fn receive_body(state: &Arc<ObjectState>) -> Result<Option<OwnedReceive>> {
    if let Some(cache) = &state.receive_cache
        && let Some(result) = take_cached_receive(cache)
    {
        return result;
    }
    let (reply, wait) = oneshot::channel();
    state
        .shared
        .send(Command::Receive {
            stream: state.handle,
            reply,
        })
        .await?;
    receive_reply(wait).await
}

async fn receive_body_guarded(
    state: &Arc<ObjectState>,
    directions: &Arc<StreamDirections>,
) -> Result<Option<OwnedReceive>> {
    let mut receive = directions
        .receive
        .try_lock()
        .map_err(|_| NativeError::message("a receive is already pending for this native stream"))?;
    if *receive != ReceiveDirectionState::Open {
        return Err(NativeError::message(
            "native stream receive direction is not open",
        ));
    }
    let result = receive_body(state).await;
    if receive_result_is_terminal(&result) {
        *receive = ReceiveDirectionState::Ended;
    }
    result
}

fn receive_result_is_terminal(result: &Result<Option<OwnedReceive>>) -> bool {
    matches!(result, Ok(None)) || matches!(result, Err(error) if error.is_terminal())
}

async fn finish_send(
    state: &Arc<ObjectState>,
    directions: &Arc<StreamDirections>,
    done: &AtomicBool,
) -> Result<()> {
    let send = directions.send.lock().await;
    if *send != SendDirectionState::Open || directions.closing.load(Ordering::Acquire) {
        return Err(NativeError::message(
            "native stream send direction is not open",
        ));
    }
    let mut guard = SendOperationGuard {
        state: send,
        shared: state.shared.clone(),
        stream: state.handle,
        whole_stream: false,
        armed: true,
    };
    let (reply, wait) = oneshot::channel();
    if let Err(error) = state
        .shared
        .send(Command::FinishSend {
            stream: state.handle,
            reply,
        })
        .await
    {
        guard.disarm();
        return Err(error);
    }
    *guard.state = SendDirectionState::Finished;
    done.store(true, Ordering::Release);
    guard.disarm();
    match receive_reply(wait).await {
        Ok(()) => Ok(()),
        Err(error) => {
            *guard.state = SendDirectionState::Open;
            done.store(false, Ordering::Release);
            Err(error)
        }
    }
}

async fn abort_half(
    state: &Arc<ObjectState>,
    direction: Direction,
    application_code: u64,
) -> Result<()> {
    let (reply, wait) = oneshot::channel();
    state
        .shared
        .send(Command::AbortHalf {
            stream: state.handle,
            direction,
            application_code,
            reply,
        })
        .await?;
    receive_reply(wait).await
}

async fn abort_half_guarded(
    state: &Arc<ObjectState>,
    directions: &Arc<StreamDirections>,
    direction: Direction,
    application_code: u64,
) -> Result<()> {
    match direction {
        Direction::Send => {
            let send = directions.send.lock().await;
            if *send != SendDirectionState::Open || directions.closing.load(Ordering::Acquire) {
                return Err(NativeError::message(
                    "native stream send direction is not open",
                ));
            }
            let mut guard = SendOperationGuard {
                state: send,
                shared: state.shared.clone(),
                stream: state.handle,
                whole_stream: false,
                armed: true,
            };
            match abort_half(state, direction, application_code).await {
                Ok(()) => {
                    *guard.state = SendDirectionState::Aborted;
                    guard.disarm();
                    Ok(())
                }
                Err(error) => {
                    guard.disarm();
                    Err(error)
                }
            }
        }
        Direction::Receive => {
            if directions.closing.load(Ordering::Acquire) {
                return Err(NativeError::message("native stream is closing"));
            }
            let mut cancel = ReceiveCancellationGuard {
                shared: state.shared.clone(),
                stream: state.handle,
                armed: true,
            };
            if let Ok(mut receive) = directions.receive.try_lock() {
                if *receive != ReceiveDirectionState::Open {
                    cancel.armed = false;
                    return Err(NativeError::message(
                        "native stream receive direction is not open",
                    ));
                }
                if let Err(error) = abort_half(state, direction, application_code).await {
                    cancel.armed = false;
                    return Err(error);
                }
                *receive = ReceiveDirectionState::Aborted;
                cancel.armed = false;
                return Ok(());
            }
            if let Err(error) = abort_half(state, direction, application_code).await {
                cancel.armed = false;
                return Err(error);
            }
            let mut receive = directions.receive.lock().await;
            *receive = ReceiveDirectionState::Aborted;
            cancel.armed = false;
            Ok(())
        }
    }
}

async fn abort_stream(
    state: &Arc<ObjectState>,
    directions: &Arc<StreamDirections>,
    application_code: u64,
) -> Result<()> {
    let send = directions.send.lock().await;
    if *send != SendDirectionState::Open || directions.closing.load(Ordering::Acquire) {
        return Err(NativeError::message("native stream is not open"));
    }
    directions.closing.store(true, Ordering::Release);
    let mut guard = SendOperationGuard {
        state: send,
        shared: state.shared.clone(),
        stream: state.handle,
        whole_stream: true,
        armed: true,
    };
    let (reply, wait) = oneshot::channel();
    if let Err(error) = state
        .shared
        .send(Command::AbortStream {
            stream: state.handle,
            application_code,
            reply,
        })
        .await
    {
        directions.closing.store(false, Ordering::Release);
        guard.disarm();
        return Err(error);
    }
    match receive_reply(wait).await {
        Ok(()) => {
            let mut receive = directions.receive.lock().await;
            *receive = ReceiveDirectionState::Aborted;
            *guard.state = SendDirectionState::Aborted;
            guard.disarm();
            Ok(())
        }
        Err(error) => {
            directions.closing.store(false, Ordering::Release);
            guard.disarm();
            Err(error)
        }
    }
}

type Reply<T> = oneshot::Sender<Result<T>>;

#[derive(Clone, Copy)]
enum Direction {
    Send,
    Receive,
}

enum Command {
    Listen {
        config: EndpointConfig,
        reply: Reply<Listener>,
    },
    ListenerPort {
        handle: Handle,
        reply: Reply<u16>,
    },
    Dial {
        config: EndpointConfig,
        reply: Reply<Connection>,
    },
    OpenBi {
        connection: Arc<ObjectState>,
        operation: OperationId,
        reply: Reply<BiStream>,
    },
    Send {
        stream: Handle,
        operation: OperationId,
        body: Vec<u8>,
        reply: Reply<()>,
    },
    Receive {
        stream: Handle,
        reply: Reply<Option<OwnedReceive>>,
    },
    FinishSend {
        stream: Handle,
        reply: Reply<()>,
    },
    AbortHalf {
        stream: Handle,
        direction: Direction,
        application_code: u64,
        reply: Reply<()>,
    },
    AbortStream {
        stream: Handle,
        application_code: u64,
        reply: Reply<()>,
    },
    StartClose {
        handle: Handle,
        kind: ObjectKind,
        application_code: u64,
        reply: Reply<()>,
    },
    Diagnostics {
        reply: Reply<Diagnostics>,
    },
}

enum Cleanup {
    ObjectDropped {
        handle: Handle,
        kind: ObjectKind,
    },
    StreamHalfDropped {
        stream: Handle,
        direction: Direction,
        abort: bool,
    },
    RespondAdmission {
        id: u64,
        status: u16,
        reply: Option<Reply<()>>,
    },
    FailAdmission(u64),
    Shutdown,
}

struct ListenerRecord {
    state: Weak<ObjectState>,
    terminal: watch::Sender<Option<CloseReason>>,
    delivery: mpsc::Sender<ListenerEvent>,
    delivery_rejections: watch::Sender<u64>,
    close_started: bool,
}

struct ConnectionRecord {
    state: Weak<ObjectState>,
    terminal: watch::Sender<Option<CloseReason>>,
    delivery: mpsc::Sender<BiStream>,
    delivery_rejections: watch::Sender<u64>,
    close_started: bool,
}

#[allow(clippy::struct_excessive_bools)]
struct StreamRecord {
    terminal: watch::Sender<Option<CloseReason>>,
    send_stopped: watch::Sender<Option<CloseReason>>,
    receive_ended: watch::Sender<Option<Result<()>>>,
    receive_cache: Arc<StdMutex<Option<RetiredReceive>>>,
    pending_receive: Option<Reply<Option<OwnedReceive>>>,
    queued_receives: VecDeque<Vec<u8>>,
    receive_end: Option<Result<()>>,
    pending_send_count: usize,
    send_dropped: bool,
    receive_dropped: bool,
    send_abort_complete: bool,
    receive_abort_complete: bool,
    send_abort_retry: bool,
    receive_abort_retry: bool,
    close_started: bool,
    object_dropped: bool,
}

struct RetiredReceive {
    queued: VecDeque<Vec<u8>>,
    end: Result<()>,
}

fn take_cached_receive(
    cache: &Arc<StdMutex<Option<RetiredReceive>>>,
) -> Option<Result<Option<OwnedReceive>>> {
    let mut cache = cache.lock().expect("stream receive cache lock poisoned");
    let retired = cache.as_mut()?;
    if let Some(body) = retired.queued.pop_front() {
        return Some(Ok(Some(OwnedReceive(body))));
    }
    Some(retired.end.clone().map(|()| None))
}

struct PendingDial {
    // Ownership keeps the ID reserved until the provider completion arrives.
    _operation: OperationId,
    expected: Handle,
    connection: Connection,
    reply: Option<Reply<Connection>>,
    cancel_requested: bool,
}

struct PendingOpen {
    // Ownership keeps the ID reserved until the provider completion arrives.
    _operation: OperationId,
    expected: Handle,
    parent: Handle,
    stream: BiStream,
    reply: Option<Reply<BiStream>>,
    cancel_requested: bool,
}

struct PendingSend {
    // Ownership keeps the ID reserved until the provider completion arrives.
    _operation: OperationId,
    expected: Handle,
    reply: Option<Reply<()>>,
}

#[allow(clippy::struct_excessive_bools)]
struct Owner {
    shared: Shared,
    raw: RawTransport,
    transport_terminal: watch::Sender<Option<Result<()>>>,
    delivery_capacity: usize,
    listeners: HashMap<Handle, ListenerRecord>,
    connections: HashMap<Handle, ConnectionRecord>,
    streams: HashMap<Handle, StreamRecord>,
    retired_receives: HashMap<Handle, Arc<StdMutex<Option<RetiredReceive>>>>,
    early_send_stops: HashMap<Handle, CloseReason>,
    early_receive_ends: HashMap<Handle, Result<()>>,
    pending_dials: HashMap<u64, PendingDial>,
    pending_opens: HashMap<u64, PendingOpen>,
    pending_sends: HashMap<u64, PendingSend>,
    admissions: HashMap<u64, RawEvent>,
    retirements: VecDeque<(Handle, ObjectKind)>,
    close_retries: VecDeque<(Handle, ObjectKind, u64)>,
    next_admission: u64,
    last_event_sequence: u64,
    shutdown_barrier_requested: bool,
    shutdown_requested: bool,
    transport_close_admitted: bool,
    stopped_seen: bool,
    events_empty: bool,
    cleanup_failed: bool,
    failure: Option<NativeError>,
}

impl Drop for Owner {
    fn drop(&mut self) {
        let Some(raw) = self.raw.take() else {
            return;
        };
        self.shared.mark_shutdown_started();
        if self.transport_terminal.borrow().is_none() {
            self.transport_terminal
                .send_replace(Some(Err(NativeError::message(
                    "native owner task stopped; emergency cleanup started",
                ))));
        }
        let admissions = self
            .admissions
            .drain()
            .map(|(_, event)| event)
            .collect::<Vec<_>>();
        let mut handles = HashSet::new();
        handles.extend(
            self.listeners
                .keys()
                .copied()
                .map(|handle| (handle, ObjectKind::Listener)),
        );
        handles.extend(
            self.connections
                .keys()
                .copied()
                .map(|handle| (handle, ObjectKind::Connection)),
        );
        handles.extend(
            self.streams
                .keys()
                .copied()
                .map(|handle| (handle, ObjectKind::Stream)),
        );
        handles.extend(self.retirements.drain(..));
        handles.extend(
            self.early_send_stops
                .keys()
                .chain(self.early_receive_ends.keys())
                .copied()
                .map(|handle| (handle, ObjectKind::Stream)),
        );
        enqueue_finalizer(FinalizerJob {
            raw,
            admissions,
            handles,
            failure_reporter: self.transport_terminal.clone(),
        });
    }
}

fn emergency_cleanup(
    mut raw: RawTransport,
    admissions: Vec<RawEvent>,
    mut handles: HashSet<(Handle, ObjectKind)>,
    failure_reporter: Option<&watch::Sender<Option<Result<()>>>>,
) -> Result<()> {
    let mut first_failure = None;
    for event in admissions {
        if let Err(error) = raw.respond_admission(&event, 500) {
            remember_cleanup_failure(&mut first_failure, error, failure_reporter);
        }
        raw.release_event(event);
    }
    let mut close_admitted = false;
    let mut stopped = false;
    while !raw.released() {
        if !close_admitted {
            match raw.close() {
                Ok(()) => close_admitted = true,
                Err(error) if error.is_retryable_release() => {}
                Err(error) => {
                    remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                }
            }
        }
        loop {
            match raw.next_event() {
                Ok(Some(event)) => {
                    if let Ok(info) = raw.event_info(&event) {
                        if info.kind == EventKind::Stopped {
                            stopped = true;
                        }
                        if let Some(kind) = ObjectKind::from_raw(info.subject_kind) {
                            handles.insert((info.subject, kind));
                        }
                        if matches!(
                            info.kind,
                            EventKind::Http3Admission | EventKind::WebTransportAdmission
                        ) && let Err(error) = raw.respond_admission(&event, 503)
                        {
                            remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                        }
                    }
                    raw.release_event(event);
                }
                Ok(None) => break,
                Err(error) if error.is_retryable_release() => break,
                Err(error) => {
                    remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                    break;
                }
            }
        }
        if stopped {
            handles.retain(|(handle, kind)| match raw.release_handle(*handle, *kind) {
                Ok(()) => false,
                Err(error) if error.is_retryable_release() => true,
                Err(error) => {
                    remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                    true
                }
            });
            if handles.is_empty() {
                match raw.drain() {
                    Ok(()) => match raw.release() {
                        Ok(()) => return first_failure.map_or(Ok(()), Err),
                        Err(error) if error.is_retryable_release() => {}
                        Err(error) => {
                            remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                        }
                    },
                    Err(error) if error.is_retryable_release() => {}
                    Err(error) => {
                        remember_cleanup_failure(&mut first_failure, error, failure_reporter);
                    }
                }
            }
        }
        std::thread::sleep(RELEASE_RETRY);
    }
    first_failure.map_or(Ok(()), Err)
}

fn remember_cleanup_failure(
    first_failure: &mut Option<NativeError>,
    error: NativeError,
    failure_reporter: Option<&watch::Sender<Option<Result<()>>>>,
) {
    if first_failure.is_some() {
        return;
    }
    if let Some(reporter) = failure_reporter {
        reporter.send_replace(Some(Err(NativeError::message(format!(
            "native emergency cleanup encountered an error and will keep retrying: {error}",
        )))));
    }
    *first_failure = Some(error);
}

struct BorrowedWakeFd(RawFd);
impl AsRawFd for BorrowedWakeFd {
    fn as_raw_fd(&self) -> RawFd {
        self.0
    }
}

async fn wait_for_wake(
    wakes: &[AsyncFd<BorrowedWakeFd>],
) -> std::io::Result<AsyncFdReadyGuard<'_, BorrowedWakeFd>> {
    let futures = wakes
        .iter()
        .map(|wake| Box::pin(wake.readable()))
        .collect::<Vec<_>>();
    let (result, _, _) = select_all(futures).await;
    result
}

fn command_queue_is_drained(shared: &Shared, commands_empty: bool) -> bool {
    commands_empty && shared.command_reservations.load(Ordering::Acquire) == 0
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn owner_loop(
    shared: Shared,
    transport_terminal: watch::Sender<Option<Result<()>>>,
    raw: RawTransport,
    delivery_capacity: usize,
    mut commands: mpsc::Receiver<Command>,
    mut cleanup: mpsc::UnboundedReceiver<Cleanup>,
    ready: oneshot::Sender<Result<()>>,
) {
    let wake_fds = match raw.wake_fds() {
        Ok(wake_fds) => wake_fds,
        Err(error) => {
            let _ = ready.send(Err(error));
            spawn_failed_start_cleanup(raw, &transport_terminal);
            return;
        }
    };
    let mut wakes = Vec::with_capacity(wake_fds.len());
    for wake_fd in wake_fds {
        match AsyncFd::with_interest(BorrowedWakeFd(wake_fd), Interest::READABLE) {
            Ok(wake) => wakes.push(wake),
            Err(error) => {
                let _ = ready.send(Err(NativeError::message(error.to_string())));
                drop(wakes);
                spawn_failed_start_cleanup(raw, &transport_terminal);
                return;
            }
        }
    }
    let mut owner = Owner {
        shared,
        raw,
        transport_terminal,
        delivery_capacity,
        listeners: HashMap::new(),
        connections: HashMap::new(),
        streams: HashMap::new(),
        retired_receives: HashMap::new(),
        early_send_stops: HashMap::new(),
        early_receive_ends: HashMap::new(),
        pending_dials: HashMap::new(),
        pending_opens: HashMap::new(),
        pending_sends: HashMap::new(),
        admissions: HashMap::new(),
        retirements: VecDeque::new(),
        close_retries: VecDeque::new(),
        next_admission: 1,
        last_event_sequence: 0,
        shutdown_barrier_requested: false,
        shutdown_requested: false,
        transport_close_admitted: false,
        stopped_seen: false,
        events_empty: true,
        cleanup_failed: false,
        failure: None,
    };
    let _ = ready.send(Ok(()));
    let mut wakes = Some(wakes);

    while !owner.raw.released() {
        let timeout = if owner.stopped_seen
            || (owner.shutdown_requested && !owner.transport_close_admitted)
            || !owner.close_retries.is_empty()
        {
            RELEASE_RETRY
        } else {
            owner.raw.poll_timeout().unwrap_or(IDLE_POLL)
        };
        tokio::select! {
            Some(item) = cleanup.recv() => owner.cleanup(item),
            Some(command) = commands.recv() => owner.command(command),
            readiness = async {
                match &wakes {
                    Some(wakes) => wait_for_wake(wakes).await,
                    None => std::future::pending().await,
                }
            } => {
                match readiness {
                    Ok(mut guard) => match owner.drain_events() {
                        Ok(true) => guard.clear_ready(),
                        Ok(false) => {}
                        Err(error) => owner.fail_transport(error),
                    },
                    Err(error) => owner.fail_transport(NativeError::message(error.to_string())),
                }
            }
            () = time::sleep(timeout) => {
                if let Err(error) = owner.drain_events() {
                    owner.fail_transport(error);
                }
            }
        }
        if owner.shutdown_barrier_requested
            && command_queue_is_drained(&owner.shared, commands.is_empty())
        {
            owner.begin_shutdown();
        }
        owner.cancel_abandoned_operations();
        owner.retry_transport_close();
        if owner.cleanup_failed {
            break;
        }
        owner.retry_directional_aborts();
        owner.retry_object_closes();
        owner.retry_retirements();
        if owner.cleanup_failed {
            break;
        }
        if owner.ready_for_release() {
            drop(wakes.take());
            match owner.raw.release() {
                Ok(()) => {
                    let result = owner.failure.take().map_or(Ok(()), Err);
                    owner.transport_terminal.send_replace(Some(result));
                }
                Err(error) if error.is_retryable_release() => {}
                Err(error) => {
                    owner.fail_cleanup(error);
                    break;
                }
            }
        }
        if owner.cleanup_failed {
            break;
        }
    }
}

fn spawn_failed_start_cleanup(raw: RawTransport, terminal: &watch::Sender<Option<Result<()>>>) {
    enqueue_finalizer(FinalizerJob {
        raw,
        admissions: Vec::new(),
        handles: HashSet::new(),
        failure_reporter: terminal.clone(),
    });
}

impl Owner {
    #[allow(clippy::too_many_lines)]
    fn command(&mut self, command: Command) {
        if self.shutdown_requested {
            Self::reject_command(command);
            return;
        }
        match command {
            Command::Listen { config, reply } => {
                let result = ffi::endpoint(&config).and_then(|endpoint| self.raw.listen(&endpoint));
                match result {
                    Ok(handle) => {
                        let (terminal_tx, terminal_rx) = watch::channel(None);
                        let (delivery, events) = mpsc::channel(self.delivery_capacity);
                        let (delivery_rejections, rejections) = watch::channel(0);
                        let state = Arc::new(ObjectState {
                            shared: self.shared.clone(),
                            handle,
                            kind: ObjectKind::Listener,
                            terminal: terminal_rx,
                            send_stopped: None,
                            receive_ended: None,
                            protocol: None,
                            receive_cache: None,
                        });
                        self.listeners.insert(
                            handle,
                            ListenerRecord {
                                state: Arc::downgrade(&state),
                                terminal: terminal_tx,
                                delivery,
                                delivery_rejections,
                                close_started: false,
                            },
                        );
                        let _ = reply.send(Ok(Listener {
                            state,
                            events: Mutex::new(DeliveryReceiver {
                                items: events,
                                rejections,
                                reported_rejections: 0,
                            }),
                        }));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::ListenerPort { handle, reply } => {
                let _ = reply.send(self.raw.listener_port(handle));
            }
            Command::Dial { config, reply } => {
                let operation = self.shared.next_operation();
                let operation_id = operation.get();
                let result = ffi::endpoint(&config)
                    .and_then(|endpoint| self.raw.dial(&endpoint, operation_id));
                match result {
                    Ok(handle) => {
                        let connection = self.make_connection(handle);
                        self.pending_dials.insert(
                            operation_id,
                            PendingDial {
                                _operation: operation,
                                expected: handle,
                                connection,
                                reply: Some(reply),
                                cancel_requested: false,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::OpenBi {
                connection,
                operation,
                reply,
            } => {
                if reply.is_closed() {
                    return;
                }
                let operation_id = operation.get();
                match self.raw.open_bi(connection.handle, operation_id) {
                    Ok(handle) => {
                        let stream = self.make_stream(handle, connection.protocol.clone());
                        self.pending_opens.insert(
                            operation_id,
                            PendingOpen {
                                _operation: operation,
                                expected: handle,
                                parent: connection.handle,
                                stream,
                                reply: Some(reply),
                                cancel_requested: false,
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::Send {
                stream,
                operation,
                body,
                reply,
            } => {
                if reply.is_closed() {
                    if let Some(record) = self.streams.get_mut(&stream) {
                        record.send_abort_retry = true;
                    }
                    return;
                }
                let operation_id = operation.get();
                match self.raw.send(stream, operation_id, &body) {
                    Ok(()) => {
                        if let Some(record) = self.streams.get_mut(&stream) {
                            record.pending_send_count += 1;
                        } else {
                            let error =
                                NativeError::message("admitted send referenced a non-live stream");
                            let _ = reply.send(Err(error.clone()));
                            self.fail_transport(error);
                            return;
                        }
                        self.pending_sends.insert(
                            operation_id,
                            PendingSend {
                                _operation: operation,
                                expected: stream,
                                reply: Some(reply),
                            },
                        );
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
            Command::Receive { stream, reply } => self.receive(stream, reply),
            Command::FinishSend { stream, reply } => {
                if reply.is_closed() {
                    self.defer_directional_abort(stream, Direction::Send);
                    return;
                }
                let result = self.raw.finish_send(stream);
                let rejected = result.is_err();
                if reply.send(result).is_err() && rejected {
                    self.defer_directional_abort(stream, Direction::Send);
                }
            }
            Command::AbortHalf {
                stream,
                direction,
                application_code,
                reply,
            } => {
                if reply.is_closed() {
                    self.defer_directional_abort(stream, direction);
                    return;
                }
                let result = match direction {
                    Direction::Send => self.raw.abort_send(stream, application_code),
                    Direction::Receive => self.raw.abort_receive(stream, application_code),
                };
                let admitted = result.is_ok();
                if admitted {
                    self.complete_directional_abort(stream, direction);
                    if matches!(direction, Direction::Receive) {
                        self.settle_pending_receive_abort(stream);
                    }
                }
                let _ = reply.send(result);
            }
            Command::AbortStream {
                stream,
                application_code,
                reply,
            } => {
                if reply.is_closed() {
                    self.defer_stream_abort(stream);
                    return;
                }
                let result = self.raw.abort_stream(stream, application_code);
                let admitted = result.is_ok();
                if admitted {
                    self.complete_directional_abort(stream, Direction::Send);
                    self.complete_directional_abort(stream, Direction::Receive);
                    self.settle_pending_receive_abort(stream);
                }
                let _ = reply.send(result);
            }
            Command::StartClose {
                handle,
                kind,
                application_code,
                reply,
            } => {
                let _ = reply.send(self.start_close(handle, kind, application_code));
            }
            Command::Diagnostics { reply } => {
                let _ = reply.send(self.raw.diagnostics().map(map_diagnostics));
            }
        }
    }

    fn reject_command(command: Command) {
        let error = NativeError::message("native transport is shutting down");
        match command {
            Command::Listen { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::ListenerPort { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::Dial { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::OpenBi { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::Send { reply, .. }
            | Command::FinishSend { reply, .. }
            | Command::AbortHalf { reply, .. }
            | Command::AbortStream { reply, .. }
            | Command::StartClose { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::Receive { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            Command::Diagnostics { reply } => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn cleanup(&mut self, cleanup: Cleanup) {
        match cleanup {
            Cleanup::ObjectDropped { handle, kind } => {
                if kind == ObjectKind::Stream {
                    self.retired_receives.remove(&handle);
                    if let Some(record) = self.streams.get_mut(&handle) {
                        record.object_dropped = true;
                    }
                    return;
                }
                if self.start_close(handle, kind, DROP_ABORT_CODE).is_err() {
                    self.queue_close_retry(handle, kind, DROP_ABORT_CODE);
                }
            }
            Cleanup::StreamHalfDropped {
                stream,
                direction,
                abort,
            } => {
                let receive_aborted = {
                    let Some(record) = self.streams.get_mut(&stream) else {
                        return;
                    };
                    let mut receive_aborted = false;
                    match direction {
                        Direction::Send => {
                            if !record.send_dropped && abort && !record.send_abort_complete {
                                if record.pending_send_count == 0 {
                                    if self.raw.abort_send(stream, DROP_ABORT_CODE).is_ok() {
                                        record.send_abort_complete = true;
                                        record.send_abort_retry = false;
                                    } else {
                                        record.send_abort_retry = true;
                                    }
                                } else {
                                    record.send_abort_retry = true;
                                }
                            }
                            record.send_dropped = true;
                        }
                        Direction::Receive => {
                            if !record.receive_dropped && abort && !record.receive_abort_complete {
                                if self.raw.abort_receive(stream, DROP_ABORT_CODE).is_ok() {
                                    record.receive_abort_complete = true;
                                    record.receive_abort_retry = false;
                                    receive_aborted = true;
                                } else {
                                    record.receive_abort_retry = true;
                                }
                            }
                            record.receive_dropped = true;
                        }
                    }
                    receive_aborted
                };
                if receive_aborted {
                    self.settle_pending_receive_abort(stream);
                }
            }
            Cleanup::RespondAdmission { id, status, reply } => {
                let result = self.respond_admission(id, status);
                if let Some(reply) = reply {
                    let _ = reply.send(result);
                }
            }
            Cleanup::FailAdmission(id) => {
                let _ = self.respond_admission(id, 500);
            }
            Cleanup::Shutdown => self.shutdown_barrier_requested = true,
        }
    }

    fn make_connection(&mut self, handle: Handle) -> Connection {
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let (delivery, streams) = mpsc::channel(self.delivery_capacity);
        let (delivery_rejections, rejections) = watch::channel(0);
        let protocol = Arc::new(OnceLock::new());
        let state = Arc::new(ObjectState {
            shared: self.shared.clone(),
            handle,
            kind: ObjectKind::Connection,
            terminal: terminal_rx,
            send_stopped: None,
            receive_ended: None,
            protocol: Some(protocol),
            receive_cache: None,
        });
        self.connections.insert(
            handle,
            ConnectionRecord {
                state: Arc::downgrade(&state),
                terminal: terminal_tx,
                delivery,
                delivery_rejections,
                close_started: false,
            },
        );
        Connection {
            state,
            streams: Arc::new(Mutex::new(DeliveryReceiver {
                items: streams,
                rejections,
                reported_rejections: 0,
            })),
        }
    }

    fn track_orphan_connection(&mut self, handle: Handle) {
        let (terminal, _) = watch::channel(None);
        let (delivery, _) = mpsc::channel(self.delivery_capacity);
        let (delivery_rejections, _) = watch::channel(0);
        self.connections.entry(handle).or_insert(ConnectionRecord {
            state: Weak::new(),
            terminal,
            delivery,
            delivery_rejections,
            close_started: false,
        });
        if self
            .start_close(handle, ObjectKind::Connection, DROP_ABORT_CODE)
            .is_err()
        {
            self.queue_close_retry(handle, ObjectKind::Connection, DROP_ABORT_CODE);
        }
    }

    fn track_orphan_stream(&mut self, handle: Handle) {
        let (terminal, _) = watch::channel(None);
        let receive_cache = Arc::new(StdMutex::new(None));
        let send_stop = self.early_send_stops.remove(&handle);
        let receive_end = self.early_receive_ends.remove(&handle);
        let (send_stopped, _) = watch::channel(send_stop);
        let (receive_ended, _) = watch::channel(receive_end.clone());
        self.streams.entry(handle).or_insert(StreamRecord {
            terminal,
            send_stopped,
            receive_ended,
            receive_cache,
            pending_receive: None,
            queued_receives: VecDeque::new(),
            receive_end,
            pending_send_count: 0,
            send_dropped: true,
            receive_dropped: true,
            send_abort_complete: false,
            receive_abort_complete: false,
            send_abort_retry: false,
            receive_abort_retry: false,
            close_started: false,
            object_dropped: true,
        });
        if self
            .start_close(handle, ObjectKind::Stream, DROP_ABORT_CODE)
            .is_err()
        {
            self.queue_close_retry(handle, ObjectKind::Stream, DROP_ABORT_CODE);
        }
    }

    fn make_stream(
        &mut self,
        handle: Handle,
        protocol: Option<Arc<OnceLock<Protocol>>>,
    ) -> BiStream {
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let receive_cache = Arc::new(StdMutex::new(None));
        let send_stop = self.early_send_stops.remove(&handle);
        let receive_end = self.early_receive_ends.remove(&handle);
        let (send_stopped_tx, send_stopped_rx) = watch::channel(send_stop);
        let (receive_ended_tx, receive_ended_rx) = watch::channel(receive_end.clone());
        let state = Arc::new(ObjectState {
            shared: self.shared.clone(),
            handle,
            kind: ObjectKind::Stream,
            terminal: terminal_rx,
            send_stopped: Some(send_stopped_rx),
            receive_ended: Some(receive_ended_rx),
            protocol,
            receive_cache: Some(receive_cache.clone()),
        });
        self.streams.insert(
            handle,
            StreamRecord {
                terminal: terminal_tx,
                send_stopped: send_stopped_tx,
                receive_ended: receive_ended_tx,
                receive_cache,
                pending_receive: None,
                queued_receives: VecDeque::new(),
                receive_end,
                pending_send_count: 0,
                send_dropped: false,
                receive_dropped: false,
                send_abort_complete: false,
                receive_abort_complete: false,
                send_abort_retry: false,
                receive_abort_retry: false,
                close_started: false,
                object_dropped: false,
            },
        );
        BiStream::new(state)
    }

    fn receive(&mut self, stream: Handle, reply: Reply<Option<OwnedReceive>>) {
        if let Some(cache) = self.retired_receives.get(&stream)
            && let Some(result) = take_cached_receive(cache)
        {
            let _ = reply.send(result);
            return;
        }
        let Some(record) = self.streams.get_mut(&stream) else {
            let _ = reply.send(Err(NativeError::message("native stream is no longer live")));
            return;
        };
        clear_closed_reply(&mut record.pending_receive);
        if record.pending_receive.is_some() {
            let _ = reply.send(Err(NativeError::message(
                "a receive is already pending for this stream",
            )));
            return;
        }
        if let Some(body) = record.queued_receives.pop_front() {
            deliver_receive(
                &mut record.queued_receives,
                reply,
                Ok(Some(OwnedReceive(body))),
            );
            return;
        }
        if reply.is_closed() {
            return;
        }
        match self.raw.receive(stream) {
            Ok(body) => deliver_receive(
                &mut record.queued_receives,
                reply,
                Ok(Some(OwnedReceive(body))),
            ),
            Err(error) if error.is_would_block() => {
                if let Some(end) = &record.receive_end {
                    let _ = reply.send(end.clone().map(|()| None));
                } else {
                    record.pending_receive = Some(reply);
                }
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn start_close(&mut self, handle: Handle, kind: ObjectKind, code: u64) -> Result<()> {
        let started = match kind {
            ObjectKind::Listener => self
                .listeners
                .get(&handle)
                .map(|record| record.close_started),
            ObjectKind::Connection => self
                .connections
                .get(&handle)
                .map(|record| record.close_started),
            ObjectKind::Stream => self.streams.get(&handle).and_then(|record| {
                (record.pending_send_count == 0).then_some(record.close_started)
            }),
        };
        match started {
            Some(true) => return Ok(()),
            Some(false) => {}
            None => return Err(NativeError::message("native object is no longer live")),
        }
        let result = match kind {
            ObjectKind::Listener => self.raw.close_listener(handle),
            ObjectKind::Connection => self.raw.close_connection(handle, code),
            ObjectKind::Stream => self.raw.close_stream(handle),
        };
        result?;
        match kind {
            ObjectKind::Listener => {
                self.listeners
                    .get_mut(&handle)
                    .expect("listener remains live")
                    .close_started = true;
            }
            ObjectKind::Connection => {
                self.connections
                    .get_mut(&handle)
                    .expect("connection remains live")
                    .close_started = true;
            }
            ObjectKind::Stream => {
                self.streams
                    .get_mut(&handle)
                    .expect("stream remains live")
                    .close_started = true;
            }
        }
        Ok(())
    }

    fn queue_close_retry(&mut self, handle: Handle, kind: ObjectKind, code: u64) {
        if !self
            .close_retries
            .iter()
            .any(|entry| *entry == (handle, kind, code))
        {
            self.close_retries.push_back((handle, kind, code));
        }
    }

    fn cancel_abandoned_operations(&mut self) {
        for pending in self.pending_dials.values_mut() {
            if pending
                .reply
                .as_ref()
                .is_some_and(oneshot::Sender::is_closed)
                && !pending.cancel_requested
                && self.raw.cancel_dial(pending.expected).is_ok()
            {
                pending.cancel_requested = true;
            }
        }
        let abandoned_opens = self
            .pending_opens
            .iter()
            .filter_map(|(operation, pending)| {
                (pending
                    .reply
                    .as_ref()
                    .is_some_and(oneshot::Sender::is_closed)
                    && !pending.cancel_requested)
                    .then_some((*operation, pending.expected))
            })
            .collect::<Vec<_>>();
        for (operation, handle) in abandoned_opens {
            if self
                .start_close(handle, ObjectKind::Stream, DROP_ABORT_CODE)
                .is_ok()
                && let Some(pending) = self.pending_opens.get_mut(&operation)
            {
                pending.cancel_requested = true;
            }
        }
        let abandoned_sends = self
            .pending_sends
            .values()
            .filter_map(|pending| {
                pending
                    .reply
                    .as_ref()
                    .is_some_and(oneshot::Sender::is_closed)
                    .then_some(pending.expected)
            })
            .collect::<Vec<_>>();
        for stream in abandoned_sends {
            self.defer_directional_abort(stream, Direction::Send);
        }
    }

    fn settle_pending_receive_abort(&mut self, stream: Handle) {
        if let Some(record) = self.streams.get_mut(&stream) {
            settle_receive_abort(&mut record.receive_end, &mut record.pending_receive);
            if let Some(end) = record.receive_end.clone() {
                record.receive_ended.send_replace(Some(end));
            }
        }
    }

    fn complete_directional_abort(&mut self, stream: Handle, direction: Direction) {
        if let Some(record) = self.streams.get_mut(&stream) {
            match direction {
                Direction::Send => {
                    record.send_abort_complete = true;
                    record.send_abort_retry = false;
                }
                Direction::Receive => {
                    record.receive_abort_complete = true;
                    record.receive_abort_retry = false;
                }
            }
        }
    }

    fn defer_directional_abort(&mut self, stream: Handle, direction: Direction) {
        if let Some(record) = self.streams.get_mut(&stream) {
            match direction {
                Direction::Send if !record.send_abort_complete => record.send_abort_retry = true,
                Direction::Receive if !record.receive_abort_complete => {
                    record.receive_abort_retry = true;
                }
                Direction::Send | Direction::Receive => {}
            }
        }
    }

    fn defer_stream_abort(&mut self, stream: Handle) {
        self.defer_directional_abort(stream, Direction::Send);
        self.defer_directional_abort(stream, Direction::Receive);
        self.queue_close_retry(stream, ObjectKind::Stream, DROP_ABORT_CODE);
    }

    fn retry_directional_aborts(&mut self) {
        let send = self
            .streams
            .iter()
            .filter_map(|(handle, record)| {
                (record.send_abort_retry && record.pending_send_count == 0).then_some(*handle)
            })
            .collect::<Vec<_>>();
        for handle in send {
            if self.raw.abort_send(handle, DROP_ABORT_CODE).is_ok()
                && let Some(record) = self.streams.get_mut(&handle)
            {
                record.send_abort_complete = true;
                record.send_abort_retry = false;
            }
        }
        let receive = self
            .streams
            .iter()
            .filter_map(|(handle, record)| record.receive_abort_retry.then_some(*handle))
            .collect::<Vec<_>>();
        for handle in receive {
            if self.raw.abort_receive(handle, DROP_ABORT_CODE).is_ok() {
                if let Some(record) = self.streams.get_mut(&handle) {
                    record.receive_abort_complete = true;
                    record.receive_abort_retry = false;
                }
                self.settle_pending_receive_abort(handle);
            }
        }
    }

    fn retry_object_closes(&mut self) {
        let count = self.close_retries.len();
        for _ in 0..count {
            let Some((handle, kind, code)) = self.close_retries.pop_front() else {
                break;
            };
            if self.start_close(handle, kind, code).is_err()
                && match kind {
                    ObjectKind::Listener => self.listeners.contains_key(&handle),
                    ObjectKind::Connection => self.connections.contains_key(&handle),
                    ObjectKind::Stream => self.streams.contains_key(&handle),
                }
            {
                self.close_retries.push_back((handle, kind, code));
            }
        }
    }

    fn respond_admission(&mut self, id: u64, status: u16) -> Result<()> {
        let event = self
            .admissions
            .remove(&id)
            .ok_or_else(|| NativeError::message("admission is no longer live"))?;
        let result = self.raw.respond_admission(&event, status);
        self.raw.release_event(event);
        result
    }

    fn drain_events(&mut self) -> Result<bool> {
        self.events_empty = false;
        for _ in 0..EVENT_BATCH {
            let Some(event) = self.raw.next_event()? else {
                self.events_empty = true;
                return Ok(true);
            };
            let info = match self.raw.event_info(&event) {
                Ok(info) => info,
                Err(error) => {
                    self.raw.release_event(event);
                    return Err(error);
                }
            };
            if let Err(error) = self.validate_event(info) {
                self.raw.release_event(event);
                return Err(error);
            }
            self.handle_event(event, info);
            if info.kind == EventKind::Unknown {
                return Err(NativeError::message(
                    "native transport returned an unknown event kind",
                ));
            }
            if info.fatal() && info.kind != EventKind::Stopped {
                return Err(if info.status == 0 {
                    NativeError::message("native transport reported a fatal event")
                } else {
                    info.error()
                });
            }
        }
        Ok(false)
    }

    fn validate_event(&mut self, info: EventInfo) -> Result<()> {
        if info.sequence == 0 || info.sequence <= self.last_event_sequence {
            return Err(NativeError::message(
                "native transport event sequence is not strictly increasing",
            ));
        }
        self.last_event_sequence = info.sequence;
        if !info.subject_kind_is_valid() {
            return Err(NativeError::message(
                "native transport event subject kind does not match its event kind",
            ));
        }
        if info.has_invalid_transport_error() {
            return Err(NativeError::message(
                "native transport event reported a transport error with zero status",
            ));
        }
        if info.has_conflicting_origin() {
            return Err(NativeError::message(
                "native transport event has conflicting local and peer origins",
            ));
        }
        Ok(())
    }

    fn handle_event(&mut self, event: RawEvent, info: EventInfo) {
        match info.kind {
            EventKind::ConnectionReady => self.connection_ready(event, info),
            EventKind::ConnectionFailed => self.connection_failed(event, info),
            EventKind::ConnectionClosed => {
                self.raw.release_event(event);
                self.finish_connection(info.subject, info.close_reason());
            }
            EventKind::StreamReady => self.stream_ready(event, info),
            EventKind::StreamFailed => self.stream_failed(event, info),
            EventKind::StreamReadable => {
                self.raw.release_event(event);
                self.stream_readable(info.subject);
            }
            EventKind::ReceiveFin => {
                self.raw.release_event(event);
                self.receive_fin(info);
            }
            EventKind::SendComplete => self.send_complete(event, info),
            EventKind::SendStopped => {
                self.raw.release_event(event);
                self.send_stopped(info);
            }
            EventKind::StreamClosed => {
                self.raw.release_event(event);
                self.finish_stream(info.subject, info.close_reason());
            }
            EventKind::ListenerStopped => {
                self.raw.release_event(event);
                self.finish_listener(info.subject, info.close_reason());
            }
            EventKind::Http3Admission | EventKind::WebTransportAdmission => {
                self.admission(event, info);
            }
            EventKind::Stopped => {
                self.raw.release_event(event);
                self.stopped_seen = true;
                let failure = (info.fatal() || info.status != 0).then(|| {
                    if info.status == 0 {
                        NativeError::message("native transport reported a fatal STOPPED event")
                    } else {
                        info.error()
                    }
                });
                self.request_shutdown(failure);
                self.early_send_stops.clear();
                self.early_receive_ends.clear();
                self.force_finish_objects(info.close_reason());
            }
            EventKind::Diagnostic | EventKind::Unknown => self.raw.release_event(event),
        }
    }

    fn connection_ready(&mut self, event: RawEvent, info: EventInfo) {
        self.raw.release_event(event);
        if info.operation_id != 0 {
            let Some(mut pending) = self.pending_dials.remove(&info.operation_id) else {
                self.track_orphan_connection(info.subject);
                self.fail_transport(NativeError::message(
                    "unexpected connection-ready operation",
                ));
                return;
            };
            if pending.expected != info.subject {
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(NativeError::message(
                        "connection-ready handle did not match admitted dial",
                    )));
                }
                self.track_orphan_connection(info.subject);
                self.fail_transport(NativeError::message("connection-ready handle mismatch"));
            } else if let Some(protocol) = info.protocol {
                if let Some(cell) = &pending.connection.state.protocol {
                    let _ = cell.set(protocol);
                }
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Ok(pending.connection));
                }
            } else {
                let error = NativeError::message("connection-ready event omitted its protocol");
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(error.clone()));
                }
                self.fail_transport(error);
            }
            return;
        }
        let Some((parent, delivery, delivery_rejections)) =
            self.listeners.get(&info.parent).and_then(|record| {
                record.state.upgrade().map(|state| {
                    (
                        state,
                        record.delivery.clone(),
                        record.delivery_rejections.clone(),
                    )
                })
            })
        else {
            self.track_orphan_connection(info.subject);
            return;
        };
        drop(parent);
        let connection = self.make_connection(info.subject);
        let Some(protocol) = info.protocol else {
            self.fail_transport(NativeError::message(
                "incoming connection-ready event omitted its protocol",
            ));
            return;
        };
        if let Some(cell) = &connection.state.protocol {
            let _ = cell.set(protocol);
        }
        if let Err(error) = delivery.try_send(ListenerEvent::Connection(connection)) {
            record_delivery_rejection(&delivery_rejections);
            drop(error.into_inner());
        }
    }

    fn connection_failed(&mut self, event: RawEvent, info: EventInfo) {
        self.raw.release_event(event);
        if is_remote_setup_failure(info.operation_id, info.peer(), info.server()) {
            self.finish_connection(info.subject, info.close_reason());
            return;
        }
        let invariant_error = match self.pending_dials.remove(&info.operation_id) {
            Some(mut pending) if pending.expected == info.subject => {
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(info.error()));
                }
                None
            }
            Some(mut pending) => {
                let error =
                    NativeError::message("connection-failed handle did not match admitted dial");
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(error.clone()));
                }
                Some(error)
            }
            None => Some(NativeError::message(
                "connection-failed event did not match a pending dial operation",
            )),
        };
        if let Some(error) = invariant_error {
            self.fail_transport(error);
        }
        self.finish_connection(info.subject, info.close_reason());
    }

    fn stream_ready(&mut self, event: RawEvent, info: EventInfo) {
        self.raw.release_event(event);
        if info.operation_id != 0 {
            let Some(mut pending) = self.pending_opens.remove(&info.operation_id) else {
                self.track_orphan_stream(info.subject);
                self.fail_transport(NativeError::message("unexpected stream-ready operation"));
                return;
            };
            if pending.expected != info.subject || pending.parent != info.parent {
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(NativeError::message(
                        "stream-ready handles did not match admitted open",
                    )));
                }
                self.track_orphan_stream(info.subject);
                self.fail_transport(NativeError::message("stream-ready handle mismatch"));
            } else if let Some(reply) = pending.reply.take() {
                let _ = reply.send(Ok(pending.stream));
            }
            return;
        }
        let Some((parent, delivery, delivery_rejections)) =
            self.connections.get(&info.parent).and_then(|record| {
                record.state.upgrade().map(|state| {
                    (
                        state,
                        record.delivery.clone(),
                        record.delivery_rejections.clone(),
                    )
                })
            })
        else {
            self.track_orphan_stream(info.subject);
            return;
        };
        let stream = self.make_stream(info.subject, parent.protocol.clone());
        if let Err(error) = delivery.try_send(stream) {
            record_delivery_rejection(&delivery_rejections);
            drop(error.into_inner());
        }
    }

    fn stream_failed(&mut self, event: RawEvent, info: EventInfo) {
        self.raw.release_event(event);
        if is_remote_setup_failure(info.operation_id, info.peer(), info.server()) {
            self.finish_stream(info.subject, info.close_reason());
            return;
        }
        let invariant_error = match self.pending_opens.remove(&info.operation_id) {
            Some(mut pending)
                if pending.expected == info.subject && pending.parent == info.parent =>
            {
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(info.error()));
                }
                None
            }
            Some(mut pending) => {
                let error =
                    NativeError::message("stream-failed handles did not match admitted open");
                if let Some(reply) = pending.reply.take() {
                    let _ = reply.send(Err(error.clone()));
                }
                Some(error)
            }
            None => Some(NativeError::message(
                "stream-failed event did not match a pending open operation",
            )),
        };
        if let Some(error) = invariant_error {
            self.fail_transport(error);
        }
        self.finish_stream(info.subject, info.close_reason());
    }

    fn send_complete(&mut self, event: RawEvent, info: EventInfo) {
        self.raw.release_event(event);
        let Some(mut pending) = self.pending_sends.remove(&info.operation_id) else {
            self.fail_transport(NativeError::message("unexpected send completion"));
            return;
        };
        if let Some(record) = self.streams.get_mut(&pending.expected) {
            record.pending_send_count = record.pending_send_count.saturating_sub(1);
        }
        if pending.expected != info.subject {
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(Err(NativeError::message(
                    "send-complete handle did not match admitted send",
                )));
            }
            self.fail_transport(NativeError::message("send-complete handle mismatch"));
            return;
        }
        let result = if info.status == 0 {
            Ok(())
        } else {
            Err(info.error())
        };
        if pending
            .reply
            .take()
            .is_some_and(|reply| reply.send(result).is_err())
        {
            self.defer_directional_abort(info.subject, Direction::Send);
        }
    }

    fn stream_readable(&mut self, handle: Handle) {
        let Some(record) = self.streams.get_mut(&handle) else {
            return;
        };
        let Some(reply) = record.pending_receive.take() else {
            return;
        };
        if reply.is_closed() {
            return;
        }
        match self.raw.receive(handle) {
            Ok(body) => deliver_receive(
                &mut record.queued_receives,
                reply,
                Ok(Some(OwnedReceive(body))),
            ),
            Err(error) if error.is_would_block() => record.pending_receive = Some(reply),
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn send_stopped(&mut self, info: EventInfo) {
        let reason = info.send_stop_reason();
        let Some(record) = self.streams.get_mut(&info.subject) else {
            self.early_send_stops.insert(info.subject, reason);
            return;
        };
        record.send_stopped.send_replace(Some(reason));
    }

    fn receive_fin(&mut self, info: EventInfo) {
        let end = if info.clean_fin() {
            Ok(())
        } else {
            Err(info.error().terminal())
        };
        let Some(record) = self.streams.get_mut(&info.subject) else {
            self.early_receive_ends.insert(info.subject, end);
            return;
        };
        let end = record.receive_end.get_or_insert(end).clone();
        record.receive_ended.send_replace(Some(end.clone()));
        let Some(reply) = record.pending_receive.take() else {
            return;
        };
        if reply.is_closed() {
            return;
        }
        match self.raw.receive(info.subject) {
            Ok(body) => deliver_receive(
                &mut record.queued_receives,
                reply,
                Ok(Some(OwnedReceive(body))),
            ),
            Err(error) if error.is_would_block() => {
                let _ = reply.send(end.map(|()| None));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    fn allocate_admission_id(&mut self) -> u64 {
        next_unused_id(&mut self.next_admission, &self.admissions)
    }

    fn admission(&mut self, event: RawEvent, info: EventInfo) {
        if self.shutdown_requested || self.stopped_seen {
            let _ = self.raw.respond_admission(&event, 503);
            self.raw.release_event(event);
            return;
        }
        let Ok(admission) = self.raw.admission_owned(&event) else {
            let _ = self.raw.respond_admission(&event, 500);
            self.raw.release_event(event);
            return;
        };
        let Some((delivery, delivery_rejections)) = self
            .listeners
            .get(&admission.listener)
            .map(|record| (record.delivery.clone(), record.delivery_rejections.clone()))
        else {
            let _ = self.raw.respond_admission(&event, 503);
            self.raw.release_event(event);
            return;
        };
        let id = self.allocate_admission_id();
        let wrapper = Admission {
            inner: Arc::new(AdmissionInner {
                shared: self.shared.clone(),
                id,
                request: AdmissionRequest {
                    protocol: admission.protocol,
                    headers: admission.headers,
                    method: admission.method,
                    path: admission.path,
                    authority: admission.authority,
                    origin: admission.origin,
                },
                state: AtomicU8::new(ADMISSION_PENDING),
            }),
        };
        self.admissions.insert(id, event);
        let event = if info.kind == EventKind::Http3Admission {
            ListenerEvent::Http3Admission(wrapper)
        } else {
            ListenerEvent::WebTransportAdmission(wrapper)
        };
        if let Err(error) = delivery.try_send(event) {
            record_delivery_rejection(&delivery_rejections);
            drop(error.into_inner());
        }
    }

    fn retire_handle(&mut self, handle: Handle, kind: ObjectKind) {
        if !self
            .retirements
            .iter()
            .any(|pending| *pending == (handle, kind))
        {
            self.retirements.push_back((handle, kind));
        }
    }

    fn finish_listener(&mut self, handle: Handle, reason: CloseReason) {
        if let Some(record) = self.listeners.remove(&handle) {
            record.terminal.send_replace(Some(reason));
        }
        self.retire_handle(handle, ObjectKind::Listener);
    }

    fn finish_connection(&mut self, handle: Handle, reason: CloseReason) {
        if let Some(record) = self.connections.remove(&handle) {
            record.terminal.send_replace(Some(reason));
        }
        self.retire_handle(handle, ObjectKind::Connection);
    }

    fn finish_stream(&mut self, handle: Handle, reason: CloseReason) {
        let Some(mut record) = self.streams.remove(&handle) else {
            self.retire_handle(handle, ObjectKind::Stream);
            return;
        };
        loop {
            match self.raw.receive(handle) {
                Ok(body) => record.queued_receives.push_back(body),
                Err(error) if error.is_would_block() => break,
                Err(error) => {
                    record.receive_end.get_or_insert(Err(error.terminal()));
                    break;
                }
            }
        }
        let end = record.receive_end.take().unwrap_or_else(|| {
            Err(
                NativeError::message("native stream closed before the receive direction ended")
                    .terminal(),
            )
        });
        if record.send_stopped.borrow().is_none() {
            record.send_stopped.send_replace(Some(reason));
        }
        if record.receive_ended.borrow().is_none() {
            record.receive_ended.send_replace(Some(end.clone()));
        }
        if let Some(reply) = record.pending_receive.take() {
            if reply.is_closed() {
                drop(reply);
            } else if let Some(body) = record.queued_receives.pop_front() {
                deliver_receive(
                    &mut record.queued_receives,
                    reply,
                    Ok(Some(OwnedReceive(body))),
                );
            } else {
                let _ = reply.send(end.clone().map(|()| None));
            }
        }
        if !record.object_dropped {
            *record
                .receive_cache
                .lock()
                .expect("stream receive cache lock poisoned") = Some(RetiredReceive {
                queued: record.queued_receives,
                end,
            });
            self.retired_receives
                .insert(handle, record.receive_cache.clone());
        }
        record.terminal.send_replace(Some(reason));
        self.retire_handle(handle, ObjectKind::Stream);
    }

    fn force_finish_objects(&mut self, reason: CloseReason) {
        for handle in self.streams.keys().copied().collect::<Vec<_>>() {
            self.finish_stream(handle, reason);
        }
        for handle in self.connections.keys().copied().collect::<Vec<_>>() {
            self.finish_connection(handle, reason);
        }
        for handle in self.listeners.keys().copied().collect::<Vec<_>>() {
            self.finish_listener(handle, reason);
        }
        let error = NativeError::closed(reason);
        self.notify_pending(&error);
        self.pending_dials.clear();
        self.pending_opens.clear();
        self.pending_sends.clear();
    }

    fn notify_pending(&mut self, error: &NativeError) {
        for pending in self.pending_dials.values_mut() {
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(Err(error.clone()));
            }
        }
        for pending in self.pending_opens.values_mut() {
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(Err(error.clone()));
            }
        }
        for pending in self.pending_sends.values_mut() {
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }

    fn request_shutdown(&mut self, failure: Option<NativeError>) {
        self.shared.mark_shutdown_started();
        if let Some(error) = failure {
            self.notify_pending(&error);
            self.failure.get_or_insert(error);
        }
        if !self.shutdown_requested {
            self.shutdown_requested = true;
            for id in self.admissions.keys().copied().collect::<Vec<_>>() {
                let _ = self.respond_admission(id, 500);
            }
        }
        self.retry_transport_close();
    }

    fn begin_shutdown(&mut self) {
        self.request_shutdown(None);
    }

    fn fail_transport(&mut self, error: NativeError) {
        self.request_shutdown(Some(error));
    }

    fn fail_cleanup(&mut self, error: NativeError) {
        self.shared.mark_shutdown_started();
        self.notify_pending(&error);
        let reported = self.failure.get_or_insert(error).clone();
        self.transport_terminal.send_replace(Some(Err(reported)));
        self.cleanup_failed = true;
    }

    fn retry_transport_close(&mut self) {
        if !self.shutdown_requested || self.transport_close_admitted || self.cleanup_failed {
            return;
        }
        match self.raw.close() {
            Ok(()) => self.transport_close_admitted = true,
            Err(error) if error.is_retryable_release() => {}
            Err(error) => self.fail_cleanup(error),
        }
    }

    fn retry_retirements(&mut self) {
        let count = self.retirements.len();
        for _ in 0..count {
            let Some((handle, kind)) = self.retirements.pop_front() else {
                break;
            };
            match self.raw.release_handle(handle, kind) {
                Ok(()) => {}
                Err(error) if error.is_retryable_release() => {
                    self.retirements.push_back((handle, kind));
                }
                Err(error) => self.fail_cleanup(error),
            }
        }
    }

    fn ready_for_release(&mut self) -> bool {
        if !self.shutdown_requested || !self.stopped_seen || !self.events_empty {
            return false;
        }
        if !self.listeners.is_empty() || !self.connections.is_empty() || !self.streams.is_empty() {
            self.force_finish_objects(CloseReason::Local { code: 0 });
        }
        self.retry_retirements();
        if self.cleanup_failed {
            return false;
        }
        if !self.retirements.is_empty() || !self.admissions.is_empty() {
            return false;
        }
        match self.raw.drain() {
            Ok(()) => true,
            Err(error) if error.is_retryable_release() => false,
            Err(error) => {
                self.fail_cleanup(error);
                false
            }
        }
    }
}

fn clear_closed_reply<T>(pending: &mut Option<Reply<T>>) {
    if pending.as_ref().is_some_and(oneshot::Sender::is_closed) {
        *pending = None;
    }
}

fn settle_receive_abort(
    receive_end: &mut Option<Result<()>>,
    pending: &mut Option<Reply<Option<OwnedReceive>>>,
) {
    let error = NativeError::message("native receive direction aborted").terminal();
    receive_end.get_or_insert_with(|| Err(error.clone()));
    if let Some(reply) = pending.take() {
        let _ = reply.send(Err(error));
    }
}

const fn is_remote_setup_failure(operation_id: u64, peer: bool, server: bool) -> bool {
    operation_id == 0 && (peer || server)
}

fn next_unused_id<T>(next: &mut u64, used: &HashMap<u64, T>) -> u64 {
    loop {
        let id = *next;
        *next = next.wrapping_add(1);
        if id != 0 && !used.contains_key(&id) {
            return id;
        }
    }
}

fn deliver_receive(
    queue: &mut VecDeque<Vec<u8>>,
    reply: Reply<Option<OwnedReceive>>,
    result: Result<Option<OwnedReceive>>,
) {
    if let Err(Ok(Some(body))) = reply.send(result) {
        queue.push_front(body.into_bytes());
    }
}

fn map_diagnostics(value: ffi::RawDiagnostics) -> Diagnostics {
    let state = match value.state {
        0 => TransportState::Running,
        1 => TransportState::Stopping,
        2 => TransportState::Stopped,
        value => TransportState::Unknown(value),
    };
    Diagnostics {
        state,
        terminal_status: value.terminal_status,
        queue_depth: value.queue_depth,
        ordinary_queue_depth: value.ordinary_queue_depth,
        live_listeners: value.live_listeners,
        live_connections: value.live_connections,
        live_streams: value.live_streams,
        pending_send_count: value.pending_send_count,
        pending_send_bytes: value.pending_send_bytes,
        receive_owned_count: value.receive_owned_count,
        receive_owned_bytes: value.receive_owned_bytes,
        events_enqueued: value.events_enqueued,
        events_dequeued: value.events_dequeued,
        events_rejected: value.events_rejected,
        provider_error_code: value.provider_error_code,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use tokio::sync::{Mutex, mpsc, oneshot, watch};

    use super::{
        Command, ObjectKind, ObjectState, ReceiveDirectionState, RetiredReceive,
        SendDirectionState, Shared, StreamDirections, abort_half_guarded, clear_closed_reply,
        command_queue_is_drained, is_remote_setup_failure, next_unused_id, receive_body_guarded,
        receive_result_is_terminal, send_body, settle_receive_abort, take_cached_receive,
    };
    use crate::ffi::Handle;

    fn shared() -> (
        Shared,
        mpsc::Receiver<Command>,
        mpsc::UnboundedReceiver<super::Cleanup>,
    ) {
        let (commands, command_rx) = mpsc::channel(1);
        let (cleanup, cleanup_rx) = mpsc::unbounded_channel();
        (
            Shared {
                commands,
                cleanup,
                operations: Arc::new(AtomicU64::new(1)),
                active_operations: Arc::new(StdMutex::new(std::collections::HashSet::new())),
                accepting_commands: Arc::new(AtomicBool::new(true)),
                command_reservations: Arc::new(AtomicU64::new(0)),
                shutdown_started: watch::channel(false).0,
            },
            command_rx,
            cleanup_rx,
        )
    }

    fn stream_state(shared: Shared) -> Arc<ObjectState> {
        let (_, terminal) = watch::channel(None);
        let (_, send_stopped) = watch::channel(None);
        let (_, receive_ended) = watch::channel(None);
        Arc::new(ObjectState {
            shared,
            handle: Handle::default(),
            kind: ObjectKind::Stream,
            terminal,
            send_stopped: Some(send_stopped),
            receive_ended: Some(receive_ended),
            protocol: None,
            receive_cache: Some(Arc::new(StdMutex::new(None))),
        })
    }

    #[test]
    fn operation_ids_skip_zero_after_wrap() {
        let (mut shared, _commands, _cleanup) = shared();
        let operations = Arc::new(AtomicU64::new(u64::MAX));
        shared.operations = operations.clone();
        let maximum = shared.next_operation();
        let one = shared.next_operation();
        assert_eq!(maximum.get(), u64::MAX);
        assert_eq!(one.get(), 1);
        assert_eq!(operations.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn operation_ids_skip_reserved_value_after_wrap() {
        let (mut shared, _commands, _cleanup) = shared();
        shared.operations = Arc::new(AtomicU64::new(u64::MAX));
        shared
            .active_operations
            .lock()
            .expect("operation ID registry lock poisoned")
            .insert(1);
        let maximum = shared.next_operation();
        let two = shared.next_operation();
        assert_eq!(maximum.get(), u64::MAX);
        assert_eq!(two.get(), 2);
    }

    #[test]
    fn admission_ids_skip_zero_and_reserved_values_after_wrap() {
        let mut next = u64::MAX;
        let used = HashMap::from([(1, true)]);
        assert_eq!(next_unused_id(&mut next, &used), u64::MAX);
        assert_eq!(next_unused_id(&mut next, &used), 2);
        assert_eq!(next, 3);
    }

    #[test]
    fn zero_operation_id_is_reserved_only_for_remote_setup() {
        assert!(is_remote_setup_failure(0, true, false));
        assert!(is_remote_setup_failure(0, false, true));
        assert!(!is_remote_setup_failure(0, false, false));
        assert!(!is_remote_setup_failure(1, true, true));
    }

    #[tokio::test]
    async fn command_gate_rejects_work_after_shutdown() {
        let (shared, _commands, _cleanup) = shared();
        shared.begin_shutdown();
        let (reply, wait) = oneshot::channel();
        assert!(shared.send(Command::Diagnostics { reply }).await.is_err());
        assert!(wait.await.is_err(), "rejected command must drop its reply");
    }

    #[tokio::test]
    async fn shutdown_barrier_waits_for_reserved_command_permit() {
        let (shared, mut commands, _cleanup) = shared();
        let (first_reply, first_wait) = oneshot::channel();
        shared
            .send(Command::Diagnostics { reply: first_reply })
            .await
            .expect("fill bounded command queue");

        let (reply, wait) = oneshot::channel();
        let pending_shared = shared.clone();
        let pending =
            tokio::spawn(async move { pending_shared.send(Command::Diagnostics { reply }).await });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while shared.command_reservations.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("command reservation did not become observable");
        assert!(!command_queue_is_drained(&shared, true));

        shared.begin_shutdown();
        assert!(commands.recv().await.is_some(), "queued command");
        assert!(pending.await.expect("pending send task").is_err());
        assert!(wait.await.is_err(), "dropped command must cancel its reply");
        assert!(
            first_wait.await.is_err(),
            "consumed command must drop its reply"
        );
        assert!(command_queue_is_drained(&shared, true));
    }

    #[tokio::test]
    async fn concurrent_receive_is_rejected() {
        let (shared, _commands, _cleanup) = shared();
        let state = stream_state(shared);
        let directions = Arc::new(StreamDirections {
            send: Mutex::new(SendDirectionState::Open),
            receive: Mutex::new(ReceiveDirectionState::Open),
            closing: AtomicBool::new(false),
        });
        let _active_receive = directions.receive.lock().await;
        let error = receive_body_guarded(&state, &directions)
            .await
            .expect_err("second receive must be rejected");
        assert!(error.to_string().contains("already pending"));
    }

    #[tokio::test]
    async fn send_after_terminal_direction_is_rejected_locally() {
        let (shared, _commands, _cleanup) = shared();
        let state = stream_state(shared);
        let directions = Arc::new(StreamDirections {
            send: Mutex::new(SendDirectionState::Finished),
            receive: Mutex::new(ReceiveDirectionState::Open),
            closing: AtomicBool::new(false),
        });
        let error = send_body(&state, &directions, b"late")
            .await
            .expect_err("send after finish must fail");
        assert!(error.to_string().contains("not open"));
    }

    #[tokio::test]
    async fn synchronous_send_error_keeps_direction_open() {
        let (shared, mut commands, mut cleanup) = shared();
        let state = stream_state(shared);
        let directions = Arc::new(StreamDirections {
            send: Mutex::new(SendDirectionState::Open),
            receive: Mutex::new(ReceiveDirectionState::Open),
            closing: AtomicBool::new(false),
        });
        let send_state = state.clone();
        let send_directions = directions.clone();
        let task =
            tokio::spawn(async move { send_body(&send_state, &send_directions, b"body").await });
        let command = commands.recv().await.expect("send command");
        let Command::Send { reply, .. } = command else {
            panic!("expected send command");
        };
        let _ = reply.send(Err(crate::NativeError::message("send was not admitted")));
        assert!(task.await.expect("send task").is_err());
        assert!(*directions.send.lock().await == SendDirectionState::Open);
        assert!(cleanup.try_recv().is_err());
    }

    #[tokio::test]
    async fn synchronous_receive_abort_error_keeps_direction_open() {
        let (shared, mut commands, mut cleanup) = shared();
        let state = stream_state(shared);
        let directions = Arc::new(StreamDirections {
            send: Mutex::new(SendDirectionState::Open),
            receive: Mutex::new(ReceiveDirectionState::Open),
            closing: AtomicBool::new(false),
        });
        let abort_state = state.clone();
        let abort_directions = directions.clone();
        let task = tokio::spawn(async move {
            abort_half_guarded(
                &abort_state,
                &abort_directions,
                super::Direction::Receive,
                9,
            )
            .await
        });
        let command = commands.recv().await.expect("receive-abort command");
        let Command::AbortHalf { reply, .. } = command else {
            panic!("expected receive-abort command");
        };
        let _ = reply.send(Err(crate::NativeError::message("abort was not admitted")));
        assert!(task.await.expect("receive-abort task").is_err());
        assert!(*directions.receive.lock().await == ReceiveDirectionState::Open);
        assert!(cleanup.try_recv().is_err());
    }

    #[tokio::test]
    async fn canceled_send_aborts_direction_and_schedules_cleanup() {
        let (shared, mut commands, mut cleanup) = shared();
        let state = stream_state(shared);
        let directions = Arc::new(StreamDirections {
            send: Mutex::new(SendDirectionState::Open),
            receive: Mutex::new(ReceiveDirectionState::Open),
            closing: AtomicBool::new(false),
        });
        let send_state = state.clone();
        let send_directions = directions.clone();
        let task =
            tokio::spawn(async move { send_body(&send_state, &send_directions, b"body").await });
        let command = commands.recv().await.expect("send command");
        task.abort();
        let _ = task.await;
        assert!(*directions.send.lock().await == SendDirectionState::Aborted);
        assert!(matches!(
            cleanup.recv().await,
            Some(super::Cleanup::StreamHalfDropped {
                direction: super::Direction::Send,
                abort: true,
                ..
            })
        ));
        drop(command);
    }

    #[test]
    fn transient_receive_error_does_not_end_direction() {
        let transient = Err(crate::NativeError::message("temporary receive failure"));
        let terminal = Err(crate::NativeError::message("stream ended").terminal());
        assert!(!receive_result_is_terminal(&transient));
        assert!(receive_result_is_terminal(&terminal));
    }

    #[test]
    fn closed_pending_receive_is_cleared() {
        let (reply, wait) = oneshot::channel::<crate::Result<Option<super::OwnedReceive>>>();
        drop(wait);
        let mut pending = Some(reply);
        clear_closed_reply(&mut pending);
        assert!(pending.is_none());
    }

    #[tokio::test]
    async fn receive_abort_settles_pending_receive() {
        let (reply, wait) = oneshot::channel();
        let mut pending = Some(reply);
        let mut end = None;
        settle_receive_abort(&mut end, &mut pending);
        assert!(pending.is_none());
        let error = wait
            .await
            .expect("receive abort reply")
            .expect_err("receive must fail after abort");
        assert!(error.is_terminal());
        assert!(end.as_ref().is_some_and(std::result::Result::is_err));
    }

    fn admission(shared: Shared, id: u64) -> super::Admission {
        super::Admission {
            inner: Arc::new(super::AdmissionInner {
                shared,
                id,
                request: super::AdmissionRequest {
                    protocol: crate::Protocol::Http3,
                    headers: Vec::new(),
                    method: Vec::new(),
                    path: Vec::new(),
                    authority: Vec::new(),
                    origin: Vec::new(),
                },
                state: AtomicU8::new(super::ADMISSION_PENDING),
            }),
        }
    }

    #[tokio::test]
    async fn invalid_admission_status_does_not_consume_decision() {
        let (shared, _commands, mut cleanup) = shared();
        let admission = admission(shared, 7);
        assert!(admission.reject(399).await.is_err());
        assert_eq!(
            admission.inner.state.load(Ordering::Acquire),
            super::ADMISSION_PENDING
        );
        drop(admission);
        assert!(matches!(
            cleanup.recv().await,
            Some(super::Cleanup::FailAdmission(7))
        ));
    }

    #[tokio::test]
    async fn failed_admission_response_consumes_decision() {
        let (shared, _commands, mut cleanup) = shared();
        let admission = admission(shared, 8);
        let responder = admission.clone();
        let task = tokio::spawn(async move { responder.accept().await });
        let Some(super::Cleanup::RespondAdmission { reply, .. }) = cleanup.recv().await else {
            panic!("expected admission response");
        };
        reply
            .expect("response waiter")
            .send(Err(crate::NativeError::message("response delivery failed")))
            .expect("response receiver");
        assert!(task.await.expect("admission task").is_err());
        assert_eq!(
            admission.inner.state.load(Ordering::Acquire),
            super::ADMISSION_ANSWERED
        );
        drop(admission);
        assert!(cleanup.try_recv().is_err());
    }

    #[tokio::test]
    async fn canceled_admission_response_stays_answered() {
        let (shared, _commands, mut cleanup) = shared();
        let admission = admission(shared, 9);
        let responder = admission.clone();
        let task = tokio::spawn(async move { responder.accept().await });
        let response = cleanup.recv().await.expect("admission response");
        task.abort();
        let _ = task.await;
        assert_eq!(
            admission.inner.state.load(Ordering::Acquire),
            super::ADMISSION_ANSWERED
        );
        drop(response);
        drop(admission);
        assert!(cleanup.try_recv().is_err());
    }

    #[tokio::test]
    async fn delivery_rejection_is_reported_before_later_items() {
        let (items, receiver) = mpsc::channel(1);
        let (rejections, rejection_rx) = watch::channel(0);
        let mut receiver = super::DeliveryReceiver {
            items: receiver,
            rejections: rejection_rx,
            reported_rejections: 0,
        };
        super::record_delivery_rejection(&rejections);
        items.send(3_u8).await.expect("delivery item");
        let error = receiver
            .receive("test")
            .await
            .expect_err("rejection should be observable");
        assert!(error.to_string().contains("rejected 1 incoming item"));
        assert_eq!(receiver.receive("test").await.expect("item"), Some(3));
    }

    #[test]
    fn terminal_receive_cache_preserves_data_before_fin() {
        let cache = Arc::new(StdMutex::new(Some(RetiredReceive {
            queued: VecDeque::from([b"one".to_vec(), b"two".to_vec()]),
            end: Ok(()),
        })));
        assert_eq!(
            take_cached_receive(&cache)
                .expect("terminal cache")
                .expect("first body")
                .expect("body")
                .as_bytes(),
            b"one"
        );
        assert_eq!(
            take_cached_receive(&cache)
                .expect("terminal cache")
                .expect("second body")
                .expect("body")
                .as_bytes(),
            b"two"
        );
        assert!(
            take_cached_receive(&cache)
                .expect("terminal cache")
                .expect("clean fin")
                .is_none()
        );
    }
}
