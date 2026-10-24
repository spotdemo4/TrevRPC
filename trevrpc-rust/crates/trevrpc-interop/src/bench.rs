//! The `bench` subcommand: a load client for the contract in `bench/README.md`.
//!
//! Workers are tasks that share one connection and keep one call or stream in flight each. They
//! run for a warmup period and then a measured window, and only operations that start and finish
//! inside the window count. The client prints one JSON line with the throughput and latency
//! percentiles.

use std::collections::BTreeMap;
use std::time::Duration;

use tokio::time::{Instant, timeout_at};
use trevrpc::Error;
use trevrpc::client::{
    BidirectionalCall, CallOptions, ChannelConfig, QmuxTransport, QuicTransport,
    StreamingRpcTransport,
};
use trevrpc::wire::MAX_METADATA_VALUE_LEN;

use crate::proto::{
    CompressedEchoClient, EchoClient, EchoRequest, EchoResponse, UnreliableEchoClient,
};
use crate::records::{Records, WORKER_SEED};
use crate::{BoxError, Flags, connect};

/// The trailing metadata key that counts the requests the server received.
const RECEIVED_METADATA_KEY: &str = "interop-received";

/// The request metadata key `--metadata-size` fills.
const PAD_METADATA_KEY: &str = "bench-pad";

/// How many distinct error messages the stderr breakdown lists.
const ERROR_BREAKDOWN_LEN: usize = 5;

/// How long past the end of the window an operation may run before it counts as failed, so a
/// hung call cannot stall the run.
const STRAGGLER_GRACE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Unary,
    ServerStream,
    ClientStream,
    Bidi,
    /// One request asks for `messages` responses, which may travel as datagrams.
    UnreliableServerStream,
    /// `messages` requests, which may travel as datagrams, ask for nothing back.
    UnreliableClientStream,
}

impl Scenario {
    fn parse(name: &str) -> Result<Self, BoxError> {
        match name {
            "unary" => Ok(Self::Unary),
            "server-stream" => Ok(Self::ServerStream),
            "client-stream" => Ok(Self::ClientStream),
            "bidi" => Ok(Self::Bidi),
            "unreliable-server-stream" => Ok(Self::UnreliableServerStream),
            "unreliable-client-stream" => Ok(Self::UnreliableClientStream),
            other => Err(format!("unknown --scenario {other:?}").into()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::ServerStream => "server-stream",
            Self::ClientStream => "client-stream",
            Self::Bidi => "bidi",
            Self::UnreliableServerStream => "unreliable-server-stream",
            Self::UnreliableClientStream => "unreliable-client-stream",
        }
    }
}

/// What `EchoRequest.message` holds.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Payload {
    /// `size` times the letter `x`, the same in every request.
    Repeat,
    /// A new `records` message for every request.
    Records,
}

impl Payload {
    fn parse(name: &str) -> Result<Self, BoxError> {
        match name {
            "repeat" => Ok(Self::Repeat),
            "records" => Ok(Self::Records),
            other => Err(format!("unknown --payload {other:?}, want repeat or records").into()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Repeat => "repeat",
            Self::Records => "records",
        }
    }
}

/// How messages are compressed; see the "Compression" section of `bench/README.md`.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CompressionMode {
    None,
    /// Each message on its own, with the `--dictionary` dictionary.
    Dictionary,
    /// Each streamed direction with one context, through `CompressedEcho`.
    Stream,
    /// Stream compression starting from the `--dictionary` dictionary.
    StreamDictionary,
}

impl CompressionMode {
    fn parse(name: &str) -> Result<Self, BoxError> {
        match name {
            "none" => Ok(Self::None),
            "dictionary" => Ok(Self::Dictionary),
            "stream" => Ok(Self::Stream),
            "stream-dictionary" => Ok(Self::StreamDictionary),
            other => Err(format!(
                "unknown --compression {other:?}, want none, dictionary, stream or stream-dictionary"
            )
            .into()),
        }
    }

    const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Dictionary => "dictionary",
            Self::Stream => "stream",
            Self::StreamDictionary => "stream-dictionary",
        }
    }

    const fn uses_dictionary(self) -> bool {
        matches!(self, Self::Dictionary | Self::StreamDictionary)
    }

    /// Whether the calls go to `CompressedEcho`, whose methods allow stream compression.
    const fn streams(self) -> bool {
        matches!(self, Self::Stream | Self::StreamDictionary)
    }

    /// Whether the mode runs `scenario`: stream compression applies to streams only, and no mode
    /// runs `server-stream`, whose responses repeat one message, or the datagram scenarios.
    const fn runs(self, scenario: Scenario) -> bool {
        match self {
            Self::None => true,
            Self::Dictionary => matches!(
                scenario,
                Scenario::Unary | Scenario::ClientStream | Scenario::Bidi
            ),
            Self::Stream | Self::StreamDictionary => {
                matches!(scenario, Scenario::ClientStream | Scenario::Bidi)
            }
        }
    }
}

/// What one operation moved.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Outcome {
    /// The application messages that were delivered.
    messages: u64,
    /// The messages that were sent but not delivered, for the unreliable scenarios.
    lost: u64,
}

impl Outcome {
    /// An operation that moved `messages` reliably.
    const fn delivered(messages: u64) -> Self {
        Self { messages, lost: 0 }
    }

    /// An operation that sent `sent` unreliable messages, `delivered` of which arrived.
    const fn unreliable(sent: u64, delivered: u64) -> Self {
        Self {
            messages: delivered,
            lost: sent.saturating_sub(delivered),
        }
    }
}

struct Params {
    scenario: Scenario,
    size: usize,
    /// Bytes of the `bench-pad` request metadata value on every call; no metadata when zero.
    metadata_size: usize,
    concurrency: usize,
    duration: Duration,
    warmup: Duration,
    messages: usize,
    payload: Payload,
    compression: CompressionMode,
}

impl Params {
    fn parse(flags: &Flags) -> Result<Self, BoxError> {
        let scenario = Scenario::parse(flags.require("scenario")?)?;
        let payload = Payload::parse(flags.get("payload").unwrap_or("repeat"))?;
        let compression = CompressionMode::parse(flags.get("compression").unwrap_or("none"))?;
        if !compression.runs(scenario) {
            return Err(format!(
                "--compression {} does not run --scenario {}",
                compression.name(),
                scenario.name()
            )
            .into());
        }
        if compression.uses_dictionary() != flags.get("dictionary").is_some() {
            return Err(
                "--dictionary is required by, and only used with, --compression dictionary and stream-dictionary"
                    .into(),
            );
        }
        let size = number(flags, "size", 64)?;
        let metadata_size = number(flags, "metadata-size", 0)?;
        if metadata_size > MAX_METADATA_VALUE_LEN {
            return Err(format!(
                "--metadata-size {metadata_size} exceeds the {MAX_METADATA_VALUE_LEN}-byte metadata value limit"
            )
            .into());
        }
        let concurrency = number(flags, "concurrency", 1)?;
        let messages = number(flags, "messages", 100)?;
        if concurrency == 0 || messages == 0 {
            return Err("--concurrency and --messages must be positive".into());
        }
        let duration = seconds(flags, "duration", 5.0)?;
        if duration.is_zero() {
            return Err("--duration must be positive".into());
        }
        Ok(Self {
            scenario,
            size,
            metadata_size,
            concurrency,
            duration,
            warmup: seconds(flags, "warmup", 1.0)?,
            messages,
            payload,
            compression,
        })
    }
}

impl Params {
    /// The options of every call: streams stay open for the whole run and the loop bounds every
    /// operation itself, so nothing times out or runs out of messages, and the calls carry the
    /// padding metadata.
    fn call_options(&self) -> CallOptions {
        let options = CallOptions::new()
            .without_timeout()
            .with_max_response_messages(None)
            .with_max_response_stream_body_size(None)
            .with_stream_idle_timeout(None);
        if self.metadata_size == 0 {
            options
        } else {
            options.with_metadata(PAD_METADATA_KEY, vec![b'x'; self.metadata_size])
        }
    }
}

fn number(flags: &Flags, name: &str, default: usize) -> Result<usize, BoxError> {
    flags.get(name).map_or(Ok(default), |value| {
        value
            .parse()
            .map_err(|_| format!("invalid --{name} {value:?}").into())
    })
}

fn seconds(flags: &Flags, name: &str, default: f64) -> Result<Duration, BoxError> {
    let value = match flags.get(name) {
        Some(value) => value
            .parse::<f64>()
            .map_err(|_| format!("invalid --{name} {value:?}"))?,
        None => default,
    };
    Duration::try_from_secs_f64(value).map_err(|_| format!("invalid --{name} {value}").into())
}

/// The transport a run measures and how to read its connection statistics.
enum Link {
    Quic {
        endpoint: quinn::Endpoint,
        transport: QuicTransport,
    },
    Qmux(QmuxTransport),
}

impl Link {
    /// Connects over `--transport` with the `--dictionary` compression, if any.
    async fn connect(flags: &Flags) -> Result<Self, BoxError> {
        let tls = connect::tls_config(flags)?;
        let server_name = connect::server_name(flags);
        let compression = connect::compression(flags)?;
        match flags.get("transport").unwrap_or("quic") {
            "quic" => {
                let addr = connect::resolve(flags, "addr").await?;
                let endpoint = connect::client_endpoint(addr, &ChannelConfig::new(), tls)?;
                let connection = endpoint.connect(addr, server_name)?.await?;
                Ok(Self::Quic {
                    endpoint,
                    transport: QuicTransport::new(connection).with_compression(&compression)?,
                })
            }
            "tcp" => {
                let addr = connect::resolve(flags, "tcp-addr").await?;
                Ok(Self::Qmux(
                    QmuxTransport::connect(addr, server_name, tls)
                        .await?
                        .with_compression(&compression)?,
                ))
            }
            other => Err(format!("unknown --transport {other:?}, want quic or tcp").into()),
        }
    }

    const fn name(&self) -> &'static str {
        match self {
            Self::Quic { .. } => "quic",
            Self::Qmux(_) => "qmux",
        }
    }

    /// Bytes the connection has sent and received so far, where the stack exposes them: the UDP
    /// payload of the QUIC connection. The `QMux` session hides its TCP stream.
    fn wire_bytes(&self) -> Option<u64> {
        match self {
            Self::Quic { transport, .. } => {
                let stats = transport.connection().stats();
                Some(stats.udp_tx.bytes.saturating_add(stats.udp_rx.bytes))
            }
            Self::Qmux(_) => None,
        }
    }

    async fn close(&self) {
        match self {
            Self::Quic {
                endpoint,
                transport,
            } => {
                transport.connection().close(0_u8.into(), b"done");
                endpoint.wait_idle().await;
            }
            Self::Qmux(transport) => transport.close(),
        }
    }
}

/// The measured window, as instants.
#[derive(Clone, Copy)]
struct Window {
    start: Instant,
    end: Instant,
}

impl Window {
    /// When an operation still running counts as hung.
    fn straggler_deadline(self) -> Instant {
        self.end.checked_add(STRAGGLER_GRACE).unwrap_or(self.end)
    }
}

#[derive(Default)]
struct WorkerResult {
    latencies: Vec<Duration>,
    /// The messages the measured operations delivered.
    messages: u64,
    /// The messages the measured operations lost.
    lost: u64,
    /// Failures by message, reported on stderr to explain a non-zero error count.
    errors: BTreeMap<String, u64>,
}

/// Where a worker's request messages come from.
enum Messages {
    /// The same message every time.
    Repeat(String),
    /// A new `records` message of `size` bytes every time.
    Records { records: Records, size: usize },
}

impl Messages {
    fn new(payload: Payload, size: usize, worker: usize) -> Self {
        match payload {
            Payload::Repeat => Self::Repeat("x".repeat(size)),
            Payload::Records => Self::Records {
                records: Records::new(
                    WORKER_SEED.wrapping_add(u64::try_from(worker).unwrap_or(u64::MAX)),
                ),
                size,
            },
        }
    }

    fn next(&mut self) -> String {
        match self {
            Self::Repeat(message) => message.clone(),
            Self::Records { records, size } => records.message(*size),
        }
    }
}

/// One load worker: its clients and, for `bidi`, the stream it keeps open.
struct Worker<T> {
    client: EchoClient<T>,
    unreliable: UnreliableEchoClient<T>,
    /// The `CompressedEcho` client, which the stream compression modes call instead of `client`.
    compressed: CompressedEchoClient<T>,
    streams: bool,
    scenario: Scenario,
    messages_source: Messages,
    options: CallOptions,
    messages: usize,
    bidi: Option<BidirectionalCall<EchoRequest, EchoResponse>>,
}

impl<T> Worker<T>
where
    T: StreamingRpcTransport + Clone,
{
    /// The next request: a new message, with nothing else set.
    fn request(&mut self) -> EchoRequest {
        EchoRequest {
            message: self.messages_source.next(),
            ..EchoRequest::default()
        }
    }

    /// Runs one operation of the scenario.
    async fn operation(&mut self) -> Result<Outcome, Error> {
        match self.scenario {
            Scenario::Unary => {
                let request = self.request();
                self.client
                    .unary(&request)
                    .options(|_| self.options.clone())
                    .await?;
                Ok(Outcome::delivered(1))
            }
            Scenario::ServerStream => self.server_stream().await,
            Scenario::ClientStream => self.client_stream().await,
            Scenario::Bidi => self.bidi().await,
            Scenario::UnreliableServerStream => self.unreliable_server_stream().await,
            Scenario::UnreliableClientStream => self.unreliable_client_stream().await,
        }
    }

    // `&mut self` because the bidi call makes `Worker` not `Sync`, and a shared reference held
    // across an await would make the spawned worker not `Send`.
    #[allow(clippy::needless_pass_by_ref_mut)]
    async fn server_stream(&mut self) -> Result<Outcome, Error> {
        use futures_util::StreamExt as _;

        let request = EchoRequest {
            count: u32::try_from(self.messages).unwrap_or(u32::MAX),
            ..self.request()
        };
        let mut responses = self
            .client
            .server_stream(&request)
            .options(|_| self.options.clone())
            .await?;
        let mut received = 0_usize;
        while let Some(response) = responses.next().await {
            response?;
            received = received.saturating_add(1);
        }
        short_stream_check(received, self.messages)?;
        Ok(Outcome::delivered(count_of(received)))
    }

    #[allow(clippy::needless_pass_by_ref_mut)] // see `server_stream`
    async fn client_stream(&mut self) -> Result<Outcome, Error> {
        let call = if self.streams {
            self.compressed.client_stream()
        } else {
            self.client.client_stream()
        }
        .options(|_| self.options.clone())
        .await?;
        for _ in 0..self.messages {
            let request = self.request();
            call.send(request).await?;
        }
        let response = call.close_and_recv().await?;
        short_stream_check(
            usize::try_from(response.index).unwrap_or(usize::MAX),
            self.messages,
        )?;
        Ok(Outcome::delivered(count_of(self.messages)))
    }

    /// One request asks for `messages` responses; the operation ends with the status, and the
    /// responses that did not arrive are lost.
    #[allow(clippy::needless_pass_by_ref_mut)] // see `server_stream`
    async fn unreliable_server_stream(&mut self) -> Result<Outcome, Error> {
        use futures_util::StreamExt as _;

        let (sender, mut responses) = self
            .unreliable
            .bidi()
            .options(|_| self.options.clone())
            .await?
            .split();
        let wanted = u32::try_from(self.messages).unwrap_or(u32::MAX);
        sender
            .send(EchoRequest {
                count: wanted,
                ..self.request()
            })
            .await?;
        sender.finish()?;
        let mut delivered = 0_u64;
        while let Some(response) = responses.next().await {
            response?;
            delivered = delivered.saturating_add(1);
        }
        Ok(Outcome::unreliable(u64::from(wanted), delivered))
    }

    /// `messages` requests ask for no responses; the server counts the ones that arrived, and
    /// the rest are lost.
    #[allow(clippy::needless_pass_by_ref_mut)] // see `server_stream`
    async fn unreliable_client_stream(&mut self) -> Result<Outcome, Error> {
        use futures_util::StreamExt as _;

        let (sender, mut responses) = self
            .unreliable
            .bidi()
            .options(|_| self.options.clone())
            .await?
            .split();
        for _ in 0..self.messages {
            let request = self.request();
            sender.send(request).await?;
        }
        sender.finish()?;
        while let Some(response) = responses.next().await {
            response?;
        }
        let received = responses
            .terminal_metadata()
            .and_then(|metadata| metadata.get(RECEIVED_METADATA_KEY))
            .and_then(|value| std::str::from_utf8(value).ok())
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| {
                Error::from(trevrpc::Status::internal(format!(
                    "the status has no {RECEIVED_METADATA_KEY} metadata"
                )))
            })?;
        Ok(Outcome::unreliable(count_of(self.messages), received))
    }

    async fn bidi(&mut self) -> Result<Outcome, Error> {
        let request = self.request();
        let call = match &mut self.bidi {
            Some(call) => call,
            empty @ None => {
                let call = if self.streams {
                    self.compressed.bidi()
                } else {
                    self.client.bidi()
                };
                empty.insert(call.options(|_| self.options.clone()).await?)
            }
        };
        let sent = call.send(request).await;
        let result = match sent {
            Ok(()) => call.recv().await.and_then(|response| {
                response.map(drop).ok_or_else(|| {
                    Error::from(trevrpc::Status::unavailable("bidi stream ended early"))
                })
            }),
            Err(error) => Err(error),
        };
        if result.is_err() {
            // A failed stream is gone; the next operation opens a fresh one.
            self.bidi = None;
        }
        result.map(|()| Outcome::delivered(1))
    }
}

/// A message count as the 64-bit counter the report uses.
fn count_of(count: usize) -> u64 {
    u64::try_from(count).unwrap_or(u64::MAX)
}

fn short_stream_check(got: usize, want: usize) -> Result<(), Error> {
    if got == want {
        Ok(())
    } else {
        Err(Error::from(trevrpc::Status::internal(format!(
            "stream carried {got} messages, want {want}"
        ))))
    }
}

/// Loops operations until the window ends, recording those that start and finish inside it.
async fn work<T>(mut worker: Worker<T>, window: Window) -> WorkerResult
where
    T: StreamingRpcTransport + Clone,
{
    let mut result = WorkerResult::default();
    loop {
        let started = Instant::now();
        if started >= window.end {
            return result;
        }
        let outcome = timeout_at(window.straggler_deadline(), worker.operation()).await;
        let finished = Instant::now();
        let measured = started >= window.start;
        match outcome {
            Ok(Ok(moved)) => {
                if measured && finished <= window.end {
                    result.latencies.push(finished.duration_since(started));
                    result.messages = result.messages.saturating_add(moved.messages);
                    result.lost = result.lost.saturating_add(moved.lost);
                }
            }
            failed => {
                if measured {
                    let message = match &failed {
                        Ok(Err(error)) => error.to_string(),
                        _ => "operation timed out".to_owned(),
                    };
                    let count = result.errors.entry(message).or_default();
                    *count = count.saturating_add(1);
                }
                worker.bidi = None;
                // Do not spin on a dead connection.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
    }
}

/// Runs the workers and the wire-byte sampling over `transport`.
async fn measure<T>(
    transport: T,
    params: &Params,
    link: &Link,
) -> Result<(Vec<WorkerResult>, Option<u64>), BoxError>
where
    T: StreamingRpcTransport + Clone,
{
    let start = Instant::now()
        .checked_add(params.warmup)
        .ok_or("--warmup is too long")?;
    let window = Window {
        start,
        end: start
            .checked_add(params.duration)
            .ok_or("--duration is too long")?,
    };
    let workers: Vec<_> = (0..params.concurrency)
        .map(|index| {
            tokio::spawn(work(
                Worker {
                    client: EchoClient::new(transport.clone()),
                    unreliable: UnreliableEchoClient::new(transport.clone()),
                    compressed: CompressedEchoClient::new(transport.clone()),
                    streams: params.compression.streams(),
                    scenario: params.scenario,
                    messages_source: Messages::new(params.payload, params.size, index),
                    options: params.call_options(),
                    messages: params.messages,
                    bidi: None,
                },
                window,
            ))
        })
        .collect();

    tokio::time::sleep_until(window.start).await;
    let before = link.wire_bytes();
    tokio::time::sleep_until(window.end).await;
    let after = link.wire_bytes();
    let wire = before
        .zip(after)
        .map(|(before, after)| after.saturating_sub(before));

    let mut results = Vec::with_capacity(workers.len());
    for worker in workers {
        results.push(worker.await?);
    }
    Ok((results, wire))
}

/// Runs the load and prints the JSON report.
pub async fn run(flags: &Flags) -> Result<(), BoxError> {
    let params = Params::parse(flags)?;
    let link = Link::connect(flags).await?;
    let measured = match &link {
        Link::Quic { transport, .. } => measure(transport.clone(), &params, &link).await,
        Link::Qmux(transport) => measure(transport.clone(), &params, &link).await,
    };
    link.close().await;
    let (results, wire) = measured?;

    let report = Report::new(&params, link.name(), results, wire)?;
    println!("{}", report.json());
    Ok(())
}

/// The failures of every worker counted by message, most frequent first.
fn error_breakdown(results: &[WorkerResult]) -> Vec<(&str, u64)> {
    let mut totals = BTreeMap::<&str, u64>::new();
    for (message, failures) in results.iter().flat_map(|result| &result.errors) {
        let total = totals.entry(message).or_default();
        *total = total.saturating_add(*failures);
    }
    let mut breakdown: Vec<_> = totals.into_iter().collect();
    breakdown.sort_by_key(|&(_, failures)| std::cmp::Reverse(failures));
    breakdown
}

struct Report {
    scenario: Scenario,
    payload: Payload,
    compression: CompressionMode,
    transport: &'static str,
    size: usize,
    metadata_size: usize,
    concurrency: usize,
    duration: f64,
    ops: u64,
    msgs: u64,
    /// Messages sent but not delivered, for the unreliable scenarios.
    lost: u64,
    errors: u64,
    /// Sorted ascending.
    latencies: Vec<Duration>,
    wire_bytes: Option<u64>,
}

impl Report {
    fn new(
        params: &Params,
        transport: &'static str,
        results: Vec<WorkerResult>,
        wire_bytes: Option<u64>,
    ) -> Result<Self, BoxError> {
        let breakdown = error_breakdown(&results);
        let errors = breakdown.iter().map(|&(_, failures)| failures).sum();
        if errors > 0 {
            eprintln!("bench: {errors} operations failed:");
            for (message, failures) in breakdown.into_iter().take(ERROR_BREAKDOWN_LEN) {
                eprintln!("bench:   {failures} x {message}");
            }
        }
        let msgs = results
            .iter()
            .fold(0_u64, |total, result| total.saturating_add(result.messages));
        let lost = results
            .iter()
            .fold(0_u64, |total, result| total.saturating_add(result.lost));
        let mut latencies: Vec<Duration> = results
            .into_iter()
            .flat_map(|result| result.latencies)
            .collect();
        latencies.sort_unstable();
        let ops = u64::try_from(latencies.len())?;
        if ops == 0 {
            return Err(
                format!("no operation completed in the measured window ({errors} failed)").into(),
            );
        }
        Ok(Self {
            scenario: params.scenario,
            payload: params.payload,
            compression: params.compression,
            transport,
            size: params.size,
            metadata_size: params.metadata_size,
            concurrency: params.concurrency,
            duration: params.duration.as_secs_f64(),
            ops,
            msgs,
            lost,
            errors,
            latencies,
            wire_bytes,
        })
    }

    /// The nearest-rank percentile `permille / 1000` of the latencies, in microseconds.
    fn percentile(&self, permille: usize) -> f64 {
        let rank = self
            .latencies
            .len()
            .saturating_mul(permille)
            .div_ceil(1000)
            .saturating_sub(1);
        self.latencies
            .get(rank)
            .or_else(|| self.latencies.last())
            .map_or(0.0, |latency| latency.as_secs_f64() * 1e6)
    }

    fn json(&self) -> String {
        let msgs = count(self.msgs);
        let msgs_per_sec = msgs / self.duration;
        // Nothing delivered leaves no message to divide the wire bytes by.
        let wire = self
            .wire_bytes
            .filter(|_| self.msgs > 0)
            .map_or_else(String::new, |bytes| {
                format!(r#","wire_bytes_per_msg":{:.1}"#, count(bytes) / msgs)
            });
        format!(
            concat!(
                r#"{{"scenario":"{}","transport":"{}","size":{},"metadata_size":{},"concurrency":{},"duration_s":{:?},"#,
                r#""ops":{},"msgs":{},"errors":{},"ops_per_sec":{:.1},"msgs_per_sec":{:.1},"#,
                r#""bytes_per_sec":{:.1},"latency_us":{{"p50":{:.1},"p90":{:.1},"p99":{:.1},"p999":{:.1}}}{},"lost":{},"#,
                r#""payload":"{}","compression":"{}"}}"#,
            ),
            self.scenario.name(),
            self.transport,
            self.size,
            self.metadata_size,
            self.concurrency,
            self.duration,
            self.ops,
            self.msgs,
            self.errors,
            count(self.ops) / self.duration,
            msgs_per_sec,
            msgs_per_sec * count(u64::try_from(self.size).unwrap_or(u64::MAX)),
            self.percentile(500),
            self.percentile(900),
            self.percentile(990),
            self.percentile(999),
            wire,
            self.lost,
            self.payload.name(),
            self.compression.name(),
        )
    }
}

/// A count as a float for rates; precision loss above 2^53 is irrelevant here.
#[allow(clippy::as_conversions, clippy::cast_precision_loss)]
const fn count(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::{WorkerResult, error_breakdown};

    fn worker(errors: &[(&str, u64)]) -> WorkerResult {
        WorkerResult {
            errors: errors
                .iter()
                .map(|&(message, failures)| (message.to_owned(), failures))
                .collect(),
            ..WorkerResult::default()
        }
    }

    #[test]
    fn error_breakdown_sums_across_workers_and_sorts_by_count() {
        let results = [
            worker(&[("operation timed out", 1), ("flow control error", 5)]),
            worker(&[]),
            worker(&[("flow control error", 4), ("connection closed", 2)]),
        ];
        assert_eq!(
            error_breakdown(&results),
            [
                ("flow control error", 9),
                ("connection closed", 2),
                ("operation timed out", 1),
            ]
        );
    }
}
