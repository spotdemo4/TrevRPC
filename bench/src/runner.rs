use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::campaign::{Campaign, Cell, Peer, RpcKind, Stack};
use crate::certificate::{self, Certificates};
use crate::metrics::{METRICS_SCOPE, ProcessDelta, ProcessMonitor};
use crate::network::{Endpoint, NetworkSnapshot, NetworkTopology};
use crate::process::{ManagedChild, OutputLimits, PollLine};
use crate::protocol::{
    self, Armed, Capabilities, HistogramBucket, PeerSample, Prepared, Ready, Role, ValidatedSample,
};
use crate::{BoxError, SCHEMA_VERSION, report};

const RUN_ENTIRE_CAMPAIGN_ENV: &str = "TREVRPC_BENCH_RUN_ENTIRE_CAMPAIGN";
const MAX_CAPABILITY_OUTPUT_BYTES: usize = 64 * 1024;
// log_linear_v1 has at most 28,671 distinct u64 buckets; 3 MiB bounds a
// maximally sparse sample event without imposing the capability-output limit.
const MAX_BENCHMARK_EVENT_BYTES: usize = 3 * 1024 * 1024;
const MAX_DIAGNOSTIC_STREAM_BYTES: u64 = 8 * 1024;
const PEER_POLL_INTERVAL: Duration = Duration::from_millis(10);

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SampleRecord {
    pub schema_version: u32,
    pub campaign_id: String,
    pub sample_id: String,
    pub cell_id: String,
    pub repetition: u32,
    pub client_peer: String,
    pub server_peer: String,
    pub stack: Stack,
    pub rpc_kind: RpcKind,
    pub concurrency: usize,
    pub warmup_ms: u64,
    pub measurement_ms: u64,
    pub request_bytes: u32,
    pub response_bytes: u32,
    pub messages_per_stream: u32,
    pub admission_ns: u64,
    pub elapsed_ns: u64,
    pub drain_ns: u64,
    pub completed: u64,
    pub failed: u64,
    pub request_messages: u64,
    pub response_messages: u64,
    pub operations_per_second: f64,
    pub request_messages_per_second: f64,
    pub response_messages_per_second: f64,
    pub latency_p50_ns: u64,
    pub latency_p99_ns: u64,
    pub latency_max_ns: u64,
    pub client: ProcessDelta,
    pub server: ProcessDelta,
    pub histogram: Vec<HistogramBucket>,
}

#[derive(Serialize)]
struct Manifest<'a> {
    schema_version: u32,
    generated_unix_ms: u128,
    campaign: &'a Campaign,
    campaign_sha256: String,
    source_commit: String,
    source_dirty: bool,
    peer_artifacts: Vec<Artifact>,
    metrics_scope: &'static str,
    network_environment: &'a NetworkSnapshot,
}

#[derive(Serialize)]
struct Artifact {
    peer: String,
    executable: String,
    sha256: String,
}

pub fn print_capabilities(campaign: &Campaign) -> Result<(), BoxError> {
    for peer in &campaign.peers {
        let capabilities = capabilities(peer, Duration::from_millis(campaign.startup_timeout_ms))?;
        validate_capabilities(campaign, peer, &capabilities)?;
        println!("{}", serde_json::to_string(&capabilities)?);
    }
    Ok(())
}

pub fn run(campaign: &Campaign, campaign_path: &Path, output: &Path) -> Result<(), BoxError> {
    let run_entire_campaign = parse_run_entire_campaign(std::env::var(RUN_ENTIRE_CAMPAIGN_ENV))?;
    if output.join("manifest.json").exists() || output.join("samples.jsonl").exists() {
        return Err(format!(
            "output directory {} already contains a campaign",
            output.display()
        )
        .into());
    }
    fs::create_dir_all(output.join("raw"))?;
    for peer in &campaign.peers {
        let capabilities = capabilities(peer, Duration::from_millis(campaign.startup_timeout_ms))?;
        validate_capabilities(campaign, peer, &capabilities)?;
    }
    let topology = NetworkTopology::create(&campaign.network)?;
    let certificates = certificate::generate(output, &topology.certificate_ips())?;
    write_manifest(campaign, campaign_path, output, topology.snapshot())?;

    let samples_path = output.join("samples.jsonl");
    let mut samples = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&samples_path)?;
    run_samples(
        campaign,
        &mut samples,
        run_entire_campaign,
        |cell, rpc_kind, concurrency, repetition| {
            run_sample(
                campaign,
                cell,
                rpc_kind,
                concurrency,
                repetition,
                &certificates,
                &topology,
                output,
            )
        },
    )?;
    report::generate(output)
}

fn parse_run_entire_campaign(value: Result<String, std::env::VarError>) -> Result<bool, BoxError> {
    match value {
        Err(std::env::VarError::NotPresent) => Ok(false),
        Ok(value) => match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(format!("{RUN_ENTIRE_CAMPAIGN_ENV} must be true or false").into()),
        },
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(format!("{RUN_ENTIRE_CAMPAIGN_ENV} must be true or false").into())
        }
    }
}

fn run_samples(
    campaign: &Campaign,
    samples: &mut impl Write,
    run_entire_campaign: bool,
    mut execute: impl FnMut(&Cell, RpcKind, usize, u32) -> Result<SampleRecord, BoxError>,
) -> Result<(), BoxError> {
    let mut failures = Vec::new();
    for repetition in 1..=campaign.repetitions {
        let mut matrix = campaign
            .concurrencies
            .iter()
            .flat_map(|&concurrency| {
                campaign.rpc_kinds.iter().flat_map(move |&rpc_kind| {
                    campaign
                        .cells
                        .iter()
                        .map(move |cell| (concurrency, rpc_kind, cell))
                })
            })
            .collect::<Vec<_>>();
        if repetition % 2 == 0 {
            matrix.reverse();
        }
        for (concurrency, rpc_kind, cell) in matrix {
            match execute(cell, rpc_kind, concurrency, repetition) {
                Ok(sample) => {
                    serde_json::to_writer(&mut *samples, &sample)?;
                    samples.write_all(b"\n")?;
                    samples.flush()?;
                }
                Err(error) if run_entire_campaign => {
                    let sample_id = sample_id(cell, rpc_kind, concurrency, repetition);
                    eprintln!("failed {sample_id}: {error}");
                    failures.push((sample_id, error.to_string()));
                }
                Err(error) => return Err(error),
            }
        }
    }
    if failures.is_empty() {
        return Ok(());
    }

    let mut message = format!("{} benchmark samples failed", failures.len());
    for (sample_id, error) in failures {
        let _ = write!(message, "\n{sample_id}: {error}");
    }
    Err(message.into())
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn run_sample(
    campaign: &Campaign,
    cell: &Cell,
    rpc_kind: RpcKind,
    concurrency: usize,
    repetition: u32,
    certificates: &Certificates,
    topology: &NetworkTopology,
    output: &Path,
) -> Result<SampleRecord, BoxError> {
    let sample_id = sample_id(cell, rpc_kind, concurrency, repetition);
    eprintln!("running {sample_id}");
    let raw = output.join("raw").join(&sample_id);
    fs::create_dir_all(&raw)?;
    let server_peer = campaign.peer(&cell.server).ok_or("missing server peer")?;
    let client_peer = campaign.peer(&cell.client).ok_or("missing client peer")?;

    let startup_timeout = Duration::from_millis(campaign.startup_timeout_ms);
    let (mut server, mut client) = if cell.stack == Stack::TrevrpcWebtransport {
        let mut client = PeerProcess::spawn(
            client_peer,
            &client_arguments(campaign, cell, rpc_kind, concurrency, certificates, None),
            &raw,
            "client",
            topology,
            Endpoint::Client,
        )?;
        let prepared_line = client.event("client prepared", "prepared", startup_timeout)?;
        let prepared: Prepared = parse_expected(&prepared_line, "prepared")?;
        prepared.validate(&client_peer.id, client.id())?;

        let mut server = PeerProcess::spawn(
            server_peer,
            &server_arguments(cell, topology, certificates, Some(&prepared.origin)),
            &raw,
            "server",
            topology,
            Endpoint::Server,
        )?;
        let ready_line = server.event("server ready", "ready", startup_timeout)?;
        let ready: Ready = parse_expected(&ready_line, "ready")?;
        ready.validate(&server_peer.id, server.id())?;
        client.send(&format!("CONNECT {}", ready.address))?;
        (server, client)
    } else {
        let mut server = PeerProcess::spawn(
            server_peer,
            &server_arguments(cell, topology, certificates, None),
            &raw,
            "server",
            topology,
            Endpoint::Server,
        )?;
        let ready_line = server.event("server ready", "ready", startup_timeout)?;
        let ready: Ready = parse_expected(&ready_line, "ready")?;
        ready.validate(&server_peer.id, server.id())?;

        let client = PeerProcess::spawn(
            client_peer,
            &client_arguments(
                campaign,
                cell,
                rpc_kind,
                concurrency,
                certificates,
                Some(&ready.address),
            ),
            &raw,
            "client",
            topology,
            Endpoint::Client,
        )?;
        (server, client)
    };
    let armed_timeout = campaign
        .startup_timeout_ms
        .saturating_add(campaign.timing.warmup_ms)
        .saturating_add(1000);
    let armed_line = wait_for_client_event(
        "client armed",
        "armed",
        &mut client,
        &mut server,
        Duration::from_millis(armed_timeout),
    )?;
    let armed: Armed = parse_expected(&armed_line, "armed")?;
    armed.validate(&client_peer.id, client.id())?;

    let server_monitor = ProcessMonitor::start(server.id())?;
    let client_monitor = ProcessMonitor::start(client.id())?;
    client.send("START")?;
    let result_timeout = campaign
        .timing
        .measurement_ms
        .saturating_add(campaign.drain_timeout_ms)
        .saturating_add(1000);
    let sample_line = wait_for_client_event(
        "client sample",
        "sample",
        &mut client,
        &mut server,
        Duration::from_millis(result_timeout),
    )?;
    let peer_sample: PeerSample = parse_expected(&sample_line, "sample")?;
    let validated = peer_sample.validate(
        &client_peer.id,
        rpc_kind,
        campaign.timing.measurement_ms,
        campaign.workload.messages_per_stream,
    )?;
    let client_metrics = client_monitor.finish();
    let server_metrics = server_monitor.finish();

    let client_exit_timeout = if cell.stack == Stack::TrevrpcWebtransport {
        Duration::from_secs(20)
    } else {
        Duration::from_secs(5)
    };
    client.wait_success(client_exit_timeout)?;
    server.send("SHUTDOWN")?;
    let stopped = server.event("server stopped", "stopped", Duration::from_secs(5))?;
    let stopped_header = protocol::parse_header(&stopped)?;
    if stopped_header.event != "stopped" || stopped_header.peer != server_peer.id {
        return Err(format!("server {} did not acknowledge shutdown", server_peer.id).into());
    }
    server.wait_success(Duration::from_secs(5))?;

    Ok(record(
        campaign,
        cell,
        sample_id,
        rpc_kind,
        concurrency,
        repetition,
        validated,
        client_metrics,
        server_metrics,
        peer_sample.histogram,
    ))
}

fn server_arguments(
    cell: &Cell,
    topology: &NetworkTopology,
    certificates: &Certificates,
    webtransport_origin: Option<&str>,
) -> Vec<String> {
    let mut arguments = vec![
        "server".to_owned(),
        "--stack".to_owned(),
        cell.stack.as_str().to_owned(),
        "--listen".to_owned(),
        topology.server_listen(),
        "--cert".to_owned(),
        path_string(&certificates.certificate),
        "--key".to_owned(),
        path_string(&certificates.private_key),
    ];
    if let Some(origin) = webtransport_origin {
        arguments.extend(["--webtransport-origin".to_owned(), origin.to_owned()]);
    }
    arguments
}

fn client_arguments(
    campaign: &Campaign,
    cell: &Cell,
    rpc_kind: RpcKind,
    concurrency: usize,
    certificates: &Certificates,
    address: Option<&str>,
) -> Vec<String> {
    let certificate = if cell.stack == Stack::TrevrpcWebtransport {
        &certificates.certificate
    } else {
        &certificates.ca
    };
    let mut arguments = vec![
        "client".to_owned(),
        "--stack".to_owned(),
        cell.stack.as_str().to_owned(),
    ];
    if let Some(address) = address {
        arguments.extend(["--address".to_owned(), address.to_owned()]);
    }
    arguments.extend([
        "--cert".to_owned(),
        path_string(certificate),
        "--rpc".to_owned(),
        rpc_kind.as_str().to_owned(),
        "--concurrency".to_owned(),
        concurrency.to_string(),
        "--warmup-ms".to_owned(),
        campaign.timing.warmup_ms.to_string(),
        "--measurement-ms".to_owned(),
        campaign.timing.measurement_ms.to_string(),
        "--request-bytes".to_owned(),
        campaign.workload.request_bytes.to_string(),
        "--response-bytes".to_owned(),
        campaign.workload.response_bytes.to_string(),
        "--messages-per-stream".to_owned(),
        campaign.workload.messages_per_stream.to_string(),
    ]);
    arguments
}

fn sample_id(cell: &Cell, rpc_kind: RpcKind, concurrency: usize, repetition: u32) -> String {
    format!(
        "{}-{}-{}-c{}-r{}",
        cell.id,
        cell.stack.as_str(),
        rpc_kind.as_str(),
        concurrency,
        repetition
    )
}

#[allow(clippy::too_many_arguments)]
fn record(
    campaign: &Campaign,
    cell: &Cell,
    sample_id: String,
    rpc_kind: RpcKind,
    concurrency: usize,
    repetition: u32,
    sample: ValidatedSample,
    client: ProcessDelta,
    server: ProcessDelta,
    histogram: Vec<HistogramBucket>,
) -> SampleRecord {
    let seconds = sample.admission_ns as f64 / 1_000_000_000.0;
    SampleRecord {
        schema_version: SCHEMA_VERSION,
        campaign_id: campaign.campaign_id.clone(),
        sample_id,
        cell_id: cell.id.clone(),
        repetition,
        client_peer: cell.client.clone(),
        server_peer: cell.server.clone(),
        stack: cell.stack,
        rpc_kind,
        concurrency,
        warmup_ms: campaign.timing.warmup_ms,
        measurement_ms: campaign.timing.measurement_ms,
        request_bytes: campaign.workload.request_bytes,
        response_bytes: campaign.workload.response_bytes,
        messages_per_stream: campaign.workload.messages_per_stream,
        admission_ns: sample.admission_ns,
        elapsed_ns: sample.elapsed_ns,
        drain_ns: sample.drain_ns,
        completed: sample.completed,
        failed: sample.failed,
        request_messages: sample.request_messages,
        response_messages: sample.response_messages,
        operations_per_second: sample.completed as f64 / seconds,
        request_messages_per_second: sample.request_messages as f64 / seconds,
        response_messages_per_second: sample.response_messages as f64 / seconds,
        latency_p50_ns: sample.latency_p50_ns,
        latency_p99_ns: sample.latency_p99_ns,
        latency_max_ns: sample.latency_max_ns,
        client,
        server,
        histogram,
    }
}

fn capabilities(peer: &Peer, timeout: Duration) -> Result<Capabilities, BoxError> {
    let capture = CapabilityCapture::new(peer)?;
    let mut command = peer_command(peer)?;
    crate::process::configure_session(&mut command);
    let mut child = command
        .arg("capabilities")
        .stdin(Stdio::null())
        .stdout(Stdio::from(File::create(&capture.stdout)?))
        .stderr(Stdio::from(File::create(&capture.stderr)?))
        .spawn()?;
    let deadline = Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if capture.output_too_large()? {
            if crate::process::kill_process_group(child.id()).is_err() {
                let _ = child.kill();
            }
            let _ = child.wait();
            return Err(format!("peer {} capabilities exceeded output limit", peer.id).into());
        }
        if Instant::now() >= deadline {
            if crate::process::kill_process_group(child.id()).is_err() {
                let _ = child.kill();
            }
            let _ = child.wait();
            return Err(format!("peer {} capabilities timed out", peer.id).into());
        }
        thread::sleep(Duration::from_millis(10));
    };
    if crate::process::kill_process_group(child.id()).is_err() {
        let _ = child.kill();
    }
    let stdout = CapabilityCapture::read(&capture.stdout, &peer.id, "stdout")?;
    let stderr = CapabilityCapture::read(&capture.stderr, &peer.id, "stderr")?;
    if !status.success() {
        return Err(format!(
            "peer {} capabilities failed: {}",
            peer.id,
            String::from_utf8_lossy(&stderr).trim()
        )
        .into());
    }
    let stdout = String::from_utf8(stdout)?;
    let mut lines = stdout.lines().filter(|line| !line.trim().is_empty());
    let line = lines
        .next()
        .ok_or_else(|| format!("peer {} emitted no capabilities", peer.id))?;
    if lines.next().is_some() {
        return Err(format!("peer {} emitted extra capability output", peer.id).into());
    }
    Ok(serde_json::from_str(line)?)
}

fn validate_capabilities(
    campaign: &Campaign,
    peer: &Peer,
    capabilities: &Capabilities,
) -> Result<(), BoxError> {
    let mut roles = BTreeMap::<Role, BTreeSet<Stack>>::new();
    for cell in &campaign.cells {
        if cell.client == peer.id {
            roles.entry(Role::Client).or_default().insert(cell.stack);
        }
        if cell.server == peer.id {
            roles.entry(Role::Server).or_default().insert(cell.stack);
        }
    }
    capabilities.validate(&peer.id, &roles, &campaign.rpc_kinds)
}

struct CapabilityCapture {
    directory: PathBuf,
    stdout: PathBuf,
    stderr: PathBuf,
}

impl CapabilityCapture {
    fn new(peer: &Peer) -> Result<Self, BoxError> {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "trevrpc-bench-capabilities-{}-{}-{nonce}",
            std::process::id(),
            peer.id
        ));
        fs::create_dir(&directory)?;
        Ok(Self {
            stdout: directory.join("stdout"),
            stderr: directory.join("stderr"),
            directory,
        })
    }

    fn output_too_large(&self) -> Result<bool, BoxError> {
        for path in [&self.stdout, &self.stderr] {
            if path.metadata()?.len() > MAX_CAPABILITY_OUTPUT_BYTES as u64 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn read(path: &Path, peer: &str, stream: &str) -> Result<Vec<u8>, BoxError> {
        let output = fs::read(path)?;
        if output.len() > MAX_CAPABILITY_OUTPUT_BYTES {
            return Err(format!("peer {peer} capabilities {stream} exceeded output limit").into());
        }
        Ok(output)
    }
}

impl Drop for CapabilityCapture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

fn peer_command(peer: &Peer) -> Result<Command, BoxError> {
    let executable = peer.command.first().ok_or("empty peer command")?;
    let mut command = Command::new(executable);
    command.args(&peer.command[1..]);
    Ok(command)
}

struct PeerProcess {
    child: ManagedChild,
    peer: String,
    role: String,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

enum EventPoll {
    Line(String),
    Empty,
    Disconnected,
}

impl PeerProcess {
    fn spawn(
        peer: &Peer,
        arguments: &[String],
        raw: &Path,
        role: &str,
        topology: &NetworkTopology,
        endpoint: Endpoint,
    ) -> Result<Self, BoxError> {
        let stdout_path = raw.join(format!("{role}.stdout"));
        let stderr_path = raw.join(format!("{role}.stderr"));
        let mut command = peer_command(peer)?;
        topology.configure_command(&mut command, endpoint);
        command.args(arguments);
        let child = ManagedChild::spawn(
            command,
            &stdout_path,
            &stderr_path,
            OutputLimits {
                max_line_bytes: MAX_BENCHMARK_EVENT_BYTES,
                max_stdout_bytes: usize::MAX,
                max_stderr_bytes: usize::MAX,
                diagnostic_tail_bytes: usize::try_from(MAX_DIAGNOSTIC_STREAM_BYTES)
                    .unwrap_or(8 * 1024),
            },
        )
        .map_err(|error| format!("failed to start {} {role}: {error}", peer.id))?;
        Ok(Self {
            child,
            peer: peer.id.clone(),
            role: role.to_owned(),
            stdout_path,
            stderr_path,
        })
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn send(&mut self, command: &str) -> Result<(), BoxError> {
        self.child.send(command)
    }

    fn event(
        &mut self,
        phase: &str,
        expected: &str,
        timeout: Duration,
    ) -> Result<String, BoxError> {
        let deadline = Instant::now() + timeout;
        let mut exit_status = None;
        loop {
            match self.poll_event()? {
                EventPoll::Line(line) => {
                    return validate_peer_event(phase, expected, line, &[self]);
                }
                EventPoll::Disconnected => {
                    if exit_status.is_none() {
                        exit_status = self.child.try_wait()?;
                    }
                    let detail = exit_status.map_or_else(
                        || "closed its event stream".to_owned(),
                        |status| format!("exited with {status}"),
                    );
                    return Err(phase_failure(
                        phase,
                        format!("{} {} {detail}", self.role, self.peer),
                        &[self],
                    ));
                }
                EventPoll::Empty => {}
            }
            if exit_status.is_none()
                && let Some(status) = self.child.try_wait()?
            {
                exit_status = Some(status);
                self.terminate_group();
            }
            if Instant::now() >= deadline {
                if let EventPoll::Line(line) = self.poll_event()? {
                    return validate_peer_event(phase, expected, line, &[self]);
                }
                let detail = exit_status.map_or_else(
                    || format!("{} {} was silent", self.role, self.peer),
                    |status| format!("{} {} exited with {status}", self.role, self.peer),
                );
                return Err(phase_failure(phase, detail, &[self]));
            }
            thread::sleep(
                PEER_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }

    fn poll_event(&mut self) -> Result<EventPoll, BoxError> {
        match self.child.poll_line()? {
            PollLine::Line(line) => Ok(EventPoll::Line(line)),
            PollLine::Empty => Ok(EventPoll::Empty),
            PollLine::Disconnected => Ok(EventPoll::Disconnected),
            PollLine::LineTooLong => Err("peer emitted an overlong event line".into()),
            PollLine::StreamTooLong => Err("peer stdout exceeded the configured limit".into()),
        }
    }

    fn wait_success(&mut self, timeout: Duration) -> Result<(), BoxError> {
        let status = self.child.wait(timeout)?;
        if !status.success() {
            return Err(format!("peer process {} exited with {status}", self.child.id()).into());
        }
        if self.child.stdout_overflowed() || self.child.stderr_overflowed() {
            return Err(format!("peer process {} exceeded output limits", self.child.id()).into());
        }
        let extra_lines = self.child.drain_output()?;
        if !extra_lines.is_empty() {
            return Err(format!(
                "peer process {} emitted {} extra event(s)",
                self.child.id(),
                extra_lines.len()
            )
            .into());
        }
        Ok(())
    }

    fn terminate_group(&mut self) {
        self.child.terminate_group();
    }
}

fn wait_for_client_event(
    phase: &str,
    expected: &str,
    client: &mut PeerProcess,
    server: &mut PeerProcess,
    timeout: Duration,
) -> Result<String, BoxError> {
    let deadline = Instant::now() + timeout;
    let mut client_exit: Option<ExitStatus> = None;
    let mut server_exit: Option<ExitStatus> = None;
    loop {
        match server.poll_event()? {
            EventPoll::Line(line) => {
                return Err(server_event_failure(phase, &line, &[&*client, &*server]));
            }
            EventPoll::Disconnected => {
                if server_exit.is_none() {
                    server_exit = server.child.try_wait()?;
                }
                let detail = server_exit.map_or_else(
                    || format!("server {} closed its event stream", server.peer),
                    |status| format!("server {} exited with {status}", server.peer),
                );
                return Err(phase_failure(phase, detail, &[&*client, &*server]));
            }
            EventPoll::Empty => {}
        }

        match client.poll_event()? {
            EventPoll::Line(line) => {
                return validate_peer_event(phase, expected, line, &[&*client, &*server]);
            }
            EventPoll::Disconnected => {
                if client_exit.is_none() {
                    client_exit = client.child.try_wait()?;
                }
                let detail = client_exit.map_or_else(
                    || format!("client {} closed its event stream", client.peer),
                    |status| format!("client {} exited with {status}", client.peer),
                );
                return Err(phase_failure(phase, detail, &[&*client, &*server]));
            }
            EventPoll::Empty => {}
        }

        if server_exit.is_none()
            && let Some(status) = server.child.try_wait()?
        {
            server_exit = Some(status);
            server.terminate_group();
        }
        if client_exit.is_none()
            && let Some(status) = client.child.try_wait()?
        {
            client_exit = Some(status);
            client.terminate_group();
        }
        if Instant::now() >= deadline {
            if let EventPoll::Line(line) = server.poll_event()? {
                return Err(server_event_failure(phase, &line, &[&*client, &*server]));
            }
            if let EventPoll::Line(line) = client.poll_event()? {
                return validate_peer_event(phase, expected, line, &[&*client, &*server]);
            }
            let detail = server_exit.map_or_else(
                || {
                    client_exit.map_or_else(
                        || format!("client {} was silent", client.peer),
                        |status| format!("client {} exited with {status}", client.peer),
                    )
                },
                |status| format!("server {} exited with {status}", server.peer),
            );
            return Err(phase_failure(phase, detail, &[&*client, &*server]));
        }
        thread::sleep(PEER_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

fn server_event_failure(phase: &str, line: &str, peers: &[&PeerProcess]) -> BoxError {
    let header = match protocol::parse_header(line) {
        Ok(header) => header,
        Err(error) => return phase_failure(phase, error, peers),
    };
    let detail = if header.event == "error" {
        match protocol::peer_error(line) {
            Ok(error) => error,
            Err(error) => return phase_failure(phase, error, peers),
        }
    } else {
        format!("unexpected server event {}", header.event)
    };
    phase_failure(phase, detail, peers)
}

fn validate_peer_event(
    phase: &str,
    expected: &str,
    line: String,
    peers: &[&PeerProcess],
) -> Result<String, BoxError> {
    let header =
        protocol::parse_header(&line).map_err(|error| phase_failure(phase, error, peers))?;
    if header.event == "error" {
        return Err(phase_failure(phase, protocol::peer_error(&line)?, peers));
    }
    if header.event != expected {
        return Err(phase_failure(
            phase,
            format!("expected {expected} event, got {}", header.event),
            peers,
        ));
    }
    Ok(line)
}

fn phase_failure(phase: &str, detail: impl std::fmt::Display, peers: &[&PeerProcess]) -> BoxError {
    let mut message = format!("phase {phase:?}: {detail}");
    for peer in peers {
        append_output_tail(&mut message, peer, "stdout", &peer.stdout_path);
        append_output_tail(&mut message, peer, "stderr", &peer.stderr_path);
    }
    message.into()
}

fn append_output_tail(message: &mut String, peer: &PeerProcess, stream: &str, path: &Path) {
    let _ = write!(message, "\n--- {} {} {stream}", peer.role, peer.peer);
    match read_output_tail(path) {
        Ok((output, truncated)) => {
            if truncated {
                message.push_str(" (truncated)");
            }
            message.push_str(" ---\n");
            message.push_str(&output);
            if !output.ends_with('\n') {
                message.push('\n');
            }
        }
        Err(error) => {
            let _ = writeln!(message, " unavailable: {error} ---");
        }
    }
}

fn read_output_tail(path: &Path) -> Result<(String, bool), io::Error> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    let truncated = length > MAX_DIAGNOSTIC_STREAM_BYTES;
    if truncated {
        file.seek(SeekFrom::Start(length - MAX_DIAGNOSTIC_STREAM_BYTES))?;
    }
    let mut bytes =
        Vec::with_capacity(usize::try_from(length.min(MAX_DIAGNOSTIC_STREAM_BYTES)).unwrap_or(0));
    file.take(MAX_DIAGNOSTIC_STREAM_BYTES)
        .read_to_end(&mut bytes)?;
    Ok((String::from_utf8_lossy(&bytes).into_owned(), truncated))
}

fn parse_expected<T>(line: &str, expected: &str) -> Result<T, BoxError>
where
    T: serde::de::DeserializeOwned,
{
    let header = protocol::parse_header(line)?;
    if header.event != expected {
        return Err(format!("expected {expected} event, got {}", header.event).into());
    }
    Ok(serde_json::from_str(line)?)
}

fn write_manifest(
    campaign: &Campaign,
    campaign_path: &Path,
    output: &Path,
    network_environment: &NetworkSnapshot,
) -> Result<(), BoxError> {
    let campaign_bytes = fs::read(campaign_path)?;
    let mut artifacts = Vec::with_capacity(campaign.peers.len());
    for peer in &campaign.peers {
        let executable = peer.command.first().ok_or("empty peer command")?;
        let resolved = resolve_executable(executable);
        artifacts.push(Artifact {
            peer: peer.id.clone(),
            executable: resolved.to_string_lossy().into_owned(),
            sha256: sha256_file(&resolved).unwrap_or_else(|_| "unavailable".to_owned()),
        });
    }
    let source_commit = std::env::var("TREVRPC_BENCH_SOURCE_COMMIT")
        .ok()
        .or_else(|| git_output(["rev-parse", "HEAD"]).ok())
        .unwrap_or_else(|| "unknown".to_owned());
    let source_dirty = std::env::var("TREVRPC_BENCH_SOURCE_DIRTY").map_or_else(
        |_| git_output(["status", "--short"]).is_ok_and(|value| !value.is_empty()),
        |value| value == "true",
    );
    let manifest = Manifest {
        schema_version: SCHEMA_VERSION,
        generated_unix_ms: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_millis(),
        campaign,
        campaign_sha256: sha256(&campaign_bytes),
        source_commit,
        source_dirty,
        peer_artifacts: artifacts,
        metrics_scope: METRICS_SCOPE,
        network_environment,
    };
    let file = File::create(output.join("manifest.json"))?;
    serde_json::to_writer_pretty(file, &manifest)?;
    Ok(())
}

fn git_output<const N: usize>(args: [&str; N]) -> Result<String, BoxError> {
    let output = Command::new("git").args(args).output()?;
    if !output.status.success() {
        return Err("git command failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn sha256_file(path: &Path) -> Result<String, BoxError> {
    Ok(sha256(&fs::read(path)?))
}

fn sha256(input: &[u8]) -> String {
    let digest = Sha256::digest(input);
    base16ct::lower::encode_string(digest.as_slice())
}

fn resolve_executable(executable: &str) -> PathBuf {
    let path = PathBuf::from(executable);
    if path.components().count() > 1 {
        return path;
    }
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|value| std::env::split_paths(&value).collect::<Vec<_>>())
        .map(|directory| directory.join(executable))
        .find(|candidate| candidate.is_file())
        .unwrap_or(path)
}

fn path_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::ffi::OsString;
    use std::fs;
    use std::os::unix::ffi::OsStringExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use super::{
        MAX_BENCHMARK_EVENT_BYTES, MAX_CAPABILITY_OUTPUT_BYTES, MAX_DIAGNOSTIC_STREAM_BYTES,
        PeerProcess, RUN_ENTIRE_CAMPAIGN_ENV, SampleRecord, client_arguments,
        parse_run_entire_campaign, read_output_tail, run_samples, sample_id, server_arguments,
        sha256, validate_capabilities, wait_for_client_event,
    };
    use crate::SCHEMA_VERSION;
    use crate::campaign::{Campaign, Cell, Network, Peer, RpcKind, Stack, Timing, Workload};
    use crate::certificate::Certificates;
    use crate::network::{Endpoint, NetworkTopology};
    use crate::protocol::{Capabilities, PeerSample, Role};

    fn campaign() -> Campaign {
        Campaign {
            schema_version: SCHEMA_VERSION,
            campaign_id: "role-specific".to_owned(),
            repetitions: 1,
            peers: vec![
                Peer {
                    id: "client".to_owned(),
                    command: vec!["client-peer".to_owned()],
                },
                Peer {
                    id: "server".to_owned(),
                    command: vec!["server-peer".to_owned()],
                },
            ],
            cells: vec![Cell {
                id: "native".to_owned(),
                client: "client".to_owned(),
                server: "server".to_owned(),
                stack: Stack::TrevrpcNativeQuic,
            }],
            rpc_kinds: vec![RpcKind::Unary],
            concurrencies: vec![1],
            timing: Timing {
                warmup_ms: 0,
                measurement_ms: 1,
            },
            workload: Workload {
                request_bytes: 1,
                response_bytes: 1,
                messages_per_stream: 1,
            },
            network: Network::default(),
            startup_timeout_ms: 1000,
            drain_timeout_ms: 1000,
        }
    }

    fn test_raw_directory(name: &str) -> PathBuf {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "trevrpc-bench-{name}-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).expect("create test raw directory");
        path
    }

    fn scripted_peer(id: &str, script: &str) -> Peer {
        Peer {
            id: id.to_owned(),
            command: vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()],
        }
    }

    fn spawn_scripted_peer(peer: &Peer, raw: &Path, role: &str) -> PeerProcess {
        let topology = NetworkTopology::create(&Network::default()).expect("loopback topology");
        PeerProcess::spawn(peer, &[], raw, role, &topology, Endpoint::Server).expect("spawn peer")
    }

    fn capabilities(peer: &str, roles: &[&str], stacks: &[Stack]) -> Capabilities {
        Capabilities {
            schema_version: SCHEMA_VERSION,
            event: "capabilities".to_owned(),
            peer: peer.to_owned(),
            rpc_kinds: vec!["unary".to_owned()],
            roles: roles
                .iter()
                .map(|role| {
                    let role = match *role {
                        "client" => Role::Client,
                        "server" => Role::Server,
                        _ => panic!("unknown role"),
                    };
                    (role, stacks.to_vec())
                })
                .collect::<BTreeMap<_, _>>(),
            histogram: "log_linear_v1".to_owned(),
        }
    }

    fn certificates() -> Certificates {
        Certificates {
            ca: PathBuf::from("/certificates/ca.pem"),
            certificate: PathBuf::from("/certificates/server.pem"),
            private_key: PathBuf::from("/certificates/server-key.pem"),
        }
    }

    fn option_value<'a>(arguments: &'a [String], option: &str) -> Option<&'a str> {
        arguments
            .iter()
            .position(|argument| argument == option)
            .and_then(|index| arguments.get(index + 1))
            .map(String::as_str)
    }

    #[test]
    fn parses_run_entire_campaign_strictly() {
        assert!(!parse_run_entire_campaign(Err(std::env::VarError::NotPresent)).expect("unset"));
        assert!(parse_run_entire_campaign(Ok("true".to_owned())).expect("true"));
        assert!(!parse_run_entire_campaign(Ok("false".to_owned())).expect("false"));

        for value in ["", "TRUE", "1", " true"] {
            let error = parse_run_entire_campaign(Ok(value.to_owned()))
                .expect_err("invalid value")
                .to_string();
            assert_eq!(
                error,
                format!("{RUN_ENTIRE_CAMPAIGN_ENV} must be true or false")
            );
        }

        let error = parse_run_entire_campaign(Err(std::env::VarError::NotUnicode(
            OsString::from_vec(vec![0xff]),
        )))
        .expect_err("non-Unicode value")
        .to_string();
        assert_eq!(
            error,
            format!("{RUN_ENTIRE_CAMPAIGN_ENV} must be true or false")
        );
    }

    #[test]
    fn sample_matrix_fails_fast_by_default() {
        let mut campaign = campaign();
        campaign.rpc_kinds = vec![RpcKind::Unary, RpcKind::Bidi];
        campaign.concurrencies = vec![1, 2];
        let mut attempts = Vec::new();
        let mut samples = Vec::new();

        let error = run_samples(
            &campaign,
            &mut samples,
            false,
            |cell, rpc_kind, concurrency, repetition| {
                let sample_id = sample_id(cell, rpc_kind, concurrency, repetition);
                attempts.push(sample_id.clone());
                Err::<SampleRecord, _>(format!("failure {sample_id}").into())
            },
        )
        .expect_err("first sample failure")
        .to_string();

        assert_eq!(attempts.len(), 1);
        assert_eq!(error, format!("failure {}", attempts[0]));
        assert!(samples.is_empty());
    }

    #[test]
    fn sample_matrix_can_collect_every_failure() {
        let mut campaign = campaign();
        campaign.repetitions = 2;
        campaign.rpc_kinds = vec![RpcKind::Unary, RpcKind::Bidi];
        campaign.concurrencies = vec![1, 2];
        let mut attempts = Vec::new();
        let mut samples = Vec::new();

        let error = run_samples(
            &campaign,
            &mut samples,
            true,
            |cell, rpc_kind, concurrency, repetition| {
                let sample_id = sample_id(cell, rpc_kind, concurrency, repetition);
                attempts.push(sample_id.clone());
                Err::<SampleRecord, _>(format!("failure {sample_id}").into())
            },
        )
        .expect_err("campaign failures")
        .to_string();

        assert_eq!(attempts.len(), 8);
        assert!(error.starts_with("8 benchmark samples failed\n"));
        let positions = attempts
            .iter()
            .map(|sample_id| {
                error
                    .find(&format!("{sample_id}: failure {sample_id}"))
                    .expect("sample failure in aggregate")
            })
            .collect::<Vec<_>>();
        assert!(positions.windows(2).all(|window| window[0] < window[1]));
        assert!(samples.is_empty());
    }

    #[test]
    fn validates_capabilities_for_each_peers_actual_cell_usage() {
        let campaign = campaign();
        validate_capabilities(
            &campaign,
            &campaign.peers[0],
            &capabilities("client", &["client"], &[Stack::TrevrpcNativeQuic]),
        )
        .expect("client-only peer");
        validate_capabilities(
            &campaign,
            &campaign.peers[1],
            &capabilities("server", &["server"], &[Stack::TrevrpcNativeQuic]),
        )
        .expect("server-only peer");

        let error = validate_capabilities(
            &campaign,
            &campaign.peers[0],
            &capabilities("client", &["client"], &[Stack::TrevrpcWebtransport]),
        )
        .expect_err("wrong stack");
        assert!(error.to_string().contains("trevrpc_native_quic"));
    }

    #[test]
    fn sample_ids_distinguish_stacks() {
        let mut cell = campaign().cells.remove(0);
        let native = sample_id(&cell, RpcKind::Unary, 1, 1);
        cell.id = "other".to_owned();
        let other = sample_id(&cell, RpcKind::Unary, 1, 1);
        assert_ne!(native, other);
        assert!(native.contains("trevrpc_native_quic"));
    }

    #[test]
    fn benchmark_event_limit_accepts_large_valid_histogram() {
        let histogram = (1..=1800u64)
            .map(|upper_bound| {
                serde_json::json!({
                    "upper_bound_ns": upper_bound.to_string(),
                    "count": "1"
                })
            })
            .collect::<Vec<_>>();
        let line = serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "event": "sample",
            "peer": "client",
            "rpc_kind": "unary",
            "admission_ns": "1000000",
            "elapsed_ns": "1000000",
            "drain_ns": "0",
            "completed": "1800",
            "failed": "0",
            "request_messages": "1800",
            "response_messages": "1800",
            "histogram": histogram
        })
        .to_string();
        assert!(line.len() > MAX_CAPABILITY_OUTPUT_BYTES);
        assert!(line.len() <= MAX_BENCHMARK_EVENT_BYTES);
        let sample: PeerSample = serde_json::from_str(&line).expect("valid sample event");
        sample
            .validate("client", RpcKind::Unary, 1, 1)
            .expect("valid large histogram");
    }

    #[test]
    fn webtransport_arguments_prepare_client_before_server_address_exists() {
        let campaign = campaign();
        let mut cell = campaign.cells[0].clone();
        cell.stack = Stack::TrevrpcWebtransport;
        let certificates = certificates();
        let topology = NetworkTopology::create(&Network::default()).expect("loopback topology");

        let client = client_arguments(&campaign, &cell, RpcKind::Unary, 1, &certificates, None);
        assert_eq!(
            option_value(&client, "--cert"),
            Some("/certificates/server.pem")
        );
        assert_eq!(option_value(&client, "--address"), None);

        let server = server_arguments(
            &cell,
            &topology,
            &certificates,
            Some("http://127.0.0.1:4443"),
        );
        assert_eq!(
            option_value(&server, "--webtransport-origin"),
            Some("http://127.0.0.1:4443")
        );
    }

    #[test]
    fn native_client_arguments_keep_address_and_ca_certificate() {
        let campaign = campaign();
        let cell = &campaign.cells[0];
        let arguments = client_arguments(
            &campaign,
            cell,
            RpcKind::Unary,
            1,
            &certificates(),
            Some("127.0.0.1:43117"),
        );
        assert_eq!(
            option_value(&arguments, "--cert"),
            Some("/certificates/ca.pem")
        );
        assert_eq!(
            option_value(&arguments, "--address"),
            Some("127.0.0.1:43117")
        );
        assert_eq!(option_value(&arguments, "--webtransport-origin"), None);
    }

    #[test]
    fn client_wait_surfaces_server_error_before_timeout() {
        let raw = test_raw_directory("server-error");
        let client_peer = scripted_peer("client", "sleep 10");
        let server_peer = scripted_peer(
            "server",
            "printf '%s\\n' '{\"schema_version\":4,\"event\":\"error\",\"peer\":\"server\",\"phase\":\"serve\",\"code\":\"worker_failed\",\"message\":\"worker startup failed\"}'",
        );
        let mut client = spawn_scripted_peer(&client_peer, &raw, "client");
        let mut server = spawn_scripted_peer(&server_peer, &raw, "server");

        let error = wait_for_client_event(
            "client armed",
            "armed",
            &mut client,
            &mut server,
            Duration::from_secs(2),
        )
        .expect_err("server error must fail the wait")
        .to_string();

        assert!(error.contains("client armed"));
        assert!(error.contains("worker_failed"));
        assert!(error.contains("worker startup failed"));
        drop(client);
        drop(server);
        fs::remove_dir_all(raw).expect("remove test raw directory");
    }

    #[test]
    fn event_reports_peer_exit_with_phase_and_stderr() {
        let raw = test_raw_directory("peer-exit");
        let peer = scripted_peer("server", "printf 'worker exhausted\\n' >&2; exit 7");
        let mut process = spawn_scripted_peer(&peer, &raw, "server");

        let error = process
            .event("server ready", "ready", Duration::from_secs(2))
            .expect_err("peer exit must fail the wait")
            .to_string();

        assert!(error.contains("server ready"));
        assert!(error.contains("exit status: 7"), "{error}");
        assert!(error.contains("worker exhausted"));
        drop(process);
        fs::remove_dir_all(raw).expect("remove test raw directory");
    }

    #[test]
    fn diagnostic_output_tail_is_bounded() {
        let raw = test_raw_directory("output-tail");
        let path = raw.join("peer.stderr");
        let mut output =
            vec![b'x'; usize::try_from(MAX_DIAGNOSTIC_STREAM_BYTES).expect("tail size") + 32];
        output.extend_from_slice(b"ending marker\n");
        fs::write(&path, output).expect("write diagnostic fixture");

        let (tail, truncated) = read_output_tail(&path).expect("read diagnostic tail");

        assert!(truncated);
        assert!(tail.len() <= usize::try_from(MAX_DIAGNOSTIC_STREAM_BYTES).expect("tail size"));
        assert!(tail.ends_with("ending marker\n"));
        fs::remove_dir_all(raw).expect("remove test raw directory");
    }

    #[test]
    fn encodes_sha256_as_lowercase_hex() {
        assert_eq!(
            sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
