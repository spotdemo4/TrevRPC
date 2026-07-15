use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::campaign::{Campaign, Cell, Peer, RpcKind, Stack};
use crate::certificate::{self, Certificates};
use crate::metrics::{ProcessDelta, ProcessMonitor};
use crate::protocol::{
    self, Armed, Capabilities, HistogramBucket, PeerSample, Ready, ValidatedSample,
};
use crate::{BoxError, SCHEMA_VERSION, report};

const MAX_CAPABILITY_OUTPUT_BYTES: usize = 64 * 1024;

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
    if output.join("manifest.json").exists() || output.join("samples.jsonl").exists() {
        return Err(format!(
            "output directory {} already contains a campaign",
            output.display()
        )
        .into());
    }
    fs::create_dir_all(output.join("raw"))?;
    let certificates = certificate::generate(output)?;
    for peer in &campaign.peers {
        let capabilities = capabilities(peer, Duration::from_millis(campaign.startup_timeout_ms))?;
        validate_capabilities(campaign, peer, &capabilities)?;
    }
    write_manifest(campaign, campaign_path, output)?;

    let samples_path = output.join("samples.jsonl");
    let mut samples = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&samples_path)?;
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
            let sample = run_sample(
                campaign,
                cell,
                rpc_kind,
                concurrency,
                repetition,
                &certificates,
                output,
            )?;
            serde_json::to_writer(&mut samples, &sample)?;
            samples.write_all(b"\n")?;
            samples.flush()?;
        }
    }
    report::generate(output)
}

fn run_sample(
    campaign: &Campaign,
    cell: &Cell,
    rpc_kind: RpcKind,
    concurrency: usize,
    repetition: u32,
    certificates: &Certificates,
    output: &Path,
) -> Result<SampleRecord, BoxError> {
    let sample_id = sample_id(cell, rpc_kind, concurrency, repetition);
    eprintln!("running {sample_id}");
    let raw = output.join("raw").join(&sample_id);
    fs::create_dir_all(&raw)?;
    let server_peer = campaign.peer(&cell.server).ok_or("missing server peer")?;
    let client_peer = campaign.peer(&cell.client).ok_or("missing client peer")?;

    let mut server = PeerProcess::spawn(
        server_peer,
        &[
            "server".to_owned(),
            "--stack".to_owned(),
            cell.stack.as_str().to_owned(),
            "--listen".to_owned(),
            "127.0.0.1:0".to_owned(),
            "--cert".to_owned(),
            path_string(&certificates.certificate),
            "--key".to_owned(),
            path_string(&certificates.private_key),
        ],
        &raw,
        "server",
    )?;
    let ready_line = server.event(Duration::from_millis(campaign.startup_timeout_ms))?;
    let ready: Ready = parse_expected(&ready_line, "ready")?;
    ready.validate(&server_peer.id, server.id())?;

    let mut client = PeerProcess::spawn(
        client_peer,
        &[
            "client".to_owned(),
            "--stack".to_owned(),
            cell.stack.as_str().to_owned(),
            "--address".to_owned(),
            ready.address,
            "--cert".to_owned(),
            path_string(&certificates.ca),
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
        ],
        &raw,
        "client",
    )?;
    let armed_timeout = campaign
        .startup_timeout_ms
        .saturating_add(campaign.timing.warmup_ms)
        .saturating_add(1000);
    let armed_line = client.event(Duration::from_millis(armed_timeout))?;
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
    let sample_line = client.event(Duration::from_millis(result_timeout))?;
    let peer_sample: PeerSample = parse_expected(&sample_line, "sample")?;
    let validated = peer_sample.validate(
        &client_peer.id,
        rpc_kind,
        campaign.timing.measurement_ms,
        campaign.workload.messages_per_stream,
    )?;
    let client_metrics = client_monitor.finish(client.id());
    let server_metrics = server_monitor.finish(server.id());

    client.wait_success(Duration::from_secs(5))?;
    server.send("SHUTDOWN")?;
    let stopped = server.event(Duration::from_secs(5))?;
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
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("peer {} capabilities exceeded output limit", peer.id).into());
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("peer {} capabilities timed out", peer.id).into());
        }
        thread::sleep(Duration::from_millis(10));
    };
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
    let mut roles = BTreeSet::new();
    let mut stacks = BTreeSet::new();
    for cell in &campaign.cells {
        if cell.client == peer.id {
            roles.insert("client");
            stacks.insert(cell.stack);
        }
        if cell.server == peer.id {
            roles.insert("server");
            stacks.insert(cell.stack);
        }
    }
    capabilities.validate(
        &peer.id,
        &roles.into_iter().collect::<Vec<_>>(),
        &campaign.rpc_kinds,
        &stacks.into_iter().collect::<Vec<_>>(),
    )
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
    child: Child,
    stdin: ChildStdin,
    events: Receiver<Result<String, String>>,
}

impl PeerProcess {
    fn spawn(peer: &Peer, arguments: &[String], raw: &Path, role: &str) -> Result<Self, BoxError> {
        let stdout_path = raw.join(format!("{role}.stdout"));
        let stderr_path = raw.join(format!("{role}.stderr"));
        let stderr = File::create(&stderr_path)?;
        let mut command = peer_command(peer)?;
        let mut child = command
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| format!("failed to start {} {role}: {error}", peer.id))?;
        let stdin = child.stdin.take().ok_or("peer stdin was not piped")?;
        let stdout = child.stdout.take().ok_or("peer stdout was not piped")?;
        let (sender, events) = mpsc::channel();
        thread::spawn(move || {
            let mut raw_output = match File::create(stdout_path) {
                Ok(file) => file,
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    return;
                }
            };
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        if let Err(error) = writeln!(raw_output, "{line}") {
                            let _ = sender.send(Err(error.to_string()));
                            return;
                        }
                        if sender.send(Ok(line)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = sender.send(Err(error.to_string()));
                        return;
                    }
                }
            }
        });
        Ok(Self {
            child,
            stdin,
            events,
        })
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn send(&mut self, command: &str) -> Result<(), BoxError> {
        writeln!(self.stdin, "{command}")?;
        self.stdin.flush()?;
        Ok(())
    }

    fn event(&self, timeout: Duration) -> Result<String, BoxError> {
        let line = self
            .events
            .recv_timeout(timeout)
            .map_err(|error| format!("timed out waiting for peer event: {error}"))?
            .map_err(|error| format!("failed to read peer output: {error}"))?;
        let header = protocol::parse_header(&line)?;
        if header.event == "error" {
            return Err(protocol::peer_error(&line)?.into());
        }
        Ok(line)
    }

    fn wait_success(&mut self, timeout: Duration) -> Result<(), BoxError> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait()? {
                if status.success() {
                    return Ok(());
                }
                return Err(
                    format!("peer process {} exited with {status}", self.child.id()).into(),
                );
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                return Err(format!("peer process {} did not exit", self.child.id()).into());
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for PeerProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
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
        metrics_scope: "direct_peer_process_procfs_10ms",
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
    use super::{sample_id, sha256, validate_capabilities};
    use crate::SCHEMA_VERSION;
    use crate::campaign::{Campaign, Cell, Peer, RpcKind, Stack, Timing, Workload};
    use crate::protocol::Capabilities;

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
                id: "grpc".to_owned(),
                client: "client".to_owned(),
                server: "server".to_owned(),
                stack: Stack::GrpcHttp2,
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
            startup_timeout_ms: 1000,
            drain_timeout_ms: 1000,
        }
    }

    fn capabilities(peer: &str, roles: &[&str], stacks: Vec<Stack>) -> Capabilities {
        Capabilities {
            schema_version: SCHEMA_VERSION,
            event: "capabilities".to_owned(),
            peer: peer.to_owned(),
            roles: roles.iter().map(ToString::to_string).collect(),
            rpc_kinds: vec!["unary".to_owned()],
            stacks,
            histogram: "log_linear_v1".to_owned(),
        }
    }

    #[test]
    fn validates_capabilities_for_each_peers_actual_cell_usage() {
        let campaign = campaign();
        validate_capabilities(
            &campaign,
            &campaign.peers[0],
            &capabilities("client", &["client"], vec![Stack::GrpcHttp2]),
        )
        .expect("client-only peer");
        validate_capabilities(
            &campaign,
            &campaign.peers[1],
            &capabilities("server", &["server"], vec![Stack::GrpcHttp2]),
        )
        .expect("server-only peer");

        let error = validate_capabilities(
            &campaign,
            &campaign.peers[0],
            &capabilities("client", &["client"], vec![Stack::TrevrpcNativeQuic]),
        )
        .expect_err("wrong stack");
        assert!(error.to_string().contains("grpc_http2"));
    }

    #[test]
    fn sample_ids_distinguish_stacks() {
        let mut cell = campaign().cells.remove(0);
        let grpc = sample_id(&cell, RpcKind::Unary, 1, 1);
        cell.stack = Stack::TrevrpcNativeQuic;
        let trevrpc = sample_id(&cell, RpcKind::Unary, 1, 1);
        assert_ne!(grpc, trevrpc);
        assert!(grpc.contains("grpc_http2"));
        assert!(trevrpc.contains("trevrpc_native_quic"));
    }

    #[test]
    fn encodes_sha256_as_lowercase_hex() {
        assert_eq!(
            sha256(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
