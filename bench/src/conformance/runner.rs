use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;

use crate::BoxError;
use crate::conformance::corpus::{
    AllowancePolicy, Case, LoadedSuite, Peer, PeerResolution, sha256_file,
};
use crate::conformance::protocol::{self, ActualResult};
use crate::process::{ManagedChild, OutputLimits, PollLine};

#[derive(Clone, Debug)]
pub struct PeerOverride {
    pub id: String,
    pub executable: PathBuf,
}

#[derive(Debug, Serialize)]
struct Manifest {
    schema_version: u32,
    protocol_version: u32,
    suite_id: String,
    suite_path: String,
    suite_sha256: String,
    golden_path: String,
    golden_sha256: String,
    corpus: Vec<PathHash>,
    source_revision: String,
    source_dirty: bool,
    peer_resolution: PeerResolution,
    allowance_policy: AllowancePolicy,
    limits: LimitsManifest,
    peers: Vec<PeerArtifact>,
}

#[derive(Debug, Serialize)]
struct PathHash {
    path: String,
    sha256: String,
}

#[derive(Debug)]
struct ReplayInputs {
    suite: PathHash,
    golden: PathHash,
    corpus: Vec<PathHash>,
}

#[derive(Debug, Serialize)]
struct LimitsManifest {
    startup_timeout_ms: u64,
    case_timeout_ms: u64,
    shutdown_timeout_ms: u64,
    max_command_bytes: usize,
    max_event_bytes: usize,
    max_stdout_bytes: usize,
    max_stderr_bytes: usize,
    crash_budget: u32,
}

#[derive(Debug, Serialize)]
struct PeerArtifact {
    peer: String,
    executable: String,
    sha256: String,
    argv: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ResultRecord<'a> {
    schema_version: u32,
    peer: &'a str,
    corpus_kind: &'a str,
    case_id: &'a str,
    operation: &'a str,
    attempt: u32,
    sequence: String,
    duration_ns: String,
    status: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expected: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    actual: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_category: Option<&'a str>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct Counts {
    total: usize,
    passed: usize,
    allowed: usize,
    failed: usize,
    skipped: usize,
}

#[derive(Debug, Serialize)]
struct Summary {
    schema_version: u32,
    suite_id: String,
    peers: BTreeMap<String, Counts>,
    overall: Counts,
    clean_shutdown: bool,
}

struct PeerSession {
    child: ManagedChild,
}

pub fn validate(path: &Path) -> Result<LoadedSuite, BoxError> {
    LoadedSuite::load(path)
}

pub fn parse_override(value: &str) -> Result<PeerOverride, BoxError> {
    let (id, executable) = value
        .split_once('=')
        .ok_or("peer override must be ID=/absolute/path")?;
    if id.is_empty() || executable.is_empty() {
        return Err("peer override must be ID=/absolute/path".into());
    }
    let executable = PathBuf::from(executable);
    if !executable.is_absolute() {
        return Err("peer override executable must be absolute".into());
    }
    Ok(PeerOverride {
        id: id.to_owned(),
        executable,
    })
}

pub fn run(
    loaded: &LoadedSuite,
    output: &Path,
    overrides: &[PeerOverride],
) -> Result<(), BoxError> {
    reject_existing_artifacts(output)?;
    let commands = resolved_commands(loaded, overrides)?;
    let replay_inputs = copy_replay_inputs(loaded, output)?;
    fs::create_dir_all(output.join("raw"))?;
    write_manifest(loaded, output, &commands, &replay_inputs)?;
    let mut results = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output.join("results.jsonl"))?;
    let mut per_peer = BTreeMap::new();
    let mut sequence = 0u64;
    let mut clean_shutdown = true;

    for peer in &loaded.suite.peers {
        let command = commands
            .get(&peer.id)
            .ok_or("missing resolved peer command")?;
        let (counts, peer_shutdown) =
            run_peer(loaded, output, peer, command, &mut results, &mut sequence)?;
        clean_shutdown &= peer_shutdown;
        per_peer.insert(peer.id.clone(), counts);
    }
    write_summary(loaded, output, per_peer, clean_shutdown)
}

struct PeerProgress {
    counts: Counts,
    crashes: u32,
    attempt: u32,
    session: Option<PeerSession>,
}

#[derive(Clone, Copy)]
struct CaseContext<'a> {
    peer: &'a Peer,
    corpus_kind: &'a str,
    case: &'a Case,
    sequence: u64,
    loaded: &'a LoadedSuite,
}

fn run_peer(
    loaded: &LoadedSuite,
    output: &Path,
    peer: &Peer,
    command: &[String],
    results: &mut File,
    sequence: &mut u64,
) -> Result<(Counts, bool), BoxError> {
    let mut progress = PeerProgress {
        counts: Counts::default(),
        crashes: 0,
        attempt: 0,
        session: None,
    };
    for corpus in &loaded.corpora {
        for case in &corpus.cases {
            progress.counts.total += 1;
            *sequence = sequence.checked_add(1).ok_or("sequence overflow")?;
            let context = CaseContext {
                peer,
                corpus_kind: &corpus.kind,
                case,
                sequence: *sequence,
                loaded,
            };
            run_peer_case(&mut progress, command, output, results, context)?;
        }
    }
    let clean_shutdown = progress.session.is_none_or(|mut session| {
        shutdown_peer(
            &mut session,
            Duration::from_millis(loaded.suite.shutdown_timeout_ms),
        )
    });
    Ok((progress.counts, clean_shutdown))
}

fn run_peer_case(
    progress: &mut PeerProgress,
    command: &[String],
    output: &Path,
    results: &mut File,
    context: CaseContext<'_>,
) -> Result<(), BoxError> {
    if progress.crashes >= context.loaded.suite.crash_budget {
        progress.counts.skipped += 1;
        let record = case_record(
            context,
            progress.attempt,
            "0".to_owned(),
            "skip",
            Some(context.case.expected_payload(&context.loaded.goldens)?),
            None,
            Some("crash_budget_exhausted"),
        );
        return write_record(results, &record);
    }
    if progress.session.is_none()
        && let Err(error) = start_peer(progress, command, output, context)
    {
        progress.crashes += 1;
        progress.counts.failed += 1;
        let record = failure_record(
            context,
            progress.attempt,
            "process_protocol",
            error.to_string(),
        )?;
        write_record(results, &record)?;
        return Ok(());
    }
    execute_case(progress, results, context)
}

fn start_peer(
    progress: &mut PeerProgress,
    command: &[String],
    output: &Path,
    context: CaseContext<'_>,
) -> Result<(), BoxError> {
    progress.attempt += 1;
    progress.session = Some(spawn_peer(
        context.loaded,
        context.peer,
        command,
        output,
        progress.attempt,
    )?);
    Ok(())
}

fn execute_case(
    progress: &mut PeerProgress,
    results: &mut File,
    context: CaseContext<'_>,
) -> Result<(), BoxError> {
    let started = Instant::now();
    let expected = context.case.expected_payload(&context.loaded.goldens)?;
    let peer_allowance = if context.loaded.suite.allowance_policy == AllowancePolicy::PeerSpecific {
        context.case.peer_allowance(&context.peer.id)?
    } else {
        None
    };
    let outcome = run_case(
        progress.session.as_mut().ok_or("missing peer session")?,
        context.peer,
        context.case,
        context.sequence,
        context.loaded,
    );
    let duration_ns = started.elapsed().as_nanos().to_string();
    match outcome {
        Ok(actual) if actual.payload == expected => {
            progress.counts.passed += 1;
            let record = case_record(
                context,
                progress.attempt,
                duration_ns,
                "pass",
                None,
                None,
                None,
            );
            write_record(results, &record)
        }
        Ok(actual) if peer_allowance.as_ref() == Some(&actual.payload) => {
            progress.counts.allowed += 1;
            let record = case_record(
                context,
                progress.attempt,
                duration_ns,
                "allowed",
                Some(expected),
                Some(actual.payload),
                Some("peer_allowance"),
            );
            write_record(results, &record)
        }
        Ok(actual) => {
            progress.counts.failed += 1;
            let record = case_record(
                context,
                progress.attempt,
                duration_ns,
                "fail",
                Some(expected),
                Some(actual.payload),
                Some("mismatch"),
            );
            write_record(results, &record)
        }
        Err(error) => {
            record_process_failure(progress, results, context, expected, duration_ns, &error)
        }
    }
}

fn record_process_failure(
    progress: &mut PeerProgress,
    results: &mut File,
    context: CaseContext<'_>,
    expected: Value,
    duration_ns: String,
    error: &BoxError,
) -> Result<(), BoxError> {
    progress.crashes += 1;
    progress.counts.failed += 1;
    if let Some(mut failed) = progress.session.take() {
        failed.child.terminate_group();
        let _ = failed.child.wait(Duration::from_millis(
            context.loaded.suite.shutdown_timeout_ms,
        ));
    }
    let record = case_record(
        context,
        progress.attempt,
        duration_ns,
        "fail",
        Some(expected),
        Some(Value::String(error.to_string())),
        Some("process_protocol"),
    );
    write_record(results, &record)
}

fn write_summary(
    loaded: &LoadedSuite,
    output: &Path,
    per_peer: BTreeMap<String, Counts>,
    clean_shutdown: bool,
) -> Result<(), BoxError> {
    let overall = per_peer
        .values()
        .fold(Counts::default(), |mut total, counts| {
            total.total += counts.total;
            total.passed += counts.passed;
            total.allowed += counts.allowed;
            total.failed += counts.failed;
            total.skipped += counts.skipped;
            total
        });
    let summary = Summary {
        schema_version: 1,
        suite_id: loaded.suite.suite_id.clone(),
        peers: per_peer,
        overall,
        clean_shutdown,
    };
    serde_json::to_writer_pretty(File::create(output.join("summary.json"))?, &summary)?;
    if overall.failed == 0 && overall.skipped == 0 && clean_shutdown {
        Ok(())
    } else {
        Err(format!(
            "conformance failed: {} failed, {} skipped, clean_shutdown={clean_shutdown}",
            overall.failed, overall.skipped
        )
        .into())
    }
}

fn shutdown_peer(session: &mut PeerSession, timeout: Duration) -> bool {
    if session.child.send("STOP").is_err() {
        session.child.terminate_group();
    }
    let Ok(status) = session.child.wait(timeout) else {
        return false;
    };
    if !status.success() || session.child.stdout_overflowed() || session.child.stderr_overflowed() {
        return false;
    }
    session
        .child
        .drain_output()
        .is_ok_and(|extra_lines| extra_lines.is_empty())
}

fn run_case(
    session: &mut PeerSession,
    peer: &Peer,
    case: &Case,
    sequence: u64,
    loaded: &LoadedSuite,
) -> Result<ActualResult, BoxError> {
    let fields = case.command_fields(&loaded.goldens)?;
    let command = protocol::command(sequence, case, &fields);
    if command.len() > loaded.suite.max_command_bytes {
        return Err(format!(
            "generated command for {} is {} bytes, exceeding suite limit {}",
            case.id(),
            command.len(),
            loaded.suite.max_command_bytes
        )
        .into());
    }
    session.child.send(&command)?;
    let deadline = Instant::now() + Duration::from_millis(loaded.suite.case_timeout_ms);
    let line = session.child.recv_line(deadline)?;
    if protocol::parse_fatal(&line, &peer.id)? {
        return Err("peer emitted fatal event".into());
    }
    let actual = protocol::parse_result(&line, &peer.id, sequence, case)?;
    if session.child.stdout_overflowed() || session.child.stderr_overflowed() {
        return Err("peer exceeded configured output limits".into());
    }
    match session.child.poll_line()? {
        PollLine::Empty => Ok(actual),
        PollLine::Line(line) => Err(format!("peer emitted extra result/output: {line}").into()),
        PollLine::LineTooLong => Err("peer emitted an overlong extra line".into()),
        PollLine::StreamTooLong => Err("peer stdout exceeded the configured limit".into()),
        PollLine::Disconnected => Err("peer disconnected after result".into()),
    }
}

fn spawn_peer(
    loaded: &LoadedSuite,
    peer: &Peer,
    argv: &[String],
    output: &Path,
    attempt: u32,
) -> Result<PeerSession, BoxError> {
    let raw = output.join("raw").join(&peer.id);
    fs::create_dir_all(&raw)?;
    let stdout = raw.join(format!("attempt-{attempt:04}.stdout"));
    let stderr = raw.join(format!("attempt-{attempt:04}.stderr"));
    let mut command = Command::new(argv.first().ok_or("empty peer command")?);
    command.args(&argv[1..]);
    let mut child = ManagedChild::spawn(
        command,
        &stdout,
        &stderr,
        OutputLimits {
            max_line_bytes: loaded.suite.max_event_bytes,
            max_stdout_bytes: loaded.suite.max_stdout_bytes,
            max_stderr_bytes: loaded.suite.max_stderr_bytes,
            diagnostic_tail_bytes: 8192,
        },
    )?;
    let ready_line =
        child.recv_line(Instant::now() + Duration::from_millis(loaded.suite.startup_timeout_ms))?;
    protocol::parse_ready(&ready_line, &peer.id, &peer.required_capabilities)?;
    if child.stdout_overflowed() || child.stderr_overflowed() {
        return Err("peer exceeded startup output limits".into());
    }
    Ok(PeerSession { child })
}

fn failure_record<'a>(
    context: CaseContext<'a>,
    attempt: u32,
    category: &'a str,
    detail: String,
) -> Result<ResultRecord<'a>, BoxError> {
    Ok(case_record(
        context,
        attempt,
        "0".to_owned(),
        "fail",
        Some(context.case.expected_payload(&context.loaded.goldens)?),
        Some(Value::String(detail)),
        Some(category),
    ))
}

fn case_record<'a>(
    context: CaseContext<'a>,
    attempt: u32,
    duration_ns: String,
    status: &'a str,
    expected: Option<Value>,
    actual: Option<Value>,
    error_category: Option<&'a str>,
) -> ResultRecord<'a> {
    ResultRecord {
        schema_version: 1,
        peer: &context.peer.id,
        corpus_kind: context.corpus_kind,
        case_id: context.case.id(),
        operation: context.case.operation(),
        attempt,
        sequence: context.sequence.to_string(),
        duration_ns,
        status,
        expected,
        actual,
        error_category,
    }
}

fn write_record(file: &mut File, record: &ResultRecord<'_>) -> Result<(), BoxError> {
    serde_json::to_writer(&mut *file, record)?;
    file.write_all(b"\n")?;
    file.flush()?;
    Ok(())
}

fn resolved_commands(
    loaded: &LoadedSuite,
    overrides: &[PeerOverride],
) -> Result<BTreeMap<String, Vec<String>>, BoxError> {
    let known = loaded
        .suite
        .peers
        .iter()
        .map(|peer| peer.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen = BTreeSet::new();
    let mut canonical_overrides = BTreeMap::new();
    for override_ in overrides {
        if !known.contains(override_.id.as_str()) {
            return Err(format!("unknown peer override {:?}", override_.id).into());
        }
        if !seen.insert(override_.id.clone()) {
            return Err(format!("duplicate peer override {:?}", override_.id).into());
        }
        canonical_overrides.insert(
            override_.id.clone(),
            canonical_executable(&override_.executable)?,
        );
    }
    if loaded.suite.peer_resolution == PeerResolution::AbsoluteOverrides
        && seen.len() != known.len()
    {
        let missing = known
            .iter()
            .filter(|peer| !seen.contains::<str>(peer))
            .copied()
            .collect::<Vec<_>>();
        return Err(format!(
            "suite {} requires exactly one absolute override per peer; missing {missing:?}",
            loaded.suite.suite_id
        )
        .into());
    }

    let mut result = BTreeMap::new();
    for peer in &loaded.suite.peers {
        let mut command = peer.command.clone();
        let executable = if let Some(path) = canonical_overrides.get(&peer.id) {
            path.clone()
        } else if loaded.suite.peer_resolution == PeerResolution::AbsoluteOverrides {
            return Err(format!("missing required peer override {:?}", peer.id).into());
        } else {
            resolve_path_executable(command.first().ok_or("empty peer command")?)?
        };
        command[0] = executable.to_string_lossy().into_owned();
        result.insert(peer.id.clone(), command);
    }
    Ok(result)
}

fn reject_existing_artifacts(output: &Path) -> Result<(), BoxError> {
    for name in [
        "inputs",
        "manifest.json",
        "results.jsonl",
        "summary.json",
        "raw",
    ] {
        if output.join(name).exists() {
            return Err(format!(
                "output directory {} already contains conformance artifacts",
                output.display()
            )
            .into());
        }
    }
    Ok(())
}

fn copy_replay_inputs(loaded: &LoadedSuite, output: &Path) -> Result<ReplayInputs, BoxError> {
    let sources = std::iter::once(&loaded.suite_path)
        .chain(std::iter::once(&loaded.golden_path))
        .chain(loaded.corpus_paths.iter())
        .collect::<Vec<_>>();
    let common_root = common_ancestor(&sources)?;

    let copy = |source: &Path| -> Result<PathHash, BoxError> {
        let relative = source.strip_prefix(&common_root).map_err(|_| {
            format!(
                "replay input {} is outside common root {}",
                source.display(),
                common_root.display()
            )
        })?;
        if relative.as_os_str().is_empty() {
            return Err("replay input path must name a file below its common root".into());
        }
        let artifact_relative = Path::new("inputs").join(relative);
        let destination = output.join(&artifact_relative);
        fs::create_dir_all(destination.parent().ok_or("replay input has no parent")?)?;
        fs::copy(source, &destination)?;
        Ok(PathHash {
            path: display(&artifact_relative),
            sha256: sha256_file(&destination)?,
        })
    };

    Ok(ReplayInputs {
        suite: copy(&loaded.suite_path)?,
        golden: copy(&loaded.golden_path)?,
        corpus: loaded
            .corpus_paths
            .iter()
            .map(|path| copy(path))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn common_ancestor(paths: &[&PathBuf]) -> Result<PathBuf, BoxError> {
    let first = paths.first().ok_or("no replay inputs")?;
    for ancestor in first
        .parent()
        .ok_or("replay input has no parent")?
        .ancestors()
    {
        if paths.iter().all(|path| path.starts_with(ancestor)) {
            return Ok(ancestor.to_path_buf());
        }
    }
    Err("replay inputs have no common ancestor".into())
}

fn write_manifest(
    loaded: &LoadedSuite,
    output: &Path,
    commands: &BTreeMap<String, Vec<String>>,
    replay_inputs: &ReplayInputs,
) -> Result<(), BoxError> {
    let peers = loaded
        .suite
        .peers
        .iter()
        .map(|peer| {
            let argv = commands.get(&peer.id).ok_or("missing command")?.clone();
            let executable = PathBuf::from(argv.first().ok_or("empty command")?);
            Ok(PeerArtifact {
                peer: peer.id.clone(),
                executable: display(&executable),
                sha256: sha256_file(&executable)?,
                argv,
            })
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    let (source_revision, source_dirty) = source_identity()?;
    let manifest = Manifest {
        schema_version: loaded.suite.schema_version,
        protocol_version: loaded.suite.protocol_version,
        suite_id: loaded.suite.suite_id.clone(),
        suite_path: replay_inputs.suite.path.clone(),
        suite_sha256: replay_inputs.suite.sha256.clone(),
        golden_path: replay_inputs.golden.path.clone(),
        golden_sha256: replay_inputs.golden.sha256.clone(),
        corpus: replay_inputs
            .corpus
            .iter()
            .map(|entry| PathHash {
                path: entry.path.clone(),
                sha256: entry.sha256.clone(),
            })
            .collect(),
        source_revision,
        source_dirty,
        peer_resolution: loaded.suite.peer_resolution,
        allowance_policy: loaded.suite.allowance_policy,
        limits: LimitsManifest {
            startup_timeout_ms: loaded.suite.startup_timeout_ms,
            case_timeout_ms: loaded.suite.case_timeout_ms,
            shutdown_timeout_ms: loaded.suite.shutdown_timeout_ms,
            max_command_bytes: loaded.suite.max_command_bytes,
            max_event_bytes: loaded.suite.max_event_bytes,
            max_stdout_bytes: loaded.suite.max_stdout_bytes,
            max_stderr_bytes: loaded.suite.max_stderr_bytes,
            crash_budget: loaded.suite.crash_budget,
        },
        peers,
    };
    serde_json::to_writer_pretty(File::create(output.join("manifest.json"))?, &manifest)?;
    Ok(())
}

fn resolve_path_executable(executable: &str) -> Result<PathBuf, BoxError> {
    let path = PathBuf::from(executable);
    if path.is_absolute() {
        return canonical_executable(&path);
    }
    let mut components = path.components();
    if !matches!(
        (components.next(), components.next()),
        (Some(Component::Normal(_)), None)
    ) {
        return Err(format!(
            "relative peer command containing a path separator is forbidden: {executable:?}"
        )
        .into());
    }
    let path_value = std::env::var_os("PATH").ok_or("PATH is not set")?;
    let directories = std::env::split_paths(&path_value).collect::<Vec<_>>();
    if directories
        .iter()
        .any(|directory| directory.as_os_str().is_empty() || !directory.is_absolute())
    {
        return Err("PATH contains an empty or relative entry".into());
    }
    for directory in directories {
        let candidate = directory.join(executable);
        if is_executable_file(&candidate)? {
            return canonical_executable(&candidate);
        }
    }
    Err(format!("peer executable {executable:?} was not found on PATH").into())
}

fn canonical_executable(path: &Path) -> Result<PathBuf, BoxError> {
    let canonical = path.canonicalize().map_err(|error| {
        format!(
            "failed to canonicalize peer executable {}: {error}",
            path.display()
        )
    })?;
    if !is_executable_file(&canonical)? {
        return Err(format!(
            "peer executable {} is not a regular executable file",
            canonical.display()
        )
        .into());
    }
    Ok(canonical)
}

fn is_executable_file(path: &Path) -> Result<bool, BoxError> {
    let metadata = match path.metadata() {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
}

fn source_identity() -> Result<(String, bool), BoxError> {
    match (
        std::env::var("TREVRPC_BENCH_SOURCE_COMMIT"),
        std::env::var("TREVRPC_BENCH_SOURCE_DIRTY"),
    ) {
        (Ok(revision), Ok(dirty)) => {
            if revision.is_empty() {
                return Err("TREVRPC_BENCH_SOURCE_COMMIT must not be empty".into());
            }
            let dirty = match dirty.as_str() {
                "true" => true,
                "false" => false,
                _ => return Err("TREVRPC_BENCH_SOURCE_DIRTY must be true or false".into()),
            };
            Ok((revision, dirty))
        }
        (Err(std::env::VarError::NotPresent), Err(std::env::VarError::NotPresent)) => Ok((
            git_output(["rev-parse", "HEAD"]).unwrap_or_else(|_| "unknown".to_owned()),
            git_output(["status", "--short"]).is_ok_and(|value| !value.is_empty()),
        )),
        _ => Err("source identity environment variables must be set together".into()),
    }
}

fn git_output<const N: usize>(args: [&str; N]) -> Result<String, BoxError> {
    let output = Command::new("git").args(args).output()?;
    if !output.status.success() {
        return Err("git command failed".into());
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}

fn display(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::{Value, json};

    use crate::conformance::corpus::LoadedSuite;

    use super::{PeerOverride, canonical_executable, parse_override, resolved_commands, run};

    static FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn peer_override_must_be_absolute() {
        assert!(parse_override("go=relative").is_err());
        assert!(parse_override("go=/tmp/peer").is_ok());
    }

    #[test]
    fn absolute_override_suites_require_an_exact_executable_set() {
        let suite_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/suites/m3.json");
        let loaded = LoadedSuite::load(&suite_path).expect("load M3 suite");
        let executable = std::env::current_exe().expect("test executable");
        let overrides = loaded
            .suite
            .peers
            .iter()
            .map(|peer| PeerOverride {
                id: peer.id.clone(),
                executable: executable.clone(),
            })
            .collect::<Vec<_>>();
        let commands = resolved_commands(&loaded, &overrides).expect("resolve exact overrides");
        assert_eq!(commands.len(), loaded.suite.peers.len());
        let canonical = executable.canonicalize().expect("canonical executable");
        assert!(
            commands
                .values()
                .all(|argv| Path::new(&argv[0]) == canonical)
        );

        assert!(resolved_commands(&loaded, &overrides[1..]).is_err());
        let mut duplicate = overrides.clone();
        duplicate.push(overrides[0].clone());
        assert!(resolved_commands(&loaded, &duplicate).is_err());
        let mut unknown = overrides;
        unknown.push(PeerOverride {
            id: "unknown".to_owned(),
            executable,
        });
        assert!(resolved_commands(&loaded, &unknown).is_err());
    }

    #[test]
    fn executable_resolution_rejects_non_executable_files() {
        let path = std::env::temp_dir().join(format!(
            "trevrpc-conformance-non-executable-{}",
            std::process::id()
        ));
        fs::write(&path, b"not executable").expect("write fixture");
        assert!(canonical_executable(&path).is_err());
        let _ = fs::remove_file(path);
    }

    #[test]
    fn successful_run_copies_replayable_exact_inputs() {
        let fixture = Fixture::new(1, 1, 200, 4096);
        let peer = fixture.peer(
            r#"printf '{"schema_version":1,"event":"result","peer":"go","sequence":"1","case_id":"test.case-0","operation":"codec.decode","outcome":"error","category":"unsupported_wire_version","status_code":9}\n'"#,
        );
        let output = fixture.root.join("output");
        let loaded = LoadedSuite::load(&fixture.suite).expect("load fixture suite");
        if let Err(error) = run(
            &loaded,
            &output,
            &[PeerOverride {
                id: "go".to_owned(),
                executable: peer,
            }],
        ) {
            panic!(
                "successful conformance run failed: {error}; results={:?}; stdout={:?}; stderr={:?}",
                fs::read_to_string(output.join("results.jsonl")),
                fs::read_to_string(output.join("raw/go/attempt-0001.stdout")),
                fs::read_to_string(output.join("raw/go/attempt-0001.stderr"))
            );
        }

        let manifest: Value =
            serde_json::from_slice(&fs::read(output.join("manifest.json")).expect("read manifest"))
                .expect("parse manifest");
        assert_eq!(
            manifest["suite_path"],
            "inputs/conformance/suites/test.json"
        );
        assert_eq!(
            manifest["golden_path"],
            "inputs/testdata/wire-golden-vectors.txt"
        );
        assert_eq!(
            manifest["corpus"][0]["path"],
            "inputs/conformance/corpus/v1/test.json"
        );
        for (source, relative) in [
            (&fixture.suite, manifest["suite_path"].as_str().unwrap()),
            (&fixture.golden, manifest["golden_path"].as_str().unwrap()),
            (
                &fixture.corpus,
                manifest["corpus"][0]["path"].as_str().unwrap(),
            ),
        ] {
            assert_eq!(
                fs::read(source).expect("read source input"),
                fs::read(output.join(relative)).expect("read copied input")
            );
        }
        LoadedSuite::load(&output.join("inputs/conformance/suites/test.json"))
            .expect("replay copied suite");
    }

    #[test]
    fn stdout_flood_is_bounded_and_fails_the_case() {
        let fixture = Fixture::new(1, 1, 200, 4096);
        let peer =
            fixture.peer(r#"i=0; while [ "$i" -lt 10000 ]; do printf 'x\n'; i=$((i + 1)); done"#);
        let output = fixture.run_failure(&peer);
        let summary = summary(&output);
        assert_eq!(summary["overall"]["failed"], 1);
        let stdout = output.join("raw/go/attempt-0001.stdout");
        assert!(fs::metadata(stdout).expect("stdout metadata").len() <= 4096);
    }

    #[test]
    fn fatal_timeout_and_crash_budget_restart_are_enforced() {
        let fatal = Fixture::new(1, 1, 200, 4096);
        let fatal_peer = fatal.peer(
            r#"printf '{"schema_version":1,"event":"fatal","peer":"go","message":"rejected"}\n'; sleep 10"#,
        );
        let fatal_output = fatal.run_failure(&fatal_peer);
        assert_eq!(summary(&fatal_output)["overall"]["failed"], 1);
        let fatal_results =
            fs::read_to_string(fatal_output.join("results.jsonl")).expect("fatal results");
        assert!(
            fatal_results.contains("peer emitted fatal event"),
            "unexpected fatal result: {fatal_results}"
        );

        let timeout = Fixture::new(1, 1, 50, 4096);
        let timeout_peer = timeout.peer("sleep 10");
        let timeout_output = timeout.run_failure(&timeout_peer);
        assert!(
            fs::read_to_string(timeout_output.join("results.jsonl"))
                .expect("timeout results")
                .contains("receive timed out")
        );

        let restart = Fixture::new(3, 2, 200, 4096);
        let restart_peer = restart.peer(
            r#"printf '{"schema_version":1,"event":"fatal","peer":"go","message":"crash"}\n'; sleep 10"#,
        );
        let restart_output = restart.run_failure(&restart_peer);
        let restart_summary = summary(&restart_output);
        assert_eq!(restart_summary["overall"]["failed"], 2);
        assert_eq!(restart_summary["overall"]["skipped"], 1);
        assert!(restart_output.join("raw/go/attempt-0001.stdout").is_file());
        assert!(restart_output.join("raw/go/attempt-0002.stdout").is_file());
        assert!(!restart_output.join("raw/go/attempt-0003.stdout").exists());
    }

    #[test]
    fn oversized_generated_commands_never_reach_the_peer() {
        let fixture = Fixture::new(1, 1, 200, 8);
        let marker = fixture.root.join("command-received");
        let peer = fixture.peer(&format!("touch {}; exit 2", marker.display()));
        let output = fixture.run_failure(&peer);
        assert!(!marker.exists());
        assert!(
            fs::read_to_string(output.join("results.jsonl"))
                .expect("oversized command results")
                .contains("exceeding suite limit")
        );
    }

    fn summary(output: &Path) -> Value {
        serde_json::from_slice(&fs::read(output.join("summary.json")).expect("read summary"))
            .expect("parse summary")
    }

    struct Fixture {
        root: PathBuf,
        suite: PathBuf,
        corpus: PathBuf,
        golden: PathBuf,
    }

    impl Fixture {
        fn new(
            case_count: usize,
            crash_budget: u32,
            case_timeout_ms: u64,
            max_command_bytes: usize,
        ) -> Self {
            let id = FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "trevrpc-conformance-runner-{}-{id}",
                std::process::id()
            ));
            let suites = root.join("conformance/suites");
            let corpora = root.join("conformance/corpus/v1");
            let testdata = root.join("testdata");
            fs::create_dir_all(&suites).expect("create suite directory");
            fs::create_dir_all(&corpora).expect("create corpus directory");
            fs::create_dir_all(&testdata).expect("create testdata directory");

            let corpus = corpora.join("test.json");
            let cases = (0..case_count)
                .map(|index| {
                    json!({
                        "id": format!("test.case-{index}"),
                        "operation": "codec.decode",
                        "message_type": "rpc_request",
                        "body_hex": "0a0373766312016d1a0268693002",
                        "expected_error": {
                            "category": "unsupported_wire_version",
                            "status_code": 9
                        }
                    })
                })
                .collect::<Vec<_>>();
            fs::write(
                &corpus,
                serde_json::to_vec_pretty(&json!({
                    "schema_version": 1,
                    "kind": "test",
                    "cases": cases
                }))
                .expect("serialize corpus"),
            )
            .expect("write corpus");

            let golden = testdata.join("wire-golden-vectors.txt");
            fs::write(&golden, "# unused by this fixture\n").expect("write golden file");
            let suite = suites.join("test.json");
            fs::write(
                &suite,
                serde_json::to_vec_pretty(&json!({
                    "schema_version": 1,
                    "suite_id": "test",
                    "protocol_version": 1,
                    "peer_resolution": "absolute_overrides",
                    "allowance_policy": "forbid",
                    "startup_timeout_ms": 200,
                    "case_timeout_ms": case_timeout_ms,
                    "shutdown_timeout_ms": 200,
                    "max_command_bytes": max_command_bytes,
                    "max_event_bytes": 512,
                    "max_stdout_bytes": 4096,
                    "max_stderr_bytes": 4096,
                    "crash_budget": crash_budget,
                    "golden_path": "../../testdata/wire-golden-vectors.txt",
                    "corpus_paths": ["../corpus/v1/test.json"],
                    "peers": [{
                        "id": "go",
                        "command": ["trevrpc-conformance-go", "--protocol", "1"],
                        "required_capabilities": [
                            "codec.decode",
                            "codec.encode",
                            "framing.decode_stream",
                            "framing.encode",
                            "state.client_stream",
                            "state.server_stream"
                        ]
                    }]
                }))
                .expect("serialize suite"),
            )
            .expect("write suite");
            Self {
                root,
                suite,
                corpus,
                golden,
            }
        }

        fn peer(&self, action: &str) -> PathBuf {
            let path = self.root.join(format!(
                "peer-{}",
                FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
            ));
            let script = format!(
                "#!/bin/sh\nprintf '{{\"schema_version\":1,\"event\":\"ready\",\"peer\":\"go\",\"pid\":%s,\"capabilities\":[\"codec.decode\",\"codec.encode\",\"framing.decode_stream\",\"framing.encode\",\"state.client_stream\",\"state.server_stream\"]}}\\n' \"$$\"\nwhile IFS='\t' read -r run sequence case_id operation rest; do\n  if [ \"$run\" = STOP ]; then exit 0; fi\n  {action}\ndone\n"
            );
            fs::write(&path, script).expect("write peer");
            let mut permissions = fs::metadata(&path).expect("peer metadata").permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).expect("make peer executable");
            path
        }

        fn run_failure(&self, peer: &Path) -> PathBuf {
            let output = self.root.join(format!(
                "output-{}",
                FIXTURE_ID.fetch_add(1, Ordering::Relaxed)
            ));
            let loaded = LoadedSuite::load(&self.suite).expect("load fixture suite");
            run(
                &loaded,
                &output,
                &[PeerOverride {
                    id: "go".to_owned(),
                    executable: peer.to_path_buf(),
                }],
            )
            .expect_err("malicious peer must fail conformance");
            output
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
