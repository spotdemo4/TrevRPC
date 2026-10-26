//! Runs the built `server`, `client` and `bench` subcommands against each other, checking the
//! command-line contracts in `interop/README.md` and `bench/README.md`.

// Test helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_trevrpc-interop-rust");
const SCENARIOS: [&str; 17] = [
    "unary",
    "unary-metadata",
    "unary-error",
    "unary-large",
    "client-stream",
    "server-stream",
    "server-stream-error",
    "bidi",
    "bidi-error",
    "unary-headers",
    "server-stream-headers",
    "unreliable",
    "compressed",
    "compressed-stream",
    "refused-stream",
    "compressed-plain",
    "connection-metadata",
];

/// Writes the compression dictionary of the shared golden vectors into `dir`, as `interop/run.py`
/// does, and returns its path.
fn write_dictionary(dir: &std::path::Path) -> PathBuf {
    let hex = include_str!("../../../../testdata/zstd-golden-vectors.txt")
        .lines()
        .find_map(|line| line.strip_prefix("dict.d1.dictionary = "))
        .expect("the golden vectors have a dictionary");
    let dictionary: Vec<u8> = hex
        .as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    let path = dir.join("interop.dict");
    std::fs::write(&path, dictionary).unwrap();
    path
}

/// A running `server` subcommand with its certificate.
struct Server {
    process: Child,
    dir: PathBuf,
    cert: PathBuf,
    native: String,
    qmux: String,
}

impl Server {
    /// Starts a server that runs until its stdin closes, which `finish` or a drop does.
    fn start(name: &str) -> Self {
        Self::start_with(name, &[])
    }

    /// Starts a server like [`Server::start`] with `flags` added to its command line.
    fn start_with(name: &str, flags: &[&std::ffi::OsStr]) -> Self {
        let dir =
            std::env::temp_dir().join(format!("trevrpc-interop-{}-{name}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let cert_path = dir.join("cert.pem");
        let key_path = dir.join("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();

        let mut process = Command::new(BIN)
            .arg("server")
            .arg("--cert")
            .arg(&cert_path)
            .arg("--key")
            .arg(&key_path)
            .args(flags)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut ports = String::new();
        BufReader::new(process.stdout.take().expect("server stdout"))
            .read_line(&mut ports)
            .unwrap();
        assert!(ports.contains("\"http3\":") && ports.contains("\"webtransport\":"));
        let native = port(&ports, "native");
        let qmux = port(&ports, "qmux");
        Self {
            process,
            dir,
            cert: cert_path,
            native,
            qmux,
        }
    }

    /// Runs a subcommand against the server with the connection flags every client takes.
    fn run(&self, subcommand: &str, flags: &[&str]) -> Output {
        Command::new(BIN)
            .arg(subcommand)
            .arg("--addr")
            .arg(format!("127.0.0.1:{}", self.native))
            .arg("--tcp-addr")
            .arg(format!("127.0.0.1:{}", self.qmux))
            .arg("--ca")
            .arg(&self.cert)
            .args(flags)
            .output()
            .unwrap()
    }

    /// Closes stdin to ask the server to shut down gracefully.
    fn finish(mut self) {
        drop(self.process.stdin.take());
        assert!(self.process.wait().unwrap().success());
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn port(line: &str, name: &str) -> String {
    line.split(&format!("\"{name}\":"))
        .nth(1)
        .and_then(|rest| rest.split([',', '}']).next())
        .unwrap_or_else(|| panic!("no {name} port in {line:?}"))
        .trim()
        .to_owned()
}

#[test]
fn rust_client_passes_every_scenario_against_rust_server() {
    let server = Server::start("client");
    let client = server.run("client", &[]);
    assert_passed(&client);
    server.finish();
}

#[test]
fn every_scenario_passes_with_compression_on_both_sides() {
    let dir = std::env::temp_dir().join(format!("trevrpc-interop-{}-dict", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dictionary = write_dictionary(&dir);
    let server = Server::start_with(
        "compressed",
        &["--dictionary".as_ref(), dictionary.as_os_str()],
    );
    let client = server.run("client", &["--dictionary", dictionary.to_str().unwrap()]);
    assert_passed(&client);
    server.finish();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Asserts that a client run passed every scenario.
fn assert_passed(client: &Output) {
    let report = String::from_utf8_lossy(&client.stdout);
    assert!(client.status.success(), "client failed:\n{report}");
    for scenario in SCENARIOS {
        assert!(
            report.lines().any(|line| line == format!("ok {scenario}")),
            "scenario {scenario} did not pass:\n{report}"
        );
    }
}

fn assert_close(actual: f64, expected: f64, tolerance: f64, json: &str) {
    assert!(
        (actual - expected).abs() <= tolerance,
        "{actual} is not within {tolerance} of {expected} in {json}"
    );
}

/// The number after `"name":` in a flat JSON line, searching the whole line.
fn number(json: &str, name: &str) -> f64 {
    json.split(&format!("\"{name}\":"))
        .nth(1)
        .and_then(|rest| rest.split([',', '}']).next())
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or_else(|| panic!("no numeric {name} in {json}"))
}

#[test]
#[allow(clippy::too_many_lines)]
fn bench_reports_every_scenario_over_both_transports() {
    let server = Server::start("bench");
    for (transport, name, metadata_size) in [("quic", "quic", "0"), ("tcp", "qmux", "1024")] {
        for scenario in [
            "unary",
            "server-stream",
            "client-stream",
            "bidi",
            "unreliable-server-stream",
            "unreliable-client-stream",
            "paced",
            "unreliable-paced",
        ] {
            let output = server.run(
                "bench",
                &[
                    "--transport",
                    transport,
                    "--scenario",
                    scenario,
                    "--size",
                    "32",
                    "--metadata-size",
                    metadata_size,
                    "--concurrency",
                    "2",
                    "--duration",
                    "0.2",
                    "--warmup",
                    "0.05",
                    "--messages",
                    "5",
                    "--rate",
                    "1000",
                ],
            );
            let stdout = String::from_utf8(output.stdout).unwrap();
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                output.status.success(),
                "{name} {scenario} failed: {stderr}"
            );
            let mut lines = stdout.lines();
            let json = lines.next().expect("one JSON line");
            assert!(lines.next().is_none(), "more than one line:\n{stdout}");

            assert!(
                json.contains(&format!("\"scenario\":\"{scenario}\"")),
                "{json}"
            );
            assert!(
                json.contains(&format!("\"transport\":\"{name}\"")),
                "{json}"
            );
            assert_close(number(json, "size"), 32.0, 1e-9, json);
            assert_close(
                number(json, "metadata_size"),
                metadata_size.parse().unwrap(),
                1e-9,
                json,
            );
            assert_close(number(json, "concurrency"), 2.0, 1e-9, json);
            assert_close(number(json, "duration_s"), 0.2, 1e-9, json);
            assert_close(number(json, "errors"), 0.0, 1e-9, json);
            let ops = number(json, "ops");
            assert!(ops > 0.0, "{json}");
            let per_op = if scenario.ends_with("stream") {
                5.0
            } else {
                1.0
            };
            let lost = number(json, "lost");
            if scenario.ends_with("paced") {
                // Each operation is one delivered message. Two workers at 1000 messages per
                // second schedule about 400 in the window, each either delivered or lost, and
                // a stream loses none.
                let msgs = number(json, "msgs");
                assert_close(msgs, ops, 1e-9, json);
                assert_close(msgs + lost, 400.0, 100.0, json);
                if transport == "tcp" || scenario == "paced" {
                    assert_close(lost, 0.0, 1e-9, json);
                }
                assert_close(number(json, "rate"), 1000.0, 1e-9, json);
            } else if json.contains("\"rate\"") {
                panic!("only the paced scenarios report a rate: {json}");
            } else if scenario.starts_with("unreliable") && transport == "quic" {
                // Datagram messages may be lost, but each is either delivered or counted as lost.
                assert_close(number(json, "msgs") + lost, ops * per_op, 1e-9, json);
                assert!(number(json, "msgs") > 0.0, "{json}");
            } else {
                // Everything else is reliable, including unreliable methods over QMux.
                assert_close(number(json, "msgs"), ops * per_op, 1e-9, json);
                assert_close(lost, 0.0, 1e-9, json);
            }
            let msgs_per_sec = number(json, "msgs_per_sec");
            assert!(msgs_per_sec > 0.0, "{json}");
            assert!(number(json, "ops_per_sec") > 0.0, "{json}");
            // `bytes_per_sec` is `msgs_per_sec` times the size, up to rounding to one decimal.
            assert_close(
                number(json, "bytes_per_sec"),
                msgs_per_sec * 32.0,
                4.0,
                json,
            );
            let (p50, p90, p99, p999) = (
                number(json, "p50"),
                number(json, "p90"),
                number(json, "p99"),
                number(json, "p999"),
            );
            assert!(
                0.0 < p50 && p50 <= p90 && p90 <= p99 && p99 <= p999,
                "{json}"
            );
            // Only the QUIC connection exposes its wire statistics.
            assert_eq!(
                json.contains("wire_bytes_per_msg"),
                transport == "quic",
                "{json}"
            );
            if transport == "quic" {
                assert!(number(json, "wire_bytes_per_msg") > 0.0, "{json}");
            }
        }
    }
    server.finish();
}

#[test]
fn bench_metadata_size_pads_every_call() {
    let server = Server::start("bench-metadata");
    let wire_bytes_per_msg = |metadata_size: &str, scope: &str| {
        let output = server.run(
            "bench",
            &[
                "--transport",
                "quic",
                "--scenario",
                "unary",
                "--concurrency",
                "1",
                "--duration",
                "0.3",
                "--warmup",
                "0.05",
                "--metadata-size",
                metadata_size,
                "--metadata-scope",
                scope,
            ],
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{metadata_size} {scope}: {stderr}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let json = stdout.lines().next().expect("one JSON line");
        assert_close(number(json, "errors"), 0.0, 1e-9, json);
        assert_close(
            number(json, "metadata_size"),
            metadata_size.parse().unwrap(),
            1e-9,
            json,
        );
        assert!(
            json.contains(&format!(r#""metadata_scope":"{scope}""#)),
            "{json}"
        );
        number(json, "wire_bytes_per_msg")
    };

    let bare = wire_bytes_per_msg("0", "call");
    let padded = wire_bytes_per_msg("2000", "call");
    let connection = wire_bytes_per_msg("2000", "connection");

    // The padding rides in every request, so each call sends at least its bytes more.
    assert!(
        padded - bare >= 2000.0,
        "wire bytes per message: {bare} bare, {padded} padded"
    );
    // As connection metadata, it crossed the connection before the measured window.
    assert!(
        connection - bare < 50.0,
        "wire bytes per message: {bare} bare, {connection} with connection metadata"
    );
    server.finish();
}

#[test]
fn bench_spreads_workers_over_connections() {
    let server = Server::start("bench-connections");
    for transport in ["quic", "tcp"] {
        let output = server.run(
            "bench",
            &[
                "--transport",
                transport,
                "--scenario",
                "unary",
                "--concurrency",
                "6",
                "--connections",
                "3",
                "--duration",
                "0.3",
                "--warmup",
                "0.05",
            ],
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{transport}: {stderr}");
        let stdout = String::from_utf8(output.stdout).unwrap();
        let json = stdout.lines().next().expect("one JSON line");
        assert_close(number(json, "connections"), 3.0, 1e-9, json);
        assert_close(number(json, "errors"), 0.0, 1e-9, json);
        assert!(number(json, "ops") > 0.0, "{json}");
    }
    server.finish();
}

#[test]
fn bench_rejects_unusable_flags() {
    let server = Server::start("bench-flags");
    for flags in [
        &["--scenario", "nope"][..],
        &["--scenario", "unary", "--transport", "grpc"],
        &["--scenario", "unary", "--concurrency", "0"],
        &["--scenario", "unary", "--metadata-size", "-1"],
        &["--scenario", "unary", "--metadata-size", "lots"],
        // One byte over the 8 KiB metadata value limit.
        &["--scenario", "unary", "--metadata-size", "8193"],
        &["--scenario", "unary", "--metadata-scope", "stream"],
        &["--scenario", "unary", "--connections", "0"],
        &[
            "--scenario",
            "unary",
            "--concurrency",
            "2",
            "--connections",
            "3",
        ],
        &["--scenario", "unary", "--payload", "zeros"],
        &["--scenario", "unary", "--compression", "gzip"],
        // No compression runs on server-stream or the datagram scenarios.
        &["--scenario", "server-stream", "--compression", "stream"],
        &[
            "--scenario",
            "unreliable-client-stream",
            "--compression",
            "stream",
        ],
        &["--scenario", "unreliable-paced", "--compression", "stream"],
        &["--scenario", "unreliable-paced", "--rate", "0"],
        &["--scenario", "unreliable-paced", "--rate", "-5"],
        // The dictionary modes need --dictionary, and only they take it.
        &["--scenario", "bidi", "--compression", "dictionary"],
        &["--scenario", "bidi", "--dictionary", "none.dict"],
    ] {
        let output = server.run("bench", flags);
        assert!(!output.status.success(), "{flags:?} should fail");
        assert!(output.stdout.is_empty(), "{flags:?} printed to stdout");
    }
    server.finish();
}

/// Runs `bench` with `flags` and the records payload, and returns its JSON line.
fn bench_json(server: &Server, flags: &[&str]) -> String {
    let mut args = vec![
        "--payload",
        "records",
        "--size",
        "512",
        "--concurrency",
        "2",
        "--duration",
        "0.3",
        "--warmup",
        "0.05",
        "--messages",
        "5",
    ];
    args.extend_from_slice(flags);
    let output = server.run("bench", &args);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{flags:?} failed: {stderr}");
    let stdout = String::from_utf8(output.stdout).unwrap();
    let json = stdout.lines().next().expect("one JSON line").to_owned();
    assert_close(number(&json, "errors"), 0.0, 1e-9, &json);
    assert!(number(&json, "ops") > 0.0, "{json}");
    json
}

#[test]
fn bench_runs_every_compression_mode_with_the_records_payload() {
    let dir = std::env::temp_dir().join(format!("trevrpc-bench-dict-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let dictionary = write_dictionary(&dir);
    let server = Server::start_with(
        "bench-compression",
        &["--dictionary".as_ref(), dictionary.as_os_str()],
    );
    let dictionary = dictionary.to_str().unwrap();
    for transport in ["quic", "tcp"] {
        for (compression, scenarios) in [
            ("none", &["unary", "client-stream", "bidi"][..]),
            ("dictionary", &["unary", "client-stream", "bidi"]),
            ("stream", &["unary", "client-stream", "bidi"]),
            ("stream-dictionary", &["unary", "client-stream", "bidi"]),
        ] {
            for scenario in scenarios {
                let mut flags = vec![
                    "--transport",
                    transport,
                    "--scenario",
                    scenario,
                    "--compression",
                    compression,
                ];
                if compression.ends_with("dictionary") {
                    flags.extend(["--dictionary", dictionary]);
                }
                let json = bench_json(&server, &flags);
                assert!(json.contains(r#""payload":"records""#), "{json}");
                assert!(
                    json.contains(&format!(r#""compression":"{compression}""#)),
                    "{json}"
                );
            }
        }
    }

    // Each records message differs, but their keys and values repeat, which a stream context
    // learns: it carries far fewer bytes than the plain messages.
    let wire = |compression: &str| {
        number(
            &bench_json(
                &server,
                &[
                    "--transport",
                    "quic",
                    "--scenario",
                    "bidi",
                    "--compression",
                    compression,
                ],
            ),
            "wire_bytes_per_msg",
        )
    };
    let (plain, stream) = (wire("none"), wire("stream"));
    assert!(
        stream < plain * 0.6,
        "wire bytes per round trip: {plain} plain, {stream} stream-compressed"
    );
    server.finish();
    let _ = std::fs::remove_dir_all(&dir);
}
