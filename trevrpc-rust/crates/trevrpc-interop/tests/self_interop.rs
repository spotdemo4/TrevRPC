//! Runs the built `server`, `client` and `bench` subcommands against each other, checking the
//! command-line contracts in `interop/README.md` and `bench/README.md`.

// Test helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_trevrpc-interop-rust");
const SCENARIOS: [&str; 11] = [
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
];

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
    let report = String::from_utf8(client.stdout).unwrap();
    assert!(client.status.success(), "client failed:\n{report}");
    for scenario in SCENARIOS {
        assert!(
            report.lines().any(|line| line == format!("ok {scenario}")),
            "scenario {scenario} did not pass:\n{report}"
        );
    }
    server.finish();
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
fn bench_reports_every_scenario_over_both_transports() {
    let server = Server::start("bench");
    for (transport, name) in [("quic", "quic"), ("tcp", "qmux")] {
        for scenario in ["unary", "server-stream", "client-stream", "bidi"] {
            let output = server.run(
                "bench",
                &[
                    "--transport",
                    transport,
                    "--scenario",
                    scenario,
                    "--size",
                    "32",
                    "--concurrency",
                    "2",
                    "--duration",
                    "0.2",
                    "--warmup",
                    "0.05",
                    "--messages",
                    "5",
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
            assert_close(number(json, "msgs"), ops * per_op, 1e-9, json);
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
fn bench_rejects_unusable_flags() {
    let server = Server::start("bench-flags");
    for flags in [
        &["--scenario", "nope"][..],
        &["--scenario", "unary", "--transport", "grpc"],
        &["--scenario", "unary", "--concurrency", "0"],
    ] {
        let output = server.run("bench", flags);
        assert!(!output.status.success(), "{flags:?} should fail");
        assert!(output.stdout.is_empty(), "{flags:?} printed to stdout");
    }
    server.finish();
}
