//! Runs the built `server` and `client` subcommands against each other, checking the
//! command-line contract in `interop/README.md`.

use std::error::Error;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Stdio};

const BIN: &str = env!("CARGO_BIN_EXE_trevrpc-interop-rust");
const SCENARIOS: [&str; 9] = [
    "unary",
    "unary-metadata",
    "unary-error",
    "unary-large",
    "client-stream",
    "server-stream",
    "server-stream-error",
    "bidi",
    "bidi-error",
];

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!("trevrpc-interop-{}", std::process::id()));
        std::fs::create_dir_all(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn rust_client_passes_every_scenario_against_rust_server() -> Result<(), Box<dyn Error>> {
    let dir = TempDir::new()?;
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let cert_path = dir.0.join("cert.pem");
    let key_path = dir.0.join("key.pem");
    std::fs::write(&cert_path, cert.cert.pem())?;
    std::fs::write(&key_path, cert.signing_key.serialize_pem())?;

    // The server runs until its stdin closes, so keep the pipe open for the whole test.
    let mut server = Command::new(BIN)
        .arg("server")
        .arg("--cert")
        .arg(&cert_path)
        .arg("--key")
        .arg(&key_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut ports = String::new();
    BufReader::new(server.stdout.take().ok_or("server stdout")?).read_line(&mut ports)?;
    let native = ports
        .split("\"native\":")
        .nth(1)
        .and_then(|rest| rest.split([',', '}']).next())
        .ok_or_else(|| format!("unexpected port line {ports:?}"))?
        .trim()
        .to_owned();
    assert!(ports.contains("\"http3\":") && ports.contains("\"webtransport\":"));

    let client = Command::new(BIN)
        .arg("client")
        .arg("--addr")
        .arg(format!("127.0.0.1:{native}"))
        .arg("--ca")
        .arg(&cert_path)
        .output()?;
    let report = String::from_utf8(client.stdout)?;
    assert!(client.status.success(), "client failed:\n{report}");
    for scenario in SCENARIOS {
        assert!(
            report.lines().any(|line| line == format!("ok {scenario}")),
            "scenario {scenario} did not pass:\n{report}"
        );
    }

    // Closing stdin asks the server to shut down gracefully.
    drop(server.stdin.take());
    assert!(server.wait()?.success());
    Ok(())
}
