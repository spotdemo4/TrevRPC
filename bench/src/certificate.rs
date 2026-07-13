use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::BoxError;

#[derive(Clone, Debug)]
pub struct Certificates {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

pub fn generate(output: &Path) -> Result<Certificates, BoxError> {
    let directory = output.join("certificates");
    fs::create_dir_all(&directory)?;
    let ca = directory.join("ca.pem");
    let ca_key = directory.join("ca-key.pem");
    let csr = directory.join("server.csr");
    let certificate = directory.join("server.pem");
    let private_key = directory.join("server-key.pem");

    run(
        Command::new("openssl")
            .args(["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&ca_key)
            .arg("-out")
            .arg(&ca)
            .args([
                "-days",
                "1",
                "-subj",
                "/CN=TrevRPC Benchmark CA",
                "-addext",
                "basicConstraints=critical,CA:TRUE",
                "-addext",
                "keyUsage=critical,keyCertSign,cRLSign",
            ]),
        "generate benchmark CA",
    )?;
    run(
        Command::new("openssl")
            .args(["req", "-newkey", "rsa:2048", "-nodes", "-keyout"])
            .arg(&private_key)
            .arg("-out")
            .arg(&csr)
            .args([
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
            ]),
        "generate benchmark server CSR",
    )?;
    run(
        Command::new("openssl")
            .args(["x509", "-req", "-in"])
            .arg(&csr)
            .arg("-CA")
            .arg(&ca)
            .arg("-CAkey")
            .arg(&ca_key)
            .arg("-CAcreateserial")
            .arg("-out")
            .arg(&certificate)
            .args(["-days", "1", "-copy_extensions", "copyall"]),
        "sign benchmark server certificate",
    )?;
    Ok(Certificates {
        ca,
        certificate,
        private_key,
    })
}

fn run(command: &mut Command, description: &str) -> Result<(), BoxError> {
    let output = command.output()?;
    if !output.status.success() {
        return Err(format!(
            "failed to {description}: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}
