use std::fs;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::BoxError;

#[derive(Clone, Debug)]
pub struct Certificates {
    pub ca: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

pub fn generate(output: &Path, server_ips: &[IpAddr]) -> Result<Certificates, BoxError> {
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
    let subject_alt_name = subject_alt_name(server_ips);
    let mut request = Command::new("openssl");
    request
        .args([
            "req",
            "-new",
            "-newkey",
            "ec",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-nodes",
            "-keyout",
        ])
        .arg(&private_key)
        .arg("-out")
        .arg(&csr)
        .args(["-subj", "/CN=localhost", "-addext"])
        .arg(format!("subjectAltName={subject_alt_name}"));
    run(&mut request, "generate benchmark server CSR")?;
    let extfile = directory.join("server.ext");
    let ext_content = format!(
        "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\nsubjectAltName={subject_alt_name}\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n"
    );
    fs::write(&extfile, ext_content)?;
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
            .args(["-days", "1", "-extfile"])
            .arg(&extfile),
        "sign benchmark server certificate",
    )?;
    Ok(Certificates {
        ca,
        certificate,
        private_key,
    })
}

fn subject_alt_name(server_ips: &[IpAddr]) -> String {
    std::iter::once("DNS:localhost".to_owned())
        .chain(server_ips.iter().map(|address| format!("IP:{address}")))
        .collect::<Vec<_>>()
        .join(",")
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

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::subject_alt_name;

    #[test]
    fn subject_alt_name_includes_localhost_and_every_server_ip() {
        assert_eq!(
            subject_alt_name(&[
                IpAddr::V4(Ipv4Addr::new(10, 0, 2, 2)),
                IpAddr::V6(Ipv6Addr::LOCALHOST),
            ]),
            "DNS:localhost,IP:10.0.2.2,IP:::1"
        );
    }
}
