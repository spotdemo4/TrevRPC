use std::io;
use std::path::PathBuf;

pub(super) fn certificate_path() -> io::Result<PathBuf> {
    if let Some(path) = std::env::var_os("TREVRPC_EXAMPLE_CERT") {
        return Ok(PathBuf::from(path));
    }

    let home = std::env::var_os("HOME")
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "HOME is not set"))?;

    Ok(PathBuf::from(home)
        .join(".config")
        .join("trevrpc")
        .join("trevrpc-example-cert.pem"))
}
