//! Cross-runtime interop executable; see `interop/README.md` for the contract.
//!
//! ```text
//! trevrpc-interop-rust server --cert <pem> --key <pem> [--host 127.0.0.1] [--port 0]
//! trevrpc-interop-rust client --addr <host:port> --ca <pem> [--server-name localhost]
//!                             [--transport quic|tcp|auto] [--tcp-addr <host:port>]
//!                             [--expect-transport quic|qmux]
//! ```

#![forbid(unsafe_code)]

mod client;
mod proto;
mod server;

use std::collections::HashMap;
use std::error::Error;
use std::process::ExitCode;

type BoxError = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let result = match args.next().as_deref() {
        Some("server") => parse_flags(args, &["cert", "key", "host", "port"]).map(Command::Server),
        Some("client") => parse_flags(
            args,
            &[
                "addr",
                "ca",
                "server-name",
                "transport",
                "tcp-addr",
                "expect-transport",
            ],
        )
        .map(Command::Client),
        _ => Err("usage: trevrpc-interop-rust (server|client) [--flag value]...".into()),
    };
    let command = match result {
        Ok(command) => command,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };

    match command {
        Command::Server(flags) => match server::run(&flags).await {
            // The stdin reader runs on a blocking thread that cannot be cancelled, so a signal
            // shutdown would otherwise wait for stdin to close before the runtime drops.
            Ok(()) => std::process::exit(0),
            Err(error) => failure(&error),
        },
        Command::Client(flags) => match client::run(&flags).await {
            Ok(true) => ExitCode::SUCCESS,
            Ok(false) => ExitCode::FAILURE,
            Err(error) => failure(&error),
        },
    }
}

fn failure(error: &BoxError) -> ExitCode {
    eprintln!("error: {error}");
    ExitCode::FAILURE
}

enum Command {
    Server(Flags),
    Client(Flags),
}

/// Parsed `--name value` flags.
pub struct Flags(HashMap<&'static str, String>);

impl Flags {
    pub fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }

    /// # Errors
    ///
    /// Returns an error when the flag was not passed.
    pub fn require(&self, name: &str) -> Result<&str, BoxError> {
        self.get(name)
            .ok_or_else(|| format!("missing required flag --{name}").into())
    }
}

fn parse_flags(
    mut args: impl Iterator<Item = String>,
    allowed: &[&'static str],
) -> Result<Flags, BoxError> {
    let mut flags = HashMap::new();
    while let Some(arg) = args.next() {
        let name = arg
            .strip_prefix("--")
            .and_then(|name| allowed.iter().find(|allowed| **allowed == name))
            .ok_or_else(|| format!("unknown argument {arg}"))?;
        let value = args
            .next()
            .ok_or_else(|| format!("flag --{name} needs a value"))?;
        flags.insert(*name, value);
    }
    Ok(Flags(flags))
}
