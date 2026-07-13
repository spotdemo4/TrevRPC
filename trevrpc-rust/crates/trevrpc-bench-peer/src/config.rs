use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

pub(crate) const MAX_APPLICATION_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const MAX_ENCODED_FRAME_BYTES: usize = MAX_APPLICATION_PAYLOAD_BYTES + 1024;
pub(crate) const MAX_BENCHMARK_CONCURRENCY: usize = 1024;
pub(crate) const MAX_MESSAGES_PER_STREAM: u32 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RpcKind {
    Unary,
    ClientStream,
    ServerStream,
    Bidi,
}

impl RpcKind {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Unary => "unary",
            Self::ClientStream => "client_stream",
            Self::ServerStream => "server_stream",
            Self::Bidi => "bidi",
        }
    }
}

impl FromStr for RpcKind {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "unary" => Ok(Self::Unary),
            "client_stream" => Ok(Self::ClientStream),
            "server_stream" => Ok(Self::ServerStream),
            "bidi" => Ok(Self::Bidi),
            _ => Err(ConfigError(format!(
                "invalid --rpc value {value:?}; expected unary, client_stream, server_stream, or bidi"
            ))),
        }
    }
}

#[derive(Debug)]
pub(crate) enum Command {
    Capabilities,
    Server(ServerConfig),
    Client(ClientConfig),
}

#[derive(Debug)]
pub(crate) struct ServerConfig {
    pub(crate) listen: SocketAddr,
    pub(crate) certificate: PathBuf,
    pub(crate) private_key: PathBuf,
}

#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub(crate) address: SocketAddr,
    pub(crate) certificate: PathBuf,
    pub(crate) rpc: RpcKind,
    pub(crate) concurrency: usize,
    pub(crate) warmup_ms: u64,
    pub(crate) measurement_ms: u64,
    pub(crate) request_bytes: usize,
    pub(crate) response_bytes: u32,
    pub(crate) messages_per_stream: u32,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct ConfigError(String);

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for ConfigError {}

pub(crate) fn parse(args: impl IntoIterator<Item = String>) -> Result<Command, ConfigError> {
    let mut args = args.into_iter();
    let command = args.next().ok_or_else(usage)?;
    let options = parse_options(args)?;

    match command.as_str() {
        "capabilities" => {
            if options.is_empty() {
                Ok(Command::Capabilities)
            } else {
                Err(ConfigError(
                    "capabilities does not accept options".to_owned(),
                ))
            }
        }
        "server" => parse_server(options).map(Command::Server),
        "client" => parse_client(options).map(Command::Client),
        _ => Err(usage()),
    }
}

fn usage() -> ConfigError {
    ConfigError("usage: trevrpc-bench-peer-rust capabilities|server|client [options]".to_owned())
}

fn parse_options(
    mut args: impl Iterator<Item = String>,
) -> Result<BTreeMap<String, String>, ConfigError> {
    let mut options = BTreeMap::new();
    while let Some(name) = args.next() {
        if !name.starts_with("--") || name.len() == 2 {
            return Err(ConfigError(format!("unexpected argument {name:?}")));
        }
        let value = args
            .next()
            .ok_or_else(|| ConfigError(format!("missing value for {name}")))?;
        if options.insert(name.clone(), value).is_some() {
            return Err(ConfigError(format!("duplicate option {name}")));
        }
    }
    Ok(options)
}

fn parse_server(mut options: BTreeMap<String, String>) -> Result<ServerConfig, ConfigError> {
    let config = ServerConfig {
        listen: take_parsed(&mut options, "--listen")?,
        certificate: PathBuf::from(take(&mut options, "--cert")?),
        private_key: PathBuf::from(take(&mut options, "--key")?),
    };
    reject_unknown(&options)?;
    Ok(config)
}

fn parse_client(mut options: BTreeMap<String, String>) -> Result<ClientConfig, ConfigError> {
    let config = ClientConfig {
        address: take_parsed(&mut options, "--address")?,
        certificate: PathBuf::from(take(&mut options, "--cert")?),
        rpc: take_parsed(&mut options, "--rpc")?,
        concurrency: take_positive(&mut options, "--concurrency")?,
        warmup_ms: take_parsed(&mut options, "--warmup-ms")?,
        measurement_ms: take_positive(&mut options, "--measurement-ms")?,
        request_bytes: take_parsed(&mut options, "--request-bytes")?,
        response_bytes: take_parsed(&mut options, "--response-bytes")?,
        messages_per_stream: take_positive(&mut options, "--messages-per-stream")?,
    };
    reject_unknown(&options)?;
    if config.concurrency > MAX_BENCHMARK_CONCURRENCY {
        return Err(ConfigError(format!(
            "--concurrency must not exceed {MAX_BENCHMARK_CONCURRENCY}"
        )));
    }
    if config.request_bytes > MAX_APPLICATION_PAYLOAD_BYTES
        || config.response_bytes as usize > MAX_APPLICATION_PAYLOAD_BYTES
    {
        return Err(ConfigError(format!(
            "payload byte counts must not exceed {MAX_APPLICATION_PAYLOAD_BYTES}"
        )));
    }
    if config.messages_per_stream > MAX_MESSAGES_PER_STREAM {
        return Err(ConfigError(format!(
            "--messages-per-stream must not exceed {MAX_MESSAGES_PER_STREAM}"
        )));
    }
    Ok(config)
}

fn take(options: &mut BTreeMap<String, String>, name: &str) -> Result<String, ConfigError> {
    options
        .remove(name)
        .ok_or_else(|| ConfigError(format!("missing required option {name}")))
}

fn take_parsed<T>(options: &mut BTreeMap<String, String>, name: &str) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: fmt::Display,
{
    let value = take(options, name)?;
    value
        .parse()
        .map_err(|error| ConfigError(format!("invalid value for {name}: {error}")))
}

fn take_positive<T>(options: &mut BTreeMap<String, String>, name: &str) -> Result<T, ConfigError>
where
    T: FromStr + Default + PartialEq,
    T::Err: fmt::Display,
{
    let value = take_parsed(options, name)?;
    if value == T::default() {
        return Err(ConfigError(format!("{name} must be greater than zero")));
    }
    Ok(value)
}

fn reject_unknown(options: &BTreeMap<String, String>) -> Result<(), ConfigError> {
    if let Some(name) = options.keys().next() {
        Err(ConfigError(format!("unknown option {name}")))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, RpcKind, parse};

    #[test]
    fn parses_complete_client_configuration() {
        let command = parse(
            [
                "client",
                "--address",
                "127.0.0.1:1234",
                "--cert",
                "ca.pem",
                "--rpc",
                "bidi",
                "--concurrency",
                "8",
                "--warmup-ms",
                "100",
                "--measurement-ms",
                "1000",
                "--request-bytes",
                "64",
                "--response-bytes",
                "128",
                "--messages-per-stream",
                "4",
            ]
            .map(str::to_owned),
        )
        .expect("client configuration should parse");

        let Command::Client(config) = command else {
            panic!("expected client command");
        };
        assert_eq!(config.rpc, RpcKind::Bidi);
        assert_eq!(config.concurrency, 8);
        assert_eq!(config.messages_per_stream, 4);
    }

    #[test]
    fn rejects_zero_concurrency_and_unknown_options() {
        let base = [
            "client",
            "--address",
            "127.0.0.1:1234",
            "--cert",
            "ca.pem",
            "--rpc",
            "unary",
            "--concurrency",
            "0",
            "--warmup-ms",
            "0",
            "--measurement-ms",
            "1",
            "--request-bytes",
            "0",
            "--response-bytes",
            "0",
            "--messages-per-stream",
            "1",
        ];
        assert!(parse(base.map(str::to_owned)).is_err());

        let mut unknown = base.to_vec();
        unknown[8] = "1";
        unknown.extend(["--extra", "value"]);
        assert!(parse(unknown.into_iter().map(str::to_owned)).is_err());

        let mut oversized_payload = base.to_vec();
        oversized_payload[8] = "1";
        oversized_payload[16] = "67108865";
        assert!(parse(oversized_payload.into_iter().map(str::to_owned)).is_err());

        let mut oversized_stream = base.to_vec();
        oversized_stream[8] = "1";
        oversized_stream[18] = "1000001";
        assert!(parse(oversized_stream.into_iter().map(str::to_owned)).is_err());
    }
}
