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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stack {
    TrevrpcNativeQuic,
    TrevrpcHttp3,
    TrevrpcWebTransport,
}

impl FromStr for Stack {
    type Err = ConfigError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "trevrpc_native_quic" => Ok(Self::TrevrpcNativeQuic),
            "trevrpc_http3" => Ok(Self::TrevrpcHttp3),
            "trevrpc_webtransport" => Ok(Self::TrevrpcWebTransport),
            _ => Err(ConfigError(format!(
                "invalid --stack value {value:?}; expected trevrpc_native_quic, trevrpc_http3, or trevrpc_webtransport"
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
    pub(crate) stack: Stack,
    pub(crate) listen: SocketAddr,
    pub(crate) certificate: PathBuf,
    pub(crate) private_key: PathBuf,
    pub(crate) webtransport_origin: Option<String>,
}

#[derive(Debug)]
pub(crate) struct ClientConfig {
    pub(crate) stack: Stack,
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
    let stack = take_parsed(&mut options, "--stack")?;
    let webtransport_origin = if stack == Stack::TrevrpcWebTransport {
        let origin = take(&mut options, "--webtransport-origin")?;
        if origin.is_empty() {
            return Err(ConfigError(
                "--webtransport-origin must not be empty".to_owned(),
            ));
        }
        Some(origin)
    } else {
        if options.contains_key("--webtransport-origin") {
            return Err(ConfigError(
                "--webtransport-origin is only valid with trevrpc_webtransport".to_owned(),
            ));
        }
        None
    };
    let config = ServerConfig {
        stack,
        listen: take_parsed(&mut options, "--listen")?,
        certificate: PathBuf::from(take(&mut options, "--cert")?),
        private_key: PathBuf::from(take(&mut options, "--key")?),
        webtransport_origin,
    };
    reject_unknown(&options)?;
    Ok(config)
}

fn parse_client(mut options: BTreeMap<String, String>) -> Result<ClientConfig, ConfigError> {
    let config = ClientConfig {
        stack: take_parsed(&mut options, "--stack")?,
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
    match config.stack {
        Stack::TrevrpcNativeQuic => {}
        Stack::TrevrpcHttp3 => {
            return Err(ConfigError(
                "trevrpc_http3 is a server-only stack".to_owned(),
            ));
        }
        Stack::TrevrpcWebTransport => {
            return Err(ConfigError(
                "trevrpc_webtransport is a server-only stack".to_owned(),
            ));
        }
    }
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
    use super::{Command, RpcKind, Stack, parse};

    #[test]
    fn parses_complete_client_configuration() {
        let command = parse(
            [
                "client",
                "--stack",
                "trevrpc_native_quic",
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
        assert_eq!(config.stack, Stack::TrevrpcNativeQuic);
        assert_eq!(config.concurrency, 8);
        assert_eq!(config.messages_per_stream, 4);
    }

    #[test]
    fn rejects_zero_concurrency_and_unknown_options() {
        let base = [
            "client",
            "--stack",
            "trevrpc_native_quic",
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
        unknown[10] = "1";
        unknown.extend(["--extra", "value"]);
        assert!(parse(unknown.into_iter().map(str::to_owned)).is_err());

        let mut oversized_payload = base.to_vec();
        oversized_payload[10] = "1";
        oversized_payload[18] = "67108865";
        assert!(parse(oversized_payload.into_iter().map(str::to_owned)).is_err());

        let mut oversized_stream = base.to_vec();
        oversized_stream[10] = "1";
        oversized_stream[20] = "1000001";
        assert!(parse(oversized_stream.into_iter().map(str::to_owned)).is_err());
    }

    #[test]
    fn requires_stack_for_server_and_client() {
        let server = [
            "server",
            "--listen",
            "127.0.0.1:0",
            "--cert",
            "cert.pem",
            "--key",
            "key.pem",
        ];
        assert!(parse(server.map(str::to_owned)).is_err());

        let client = [
            "client",
            "--address",
            "127.0.0.1:1",
            "--cert",
            "ca.pem",
            "--rpc",
            "unary",
            "--concurrency",
            "1",
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
        assert!(parse(client.map(str::to_owned)).is_err());
    }

    #[test]
    fn http3_is_server_only_and_does_not_accept_origin() {
        let server = [
            "server",
            "--stack",
            "trevrpc_http3",
            "--listen",
            "127.0.0.1:0",
            "--cert",
            "cert.pem",
            "--key",
            "key.pem",
        ];
        let Command::Server(config) =
            parse(server.map(str::to_owned)).expect("HTTP/3 server configuration should parse")
        else {
            panic!("expected server command");
        };
        assert_eq!(config.stack, Stack::TrevrpcHttp3);
        assert!(config.webtransport_origin.is_none());

        let mut with_origin = server.to_vec();
        with_origin.extend(["--webtransport-origin", "https://benchmark.example"]);
        assert!(parse(with_origin.into_iter().map(str::to_owned)).is_err());

        let client = [
            "client",
            "--stack",
            "trevrpc_http3",
            "--address",
            "127.0.0.1:1234",
            "--cert",
            "ca.pem",
            "--rpc",
            "unary",
            "--concurrency",
            "1",
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
        assert!(parse(client.map(str::to_owned)).is_err());
    }

    #[test]
    fn webtransport_is_server_only_and_requires_origin() {
        let server = [
            "server",
            "--stack",
            "trevrpc_webtransport",
            "--listen",
            "127.0.0.1:0",
            "--cert",
            "cert.pem",
            "--key",
            "key.pem",
        ];
        assert!(parse(server.map(str::to_owned)).is_err());

        let mut empty_origin = server.to_vec();
        empty_origin.extend(["--webtransport-origin", ""]);
        assert!(parse(empty_origin.into_iter().map(str::to_owned)).is_err());

        let mut with_origin = server.to_vec();
        with_origin.extend(["--webtransport-origin", "https://benchmark.example"]);
        let Command::Server(config) = parse(with_origin.into_iter().map(str::to_owned))
            .expect("WebTransport server configuration should parse")
        else {
            panic!("expected server command");
        };
        assert_eq!(config.stack, Stack::TrevrpcWebTransport);
        assert_eq!(
            config.webtransport_origin.as_deref(),
            Some("https://benchmark.example")
        );

        let native_with_origin = [
            "server",
            "--stack",
            "trevrpc_native_quic",
            "--listen",
            "127.0.0.1:0",
            "--cert",
            "cert.pem",
            "--key",
            "key.pem",
            "--webtransport-origin",
            "https://benchmark.example",
        ];
        assert!(parse(native_with_origin.map(str::to_owned)).is_err());

        let client = [
            "client",
            "--stack",
            "trevrpc_webtransport",
            "--address",
            "127.0.0.1:1234",
            "--cert",
            "ca.pem",
            "--rpc",
            "unary",
            "--concurrency",
            "1",
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
        assert!(parse(client.map(str::to_owned)).is_err());
    }
}
