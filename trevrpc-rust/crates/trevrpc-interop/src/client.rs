//! The `client` subcommand: runs every scenario in `interop/README.md` over native QUIC, `QMux`
//! over TLS, or QUIC with the TCP fallback.

use std::collections::HashSet;
use std::future::Future;
use std::io::Write as _;
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::future::BoxFuture;
use trevrpc::client::{CallOptions, Channel, ChannelConfig, TransportKind, TransportPolicy};
use trevrpc::{Code, Error, Metadata};

use crate::proto::{EchoClient, EchoRequest, EchoResponse, UnreliableEchoClient};
use crate::{BoxError, Flags, connect};

const DEADLINE: Duration = Duration::from_secs(10);
/// Backstop for a scenario that hangs despite the per-call deadline.
const SCENARIO_BACKSTOP: Duration = Duration::from_secs(15);

type Scenario = Result<(), String>;
type Echo = EchoClient<Channel>;
type UnreliableEcho = UnreliableEchoClient<Channel>;

/// Runs every scenario and returns whether all of them passed.
pub async fn run(flags: &Flags) -> Result<bool, BoxError> {
    let server_name = connect::server_name(flags);
    let addr = connect::resolve(flags, "addr").await?;
    let tls = connect::tls_config(flags)?;

    let policy = match flags.get("transport").unwrap_or("quic") {
        "quic" => TransportPolicy::QuicOnly,
        "tcp" => TransportPolicy::TcpOnly,
        "auto" => TransportPolicy::Auto,
        other => return Err(format!("unknown --transport {other:?}").into()),
    };
    let mut config = ChannelConfig::new()
        .with_transport_policy(policy)
        .with_tcp_fallback(tls.clone());
    if flags.get("tcp-addr").is_some() {
        config = config.with_tcp_addr(connect::resolve(flags, "tcp-addr").await?);
    }
    let endpoint = connect::client_endpoint(addr, &config, tls)?;
    let channel = Channel::connect_with_config(endpoint.clone(), addr, server_name, config).await?;
    if let Some(expected) = flags.get("expect-transport") {
        let actual = match channel.transport() {
            Some(TransportKind::Quic) => "quic",
            Some(TransportKind::Qmux) => "qmux",
            None => "none",
        };
        if actual != expected {
            channel.close();
            return Err(format!("connected over {actual}, want {expected}").into());
        }
    }
    let client = EchoClient::new(channel.clone());
    let unreliable = UnreliableEchoClient::new(channel.clone());

    let mut passed = true;
    for (name, scenario) in scenarios(&client, &unreliable, &channel) {
        passed &= report(name, scenario).await;
    }

    channel.close();
    endpoint.wait_idle().await;
    Ok(passed)
}

async fn report(name: &str, scenario: impl Future<Output = Scenario>) -> bool {
    let outcome = tokio::time::timeout(SCENARIO_BACKSTOP, scenario)
        .await
        .unwrap_or_else(|_| Err("timed out".to_owned()));
    let mut stdout = std::io::stdout().lock();
    let passed = outcome.is_ok();
    let _ = match outcome {
        Ok(()) => writeln!(stdout, "ok {name}"),
        Err(reason) => writeln!(stdout, "fail {name}: {reason}"),
    };
    let _ = stdout.flush();
    passed
}

type Named<'a> = (&'static str, BoxFuture<'a, Scenario>);

fn scenarios<'a>(
    client: &'a Echo,
    unreliable_client: &'a UnreliableEcho,
    channel: &'a Channel,
) -> Vec<Named<'a>> {
    vec![
        ("unary", Box::pin(unary(client))),
        ("unary-metadata", Box::pin(unary_metadata(client))),
        ("unary-error", Box::pin(unary_error(client))),
        ("unary-large", Box::pin(unary_large(client))),
        ("client-stream", Box::pin(client_stream(client))),
        ("server-stream", Box::pin(server_stream(client))),
        ("server-stream-error", Box::pin(server_stream_error(client))),
        ("bidi", Box::pin(bidi(client))),
        ("bidi-error", Box::pin(bidi_error(client))),
        ("unary-headers", Box::pin(unary_headers(client))),
        (
            "server-stream-headers",
            Box::pin(server_stream_headers(client)),
        ),
        (
            "unreliable",
            Box::pin(unreliable(unreliable_client, channel)),
        ),
    ]
}

fn options() -> CallOptions {
    CallOptions::new().with_timeout(DEADLINE)
}

fn message(message: &str) -> EchoRequest {
    EchoRequest {
        message: message.to_owned(),
        ..EchoRequest::default()
    }
}

fn failing(code: Code, message: &str) -> EchoRequest {
    EchoRequest {
        fail_code: code.as_u32(),
        fail_message: message.to_owned(),
        ..EchoRequest::default()
    }
}

fn rpc(error: Error) -> String {
    let status = error.into_status();
    format!("rpc failed: {:?} {:?}", status.code(), status.message())
}

fn expect_response(actual: &EchoResponse, message: &str, index: u32) -> Scenario {
    if actual.message == message && actual.index == index {
        Ok(())
    } else {
        Err(format!(
            "expected ({message:?}, {index}), got ({:?}, {})",
            short(&actual.message),
            actual.index
        ))
    }
}

fn expect_status(error: Error, code: Code, message: &str) -> Scenario {
    let status = error.into_status();
    if status.code() == code && status.message() == message {
        Ok(())
    } else {
        Err(format!(
            "expected {code:?} {message:?}, got {:?} {:?}",
            status.code(),
            status.message()
        ))
    }
}

/// Truncates long messages so a failure report stays readable.
fn short(message: &str) -> String {
    message.chars().take(64).collect()
}

async fn unary(client: &Echo) -> Scenario {
    let response = client
        .unary(&message("hello"))
        .options(|_| options())
        .await
        .map_err(rpc)?;
    expect_response(&response, "hello", 0)
}

async fn unary_metadata(client: &Echo) -> Scenario {
    let options = options().with_metadata("interop-suffix", b"!".to_vec());
    let response = client
        .unary(&message("hello"))
        .options(|_| options)
        .await
        .map_err(rpc)?;
    expect_response(&response, "hello!", 0)
}

async fn unary_error(client: &Echo) -> Scenario {
    match client
        .unary(&failing(Code::NotFound, "missing"))
        .options(|_| options())
        .await
    {
        Ok(_) => Err("expected NotFound, got a response".to_owned()),
        Err(error) => expect_status(error, Code::NotFound, "missing"),
    }
}

async fn unary_large(client: &Echo) -> Scenario {
    let large = "x".repeat(1 << 20);
    let response = client
        .unary(&message(&large))
        .options(|_| options())
        .await
        .map_err(rpc)?;
    if response.message == large && response.index == 0 {
        Ok(())
    } else {
        Err(format!(
            "large message was not echoed ({} bytes, index {})",
            response.message.len(),
            response.index
        ))
    }
}

async fn client_stream(client: &Echo) -> Scenario {
    let call = client
        .client_stream()
        .options(|_| options())
        .await
        .map_err(rpc)?;
    for part in ["a", "b", "c"] {
        call.send(message(part)).await.map_err(rpc)?;
    }
    let response = call.close_and_recv().await.map_err(rpc)?;
    expect_response(&response, "abc", 3)
}

async fn server_stream(client: &Echo) -> Scenario {
    let request = EchoRequest {
        count: 3,
        ..message("s")
    };
    let mut responses = client
        .server_stream(&request)
        .options(|_| options())
        .await
        .map_err(rpc)?;
    for index in 0..3 {
        let response = responses
            .next()
            .await
            .ok_or("stream ended early")?
            .map_err(rpc)?;
        expect_response(&response, "s", index)?;
    }
    match responses.next().await {
        None => Ok(()),
        Some(Ok(_)) => Err("stream yielded more than 3 responses".to_owned()),
        Some(Err(error)) => Err(rpc(error)),
    }
}

async fn server_stream_error(client: &Echo) -> Scenario {
    let request = EchoRequest {
        count: 2,
        fail_code: Code::FailedPrecondition.as_u32(),
        fail_message: "stop".to_owned(),
        ..message("s")
    };
    let mut responses = client
        .server_stream(&request)
        .options(|_| options())
        .await
        .map_err(rpc)?;
    for index in 0..2 {
        let response = responses
            .next()
            .await
            .ok_or("stream ended early")?
            .map_err(rpc)?;
        expect_response(&response, "s", index)?;
    }
    match responses.next().await {
        Some(Err(error)) => expect_status(error, Code::FailedPrecondition, "stop"),
        Some(Ok(_)) => Err("stream yielded more than 2 responses".to_owned()),
        None => Err("stream ended with OK, expected FailedPrecondition".to_owned()),
    }
}

async fn bidi(client: &Echo) -> Scenario {
    let mut call = client.bidi().options(|_| options()).await.map_err(rpc)?;
    for (index, part) in ["1", "2", "3"].into_iter().enumerate() {
        call.send(message(part)).await.map_err(rpc)?;
        let response = call
            .recv()
            .await
            .map_err(rpc)?
            .ok_or("stream ended before the echo")?;
        expect_response(
            &response,
            part,
            u32::try_from(index).map_err(|e| e.to_string())?,
        )?;
    }
    call.close_send().map_err(rpc)?;
    match call.recv().await {
        Ok(None) => Ok(()),
        Ok(Some(_)) => Err("unexpected response after closing the send side".to_owned()),
        Err(error) => Err(rpc(error)),
    }
}

async fn bidi_error(client: &Echo) -> Scenario {
    let mut call = client.bidi().options(|_| options()).await.map_err(rpc)?;
    call.send(message("1")).await.map_err(rpc)?;
    let response = call
        .recv()
        .await
        .map_err(rpc)?
        .ok_or("stream ended before the echo")?;
    expect_response(&response, "1", 0)?;
    call.send(failing(Code::Aborted, "abort"))
        .await
        .map_err(rpc)?;
    match call.recv().await {
        Err(error) => expect_status(error, Code::Aborted, "abort"),
        Ok(Some(_)) => Err("unexpected response to the failing request".to_owned()),
        Ok(None) => Err("stream ended with OK, expected Aborted".to_owned()),
    }
}

/// The request metadata that asks the server for response headers and trailing metadata.
fn header_options() -> CallOptions {
    options().with_metadata("interop-header", b"h".to_vec())
}

fn expect_metadata(actual: &Metadata, key: &str, what: &str) -> Scenario {
    let expected = Metadata::from([(key.to_owned(), b"h".to_vec())]);
    if actual == &expected {
        Ok(())
    } else {
        Err(format!("expected {what} {expected:?}, got {actual:?}"))
    }
}

async fn unary_headers(client: &Echo) -> Scenario {
    let response = client
        .unary(&message("hello"))
        .options(|_| header_options())
        .with_response()
        .await
        .map_err(rpc)?;
    expect_response(response.message(), "hello", 0)?;
    expect_metadata(response.headers(), "interop-header", "headers")?;
    expect_metadata(response.metadata(), "interop-trailer", "trailing metadata")
}

async fn server_stream_headers(client: &Echo) -> Scenario {
    let request = EchoRequest {
        count: 2,
        ..message("s")
    };
    let mut responses = client
        .server_stream(&request)
        .options(|_| header_options())
        .await
        .map_err(rpc)?;
    // The headers precede the first message.
    let headers = responses.headers().await.map_err(rpc)?;
    expect_metadata(headers, "interop-header", "headers")?;
    for index in 0..2 {
        let response = responses
            .next()
            .await
            .ok_or("stream ended early")?
            .map_err(rpc)?;
        expect_response(&response, "s", index)?;
    }
    match responses.next().await {
        None => {}
        Some(Ok(_)) => return Err("stream yielded more than 2 responses".to_owned()),
        Some(Err(error)) => return Err(rpc(error)),
    }
    let trailers = responses
        .terminal_metadata()
        .ok_or("the stream ended without a terminal status")?;
    expect_metadata(trailers, "interop-trailer", "trailing metadata")
}

/// The requests the `unreliable` scenario sends before the large one.
const UNRELIABLE_REQUESTS: usize = 50;
/// The size of the last request, too large for a datagram.
const UNRELIABLE_LARGE: usize = 4096;

async fn unreliable(client: &UnreliableEcho, channel: &Channel) -> Scenario {
    let connection = channel.quic_connection();
    let datagrams_before = connection
        .as_ref()
        .map(|connection| connection.stats().frame_rx.datagram);

    let (sender, mut responses) = client
        .bidi()
        .options(|_| options())
        .await
        .map_err(rpc)?
        .split();
    let mut sent: Vec<String> = (0..UNRELIABLE_REQUESTS)
        .map(|index| index.to_string())
        .collect();
    sent.push("x".repeat(UNRELIABLE_LARGE));
    for part in &sent {
        let request = EchoRequest {
            count: 1,
            ..message(part)
        };
        sender.send(request).await.map_err(rpc)?;
    }
    sender.finish().map_err(rpc)?;

    // Every response is one that was sent, at most once.
    let mut unseen: HashSet<&str> = sent.iter().map(String::as_str).collect();
    let mut delivered = 0_usize;
    let mut large_echoed = false;
    while let Some(response) = responses.next().await {
        let response = response.map_err(rpc)?;
        if !unseen.remove(response.message.as_str()) {
            return Err(format!(
                "response {:?} was not sent or arrived twice",
                short(&response.message)
            ));
        }
        large_echoed |= response.message.len() == UNRELIABLE_LARGE;
        delivered = delivered.saturating_add(1);
    }
    if !large_echoed {
        return Err("the 4096-byte echo is missing".to_owned());
    }

    let trailers = responses
        .terminal_metadata()
        .ok_or("the stream ended without a terminal status")?;
    let received = counter(trailers, "interop-received")?;
    let datagrams = counter(trailers, "interop-datagrams-received")?;
    let total = sent.len();
    if channel.transport() == Some(TransportKind::Qmux) {
        // No datagrams: everything travels on the stream, reliably.
        if delivered != total || received != total || datagrams != 0 {
            return Err(format!(
                "over QMux expected {total} responses, {total} requests received and 0 as datagrams, \
                 got {delivered}, {received} and {datagrams}"
            ));
        }
        return Ok(());
    }

    if !(1..=total).contains(&received) {
        return Err(format!(
            "interop-received was {received}, want between 1 and {total}"
        ));
    }
    if datagrams == 0 {
        return Err("no request arrived as a datagram".to_owned());
    }
    if let (Some(connection), Some(before)) = (connection, datagrams_before)
        && connection.stats().frame_rx.datagram <= before
    {
        return Err("the connection received no DATAGRAM frame from the server".to_owned());
    }
    Ok(())
}

/// A decimal counter in trailing metadata.
fn counter(metadata: &Metadata, key: &str) -> Result<usize, String> {
    let value = metadata
        .get(key)
        .ok_or_else(|| format!("trailing metadata has no {key}"))?;
    std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| format!("trailing metadata {key} is not a decimal number"))
}
