//! The `client` subcommand: runs every scenario in `interop/README.md` over native QUIC.

use std::future::Future;
use std::io::Write as _;
use std::net::SocketAddr;
use std::time::Duration;

use futures_util::StreamExt;
use quinn::rustls::pki_types::CertificateDer;
use quinn::rustls::pki_types::pem::PemObject;
use trevrpc::client::{CallOptions, Channel, ChannelConfig};
use trevrpc::{Code, Error};

use crate::proto::{EchoClient, EchoRequest, EchoResponse};
use crate::{BoxError, Flags};

const DEADLINE: Duration = Duration::from_secs(10);
/// Backstop for a scenario that hangs despite the per-call deadline.
const SCENARIO_BACKSTOP: Duration = Duration::from_secs(15);

type Scenario = Result<(), String>;
type Echo = EchoClient<Channel>;

/// Runs every scenario and returns whether all of them passed.
pub async fn run(flags: &Flags) -> Result<bool, BoxError> {
    let server_name = flags.get("server-name").unwrap_or("localhost");
    let addr = tokio::net::lookup_host(flags.require("addr")?)
        .await?
        .next()
        .ok_or("--addr did not resolve to an address")?;
    let mut roots = quinn::rustls::RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(flags.require("ca")?)? {
        roots.add(cert?)?;
    }
    let tls = quinn::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let bind: SocketAddr = if addr.is_ipv6() {
        (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
    } else {
        (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
    };
    let mut endpoint = quinn::Endpoint::client(bind)?;
    endpoint.set_default_client_config(ChannelConfig::new().quic_config(tls)?);
    let channel = Channel::connect(endpoint.clone(), addr, server_name).await?;
    let client = EchoClient::new(channel.clone());

    let mut passed = true;
    for (name, scenario) in scenarios(&client) {
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

type Named<'a> = (
    &'static str,
    std::pin::Pin<Box<dyn Future<Output = Scenario> + 'a>>,
);

fn scenarios(client: &Echo) -> Vec<Named<'_>> {
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
        .unary_with_options(message("hello"), options())
        .await
        .map_err(rpc)?;
    expect_response(&response, "hello", 0)
}

async fn unary_metadata(client: &Echo) -> Scenario {
    let options = options().with_metadata("interop-suffix", b"!".to_vec());
    let response = client
        .unary_with_options(message("hello"), options)
        .await
        .map_err(rpc)?;
    expect_response(&response, "hello!", 0)
}

async fn unary_error(client: &Echo) -> Scenario {
    match client
        .unary_with_options(failing(Code::NotFound, "missing"), options())
        .await
    {
        Ok(_) => Err("expected NotFound, got a response".to_owned()),
        Err(error) => expect_status(error, Code::NotFound, "missing"),
    }
}

async fn unary_large(client: &Echo) -> Scenario {
    let large = "x".repeat(1 << 20);
    let response = client
        .unary_with_options(message(&large), options())
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
        .client_stream_with_options(options())
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
        .server_stream_with_options(request, options())
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
        .server_stream_with_options(request, options())
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
    let mut call = client.bidi_with_options(options()).await.map_err(rpc)?;
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
    let mut call = client.bidi_with_options(options()).await.map_err(rpc)?;
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
