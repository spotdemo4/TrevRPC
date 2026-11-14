//! The rate-limit interceptor end to end: a real server over QUIC and `QMux` over TLS rejects a
//! client that spends its tokens, and the client reads the retry delay from the error.

use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::greeter;
use crate::support::{self, Serving};
use trevrpc::client::{ChannelConfig, QmuxTransport, QuicTransport};
use trevrpc::server::{
    RateLimit, RateLimitOptions, RatePolicy, RequestContext, Server, ServerOptions,
};
use trevrpc::{Code, Status};

type TestResult<T = ()> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Replies to `SayHello` and counts the calls that reach it.
struct Greeter(Arc<AtomicUsize>);

#[trevrpc::async_trait]
impl greeter::Greeter for Greeter {
    async fn say_hello(
        &self,
        _context: RequestContext,
        request: greeter::HelloRequest,
    ) -> Result<greeter::HelloReply, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(greeter::HelloReply {
            message: request.name,
        })
    }

    async fn lots_of_replies(
        &self,
        _context: RequestContext,
        _request: greeter::HelloRequest,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn lots_of_greetings(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<greeter::HelloReply, Status> {
        Err(Status::unimplemented("unused"))
    }

    async fn bidi_hello(
        &self,
        _context: RequestContext,
        _requests: trevrpc::BoxStream<greeter::HelloRequest>,
    ) -> Result<trevrpc::BoxStream<greeter::HelloReply>, Status> {
        Err(Status::unimplemented("unused"))
    }
}

/// Spawns a greeter server that allows two `SayHello` calls an hour per client, and returns it
/// with the number of calls that reached the greeter.
async fn spawn_server() -> TestResult<(Serving, Arc<AtomicUsize>)> {
    let handled = Arc::new(AtomicUsize::new(0));
    let mut server = Server::new();
    server.set_options(
        ServerOptions::new().with_graceful_shutdown_timeout(Some(Duration::from_millis(500))),
    );
    server.add_interceptor(RateLimit::new(RateLimitOptions::new().with_policy(
        greeter::greeter_paths::SAY_HELLO,
        RatePolicy::new(2, Duration::from_hours(1)),
    )));
    greeter::register_greeter(&mut server, Greeter(Arc::clone(&handled)));
    let server = support::serve(server, support::self_signed()?).await?;
    Ok((server, handled))
}

/// Makes `SayHello` calls through `client` until the server rejects one, and checks the rejection.
async fn spend_the_bucket<T>(client: &greeter::GreeterClient<T>) -> TestResult
where
    T: trevrpc::client::RpcTransport,
{
    let request = greeter::HelloRequest {
        name: "hi".to_owned(),
    };
    for _ in 0..2 {
        within!(TEST_TIMEOUT, client.say_hello(&request))??;
    }
    let Err(error) = within!(TEST_TIMEOUT, client.say_hello(&request))? else {
        return Err("the third call should be rejected".into());
    };

    // One token takes 30 minutes to refill; none of that has passed.
    let delay = error.retry_delay().ok_or("no retry delay")?;
    ensure!(
        delay > Duration::from_mins(29) && delay <= Duration::from_mins(30),
        "retry delay {delay:?}"
    );
    let status = error.into_status();
    ensure_eq!(status.code(), Code::ResourceExhausted);
    ensure!(
        status
            .message()
            .starts_with("rate limit exceeded: 2 requests")
    );
    Ok(())
}

#[tokio::test]
async fn a_client_over_quic_reads_the_retry_delay() -> TestResult {
    let (server, handled) = spawn_server().await?;
    let config =
        ChannelConfig::new().quic_config(support::client_tls(&server.cert, &[trevrpc::ALPN])?)?;
    let mut endpoint = quinn::Endpoint::client(SocketAddr::from(([127, 0, 0, 1], 0)))?;
    endpoint.set_default_client_config(config);
    let connection = within!(TEST_TIMEOUT, endpoint.connect(server.udp, "localhost")?)??;
    let client = greeter::GreeterClient::new(QuicTransport::new(connection.clone()));

    spend_the_bucket(&client).await?;

    ensure_eq!(handled.load(Ordering::SeqCst), 2);
    connection.close(0_u32.into(), b"done");
    server.stop(TEST_TIMEOUT).await
}

#[tokio::test]
async fn a_client_over_qmux_reads_the_retry_delay() -> TestResult {
    let (server, handled) = spawn_server().await?;
    let transport = within!(
        TEST_TIMEOUT,
        QmuxTransport::connect(
            server.tcp,
            "localhost",
            support::client_tls(&server.cert, &[trevrpc::QMUX_ALPN])?,
        ),
    )??;
    let client = greeter::GreeterClient::new(transport.clone());

    spend_the_bucket(&client).await?;

    ensure_eq!(handled.load(Ordering::SeqCst), 2);
    transport.close();
    server.stop(TEST_TIMEOUT).await
}
