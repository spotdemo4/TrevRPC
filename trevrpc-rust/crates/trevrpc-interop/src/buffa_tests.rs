//! The buffa codec end to end, over native QUIC on loopback: an `Echo` server whose messages are
//! `buffa`'s answers clients of either codec, and a `buffa` client calls the interop `Echo`
//! server, whose messages are `prost`'s. Both libraries encode the same protobuf, so each pair
//! must see the same messages. So must a server and a client that decode them as zero-copy
//! views.

// Test helpers outside `#[test]` functions fail by panicking, like the tests themselves.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use futures_util::StreamExt as _;
use trevrpc::client::{Channel, ChannelConfig, RpcTransport as _};
use trevrpc::codec::{Buffa, BuffaViewHandle};
use trevrpc::server::{RequestContext, Server};
use trevrpc::status::Detail;
use trevrpc::{BoxStream, Code, Error, RpcKind, RpcRequest, Status};

use crate::proto::{self, buffa as pb};

#[path = "../../../tests/support/serving.rs"]
#[allow(dead_code)]
mod serving;

/// `Echo` over the `buffa` messages: the interop server's behavior, minus metadata.
struct BuffaEcho;

fn echo(request: &pb::EchoRequest, index: u32) -> pb::EchoResponse {
    pb::EchoResponse {
        message: request.message.clone(),
        index,
        ..Default::default()
    }
}

fn requested_failure(request: &pb::EchoRequest) -> Option<Status> {
    (request.fail_code != 0).then(|| {
        Status::new(
            Code::from_u32(request.fail_code),
            request.fail_message.clone(),
        )
    })
}

#[trevrpc::async_trait]
impl pb::Echo for BuffaEcho {
    async fn unary(
        &self,
        _context: RequestContext,
        request: pb::EchoRequest,
    ) -> Result<pb::EchoResponse, Status> {
        requested_failure(&request).map_or_else(|| Ok(echo(&request, 0)), Err)
    }

    async fn client_stream(
        &self,
        _context: RequestContext,
        mut requests: BoxStream<pb::EchoRequest>,
    ) -> Result<pb::EchoResponse, Status> {
        let mut message = String::new();
        let mut count = 0_u32;
        while let Some(request) = requests.next().await {
            message.push_str(&request?.message);
            count = count.saturating_add(1);
        }
        Ok(pb::EchoResponse {
            message,
            index: count,
            ..Default::default()
        })
    }

    async fn server_stream(
        &self,
        _context: RequestContext,
        request: pb::EchoRequest,
    ) -> Result<BoxStream<pb::EchoResponse>, Status> {
        let responses = (0..request.count)
            .map(|index| echo(&request, index))
            .collect::<Vec<_>>();
        Ok(trevrpc::stream::from_iter(responses))
    }

    async fn bidi(
        &self,
        _context: RequestContext,
        requests: BoxStream<pb::EchoRequest>,
    ) -> Result<BoxStream<pb::EchoResponse>, Status> {
        Ok(Box::pin(
            requests.map(|request| request.map(|request| echo(&request, 0))),
        ))
    }
}

/// `Echo` over request views, answering like [`BuffaEcho`]. Each request it reads must borrow its
/// strings from the request body rather than copy them.
struct ViewEcho;

/// The response to the request view `request`, after checking that its message is read from its
/// body in place.
fn echo_view(request: &BuffaViewHandle<pb::EchoRequest>, index: u32) -> pb::EchoResponse {
    let body = request.bytes().as_ptr_range();
    let message = request.message();
    assert!(
        message.is_empty() || body.contains(&message.as_ptr()),
        "the view copied its message out of the body"
    );
    pb::EchoResponse {
        message: message.to_owned(),
        index,
        ..Default::default()
    }
}

#[trevrpc::async_trait]
impl pb::views::Echo for ViewEcho {
    async fn unary(
        &self,
        _context: RequestContext,
        request: BuffaViewHandle<pb::EchoRequest>,
    ) -> Result<pb::EchoResponse, Status> {
        if request.fail_code() != 0 {
            return Err(Status::new(
                Code::from_u32(request.fail_code()),
                request.fail_message(),
            ));
        }
        Ok(echo_view(&request, 0))
    }

    async fn client_stream(
        &self,
        _context: RequestContext,
        mut requests: BoxStream<BuffaViewHandle<pb::EchoRequest>>,
    ) -> Result<pb::EchoResponse, Status> {
        let mut message = String::new();
        let mut count = 0_u32;
        while let Some(request) = requests.next().await {
            message.push_str(&echo_view(&request?, 0).message);
            count = count.saturating_add(1);
        }
        Ok(pb::EchoResponse {
            message,
            index: count,
            ..Default::default()
        })
    }

    async fn server_stream(
        &self,
        _context: RequestContext,
        request: BuffaViewHandle<pb::EchoRequest>,
    ) -> Result<BoxStream<pb::EchoResponse>, Status> {
        let responses = (0..request.count())
            .map(|index| echo_view(&request, index))
            .collect::<Vec<_>>();
        Ok(trevrpc::stream::from_iter(responses))
    }

    async fn bidi(
        &self,
        _context: RequestContext,
        requests: BoxStream<BuffaViewHandle<pb::EchoRequest>>,
    ) -> Result<BoxStream<pb::EchoResponse>, Status> {
        Ok(Box::pin(requests.map(|request| {
            request.map(|request| echo_view(&request, 0))
        })))
    }
}

/// Serves `server` on loopback and connects a channel to its native QUIC endpoint.
async fn connect(server: Server) -> (serving::Serving, Channel, quinn::Endpoint) {
    let serving = serving::serve(server, serving::self_signed().unwrap())
        .await
        .unwrap();
    let config = ChannelConfig::new();
    let endpoint = crate::connect::client_endpoint(
        serving.udp,
        &config,
        serving::client_tls(&serving.cert, &[]).unwrap(),
    )
    .unwrap();
    let channel = Channel::connect_with_config(endpoint.clone(), serving.udp, "localhost", config)
        .await
        .unwrap();
    (serving, channel, endpoint)
}

fn buffa_request(message: &str, count: u32) -> pb::EchoRequest {
    pb::EchoRequest {
        message: message.to_owned(),
        count,
        ..Default::default()
    }
}

/// Makes every kind of call with the `buffa` client, against a server that answers like the
/// interop `Echo` server.
async fn buffa_client_calls(channel: Channel) {
    let client = pb::EchoClient::new(channel);

    let reply = client.unary(&buffa_request("unary", 0)).await.unwrap();
    assert_eq!((reply.message.as_str(), reply.index), ("unary", 0));

    let mut replies = client
        .server_stream(&buffa_request("many", 3))
        .await
        .unwrap();
    let mut indexes = Vec::new();
    while let Some(reply) = replies.next().await {
        let reply = reply.unwrap();
        assert_eq!(reply.message, "many");
        indexes.push(reply.index);
    }
    assert_eq!(indexes, [0, 1, 2]);

    let upload = client.client_stream().await.unwrap();
    for part in ["a", "b", "c"] {
        upload.send(buffa_request(part, 0)).await.unwrap();
    }
    let reply = upload.close_and_recv().await.unwrap();
    assert_eq!((reply.message.as_str(), reply.index), ("abc", 3));

    let mut chat = client.bidi().await.unwrap();
    for part in ["x", "y"] {
        chat.send(buffa_request(part, 0)).await.unwrap();
        let reply = chat.recv().await.unwrap().expect("one reply per request");
        assert_eq!(reply.message, part);
    }
    chat.close_send().unwrap();
    assert!(chat.recv().await.unwrap().is_none());

    let failed = client
        .unary(&pb::EchoRequest {
            fail_code: Code::NotFound.as_u32(),
            fail_message: "gone".to_owned(),
            ..Default::default()
        })
        .await
        .expect_err("the server fails the call")
        .into_status();
    assert_eq!((failed.code(), failed.message()), (Code::NotFound, "gone"));
}

#[tokio::test]
async fn buffa_client_calls_a_buffa_server() {
    let mut server = Server::new();
    pb::register_echo(&mut server, BuffaEcho);
    let (serving, channel, _endpoint) = connect(server).await;

    buffa_client_calls(channel).await;
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn buffa_client_calls_a_prost_server() {
    let mut server = Server::new();
    proto::register_echo(&mut server, crate::server::EchoService);
    let (serving, channel, _endpoint) = connect(server).await;

    buffa_client_calls(channel).await;
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn prost_client_calls_a_buffa_server() {
    let mut server = Server::new();
    pb::register_echo(&mut server, BuffaEcho);
    let (serving, channel, _endpoint) = connect(server).await;
    let client = proto::EchoClient::new(channel);

    let reply = client
        .unary(&proto::EchoRequest {
            message: "unary".to_owned(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        reply,
        proto::EchoResponse {
            message: "unary".to_owned(),
            index: 0,
        }
    );

    let replies = client
        .server_stream(&proto::EchoRequest {
            message: "many".to_owned(),
            count: 2,
            ..Default::default()
        })
        .await
        .unwrap()
        .map(Result::unwrap)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(
        replies,
        [0, 1].map(|index| proto::EchoResponse {
            message: "many".to_owned(),
            index,
        })
    );

    let reply = client
        .client_stream()
        .send_all(trevrpc::stream::from_iter(["a", "b"].map(|message| {
            proto::EchoRequest {
                message: message.to_owned(),
                ..Default::default()
            }
        })))
        .await
        .unwrap();
    assert_eq!((reply.message.as_str(), reply.index), ("ab", 2));
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

/// Makes every kind of call with the client that decodes response views, against a server that
/// answers like the interop `Echo` server.
async fn view_client_calls(channel: Channel) {
    let client = pb::views::EchoClient::new(channel);

    let reply = client.unary(&buffa_request("unary", 0)).await.unwrap();
    assert_eq!((reply.message(), reply.index()), ("unary", 0));
    let body = reply.bytes().as_ptr_range();
    assert!(
        body.contains(&reply.message().as_ptr()),
        "the response view copied its message out of the body"
    );

    let replies = client
        .server_stream(&buffa_request("many", 3))
        .await
        .unwrap()
        .map(|reply| {
            let reply = reply.unwrap();
            (reply.message().to_owned(), reply.index())
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(replies, [0, 1, 2].map(|index| ("many".to_owned(), index)));

    let upload = client.client_stream().await.unwrap();
    for part in ["a", "b", "c"] {
        upload.send(buffa_request(part, 0)).await.unwrap();
    }
    let reply = upload.close_and_recv().await.unwrap();
    assert_eq!((reply.message(), reply.index()), ("abc", 3));

    let mut chat = client.bidi().await.unwrap();
    for part in ["x", "y"] {
        chat.send(buffa_request(part, 0)).await.unwrap();
        let reply = chat.recv().await.unwrap().expect("one reply per request");
        assert_eq!(reply.message(), part);
    }
    chat.close_send().unwrap();
    assert!(chat.recv().await.unwrap().is_none());
}

#[tokio::test]
async fn view_client_calls_a_view_server() {
    let mut server = Server::new();
    pb::views::register_echo(&mut server, ViewEcho);
    let (serving, channel, _endpoint) = connect(server).await;

    view_client_calls(channel).await;
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

#[tokio::test]
async fn owned_and_view_clients_and_servers_call_each_other() {
    let mut server = Server::new();
    pb::views::register_echo(&mut server, ViewEcho);
    let (serving, channel, _endpoint) = connect(server).await;
    buffa_client_calls(channel).await;
    serving.stop(Duration::from_secs(5)).await.unwrap();

    let mut server = Server::new();
    proto::register_echo(&mut server, crate::server::EchoService);
    let (serving, channel, _endpoint) = connect(server).await;
    view_client_calls(channel).await;
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

/// A body that is not an `EchoRequest` or `EchoResponse`: field 1 claims five bytes, and one
/// follows.
const MALFORMED: [u8; 3] = [0x0a, 0x05, b'h'];

#[tokio::test]
async fn buffa_and_view_servers_reject_malformed_requests() {
    let mut owned = Server::new();
    pb::register_echo(&mut owned, BuffaEcho);
    let mut views = Server::new();
    pb::views::register_echo(&mut views, ViewEcho);
    for server in [owned, views] {
        let (serving, channel, _endpoint) = connect(server).await;
        let status = channel
            .call(RpcRequest::new(pb::echo_paths::UNARY), MALFORMED.to_vec())
            .await
            .expect_err("the server cannot decode the request")
            .into_status();
        assert_eq!(status.code(), Code::InvalidArgument, "{status}");
        serving.stop(Duration::from_secs(5)).await.unwrap();
    }
}

#[tokio::test]
async fn buffa_and_view_clients_reject_malformed_responses() {
    let mut server = Server::new();
    server.route(pb::echo_paths::UNARY, |_body| async {
        Ok(MALFORMED.to_vec())
    });
    server.route_streaming(
        pb::echo_paths::SERVER_STREAM,
        RpcKind::ServerStreaming,
        |_body, _requests| async { Ok(trevrpc::stream::from_iter([MALFORMED.to_vec()])) },
    );
    let (serving, channel, _endpoint) = connect(server).await;
    let request = buffa_request("x", 1);

    let owned = pb::EchoClient::new(channel.clone());
    let failed = owned.unary(&request).await.expect_err("malformed response");
    assert!(matches!(failed, Error::Decode(_)), "{failed}");
    let views = pb::views::EchoClient::new(channel.clone());
    let failed = views.unary(&request).await.expect_err("malformed response");
    assert!(matches!(failed, Error::Decode(_)), "{failed}");

    // A malformed streamed response fails the stream, as with prost.
    let mut replies = owned.server_stream(&request).await.unwrap();
    let status = replies.next().await.unwrap().unwrap_err().into_status();
    assert_eq!(status.code(), Code::Internal, "{status}");
    let mut replies = views.server_stream(&request).await.unwrap();
    let status = replies.next().await.unwrap().unwrap_err().into_status();
    assert_eq!(status.code(), Code::Internal, "{status}");
    serving.stop(Duration::from_secs(5)).await.unwrap();
}

#[test]
fn status_details_carry_buffa_messages() {
    let request = buffa_request("detail", 7);
    let detail = Detail::from_message(&Buffa, &request).unwrap();
    assert_eq!(detail.type_name(), "trevrpc.interop.v1.EchoRequest");

    let decoded: pb::EchoRequest = detail.decode(&Buffa).unwrap();
    assert_eq!((decoded.message.as_str(), decoded.count), ("detail", 7));
    let malformed = Detail::new(detail.type_name(), MALFORMED);
    assert!(malformed.decode::<pb::EchoRequest>(&Buffa).is_err());
}
