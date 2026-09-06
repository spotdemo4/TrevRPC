#![cfg(feature = "system-native")]

use std::net::UdpSocket;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use tokio::sync::oneshot;
use trevrpc::client::{
    Channel, ChannelConfig, ChannelEvent, ChannelPhase, RpcTransport, StreamingRpcTransport,
};
use trevrpc::native::{
    EndpointConfig, NativeClientConfig, NativeServerConfig, NativeTransport, Protocol,
    RawNativeTransport, TransportConfig,
};
use trevrpc::server::Server;
use trevrpc::{ALPN, BoxStream, Code, RpcKind, RpcRequest, RpcStreamFrameKind, Status};

struct TempDirectory(PathBuf);

impl Drop for TempDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[cfg(target_os = "linux")]
fn thread_count() -> usize {
    std::fs::read_dir("/proc/self/task")
        .expect("read Linux process threads")
        .count()
}

fn endpoint(port: u16) -> EndpointConfig {
    EndpointConfig {
        protocol: Protocol::Native,
        host: "127.0.0.1".to_owned(),
        port,
        alpn: ALPN.to_vec(),
        ..EndpointConfig::default()
    }
}

async fn collect_successful_stream(
    mut frames: BoxStream<trevrpc::RpcStreamFrame>,
) -> Result<Vec<Vec<u8>>, trevrpc::Error> {
    let mut bodies = Vec::new();
    let mut status_seen = false;
    while let Some(frame) = frames.next().await {
        let frame = frame?;
        match frame.frame_kind() {
            Some(RpcStreamFrameKind::Message) => {
                assert!(!status_seen, "message must not follow terminal status");
                bodies.push(frame.body);
            }
            Some(RpcStreamFrameKind::Status) => {
                assert!(!status_seen, "terminal status must be unique");
                assert!(frame.status_value().is_ok());
                status_seen = true;
            }
            None => panic!("native response used an unknown stream frame kind"),
        }
    }
    assert!(
        status_seen,
        "native response stream omitted terminal status"
    );
    Ok(bodies)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_server_routes_all_rpc_shapes_over_real_provider()
-> Result<(), Box<dyn std::error::Error>> {
    let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
    let certificate_pem = certificate.cert.pem();
    let private_key_pem = certificate.signing_key.serialize_pem();
    let unique = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let certificate_directory = TempDirectory(std::env::temp_dir().join(format!(
        "trevrpc-native-server-{}-{unique}",
        std::process::id()
    )));
    std::fs::create_dir(&certificate_directory.0)?;
    let certificate_path = certificate_directory.0.join("certificate.pem");
    let private_key_path = certificate_directory.0.join("private-key.pem");
    std::fs::write(&certificate_path, certificate_pem)?;
    std::fs::write(&private_key_path, private_key_pem)?;
    let port = UdpSocket::bind(("127.0.0.1", 0))?.local_addr()?.port();

    let mut server = Server::new();
    server.route("example.Greeter", "SayHello", |body| async move {
        let mut response = b"hello ".to_vec();
        response.extend_from_slice(&body);
        Ok(response)
    });
    server.route_streaming(
        "example.Greeter",
        "LotsOfReplies",
        RpcKind::ServerStreaming,
        |body, _requests| async move {
            let mut first = body.clone();
            first.extend_from_slice(b"-one");
            let mut second = body;
            second.extend_from_slice(b"-two");
            Ok(trevrpc::stream::from_iter([first, second]))
        },
    );
    server.route_streaming(
        "example.Greeter",
        "DelayedReply",
        RpcKind::ServerStreaming,
        |body, _requests| async move {
            let responses: BoxStream<Vec<u8>> = Box::pin(futures_util::stream::once(async move {
                tokio::time::sleep(Duration::from_millis(100)).await;
                Ok(body)
            }));
            Ok(responses)
        },
    );
    server.route_streaming(
        "example.Greeter",
        "LotsOfGreetings",
        RpcKind::ClientStreaming,
        |_body, mut requests| async move {
            let mut response = Vec::new();
            while let Some(request) = requests.next().await {
                response.extend_from_slice(&request?);
            }
            Ok(trevrpc::stream::from_iter([response]))
        },
    );
    server.route_streaming(
        "example.Greeter",
        "BidiHello",
        RpcKind::BidirectionalStreaming,
        |_body, mut requests| async move {
            let mut responses = Vec::new();
            while let Some(request) = requests.next().await {
                let mut response = b"echo:".to_vec();
                response.extend_from_slice(&request?);
                responses.push(response);
            }
            Ok(trevrpc::stream::from_iter(responses))
        },
    );
    server.route_streaming(
        "example.Greeter",
        "RejectUpload",
        RpcKind::BidirectionalStreaming,
        |_body, _requests| async move {
            Err(Status::new(Code::PermissionDenied, "upload rejected").into())
        },
    );

    let mut server_endpoint = endpoint(port);
    server_endpoint.cert_file = Some(certificate_path.to_string_lossy().into_owned());
    server_endpoint.key_file = Some(private_key_path.to_string_lossy().into_owned());
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server_task = tokio::spawn(server.serve_native_with_shutdown(
        NativeServerConfig::new(server_endpoint),
        async {
            let _ = shutdown_rx.await;
        },
    ));

    let mut client_endpoint = endpoint(port);
    client_endpoint.skip_certificate_validation = true;
    let mut transport = None;
    let mut last_error = None;
    for _ in 0..50 {
        match RawNativeTransport::connect(NativeClientConfig::new(client_endpoint.clone())).await {
            Ok(value) => {
                transport = Some(value);
                break;
            }
            Err(error) => {
                last_error = Some(error.to_string());
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let transport = transport.unwrap_or_else(|| {
        panic!(
            "native server should become connectable: {}",
            last_error.unwrap_or_default()
        )
    });

    #[cfg(target_os = "linux")]
    {
        let load_runtime = NativeTransport::new(TransportConfig {
            stream_capacity: 128,
            delivery_capacity: 128,
            ..TransportConfig::default()
        })
        .await?;
        let load_connection = load_runtime.dial(client_endpoint.clone()).await?;
        let baseline_threads = thread_count();
        let mut peak_threads = baseline_threads;
        let mut idle_streams = Vec::with_capacity(64);
        for _ in 0..64 {
            idle_streams.push(
                tokio::time::timeout(Duration::from_secs(5), load_connection.open_bi())
                    .await
                    .expect("idle native stream open timed out")?,
            );
            peak_threads = peak_threads.max(thread_count());
        }
        assert!(
            peak_threads <= baseline_threads + 4,
            "idle native streams grew OS threads: baseline={baseline_threads}, peak={peak_threads}"
        );
        drop(idle_streams);
        drop(load_connection);
        tokio::time::timeout(Duration::from_secs(5), load_runtime.close())
            .await
            .expect("idle-load native runtime close timed out")?;
    }

    let response = transport
        .call(RpcRequest::new(
            "example.Greeter",
            "SayHello",
            b"world".to_vec(),
        ))
        .await
        .map_err(|error| std::io::Error::other(format!("unary call failed: {error}")))?;
    assert_eq!(response.body, b"hello world");
    assert_eq!(response.status, 0);

    let server_stream = transport
        .streaming_call(
            RpcRequest::new("example.Greeter", "LotsOfReplies", b"hello".to_vec())
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::empty(),
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!("server-streaming setup failed: {error}"))
        })?;
    assert_eq!(
        collect_successful_stream(server_stream)
            .await
            .map_err(|error| std::io::Error::other(format!(
                "server-streaming response failed: {error}"
            )))?,
        [b"hello-one".to_vec(), b"hello-two".to_vec()]
    );

    let client_stream = transport
        .streaming_call(
            RpcRequest::new("example.Greeter", "LotsOfGreetings", Vec::new())
                .with_kind(RpcKind::ClientStreaming),
            trevrpc::stream::from_iter([b"hello ".to_vec(), b"world".to_vec()]),
        )
        .await
        .map_err(|error| {
            std::io::Error::other(format!("client-streaming setup failed: {error}"))
        })?;
    assert_eq!(
        collect_successful_stream(client_stream)
            .await
            .map_err(|error| std::io::Error::other(format!(
                "client-streaming response failed: {error}"
            )))?,
        [b"hello world".to_vec()]
    );

    let bidi_stream = transport
        .streaming_call(
            RpcRequest::new("example.Greeter", "BidiHello", Vec::new())
                .with_kind(RpcKind::BidirectionalStreaming),
            trevrpc::stream::from_iter([b"one".to_vec(), b"two".to_vec()]),
        )
        .await
        .map_err(|error| std::io::Error::other(format!("bidi setup failed: {error}")))?;
    assert_eq!(
        collect_successful_stream(bidi_stream)
            .await
            .map_err(|error| std::io::Error::other(format!("bidi response failed: {error}")))?,
        [b"echo:one".to_vec(), b"echo:two".to_vec()]
    );

    let pending_upload: BoxStream<Vec<u8>> = Box::pin(futures_util::stream::pending());
    let mut rejected_upload = transport
        .streaming_call(
            RpcRequest::new("example.Greeter", "RejectUpload", Vec::new())
                .with_kind(RpcKind::BidirectionalStreaming),
            pending_upload,
        )
        .await?;
    let terminal = tokio::time::timeout(Duration::from_secs(5), rejected_upload.next())
        .await
        .expect("terminal status must abort and settle a pending native upload")
        .expect("rejected upload must emit terminal status")?;
    assert_eq!(terminal.status_value().code(), Code::PermissionDenied);
    assert!(
        tokio::time::timeout(Duration::from_secs(5), rejected_upload.next())
            .await
            .expect("rejected native response must drain to FIN")
            .is_none()
    );

    let escaped_transport =
        RawNativeTransport::connect(NativeClientConfig::new(client_endpoint.clone())).await?;
    let escaped_stream = escaped_transport
        .streaming_call(
            RpcRequest::new("example.Greeter", "DelayedReply", b"leased".to_vec())
                .with_kind(RpcKind::ServerStreaming),
            trevrpc::stream::empty(),
        )
        .await?;
    drop(escaped_transport);
    assert_eq!(
        collect_successful_stream(escaped_stream).await?,
        [b"leased".to_vec()]
    );

    let mut limited_config = TransportConfig::default();
    limited_config.max_receive_owned_bytes = 1024;
    let limited_runtime = NativeTransport::new(limited_config).await?;
    let capacity_error = Channel::connect_native_with_config(
        limited_runtime.clone(),
        client_endpoint.clone(),
        ChannelConfig::new().with_max_frame_size(1025),
    )
    .await
    .err()
    .expect("native channel should reject an impossible frame-size override");
    assert_eq!(capacity_error.into_status().code(), Code::InvalidArgument);
    limited_runtime.close().await?;

    let channel_runtime = NativeTransport::new(TransportConfig::default()).await?;
    let channel = Channel::connect_native(channel_runtime.clone(), client_endpoint.clone()).await?;
    let mut channel_states = channel.subscribe_state();
    let mut channel_events = channel.subscribe_events();
    channel_runtime.close().await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while channel_states.borrow_and_update().phase() != ChannelPhase::Closed {
            channel_states
                .changed()
                .await
                .expect("native channel state sender should remain live");
        }
    })
    .await
    .expect("native channel should close after permanent runtime shutdown");
    tokio::time::sleep(Duration::from_millis(250)).await;
    loop {
        match channel_events.try_recv() {
            Ok(ChannelEvent::ReconnectAttempt { .. }) => {
                panic!("permanently stopped native runtime must not reconnect")
            }
            Ok(_) => {}
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            | Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(count)) => {
                panic!("native channel lifecycle events lagged by {count}")
            }
        }
    }

    transport.close().await?;
    shutdown_tx
        .send(())
        .expect("native server shutdown receiver should be live");
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("native server should shut down")??;
    Ok(())
}
