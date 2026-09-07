#![allow(dead_code)]

use std::os::fd::RawFd;

use trevrpc_c_sys as sys;

use crate::ffi::TestingControl;
use crate::{NativeTransport, Result, TransportConfig};

pub(crate) struct TestingProvider {
    pub(crate) transport: NativeTransport,
    pub(crate) control: TestingControl,
}

impl TestingProvider {
    pub(crate) async fn create(config: TransportConfig) -> Result<Self> {
        let (transport, control) = NativeTransport::new_testing(config).await?;
        Ok(Self { transport, control })
    }

    pub(crate) fn push_event(&self, event: &sys::trevrpc_transport_testing_event_v1) -> Result<()> {
        self.control.push_event(event)
    }

    pub(crate) fn push_receive(
        &self,
        receive: &sys::trevrpc_transport_testing_receive_v1,
    ) -> Result<()> {
        self.control.push_receive(receive)
    }

    pub(crate) fn script_status(&self, status: i32) -> Result<()> {
        self.control.script_status(status)
    }

    pub(crate) fn clear_status_script(&self) -> Result<()> {
        self.control.clear_status_script()
    }

    pub(crate) fn set_checkpoint(&self, checkpoint: u32, enabled: bool) -> Result<()> {
        self.control.set_checkpoint(checkpoint, enabled)
    }

    pub(crate) fn wait_checkpoint(&self, checkpoint: u32) -> Result<()> {
        self.control.wait_checkpoint(checkpoint)
    }

    pub(crate) fn release_checkpoint(&self, checkpoint: u32) -> Result<()> {
        self.control.release_checkpoint(checkpoint)
    }

    pub(crate) fn set_poll_timeout_ms(&self, timeout_ms: i32) -> Result<()> {
        self.control.set_poll_timeout_ms(timeout_ms)
    }

    pub(crate) fn push_stopped(&self, status: i32) -> Result<()> {
        self.control.push_stopped(status)
    }

    pub(crate) fn counters(&self) -> Result<sys::trevrpc_transport_testing_counters_v1> {
        self.control.counters()
    }

    pub(crate) fn wake_fd(&self, index: u32) -> Result<RawFd> {
        self.control.wake_fd(index)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TestingProvider;
    use crate::ffi::{Handle, TestingAdmission, TestingControl, TestingEvent};
    use crate::{
        BiStream, CloseReason, Connection, EndpointConfig, Listener, ListenerEvent, Protocol,
        TransportConfig, TransportState,
    };
    use trevrpc_c_sys as sys;

    const OWNER: u64 = 0x5445_5354_5241_4e53;
    const LOCAL_CLIENT: u32 =
        sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLIENT | sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL;

    fn handle(slot: u32) -> Handle {
        sys::trevrpc_transport_handle_v1 {
            owner: OWNER,
            slot,
            generation: 1,
        }
        .into()
    }

    fn local_client_event(
        kind: u32,
        subject_kind: u32,
        subject: Handle,
        parent: Handle,
        operation_id: u64,
    ) -> TestingEvent {
        TestingEvent {
            kind,
            flags: LOCAL_CLIENT,
            status: 0,
            subject_kind,
            subject,
            parent,
            operation_id,
        }
    }

    async fn wait_for_operation(
        provider: &TestingProvider,
        operation: u32,
    ) -> sys::trevrpc_transport_testing_counters_v1 {
        wait_for_operation_count(provider, operation, 1).await
    }

    async fn wait_for_operation_count(
        provider: &TestingProvider,
        operation: u32,
        minimum: u64,
    ) -> sys::trevrpc_transport_testing_counters_v1 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let counters = provider.counters().expect("read testing counters");
                if counters.operation_calls[operation as usize] >= minimum {
                    return counters;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native owner did not call testing provider")
    }

    async fn wait_for_counters(
        provider: &TestingProvider,
        predicate: impl Fn(&sys::trevrpc_transport_testing_counters_v1) -> bool,
    ) -> sys::trevrpc_transport_testing_counters_v1 {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let counters = provider.counters().expect("read testing counters");
                if predicate(&counters) {
                    return counters;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native owner did not reach expected testing state")
    }

    fn native_endpoint() -> EndpointConfig {
        EndpointConfig {
            protocol: Protocol::Native,
            host: "127.0.0.1".to_owned(),
            ..EndpointConfig::default()
        }
    }

    async fn dial_connection(provider: &TestingProvider, connection_handle: Handle) -> Connection {
        let (connection, ()) = tokio::join!(provider.transport.dial(native_endpoint()), async {
            let counters =
                wait_for_operation(provider, sys::TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL).await;
            provider
                .control
                .push_simple_event(&local_client_event(
                    sys::TREVRPC_TRANSPORT_EVENT_CONNECTION_READY,
                    sys::TREVRPC_TRANSPORT_OBJECT_CONNECTION,
                    connection_handle,
                    Handle::default(),
                    counters.last_dial_operation_id,
                ))
                .expect("inject connection ready");
        });
        connection.expect("complete deterministic dial")
    }

    async fn open_stream(
        provider: &TestingProvider,
        connection: &Connection,
        connection_handle: Handle,
        stream_handle: Handle,
    ) -> BiStream {
        let (stream, ()) = tokio::join!(connection.open_bi(), async {
            let counters = wait_for_operation(
                provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM,
            )
            .await;
            provider
                .control
                .push_simple_event(&local_client_event(
                    sys::TREVRPC_TRANSPORT_EVENT_STREAM_READY,
                    sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                    stream_handle,
                    connection_handle,
                    counters.last_open_operation_id,
                ))
                .expect("inject stream ready");
        });
        stream.expect("complete deterministic stream open")
    }

    async fn listen(provider: &TestingProvider, protocol: Protocol) -> Listener {
        provider
            .transport
            .listen(EndpointConfig {
                protocol,
                host: "127.0.0.1".to_owned(),
                defer_admission: true,
                ..EndpointConfig::default()
            })
            .await
            .expect("create deterministic listener")
    }

    fn push_http3_admission(provider: &TestingProvider, path: &[u8]) {
        provider
            .control
            .push_admission(&TestingAdmission {
                kind: sys::TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION,
                protocol: Protocol::Http3,
                listener: handle(1),
                headers: &[(b"x-test".as_slice(), b"yes".as_slice())],
                method: b"POST",
                path,
                authority: b"localhost",
                origin: b"https://localhost",
            })
            .expect("inject HTTP/3 admission");
    }

    async fn complete_send(
        provider: &TestingProvider,
        stream: &BiStream,
        connection_handle: Handle,
        stream_handle: Handle,
    ) {
        let (send, ()) = tokio::join!(stream.send(b"request"), async {
            let counters = wait_for_operation(
                provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND,
            )
            .await;
            provider
                .control
                .push_simple_event(&local_client_event(
                    sys::TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE,
                    sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                    stream_handle,
                    connection_handle,
                    counters.last_send_operation_id,
                ))
                .expect("inject send completion");
        });
        send.expect("complete deterministic send");
    }

    async fn deliver_response(
        provider: &TestingProvider,
        stream: &BiStream,
        connection_handle: Handle,
        stream_handle: Handle,
    ) {
        provider
            .control
            .push_receive_data(stream_handle, b"response")
            .expect("inject receive body");
        provider
            .control
            .push_simple_event(&local_client_event(
                sys::TREVRPC_TRANSPORT_EVENT_STREAM_READABLE,
                sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                stream_handle,
                connection_handle,
                0,
            ))
            .expect("inject readable event");
        let receive = stream
            .receive()
            .await
            .expect("receive deterministic body")
            .expect("receive body before FIN");
        assert_eq!(receive.as_bytes(), b"response");
        drop(receive);

        let mut receive_fin = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            stream_handle,
            connection_handle,
            0,
        );
        receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
        provider
            .control
            .push_simple_event(&receive_fin)
            .expect("inject receive FIN");
        assert!(stream.receive().await.expect("receive clean FIN").is_none());
    }

    fn assert_provider_ownership(provider: &TestingProvider) {
        let counters = provider.counters().expect("read final testing counters");
        assert_eq!(counters.events_injected, 5);
        assert_eq!(counters.events_popped, 5);
        assert_eq!(counters.events_released, 5);
        assert_eq!(counters.receives_injected, 1);
        assert_eq!(counters.receives_popped, 1);
        assert_eq!(counters.receives_released, 1);
    }

    async fn run_roundtrip() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        assert!(provider.wake_fd(0).expect("read wake descriptor") >= 0);
        assert_eq!(
            provider
                .transport
                .diagnostics()
                .await
                .expect("read diagnostics")
                .state,
            TransportState::Running
        );

        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        assert_eq!(connection.handle(), (OWNER, 2, 1));
        assert_eq!(connection.protocol(), Some(Protocol::Native));

        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        assert_eq!(stream.handle(), (OWNER, 3, 1));
        complete_send(&provider, &stream, connection_handle, stream_handle).await;
        deliver_response(&provider, &stream, connection_handle, stream_handle).await;
        assert_provider_ownership(&provider);

        drop(stream);
        drop(connection);
        provider.push_stopped(0).expect("inject stopped event");
        tokio::time::timeout(Duration::from_secs(5), provider.transport.close())
            .await
            .expect("testing transport close timed out")
            .expect("close testing transport");
        assert!(provider.control.counters().is_err());
    }

    #[tokio::test]
    async fn owner_drives_dial_stream_send_receive_and_stop() {
        run_roundtrip().await;
    }

    #[tokio::test]
    async fn directional_terminal_events_survive_pre_ready_delivery() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);

        let (stream, ()) = tokio::join!(connection.open_bi(), async {
            let counters = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM,
            )
            .await;
            let mut stopped = local_client_event(
                sys::TREVRPC_TRANSPORT_EVENT_SEND_STOPPED,
                sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                stream_handle,
                connection_handle,
                0,
            );
            stopped.flags &= !sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL;
            stopped.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL
                | sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER
                | sys::TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET;
            stopped.status = -libc::ECANCELED;
            provider
                .control
                .push_simple_event(&stopped)
                .expect("inject pre-ready send stop");
            provider
                .control
                .push_simple_event(&local_client_event(
                    sys::TREVRPC_TRANSPORT_EVENT_STREAM_READY,
                    sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                    stream_handle,
                    connection_handle,
                    counters.last_open_operation_id,
                ))
                .expect("inject stream ready");
        });
        let (send, receive) = stream.expect("complete deterministic stream open").split();
        assert_eq!(
            send.stopped().await.expect("observe peer send stop"),
            CloseReason::Peer {
                code: 0,
                peer_reset: true,
            }
        );

        let mut receive_fin = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            stream_handle,
            connection_handle,
            0,
        );
        receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL
            | sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
        provider
            .control
            .push_simple_event(&receive_fin)
            .expect("inject receive FIN");
        receive.ended().await.expect("observe receive FIN");

        drop(send);
        drop(receive);
        drop(connection);
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    async fn wait_for_command_rejection(provider: &TestingProvider) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if provider.transport.diagnostics().await.is_err() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("stopped transport kept accepting commands");
    }

    #[tokio::test]
    async fn spontaneous_stopped_closes_command_admission() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        provider.push_stopped(0).expect("inject stopped event");
        wait_for_command_rejection(&provider).await;
        provider
            .transport
            .close()
            .await
            .expect("close spontaneously stopped transport");
        assert!(provider.control.counters().is_err());
    }

    #[tokio::test]
    async fn failed_stopped_is_the_terminal_transport_result() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        provider
            .push_stopped(-libc::EIO)
            .expect("inject failed stopped event");
        wait_for_command_rejection(&provider).await;
        let error = provider
            .transport
            .close()
            .await
            .expect_err("failed STOPPED must not report success");
        assert_eq!(error.code(), -libc::EIO);
        assert!(provider.control.counters().is_err());
    }

    #[tokio::test]
    async fn canceled_admitted_dial_requests_provider_cancellation() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        provider
            .set_poll_timeout_ms(1)
            .expect("enable cancellation polling");

        let mut dial = Box::pin(provider.transport.dial(native_endpoint()));
        let counters = tokio::select! {
            _ = &mut dial => panic!("dial completed before provider event"),
            counters = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL,
            ) => counters,
        };
        drop(dial);
        let canceled = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL_CANCEL,
        )
        .await;
        assert_eq!(
            canceled.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL_CANCEL as usize],
            1
        );

        let mut failed = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_CONNECTION_FAILED,
            sys::TREVRPC_TRANSPORT_OBJECT_CONNECTION,
            handle(2),
            Handle::default(),
            counters.last_dial_operation_id,
        );
        failed.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL;
        failed.status = -libc::ECANCELED;
        provider
            .control
            .push_simple_event(&failed)
            .expect("inject canceled dial completion");
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_HANDLE,
        )
        .await;

        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn canceled_admitted_open_closes_stream_until_completion() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        provider
            .set_poll_timeout_ms(1)
            .expect("enable cancellation polling");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;

        let mut open = Box::pin(connection.open_bi());
        let counters = tokio::select! {
            _ = &mut open => panic!("open completed before provider event"),
            counters = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM,
            ) => counters,
        };
        drop(open);
        let canceled = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_STREAM,
        )
        .await;
        assert_eq!(
            canceled.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_STREAM as usize],
            1
        );

        let mut failed = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_STREAM_FAILED,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            handle(3),
            connection_handle,
            counters.last_open_operation_id,
        );
        failed.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL;
        failed.status = -libc::ECANCELED;
        provider
            .control
            .push_simple_event(&failed)
            .expect("inject canceled open completion");
        wait_for_operation_count(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_HANDLE,
            1,
        )
        .await;

        drop(connection);
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_admitted_send_aborts_only_after_completion() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        provider
            .set_checkpoint(
                sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION,
                true,
            )
            .expect("enable send-admission checkpoint");

        let control = provider.control.clone();
        let checkpoint = tokio::task::spawn_blocking(move || {
            control.wait_checkpoint(sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION)
        });
        let mut send = Box::pin(stream.send(b"cancel me"));
        tokio::select! {
            result = &mut send => panic!("send completed at admission checkpoint: {result:?}"),
            reached = checkpoint => reached
                .expect("checkpoint waiter panicked")
                .expect("wait for send-admission checkpoint"),
        }
        drop(send);
        let counters = provider.counters().expect("read admitted send counters");
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            0
        );

        provider
            .release_checkpoint(sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION)
            .expect("release send-admission checkpoint");
        provider
            .control
            .push_simple_event(&local_client_event(
                sys::TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE,
                sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                stream_handle,
                connection_handle,
                counters.last_send_operation_id,
            ))
            .expect("inject canceled send completion");
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND,
        )
        .await;
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            1
        );

        drop(stream);
        drop(connection);
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn canceled_admitted_finish_does_not_abort_send_direction() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream = open_stream(&provider, &connection, connection_handle, handle(3)).await;
        let (send, receive) = stream.split();
        provider
            .set_checkpoint(
                sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION,
                true,
            )
            .expect("enable finish-send checkpoint");

        let control = provider.control.clone();
        let checkpoint = tokio::task::spawn_blocking(move || {
            control.wait_checkpoint(
                sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION,
            )
        });
        let mut finish = Box::pin(send.finish());
        tokio::select! {
            _ = &mut finish => panic!("finish completed at admission checkpoint"),
            reached = checkpoint => reached
                .expect("checkpoint waiter panicked")
                .expect("wait for finish-send checkpoint"),
        }
        drop(finish);
        drop(send);
        drop(receive);
        provider
            .release_checkpoint(
                sys::TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION,
            )
            .expect("release finish-send checkpoint");

        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE,
        )
        .await;
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_FINISH_SEND as usize],
            1
        );
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            0
        );

        drop(connection);
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    async fn run_half_drop_case(send_done: bool, receive_done: bool, receive_first: bool) {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        let (send, receive) = stream.split();

        if send_done {
            send.finish().await.expect("finish send direction");
        }
        if receive_done {
            let mut receive_fin = local_client_event(
                sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
                sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                stream_handle,
                connection_handle,
                0,
            );
            receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
            provider
                .control
                .push_simple_event(&receive_fin)
                .expect("inject receive FIN");
            receive.ended().await.expect("observe receive FIN");
        }

        if receive_first {
            drop(receive);
            drop(send);
        } else {
            drop(send);
            drop(receive);
        }
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_FINISH_SEND as usize],
            u64::from(send_done)
        );
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            u64::from(!send_done)
        );
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            u64::from(!receive_done)
        );
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_STREAM as usize],
            0
        );

        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn split_half_drop_matrix_aborts_only_open_directions() {
        for send_done in [false, true] {
            for receive_done in [false, true] {
                for receive_first in [false, true] {
                    run_half_drop_case(send_done, receive_done, receive_first).await;
                }
            }
        }
    }

    async fn run_explicit_half_abort_case(abort_send: bool, receive_first: bool) {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream = open_stream(&provider, &connection, connection_handle, handle(3)).await;
        let (send, receive) = stream.split();

        if abort_send {
            send.abort(41).await.expect("abort send direction");
        } else {
            receive.abort(42).await.expect("abort receive direction");
        }
        let counters = provider.counters().expect("read explicit abort counters");
        let operation = if abort_send {
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND
        } else {
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE
        };
        assert_eq!(counters.operation_calls[operation as usize], 1);

        if receive_first {
            drop(receive);
            drop(send);
        } else {
            drop(send);
            drop(receive);
        }
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            1
        );
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            1
        );
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_STREAM as usize],
            0
        );

        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn explicit_half_abort_is_not_repeated_by_drop() {
        for abort_send in [false, true] {
            for receive_first in [false, true] {
                run_explicit_half_abort_case(abort_send, receive_first).await;
            }
        }
    }

    async fn run_failed_half_abort_retry_case(abort_send: bool) {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream = open_stream(&provider, &connection, connection_handle, handle(3)).await;
        let (send, receive) = stream.split();
        provider
            .script_status(-libc::EIO)
            .expect("script abort rejection");

        let error = if abort_send {
            send.abort(51)
                .await
                .expect_err("reject explicit send abort")
        } else {
            receive
                .abort(52)
                .await
                .expect_err("reject explicit receive abort")
        };
        assert_eq!(error.code(), -libc::EIO);
        drop(send);
        drop(receive);
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls[sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND as usize],
            if abort_send { 2 } else { 1 }
        );
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            if abort_send { 1 } else { 2 }
        );

        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn rejected_explicit_half_abort_is_retried_once_by_drop() {
        run_failed_half_abort_retry_case(false).await;
        run_failed_half_abort_retry_case(true).await;
    }

    #[tokio::test]
    async fn transient_receive_errors_leave_direction_retryable() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream = open_stream(&provider, &connection, connection_handle, handle(3)).await;

        for (status, body) in [
            (-libc::EIO, b"after-io".as_slice()),
            (-libc::ESHUTDOWN, b"after-shutdown".as_slice()),
        ] {
            provider
                .script_status(status)
                .expect("script transient receive error");
            let error = stream.receive().await.expect_err("transient receive error");
            assert_eq!(error.code(), status);

            provider
                .control
                .push_receive_data(handle(3), body)
                .expect("inject retry body");
            let received = stream
                .receive()
                .await
                .expect("retry receive")
                .expect("retry body");
            assert_eq!(received.as_bytes(), body);
        }

        drop(stream);
        drop(connection);
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn receive_fin_before_abort_keeps_the_direction_terminal() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        let (send, receive) = stream.split();
        let mut pending = Box::pin(receive.receive());
        tokio::select! {
            result = &mut pending => panic!("receive completed before FIN: {result:?}"),
            _ = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE,
            ) => {}
        }
        let mut receive_fin = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            stream_handle,
            connection_handle,
            0,
        );
        receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
        provider
            .control
            .push_simple_event(&receive_fin)
            .expect("inject receive FIN");
        assert!((&mut pending).await.expect("receive clean FIN").is_none());
        drop(pending);
        let error = receive
            .abort(61)
            .await
            .expect_err("receive abort after FIN must be rejected locally");
        assert!(error.to_string().contains("not open"));

        drop(receive);
        drop(send);
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            0
        );
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn explicit_receive_abort_settles_pending_receive_before_late_fin() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        let (send, receive) = stream.split();
        let mut pending = Box::pin(receive.receive());
        tokio::select! {
            result = &mut pending => panic!("receive completed before abort: {result:?}"),
            _ = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE,
            ) => {}
        }
        let (received, aborted) = tokio::join!(&mut pending, receive.abort(62));
        assert!(
            received
                .expect_err("abort must terminate pending receive")
                .is_terminal()
        );
        aborted.expect("abort receive direction");
        drop(pending);

        let before_fin = provider.counters().expect("read abort counters");
        let mut receive_fin = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            stream_handle,
            connection_handle,
            0,
        );
        receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
        provider
            .control
            .push_simple_event(&receive_fin)
            .expect("inject late receive FIN");
        wait_for_counters(&provider, |counters| {
            counters.events_popped > before_fin.events_popped
        })
        .await;
        let error = receive
            .ended()
            .await
            .expect_err("late FIN must not replace the abort result");
        assert!(error.to_string().contains("aborted"));

        drop(receive);
        drop(send);
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            1
        );
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn canceling_owned_receive_wait_aborts_receive_once() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;
        let (send, receive) = stream.split();
        let receive_task = tokio::spawn(async move { receive.receive().await });
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE,
        )
        .await;
        receive_task.abort();
        assert!(
            receive_task
                .await
                .expect_err("receive task must be canceled")
                .is_cancelled()
        );
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE,
        )
        .await;

        let before_fin = provider.counters().expect("read cancellation counters");
        let mut receive_fin = local_client_event(
            sys::TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN,
            sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
            stream_handle,
            connection_handle,
            0,
        );
        receive_fin.flags |= sys::TREVRPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN;
        provider
            .control
            .push_simple_event(&receive_fin)
            .expect("inject late receive FIN");
        wait_for_counters(&provider, |counters| {
            counters.events_popped > before_fin.events_popped
        })
        .await;

        drop(send);
        drop(connection);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION,
        )
        .await;
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE as usize],
            1
        );
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    async fn take_http3_admission(
        provider: &TestingProvider,
        listener: &Listener,
    ) -> crate::Admission {
        push_http3_admission(provider, b"/trevrpc");
        match listener
            .accept()
            .await
            .expect("accept listener event")
            .expect("listener event before shutdown")
        {
            ListenerEvent::Http3Admission(admission) => admission,
            ListenerEvent::Connection(_) | ListenerEvent::WebTransportAdmission(_) => {
                panic!("expected HTTP/3 admission")
            }
        }
    }

    async fn close_listener_provider(provider: TestingProvider, listener: Listener) {
        drop(listener);
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_LISTENER,
        )
        .await;
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn admission_accept_reject_and_drop_are_exactly_once() {
        for expected_status in [200, 403, 500] {
            let provider = TestingProvider::create(TransportConfig::default())
                .await
                .expect("create testing provider");
            let listener = listen(&provider, Protocol::Http3).await;
            let admission = take_http3_admission(&provider, &listener).await;
            assert_eq!(admission.request().protocol, Protocol::Http3);
            assert_eq!(
                admission.request().headers,
                vec![(b"x-test".to_vec(), b"yes".to_vec())]
            );
            assert_eq!(admission.request().method, b"POST");
            assert_eq!(admission.request().path, b"/trevrpc");
            assert_eq!(admission.request().authority, b"localhost");
            assert_eq!(admission.request().origin, b"https://localhost");

            match expected_status {
                200 => admission.accept().await.expect("accept admission"),
                403 => admission.reject(403).await.expect("reject admission"),
                500 => drop(admission),
                _ => unreachable!(),
            }
            let counters = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND,
            )
            .await;
            assert_eq!(counters.admissions_responded, 1);
            assert_eq!(counters.last_admission_status, expected_status);
            assert_eq!(counters.events_released, 1);
            close_listener_provider(provider, listener).await;
        }
    }

    #[tokio::test]
    async fn admission_delivery_overflow_reports_rejection_and_fails_closed() {
        let provider = TestingProvider::create(TransportConfig {
            delivery_capacity: 1,
            ..TransportConfig::default()
        })
        .await
        .expect("create testing provider");
        let listener = listen(&provider, Protocol::Http3).await;
        push_http3_admission(&provider, b"/first");
        push_http3_admission(&provider, b"/second");
        let overflow = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND,
        )
        .await;
        assert_eq!(overflow.admissions_responded, 1);
        assert_eq!(overflow.last_admission_status, 500);
        assert_eq!(overflow.events_released, 1);

        let Err(error) = listener.accept().await else {
            panic!("overflow must be reported before queued admission");
        };
        assert!(error.to_string().contains("rejected 1"));
        let admission = match listener
            .accept()
            .await
            .expect("read retained admission")
            .expect("retained admission before shutdown")
        {
            ListenerEvent::Http3Admission(admission) => admission,
            ListenerEvent::Connection(_) | ListenerEvent::WebTransportAdmission(_) => {
                panic!("expected retained HTTP/3 admission")
            }
        };
        assert_eq!(admission.request().path, b"/first");
        admission.accept().await.expect("accept retained admission");
        let counters = wait_for_operation_count(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND,
            2,
        )
        .await;
        assert_eq!(counters.admissions_responded, 2);
        assert_eq!(counters.last_admission_status, 200);
        assert_eq!(counters.events_released, 2);
        close_listener_provider(provider, listener).await;
    }

    #[tokio::test]
    async fn admission_provider_error_consumes_the_decision() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let listener = listen(&provider, Protocol::Http3).await;
        let admission = take_http3_admission(&provider, &listener).await;
        provider
            .script_status(-libc::EIO)
            .expect("script admission response failure");
        let error = admission
            .accept()
            .await
            .expect_err("provider response failure must surface");
        assert_eq!(error.code(), -libc::EIO);
        drop(admission);
        drop(listener);
        let counters = wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_LISTENER,
        )
        .await;
        assert_eq!(
            counters.operation_calls
                [sys::TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND as usize],
            1
        );
        assert_eq!(counters.admissions_responded, 1);
        assert_eq!(counters.last_admission_status, 200);
        assert_eq!(counters.events_released, 1);
        provider.push_stopped(0).expect("inject stopped event");
        provider
            .transport
            .close()
            .await
            .expect("close testing transport");
    }

    #[tokio::test]
    async fn fatal_shutdown_retains_admitted_send_until_completion() {
        let provider = TestingProvider::create(TransportConfig::default())
            .await
            .expect("create testing provider");
        let connection_handle = handle(2);
        let connection = dial_connection(&provider, connection_handle).await;
        let stream_handle = handle(3);
        let stream = open_stream(&provider, &connection, connection_handle, stream_handle).await;

        let mut send = Box::pin(stream.send(b"pending"));
        let counters = tokio::select! {
            result = &mut send => panic!("send completed before provider event: {result:?}"),
            counters = wait_for_operation(
                &provider,
                sys::TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND,
            ) => counters,
        };
        provider
            .control
            .push_simple_event(&TestingEvent {
                kind: sys::TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC,
                flags: sys::TREVRPC_TRANSPORT_EVENT_FLAG_FATAL
                    | sys::TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL
                    | sys::TREVRPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR
                    | sys::TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL,
                status: -libc::EIO,
                subject_kind: sys::TREVRPC_TRANSPORT_OBJECT_NONE,
                subject: Handle::default(),
                parent: Handle::default(),
                operation_id: 0,
            })
            .expect("inject fatal diagnostic");
        let send_error = tokio::time::timeout(Duration::from_secs(5), &mut send)
            .await
            .expect("fatal diagnostic did not settle send")
            .expect_err("fatal diagnostic must fail send");
        assert_eq!(send_error.code(), -libc::EIO);

        provider
            .control
            .push_simple_event(&local_client_event(
                sys::TREVRPC_TRANSPORT_EVENT_SEND_COMPLETE,
                sys::TREVRPC_TRANSPORT_OBJECT_STREAM,
                stream_handle,
                connection_handle,
                counters.last_send_operation_id,
            ))
            .expect("inject required send completion");
        wait_for_operation(
            &provider,
            sys::TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_TRANSPORT,
        )
        .await;
        provider.push_stopped(0).expect("inject stopped event");
        let error = provider
            .transport
            .close()
            .await
            .expect_err("fatal diagnostic must remain terminal");
        assert_eq!(error.code(), -libc::EIO);
        assert!(provider.control.counters().is_err());
    }

    #[tokio::test]
    #[ignore = "run through the sanitizer-backed Nix FFI stress check"]
    async fn ffi_lifecycle_stress_is_leak_free() {
        let cycles = std::env::var("TREVRPC_NATIVE_FFI_STRESS_CYCLES")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(256);
        for _ in 0..cycles {
            run_roundtrip().await;
        }
    }

    #[cfg(target_os = "linux")]
    fn thread_count() -> usize {
        std::fs::read_dir("/proc/self/task")
            .expect("read Linux process threads")
            .count()
    }

    #[cfg(target_os = "linux")]
    fn wait_for_control_release(control: &TestingControl) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while control.counters().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "native emergency cleanup did not release testing transport"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(target_os = "linux")]
    fn run_owner_failure_cleanup() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("create current-thread runtime");
        let provider = runtime
            .block_on(TestingProvider::create(TransportConfig::default()))
            .expect("create testing provider");
        let control = provider.control.clone();
        drop(runtime);
        control
            .push_stopped(0)
            .expect("inject STOPPED after owner task destruction");
        drop(provider);
        wait_for_control_release(&control);
    }

    #[cfg(target_os = "linux")]
    #[test]
    #[ignore = "run through the sanitizer-backed Nix FFI stress check"]
    fn emergency_cleanup_churn_has_bounded_threads() {
        run_owner_failure_cleanup();
        let baseline = thread_count();
        for _ in 0..64 {
            run_owner_failure_cleanup();
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let current = thread_count();
            if current <= baseline {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "native cleanup retained threads: baseline={baseline}, current={current}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
