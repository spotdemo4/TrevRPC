use std::future::Future;

use crate::{BoxStream, Result, RpcResponse, RpcStreamFrame, RpcStreamFrameKind};

/// Backend operations needed by the transport-neutral client response driver.
///
/// Implementations own wire framing and byte/body decoding. The driver owns the logical response
/// lifecycle: upload settlement, terminal-status precedence, and the response-stream end state.
pub(crate) trait ClientResponseBackend {
    fn read_unary_response(&mut self) -> impl Future<Output = Result<RpcResponse>> + Send;

    fn read_unary_end(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn read_stream_frame(&mut self) -> impl Future<Output = Result<Option<RpcStreamFrame>>> + Send;

    fn abort_upload_and_settle(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn drain_after_terminal_status(&mut self) -> impl Future<Output = Result<()>> + Send;

    fn mark_complete(&mut self);
}

struct ResponseDriverState<B> {
    backend: B,
    complete: bool,
}

/// Drives one unary response and verifies that the response stream has ended.
pub(crate) async fn read_unary_response<B>(mut backend: B) -> Result<RpcResponse>
where
    B: ClientResponseBackend,
{
    let response = backend.read_unary_response().await?;
    backend.read_unary_end().await?;
    backend.mark_complete();
    Ok(response)
}

/// Drives the logical lifecycle of a streaming client response.
pub(crate) fn streaming_response<B>(backend: B) -> BoxStream<RpcStreamFrame>
where
    B: ClientResponseBackend + Send + 'static,
{
    Box::pin(futures_util::stream::unfold(
        ResponseDriverState {
            backend,
            complete: false,
        },
        |mut state| async move {
            if state.complete {
                return None;
            }

            let item = match state.backend.read_stream_frame().await {
                Ok(Some(frame)) if frame.frame_kind() == Some(RpcStreamFrameKind::Status) => {
                    let status = frame.status_value();
                    let upload_result = state.backend.abort_upload_and_settle().await;
                    let drain_result = state.backend.drain_after_terminal_status().await;
                    state.backend.mark_complete();
                    state.complete = true;

                    if let Err(error) = drain_result {
                        Err(error)
                    } else if status.is_ok() {
                        upload_result.map(|()| frame)
                    } else {
                        Ok(frame)
                    }
                }
                Ok(Some(frame)) => Ok(frame),
                Ok(None) => {
                    let _ = state.backend.abort_upload_and_settle().await;
                    return None;
                }
                Err(error) => {
                    let _ = state.backend.abort_upload_and_settle().await;
                    state.backend.mark_complete();
                    state.complete = true;
                    Err(error)
                }
            };

            Some((item, state))
        },
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::future::Future;
    use std::sync::{Arc, Mutex};

    use futures_util::StreamExt;

    use crate::{Code, Error, Result, RpcResponse, RpcStreamFrame, Status};

    use super::{ClientResponseBackend, read_unary_response, streaming_response};

    struct TestBackend {
        unary: Option<Result<RpcResponse>>,
        unary_end: Option<Result<()>>,
        frames: Arc<Mutex<VecDeque<Result<Option<RpcStreamFrame>>>>>,
        upload: Arc<Mutex<Vec<&'static str>>>,
        drain: Option<Result<()>>,
        complete: Arc<Mutex<bool>>,
    }

    impl TestBackend {
        fn stream(frames: impl IntoIterator<Item = Result<Option<RpcStreamFrame>>>) -> Self {
            Self {
                unary: None,
                unary_end: Some(Ok(())),
                frames: Arc::new(Mutex::new(frames.into_iter().collect())),
                upload: Arc::new(Mutex::new(Vec::new())),
                drain: Some(Ok(())),
                complete: Arc::new(Mutex::new(false)),
            }
        }
    }

    impl ClientResponseBackend for TestBackend {
        fn read_unary_response(&mut self) -> impl Future<Output = Result<RpcResponse>> + Send {
            let result = self.unary.take().unwrap_or_else(|| {
                Err(Error::from(Status::internal("missing test unary response")))
            });
            async move { result }
        }

        fn read_unary_end(&mut self) -> impl Future<Output = Result<()>> + Send {
            let result = self.unary_end.take().unwrap_or(Ok(()));
            async move { result }
        }

        fn read_stream_frame(
            &mut self,
        ) -> impl Future<Output = Result<Option<RpcStreamFrame>>> + Send {
            let result = self
                .frames
                .lock()
                .expect("frame lock should not be poisoned")
                .pop_front()
                .unwrap_or(Ok(None));
            async move { result }
        }

        fn abort_upload_and_settle(&mut self) -> impl Future<Output = Result<()>> + Send {
            self.upload
                .lock()
                .expect("upload lock should not be poisoned")
                .push("abort");
            async { Ok(()) }
        }

        fn drain_after_terminal_status(&mut self) -> impl Future<Output = Result<()>> + Send {
            self.upload
                .lock()
                .expect("upload lock should not be poisoned")
                .push("drain");
            let result = self.drain.take().unwrap_or(Ok(()));
            async move { result }
        }

        fn mark_complete(&mut self) {
            *self
                .complete
                .lock()
                .expect("complete lock should not be poisoned") = true;
        }
    }

    #[tokio::test]
    async fn terminal_status_settles_upload_before_clean_completion() {
        let backend =
            TestBackend::stream([Ok(Some(RpcStreamFrame::status(Status::ok()))), Ok(None)]);
        let upload = Arc::clone(&backend.upload);
        let complete = Arc::clone(&backend.complete);
        let mut response = streaming_response(backend);

        let frame = response
            .next()
            .await
            .expect("terminal frame should be emitted")
            .expect("terminal frame should succeed");
        assert_eq!(frame.frame_kind(), Some(crate::RpcStreamFrameKind::Status));
        assert!(response.next().await.is_none());
        assert_eq!(
            *upload.lock().expect("upload lock should not be poisoned"),
            vec!["abort", "drain"]
        );
        assert!(
            *complete
                .lock()
                .expect("complete lock should not be poisoned")
        );
    }

    #[tokio::test]
    async fn remote_error_status_precedes_upload_settlement_failure() {
        let backend = TestBackend::stream([Ok(Some(RpcStreamFrame::status(Status::new(
            Code::PermissionDenied,
            "denied",
        ))))]);
        let mut response = streaming_response(backend);
        let status = response
            .next()
            .await
            .expect("status should be emitted")
            .expect("remote status is represented as a frame")
            .status_value();
        assert_eq!(status.code(), Code::PermissionDenied);
    }

    #[tokio::test]
    async fn unary_driver_checks_transport_end_before_completion() {
        let backend = TestBackend {
            unary: Some(Ok(RpcResponse::ok(Vec::new()))),
            unary_end: Some(Err(Error::from(Status::internal("trailing frame")))),
            ..TestBackend::stream([])
        };
        let error = read_unary_response(backend)
            .await
            .expect_err("trailing unary data should fail");
        assert_eq!(error.into_status().code(), Code::Internal);
    }
}
