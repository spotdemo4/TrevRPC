use std::future::Future;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::{Error, Result};

pub(crate) struct UploadWriter {
    cancel: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Result<()>>>,
}

impl UploadWriter {
    pub(crate) fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = Result<()>> + Send + 'static,
    {
        let (cancel, cancelled) = oneshot::channel();
        let task = tokio::spawn(async move {
            tokio::select! {
                biased;
                result = future => result,
                _ = cancelled => Ok(()),
            }
        });
        Self {
            cancel: Some(cancel),
            task: Some(task),
        }
    }

    #[cfg(test)]
    const fn from_task(task: JoinHandle<Result<()>>) -> Self {
        Self {
            cancel: None,
            task: Some(task),
        }
    }

    pub(crate) async fn abort_and_settle(&mut self) -> Result<()> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };

        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        }

        match task.await {
            Ok(result) => result,
            Err(error) => Err(Error::transport(error)),
        }
    }
}

impl Drop for UploadWriter {
    fn drop(&mut self) {
        if let Some(cancel) = self.cancel.take() {
            let _ = cancel.send(());
        } else if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::sync::oneshot;

    use crate::{Code, Error, Status};

    use super::UploadWriter;

    struct DropProbe(Option<oneshot::Sender<()>>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn upload_writer_abort_waits_for_task_drop() {
        let (dropped, dropped_rx) = oneshot::channel();
        let (started, started_rx) = oneshot::channel();
        let mut writer = UploadWriter::spawn(async move {
            let _probe = DropProbe(Some(dropped));
            let _ = started.send(());
            std::future::pending::<crate::Result<()>>().await
        });
        started_rx.await.expect("writer task should start");

        writer
            .abort_and_settle()
            .await
            .expect("transport-owned cancellation should settle cleanly");

        dropped_rx
            .await
            .expect("writer task resources must be dropped before settlement returns");
    }

    #[tokio::test]
    async fn upload_writer_suppresses_transport_owned_cancellation() {
        let mut writer = UploadWriter::spawn(std::future::pending::<crate::Result<()>>());

        writer
            .abort_and_settle()
            .await
            .expect("cooperative transport cancellation should be suppressed");
    }

    #[tokio::test]
    async fn upload_writer_preserves_completed_cancelled_status() {
        let mut writer = UploadWriter::spawn(async {
            Err(Error::from(Status::cancelled("local upload failed")))
        });
        while !writer
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            tokio::task::yield_now().await;
        }

        let status = writer
            .abort_and_settle()
            .await
            .expect_err("completed status error must survive settlement")
            .into_status();

        assert_eq!(status.code(), Code::Cancelled);
        assert_eq!(status.message(), "local upload failed");
    }

    #[tokio::test]
    async fn upload_writer_preserves_completed_transport_failure() {
        let mut writer = UploadWriter::spawn(async {
            Err(Error::transport(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "upload pipe failed",
            )))
        });
        while !writer
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            tokio::task::yield_now().await;
        }

        let status = writer
            .abort_and_settle()
            .await
            .expect_err("completed transport failure must survive settlement")
            .into_status();

        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("upload pipe failed"));
    }

    #[tokio::test]
    async fn upload_writer_preserves_preexisting_join_cancellation() {
        let task = tokio::spawn(std::future::pending::<crate::Result<()>>());
        task.abort();
        let mut writer = UploadWriter::from_task(task);

        let status = writer
            .abort_and_settle()
            .await
            .expect_err("pre-existing cancellation must not be suppressed")
            .into_status();

        assert_eq!(status.code(), Code::Unavailable);
    }

    #[tokio::test]
    async fn upload_writer_preserves_task_panic() {
        let mut writer = UploadWriter::spawn(async {
            panic!("writer panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        while !writer
            .task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            tokio::task::yield_now().await;
        }

        let status = writer
            .abort_and_settle()
            .await
            .expect_err("writer panic must survive settlement")
            .into_status();

        assert_eq!(status.code(), Code::Unavailable);
        assert!(status.message().contains("writer panic"));
    }
}
