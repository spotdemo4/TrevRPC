//! Tells the request half of a streaming call how the call ended while it is still uploading.
//!
//! The response of a streaming call is only read when the caller polls it, but a server can end
//! the call at any moment with a terminal STATUS, and the request half must then stop: sends fail
//! with that status instead of waiting on flow control for a peer that no longer reads. The
//! response is therefore read by a task that records the end of the call before passing frames on.
//!
//! # Read-ahead
//!
//! Normally the task reads one frame ahead of the caller (plus two queued for it), so the
//! transport's flow control still paces a server that sends faster than the caller reads. A send
//! that is blocked, or that finds the upload stopped, needs the STATUS even when the caller is not
//! reading and frames are queued ahead of it, so the task then keeps reading and buffers frames.
//! It buffers at most the call's own response limits (`max_response_messages` and
//! `max_response_stream_body_size`), which the response stream would enforce on the caller anyway,
//! or 4096 messages and 16 MiB when they are unset, as they are by default, plus the two queued
//! frames. While the upload merely
//! blocks, reaching that bound pauses reading again. Once the upload has stopped, reaching it
//! ends the call for sends with `ResourceExhausted`, since the STATUS cannot be found; the
//! response stream still delivers everything buffered.

use std::collections::VecDeque;
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::{BoxStream, Error, ResponseFrame, Result, Status};

/// Frames queued for the caller ahead of the reading task.
const QUEUED_FRAMES: usize = 2;
/// The message bound when the call sets no `max_response_messages`.
const FALLBACK_MAX_FRAMES: usize = 4096;
/// The byte bound when the call sets no `max_response_stream_body_size`.
const FALLBACK_MAX_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug)]
enum End {
    Running,
    /// The server sent this terminal STATUS.
    Status(Status),
    /// The response failed with this error, such as a lost connection, instead of a STATUS.
    Failed(Status),
    /// The response ended without a STATUS, or nobody reads it any more.
    Unknown,
    /// The upload stopped and the STATUS is behind more buffered responses than the call allows.
    BacklogFull,
}

impl End {
    /// The error sends fail with once the call has ended, or `None` while it is running.
    fn error(&self) -> Option<Error> {
        Some(match self {
            Self::Running => return None,
            Self::Status(status) if !status.is_ok() => Error::from(status.clone()),
            Self::Status(_) => Error::from(Status::cancelled(
                "the server completed the call before the request stream finished",
            )),
            Self::Failed(status) => Error::from(status.clone()),
            Self::Unknown => Error::from(Status::cancelled("request stream is closed")),
            Self::BacklogFull => Error::from(Status::resource_exhausted(
                "the request stream stopped and the terminal status is behind more unread \
                 responses than the call may buffer",
            )),
        })
    }
}

/// What the request half is doing, as far as the reading task needs to know.
#[derive(Clone, Copy, Debug, Default)]
struct Upload {
    /// Sends that are waiting for room in the request stream.
    waiting: usize,
    /// The request stream was dropped before the caller finished it.
    stopped: bool,
}

/// Limits on how much the reading task buffers for a stalled or stopped upload.
#[derive(Clone, Copy, Debug)]
pub(super) struct ReadAhead {
    frames: usize,
    bytes: usize,
}

impl ReadAhead {
    pub(super) fn new(max_messages: Option<usize>, max_stream_body_size: Option<usize>) -> Self {
        Self {
            frames: max_messages.unwrap_or(FALLBACK_MAX_FRAMES).max(1),
            bytes: max_stream_body_size.unwrap_or(FALLBACK_MAX_BYTES).max(1),
        }
    }
}

/// Creates the two halves of a call's end signal. `deadline` bounds how long [`EndWatch::ended`]
/// waits.
pub(super) fn channel(deadline: Option<Instant>) -> (EndSender, EndWatch) {
    let (end, end_receiver) = watch::channel(End::Running);
    let (upload, upload_receiver) = watch::channel(Upload::default());
    (
        EndSender {
            end,
            upload: upload_receiver,
        },
        EndWatch {
            end: end_receiver,
            upload: Arc::new(upload),
            deadline,
        },
    )
}

/// The response side's half: records how the call ended. Dropping it records an unknown end.
pub(super) struct EndSender {
    end: watch::Sender<End>,
    upload: watch::Receiver<Upload>,
}

impl EndSender {
    /// Records the end of the call unless it was recorded already.
    fn end(&self, end: End) {
        self.end.send_if_modified(|current| {
            let running = matches!(current, End::Running);
            if running {
                *current = end;
            }
            running
        });
    }
}

impl Drop for EndSender {
    fn drop(&mut self) {
        self.end(End::Unknown);
    }
}

/// The request side's half: learns how the call ended, and tells the response side when sends
/// need to know.
#[derive(Clone)]
pub(super) struct EndWatch {
    end: watch::Receiver<End>,
    upload: Arc<watch::Sender<Upload>>,
    deadline: Option<Instant>,
}

impl EndWatch {
    /// The error sends fail with if the call has already ended.
    pub(super) fn error_if_ended(&self) -> Option<Error> {
        self.end.borrow().error()
    }

    /// Resolves with the error that sends fail with once the call has ended: the server's
    /// STATUS when it is not `OK`, otherwise a `Cancelled` status saying why. The response side
    /// resolves it with a STATUS, the end of the response, or the buffer bound; without a
    /// deadline it waits as long as the server stays silent, like the call itself.
    pub(super) async fn ended(&mut self) -> Error {
        let wait = self.end.wait_for(|end| !matches!(end, End::Running));
        let end = if let Some(deadline) = self.deadline {
            match tokio::time::timeout_at(deadline, wait).await {
                Ok(end) => end,
                Err(_) => return Error::from(Status::deadline_exceeded("RPC deadline exceeded")),
            }
        } else {
            wait.await
        };
        end.ok()
            .and_then(|end| end.error())
            .unwrap_or_else(|| Error::from(Status::cancelled("request stream is closed")))
    }

    /// Marks a send as waiting for room in the request stream until the guard is dropped.
    pub(super) fn waiting_send(&self) -> WaitingSend {
        self.upload.send_modify(|upload| {
            upload.waiting = upload.waiting.saturating_add(1);
        });
        WaitingSend(Arc::clone(&self.upload))
    }

    /// Records that the request stream was dropped before the caller finished it.
    pub(super) fn upload_stopped(&self) {
        self.upload.send_if_modified(|upload| {
            let changed = !upload.stopped;
            upload.stopped = true;
            changed
        });
    }
}

/// A send that is waiting for room in the request stream.
pub(super) struct WaitingSend(Arc<watch::Sender<Upload>>);

impl Drop for WaitingSend {
    fn drop(&mut self) {
        self.0.send_modify(|upload| {
            upload.waiting = upload.waiting.saturating_sub(1);
        });
    }
}

/// Reads `response` in a task, recording the terminal STATUS in `end` as soon as it arrives.
/// Dropping the returned stream stops the task and drops `response`, which cancels the call.
pub(super) fn read_response(
    response: BoxStream<ResponseFrame>,
    end: EndSender,
    limits: ReadAhead,
) -> BoxStream<ResponseFrame> {
    let (frames, receiver) = mpsc::channel(QUEUED_FRAMES);
    let task = tokio::spawn(pump(response, end, frames, limits));
    Box::pin(ReadResponse { receiver, task })
}

const fn frame_bytes(item: &Result<ResponseFrame>) -> usize {
    match item {
        Ok(ResponseFrame::Message(body)) => body.len(),
        _ => 0,
    }
}

async fn pump(
    mut response: BoxStream<ResponseFrame>,
    mut end: EndSender,
    frames: mpsc::Sender<Result<ResponseFrame>>,
    limits: ReadAhead,
) {
    let mut backlog = VecDeque::new();
    let mut backlog_bytes = 0_usize;
    let mut reading = true;
    let mut watching_upload = true;
    loop {
        if !reading && backlog.is_empty() {
            break;
        }
        let upload = *end.upload.borrow();
        let full = backlog.len() >= limits.frames || backlog_bytes >= limits.bytes;
        if full && reading && upload.stopped {
            end.end(End::BacklogFull);
        }
        let may_read = reading
            && if upload.stopped || upload.waiting > 0 {
                !full
            } else {
                backlog.is_empty()
            };
        tokio::select! {
            biased;
            reserved = frames.reserve(), if !backlog.is_empty() => {
                let Ok(permit) = reserved else {
                    break;
                };
                if let Some(item) = backlog.pop_front() {
                    backlog_bytes = backlog_bytes.saturating_sub(frame_bytes(&item));
                    permit.send(item);
                }
            }
            changed = end.upload.changed(), if watching_upload => {
                watching_upload = changed.is_ok();
            }
            item = response.next(), if may_read => {
                if let Some(item) = item {
                    match &item {
                        Ok(ResponseFrame::Status(status)) => end.end(End::Status(status.clone())),
                        Err(error) => end.end(End::Failed(error.status())),
                        Ok(_) => {}
                    }
                    backlog_bytes = backlog_bytes.saturating_add(frame_bytes(&item));
                    backlog.push_back(item);
                } else {
                    reading = false;
                    end.end(End::Unknown);
                }
            }
        }
    }
}

struct ReadResponse {
    receiver: mpsc::Receiver<Result<ResponseFrame>>,
    task: JoinHandle<()>,
}

impl futures_core::Stream for ReadResponse {
    type Item = Result<ResponseFrame>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl Drop for ReadResponse {
    fn drop(&mut self) {
        self.task.abort();
    }
}
