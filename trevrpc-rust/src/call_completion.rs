//! How a server call ends: the finish functions of its interceptors, its metrics and its log line.
//!
//! A [`CallCompletion`] belongs to the task that serves the call, not to the future that runs its
//! handler. That future is dropped whenever the call ends before it does, because the peer stopped
//! the response, the request failed, the deadline passed or the server shut down, and what it
//! owned would then be lost. The completion is created before the interceptors run, collects their
//! finish functions as they return, and reports the call exactly once: with the outcome its owner
//! gives it, or, when the owner drops it first, with the outcome that the call's cancellation
//! source implies.
//!
//! A call that writes its response reports only once the write and the end of the response have
//! completed: until then its outcome is only the one it means to report
//! ([`CallCompletion::conclude`]), and a write that is interrupted, or fails, reports why
//! instead. The metrics and the finish functions therefore never hear `Ok` for a response that
//! did not go out.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::server::{
    CancellationSource, CancellationToken, Finish, Metrics, MetricsPath, RpcFinished,
    record_rpc_started, split_or_empty, wire_status,
};
use crate::{Code, ResponseEnvelope, ResponseFrame, Result, Status};

/// Reports the end of one call, once.
pub(crate) struct CallCompletion {
    metrics: Arc<dyn Metrics>,
    path: Arc<str>,
    /// Whether the server registers `path`.
    registered: bool,
    /// Whether the call is a streaming one, which only changes its log line.
    streaming: bool,
    request_body_len: usize,
    started_at: Instant,
    /// What ends the call, `None` when nothing cancels it.
    cancellation: Option<CancellationToken>,
    /// The call's deadline, once it is admitted.
    deadline: Option<Instant>,
    /// The span the call runs in, which the log events of its end belong to.
    span: tracing::Span,
    /// The finish functions of the call's interceptors, in the order they returned.
    finishes: Vec<Finish>,
    response_body_len: usize,
    state: State,
}

/// How far the report of a call has got.
enum State {
    /// The outcome is reported as soon as it is settled.
    Open,
    /// The call reports after its response is written, and has settled no outcome yet.
    AwaitingWrite,
    /// The call reports after its response is written, and has settled its outcome: the status it
    /// failed with, or `None` if it succeeded. [`CallCompletion::commit`] reports it.
    Concluded(Option<Status>),
    /// The call has been reported.
    Reported,
}

impl CallCompletion {
    /// Starts reporting a call of `path`, which tells the metrics that it started.
    pub(crate) fn start(
        metrics: Arc<dyn Metrics>,
        path: Arc<str>,
        registered: bool,
        streaming: bool,
        request_body_len: usize,
        cancellation: Option<CancellationToken>,
    ) -> Self {
        record_rpc_started(
            metrics.as_ref(),
            MetricsPath {
                path: &path,
                registered,
            },
            request_body_len,
        );
        Self {
            metrics,
            path,
            registered,
            streaming,
            request_body_len,
            started_at: Instant::now(),
            cancellation,
            deadline: None,
            span: tracing::Span::none(),
            finishes: Vec::new(),
            response_body_len: 0,
            state: State::Open,
        }
    }

    /// Holds the outcome of [`Self::conclude`] until [`Self::commit`], which the writer of the
    /// call's response calls after the response went out.
    pub(crate) fn report_after_write(&mut self) {
        if matches!(self.state, State::Open) {
            self.state = State::AwaitingWrite;
        }
    }

    /// The method path of the call.
    pub(crate) fn path(&self) -> Arc<str> {
        Arc::clone(&self.path)
    }

    /// The call's deadline, once [`Self::set_deadline`] has been told it.
    pub(crate) const fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    pub(crate) const fn set_deadline(&mut self, deadline: Option<Instant>) {
        self.deadline = deadline;
    }

    /// Sets the span the call runs in, which the log events of its end belong to.
    pub(crate) fn set_span(&mut self, span: tracing::Span) {
        self.span = span;
    }

    /// Adds the finish function of an interceptor that has run, which runs when the call ends.
    pub(crate) fn add_finish(&mut self, finish: Finish) {
        self.finishes.push(finish);
    }

    /// Ends the call, which failed with `error` unless that is `None`. Only the first end counts.
    pub(crate) fn finish(&mut self, error: Option<&Status>) {
        if matches!(
            std::mem::replace(&mut self.state, State::Reported),
            State::Reported
        ) {
            return;
        }
        let finishes = std::mem::take(&mut self.finishes);
        let elapsed = self.started_at.elapsed();
        self.span.in_scope(|| {
            record_rpc_finished(
                self.metrics.as_ref(),
                MetricsPath {
                    path: &self.path,
                    registered: self.registered,
                },
                self.request_body_len,
                self.response_body_len,
                error,
                elapsed,
                finishes,
            );
            log_rpc_finished(
                &self.path,
                self.request_body_len,
                self.response_body_len,
                error,
                elapsed,
                self.streaming,
            );
        });
    }

    /// Settles the outcome of the call, which failed with `error` unless that is `None`. It is
    /// reported at once, unless the call reports after its write: then [`Self::commit`] reports
    /// it, and the first outcome settled stays.
    pub(crate) fn conclude(&mut self, error: Option<&Status>) {
        match self.state {
            State::Open => self.finish(error),
            State::AwaitingWrite => self.state = State::Concluded(error.cloned()),
            State::Concluded(_) | State::Reported => {}
        }
    }

    /// Reports the outcome the call settled, now that its response has gone out. Does nothing for
    /// a call that was reported already, or settled nothing.
    pub(crate) fn commit(&mut self) {
        match std::mem::replace(&mut self.state, State::AwaitingWrite) {
            State::Concluded(error) => self.finish(error.as_ref()),
            other => self.state = other,
        }
    }

    /// Ends the call because it was cancelled: with the code that the first cancellation source
    /// stands for, or `Cancelled` when nothing cancelled it, as when its owner gives up on it.
    pub(crate) fn cancelled(&mut self) {
        if matches!(self.state, State::Reported) {
            return;
        }
        let source = self
            .cancellation
            .as_ref()
            .and_then(CancellationToken::source);
        self.finish(Some(&cancelled_status(source)));
    }

    /// Ends a unary call with its `result` and returns it as the caller gets it.
    pub(crate) fn finish_response(
        &mut self,
        result: std::result::Result<ResponseEnvelope<Vec<u8>>, Status>,
    ) -> std::result::Result<ResponseEnvelope<Vec<u8>>, Status> {
        if let Ok(response) = &result {
            self.response_body_len = response.message().len();
        }
        self.conclude(result.as_ref().err());
        result.map_err(wire_status)
    }

    /// Takes a response frame a streaming call has produced, and returns the frame to send.
    ///
    /// The terminal STATUS ends the call: an error stands for a failed STATUS, and any status the
    /// call ended with goes out as [`wire_status`] makes it, so that nothing reaches the peer
    /// without redaction. A status after the first end only has that redaction applied.
    pub(crate) fn frame(&mut self, frame: Result<ResponseFrame>) -> ResponseFrame {
        match frame {
            Ok(ResponseFrame::Message(body)) => {
                self.response_body_len = self.response_body_len.saturating_add(body.len());
                ResponseFrame::Message(body)
            }
            Ok(frame @ ResponseFrame::Headers(_)) => frame,
            Ok(ResponseFrame::Status(status)) => self.finish_status(status),
            Err(error) => self.finish_status(error.into_status()),
        }
    }

    fn finish_status(&mut self, status: Status) -> ResponseFrame {
        self.conclude((status.code() != Code::Ok).then_some(&status));
        ResponseFrame::Status(wire_status(status))
    }
}

impl Drop for CallCompletion {
    fn drop(&mut self) {
        self.cancelled();
    }
}

/// The status that stands for a call a cancellation ended, which is not sent: the peer, the
/// deadline or the server ended the call, so there is no one to tell.
fn cancelled_status(source: Option<CancellationSource>) -> Status {
    match source {
        Some(CancellationSource::Deadline) => Status::deadline_exceeded("RPC deadline exceeded"),
        Some(CancellationSource::PeerReset) => Status::cancelled("RPC cancelled by the peer"),
        Some(CancellationSource::ConnectionLost) => Status::unavailable("connection lost"),
        Some(CancellationSource::ServerShutdown) => Status::unavailable("server is shutting down"),
        None => Status::cancelled("RPC cancelled"),
    }
}

/// Reports a finished call to the metrics and runs the finish functions of its interceptors, the
/// last added first.
pub(crate) fn record_rpc_finished(
    metrics: &dyn Metrics,
    path: MetricsPath<'_>,
    request_body_len: usize,
    response_body_len: usize,
    error: Option<&Status>,
    elapsed: Duration,
    finishes: Vec<Finish>,
) {
    let enabled = metrics.enabled();
    if !enabled && finishes.is_empty() {
        return;
    }

    let (service, method) = split_or_empty(path.path);
    let event = RpcFinished {
        service: service.to_owned(),
        method: method.to_owned(),
        request_body_len,
        response_body_len,
        code: error.map_or(Code::Ok, Status::code),
        elapsed,
        error: error.cloned(),
        registered: path.registered,
    };
    for finish in finishes.into_iter().rev() {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| finish(&event))).is_err() {
            tracing::error!(service, method, "RPC interceptor finish panicked");
        }
    }
    if enabled {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            metrics.rpc_finished(&event);
        }));
    }
}

/// Logs the end of a call, with the status that failed it, if any, and reports a failure with an
/// `Internal` status as an error: the caller is told only that the server failed, so this is
/// where its cause is recorded, in the span of the call if the caller is in it.
fn log_rpc_finished(
    path: &str,
    request_body_len: usize,
    response_body_len: usize,
    error: Option<&Status>,
    elapsed: Duration,
    streaming: bool,
) {
    let (service, method) = split_or_empty(path);
    let code = error.map_or(Code::Ok, Status::code);
    let cause = error.map(tracing::field::display);
    if streaming {
        tracing::info!(
            service,
            method,
            request_body_len,
            response_body_len,
            status = ?code,
            error = cause,
            elapsed_ms = elapsed.as_millis(),
            "streaming rpc finished"
        );
    } else {
        tracing::info!(
            service,
            method,
            request_body_len,
            response_body_len,
            status = ?code,
            error = cause,
            elapsed_ms = elapsed.as_millis(),
            "rpc finished"
        );
    }
    if let Some(error) = error.filter(|error| error.code() == Code::Internal) {
        tracing::error!(service, method, %error, "RPC failed with an internal error");
    }
}

/// Wraps the frames of a streaming call, reporting its end as the terminal STATUS passes, or as
/// the stream is dropped.
pub(crate) struct CompletedFrames {
    frames: crate::BoxStream<ResponseFrame>,
    completion: CallCompletion,
}

impl CompletedFrames {
    pub(crate) const fn new(
        frames: crate::BoxStream<ResponseFrame>,
        completion: CallCompletion,
    ) -> Self {
        Self { frames, completion }
    }
}

impl futures_core::Stream for CompletedFrames {
    type Item = Result<ResponseFrame>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.frames
            .as_mut()
            .poll_next(cx)
            .map(|item| item.map(|frame| Ok(this.completion.frame(frame))))
    }
}
