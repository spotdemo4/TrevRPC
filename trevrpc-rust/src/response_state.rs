use std::io;

use bytes::Bytes;
use trevrpc::{Code, Metadata, ResponseEnvelope, ResponseFrame, Status};

use crate::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResponseStateFailureKind {
    InvalidMetadata,
    MalformedProtobuf,
    MisplacedHeaders,
    MissingTerminalStatus,
    RemoteStatus,
    TrailingFrame,
}

#[allow(dead_code)]
impl ResponseStateFailureKind {
    pub(crate) const fn category(self) -> &'static str {
        match self {
            Self::InvalidMetadata => "invalid_metadata",
            Self::MalformedProtobuf => "malformed_protobuf",
            Self::MisplacedHeaders => "misplaced_headers",
            Self::MissingTerminalStatus => "missing_terminal_status",
            Self::RemoteStatus => "remote_status",
            Self::TrailingFrame => "trailing_frame",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct ResponseStateFailure {
    kind: ResponseStateFailureKind,
    status: Status,
}

#[allow(dead_code)]
impl ResponseStateFailure {
    const fn new(kind: ResponseStateFailureKind, status: Status) -> Self {
        Self { kind, status }
    }

    /// The failure for a frame that follows STATUS.
    pub(crate) fn trailing_frame() -> Self {
        Self::new(
            ResponseStateFailureKind::TrailingFrame,
            Status::internal("response stream continued after terminal status"),
        )
    }

    /// The failure for a MESSAGE payload that does not decode as the expected protobuf message.
    pub(crate) fn malformed_protobuf() -> Self {
        Self::new(
            ResponseStateFailureKind::MalformedProtobuf,
            Status::internal("response stream contained a malformed protobuf payload"),
        )
    }

    pub(crate) const fn kind(&self) -> ResponseStateFailureKind {
        self.kind
    }

    pub(crate) const fn status(&self) -> &Status {
        &self.status
    }

    pub(crate) fn into_status(self) -> Status {
        self.status
    }
}

/// What one response frame means for the call.
pub enum ResponseEvent {
    Headers(Metadata),
    Message(Bytes),
    Terminal,
}

/// Where a response stands in the grammar `[HEADERS] MESSAGE* STATUS`: HEADERS at most once and
/// before anything else, nothing after STATUS, and an end of stream only after STATUS.
///
/// The terminal status is committed only once the end of the stream is seen, so a status that is
/// followed by anything but a clean end never counts.
#[derive(Default)]
pub enum ResponseState {
    /// No frame has arrived yet.
    #[default]
    AwaitingFirst,
    /// A HEADERS or MESSAGE frame has arrived, so a HEADERS frame no longer fits the grammar.
    Streaming,
    /// STATUS has arrived; only the clean end of the stream may follow.
    StatusPending(Status),
    /// The response is over. Holds the terminal status if the stream ended cleanly after it, and
    /// nothing if the call failed or was aborted first.
    Done(Option<Status>),
}

impl ResponseState {
    /// Whether the response is over, whether it completed or failed.
    pub(crate) const fn is_done(&self) -> bool {
        matches!(self, Self::Done(_))
    }

    /// Rejects any frame that arrives once STATUS has, or the response is over.
    pub(crate) fn ensure_open(&self) -> Result<(), ResponseStateFailure> {
        match self {
            Self::AwaitingFirst | Self::Streaming => Ok(()),
            Self::StatusPending(_) | Self::Done(_) => Err(ResponseStateFailure::trailing_frame()),
        }
    }

    pub(crate) fn accept(
        &mut self,
        frame: ResponseFrame,
    ) -> Result<ResponseEvent, ResponseStateFailure> {
        self.ensure_open()?;
        let event = self.advance(frame);
        if event.is_err() {
            self.abort();
        }
        event
    }

    fn advance(&mut self, frame: ResponseFrame) -> Result<ResponseEvent, ResponseStateFailure> {
        let first_frame = matches!(self, Self::AwaitingFirst);
        match frame {
            ResponseFrame::Headers(metadata) => {
                if !first_frame {
                    return Err(ResponseStateFailure::new(
                        ResponseStateFailureKind::MisplacedHeaders,
                        Status::internal(
                            "response stream sent HEADERS after another response frame",
                        ),
                    ));
                }
                validate_metadata(&metadata)?;
                *self = Self::Streaming;
                Ok(ResponseEvent::Headers(metadata))
            }
            ResponseFrame::Message(body) => {
                *self = Self::Streaming;
                Ok(ResponseEvent::Message(body))
            }
            ResponseFrame::Status(status) => {
                validate_metadata(status.metadata())?;
                *self = Self::StatusPending(status);
                Ok(ResponseEvent::Terminal)
            }
        }
    }

    /// Ends the response at the clean end of the stream, committing the terminal status.
    ///
    /// A non-OK status is returned as the failure. Does nothing if the response is already over.
    pub(crate) fn finish(&mut self) -> Result<(), ResponseStateFailure> {
        match std::mem::replace(self, Self::Done(None)) {
            Self::Done(status) => {
                *self = Self::Done(status);
                Ok(())
            }
            Self::AwaitingFirst | Self::Streaming => Err(ResponseStateFailure::new(
                ResponseStateFailureKind::MissingTerminalStatus,
                Status::internal("response stream ended before final status"),
            )),
            Self::StatusPending(status) => {
                *self = Self::Done(Some(status.clone()));
                if status.code() == Code::Ok {
                    Ok(())
                } else {
                    Err(ResponseStateFailure::new(
                        ResponseStateFailureKind::RemoteStatus,
                        status,
                    ))
                }
            }
        }
    }

    /// Ends the response without a terminal status, dropping one that is still pending. Does
    /// nothing to a status already committed.
    pub(crate) fn abort(&mut self) {
        if !matches!(self, Self::Done(Some(_))) {
            *self = Self::Done(None);
        }
    }

    /// Classifies a failure to read the next frame: one that interrupts the data after STATUS is
    /// malformed trailing data, whatever read it. Pass every read failure through here.
    pub(crate) fn read_failed(&self, error: Error) -> Error {
        match self {
            Self::StatusPending(_) => trailing_read_error(error),
            _ => error,
        }
    }

    /// The terminal status, once the stream ended cleanly after it.
    pub(crate) const fn terminal_status(&self) -> Option<&Status> {
        match self {
            Self::Done(status) => status.as_ref(),
            _ => None,
        }
    }
}

/// Requires the stream to end after its terminal status, given what it produced next.
///
/// The one place that classifies what follows STATUS, shared by every reader: a known frame is a
/// protocol error, and a read that fails is malformed trailing data unless the transport itself
/// failed (which stays retryable).
pub(crate) fn require_end<T>(next: Option<crate::Result<T>>) -> crate::Result<()> {
    match next {
        None => Ok(()),
        Some(Ok(_)) => Err(Error::from(
            ResponseStateFailure::trailing_frame().into_status(),
        )),
        Some(Err(error)) => Err(trailing_read_error(error)),
    }
}

fn trailing_read_error(error: Error) -> Error {
    let malformed_trailing = match &error {
        Error::Decode(_) | Error::FrameTooLarge { .. } => true,
        Error::Status(status) => matches!(
            status.code(),
            Code::InvalidArgument | Code::ResourceExhausted | Code::Internal | Code::DataLoss
        ),
        Error::Transport(error) => error.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof
            )
        }),
        Error::Encode(_) => false,
    };

    if malformed_trailing {
        Error::from(Status::internal(
            "response stream contained malformed trailing data after terminal status",
        ))
    } else {
        error
    }
}

fn validate_metadata(metadata: &Metadata) -> Result<(), ResponseStateFailure> {
    trevrpc::wire::validate_metadata(metadata).map_err(|_| {
        ResponseStateFailure::new(
            ResponseStateFailureKind::InvalidMetadata,
            Status::internal("response stream contained invalid metadata"),
        )
    })
}

/// Collects a unary response: `[HEADERS] MESSAGE STATUS`.
///
/// Used by transports that read a unary response frame by frame.
#[derive(Default)]
pub struct UnaryResponse {
    state: ResponseState,
    headers: Metadata,
    message: Option<Bytes>,
}

impl UnaryResponse {
    /// Feeds one frame. Returns whether it was the terminal status, after which only the clean
    /// end of the stream remains.
    pub(crate) fn accept(&mut self, frame: ResponseFrame) -> Result<bool, Status> {
        match self
            .state
            .accept(frame)
            .map_err(ResponseStateFailure::into_status)?
        {
            ResponseEvent::Headers(headers) => self.headers = headers,
            ResponseEvent::Message(body) => {
                if self.message.replace(body).is_some() {
                    return Err(Status::internal(
                        "unary response contained more than one message",
                    ));
                }
            }
            ResponseEvent::Terminal => return Ok(true),
        }
        Ok(false)
    }

    /// Completes the call at the end of the stream.
    ///
    /// A non-OK status becomes the error, carrying its trailing metadata.
    pub(crate) fn finish(mut self) -> Result<ResponseEnvelope<Bytes>, Status> {
        self.state
            .finish()
            .map_err(ResponseStateFailure::into_status)?;
        let metadata = self
            .state
            .terminal_status()
            .map(|status| status.metadata().clone())
            .unwrap_or_default();
        let message = self
            .message
            .ok_or_else(|| Status::internal("unary response contained no message"))?;
        Ok(ResponseEnvelope::new(message)
            .with_headers(self.headers)
            .with_metadata(metadata))
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use trevrpc::{Code, Metadata, ResponseFrame, Status};

    use super::{
        ResponseEvent, ResponseState, ResponseStateFailureKind, UnaryResponse, require_end,
    };

    fn headers() -> ResponseFrame {
        let mut metadata = Metadata::new();
        metadata.insert("x-a".to_owned(), b"1".to_vec());
        ResponseFrame::Headers(metadata)
    }

    fn message() -> ResponseFrame {
        ResponseFrame::message(Bytes::from_static(b"m"))
    }

    #[test]
    fn clean_eof_without_terminal_status_is_exact_internal_error() {
        let mut state = ResponseState::default();

        let failure = state
            .finish()
            .expect_err("clean EOF without status must fail");

        assert_eq!(
            failure.kind(),
            ResponseStateFailureKind::MissingTerminalStatus
        );
        assert_eq!(failure.status().code(), Code::Internal);
        assert_eq!(
            failure.status().message(),
            "response stream ended before final status"
        );
    }

    #[test]
    fn terminal_status_is_committed_only_at_clean_eof() {
        let mut state = ResponseState::default();
        let event = state
            .accept(ResponseFrame::Status(Status::ok()))
            .expect("terminal frame should be accepted");

        assert!(matches!(event, ResponseEvent::Terminal));
        assert!(state.terminal_status().is_none());
        state.finish().expect("clean EOF should commit terminal OK");
        assert!(state.terminal_status().is_some_and(Status::is_ok));
    }

    #[test]
    fn headers_are_accepted_once_and_only_first() {
        let mut state = ResponseState::default();
        assert!(matches!(
            state.accept(headers()),
            Ok(ResponseEvent::Headers(_))
        ));
        assert!(matches!(
            state.accept(message()),
            Ok(ResponseEvent::Message(_))
        ));

        let mut state = ResponseState::default();
        state.accept(headers()).expect("first HEADERS is fine");
        let failure = state
            .accept(headers())
            .err()
            .expect("second HEADERS must be rejected");
        assert_eq!(failure.kind(), ResponseStateFailureKind::MisplacedHeaders);
        assert_eq!(failure.status().code(), Code::Internal);

        let mut state = ResponseState::default();
        state.accept(message()).expect("message is fine");
        let failure = state
            .accept(headers())
            .err()
            .expect("HEADERS after MESSAGE must be rejected");
        assert_eq!(failure.kind(), ResponseStateFailureKind::MisplacedHeaders);

        let mut state = ResponseState::default();
        state
            .accept(ResponseFrame::Status(Status::ok()))
            .expect("status is fine");
        let failure = state
            .accept(headers())
            .err()
            .expect("HEADERS after STATUS must be rejected");
        assert_eq!(failure.kind(), ResponseStateFailureKind::TrailingFrame);
    }

    #[test]
    fn frames_after_the_terminal_status_are_rejected() {
        let mut state = ResponseState::default();
        state
            .accept(ResponseFrame::Status(Status::ok()))
            .expect("status is fine");
        let failure = state
            .accept(message())
            .err()
            .expect("MESSAGE after STATUS must be rejected");
        assert_eq!(failure.kind(), ResponseStateFailureKind::TrailingFrame);
    }

    #[test]
    fn abort_drops_a_pending_status_and_rejects_further_frames() {
        let mut state = ResponseState::default();
        state.accept(ResponseFrame::Status(Status::ok())).unwrap();
        state.abort();

        let failure = state.accept(message()).err().expect("rejected");
        assert_eq!(failure.kind(), ResponseStateFailureKind::TrailingFrame);
        assert!(
            state.finish().is_ok(),
            "finishing an aborted response is a no-op"
        );
        assert!(state.terminal_status().is_none());
        assert!(state.is_done());
    }

    #[test]
    fn abort_keeps_a_committed_status() {
        let mut state = ResponseState::default();
        state.accept(ResponseFrame::Status(Status::ok())).unwrap();
        state.finish().unwrap();
        state.abort();

        assert!(state.terminal_status().is_some_and(Status::is_ok));
    }

    #[test]
    fn a_rejected_frame_ends_the_response() {
        let mut state = ResponseState::default();
        state.accept(message()).unwrap();
        state.accept(headers()).err().expect("misplaced HEADERS");

        assert!(state.is_done());
        assert!(state.terminal_status().is_none());
    }

    #[test]
    fn a_failed_call_commits_its_status_at_the_end_of_the_stream() {
        let mut state = ResponseState::default();
        state
            .accept(ResponseFrame::Status(Status::new(Code::NotFound, "gone")))
            .unwrap();
        let failure = state.finish().expect_err("non-OK status fails");

        assert_eq!(failure.kind(), ResponseStateFailureKind::RemoteStatus);
        assert_eq!(
            state.terminal_status().map(Status::code),
            Some(Code::NotFound)
        );
        assert!(state.finish().is_ok(), "the failure is reported once");
    }

    #[test]
    fn read_failures_are_malformed_trailing_data_only_after_the_status() {
        let truncated = || crate::framed::unexpected_eof();
        let text = |error: crate::Error| error.into_status().message().to_owned();

        let mut state = ResponseState::default();
        state.accept(message()).unwrap();
        assert!(text(state.read_failed(truncated())).starts_with("transport unavailable"));

        state.accept(ResponseFrame::Status(Status::ok())).unwrap();
        assert_eq!(
            text(state.read_failed(truncated())),
            "response stream contained malformed trailing data after terminal status"
        );
    }

    #[test]
    fn nothing_may_follow_the_terminal_status_but_the_end() {
        let end = |next: Option<crate::Result<()>>| require_end(next);

        assert!(end(None).is_ok());

        let status = end(Some(Ok(()))).unwrap_err().into_status();
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(
            status.message(),
            "response stream continued after terminal status"
        );

        let reset =
            crate::Error::transport(std::io::Error::from(std::io::ErrorKind::ConnectionReset));
        assert_eq!(
            end(Some(Err(reset))).unwrap_err().into_status().code(),
            Code::Unavailable
        );
    }

    #[test]
    fn invalid_header_and_trailing_metadata_are_internal_errors() {
        let mut bad = Metadata::new();
        bad.insert("Bad Key".to_owned(), Vec::new());

        for frame in [
            ResponseFrame::Headers(bad.clone()),
            ResponseFrame::Status(Status::ok().with_metadata(bad.clone())),
        ] {
            let failure = ResponseState::default()
                .accept(frame)
                .err()
                .expect("invalid metadata must be rejected");
            assert_eq!(failure.kind(), ResponseStateFailureKind::InvalidMetadata);
            assert_eq!(failure.status().code(), Code::Internal);
        }
    }

    #[test]
    fn unary_response_collects_headers_message_and_trailers() {
        let mut trailers = Metadata::new();
        trailers.insert("x-t".to_owned(), b"2".to_vec());
        let mut response = UnaryResponse::default();
        assert_eq!(response.accept(headers()), Ok(false));
        assert_eq!(response.accept(message()), Ok(false));
        assert_eq!(
            response.accept(ResponseFrame::Status(
                Status::ok().with_metadata(trailers.clone())
            )),
            Ok(true)
        );

        let envelope = response.finish().expect("response should complete");
        assert_eq!(envelope.message(), &Bytes::from_static(b"m"));
        assert_eq!(envelope.headers().get("x-a"), Some(&b"1".to_vec()));
        assert_eq!(envelope.metadata(), &trailers);
    }

    #[test]
    fn unary_response_requires_exactly_one_message() {
        let mut response = UnaryResponse::default();
        response.accept(message()).expect("first message");
        let status = response
            .accept(message())
            .expect_err("second message must be rejected");
        assert_eq!(status.code(), Code::Internal);

        let mut response = UnaryResponse::default();
        response
            .accept(ResponseFrame::Status(Status::ok()))
            .expect("status");
        let status = response.finish().expect_err("no message is an error");
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn unary_failure_carries_the_status_and_its_metadata() {
        let mut trailers = Metadata::new();
        trailers.insert("x-t".to_owned(), b"2".to_vec());
        let mut response = UnaryResponse::default();
        response
            .accept(ResponseFrame::Status(
                Status::new(Code::NotFound, "gone").with_metadata(trailers.clone()),
            ))
            .expect("status");
        let status = response.finish().expect_err("failure expected");
        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.metadata(), &trailers);
    }
}
