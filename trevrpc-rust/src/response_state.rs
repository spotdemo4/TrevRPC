use bytes::Bytes;
use trevrpc::{Code, Metadata, ResponseEnvelope, ResponseFrame, Status};

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

/// Enforces the response grammar `[HEADERS] MESSAGE* STATUS`: HEADERS at most once and before
/// anything else, nothing after STATUS, and an end of stream only after STATUS.
///
/// The terminal status is committed only once the end of the stream is seen, so a status that is
/// followed by anything but a clean end never counts.
#[derive(Default)]
pub struct ResponseState {
    /// A frame has arrived, so a HEADERS frame no longer fits the grammar.
    first_frame_seen: bool,
    pending_terminal_status: Option<Status>,
    terminal_status: Option<Status>,
    finished: bool,
}

impl ResponseState {
    pub(crate) fn ensure_open(&self) -> Result<(), ResponseStateFailure> {
        if self.finished || self.pending_terminal_status.is_some() || self.terminal_status.is_some()
        {
            return Err(ResponseStateFailure::new(
                ResponseStateFailureKind::TrailingFrame,
                Status::internal("response stream continued after terminal status"),
            ));
        }
        Ok(())
    }

    pub(crate) fn accept(
        &mut self,
        frame: ResponseFrame,
    ) -> Result<ResponseEvent, ResponseStateFailure> {
        self.ensure_open()?;
        let first_frame = !self.first_frame_seen;
        self.first_frame_seen = true;

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
                Ok(ResponseEvent::Headers(metadata))
            }
            ResponseFrame::Message(body) => Ok(ResponseEvent::Message(body)),
            ResponseFrame::Status(status) => {
                validate_metadata(status.metadata())?;
                self.pending_terminal_status = Some(status);
                Ok(ResponseEvent::Terminal)
            }
        }
    }

    pub(crate) fn finish(&mut self) -> Result<(), ResponseStateFailure> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        let Some(status) = self.pending_terminal_status.take() else {
            return Err(ResponseStateFailure::new(
                ResponseStateFailureKind::MissingTerminalStatus,
                Status::internal("response stream ended before final status"),
            ));
        };
        self.terminal_status = Some(status.clone());

        if status.code() != Code::Ok {
            return Err(ResponseStateFailure::new(
                ResponseStateFailureKind::RemoteStatus,
                status,
            ));
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) fn abort(&mut self) {
        self.pending_terminal_status = None;
        self.finished = true;
    }

    pub(crate) const fn terminal_status(&self) -> Option<&Status> {
        self.terminal_status.as_ref()
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

    use super::{ResponseEvent, ResponseState, ResponseStateFailureKind, UnaryResponse};

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
