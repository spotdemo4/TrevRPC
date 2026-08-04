use std::marker::PhantomData;

use prost::Message;
use trevrpc::{Code, RpcStreamFrame, RpcStreamFrameKind, Status};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseStateFailureKind {
    InvalidMetadata,
    MalformedProtobuf,
    MissingTerminalStatus,
    RemoteStatus,
    TrailingFrame,
    UnsupportedFrameKind,
}

#[allow(dead_code)]
impl ResponseStateFailureKind {
    pub(crate) const fn category(self) -> &'static str {
        match self {
            Self::InvalidMetadata => "invalid_metadata",
            Self::MalformedProtobuf => "malformed_protobuf",
            Self::MissingTerminalStatus => "missing_terminal_status",
            Self::RemoteStatus => "remote_status",
            Self::TrailingFrame => "trailing_frame",
            Self::UnsupportedFrameKind => "unsupported_frame_kind",
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct ResponseStateFailure {
    kind: ResponseStateFailureKind,
    status: Status,
}

#[allow(dead_code)]
impl ResponseStateFailure {
    fn new(kind: ResponseStateFailureKind, status: Status) -> Self {
        Self { kind, status }
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

pub(crate) enum ResponseStateEvent<T> {
    Message(T),
    Terminal,
}

pub(crate) struct ResponseState<T> {
    pending_terminal_status: Option<Status>,
    terminal_status: Option<Status>,
    finished: bool,
    _marker: PhantomData<T>,
}

impl<T> Default for ResponseState<T> {
    fn default() -> Self {
        Self {
            pending_terminal_status: None,
            terminal_status: None,
            finished: false,
            _marker: PhantomData,
        }
    }
}

impl<T> ResponseState<T>
where
    T: Message + Default,
{
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
        frame: &RpcStreamFrame,
    ) -> Result<ResponseStateEvent<T>, ResponseStateFailure> {
        self.ensure_open()?;

        if trevrpc::wire::validate_metadata(&frame.metadata).is_err() {
            return Err(ResponseStateFailure::new(
                ResponseStateFailureKind::InvalidMetadata,
                Status::internal("response stream contained invalid metadata"),
            ));
        }

        match frame.frame_kind() {
            Some(RpcStreamFrameKind::Message) => T::decode(frame.body.as_slice())
                .map(ResponseStateEvent::Message)
                .map_err(|_| {
                    ResponseStateFailure::new(
                        ResponseStateFailureKind::MalformedProtobuf,
                        Status::internal("response stream contained a malformed protobuf payload"),
                    )
                }),
            Some(RpcStreamFrameKind::Status) => {
                self.pending_terminal_status = Some(frame.status_value());
                Ok(ResponseStateEvent::Terminal)
            }
            None => Err(ResponseStateFailure::new(
                ResponseStateFailureKind::UnsupportedFrameKind,
                Status::invalid_argument("response stream contained an unknown frame kind"),
            )),
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

    pub(crate) fn terminal_status(&self) -> Option<&Status> {
        self.terminal_status.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use prost::Message;
    use trevrpc::{Code, RpcStreamFrame, Status};

    use super::{ResponseState, ResponseStateEvent, ResponseStateFailureKind};

    #[derive(Clone, PartialEq, Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[test]
    fn clean_eof_without_terminal_status_is_exact_internal_error() {
        let mut state = ResponseState::<TestMessage>::default();

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
        let mut state = ResponseState::<TestMessage>::default();
        let event = state
            .accept(&RpcStreamFrame::status(Status::ok()))
            .expect("terminal frame should be accepted");

        assert!(matches!(event, ResponseStateEvent::Terminal));
        assert!(state.terminal_status().is_none());
        state.finish().expect("clean EOF should commit terminal OK");
        assert!(state.terminal_status().is_some_and(Status::is_ok));
    }
}
