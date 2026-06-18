use std::time::Duration;

use crate::framing::DEFAULT_MAX_FRAME_SIZE;
use crate::wire::{normalize_metadata_key, validate_metadata};
use crate::{Error, Metadata, Result, RpcRequest, RpcResponse, Status};
use prost::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallOptions {
    timeout: Option<Duration>,
    max_response_body_size: usize,
    metadata: Metadata,
}

impl Default for CallOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            max_response_body_size: DEFAULT_MAX_FRAME_SIZE,
            metadata: Metadata::new(),
        }
    }
}

impl CallOptions {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub const fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    #[must_use]
    pub const fn max_response_body_size(&self) -> usize {
        self.max_response_body_size
    }

    #[must_use]
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    #[must_use]
    pub const fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    #[must_use]
    pub const fn without_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    #[must_use]
    pub const fn with_max_response_body_size(mut self, max_response_body_size: usize) -> Self {
        self.max_response_body_size = max_response_body_size;
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        let key = key.into();
        self.metadata
            .insert(normalize_metadata_key(&key), value.into());
        self
    }

    #[must_use]
    pub fn with_metadata_map(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }
}

#[crate::async_trait]
pub trait RpcTransport: Clone + Send + Sync + 'static {
    async fn call(&self, request: RpcRequest) -> Result<RpcResponse>;
}

pub async fn unary<T, Req, Res>(
    transport: &T,
    service: &str,
    method: &str,
    request: &Req,
    options: CallOptions,
) -> Result<Res>
where
    T: RpcTransport,
    Req: Message,
    Res: Message + Default,
{
    let CallOptions {
        timeout,
        max_response_body_size,
        metadata,
    } = options;
    validate_metadata(&metadata).map_err(Error::from)?;

    let request = RpcRequest::new(service, method, request.encode_to_vec()).with_metadata(metadata);
    let response = if let Some(timeout) = timeout {
        tokio::time::timeout(timeout, transport.call(request))
            .await
            .map_err(|_| Error::from(Status::deadline_exceeded("RPC deadline exceeded")))??
    } else {
        transport.call(request).await?
    };

    validate_response_metadata(&response)?;

    if response.body.len() > max_response_body_size {
        return Err(Error::FrameTooLarge {
            len: response.body.len(),
            max: max_response_body_size,
        });
    }

    let status = Status::from_response(&response);
    if !status.is_ok() {
        return Err(Error::from(status));
    }

    Res::decode(response.body.as_slice()).map_err(Error::from)
}

fn validate_response_metadata(response: &RpcResponse) -> Result<()> {
    validate_metadata(&response.metadata).map_err(|status| {
        Error::from(Status::internal(format!(
            "invalid response metadata: {}",
            status.message()
        )))
    })
}

#[cfg(test)]
mod tests {
    use super::CallOptions;

    #[test]
    fn call_options_normalize_metadata_keys() {
        let options = CallOptions::new().with_metadata("Authorization", b"ok".to_vec());

        assert_eq!(
            options.metadata().get("authorization").map(Vec::as_slice),
            Some(&b"ok"[..])
        );
        assert!(!options.metadata().contains_key("Authorization"));
    }
}
