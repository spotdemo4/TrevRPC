use std::marker::PhantomData;

use prost::Message;

use crate::{Error, Result};

#[crate::async_trait]
pub trait MessageStream<T>: Send {
    /// Returns the next message from the stream, or `None` after the stream finishes.
    async fn next(&mut self) -> Option<Result<T>>;

    /// Returns true when calling `next` is expected to complete immediately.
    ///
    /// Transports use this to batch ready stream items without adding latency to
    /// streams backed by asynchronous producers.
    fn is_non_blocking(&self) -> bool {
        false
    }

    /// Drains immediately ready items into `out` without awaiting one future per item.
    ///
    /// Returns `Ok(true)` when the stream is exhausted. The default implementation
    /// drains nothing, preserving compatibility for streams backed by async work.
    fn drain_ready(&mut self, limit: usize, out: &mut Vec<T>) -> Result<bool> {
        let _ = limit;
        let _ = out;
        Ok(false)
    }
}

pub type BoxMessageStream<T> = Box<dyn MessageStream<T> + Send + 'static>;

pub struct EmptyStream<T> {
    _marker: PhantomData<T>,
}

impl<T> Default for EmptyStream<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

#[crate::async_trait]
impl<T> MessageStream<T> for EmptyStream<T>
where
    T: Send + 'static,
{
    async fn next(&mut self) -> Option<Result<T>> {
        None
    }

    fn is_non_blocking(&self) -> bool {
        true
    }

    fn drain_ready(&mut self, limit: usize, out: &mut Vec<T>) -> Result<bool> {
        let _ = limit;
        let _ = out;
        Ok(true)
    }
}

/// Returns an empty boxed message stream.
#[must_use]
pub fn empty<T>() -> BoxMessageStream<T>
where
    T: Send + 'static,
{
    Box::new(EmptyStream::default())
}

pub(crate) struct PrefixedStream<T> {
    first: Option<Result<T>>,
    inner: BoxMessageStream<T>,
}

impl<T> PrefixedStream<T> {
    pub(crate) fn new(first: Result<T>, inner: BoxMessageStream<T>) -> Self {
        Self {
            first: Some(first),
            inner,
        }
    }
}

#[crate::async_trait]
impl<T> MessageStream<T> for PrefixedStream<T>
where
    T: Send + 'static,
{
    async fn next(&mut self) -> Option<Result<T>> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }

        self.inner.next().await
    }

    fn is_non_blocking(&self) -> bool {
        self.first.is_some() || self.inner.is_non_blocking()
    }

    fn drain_ready(&mut self, limit: usize, out: &mut Vec<T>) -> Result<bool> {
        if out.len() >= limit {
            return Ok(false);
        }
        if let Some(first) = self.first.take() {
            out.push(first?);
        }
        if out.len() >= limit {
            return Ok(false);
        }
        self.inner.drain_ready(limit, out)
    }
}

pub(crate) fn prefixed<T>(first: Result<T>, inner: BoxMessageStream<T>) -> BoxMessageStream<T>
where
    T: Send + 'static,
{
    Box::new(PrefixedStream::new(first, inner))
}

pub struct IterStream<I> {
    iter: I,
}

impl<I> IterStream<I> {
    /// Creates a stream backed by an iterator.
    pub const fn new(iter: I) -> Self {
        Self { iter }
    }
}

#[crate::async_trait]
impl<T, I> MessageStream<T> for IterStream<I>
where
    T: Send + 'static,
    I: Iterator<Item = T> + Send,
{
    async fn next(&mut self) -> Option<Result<T>> {
        self.iter.next().map(Ok)
    }

    fn is_non_blocking(&self) -> bool {
        true
    }

    fn drain_ready(&mut self, limit: usize, out: &mut Vec<T>) -> Result<bool> {
        while out.len() < limit {
            let Some(item) = self.iter.next() else {
                return Ok(true);
            };
            out.push(item);
        }
        Ok(false)
    }
}

/// Returns a boxed message stream that yields items from an iterator.
#[must_use]
pub fn from_iter<T, I>(iter: I) -> BoxMessageStream<T>
where
    T: Send + 'static,
    I: IntoIterator<Item = T>,
    I::IntoIter: Send + 'static,
{
    Box::new(IterStream::new(iter.into_iter()))
}

pub struct EncodeStream<T> {
    inner: BoxMessageStream<T>,
}

impl<T> EncodeStream<T> {
    /// Creates a stream that encodes protobuf messages into byte bodies.
    #[must_use]
    pub fn new(inner: BoxMessageStream<T>) -> Self {
        Self { inner }
    }
}

#[crate::async_trait]
impl<T> MessageStream<Vec<u8>> for EncodeStream<T>
where
    T: Message + Send + 'static,
{
    async fn next(&mut self) -> Option<Result<Vec<u8>>> {
        match self.inner.next().await {
            Some(Ok(message)) => Some(Ok(message.encode_to_vec())),
            Some(Err(error)) => Some(Err(error)),
            None => None,
        }
    }

    fn is_non_blocking(&self) -> bool {
        self.inner.is_non_blocking()
    }

    fn drain_ready(&mut self, limit: usize, out: &mut Vec<Vec<u8>>) -> Result<bool> {
        let remaining = limit.saturating_sub(out.len());
        if remaining == 0 {
            return Ok(false);
        }
        let mut messages = Vec::with_capacity(remaining);
        let done = self.inner.drain_ready(remaining, &mut messages)?;
        out.extend(messages.into_iter().map(|message| message.encode_to_vec()));
        Ok(done)
    }
}

/// Wraps a message stream and encodes each protobuf message into bytes.
#[must_use]
pub fn encode<T>(inner: BoxMessageStream<T>) -> BoxMessageStream<Vec<u8>>
where
    T: Message + Send + 'static,
{
    Box::new(EncodeStream::new(inner))
}

pub struct DecodeStream<T> {
    inner: BoxMessageStream<Vec<u8>>,
    _marker: PhantomData<T>,
}

impl<T> DecodeStream<T> {
    /// Creates a stream that decodes byte bodies into protobuf messages.
    #[must_use]
    pub fn new(inner: BoxMessageStream<Vec<u8>>) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

#[crate::async_trait]
impl<T> MessageStream<T> for DecodeStream<T>
where
    T: Message + Default + Send + 'static,
{
    async fn next(&mut self) -> Option<Result<T>> {
        match self.inner.next().await {
            Some(Ok(body)) => Some(T::decode(body.as_slice()).map_err(Error::from)),
            Some(Err(error)) => Some(Err(error)),
            None => None,
        }
    }

    fn is_non_blocking(&self) -> bool {
        self.inner.is_non_blocking()
    }

    fn drain_ready(&mut self, limit: usize, out: &mut Vec<T>) -> Result<bool> {
        let remaining = limit.saturating_sub(out.len());
        if remaining == 0 {
            return Ok(false);
        }
        let mut bodies = Vec::with_capacity(remaining);
        let done = self.inner.drain_ready(remaining, &mut bodies)?;
        for body in bodies {
            out.push(T::decode(body.as_slice()).map_err(Error::from)?);
        }
        Ok(done)
    }
}

/// Wraps a byte stream and decodes each body into a protobuf message.
#[must_use]
pub fn decode<T>(inner: BoxMessageStream<Vec<u8>>) -> BoxMessageStream<T>
where
    T: Message + Default + Send + 'static,
{
    Box::new(DecodeStream::new(inner))
}

#[cfg(test)]
mod tests {
    use super::{decode, encode, from_iter};

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    #[tokio::test]
    async fn encode_decode_stream_round_trips_messages() {
        let messages = from_iter([TestMessage {
            value: "hello".to_owned(),
        }]);
        let encoded = encode(messages);
        let mut decoded = decode::<TestMessage>(encoded);

        assert_eq!(
            decoded.next().await.expect("stream should yield").unwrap(),
            TestMessage {
                value: "hello".to_owned(),
            }
        );
        assert!(decoded.next().await.is_none());
    }

    #[test]
    fn ready_drain_encodes_and_decodes_nonblocking_streams() {
        let messages = from_iter([
            TestMessage {
                value: "one".to_owned(),
            },
            TestMessage {
                value: "two".to_owned(),
            },
        ]);
        let mut encoded = encode(messages);
        let mut bodies = Vec::new();

        assert!(encoded.drain_ready(8, &mut bodies).unwrap());
        assert_eq!(bodies.len(), 2);

        let mut decoded = decode::<TestMessage>(from_iter(bodies));
        let mut drained = Vec::new();
        assert!(decoded.drain_ready(8, &mut drained).unwrap());
        assert_eq!(drained[0].value, "one");
        assert_eq!(drained[1].value, "two");
    }
}
