use std::marker::PhantomData;

use prost::Message;

use crate::{Error, Result};

#[crate::async_trait]
pub trait MessageStream<T>: Send {
    /// Returns the next message from the stream, or `None` after the stream finishes.
    async fn next(&mut self) -> Option<Result<T>>;
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
}

/// Returns an empty boxed message stream.
#[must_use]
pub fn empty<T>() -> BoxMessageStream<T>
where
    T: Send + 'static,
{
    Box::new(EmptyStream::default())
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
}
