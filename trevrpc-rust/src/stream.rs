use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use prost::Message;

use crate::{Error, Result};

/// A sendable, heap-allocated asynchronous message stream.
///
/// This is a standard [`futures_core::Stream`], so callers can use
/// [`futures_util::StreamExt`] and the rest of the futures ecosystem directly.
pub type BoxStream<T> = Pin<Box<dyn Stream<Item = Result<T>> + Send + 'static>>;

pub struct EmptyStream<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> Default for EmptyStream<T> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Stream for EmptyStream<T> {
    type Item = Result<T>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// Returns an empty boxed message stream.
#[must_use]
pub fn empty<T>() -> BoxStream<T>
where
    T: Send + 'static,
{
    Box::pin(EmptyStream::default())
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

impl<T, I> Stream for IterStream<I>
where
    I: Iterator<Item = T> + Unpin,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.iter.next().map(Ok))
    }
}

/// Returns a boxed message stream that yields items from an iterator.
#[must_use]
pub fn from_iter<T, I>(iter: I) -> BoxStream<T>
where
    T: Send + 'static,
    I: IntoIterator<Item = T>,
    I::IntoIter: Send + Unpin + 'static,
{
    Box::pin(IterStream::new(iter.into_iter()))
}

pub struct EncodeStream<T> {
    inner: BoxStream<T>,
}

impl<T> EncodeStream<T> {
    /// Creates a stream that encodes protobuf messages into byte bodies.
    #[must_use]
    pub fn new(inner: BoxStream<T>) -> Self {
        Self { inner }
    }
}

impl<T> Stream for EncodeStream<T>
where
    T: Message,
{
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => Poll::Ready(Some(Ok(message.encode_to_vec()))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a message stream and encodes each protobuf message into bytes.
#[must_use]
pub fn encode<T>(inner: BoxStream<T>) -> BoxStream<Vec<u8>>
where
    T: Message + Send + 'static,
{
    Box::pin(EncodeStream::new(inner))
}

pub struct DecodeStream<T> {
    inner: BoxStream<Vec<u8>>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> DecodeStream<T> {
    /// Creates a stream that decodes byte bodies into protobuf messages.
    #[must_use]
    pub fn new(inner: BoxStream<Vec<u8>>) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<T> Stream for DecodeStream<T>
where
    T: Message + Default,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(body))) => {
                Poll::Ready(Some(T::decode(body.as_slice()).map_err(Error::from)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a byte stream and decodes each body into a protobuf message.
#[must_use]
pub fn decode<T>(inner: BoxStream<Vec<u8>>) -> BoxStream<T>
where
    T: Message + Default + Send + 'static,
{
    Box::pin(DecodeStream::new(inner))
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

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

    #[tokio::test]
    async fn standard_stream_ext_consumes_iterator_streams() {
        let values = from_iter([1_u8, 2, 3])
            .map(|value| value.expect("iterator stream should not fail"))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(values, [1, 2, 3]);
    }
}
