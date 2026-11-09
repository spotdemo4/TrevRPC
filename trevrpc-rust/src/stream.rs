use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use prost::Message;

use crate::{Error, Result, Status};

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
pub fn encode<T>(inner: BoxStream<T>) -> BoxStream<Vec<u8>>
where
    T: Message + Send + 'static,
{
    Box::pin(EncodeStream::new(inner))
}

/// Checks the encoded body of a request message before it is decoded; see [`decode_validated`].
type ValidateBody = Box<dyn Fn(&[u8]) -> std::result::Result<(), Status> + Send>;

pub struct DecodeStream<T> {
    inner: BoxStream<Vec<u8>>,
    validate: Option<ValidateBody>,
    _marker: PhantomData<fn() -> T>,
}

impl<T> DecodeStream<T> {
    /// Creates a stream that decodes byte bodies into protobuf messages.
    #[must_use]
    pub fn new(inner: BoxStream<Vec<u8>>) -> Self {
        Self {
            inner,
            validate: None,
            _marker: PhantomData,
        }
    }

    /// Creates a stream that decodes byte bodies into protobuf messages that `validate` accepts;
    /// see [`decode_validated`].
    #[must_use]
    pub fn validated<F>(inner: BoxStream<Vec<u8>>, validate: F) -> Self
    where
        F: Fn(&[u8]) -> std::result::Result<(), Status> + Send + 'static,
    {
        Self {
            inner,
            validate: Some(Box::new(validate)),
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
                let message = T::decode(body.as_slice()).map_err(Error::from);
                let checked = match (&self.validate, message) {
                    (Some(validate), Ok(message)) => {
                        validate(&body).map(|()| message).map_err(Error::from)
                    }
                    (_, message) => message,
                };
                Poll::Ready(Some(checked))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a byte stream and decodes each body into a protobuf message.
pub fn decode<T>(inner: BoxStream<Vec<u8>>) -> BoxStream<T>
where
    T: Message + Default + Send + 'static,
{
    Box::pin(DecodeStream::new(inner))
}

/// Wraps a byte stream and decodes each body into a protobuf message, which `validate` checks in
/// its encoded form. A body `validate` rejects ends the stream with the status it returns, as a
/// body that does not decode does. Code generated with `validate=true` checks request messages
/// with it.
pub fn decode_validated<T, F>(inner: BoxStream<Vec<u8>>, validate: F) -> BoxStream<T>
where
    T: Message + Default + Send + 'static,
    F: Fn(&[u8]) -> std::result::Result<(), Status> + Send + 'static,
{
    Box::pin(DecodeStream::validated(inner, validate))
}

#[cfg(test)]
mod tests {
    use futures_util::StreamExt;

    use super::{decode, decode_validated, encode, from_iter};
    use crate::{Code, Status};

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
    async fn validated_decode_stream_fails_the_message_its_check_rejects() {
        let messages = from_iter(["a", "bad", "c"].map(|value| TestMessage {
            value: value.to_owned(),
        }));
        let bad = TestMessage {
            value: "bad".to_owned(),
        };
        let mut decoded = decode_validated::<TestMessage, _>(encode(messages), move |body| {
            if body == prost::Message::encode_to_vec(&bad) {
                Err(Status::invalid_argument("bad value"))
            } else {
                Ok(())
            }
        });

        assert_eq!(
            decoded.next().await.expect("first message").unwrap().value,
            "a"
        );
        let status = decoded
            .next()
            .await
            .expect("second message")
            .expect_err("the check rejects it")
            .into_status();
        assert_eq!(
            (status.code(), status.message()),
            (Code::InvalidArgument, "bad value")
        );
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
