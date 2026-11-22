use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_core::Stream;

use crate::client::RequestCheck;
use crate::codec::{Decode, Encode, Prost};
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

pub struct EncodeStream<T, C = Prost> {
    inner: BoxStream<T>,
    codec: C,
    check: Option<RequestCheck>,
}

impl<T, C> EncodeStream<T, C> {
    /// Creates a stream that encodes messages into byte bodies with `codec`.
    #[must_use]
    pub fn new(inner: BoxStream<T>, codec: C) -> Self {
        Self {
            inner,
            codec,
            check: None,
        }
    }
}

// The codec is never pinned.
impl<T, C> Unpin for EncodeStream<T, C> {}

impl<T, C> Stream for EncodeStream<T, C>
where
    C: Encode<T>,
{
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(message))) => {
                let checked = self
                    .codec
                    .encode(&message)
                    .map_err(Error::from)
                    .and_then(|body| match &self.check {
                        Some(check) => check.check(&body).map(|()| body),
                        None => Ok(body),
                    });
                Poll::Ready(Some(checked))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a message stream and encodes each message into bytes with `codec`.
pub fn encode<T, C>(inner: BoxStream<T>, codec: C) -> BoxStream<Vec<u8>>
where
    T: Send + 'static,
    C: Encode<T>,
{
    Box::pin(EncodeStream::new(inner, codec))
}

/// [`encode`] for the request messages of a client call, which `check` vets in their encoded form
/// when there is one. A message it rejects ends the stream with the status it returns, as an
/// error of the wrapped stream does, which cancels the call.
pub(crate) fn encode_checked<T, C>(
    inner: BoxStream<T>,
    codec: C,
    check: Option<RequestCheck>,
) -> BoxStream<Vec<u8>>
where
    T: Send + 'static,
    C: Encode<T>,
{
    Box::pin(EncodeStream {
        inner,
        codec,
        check,
    })
}

/// Checks the encoded body of a request message before it is decoded; see [`decode_validated`].
type ValidateBody = Box<dyn Fn(&[u8]) -> std::result::Result<(), Status> + Send>;

pub struct DecodeStream<T, C = Prost> {
    inner: BoxStream<Bytes>,
    codec: C,
    validate: Option<ValidateBody>,
    _marker: PhantomData<fn() -> T>,
}

impl<T, C> DecodeStream<T, C> {
    /// Creates a stream that decodes byte bodies into messages with `codec`.
    #[must_use]
    pub fn new(inner: BoxStream<Bytes>, codec: C) -> Self {
        Self {
            inner,
            codec,
            validate: None,
            _marker: PhantomData,
        }
    }

    /// Creates a stream that decodes byte bodies with `codec` into messages that `validate`
    /// accepts; see [`decode_validated`].
    #[must_use]
    pub fn validated<F>(inner: BoxStream<Bytes>, codec: C, validate: F) -> Self
    where
        F: Fn(&[u8]) -> std::result::Result<(), Status> + Send + 'static,
    {
        Self {
            inner,
            codec,
            validate: Some(Box::new(validate)),
            _marker: PhantomData,
        }
    }
}

// The codec is never pinned.
impl<T, C> Unpin for DecodeStream<T, C> {}

impl<T, C> Stream for DecodeStream<T, C>
where
    C: Decode<T>,
{
    type Item = Result<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(body))) => {
                let checked = match &self.validate {
                    None => self.codec.decode(body).map_err(Error::from),
                    Some(validate) => self
                        .codec
                        .decode(body.clone())
                        .map_err(Error::from)
                        .and_then(|message| validate(&body).map(|()| message).map_err(Error::from)),
                };
                Poll::Ready(Some(checked))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Wraps a byte stream and decodes each body into a message with `codec`.
pub fn decode<T, C>(inner: BoxStream<Bytes>, codec: C) -> BoxStream<T>
where
    T: 'static,
    C: Decode<T>,
{
    Box::pin(DecodeStream::new(inner, codec))
}

/// Wraps a byte stream and decodes each body into a message with `codec`, which `validate` checks
/// in its encoded form. A body `validate` rejects ends the stream with the status it returns, as
/// a body that does not decode does. Code generated with `validate=true` checks request messages
/// with it.
pub fn decode_validated<T, C, F>(inner: BoxStream<Bytes>, codec: C, validate: F) -> BoxStream<T>
where
    T: 'static,
    C: Decode<T>,
    F: Fn(&[u8]) -> std::result::Result<(), Status> + Send + 'static,
{
    Box::pin(DecodeStream::validated(inner, codec, validate))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::StreamExt;

    use super::{BoxStream, decode, decode_validated, encode, encode_checked, from_iter};
    use crate::client::RequestCheck;
    use crate::codec::Prost;
    use crate::{Code, Status};

    #[derive(Clone, PartialEq, prost::Message)]
    struct TestMessage {
        #[prost(string, tag = "1")]
        value: String,
    }

    /// The encoded bodies of `stream` as the server receives them.
    fn to_bytes(stream: BoxStream<Vec<u8>>) -> BoxStream<Bytes> {
        Box::pin(stream.map(|body| body.map(Bytes::from)))
    }

    #[tokio::test]
    async fn encode_decode_stream_round_trips_messages() {
        let messages = from_iter([TestMessage {
            value: "hello".to_owned(),
        }]);
        let encoded = encode(messages, Prost);
        let mut decoded = decode::<TestMessage, _>(to_bytes(encoded), Prost);

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
        let mut decoded = decode_validated::<TestMessage, _, _>(
            to_bytes(encode(messages, Prost)),
            Prost,
            move |body| {
                if body == prost::Message::encode_to_vec(&bad) {
                    Err(Status::invalid_argument("bad value"))
                } else {
                    Ok(())
                }
            },
        );

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
    async fn checked_encode_stream_fails_the_message_its_check_rejects() {
        let messages = from_iter(["a", "bad", "c"].map(|value| TestMessage {
            value: value.to_owned(),
        }));
        let bad = TestMessage {
            value: "bad".to_owned(),
        };
        let check = RequestCheck::new(move |body| {
            if body == prost::Message::encode_to_vec(&bad) {
                Err(Status::invalid_argument("bad value"))
            } else {
                Ok(())
            }
        });
        let mut encoded = encode_checked(messages, Prost, Some(check));

        assert_eq!(
            encoded.next().await.expect("first message").unwrap(),
            prost::Message::encode_to_vec(&TestMessage {
                value: "a".to_owned(),
            })
        );
        let status = encoded
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
