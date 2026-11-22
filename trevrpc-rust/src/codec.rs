//! How request and response messages become bytes and back.
//!
//! A codec is a small value, usually zero-sized, that encodes and decodes the messages of a call.
//! Clients, streams and generated services take one, so the same runtime carries messages of
//! different protobuf libraries: [`Prost`] for `prost` messages, which is the default, and
//! [`Buffa`] for `buffa` messages, behind the `buffa` feature, whose [`BuffaView`] decodes them as
//! zero-copy views. One server can host services of both side by side, since each route picks its
//! own codec.
//!
//! The codec traits are generic over the message, so a codec implements them for every message
//! type of its library without the impls of different codecs overlapping. A codec of another
//! library implements [`Encode`] and [`Decode`] (and [`Name`], for status details) for its
//! messages in the same way.

use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use bytes::Bytes;

use crate::Status;

/// Encodes messages of type `M` into the bodies of message frames.
pub trait Encode<M>: Clone + Send + Sync + 'static {
    /// Encodes `message`.
    ///
    /// # Errors
    ///
    /// Returns an error when `message` cannot be encoded, such as when it is too large for
    /// protobuf.
    fn encode(&self, message: &M) -> Result<Vec<u8>, EncodeError>;
}

/// Decodes messages of type `M` from the bodies of message frames.
pub trait Decode<M>: Clone + Send + Sync + 'static {
    /// Decodes a message from `body`. A decoded message may keep `body`, or slices of it, instead
    /// of copying them.
    ///
    /// # Errors
    ///
    /// Returns an error when `body` is not a valid encoding of `M`.
    fn decode(&self, body: Bytes) -> Result<M, DecodeError>;
}

/// Names messages of type `M` by their full protobuf name, such as `buf.validate.Violations`,
/// which status details carry.
pub trait Name<M> {
    /// Returns the full protobuf name of `M`, without a leading dot.
    fn full_name(&self) -> String;
}

/// Decodes the request message of a unary or server-streaming route from its `body` with
/// `codec`. Generated code calls it.
///
/// # Errors
///
/// Returns [`Error::Decode`](crate::Error::Decode) when `body` does not decode.
pub fn decode<M>(codec: &impl Decode<M>, body: Bytes) -> crate::Result<M> {
    Ok(codec.decode(body)?)
}

/// [`decode`], after which `validate` checks the `body` of the message that decoded. Generated code
/// with `validate=true` calls it for request types with `buf.validate` rules, as it calls
/// [`stream::decode_validated`](crate::stream::decode_validated) for streamed requests.
///
/// # Errors
///
/// Returns [`Error::Decode`](crate::Error::Decode) when `body` does not decode, and the status
/// `validate` returns when it rejects the message.
pub fn decode_validated<M>(
    codec: &impl Decode<M>,
    body: Bytes,
    validate: impl FnOnce(&[u8]) -> Result<(), Status>,
) -> crate::Result<M> {
    // The decoded message may share the buffer, which a clone of `Bytes` only counts.
    let checked = body.clone();
    let message = codec.decode(body)?;
    validate(&checked)?;
    Ok(message)
}

/// Encodes the response message of a unary or client-streaming route with `codec`. Generated code
/// calls it.
///
/// # Errors
///
/// Returns [`Error::Encode`](crate::Error::Encode) when `message` does not encode.
pub fn encode<M>(codec: &impl Encode<M>, message: &M) -> crate::Result<Vec<u8>> {
    Ok(codec.encode(message)?)
}

/// A message that could not be encoded.
///
/// It keeps the codec's own error as its [source](StdError::source). Cloning it is cheap.
#[derive(Clone)]
pub struct EncodeError(Arc<dyn StdError + Send + Sync>);

impl EncodeError {
    /// Wraps a codec's encode error.
    #[must_use]
    pub fn new(error: impl StdError + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }
}

/// A message body that could not be decoded.
///
/// It keeps the codec's own error as its [source](StdError::source). Cloning it is cheap.
#[derive(Clone)]
pub struct DecodeError(Arc<dyn StdError + Send + Sync>);

impl DecodeError {
    /// Wraps a codec's decode error.
    #[must_use]
    pub fn new(error: impl StdError + Send + Sync + 'static) -> Self {
        Self(Arc::new(error))
    }
}

macro_rules! codec_error {
    ($error:ident) => {
        impl fmt::Debug for $error {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Debug::fmt(&*self.0, formatter)
            }
        }

        impl fmt::Display for $error {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&*self.0, formatter)
            }
        }

        impl StdError for $error {
            fn source(&self) -> Option<&(dyn StdError + 'static)> {
                Some(&*self.0)
            }
        }
    };
}

codec_error!(EncodeError);
codec_error!(DecodeError);

impl From<prost::EncodeError> for EncodeError {
    fn from(error: prost::EncodeError) -> Self {
        Self::new(error)
    }
}

impl From<prost::DecodeError> for DecodeError {
    fn from(error: prost::DecodeError) -> Self {
        Self::new(error)
    }
}

/// The codec of `prost` messages, and the default of every type that takes a codec.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Prost;

impl<M: prost::Message> Encode<M> for Prost {
    #[inline]
    fn encode(&self, message: &M) -> Result<Vec<u8>, EncodeError> {
        Ok(message.encode_to_vec())
    }
}

impl<M: prost::Message + Default> Decode<M> for Prost {
    #[inline]
    fn decode(&self, body: Bytes) -> Result<M, DecodeError> {
        M::decode(body).map_err(DecodeError::from)
    }
}

impl<M: prost::Name> Name<M> for Prost {
    fn full_name(&self) -> String {
        M::full_name()
    }
}

/// The codec of `buffa` messages.
///
/// It decodes owned messages. A decoded message copies strings out of the body; fields that
/// `buffa` generates as [`Bytes`] share it instead.
#[cfg(feature = "buffa")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct Buffa;

#[cfg(feature = "buffa")]
impl<M: buffa::Message + 'static> Encode<M> for Buffa {
    #[inline]
    fn encode(&self, message: &M) -> Result<Vec<u8>, EncodeError> {
        message.try_encode_to_vec().map_err(EncodeError::new)
    }
}

#[cfg(feature = "buffa")]
impl<M: buffa::Message + 'static> Decode<M> for Buffa {
    #[inline]
    fn decode(&self, mut body: Bytes) -> Result<M, DecodeError> {
        M::decode(&mut body).map_err(DecodeError::new)
    }
}

#[cfg(feature = "buffa")]
impl<M: buffa::MessageName> Name<M> for Buffa {
    fn full_name(&self) -> String {
        M::FULL_NAME.to_owned()
    }
}

/// The zero-copy view handle of the `buffa` message `M`, such as `FooOwnedView` for `Foo`, which
/// [`BuffaView`] decodes.
#[cfg(feature = "buffa")]
pub type BuffaViewHandle<M> = <M as buffa::HasMessageView>::ViewHandle;

/// The codec of `buffa` messages that decodes `M` as its zero-copy view handle,
/// [`BuffaViewHandle<M>`], instead of an owned `M`.
///
/// The handle keeps the message body and borrows strings, bytes and nested messages from it, so
/// decoding allocates nothing for them. Reading a field reads the body in place. A handle keeps
/// the whole buffer the body came in alive while it lives, which can be larger than the message.
///
/// It encodes owned `buffa` messages like [`Buffa`], so one codec serves a call that sends owned
/// messages and receives views. Generated code with `views=true` uses it.
#[cfg(feature = "buffa")]
pub struct BuffaView<M>(std::marker::PhantomData<fn() -> M>);

#[cfg(feature = "buffa")]
impl<M> BuffaView<M> {
    /// The codec of the views of `M`.
    #[must_use]
    pub const fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}

#[cfg(feature = "buffa")]
impl<M> Clone for BuffaView<M> {
    fn clone(&self) -> Self {
        *self
    }
}

#[cfg(feature = "buffa")]
impl<M> Copy for BuffaView<M> {}

#[cfg(feature = "buffa")]
impl<M> Default for BuffaView<M> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "buffa")]
impl<M> fmt::Debug for BuffaView<M> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("BuffaView")
    }
}

#[cfg(feature = "buffa")]
impl<M: 'static, E: buffa::Message + 'static> Encode<E> for BuffaView<M> {
    #[inline]
    fn encode(&self, message: &E) -> Result<Vec<u8>, EncodeError> {
        Buffa.encode(message)
    }
}

#[cfg(feature = "buffa")]
impl<M> Decode<BuffaViewHandle<M>> for BuffaView<M>
where
    M: buffa::HasMessageView + 'static,
{
    #[inline]
    fn decode(&self, body: Bytes) -> Result<BuffaViewHandle<M>, DecodeError> {
        M::decode_view_handle(body).map_err(DecodeError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct Note {
        #[prost(string, tag = "1")]
        text: String,
    }

    impl prost::Name for Note {
        const NAME: &'static str = "Note";
        const PACKAGE: &'static str = "test.v1";
    }

    #[test]
    fn prost_round_trips_a_message() {
        let note = Note {
            text: "hello".to_owned(),
        };
        let body = Prost.encode(&note).unwrap();
        let decoded: Note = Prost.decode(Bytes::from(body)).unwrap();
        assert_eq!(decoded, note);
        assert_eq!(Name::<Note>::full_name(&Prost), "test.v1.Note");
    }

    #[test]
    fn prost_decode_error_keeps_its_source() {
        let error =
            Decode::<Note>::decode(&Prost, Bytes::from_static(&[0x0a, 0x05, b'h'])).unwrap_err();
        let source = error.source().unwrap();
        assert!(source.downcast_ref::<prost::DecodeError>().is_some());
        assert_eq!(error.to_string(), source.to_string());
    }
}
