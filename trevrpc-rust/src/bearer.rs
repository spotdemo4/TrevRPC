//! Bearer credentials in the `authorization` metadata, as RFC 6750 defines them: the server's
//! parser and the client's builder share the token syntax.

use crate::{Metadata, Status};

/// Returns the token of the `authorization` metadata, which must hold bearer credentials as RFC
/// 6750 defines them: the case-insensitive scheme `Bearer`, one or more spaces, and the token.
///
/// # Errors
///
/// An unauthenticated status when the metadata is missing or holds anything else, with a message
/// that tells the two apart.
pub fn bearer_token(metadata: &Metadata) -> std::result::Result<&str, Status> {
    const SCHEME: &[u8] = b"bearer ";
    let malformed = || Status::unauthenticated("malformed bearer token");
    let value = metadata
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
    let (_, credentials) = value
        .split_at_checked(SCHEME.len())
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case(SCHEME))
        .ok_or_else(malformed)?;
    let token = credentials
        .iter()
        .position(|&byte| byte != b' ')
        .and_then(|start| credentials.get(start..))
        .unwrap_or_default();
    if !is_b64_token(token) {
        return Err(malformed());
    }
    std::str::from_utf8(token).map_err(|_| malformed())
}

/// Returns the request metadata entry that carries `token` as bearer credentials: the key
/// `authorization` and the value `Bearer <token>`. It is the counterpart of
/// [`server::bearer_token`](crate::server::bearer_token), and accepts the tokens that parses, the
/// RFC 6750 `b64token` syntax: one or more ASCII letters, digits and the characters `-._~+/`,
/// followed by any number of `=`.
///
/// The entry is for [`Call::metadata`](crate::client::Call::metadata), and
/// `Metadata::from([bearer_metadata(token)?])` makes the connection metadata of
/// [`ChannelConfig::with_connection_metadata`](crate::client::ChannelConfig::with_connection_metadata).
///
/// # Errors
///
/// An invalid-argument status for any other token, such as an empty one or one with whitespace,
/// which a server would reject as unauthenticated, and which a client could not tell from an
/// expired session.
pub fn bearer_metadata(token: &str) -> std::result::Result<(String, Vec<u8>), Status> {
    if !is_b64_token(token.as_bytes()) {
        return Err(Status::invalid_argument("bearer token is not a b64token"));
    }
    Ok((
        "authorization".to_owned(),
        format!("Bearer {token}").into_bytes(),
    ))
}

/// Reports whether `token` matches RFC 6750's `b64token`:
/// `1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"="`.
fn is_b64_token(token: &[u8]) -> bool {
    let mut unpadded = token;
    while let [rest @ .., b'='] = unpadded {
        unpadded = rest;
    }
    !unpadded.is_empty()
        && unpadded
            .iter()
            .all(|&byte| byte.is_ascii_alphanumeric() || b"-._~+/".contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::{bearer_metadata, bearer_token};
    use crate::{Code, Metadata};

    #[test]
    fn bearer_token_parses_rfc_6750_credentials() {
        let token = |value: Option<&str>| {
            let mut metadata = Metadata::new();
            if let Some(value) = value {
                metadata.insert("authorization".to_owned(), value.as_bytes().to_vec());
            }
            bearer_token(&metadata)
                .map(str::to_owned)
                .map_err(|status| (status.code(), status.message().to_owned()))
        };
        for (value, want) in [
            (Some("Bearer abc.DEF-123_~+/=="), "abc.DEF-123_~+/=="),
            (Some("bEaReR token"), "token"),
            (Some("Bearer   token"), "token"),
        ] {
            assert_eq!(token(value), Ok(want.to_owned()), "{value:?}");
        }
        assert_eq!(
            token(None),
            Err((Code::Unauthenticated, "missing bearer token".to_owned()))
        );
        for value in [
            "",
            "token",
            "Bearer",
            "Bearer ",
            "Basic dXNlcjpwYXNz",
            "Bearertoken",
            "Bearer\ttoken",
            "Bearer token ",
            "Bearer to ken",
            "Bearer ==",
            "Bearer a=b",
            "Bearer a,b",
        ] {
            assert_eq!(
                token(Some(value)),
                Err((Code::Unauthenticated, "malformed bearer token".to_owned())),
                "{value:?}"
            );
        }
    }

    #[test]
    fn bearer_metadata_builds_what_bearer_token_parses() {
        for token in ["abc", "abc.DEF-123_~+/", "abc.DEF-123_~+/==", "a", "a="] {
            let metadata = Metadata::from([bearer_metadata(token).expect("a b64token")]);
            assert_eq!(
                metadata.get("authorization"),
                Some(&format!("Bearer {token}").into_bytes())
            );
            assert_eq!(bearer_token(&metadata).ok(), Some(token), "{token:?}");
        }
    }

    #[test]
    fn bearer_metadata_rejects_tokens_that_are_not_b64tokens() {
        for token in [
            "",
            "==",
            " token",
            "token ",
            "to ken",
            "token\n",
            "t\u{f6}k\u{e9}n",
            "a=b",
            "a,b",
        ] {
            let status = bearer_metadata(token).expect_err("not a b64token");
            assert_eq!(status.code(), Code::InvalidArgument, "{token:?}");
        }
    }
}
