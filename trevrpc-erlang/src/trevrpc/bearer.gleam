//// Bearer credentials in the `authorization` metadata, as RFC 6750 defines
//// them: a handler's parser (`token`) and a client's builder (`metadata`)
//// share the token syntax, as the Go and Rust runtimes' do.
////
//// ```gleam
//// let assert Ok(authorization) = bearer.metadata(token)
//// let config = channel.Config(..config, connection_metadata: authorization)
////
//// fn say_hello(request, call) {
////   use token <- result.try(bearer.token(handler.metadata(call)))
////   ...
//// }
//// ```

import trevrpc/metadata.{type Metadata}
import trevrpc/status.{type Status}

/// The token of the `authorization` metadata, which must hold bearer
/// credentials: the case-insensitive scheme `Bearer`, one or more spaces,
/// and the token. An `Unauthenticated` status when the metadata is missing
/// or holds anything else, with a message that tells the two apart.
@external(erlang, "trevrpc_gleam_ffi", "bearer_token")
pub fn token(metadata: Metadata) -> Result(String, Status)

/// The request metadata that carries `token` as bearer credentials: the
/// name `authorization` and the value `Bearer <token>`, for a call's
/// options or a channel's connection metadata. It accepts the tokens
/// `token` parses, RFC 6750's `b64token` syntax: one or more ASCII letters,
/// digits and the characters `-._~+/`, followed by any number of `=`. An
/// `InvalidArgument` status for any other token, such as an empty one or
/// one with whitespace, which a server would reject as unauthenticated.
@external(erlang, "trevrpc_gleam_ffi", "bearer_metadata")
pub fn metadata(token: String) -> Result(Metadata, Status)
