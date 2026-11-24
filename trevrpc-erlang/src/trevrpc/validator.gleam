//// Request validation: a server checks request messages after they are
//// decoded and before a handler sees them, and a channel checks the ones
//// its calls send before they leave. Give a `Validator` to
//// `server.Config` or `channel.Config`; `trevrpc/violations` builds the
//// status a check fails a request that breaks its rules with, and reads
//// it back on a client.
////
//// A unary or server-streaming call whose request is invalid fails with
//// the check's status: on a server before its handler runs, on a client
//// without reaching the server. On a server, `handler.recv` returns the
//// status of an invalid streamed request, and the handler decides whether
//// the call goes on; on a client, `client.send` returns `Invalid` for one,
//// which is not sent, and the call goes on.
////
//// A server asks its validator about each request type of its routes once,
//// when it starts, and fails to start if the validator panics. A channel
//// asks about each type once, when the first call of the type starts, and
//// fails each call of a type it panicked for as the call starts. A check that panics fails its
//// call with `Internal`, as a rule that cannot be evaluated for a message
//// should. See `trevrpc_validator` for the details.

import gleam/dynamic.{type Dynamic}
import gleam/option.{type Option}
import trevrpc/status.{type Status}

/// Checks a request message: the message, as the route's codec decoded it
/// on a server or as the caller gave it on a client, and its encoding.
/// `Error(status)` fails it with `status`, normally `violations.status`'s
/// `InvalidArgument`.
pub type Check =
  fn(Dynamic, BitArray) -> Result(Nil, Status)

/// The check of the request messages of a type, named by its protobuf full
/// name such as `example.v1.HelloRequest`, or `None` when they never need
/// checking, which costs their calls nothing.
pub type Validator =
  fn(String) -> Option(Check)
