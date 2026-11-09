//! [protovalidate](https://github.com/bufbuild/protovalidate) request validation for `TrevRPC`
//! servers: requests that break their `buf.validate` rules fail before the handler runs.
//!
//! Generate services with `protoc-gen-trevrpc-rust`'s `validate=true` option, and depend on this
//! crate. The generated registration builds a [`Validator`] from the descriptors of the request
//! types that carry rules, and checks each request message with it. A request that breaks its
//! rules fails with `InvalidArgument` and a `buf.validate.Violations` status detail; on a
//! streaming route, the request message that breaks them fails the stream's item. Routes whose
//! request type has no rules are generated without validation and cost nothing.
//!
//! Validation runs on the request's encoded bytes with prost-protovalidate's runtime engine,
//! which enforces CEL rules like every other protovalidate implementation, so the request types
//! need no `prost-reflect` support. It runs inline on the task that serves the call, as decoding
//! does, so keep the rules of request types cheap to evaluate: a rule that takes long blocks that
//! executor thread while it runs.

use std::sync::Arc;

use prost::Message;
use prost_protovalidate::bridge::RuntimeBridge;
use prost_protovalidate::types::Violations;
use prost_protovalidate::{Error, Violation};
use trevrpc::{Detail, Status};

/// The status detail type of a request's rule violations.
pub const VIOLATIONS_TYPE: &str = "buf.validate.Violations";

/// How many violations the `buf.validate.Violations` detail of a broken request lists at most, so
/// that a request breaking a rule on each of many items still fails with a status that fits a
/// frame. Its message names the first violation and counts the rest.
pub const MAX_DETAIL_VIOLATIONS: usize = 100;

/// Checks request messages against the `buf.validate` rules of a set of message types, caching
/// their compiled rules. Clones share the cache.
#[derive(Clone)]
pub struct Validator {
    bridge: Arc<RuntimeBridge>,
}

impl Validator {
    /// Creates a validator for the message types of an encoded `FileDescriptorSet`, which must
    /// hold every file they need, imports included, `buf/validate/validate.proto` among them.
    ///
    /// # Panics
    ///
    /// Panics when the set does not decode. Generated code embeds it, so that is a generator bug.
    #[must_use]
    pub fn from_descriptor_set(descriptor_set: &[u8]) -> Self {
        Self {
            bridge: Arc::new(RuntimeBridge::from_fds(descriptor_set)),
        }
    }

    /// Returns the validator of the messages of type `full_name`, such as `acme.v1.Request`,
    /// compiling their rules now, so a rule that cannot compile fails at registration and not
    /// on the first call.
    ///
    /// # Panics
    ///
    /// Panics when the set has no such type or its rules cannot compile.
    #[must_use]
    #[allow(clippy::panic)] // A rule that cannot be enforced is a schema error found at startup.
    pub fn message(&self, full_name: &'static str) -> MessageValidator {
        // An empty message is the empty encoding.
        match self.bridge.validate_wire(full_name, &[]) {
            Ok(()) | Err(Error::Validation(_)) => MessageValidator {
                bridge: Arc::clone(&self.bridge),
                full_name,
            },
            Err(error) => panic!("trevrpc-protovalidate: cannot validate {full_name}: {error}"),
        }
    }
}

/// Checks the request messages of one type; see [`Validator::message`].
#[derive(Clone)]
pub struct MessageValidator {
    bridge: Arc<RuntimeBridge>,
    full_name: &'static str,
}

impl MessageValidator {
    /// Checks an encoded request message.
    ///
    /// # Errors
    ///
    /// Returns `InvalidArgument` with a `buf.validate.Violations` detail when the message breaks
    /// its rules, and `Internal` when they cannot be evaluated.
    pub fn validate(&self, message: &[u8]) -> Result<(), Status> {
        self.bridge
            .validate_wire(self.full_name, message)
            .map_err(status_from_error)
    }
}

/// Converts a protovalidate error into the status a call fails with: `InvalidArgument` with a
/// `buf.validate.Violations` detail for broken rules, and `Internal` for rules that could not be
/// evaluated.
#[must_use]
pub fn status_from_error(error: Error) -> Status {
    match error {
        Error::Validation(error) => {
            let violations = error.violations();
            let Some(first) = violations.first() else {
                return Status::invalid_argument("validation error");
            };
            let message = match violations.len().saturating_sub(1) {
                0 => format!("validation error: {first}"),
                more => format!("validation error: {first} (and {more} more)"),
            };
            let listed = Violations {
                violations: violations
                    .iter()
                    .take(MAX_DETAIL_VIOLATIONS)
                    .map(Violation::to_proto)
                    .collect(),
            };
            Status::invalid_argument(message)
                .with_details([Detail::new(VIOLATIONS_TYPE, listed.encode_to_vec())])
        }
        error => Status::internal(format!("request validation failed: {error}")),
    }
}
