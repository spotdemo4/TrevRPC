//// `buf.validate.Violations`, the status detail of a request that broke its
//// validation rules: `from_status` reads the violations of a failed call,
//// and `status` builds the status a `trevrpc/validator` check fails a
//// request with, as the Go, Rust and Python validators do.
////
//// ```gleam
//// case greeter.say_hello(channel, HelloRequest(name: ""), client.options()) {
////   Error(status) ->
////     case violations.from_status(status) {
////       Ok([violation, ..]) -> violation.rule_id
////       _ -> status.message
////     }
////   Ok(_) -> "ok"
//// }
//// ```
////
//// Unset fields are their zero values: an empty string or list, `False`, 0
//// or `None`.

import gleam/option.{type Option}
import trevrpc/status.{type Status}

/// A broken rule: the path of the field it applies to (empty for a rule on
/// the message itself), the path of the rule within `buf.validate`'s rules,
/// its ID, a message saying what broke it, and, for a rule on a map field,
/// whether it applies to the key rather than the value.
pub type Violation {
  Violation(
    field: List(FieldPathElement),
    rule: List(FieldPathElement),
    rule_id: String,
    message: String,
    for_key: Bool,
  )
}

/// One step of a field path: a field, with the index or key of a repeated
/// or map field's item. `field_number` and an `UnknownType`'s number are
/// int32s, as protobuf has them.
pub type FieldPathElement {
  FieldPathElement(
    field_number: Int,
    field_name: String,
    field_type: Option(FieldType),
    key_type: Option(FieldType),
    value_type: Option(FieldType),
    subscript: Option(Subscript),
  )
}

/// A `google.protobuf.FieldDescriptorProto.Type`.
pub type FieldType {
  TypeDouble
  TypeFloat
  TypeInt64
  TypeUint64
  TypeInt32
  TypeFixed64
  TypeFixed32
  TypeBool
  TypeString
  TypeGroup
  TypeMessage
  TypeBytes
  TypeUint32
  TypeEnum
  TypeSfixed32
  TypeSfixed64
  TypeSint32
  TypeSint64
  /// A value the type does not define.
  UnknownType(Int)
}

/// The item of a repeated or map field a path element names. An `Index` and
/// a `UintKey` are from 0 to 2^64 - 1, and an `IntKey` an int64, as
/// protobuf has them.
pub type Subscript {
  Index(Int)
  BoolKey(Bool)
  IntKey(Int)
  UintKey(Int)
  StringKey(String)
}

/// The detail's type.
pub const detail_type = "buf.validate.Violations"

/// The status a request with these violations fails with:
/// `InvalidArgument`, with a message naming the first violation and
/// counting the others, and a `buf.validate.Violations` detail listing the
/// first 100. Without violations, it has no detail. It panics for a number
/// out of its field's range (see `FieldPathElement` and `Subscript`), which
/// fails a check's call with `Internal`.
@external(erlang, "trevrpc_gleam_ffi", "violations_status")
pub fn status(violations: List(Violation)) -> Status

/// The violations of the first `buf.validate.Violations` detail of
/// `status`, or an error when it has none or it does not decode.
@external(erlang, "trevrpc_gleam_ffi", "violations_from_status")
pub fn from_status(status: Status) -> Result(List(Violation), Nil)

/// A violation as protovalidate prints it: its field path, then its
/// message, or its rule ID in brackets when it has no message, such as
/// `items[0].name: value length must be at least 1 characters`.
@external(erlang, "trevrpc_gleam_ffi", "violation_to_string")
pub fn to_string(violation: Violation) -> String
