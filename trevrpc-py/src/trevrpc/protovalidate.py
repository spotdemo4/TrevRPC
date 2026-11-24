"""`buf.validate` request validation for TrevRPC channels, with protovalidate.

It needs the `protovalidate` extra, so that trevrpc itself does not depend on protovalidate and its
CEL engine::

    pip install "trevrpc[protovalidate]"

Pass a `RequestValidator` to a channel, and requests that break their rules fail before they are
sent, with the status a validating Go or Rust server would answer::

    from trevrpc.protovalidate import RequestValidator, violations

    channel = trevrpc.Channel(address, validator=RequestValidator())
    try:
        await users.create(CreateUserRequest(name=""))
    except TrevRpcError as error:
        # INVALID_ARGUMENT, whether the client or a validating server rejected it.
        for violation in violations(error):
            print(violation.rule_id, violation.message)

`violations()` reads the `buf.validate.Violations` details of any status, so it serves without a
`RequestValidator` too. Checking requests on the client saves a round trip, but it does not replace
checking them on the server, since clients can skip it.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import protovalidate
from protobuf import (
    DescFieldValueList,
    DescFieldValueMap,
    DescFieldValueMessage,
    DescMessage,
    Extension,
    Message,
)
from protovalidate import CompilationError, EvaluationError, ViolationPb, ViolationsPb

from ._status import Code, StatusDetail, TrevRpcError
from ._validator import MessageValidator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from protovalidate import FieldPathPb

    from ._validator import ValidateFunc

__all__ = [
    "MAX_DETAIL_VIOLATIONS",
    "VIOLATIONS_TYPE",
    "RequestValidator",
    "validate",
    "violations",
]

MAX_DETAIL_VIOLATIONS = 100
"""How many violations the `buf.validate.Violations` detail of a broken request lists at most, so
that a request breaking a rule on each of many items still fails with a status that fits a frame.
Its message names the first violation and counts the rest."""

VIOLATIONS_TYPE = ViolationsPb.desc().type_name
"""`buf.validate.Violations`, the type of the status detail that lists a request's violations."""

# The options that carry rules, from protovalidate's own descriptor of buf/validate/validate.proto.
_EXTENSIONS: dict[str, Extension[Any, Any]] = {
    extension.type_name: extension.type  # pyright: ignore[reportUnknownMemberType]
    for extension in ViolationsPb.desc().file.extensions
}
_MESSAGE_RULES = _EXTENSIONS["buf.validate.message"]
_ONEOF_RULES = _EXTENSIONS["buf.validate.oneof"]
_FIELD_RULES = _EXTENSIONS["buf.validate.field"]


class RequestValidator:
    """A `trevrpc.Validator` that enforces the `buf.validate` rules of request messages with
    `validator`, a protovalidate validator, or with a new `protovalidate.Validator()` when it is
    `None`. Pass `protovalidate.Validator(registry)`, with a registry that holds their extensions,
    to enforce predefined rules, and `fail_fast=True` to stop at a message's first violation.

    - A message that breaks its rules fails with `INVALID_ARGUMENT` and a `buf.validate.Violations`
      detail, as the Go and Rust servers report it.
    - A rule that cannot be evaluated for a message fails it with `INTERNAL`.
    - A message type whose rules cannot compile, or cannot be evaluated for an empty message,
      fails every call sending it with `INTERNAL`.
    - Message types without rules, directly or in a message they hold, are not checked and cost
      nothing.
    """

    __slots__ = ("_fail_fast", "_validator")

    def __init__(
        self, validator: protovalidate.Validator | None = None, *, fail_fast: bool = False
    ) -> None:
        self._validator = validator if validator is not None else protovalidate.Validator()
        self._fail_fast = fail_fast

    def validate_func(self, message_type: type[Message], /) -> ValidateFunc | None:
        """`None` for a type without rules. For the others it compiles the rules now, through the
        validator's cache, by checking an empty message, and raises the `CompilationError` or
        `EvaluationError` that fails. protovalidate reports some broken rules, such as one naming a
        field the message lacks, only when it evaluates them."""
        if not _has_rules(message_type.desc(), set()):
            return None
        validator = self._validator
        fail_fast = self._fail_fast
        # As the Go, Rust and JavaScript integrations do, any error here fails the type closed,
        # including an evaluation error that only the empty message's default values cause. #719
        # changes that in all four together.
        validator.collect_violations(message_type(), fail_fast=fail_fast)

        def validate(message: Message) -> None:
            try:
                found = validator.collect_violations(message, fail_fast=fail_fast)
            except (CompilationError, EvaluationError) as error:
                raise TrevRpcError(Code.INTERNAL, f"request validation failed: {error}") from error
            if found:
                raise _status([violation.proto for violation in found])

        return validate


_global = MessageValidator(RequestValidator())


def validate(message: Message) -> None:
    """Checks `message` as a channel with `RequestValidator()` checks a request of its type, and
    raises the `TrevRpcError` the call would fail with: `INVALID_ARGUMENT` with a
    `buf.validate.Violations` detail for a message that breaks its rules, `INTERNAL` for rules that
    cannot be compiled or evaluated. A message of a type without rules is never invalid. The rules
    of each message type are compiled once.

    To check with another validator, wrap a `RequestValidator` in `trevrpc.MessageValidator`."""
    _global.validate(message)


def violations(error: TrevRpcError) -> list[ViolationPb]:
    """The violations that the `buf.validate.Violations` details of `error` list, in order; empty
    when it has none."""
    return [
        violation
        for details in error.find_details(ViolationsPb)
        for violation in details.violations
    ]


def _status(found: Sequence[ViolationPb]) -> TrevRpcError:
    """The status a request with violations `found` fails with."""
    message = f"validation error: {_describe(found[0])}"
    if len(found) > 1:
        message += f" (and {len(found) - 1} more)"
    listed = ViolationsPb(violations=list(found[:MAX_DETAIL_VIOLATIONS]))
    return TrevRpcError(
        Code.INVALID_ARGUMENT, message, details=[StatusDetail(VIOLATIONS_TYPE, listed.to_binary())]
    )


def _describe(violation: ViolationPb) -> str:
    """`violation` as protovalidate-go's `Violation.String` writes it: `field.path: message`."""
    text = violation.message or (f"[{violation.rule_id}]" if violation.rule_id else "[unknown]")
    path = _field_path(violation.field)
    return f"{path}: {text}" if path else text


def _field_path(path: FieldPathPb | None) -> str:
    """`path` as protovalidate-go's `FieldPathString` writes it, such as `users["ada"].name`."""
    if path is None:
        return ""
    parts: list[str] = []
    for element in path.elements:
        part = element.field_name
        subscript = element.subscript
        if subscript is not None:
            value = subscript.value
            if isinstance(value, bool):
                key = "true" if value else "false"
            elif isinstance(value, str):
                key = json.dumps(value, ensure_ascii=False)
            else:
                key = str(value)
            part += f"[{key}]"
        parts.append(part)
    return ".".join(parts)


def _has_rules(desc: DescMessage, seen: set[str]) -> bool:
    """Whether `desc`, or a message type it holds however deep, carries `buf.validate` rules.
    `seen` breaks recursive types."""
    if desc.type_name in seen:
        return False
    seen.add(desc.type_name)
    options = desc.proto.options
    if options is not None and _MESSAGE_RULES in options:
        return True
    for oneof in desc.oneofs:
        oneof_options = oneof.proto.options
        if oneof_options is not None and _ONEOF_RULES in oneof_options:
            return True
    for field in desc.fields:
        field_options = field.proto.options
        if field_options is not None and _FIELD_RULES in field_options:
            return True
        value = field.value
        match value:
            case DescFieldValueMessage():
                held: object = value.message
            case DescFieldValueList():
                held = value.element
            case DescFieldValueMap():
                held = value.value
            case _:
                held = None
        if isinstance(held, DescMessage) and _has_rules(held, seen):
            return True
    return False
