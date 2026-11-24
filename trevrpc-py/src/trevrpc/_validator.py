"""Request validation: the hook a channel checks request messages with before sending them."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from protobuf import Message

from ._status import Code, TrevRpcError

type ValidateFunc = Callable[[Message], None]
"""Checks one request message: returns if it is valid, raises if it is not."""


class Validator(Protocol):
    """Checks the request messages a channel sends, before it encodes them; see
    `ChannelOptions.validator`. `trevrpc.protovalidate.RequestValidator` implements it with
    `buf.validate` rules."""

    def validate_func(self, message_type: type[Message], /) -> ValidateFunc | None:
        """Returns the function that checks messages of `message_type`, or `None` when they never
        need checking, which costs their calls nothing. A channel asks once per message type.
        Raising means the type's rules cannot be enforced: every call sending it then fails with
        `INTERNAL`.

        The returned function reports a message that breaks its rules by raising a
        `TrevRpcError`, which the call fails with as it is, normally `INVALID_ARGUMENT` with
        details that say why. Any other exception fails the call with `INVALID_ARGUMENT`."""
        ...


class MessageValidator:
    """Checks single messages with a `Validator`, as a channel configured with one checks the
    requests it sends. It asks the validator for each message type's function once and reuses it,
    so it is meant to be built once and shared.

    Tests use it to assert that a request would be rejected, without a server::

        with pytest.raises(TrevRpcError) as caught:
            MessageValidator(validator).validate(AddRequest())
    """

    __slots__ = ("_compiled", "_validator")

    def __init__(self, validator: Validator) -> None:
        self._validator = validator
        self._compiled: dict[type[Message], ValidateFunc | None] = {}
        """Each message type's function, `None` for a type that needs no checking."""

    def validate(self, message: Message) -> None:
        """Checks `message` as a channel checks a request of its type, raising the `TrevRpcError`
        the call would fail with: the one the validator raised, `INVALID_ARGUMENT` for any other
        exception, and `INTERNAL` for a type whose rules cannot be enforced. A type the validator
        does not check is never invalid."""
        validate = self._compile(type(message))
        if validate is None:
            return
        try:
            validate(message)
        except TrevRpcError:
            raise
        except Exception as error:
            raise TrevRpcError(Code.INVALID_ARGUMENT, str(error) or type(error).__name__) from error

    def _compile(self, message_type: type[Message]) -> ValidateFunc | None:
        try:
            return self._compiled[message_type]
        except KeyError:
            pass
        validate: ValidateFunc | None
        try:
            validate = self._validator.validate_func(message_type)
        except Exception as error:
            validate = _failing(f"cannot validate {message_type.desc().type_name}: {error}", error)
        self._compiled[message_type] = validate
        return validate


def _failing(reason: str, cause: Exception) -> ValidateFunc:
    """Fails closed: a type whose rules cannot be enforced is never sent."""

    def validate(_: Message) -> None:
        raise TrevRpcError(Code.INTERNAL, reason) from cause

    return validate
