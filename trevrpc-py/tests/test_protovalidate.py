"""trevrpc.protovalidate: checking requests against `buf.validate` rules, standard and CEL, before a
channel sends them, and reading the violations of any status."""

from __future__ import annotations

import unittest

import protovalidate
import trevrpc
from protobuf import Message, Oneof
from protobuf.wkt.google.protobuf.wrappers_pb import StringValue
from protovalidate import ViolationsPb
from trevrpc import Code, MessageValidator, Method, MethodKind, TrevRpcError
from trevrpc.protovalidate import (
    MAX_DETAIL_VIOLATIONS,
    VIOLATIONS_TYPE,
    RequestValidator,
    validate,
    violations,
)

from .gen.rules_pb import (
    Batch,
    BatchRequest,
    BrokenRequest,
    ChoiceRequest,
    CreateUserRequest,
    DivisorRequest,
    FragileRequest,
    LookupRequest,
    NestedRequest,
    RangeRequest,
    SyntaxErrorRequest,
    User,
)
from .server import TestServer, start_server

VALID = CreateUserRequest(name="ada", email="ada@example.com")


def user(name: str) -> CreateUserRequest:
    return CreateUserRequest(name=name, email="ada@example.com")


def rejection(message: Message) -> TrevRpcError:
    """The error `validate` raises for `message`, which must be invalid."""
    try:
        validate(message)
    except TrevRpcError as error:
        return error
    raise AssertionError(f"{message!r} is valid")


def paths(error: TrevRpcError) -> list[tuple[str, str]]:
    """Each violation of `error`'s details: its rule ID and its field path's names."""
    return [
        (
            violation.rule_id,
            ".".join(e.field_name for e in violation.field.elements) if violation.field else "",
        )
        for violation in violations(error)
    ]


class RulesTest(unittest.TestCase):
    def test_types_without_rules_are_not_checked(self) -> None:
        validator = RequestValidator()
        # LookupRequest holds a recursive type through a list and a map.
        for message_type in (User, LookupRequest):
            self.assertIsNone(validator.validate_func(message_type), message_type)

    def test_rules_are_found_wherever_they_are(self) -> None:
        validator = RequestValidator()
        # Fields, map values, list elements two levels down, message rules and oneof rules.
        for message_type in (CreateUserRequest, BatchRequest, NestedRequest, RangeRequest):
            self.assertIsNotNone(validator.validate_func(message_type), message_type)
        self.assertIsNotNone(validator.validate_func(ChoiceRequest))

    def test_valid_message(self) -> None:
        validate(VALID)
        validate(RangeRequest(start=1, end=2))
        validate(ChoiceRequest(kind=Oneof("a", "x")))

    def test_field_rule(self) -> None:
        error = rejection(CreateUserRequest(name="", email="ada@example.com"))
        self.assertEqual(error.code, Code.INVALID_ARGUMENT)
        self.assertEqual(error.message, "validation error: name: must be at least 1 characters")
        self.assertEqual(paths(error), [("string.min_len", "name")])
        self.assertEqual([d.type for d in error.details], [VIOLATIONS_TYPE])

    def test_message_rule(self) -> None:
        error = rejection(RangeRequest(start=2, end=1))
        self.assertEqual(error.message, "validation error: start must not be after end")
        self.assertEqual(paths(error), [("range", "")])

    def test_oneof_rule(self) -> None:
        error = rejection(ChoiceRequest())
        self.assertEqual(error.code, Code.INVALID_ARGUMENT)
        self.assertEqual(paths(error), [("required", "kind")])

    def test_every_violation_is_listed_and_counted(self) -> None:
        error = rejection(CreateUserRequest(name="", email="nope"))
        self.assertEqual(
            error.message, "validation error: name: must be at least 1 characters (and 1 more)"
        )
        self.assertEqual(paths(error), [("string.min_len", "name"), ("string.email", "email")])

    def test_fail_fast_stops_at_the_first_violation(self) -> None:
        checker = MessageValidator(RequestValidator(fail_fast=True))
        with self.assertRaises(TrevRpcError) as caught:
            checker.validate(CreateUserRequest(name="", email="nope"))
        self.assertEqual(paths(caught.exception), [("string.min_len", "name")])

    def test_subscripts_in_the_message(self) -> None:
        error = rejection(BatchRequest(users={"ada": CreateUserRequest(email="ada@example.com")}))
        self.assertEqual(
            error.message, 'validation error: users["ada"].name: must be at least 1 characters'
        )
        error = rejection(NestedRequest(batch=Batch(users=[VALID, CreateUserRequest()])))
        self.assertTrue(
            error.message.startswith("validation error: batch.users[1].name: "), error.message
        )

    def test_detail_lists_at_most_max_detail_violations(self) -> None:
        count = MAX_DETAIL_VIOLATIONS + 50
        error = rejection(Batch(users=[CreateUserRequest(email="a@b.c")] * count))
        self.assertTrue(error.message.endswith(f" (and {count - 1} more)"), error.message)
        self.assertEqual(len(violations(error)), MAX_DETAIL_VIOLATIONS)

    def test_rules_that_cannot_compile_fail_every_message(self) -> None:
        with self.assertRaises(protovalidate.CompilationError):
            RequestValidator().validate_func(SyntaxErrorRequest)
        for _ in range(2):
            with self.assertRaises(TrevRpcError) as caught:
                validate(SyntaxErrorRequest(name="x"))
            self.assertEqual(caught.exception.code, Code.INTERNAL)
            prefix = "cannot validate test.rules.SyntaxErrorRequest: "
            self.assertTrue(caught.exception.message.startswith(prefix), caught.exception.message)

    def test_rules_naming_a_missing_field_fail_every_message(self) -> None:
        # protovalidate finds the missing field only when it evaluates the rule.
        self.assertIsNotNone(RequestValidator().validate_func(BrokenRequest))
        for name in ("", "x"):
            error = rejection(BrokenRequest(name=name))
            self.assertEqual(error.code, Code.INTERNAL)
            self.assertTrue(error.message.startswith("request validation failed: "), error.message)

    def test_rules_that_cannot_be_evaluated_for_the_empty_message(self) -> None:
        self.assertIsNotNone(RequestValidator().validate_func(DivisorRequest))
        validate(DivisorRequest(count=5))
        self.assertEqual(paths(rejection(DivisorRequest(count=200))), [("divides", "count")])
        error = rejection(DivisorRequest())
        self.assertEqual(error.code, Code.INTERNAL)
        self.assertTrue(error.message.startswith("request validation failed: "), error.message)

    def test_rules_that_cannot_be_evaluated(self) -> None:
        validate(FragileRequest(number="1"))
        self.assertEqual(rejection(FragileRequest(number="-1")).code, Code.INVALID_ARGUMENT)
        error = rejection(FragileRequest(number="x"))
        self.assertEqual(error.code, Code.INTERNAL)
        self.assertTrue(error.message.startswith("request validation failed: "), error.message)


class _Counting:
    """A validator that rejects messages with an exception of its own, and counts how often it was
    asked for a type's function."""

    def __init__(self, broken: bool = False) -> None:
        self.asked = 0
        self.broken = broken

    def validate_func(self, message_type: type[Message], /) -> trevrpc.ValidateFunc | None:
        self.asked += 1
        if self.broken:
            raise ValueError("no rules for you")

        def check(message: Message) -> None:
            raise ValueError(f"{message.desc().type_name} is never valid")

        return check


class MessageValidatorTest(unittest.TestCase):
    def test_functions_are_asked_for_once(self) -> None:
        validator = _Counting()
        checker = MessageValidator(validator)
        for _ in range(3):
            with self.assertRaises(TrevRpcError) as caught:
                checker.validate(User())
            self.assertEqual(caught.exception.code, Code.INVALID_ARGUMENT)
            self.assertEqual(caught.exception.message, "test.rules.User is never valid")
        self.assertEqual(validator.asked, 1)

    def test_a_validator_that_cannot_compile_fails_closed(self) -> None:
        validator = _Counting(broken=True)
        checker = MessageValidator(validator)
        for _ in range(2):
            with self.assertRaises(TrevRpcError) as caught:
                checker.validate(User())
            self.assertEqual(caught.exception.code, Code.INTERNAL)
            self.assertEqual(
                caught.exception.message, "cannot validate test.rules.User: no rules for you"
            )
        self.assertEqual(validator.asked, 1)


def _method[Req: Message](
    name: str, kind: MethodKind, request: type[Req] = CreateUserRequest
) -> Method[Req, StringValue]:
    # CreateUserRequest's name is field 1, a string, as StringValue's value, which the test server
    # takes.
    return Method(f"/test.Echo/{name}", kind, request, StringValue)


UNARY = _method("Unary", MethodKind.UNARY)
SERVER_STREAM = _method("ServerStream", MethodKind.SERVER_STREAMING)
CLIENT_STREAM = _method("ClientStream", MethodKind.CLIENT_STREAMING)
BIDI = _method("Bidi", MethodKind.BIDI_STREAMING)
INVALID = _method("Invalid", MethodKind.UNARY, StringValue)


class ChannelTest(unittest.IsolatedAsyncioTestCase):
    """A channel with a `RequestValidator`, over native QUIC to the test server."""

    server: TestServer
    channel: trevrpc.Channel

    async def asyncSetUp(self) -> None:
        self.server = await start_server()
        self.channel = self.new_channel(validator=RequestValidator())

    def new_channel(self, **options: object) -> trevrpc.Channel:
        return trevrpc.Channel(
            f"127.0.0.1:{self.server.port}",
            ca=self.server.ca,
            server_name="localhost",
            transport="quic",
            **options,  # pyright: ignore[reportArgumentType]
        )

    async def asyncTearDown(self) -> None:
        await self.channel.close()
        self.server.close()

    async def test_unary(self) -> None:
        response = await self.channel.unary(UNARY, VALID)
        self.assertEqual(response.value, "ada")
        with self.assertRaises(TrevRpcError) as caught:
            await self.channel.unary(UNARY, user(""))
        self.assertEqual(paths(caught.exception), [("string.min_len", "name")])
        self.assertEqual(len(self.server.calls), 1, "the invalid request was sent")

    async def test_server_stream(self) -> None:
        with self.assertRaises(TrevRpcError) as caught:
            self.channel.server_stream(SERVER_STREAM, CreateUserRequest(name="2", email="x"))
        self.assertEqual(paths(caught.exception), [("string.email", "email")])
        self.assertEqual(self.server.calls, [])

    async def test_a_rejected_send_leaves_the_call_going(self) -> None:
        call = self.channel.client_stream(CLIENT_STREAM)
        await call.send(user("a"))
        with self.assertRaises(TrevRpcError) as caught:
            await call.send(user(""))
        self.assertEqual(caught.exception.code, Code.INVALID_ARGUMENT)
        await call.send(user("b"))
        response = await call.close_and_recv()
        self.assertEqual(response.value, "ab")

        bidi = self.channel.bidi_stream(BIDI)
        with self.assertRaises(TrevRpcError):
            await bidi.send(user(""))
        await bidi.send(user("c"))
        received = await bidi.recv()
        self.assertEqual(received.value if received else None, "c")
        await bidi.close_send()
        self.assertIsNone(await bidi.recv())

    async def test_send_all_stops_at_a_rejected_request(self) -> None:
        call = self.channel.client_stream(CLIENT_STREAM)
        with self.assertRaises(TrevRpcError) as caught:
            await call.send_all([user("a"), user(""), user("b")])
        self.assertEqual(paths(caught.exception), [("string.min_len", "name")])
        # The requests before it were sent, none after it, and the request stream is still open.
        await call.send(user("c"))
        response = await call.close_and_recv()
        self.assertEqual(response.value, "ac")

    async def test_types_without_rules_are_sent(self) -> None:
        response = await self.channel.unary(_method("Unary", MethodKind.UNARY, User), User())
        self.assertEqual(response.value, "")

    async def test_violations_from_the_server(self) -> None:
        # Decoded with protovalidate's bindings, from a detail encoded with the tests' own.
        with self.assertRaises(TrevRpcError) as caught:
            await self.channel.unary(INVALID, StringValue(value="x"))
        error = caught.exception
        self.assertEqual(error.code, Code.INVALID_ARGUMENT)
        self.assertEqual(paths(error), [("string.min_len", "name")])
        [listed] = error.find_details(ViolationsPb)
        self.assertEqual(listed.violations[0].message, "must be at least 1 characters")
