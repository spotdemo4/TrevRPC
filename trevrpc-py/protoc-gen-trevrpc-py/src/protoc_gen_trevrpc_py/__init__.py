"""protoc-gen-trevrpc-py: generates TrevRPC clients for the messages protoc-gen-py generates.

For each `.proto` file with services it writes `<file>_trevrpc.py`, next to protoc-gen-py's
`<file>_pb.py`. Each service gets a `Method` descriptor per method, carrying its
`(trevrpc.method)` options, and a client class whose methods call them on a `trevrpc.Channel`.
"""

from __future__ import annotations

import importlib.metadata
import re
from typing import TYPE_CHECKING

from protobuf.plugin import (
    File,
    Ident,
    Module,
    Schema,
    escape_public_identifier,
    get_comments,
    python_keywords,
    run,
)

from ._options_pb import Compression, ext_method

if TYPE_CHECKING:
    from protobuf import DescFile, DescMethod, DescService

_TYPING = Module("typing")
_FINAL = _TYPING.ident("Final")
_UNPACK = _TYPING.ident("Unpack")

_TREVRPC = Module("trevrpc")
_CHANNEL = _TREVRPC.ident("Channel", type_only=True)
_CALL_OPTIONS = _TREVRPC.ident("CallOptions", type_only=True)
_METHOD = _TREVRPC.ident("Method")
_METHOD_KIND = _TREVRPC.ident("MethodKind")
_COMPRESSION = _TREVRPC.ident("Compression")
_RESPONSE_STREAM = _TREVRPC.ident("ResponseStream", type_only=True)
_CLIENT_STREAMING_CALL = _TREVRPC.ident("ClientStreamingCall", type_only=True)
_BIDI_STREAMING_CALL = _TREVRPC.ident("BidiStreamingCall", type_only=True)

_KINDS = {
    "unary": "UNARY",
    "client_streaming": "CLIENT_STREAMING",
    "server_streaming": "SERVER_STREAMING",
    "bidi_streaming": "BIDI_STREAMING",
}

_COMPRESSIONS = {
    Compression.MESSAGE: "MESSAGE",
    Compression.STREAM: "STREAM",
}

# Names a client's methods must not take: Python's keywords and the client's own attributes.
_RESERVED = frozenset(python_keywords() | {"channel"})

_RE_UPPER_TO_LOWER = re.compile("([^_])([A-Z][a-z]+)")
_RE_LOWER_TO_UPPER = re.compile("([a-z0-9])([A-Z])")


def _snake_case(text: str) -> str:
    return _RE_LOWER_TO_UPPER.sub(r"\1_\2", _RE_UPPER_TO_LOWER.sub(r"\1_\2", text)).lower()


def _method_name(method: DescMethod) -> str:
    return escape_public_identifier(_snake_case(method.name), _RESERVED)


def _descriptor_name(service: DescService, method: DescMethod) -> str:
    return f"{_snake_case(service.name)}_{_snake_case(method.name)}".upper()


def _generate(schema: Schema[None]) -> None:
    for file in schema.files_to_generate:
        if file.services:
            _generate_file(schema.generate_file(file, "_trevrpc.py"), file)


def _generate_file(f: File, desc: DescFile) -> None:
    f.preamble(desc)
    for service in desc.services:
        f.print()
        _generate_descriptors(f, service)
        f.print()
        _generate_client(f, service)


def _generate_descriptors(f: File, service: DescService) -> None:
    for method in service.methods:
        options = method.proto.options
        settings = options[ext_method] if options is not None and ext_method in options else None
        f.print(
            _descriptor_name(service, method),
            ": ",
            _FINAL,
            " = ",
            _METHOD,
            "(",
        )
        f.print(f'    "/{service.type_name}/{method.name}",')
        f.print("    ", _METHOD_KIND, ".", _KINDS[method.method_kind], ",")
        f.print("    ", Ident.for_desc(method.input), ",")
        f.print("    ", Ident.for_desc(method.output), ",")
        if settings is not None:
            if settings.unreliable_requests:
                f.print("    unreliable_requests=True,")
            if settings.unreliable_responses:
                f.print("    unreliable_responses=True,")
            compression = _COMPRESSIONS.get(settings.compression)
            if compression is not None:
                f.print("    compression=", _COMPRESSION, ".", compression, ",")
        f.print(")")
        with f.doc():
            f.print(f"The {service.type_name}.{method.name} method.")


def _generate_client(f: File, service: DescService) -> None:
    name = escape_public_identifier(f"{service.name}Client", python_keywords())
    with f.scope("class ", name, ":"):
        _print_docstring(f, service, fallback=f"A client of {service.type_name}.")
        f.print()
        f.print('__slots__ = ("channel",)')
        f.print()
        with f.scope("def __init__(self, channel: ", _CHANNEL, ") -> None:"):
            f.print("self.channel = channel")
        for method in service.methods:
            f.print()
            _generate_method(f, service, method)


def _generate_method(f: File, service: DescService, method: DescMethod) -> None:
    descriptor = _descriptor_name(service, method)
    request = Ident.for_desc(method.input)
    response = Ident.for_desc(method.output)
    options = ("**options: ", _UNPACK, "[", _CALL_OPTIONS, "]")
    local = _method_name(method)
    match method.method_kind:
        case "unary":
            with f.scope(
                "async def ", local, "(self, request: ", request, ", /, ", *options,
                ") -> ", response, ":",
            ):  # fmt: skip
                _print_docstring(f, method)
                f.print("return await self.channel.unary(", descriptor, ", request, **options)")
        case "server_streaming":
            with f.scope(
                "def ", local, "(self, request: ", request, ", /, ", *options,
                ") -> ", _RESPONSE_STREAM, "[", response, "]:",
            ):  # fmt: skip
                _print_docstring(f, method)
                f.print("return self.channel.server_stream(", descriptor, ", request, **options)")
        case "client_streaming":
            with f.scope(
                "def ", local, "(self, /, ", *options,
                ") -> ", _CLIENT_STREAMING_CALL, "[", request, ", ", response, "]:",
            ):  # fmt: skip
                _print_docstring(f, method)
                f.print("return self.channel.client_stream(", descriptor, ", **options)")
        case _:
            with f.scope(
                "def ", local, "(self, /, ", *options,
                ") -> ", _BIDI_STREAMING_CALL, "[", request, ", ", response, "]:",
            ):  # fmt: skip
                _print_docstring(f, method)
                f.print("return self.channel.bidi_stream(", descriptor, ", **options)")


def _print_docstring(f: File, desc: DescService | DescMethod, fallback: str | None = None) -> None:
    lines = get_comments(desc).lines()
    if not lines:
        if fallback is None:
            return
        lines = [fallback]
    with f.doc():
        for line in lines:
            f.print(line)


def main() -> None:
    """Runs the plugin."""
    run(
        "protoc-gen-trevrpc-py",
        importlib.metadata.version("protoc-gen-trevrpc-py"),
        _generate,
    )
