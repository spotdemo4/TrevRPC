#!/usr/bin/env python3
from __future__ import annotations

import json
import subprocess
import sys
import typing

MAX_COMMAND_BYTES = 262_144
CAPABILITIES = [
    "codec.decode",
    "codec.encode",
    "framing.decode_stream",
    "framing.encode",
    "state.client_stream",
    "state.server_stream",
]


def start_peer(executable: str, peer: str) -> subprocess.Popen[bytes]:
    process = subprocess.Popen(
        [executable, "--protocol", "1"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert process.stdout is not None
    ready_line: bytes = typing.cast(bytes, process.stdout.readline())
    assert len(ready_line) <= 65_536
    ready: dict[str, typing.Any] = typing.cast(
        dict[str, typing.Any], json.loads(ready_line)
    )
    assert set(ready) == {"schema_version", "event", "peer", "pid", "capabilities"}
    assert ready["schema_version"] == 1
    assert ready["event"] == "ready"
    assert ready["peer"] == peer
    assert isinstance(ready["pid"], int) and ready["pid"] > 0
    assert ready["capabilities"] == CAPABILITIES
    return process


def run_commands(
    executable: str, peer: str, commands: list[str]
) -> list[dict[str, typing.Any]]:
    process = start_peer(executable, peer)
    assert process.stdin is not None
    assert process.stdout is not None
    assert process.stderr is not None
    for command in commands:
        _ = process.stdin.write(command.encode("ascii") + b"\n")
    _ = process.stdin.write(b"STOP\n")
    process.stdin.flush()
    process.stdin.close()
    lines: list[bytes] = typing.cast(list[bytes], process.stdout.readlines())
    stderr: bytes = typing.cast(bytes, process.stderr.read())
    return_code = process.wait(timeout=10)
    assert return_code == 0, stderr.decode("utf-8", errors="replace")
    assert len(lines) == len(commands)
    assert all(len(line) <= 65_536 for line in lines)
    return [typing.cast(dict[str, typing.Any], json.loads(line)) for line in lines]


def expect_fatal(executable: str, peer: str, data: bytes) -> None:
    process = start_peer(executable, peer)
    stdout: bytes
    stdout, _ = process.communicate(data, timeout=10)
    assert process.returncode == 2
    lines = stdout.splitlines()
    assert len(lines) == 1
    assert len(lines[0]) <= 65_536
    event: dict[str, typing.Any] = typing.cast(
        dict[str, typing.Any], json.loads(lines[0])
    )
    assert set(event) == {"schema_version", "event", "peer", "message"}
    assert event["schema_version"] == 1
    assert event["event"] == "fatal"
    assert event["peer"] == peer
    assert isinstance(event["message"], str) and event["message"]


def encode_varint(value: int) -> bytes:
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def main() -> None:
    executable, peer = sys.argv[1:]
    results: list[dict[str, typing.Any]] = run_commands(
        executable,
        peer,
        [
            "RUN\t1\tstate.server.message_ok\tstate.server_stream\t2\t22031a0161\t08012a0a0a057472616365120107",
            "RUN\t2\tstate.server.empty_fin\tstate.server_stream\t0",
            "RUN\t3\tstate.client.one_response\tstate.client_stream\t2\t22031a0161\t0801",
            "RUN\t4\tstate.client.two_responses\tstate.client_stream\t3\t22031a0161\t22031a0162\t0801",
            "RUN\t5\tstate.client.remote_invalid_argument\tstate.client_stream\t1\t08011003",
            "RUN\t6\tstate.client.malformed_typed_payload\tstate.client_stream\t2\t220108\t0801",
            "RUN\t7\tstate.server.trailing_after_status\tstate.server_stream\t2\t0801\t22031a0161",
            "RUN\t8\tframing.partial_header\tframing.decode_stream\trpc_stream_frame\t16\t1\t00",
            "RUN\t9\tstate.client.unknown_additive\tstate.client_stream\t2\t22070a01781a026f6b\t0801",
            "RUN\t10\tstate.client.wrong_body_wire\tstate.client_stream\t2\t22021801\t0801",
            "RUN\t11\tstate.client.trailing_malformed\tstate.client_stream\t3\t22031a0161\t0801\t80",
            "RUN\t12\tstate.client.remote_zero\tstate.client_stream\t1\t0801100e",
            "RUN\t13\tstate.client.remote_two\tstate.client_stream\t3\t22031a0161\t22031a0162\t0801100e",
            "RUN\t14\tstate.client.missing_two\tstate.client_stream\t2\t22031a0161\t22031a0162",
            "RUN\t15\tstate.client.clean_ok_zero\tstate.client_stream\t1\t0801",
            "RUN\t16\tstate.server.unknown_additive\tstate.server_stream\t2\t22070a01781a026f6b\t0801",
            "RUN\t17\tstate.server.wrong_body_wire\tstate.server_stream\t2\t22021801\t0801",
            "RUN\t18\tstate.server.trailing_malformed\tstate.server_stream\t2\t0801\t80",
        ],
    )
    assert results[0]["outcome"] == "success"
    assert results[0]["transport_close_count"] == "1"
    assert results[0]["events"] == [
        {"event": "message", "body_hex": "1a0161"},
        {"event": "eof"},
        {"event": "eof"},
    ]
    assert results[1]["outcome"] == "error"
    assert results[1]["category"] == "missing_terminal_status"
    assert results[1]["transport_close_count"] == "1"
    assert results[2]["outcome"] == "success"
    assert results[2]["response_body_hex"] == "1a0161"
    assert results[3]["category"] == "response_cardinality"
    assert results[4]["category"] == "remote_status" and results[4]["status_code"] == 3
    assert results[5]["category"] == "malformed_protobuf"
    assert results[6]["category"] == "trailing_frame"
    assert results[6]["transport_close_count"] == "1"
    assert results[7]["category"] == "incomplete_frame"
    assert set(results[7]) == {
        "schema_version",
        "event",
        "peer",
        "sequence",
        "case_id",
        "operation",
        "outcome",
        "category",
        "status_code",
    }
    assert results[8]["outcome"] == "success"
    assert results[8]["response_body_hex"] == "1a026f6b"
    assert results[9]["category"] == "malformed_protobuf"
    assert results[10]["category"] == "trailing_frame"
    assert results[11]["category"] == "remote_status"
    assert results[11]["status_code"] == 14
    assert results[12]["category"] == "remote_status"
    assert results[12]["status_code"] == 14
    assert results[13]["category"] == "missing_terminal_status"
    assert results[14]["category"] == "response_cardinality"
    assert results[15]["outcome"] == "success"
    assert results[15]["events"] == [
        {"event": "message", "body_hex": "1a026f6b"},
        {"event": "eof"},
        {"event": "eof"},
    ]
    assert results[15]["transport_close_count"] == "1"
    assert results[16]["category"] == "malformed_protobuf"
    assert results[16]["transport_close_count"] == "1"
    assert results[17]["category"] == "trailing_frame"
    assert results[17]["transport_close_count"] == "1"

    invalid_commands = [
        b"RUN\t01\tbad\tstate.server_stream\t0\n",
        b"RUN\t1\tBad\tstate.server_stream\t0\n",
        b"RUN\t1\tbad\tstate.server_stream\t18446744073709551615\n",
        b"RUN\t1\tbad\tcodec.encode\trpc_response\t0\t\t\t18446744073709551615\n",
        b"RUN\t1\tbad\tcodec.decode\trpc_request\tAA\n",
        b"RUN\t1\tbad\tcodec.decode\trpc_request\ta\n",
        b"RUN\t1\tbad\tstate.server_stream\t0\textra\n",
        b"RUN\t1\tbad\tunknown\n",
        b"RUN\t1\tbad\tstate.server_stream\t0\r\n",
        b"RUN\t1\tbad\tstate.server_stream\t0\xff\n",
        b"STOP",
        b"x" * (MAX_COMMAND_BYTES + 1) + b"\n",
    ]
    for invalid in invalid_commands:
        expect_fatal(executable, peer, invalid)

    large_body = b"\x1a" + encode_varint(40_000) + b"\x00" * 40_000 + b"\x30\x01"
    oversized_result = (
        b"RUN\t1\tbound.result\tcodec.decode\trpc_request\t"
        + large_body.hex().encode("ascii")
        + b"\n"
    )
    assert len(oversized_result) <= MAX_COMMAND_BYTES + 1
    expect_fatal(executable, peer, oversized_result)


if __name__ == "__main__":
    main()
