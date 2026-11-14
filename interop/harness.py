"""Shared helpers for interop/run.py, bench/run.py, and the Android interop runner.

They generate a throwaway certificate, run interop-style servers (see interop/README.md), and run
commands under a deadline in their own process group, so a command that hangs is stopped along
with any processes it started.
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import queue
import shlex
import signal
import ssl
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast

# Default flags a package wrapper sets for its runner; see parse_with_defaults.
DEFAULTS_VARIABLE = "TREVRPC_RUNNER_DEFAULTS"
SERVER_TIMEOUT = 30
SHUTDOWN_TIMEOUT = 10
KILL_GRACE = 5
# How long a server that did not exit gets, after SIGQUIT, to print its state. A Go server prints
# the stack of every goroutine and exits.
DUMP_GRACE = 5
# How many lines from the end of a server's log a server that did not stop cleanly prints.
SERVER_LOG_LINES = 2000


class RunnerError(RuntimeError):
    """An infrastructure failure that stops the whole run."""


# --- arguments -------------------------------------------------------------------------------


@dataclass(frozen=True)
class NamedCommand:
    """A `NAME=CMD` argument; CMD is split like a shell command line."""

    name: str
    command: list[str]


def command_line(value: str) -> list[str]:
    """Parses a command that may carry arguments, as an argparse type."""
    try:
        command = shlex.split(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"cannot parse command {value!r}: {error}") from error
    if not command:
        raise argparse.ArgumentTypeError("empty command")
    return command


def named_command(value: str) -> NamedCommand:
    """Parses a `NAME=CMD` argument, as an argparse type."""
    name, separator, command = value.partition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError(f"expected NAME=CMD, got {value!r}")
    return NamedCommand(name, command_line(command))


def comma_list(value: str) -> list[str]:
    """Parses a comma-separated list, as an argparse type."""
    return [item for item in value.split(",") if item]


def parse_with_defaults(
    parser: argparse.ArgumentParser, argv: list[str], matrix: tuple[str, ...]
) -> argparse.Namespace:
    """Parses argv, falling back to the packaged matrix when no matrix option was given.

    A package wrapper puts the default flags, such as the packaged servers and clients, in
    TREVRPC_RUNNER_DEFAULTS. They apply only when none of the `matrix` options (argparse dests)
    is on the command line, so an explicit matrix replaces the defaults instead of adding to them.
    """
    defaults = os.environ.pop(DEFAULTS_VARIABLE, "")
    args = parser.parse_args(argv)
    if defaults and not any(getattr(args, name) for name in matrix):
        try:
            default_argv = shlex.split(defaults)
        except ValueError as error:
            parser.error(f"cannot parse {DEFAULTS_VARIABLE}: {error}")
        args = parser.parse_args([*default_argv, *argv])
    return args


# --- processes -------------------------------------------------------------------------------


def stop_group(
    process: subprocess.Popen[str] | subprocess.Popen[bytes], grace: float = KILL_GRACE
) -> None:
    """Terminates a running process's group, then kills it after `grace` seconds."""
    for signal_number, timeout in ((signal.SIGTERM, grace), (signal.SIGKILL, None)):
        if process.poll() is not None:
            return
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal_number)
        with contextlib.suppress(subprocess.TimeoutExpired):
            _ = process.wait(timeout=timeout)


@dataclass(frozen=True)
class Completed:
    """A finished command: its exit status, or None if its deadline passed, and its stdout."""

    returncode: int | None
    stdout: str


def run_with_deadline(
    command: list[str],
    timeout: float,
    *,
    stdout: IO[str] | int | None = None,
    stderr: IO[str] | int | None = None,
) -> Completed:
    """Runs a command in a new process group and stops the group when the deadline passes.

    stdout is captured only when it is subprocess.PIPE.
    """
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            text=True,
            start_new_session=True,
        )
    except OSError as error:
        raise RunnerError(f"could not run {shlex.join(command)}: {error}") from error
    with process:
        try:
            output, _ = process.communicate(timeout=timeout)
            return Completed(process.returncode, output or "")
        except subprocess.TimeoutExpired:
            return Completed(None, "")
        finally:
            stop_group(process)


def run_checked(
    command: list[str],
    *,
    timeout: float = 60,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Runs a command to completion and returns its stdout, raising RunnerError on failure."""
    try:
        result = subprocess.run(
            command,
            input=input_text,
            text=True,
            capture_output=True,
            timeout=timeout,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        raise RunnerError(f"timed out after {timeout}s: {shlex.join(command)}") from error
    except OSError as error:
        raise RunnerError(f"could not run {shlex.join(command)}: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RunnerError(f"exited {result.returncode}: {shlex.join(command)}\n{detail}")
    return result.stdout


# How many lines of a failed cell's log the closing summary repeats.
FAILURE_LINES = 20


@dataclass(frozen=True)
class Failure:
    """A failed cell, or a server that did not stop cleanly, with the lines that say why."""

    name: str
    details: list[str]


def failure_details(log: str, limit: int = FAILURE_LINES) -> list[str]:
    """The last `limit` lines of a failed cell's log that are not passing or skipped interop
    scenarios: its `fail` lines, browser errors, timeouts, and anything a crashing client
    printed."""
    lines = [
        line for line in log.splitlines() if line.strip() and not line.startswith(("ok ", "skip "))
    ]
    return lines[-limit:]


def failure_summary(failures: list[Failure]) -> str:
    """The closing report of failed cells and why each failed. It comes last, so it survives a
    log truncated to its final lines, as CI and Nix print them."""
    lines: list[str] = []
    for failure in failures:
        lines.append(f"failed: {failure.name}")
        lines += [f"    {detail}" for detail in failure.details]
    return "\n".join(lines)


# --- compression -----------------------------------------------------------------------------

# The golden vector holding the interop dictionary, which every interop server and compressing
# client holds for the Echo messages.
DICTIONARY_ENTRY = "dict.d1.dictionary = "


def write_vectors_dictionary(vectors: str, origin: str, directory: Path) -> Path:
    """Writes the interop dictionary from the text of the compression golden vectors, read from
    ORIGIN, to DIRECTORY and returns its path."""
    for line in vectors.splitlines():
        if line.startswith(DICTIONARY_ENTRY):
            path = directory / "interop.dict"
            _ = path.write_bytes(bytes.fromhex(line.removeprefix(DICTIONARY_ENTRY)))
            return path
    raise RunnerError(f"{origin} has no {DICTIONARY_ENTRY.strip(' =')}")


# --- certificates ----------------------------------------------------------------------------


@dataclass(frozen=True)
class Certificate:
    cert: Path
    key: Path

    def der(self) -> bytes:
        return ssl.PEM_cert_to_DER_cert(self.cert.read_text(encoding="ascii"))


def generate_certificate(directory: Path) -> Certificate:
    """Writes a self-signed ECDSA P-256 certificate for localhost and 127.0.0.1, valid 10 days."""
    certificate = Certificate(directory / "cert.pem", directory / "key.pem")
    _ = run_checked(
        [
            "openssl", "req", "-x509",
            "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes",
            "-days", "10", "-subj", "/CN=localhost",
            "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
            "-addext", "basicConstraints=critical,CA:FALSE",
            "-addext", "extendedKeyUsage=serverAuth",
            "-keyout", str(certificate.key), "-out", str(certificate.cert),
        ]
    )  # fmt: skip
    return certificate


# --- servers ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class Readiness:
    """What a server's readiness line reports: the ports of the transports it serves, and the
    scenarios it supports, or None when it supports every scenario."""

    ports: dict[str, int]
    scenarios: list[str] | None


def parse_readiness(line: str) -> Readiness:
    """Parses a server's readiness line, such as `{"native":N,"qmux":N,...}`. A server that
    serves only some transports leaves the others' ports out, and one that supports only some
    scenarios lists them under `scenarios`."""
    try:
        value = cast(object, json.loads(line))
    except json.JSONDecodeError as error:
        raise RunnerError(f"server readiness line is not JSON: {line!r}") from error
    if not isinstance(value, dict):
        raise RunnerError(f"server readiness line is not a JSON object of ports: {line!r}")
    fields = dict(cast(dict[str, object], value))
    scenarios: list[str] | None = None
    if "scenarios" in fields:
        listed = fields.pop("scenarios")
        if not isinstance(listed, list) or not all(
            isinstance(name, str) and name for name in cast(list[object], listed)
        ):
            raise RunnerError(f"server readiness line has invalid scenarios: {line!r}")
        scenarios = cast(list[str], listed)
    if not fields:
        raise RunnerError(f"server readiness line reports no port: {line!r}")
    ports: dict[str, int] = {}
    for name, port in fields.items():
        if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
            raise RunnerError(f"server readiness line has an invalid {name!r} port: {line!r}")
        ports[name] = port
    return Readiness(ports, scenarios)


class Server:
    """A running `CMD server --cert --key ARG...`, which prints its ports as one JSON line and
    runs until its stdin reaches EOF.

    Its ports land in OUT/server-NAME.ports and its stderr in OUT/server-NAME.log.
    """

    name: str
    log: Path
    ports: dict[str, int]
    # The scenarios the server supports, or None for every scenario.
    scenarios: list[str] | None
    process: subprocess.Popen[str]

    def __init__(
        self,
        name: str,
        command: list[str],
        certificate: Certificate,
        out: Path,
        arguments: list[str] | None = None,
    ) -> None:
        self.name = name
        self.log = out / f"server-{name}.log"
        argv = [
            *command,
            "server",
            "--cert",
            str(certificate.cert),
            "--key",
            str(certificate.key),
            *(arguments or []),
        ]
        with self.log.open("w", encoding="utf-8") as stderr:
            try:
                self.process = subprocess.Popen(
                    argv,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=stderr,
                    text=True,
                    start_new_session=True,
                )
            except OSError as error:
                raise RunnerError(f"could not start server {shlex.join(argv)}: {error}") from error
        # A thread reads the readiness line, then drains stdout so the server never blocks on it.
        lines: queue.Queue[str] = queue.Queue()
        stdout = cast(IO[str], self.process.stdout)
        threading.Thread(target=_drain, args=(stdout, lines), daemon=True).start()
        try:
            try:
                line = lines.get(timeout=SERVER_TIMEOUT).strip()
            except queue.Empty:
                raise RunnerError(
                    f"server {name} did not report its ports within {SERVER_TIMEOUT}s"
                    + self._log_tail()
                ) from None
            if not line:
                status = self.process.wait(timeout=SERVER_TIMEOUT)
                raise RunnerError(
                    f"server {name} exited with {status} before reporting its ports"
                    + self._log_tail()
                )
            readiness = parse_readiness(line)
            self.ports = readiness.ports
            self.scenarios = readiness.scenarios
        except BaseException:
            _ = self.stop()
            raise
        _ = (out / f"server-{name}.ports").write_text(line + "\n", encoding="utf-8")

    def port(self, name: str) -> int:
        try:
            return self.ports[name]
        except KeyError:
            raise RunnerError(f"server {self.name} did not report a {name!r} port") from None

    def missing(self, *names: str) -> list[str]:
        """The transports among `names` the server does not serve."""
        return [name for name in names if name not in self.ports]

    def stop(self) -> str | None:
        """Closes stdin and waits for the server to exit. After SHUTDOWN_TIMEOUT, it sends
        SIGQUIT, on which a Go server logs every goroutine's stack, and kills the server
        DUMP_GRACE later.

        Returns a description of the problem unless the server exited cleanly by itself.
        """
        if self.process.stdin is not None:
            with contextlib.suppress(OSError):
                self.process.stdin.close()
        problem = None
        try:
            status = self.process.wait(timeout=SHUTDOWN_TIMEOUT)
            if status != 0:
                problem = f"server {self.name} exited with {status}; see {self.log}"
        except subprocess.TimeoutExpired:
            problem = (
                f"server {self.name} did not exit within {SHUTDOWN_TIMEOUT}s; "
                "sent SIGQUIT and killed it"
            )
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.process.pid, signal.SIGQUIT)
            with contextlib.suppress(subprocess.TimeoutExpired):
                _ = self.process.wait(timeout=DUMP_GRACE)
            stop_group(self.process, grace=0)
        return problem

    def log_size(self) -> int:
        """How many bytes the server has logged so far, for log_since."""
        try:
            return self.log.stat().st_size
        except OSError:
            return 0

    def log_since(self, offset: int) -> list[str]:
        """The non-empty lines the server logged after log_size returned OFFSET."""
        try:
            with self.log.open("rb") as log:
                _ = log.seek(offset)
                text = log.read().decode("utf-8", errors="replace")
        except OSError:
            return []
        return [line for line in text.splitlines() if line.strip()]

    def _log_tail(self) -> str:
        lines = self.log.read_text(encoding="utf-8", errors="replace").splitlines()
        return "; log:\n" + "\n".join(lines[-40:]) if lines else ""


def server_details(server: Server, offset: int, limit: int = FAILURE_LINES) -> list[str]:
    """The last `limit` lines the server logged while a cell ran, from the log_size OFFSET
    taken before it, to add to the cell's failure."""
    return [f"server {server.name}: {line}" for line in server.log_since(offset)[-limit:]]


def stop_server(server: Server) -> Failure | None:
    """Stops the server. Unless it exited cleanly by itself, it prints why and the end of the
    server's log, which holds a Go server's goroutines if it hung, and returns the failure, with
    the last lines of the log, for the closing summary."""
    problem = server.stop()
    if problem is None:
        return None
    lines = server.log_since(0)
    print(problem, file=sys.stderr)
    if lines:
        print(f"--- {server.log}, at most its last {SERVER_LOG_LINES} lines", file=sys.stderr)
        print("\n".join(lines[-SERVER_LOG_LINES:]), file=sys.stderr)
        print(f"--- end of {server.log}", file=sys.stderr, flush=True)
    return Failure(f"server-{server.name}", [problem, *lines[-FAILURE_LINES:]])


def _drain(stdout: IO[str], lines: queue.Queue[str]) -> None:
    with stdout:
        lines.put(stdout.readline())
        for _ in stdout:
            pass
