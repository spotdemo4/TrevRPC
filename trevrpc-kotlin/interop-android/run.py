#!/usr/bin/env python3
"""Runs the TrevRPC interop scenarios from an Android emulator over Cronet HTTP/3.

The runner generates a certificate that is valid for the emulator's view of the host, starts an
interop server on the host (see interop/README.md), boots an emulator that trusts the certificate,
runs the instrumented test, and prints one `ok <name>` or `fail <name>: <reason>` line per
scenario. It exits 0 only if every scenario passed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import selectors
import shlex
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

EMULATOR_HOST = "10.0.2.2"
EMULATOR_PORT = 5554
SERIAL = f"emulator-{EMULATOR_PORT}"
TEST_PACKAGE = "zip.trev.trevrpc.interop.android.test"
TEST_RUNNER = "androidx.test.runner.AndroidJUnitRunner"
DEFAULT_SYSTEM_IMAGE = "system-images;android-33;google_apis;x86_64"
CA_DIRECTORY = "/system/etc/security/cacerts"
BOOT_TIMEOUT = 180
SERVER_TIMEOUT = 30
INSTRUMENTATION_TIMEOUT = 180
MIN_FREE_BYTES = 6 * 1024**3


class RunnerError(RuntimeError):
    """An infrastructure failure that prevents scenario results from being produced."""


# --- processes -------------------------------------------------------------------------------


def run(
    command: list[str],
    *,
    timeout: int = 60,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    """Runs a command to completion and raises RunnerError unless it exits 0."""
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
        raise RunnerError(
            f"command timed out after {timeout}s: {shlex.join(command)}"
        ) from error
    except OSError as error:
        raise RunnerError(f"could not run {shlex.join(command)}: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RunnerError(
            f"command exited {result.returncode}: {shlex.join(command)}\n{detail}"
        )
    return result


def probe(command: list[str]) -> str | None:
    """Returns stdout of a quick command, or None if it failed or timed out."""
    try:
        result = subprocess.run(
            command, text=True, capture_output=True, timeout=5, check=False
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    return result.stdout.strip() if result.returncode == 0 else None


def stop(
    process: subprocess.Popen[bytes] | subprocess.Popen[str] | None, timeout: int = 10
) -> None:
    """Terminates a process group, escalating to SIGKILL."""
    if process is None or process.poll() is not None:
        return
    for signal_number in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(process.pid, signal_number)
            process.wait(timeout=timeout)
            return
        except (OSError, subprocess.TimeoutExpired):
            continue


# --- certificates ----------------------------------------------------------------------------


@dataclass(frozen=True)
class Certificates:
    ca: Path
    certificate: Path
    key: Path


def generate_certificates(
    directory: Path, hosts: tuple[str, ...] = ("127.0.0.1", EMULATOR_HOST)
) -> Certificates:
    """Creates an ECDSA P-256 CA and a server certificate for localhost and the given addresses."""
    directory.mkdir(parents=True, exist_ok=True)
    ca_key, ca = directory / "ca.key", directory / "ca.pem"
    key, request, certificate = (
        directory / "server.key",
        directory / "server.csr",
        directory / "server.pem",
    )
    extensions = directory / "server.ext"
    alt_names = ",".join(["DNS:localhost", *(f"IP:{host}" for host in hosts)])
    extensions.write_text(
        f"subjectAltName={alt_names}\n"
        "basicConstraints=critical,CA:FALSE\n"
        "keyUsage=critical,digitalSignature\n"
        "extendedKeyUsage=serverAuth\n",
        encoding="utf-8",
    )
    curve = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]
    run(
        [
            "openssl",
            "req",
            "-x509",
            *curve,
            "-keyout",
            str(ca_key),
            "-out",
            str(ca),
            "-days",
            "10",
            "-subj",
            "/CN=TrevRPC interop CA",
        ]
        + [
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    run(
        [
            "openssl",
            "req",
            "-new",
            *curve,
            "-keyout",
            str(key),
            "-out",
            str(request),
            "-subj",
            "/CN=localhost",
        ]
    )
    run(
        [
            "openssl",
            "x509",
            "-req",
            "-in",
            str(request),
            "-CA",
            str(ca),
            "-CAkey",
            str(ca_key),
            "-CAcreateserial",
        ]
        + ["-out", str(certificate), "-days", "10", "-extfile", str(extensions)]
    )
    return Certificates(ca, certificate, key)


# --- interop server --------------------------------------------------------------------------


def parse_ports(line: str) -> dict[str, int]:
    """Parses the server's `{"native":N,"http3":N,"webtransport":N}` readiness line."""
    try:
        value = json.loads(line)
    except json.JSONDecodeError as error:
        raise RunnerError(f"server readiness line is not JSON: {line!r}") from error
    ports = value if isinstance(value, dict) else {}
    for name in ("native", "http3", "webtransport"):
        port = ports.get(name)
        if (
            not isinstance(port, int)
            or isinstance(port, bool)
            or not 1 <= port <= 65535
        ):
            raise RunnerError(
                f"server readiness line has no valid {name!r} port: {line!r}"
            )
    return ports


def start_server(
    command: str, certificates: Certificates, host: str, output: Path
) -> tuple[subprocess.Popen[str], dict[str, int]]:
    """Starts the interop server and waits for its readiness line."""
    arguments = [
        *shlex.split(command),
        "--host",
        host,
        "--cert",
        str(certificates.certificate),
        "--key",
        str(certificates.key),
    ]
    with (output / "server.stderr.log").open("w", encoding="utf-8") as stderr:
        try:
            process = subprocess.Popen(
                arguments,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=stderr,
                text=True,
                start_new_session=True,
            )
        except OSError as error:
            raise RunnerError(
                f"could not start server {shlex.join(arguments)}: {error}"
            ) from error
    assert process.stdout is not None
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        if not selector.select(SERVER_TIMEOUT):
            stop(process)
            close_pipes(process)
            raise RunnerError(
                f"server did not print its ports within {SERVER_TIMEOUT}s; see {output / 'server.stderr.log'}"
            )
    finally:
        selector.close()
    line = process.stdout.readline().strip()
    if not line:
        process.wait(timeout=SERVER_TIMEOUT)
        close_pipes(process)
        raise RunnerError(
            f"server exited with {process.returncode} before printing its ports; see {output / 'server.stderr.log'}"
        )
    try:
        return process, parse_ports(line)
    except RunnerError:
        stop(process)
        close_pipes(process)
        raise


def close_pipes(process: subprocess.Popen[str]) -> None:
    for pipe in (process.stdin, process.stdout):
        if pipe is not None:
            pipe.close()


def shutdown_server(process: subprocess.Popen[str]) -> None:
    """Asks the server to exit by closing stdin and requires a clean exit."""
    assert process.stdin is not None
    process.stdin.close()
    try:
        returncode = process.wait(timeout=SERVER_TIMEOUT)
    except subprocess.TimeoutExpired as error:
        raise RunnerError(
            f"server did not exit after stdin closed within {SERVER_TIMEOUT}s"
        ) from error
    finally:
        close_pipes(process)
    if returncode != 0:
        raise RunnerError(f"server exited with {returncode}")


# --- emulator --------------------------------------------------------------------------------


def adb(*arguments: str, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    return run(["adb", "-s", SERIAL, *arguments], timeout=timeout)


def adb_probe(*arguments: str) -> str | None:
    return probe(["adb", "-s", SERIAL, *arguments])


class Emulator:
    """A headless emulator with a writable system partition."""

    def __init__(self, work_dir: Path, output: Path, system_image: str) -> None:
        avd_home = work_dir / "avd"
        avd_home.mkdir(exist_ok=True)
        name = f"trevrpc-interop-{os.getpid()}"
        env = {**os.environ, "ANDROID_AVD_HOME": str(avd_home)}
        run(
            ["avdmanager", "create", "avd", "--force", "--name", name]
            + ["--package", system_image, "--device", "pixel_2"],
            input_text="no\n",
            env=env,
        )
        self.log_path = output / "emulator.log"
        command = [
            "emulator", "-avd", name, "-port", str(EMULATOR_PORT),
            "-no-window", "-no-audio", "-no-boot-anim", "-no-snapshot", "-wipe-data",
            "-writable-system", "-partition-size", "4096", "-gpu", "swiftshader_indirect", "-no-metrics",
        ]  # fmt: skip
        with self.log_path.open("wb") as log:
            self.process = subprocess.Popen(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )

    def wait_until(self, condition, description: str, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._require_running()
            if condition():
                return
            time.sleep(1)
        self._require_running()
        raise RunnerError(f"emulator did not {description} within {timeout}s")

    def _require_running(self) -> None:
        if self.process.poll() is None:
            return
        lines = self.log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        raise RunnerError(
            f"emulator exited with {self.process.returncode}; last log lines:\n"
            + "\n".join(lines[-80:])
        )

    def wait_for_boot(self, previous_boot_id: str | None = None) -> None:
        """Waits for a completed boot; a changed boot ID proves a reboot happened."""

        def booted() -> bool:
            boot_id = adb_probe("shell", "cat", "/proc/sys/kernel/random/boot_id")
            return (
                adb_probe("get-state") == "device"
                and adb_probe("shell", "getprop", "sys.boot_completed") == "1"
                and bool(boot_id)
                and boot_id != previous_boot_id
            )

        self.wait_until(booted, "boot", BOOT_TIMEOUT)

    def reboot(self) -> None:
        previous = adb_probe("shell", "cat", "/proc/sys/kernel/random/boot_id")
        if not previous:
            raise RunnerError("could not read the emulator boot ID before rebooting")
        adb("reboot")
        self.wait_for_boot(previous)

    def root(self) -> None:
        adb("root")
        self.wait_until(
            lambda: adb_probe("shell", "id", "-u") == "0", "restart adbd as root", 60
        )

    def install_ca(self, ca: Path, work_dir: Path) -> None:
        """Adds the CA to the system trust store, which Cronet's verifier consults.

        The system partition is read-only until verity is disabled and the image is remounted,
        and the platform rereads the store only at boot, so this takes two reboots.
        """
        subject_hash = run(
            ["openssl", "x509", "-subject_hash_old", "-noout", "-in", str(ca)]
        ).stdout.split()[0]
        android_ca = work_dir / f"{subject_hash}.0"
        shutil.copyfile(ca, android_ca)
        destination = f"{CA_DIRECTORY}/{android_ca.name}"
        self.root()
        adb("disable-verity")
        self.reboot()
        self.root()
        adb("remount", timeout=90)
        adb("push", str(android_ca), destination)
        adb("shell", "chmod", "644", destination)
        adb("shell", "chown", "0:0", destination)
        adb("shell", "restorecon", destination)
        self.reboot()
        digest = adb("shell", "sha256sum", destination).stdout.split()
        expected = hashlib.sha256(android_ca.read_bytes()).hexdigest()
        if not digest or digest[0] != expected:
            raise RunnerError(
                f"installed CA digest is {digest[:1]}, expected {expected}"
            )

    def shutdown(self) -> None:
        if self.process.poll() is None:
            adb_probe("emu", "kill")
        stop(self.process, timeout=30)


# --- instrumentation -------------------------------------------------------------------------

_STATUS = re.compile(r"^INSTRUMENTATION_STATUS: ([^=]+)=(.*)$")
_CODE = re.compile(r"^INSTRUMENTATION_STATUS_CODE: (-?\d+)$")
_SCENARIO = re.compile(r"\[(.+)\]$")
_STARTED, _OK = (
    1,
    0,
)  # other codes are failures: -1 error, -2 failure, -3 ignored, -4 assumption


@dataclass
class Outcomes:
    """Scenario outcomes in execution order. A reason of None means the scenario passed."""

    scenarios: dict[str, str | None] = field(default_factory=dict)
    problems: list[str] = field(default_factory=list)
    started: set[str] = field(default_factory=set)

    @property
    def passed(self) -> bool:
        return (
            bool(self.scenarios)
            and not self.problems
            and all(reason is None for reason in self.scenarios.values())
        )

    def lines(self) -> list[str]:
        lines = [
            f"ok {name}" if reason is None else f"fail {name}: {reason}"
            for name, reason in self.scenarios.items()
        ]
        lines += [f"fail instrumentation: {problem}" for problem in self.problems]
        return lines


def parse_instrumentation(text: str) -> Outcomes:
    """Parses `am instrument -r` output. Each parameterized test case is one scenario."""
    outcomes = Outcomes()
    block: dict[str, str] = {}
    last_key = ""
    result_code: str | None = None
    for line in text.splitlines():
        status, code = _STATUS.match(line), _CODE.match(line)
        if status:
            last_key = status.group(1)
            block[last_key] = status.group(2)
        elif code:
            _finish_block(outcomes, block, int(code.group(1)))
            block, last_key = {}, ""
        elif line.startswith("INSTRUMENTATION_RESULT: shortMsg="):
            outcomes.problems.append(line.split("=", 1)[1])
        elif line.startswith("INSTRUMENTATION_CODE: "):
            result_code = line.split(": ", 1)[1]
        elif last_key and not line.startswith("INSTRUMENTATION_"):
            block[last_key] += "\n" + line
    for name in outcomes.started:
        outcomes.scenarios[name] = (
            "the instrumentation process ended before the scenario finished"
        )
    if result_code != "-1" and not outcomes.problems:
        outcomes.problems.append(
            f"instrumentation did not complete (INSTRUMENTATION_CODE: {result_code})"
        )
    if not outcomes.scenarios and not outcomes.problems:
        outcomes.problems.append("the instrumented test ran no scenarios")
    return outcomes


def _finish_block(outcomes: Outcomes, block: dict[str, str], code: int) -> None:
    test = block.get("test")
    if test is None:
        return
    match = _SCENARIO.search(test)
    name = match.group(1) if match else test
    if code == _STARTED:
        outcomes.started.add(name)
        return
    outcomes.started.discard(name)
    if code == _OK:
        outcomes.scenarios[name] = None
    else:
        stack = [
            line.strip() for line in block.get("stack", "").splitlines() if line.strip()
        ]
        outcomes.scenarios[name] = (
            stack[0] if stack else f"test failed with instrumentation status {code}"
        )


def run_instrumentation(port: int, output: Path) -> Outcomes:
    command = [
        "adb", "-s", SERIAL, "shell", "am", "instrument", "-w", "-r",
        "-e", "host", EMULATOR_HOST, "-e", "port", str(port),
        f"{TEST_PACKAGE}/{TEST_RUNNER}",
    ]  # fmt: skip
    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=INSTRUMENTATION_TIMEOUT,
            check=False,
        )
        text = result.stdout + result.stderr
    except subprocess.TimeoutExpired as error:
        text = (
            _decode(error.stdout)
            + _decode(error.stderr)
            + f"\nINSTRUMENTATION_RESULT: shortMsg=timed out after {INSTRUMENTATION_TIMEOUT}s\n"
        )
    (output / "instrumentation.log").write_text(text, encoding="utf-8")
    return parse_instrumentation(text)


def _decode(value: str | bytes | None) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")


def collect_logcat(output: Path) -> None:
    try:
        text = adb("logcat", "-d", "-v", "threadtime", timeout=30).stdout
    except RunnerError as error:
        text = f"logcat unavailable: {error}\n"
    (output / "logcat.log").write_text(text, encoding="utf-8")


# --- main ------------------------------------------------------------------------------------


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--server-command",
        required=True,
        help="command that starts an interop server; --host, --cert and --key are appended",
    )
    parser.add_argument(
        "--output", type=Path, required=True, help="directory for logs and diagnostics"
    )
    parser.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="scratch directory for the AVD (needs 6 GiB free)",
    )
    parser.add_argument("--app-apk", type=Path, required=True)
    parser.add_argument("--test-apk", type=Path, required=True)
    parser.add_argument(
        "--server-host", default="0.0.0.0", help="address the server binds to"
    )
    parser.add_argument("--system-image", default=DEFAULT_SYSTEM_IMAGE)
    return parser.parse_args(argv)


def execute(args: argparse.Namespace) -> Outcomes:
    for tool in ("adb", "avdmanager", "emulator", "openssl"):
        if shutil.which(tool) is None:
            raise RunnerError(f"required command not found: {tool}")
    for apk in (args.app_apk, args.test_apk):
        if not apk.is_file():
            raise RunnerError(f"APK does not exist: {apk}")
    if args.work_dir.resolve() == args.output.resolve():
        raise RunnerError("--work-dir must differ from --output")
    args.output.mkdir(parents=True, exist_ok=True)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(args.work_dir).free
    if free < MIN_FREE_BYTES:
        raise RunnerError(
            f"{args.work_dir} has {free} bytes free, at least {MIN_FREE_BYTES} are required"
        )

    certificates = generate_certificates(args.work_dir / "certificates")
    server: subprocess.Popen[str] | None = None
    emulator: Emulator | None = None
    try:
        server, ports = start_server(
            args.server_command, certificates, args.server_host, args.output
        )
        emulator = Emulator(args.work_dir, args.output, args.system_image)
        emulator.wait_for_boot()
        emulator.install_ca(certificates.ca, args.work_dir)
        for setting in (
            "window_animation_scale",
            "transition_animation_scale",
            "animator_duration_scale",
        ):
            adb("shell", "settings", "put", "global", setting, "0")
        adb("install", "-r", str(args.app_apk), timeout=90)
        adb("install", "-r", str(args.test_apk), timeout=90)
        adb("logcat", "-c")
        outcomes = run_instrumentation(ports["http3"], args.output)
        try:
            shutdown_server(server)
        except RunnerError as error:
            outcomes.problems.append(str(error))
        return outcomes
    finally:
        stop(server)
        if emulator is not None:
            if emulator.process.poll() is None:
                collect_logcat(args.output)
            emulator.shutdown()


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        outcomes = execute(args)
    except RunnerError as error:
        print(f"fail runner: {str(error).splitlines()[0]}")
        print(f"trevrpc-interop-android: {error}", file=sys.stderr)
        return 1
    print("\n".join(outcomes.lines()), flush=True)
    return 0 if outcomes.passed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
