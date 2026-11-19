#!/usr/bin/env python3
"""Runs the TrevRPC interop scenarios from an Android emulator over Cronet HTTP/3 or HTTP/2.

The runner generates a certificate that is valid for the emulator's view of the host, starts an
interop server on the host (see interop/README.md), boots an emulator that trusts the certificate,
runs the instrumented test, and prints one `ok <name>` or `fail <name>: <reason>` line per
scenario. It exits 0 only if every scenario passed. With `--transport http2` Cronet's QUIC is
disabled, as when a network blocks UDP, and the scenarios run over the server's TCP listener.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import socket
import subprocess
import sys
import time
import zipfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

# The shared harness; appended so it never shadows modules next to this script.
sys.path.append(str(Path(__file__).resolve().parents[2] / "interop"))

from harness import (
    Certificate,
    RunnerError,
    Server,
    command_line,
    run_checked,
    stop_group,
    write_vectors_dictionary,
)

EMULATOR_HOST = "10.0.2.2"
# Each run talks to its emulator through its own adb server, and runs may share a host (CI runners
# with host networking run several jobs at once). adb normally scans console ports 5554-5585 and
# attaches to every emulator it finds, taking over other runs' emulators, so the run's server scans
# nothing below this port and knows only the emulator that registers with it. The emulator picks a
# free console port itself.
ADB_SCAN_MAX_PORT = "5553"
ADB_SERVER_START_TIMEOUT = 10
TEST_PACKAGE = "zip.trev.trevrpc.interop.android.test"
TEST_RUNNER = "androidx.test.runner.AndroidJUnitRunner"
DEFAULT_SYSTEM_IMAGE = "system-images;android-33;google_apis;x86_64"
CA_DIRECTORY = "/system/etc/security/cacerts"
BOOT_TIMEOUT = 180
NETWORK_TIMEOUT = 60
# `dumpsys connectivity` prints every network and request; on a loaded emulator it can take longer
# than a quick probe allows.
DUMPSYS_TIMEOUT = 15
INSTRUMENTATION_TIMEOUT = 180
MIN_FREE_BYTES = 6 * 1024**3
# The test APK carries the compression golden vectors, whose dictionary it compresses with.
DICTIONARY_VECTORS = "zstd-golden-vectors.txt"


# --- processes -------------------------------------------------------------------------------


def probe(command: list[str], env: dict[str, str] | None = None, timeout: float = 5) -> str | None:
    """Returns stdout of a quick command, or None if it failed or timed out."""
    try:
        result = subprocess.run(
            command, text=True, capture_output=True, timeout=timeout, env=env, check=False
        )
    except OSError, subprocess.TimeoutExpired:
        return None
    return result.stdout.strip() if result.returncode == 0 else None


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
    lines = [
        f"subjectAltName={alt_names}",
        "basicConstraints=critical,CA:FALSE",
        "keyUsage=critical,digitalSignature",
        "extendedKeyUsage=serverAuth",
    ]
    _ = extensions.write_text("\n".join(lines) + "\n", encoding="utf-8")
    curve = ["-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1", "-nodes"]
    _ = run_checked(
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
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
        ]
    )
    _ = run_checked(
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
    _ = run_checked(
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
            "-out",
            str(certificate),
            "-days",
            "10",
            "-extfile",
            str(extensions),
        ]
    )
    return Certificates(ca, certificate, key)


# --- interop server --------------------------------------------------------------------------


TRANSPORTS = ("http3", "http2")


def extract_dictionary(test_apk: Path, directory: Path) -> Path:
    """Writes the dictionary the instrumented test compresses with to DIRECTORY and returns its
    path, so the server holds the same one."""
    try:
        with zipfile.ZipFile(test_apk) as apk:
            vectors = apk.read(DICTIONARY_VECTORS).decode("utf-8")
    except (OSError, KeyError, UnicodeDecodeError, zipfile.BadZipFile) as error:
        raise RunnerError(
            f"could not read {DICTIONARY_VECTORS} from {test_apk}: {error}"
        ) from error
    return write_vectors_dictionary(vectors, f"{DICTIONARY_VECTORS} in {test_apk}", directory)


def start_server(
    command: list[str], certificates: Certificates, host: str, dictionary: Path, output: Path
) -> Server:
    """Starts the interop server on `host`, holding the compression `dictionary`, and waits for it
    to report its ports."""
    certificate = Certificate(certificates.certificate, certificates.key)
    arguments = ["--host", host, "--dictionary", str(dictionary)]
    server = Server("interop", command, certificate, output, arguments)
    try:
        for transport in TRANSPORTS:
            _ = server.port(transport)
    except RunnerError:
        _ = server.stop()
        raise
    return server


# --- emulator --------------------------------------------------------------------------------


def free_port() -> int:
    """Returns a TCP port on localhost that was free a moment ago."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return cast(int, listener.getsockname()[1])


_NETWORK_AGENT = re.compile(
    r"NetworkAgentInfo\{network\{(\d+)\}.*?\bni\{([A-Z_]+)(?:\[[^\]]*\])? ([A-Z_]+)"
)
_DEFAULT_NETWORK = re.compile(r"^\s*Active default network: (\d+)\s*$", re.MULTILINE)


def mobile_data_problem(dumpsys: str) -> str | None:
    """Returns why `dumpsys connectivity` does not show a connected mobile network as the default
    network with no Wi-Fi network other than a disconnected one, or None if it does."""
    networks = [match.groups() for match in _NETWORK_AGENT.finditer(dumpsys)]
    for network_id, kind, state in networks:
        if kind == "WIFI" and state != "DISCONNECTED":
            return f"Wi-Fi network {network_id} is {state}"
    default = _DEFAULT_NETWORK.search(dumpsys)
    if default is None:
        return "there is no default network"
    if (default.group(1), "MOBILE", "CONNECTED") not in networks:
        return f"default network {default.group(1)} is not a connected mobile network"
    return None


def emulator_serials(devices: str) -> list[str]:
    """Returns the emulator serials in `adb devices` output, in any state."""
    return [
        line.split("\t", 1)[0]
        for line in devices.splitlines()
        if line.startswith("emulator-") and "\t" in line
    ]


class WaitTimeout(RunnerError):
    """An emulator condition that did not hold before its deadline."""


class Emulator:
    """A headless emulator with a writable system partition, reached through its own adb server."""

    name: str
    env: dict[str, str]
    serial: str | None
    output: Path
    log_path: Path
    adb_server: subprocess.Popen[bytes]
    process: subprocess.Popen[bytes]

    def __init__(self, work_dir: Path, output: Path, system_image: str) -> None:
        avd_home = work_dir / "avd"
        avd_home.mkdir(exist_ok=True)
        # Emulators on a host share one netsim daemon, found through a file in the temporary
        # directory, and it goes down with the first emulator that stops, crashing the others. A
        # temporary directory of its own gives the run a daemon of its own.
        temporary = work_dir / "tmp"
        temporary.mkdir(exist_ok=True)
        self.name = f"trevrpc-interop-{os.getpid()}"
        self.serial = None
        self.output = output
        self.env = {
            **os.environ,
            "TMPDIR": str(temporary),
            "ANDROID_AVD_HOME": str(avd_home),
            "ANDROID_ADB_SERVER_PORT": str(free_port()),
            "ADB_LOCAL_TRANSPORT_MAX_PORT": ADB_SCAN_MAX_PORT,
        }
        _ = run_checked(
            [
                "avdmanager",
                "create",
                "avd",
                "--force",
                "--name",
                self.name,
                "--package",
                system_image,
                "--device",
                "pixel_2",
            ],
            input_text="no\n",
            env=self.env,
        )
        # The server runs in the foreground, so it fails rather than joins a server already on
        # the port, and it starts before the emulator, which registers with it on boot.
        self.adb_server = self._spawn(["adb", "nodaemon", "server"], output / "adb-server.log")
        try:
            self._wait_for_adb_server(output / "adb-server.log")
            self.log_path = output / "emulator.log"
            command = [
                "emulator", "-avd", self.name,
                "-no-window", "-no-audio", "-no-boot-anim", "-no-snapshot", "-wipe-data",
                "-writable-system", "-partition-size", "4096",
                "-gpu", "swiftshader_indirect", "-no-metrics",
            ]  # fmt: skip
            self.process = self._spawn(command, self.log_path)
        except BaseException:
            stop_group(self.adb_server)
            raise

    def _spawn(self, command: list[str], log_path: Path) -> subprocess.Popen[bytes]:
        with log_path.open("wb") as log:
            return subprocess.Popen(
                command,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=self.env,
                start_new_session=True,
            )

    def _wait_for_adb_server(self, log_path: Path) -> None:
        port = int(self.env["ANDROID_ADB_SERVER_PORT"])
        deadline = time.monotonic() + ADB_SERVER_START_TIMEOUT
        while time.monotonic() < deadline:
            if self.adb_server.poll() is not None:
                log = log_path.read_text(encoding="utf-8", errors="replace").strip()
                raise RunnerError(
                    f"adb server exited with {self.adb_server.returncode} on port {port}:\n{log}"
                )
            try:
                with socket.create_connection(("localhost", port), timeout=1):
                    return
            except OSError:
                time.sleep(0.1)
        raise RunnerError(
            f"adb server did not listen on port {port} within {ADB_SERVER_START_TIMEOUT}s"
        )

    def adb_command(self, *arguments: str) -> list[str]:
        if self.serial is None:
            raise RunnerError("the emulator has not registered with adb")
        return ["adb", "-s", self.serial, *arguments]

    def adb(self, *arguments: str, timeout: int = 60) -> str:
        """Runs an adb command against the emulator and returns its stdout."""
        return run_checked(self.adb_command(*arguments), timeout=timeout, env=self.env)

    def adb_probe(self, *arguments: str, timeout: float = 5) -> str | None:
        if self.serial is None:
            return None
        return probe(self.adb_command(*arguments), self.env, timeout)

    def _find_serial(self) -> str | None:
        """Returns the serial of this run's emulator once it has registered with the adb server.
        The AVD name tells it apart from any other emulator the server might know."""
        for serial in emulator_serials(probe(["adb", "devices"], self.env) or ""):
            name = probe(["adb", "-s", serial, "emu", "avd", "name"], self.env)
            if name and name.splitlines()[0].strip() == self.name:
                return serial
        return None

    def wait_until(self, condition: Callable[[], bool], description: str, timeout: int) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self._require_running()
            if condition():
                return
            time.sleep(1)
        self._require_running()
        raise WaitTimeout(f"emulator did not {description} within {timeout}s")

    def _require_running(self) -> None:
        if self.adb_server.poll() is not None:
            raise RunnerError(f"adb server exited with {self.adb_server.returncode}")
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
            if self.serial is None:
                self.serial = self._find_serial()
            boot_id = self.adb_probe("shell", "cat", "/proc/sys/kernel/random/boot_id")
            return (
                self.adb_probe("get-state") == "device"
                and self.adb_probe("shell", "getprop", "sys.boot_completed") == "1"
                and bool(boot_id)
                and boot_id != previous_boot_id
            )

        self.wait_until(booted, "boot", BOOT_TIMEOUT)

    def prepare(self, ca: Path, work_dir: Path) -> None:
        """Boots the emulator and readies it for the scenarios.

        Wi-Fi goes off before the reboots that install the CA, so every later boot comes up
        without it, and the scenarios start only once mobile data reaches the host.
        """
        self.wait_for_boot()
        self.use_mobile_data()
        self.install_ca(ca, work_dir)
        self.wait_for_mobile_data()
        for setting in (
            "window_animation_scale",
            "transition_animation_scale",
            "animator_duration_scale",
        ):
            _ = self.adb("shell", "settings", "put", "global", setting, "0")

    def _wifi_off(self) -> bool:
        """Whether Wi-Fi is off; if not, asks again to turn it off."""
        if self.adb_probe("shell", "settings", "get", "global", "wifi_on") == "0":
            return True
        _ = self.adb_probe("shell", "svc", "wifi", "disable")
        return False

    def use_mobile_data(self, timeout: int = NETWORK_TIMEOUT) -> None:
        """Turns Wi-Fi off, which lasts across reboots.

        The emulator has a mobile and a Wi-Fi network, which come up in either order after each
        boot. When the default network changes, Cronet fails the call in flight with
        ERR_NETWORK_CHANGED, and its channel can stay unable to reach the host afterwards, so the
        scenarios run on mobile data alone. Right after a boot the Wi-Fi service may not take the
        request yet, so it is repeated until the setting reads off.
        """
        self.wait_until(self._wifi_off, "turn Wi-Fi off", timeout)

    def wait_for_mobile_data(self, timeout: int = NETWORK_TIMEOUT) -> None:
        """Waits until mobile data is the default network, Wi-Fi is off, and the host answers.

        On a timeout the error names the condition last unmet, and the last `dumpsys
        connectivity` output is written to `connectivity.log` in the output directory.
        """
        problem = "nothing was checked"
        last_dumpsys = ""

        def ready() -> bool:
            nonlocal problem, last_dumpsys
            if not self._wifi_off():
                problem = "Wi-Fi is on"
                return False
            dumpsys = self.adb_probe("shell", "dumpsys", "connectivity", timeout=DUMPSYS_TIMEOUT)
            if dumpsys is None:
                problem = f"dumpsys connectivity failed or took over {DUMPSYS_TIMEOUT}s"
                return False
            last_dumpsys = dumpsys
            if (problem_now := mobile_data_problem(dumpsys)) is not None:
                problem = problem_now
                return False
            if self.adb_probe("shell", "ping", "-c", "1", "-W", "1", EMULATOR_HOST) is None:
                problem = f"{EMULATOR_HOST} did not answer a ping"
                return False
            return True

        try:
            self.wait_until(ready, f"reach {EMULATOR_HOST} over mobile data alone", timeout)
        except WaitTimeout as error:
            _ = (self.output / "connectivity.log").write_text(last_dumpsys, encoding="utf-8")
            raise WaitTimeout(f"{error}: {problem}") from error
        except RunnerError:
            _ = (self.output / "connectivity.log").write_text(last_dumpsys, encoding="utf-8")
            raise

    def reboot(self) -> None:
        previous = self.adb_probe("shell", "cat", "/proc/sys/kernel/random/boot_id")
        if not previous:
            raise RunnerError("could not read the emulator boot ID before rebooting")
        _ = self.adb("reboot")
        self.wait_for_boot(previous)

    def root(self) -> None:
        # Right after a boot adbd may not take the request yet, and `adb root` then fails (CI saw
        # it exit 1), so it is retried until adbd runs as root.
        def rooted() -> bool:
            if self.adb_probe("shell", "id", "-u") == "0":
                return True
            _ = self.adb_probe("root")
            return False

        self.wait_until(rooted, "restart adbd as root", 60)

    def install_ca(self, ca: Path, work_dir: Path) -> None:
        """Adds the CA to the system trust store, which Cronet's verifier consults.

        The system partition is read-only until verity is disabled and the image is remounted,
        and the platform rereads the store only at boot, so this takes two reboots.
        """
        subject_hash = run_checked(
            ["openssl", "x509", "-subject_hash_old", "-noout", "-in", str(ca)]
        ).split()[0]
        android_ca = work_dir / f"{subject_hash}.0"
        _ = shutil.copyfile(ca, android_ca)
        destination = f"{CA_DIRECTORY}/{android_ca.name}"
        self.root()
        _ = self.adb("disable-verity")
        self.reboot()
        self.root()
        _ = self.adb("remount", timeout=90)
        _ = self.adb("push", str(android_ca), destination)
        _ = self.adb("shell", "chmod", "644", destination)
        _ = self.adb("shell", "chown", "0:0", destination)
        _ = self.adb("shell", "restorecon", destination)
        self.reboot()
        digest = self.adb("shell", "sha256sum", destination).split()
        expected = hashlib.sha256(android_ca.read_bytes()).hexdigest()
        if not digest or digest[0] != expected:
            raise RunnerError(f"installed CA digest is {digest[:1]}, expected {expected}")

    def shutdown(self) -> None:
        if self.process.poll() is None:
            _ = self.adb_probe("emu", "kill")
        stop_group(self.process, grace=30)
        stop_group(self.adb_server)


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

    scenarios: dict[str, str | None] = field(default_factory=dict[str, str | None])
    problems: list[str] = field(default_factory=list[str])
    started: set[str] = field(default_factory=set[str])

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
        outcomes.scenarios[name] = "the instrumentation process ended before the scenario finished"
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
        stack = [line.strip() for line in block.get("stack", "").splitlines() if line.strip()]
        outcomes.scenarios[name] = (
            stack[0] if stack else f"test failed with instrumentation status {code}"
        )


def run_instrumentation(emulator: Emulator, port: int, transport: str, output: Path) -> Outcomes:
    command = emulator.adb_command(
        "shell", "am", "instrument", "-w", "-r",
        "-e", "host", EMULATOR_HOST, "-e", "port", str(port), "-e", "transport", transport,
        f"{TEST_PACKAGE}/{TEST_RUNNER}",
    )  # fmt: skip
    try:
        result = subprocess.run(
            command,
            text=True,
            capture_output=True,
            timeout=INSTRUMENTATION_TIMEOUT,
            env=emulator.env,
            check=False,
        )
        text = result.stdout + result.stderr
    except subprocess.TimeoutExpired as error:
        text = (
            _decode(error.stdout)
            + _decode(error.stderr)
            + f"\nINSTRUMENTATION_RESULT: shortMsg=timed out after {INSTRUMENTATION_TIMEOUT}s\n"
        )
    _ = (output / "instrumentation.log").write_text(text, encoding="utf-8")
    return parse_instrumentation(text)


def _decode(value: str | bytes | None) -> str:
    return value.decode(errors="replace") if isinstance(value, bytes) else (value or "")


def collect_logcat(emulator: Emulator, output: Path) -> None:
    try:
        text = emulator.adb("logcat", "-d", "-v", "threadtime", timeout=30)
    except RunnerError as error:
        text = f"logcat unavailable: {error}\n"
    _ = (output / "logcat.log").write_text(text, encoding="utf-8")


# --- main ------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Options:
    server_command: list[str]
    output: Path
    work_dir: Path
    app_apk: Path
    test_apk: Path
    server_host: str
    system_image: str
    transport: str


def parse_args(argv: list[str]) -> Options:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    _ = parser.add_argument(
        "--server-command",
        type=command_line,
        required=True,
        help="interop executable; `server --cert --key --host --dictionary` is appended",
    )
    _ = parser.add_argument(
        "--output", type=Path, required=True, help="directory for logs and diagnostics"
    )
    _ = parser.add_argument(
        "--work-dir",
        type=Path,
        required=True,
        help="scratch directory for the AVD (needs 6 GiB free)",
    )
    _ = parser.add_argument("--app-apk", type=Path, required=True)
    _ = parser.add_argument("--test-apk", type=Path, required=True)
    _ = parser.add_argument("--server-host", default="0.0.0.0", help="address the server binds to")
    _ = parser.add_argument("--system-image", default=DEFAULT_SYSTEM_IMAGE)
    _ = parser.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="http3",
        help="http3, or http2 with Cronet's QUIC disabled",
    )
    args = parser.parse_args(argv)
    return Options(
        server_command=cast(list[str], args.server_command),
        output=cast(Path, args.output),
        work_dir=cast(Path, args.work_dir),
        app_apk=cast(Path, args.app_apk),
        test_apk=cast(Path, args.test_apk),
        server_host=cast(str, args.server_host),
        system_image=cast(str, args.system_image),
        transport=cast(str, args.transport),
    )


def execute(args: Options) -> Outcomes:
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
    dictionary = extract_dictionary(args.test_apk, args.work_dir)
    server: Server | None = None
    emulator: Emulator | None = None
    try:
        server = start_server(
            args.server_command, certificates, args.server_host, dictionary, args.output
        )
        emulator = Emulator(args.work_dir, args.output, args.system_image)
        emulator.prepare(certificates.ca, args.work_dir)
        _ = emulator.adb("install", "-r", str(args.app_apk), timeout=90)
        _ = emulator.adb("install", "-r", str(args.test_apk), timeout=90)
        _ = emulator.adb("logcat", "-c")
        outcomes = run_instrumentation(
            emulator, server.port(args.transport), args.transport, args.output
        )
        if (problem := server.stop()) is not None:
            outcomes.problems.append(problem)
        return outcomes
    finally:
        if server is not None:
            _ = server.stop()
        if emulator is not None:
            if emulator.process.poll() is None:
                collect_logcat(emulator, args.output)
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
