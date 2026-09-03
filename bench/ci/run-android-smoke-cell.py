#!/usr/bin/env python3

import argparse
import json
import os
import selectors
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO, cast

SCHEMA_VERSION = 4
EMULATOR_HOST = "10.0.2.2"
EMULATOR_PORT = 5554
BOOT_TIMEOUT_SECONDS = 180
SERVER_TIMEOUT_SECONDS = 30
INSTRUMENTATION_TIMEOUT_SECONDS = 120


class SmokeFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class Arguments:
    server_id: str
    server_command: str
    app_apk: Path
    test_apk: Path
    output: Path
    system_image: str


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--server-id", required=True)
    _ = parser.add_argument("--server-command", required=True)
    _ = parser.add_argument("--app-apk", type=Path, required=True)
    _ = parser.add_argument("--test-apk", type=Path, required=True)
    _ = parser.add_argument("--output", type=Path, required=True)
    _ = parser.add_argument(
        "--system-image",
        default="system-images;android-33;google_apis;x86_64",
    )
    values = cast(dict[str, object], vars(parser.parse_args()))
    return Arguments(
        server_id=require_string(values, "server_id"),
        server_command=require_string(values, "server_command"),
        app_apk=require_path(values, "app_apk"),
        test_apk=require_path(values, "test_apk"),
        output=require_path(values, "output"),
        system_image=require_string(values, "system_image"),
    )


def require_string(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if not isinstance(value, str):
        raise SmokeFailure(f"{key} must be a string")
    return value


def require_path(values: dict[str, object], key: str) -> Path:
    value = values.get(key)
    if not isinstance(value, Path):
        raise SmokeFailure(f"{key} must be a path")
    return value


def parse_json_object(text: str, description: str) -> dict[str, object]:
    try:
        value = cast(object, json.loads(text))
    except json.JSONDecodeError as error:
        raise SmokeFailure(f"{description} is not JSON") from error
    if not isinstance(value, dict):
        raise SmokeFailure(f"{description} is not a JSON object")
    mapping = cast(dict[object, object], value)
    if not all(isinstance(key, str) for key in mapping):
        raise SmokeFailure(f"{description} has a non-string key")
    return cast(dict[str, object], mapping)


def run_checked(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 60,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise SmokeFailure(
            f"command failed with exit {result.returncode}: {' '.join(command)}\n{message}"
        )
    return result


def require_tools(names: list[str]) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise SmokeFailure(f"required command(s) not found: {', '.join(missing)}")


def adb(
    serial: str, *arguments: str, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    return run_checked(["adb", "-s", serial, *arguments], timeout=timeout)


def wait_for_boot(serial: str, timeout: int = BOOT_TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    _ = run_checked(["adb", "-s", serial, "wait-for-device"], timeout=timeout)
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["adb", "-s", serial, "shell", "getprop", "sys.boot_completed"],
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        if result.returncode == 0 and result.stdout.strip() == "1":
            _ = adb(serial, "shell", "input", "keyevent", "82")
            return
        time.sleep(1)
    raise SmokeFailure(f"emulator {serial} did not finish booting within {timeout}s")


def create_avd(output: Path, system_image: str) -> tuple[str, dict[str, str]]:
    avd_home = output / "avd"
    avd_home.mkdir()
    avd_name = f"trevrpc-smoke-{os.getpid()}"
    env = os.environ.copy()
    env["ANDROID_AVD_HOME"] = str(avd_home)
    _ = run_checked(
        [
            "avdmanager",
            "create",
            "avd",
            "--force",
            "--name",
            avd_name,
            "--package",
            system_image,
            "--device",
            "pixel_2",
        ],
        input_text="no\n",
        timeout=60,
        env=env,
    )
    return avd_name, env


def start_emulator(
    avd_name: str,
    env: dict[str, str],
    log_path: Path,
) -> tuple[subprocess.Popen[bytes], str]:
    log = log_path.open("wb")
    process = subprocess.Popen(
        [
            "emulator",
            "-avd",
            avd_name,
            "-port",
            str(EMULATOR_PORT),
            "-no-window",
            "-no-audio",
            "-no-boot-anim",
            "-no-snapshot",
            "-wipe-data",
            "-writable-system",
            "-gpu",
            "swiftshader_indirect",
            "-no-metrics",
        ],
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )
    log.close()
    return process, f"emulator-{EMULATOR_PORT}"


def install_ca(serial: str, ca_path: Path, output: Path) -> None:
    subject_hash = run_checked(
        ["openssl", "x509", "-subject_hash_old", "-noout", "-in", str(ca_path)]
    ).stdout.splitlines()[0]
    android_ca = output / f"{subject_hash}.0"
    _ = shutil.copyfile(ca_path, android_ca)

    _ = adb(serial, "root")
    _ = run_checked(["adb", "-s", serial, "wait-for-device"])
    _ = adb(serial, "remount", timeout=90)
    _ = adb(
        serial,
        "push",
        str(android_ca),
        f"/system/etc/security/cacerts/{android_ca.name}",
    )
    _ = adb(
        serial,
        "shell",
        "chmod",
        "644",
        f"/system/etc/security/cacerts/{android_ca.name}",
    )
    _ = adb(serial, "reboot")
    wait_for_boot(serial)
    _ = adb(
        serial, "shell", "test", "-f", f"/system/etc/security/cacerts/{android_ca.name}"
    )


def read_event(
    process: subprocess.Popen[str],
    expected: str,
    log: TextIO,
    timeout: int = SERVER_TIMEOUT_SECONDS,
) -> dict[str, object]:
    stdout = cast(TextIO, process.stdout)
    selector = selectors.DefaultSelector()
    _ = selector.register(stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise SmokeFailure(
                    f"server exited with {process.returncode} before {expected!r} event"
                )
            events = selector.select(min(1, deadline - time.monotonic()))
            if not events:
                continue
            line = stdout.readline()
            if line == "":
                continue
            _ = log.write(line)
            log.flush()
            event = parse_json_object(line, "server event")
            if event.get("schema_version") != SCHEMA_VERSION:
                raise SmokeFailure(f"server emitted wrong schema version: {event!r}")
            if event.get("event") == "error":
                raise SmokeFailure(f"server reported an error: {event!r}")
            if event.get("event") != expected:
                raise SmokeFailure(
                    f"server emitted {event.get('event')!r}, expected {expected!r}"
                )
            return event
    finally:
        selector.close()
    raise SmokeFailure(f"server did not emit {expected!r} within {timeout}s")


def verify_server_capabilities(server_id: str, server_command: str) -> None:
    result = run_checked([server_command, "capabilities"])
    capabilities = parse_json_object(result.stdout, "server capabilities output")
    if capabilities.get("schema_version") != SCHEMA_VERSION:
        raise SmokeFailure(f"unexpected server capability schema: {capabilities!r}")
    if capabilities.get("peer") != server_id:
        raise SmokeFailure(
            f"server peer is {capabilities.get('peer')!r}, expected {server_id!r}"
        )
    roles_value = capabilities.get("roles")
    if not isinstance(roles_value, dict):
        raise SmokeFailure(f"server capabilities have invalid roles: {capabilities!r}")
    roles = cast(dict[object, object], roles_value)
    server_roles = roles.get("server")
    if not isinstance(server_roles, list):
        raise SmokeFailure(f"server capabilities have invalid roles: {capabilities!r}")
    advertised_roles = cast(list[object], server_roles)
    if "trevrpc_webtransport" not in advertised_roles:
        raise SmokeFailure(
            f"server does not advertise HTTP/3 listener support: {capabilities!r}"
        )


def start_server(
    server_command: str,
    certificate: Path,
    private_key: Path,
    output: Path,
) -> tuple[subprocess.Popen[str], TextIO]:
    stderr = (output / "server.stderr.log").open("w", encoding="utf-8")
    stdout_log = (output / "server.stdout.jsonl").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [
            server_command,
            "server",
            "--stack",
            "trevrpc_webtransport",
            "--listen",
            "0.0.0.0:0",
            "--cert",
            str(certificate),
            "--key",
            str(private_key),
            "--webtransport-origin",
            "https://android-smoke.invalid",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr,
        text=True,
        start_new_session=True,
        env={**os.environ, "TREVRPC_BENCH_SERVER_WORKERS": "8"},
    )
    stderr.close()
    return process, stdout_log


def parse_port(address: object) -> int:
    if not isinstance(address, str) or ":" not in address:
        raise SmokeFailure(f"server ready event has invalid address: {address!r}")
    try:
        port = int(address.rsplit(":", 1)[1])
    except ValueError as error:
        raise SmokeFailure(
            f"server ready event has invalid address: {address!r}"
        ) from error
    if not 1 <= port <= 65535:
        raise SmokeFailure(f"server ready event has invalid port: {port}")
    return port


def run_instrumentation(
    serial: str,
    app_apk: Path,
    test_apk: Path,
    server_id: str,
    port: int,
    output: Path,
) -> None:
    _ = adb(serial, "install", "-r", str(app_apk), timeout=90)
    _ = adb(serial, "install", "-r", str(test_apk), timeout=90)
    _ = adb(serial, "logcat", "-c")
    command = [
        "adb",
        "-s",
        serial,
        "shell",
        "am",
        "instrument",
        "-w",
        "-r",
        "-e",
        "origin",
        f"https://{EMULATOR_HOST}:{port}",
        "-e",
        "serverId",
        server_id,
        "-e",
        "class",
        "zip.trev.trevrpc.bench.cronet.CronetInteropSmokeTest",
        "zip.trev.trevrpc.bench.cronet.test/androidx.test.runner.AndroidJUnitRunner",
    ]
    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        timeout=INSTRUMENTATION_TIMEOUT_SECONDS,
        check=False,
    )
    instrumentation_output = result.stdout + result.stderr
    _ = (output / "instrumentation.log").write_text(
        instrumentation_output, encoding="utf-8"
    )
    if (
        result.returncode != 0
        or "OK (1 test)" not in instrumentation_output
        or "INSTRUMENTATION_CODE: -1" not in instrumentation_output
    ):
        raise SmokeFailure(
            f"Android instrumentation failed with exit {result.returncode}; see {output / 'instrumentation.log'}"
        )


def stop_process(
    process: subprocess.Popen[bytes] | subprocess.Popen[str] | None,
    timeout: int = 10,
) -> None:
    if process is None or process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGTERM)
        _ = process.wait(timeout=timeout)
    except (ProcessLookupError, subprocess.TimeoutExpired):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        _ = process.wait(timeout=timeout)


def collect_logcat(serial: str, output: Path) -> None:
    result = subprocess.run(
        ["adb", "-s", serial, "logcat", "-d", "-v", "threadtime"],
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    _ = (output / "logcat.log").write_text(
        result.stdout + result.stderr, encoding="utf-8"
    )


def main() -> int:
    arguments = parse_args()
    if arguments.output.exists():
        raise SmokeFailure(f"output path already exists: {arguments.output}")
    arguments.output.mkdir(parents=True)
    for apk in [arguments.app_apk, arguments.test_apk]:
        if not apk.is_file():
            raise SmokeFailure(f"APK does not exist: {apk}")
    require_tools(
        [
            "adb",
            "avdmanager",
            "emulator",
            "openssl",
            "trevrpc-bench",
            arguments.server_command,
        ]
    )
    verify_server_capabilities(arguments.server_id, arguments.server_command)

    emulator_process: subprocess.Popen[bytes] | None = None
    server_process: subprocess.Popen[str] | None = None
    server_stdout: TextIO | None = None
    serial = f"emulator-{EMULATOR_PORT}"
    try:
        certificate_result = run_checked(
            [
                "trevrpc-bench",
                "certificates",
                "--out",
                str(arguments.output),
                "--server-ip",
                "127.0.0.1",
                "--server-ip",
                EMULATOR_HOST,
            ]
        )
        certificates = parse_json_object(
            certificate_result.stdout, "certificate command output"
        )
        ca = Path(require_string(certificates, "ca"))
        certificate = Path(require_string(certificates, "certificate"))
        private_key = Path(require_string(certificates, "private_key"))

        avd_name, emulator_env = create_avd(arguments.output, arguments.system_image)
        emulator_process, serial = start_emulator(
            avd_name,
            emulator_env,
            arguments.output / "emulator.log",
        )
        wait_for_boot(serial)
        install_ca(serial, ca, arguments.output)
        for setting in [
            "window_animation_scale",
            "transition_animation_scale",
            "animator_duration_scale",
        ]:
            _ = adb(serial, "shell", "settings", "put", "global", setting, "0")

        server_process, server_stdout = start_server(
            arguments.server_command,
            certificate,
            private_key,
            arguments.output,
        )
        ready = read_event(server_process, "ready", server_stdout)
        if ready.get("peer") != arguments.server_id:
            raise SmokeFailure(f"server ready event has wrong peer: {ready!r}")
        port = parse_port(ready.get("address"))
        run_instrumentation(
            serial,
            arguments.app_apk,
            arguments.test_apk,
            arguments.server_id,
            port,
            arguments.output,
        )

        assert server_process.stdin is not None
        _ = server_process.stdin.write("SHUTDOWN\n")
        server_process.stdin.flush()
        stopped = read_event(server_process, "stopped", server_stdout)
        if stopped.get("peer") != arguments.server_id:
            raise SmokeFailure(f"server stopped event has wrong peer: {stopped!r}")
        server_process.stdin.close()
        if server_process.wait(timeout=SERVER_TIMEOUT_SECONDS) != 0:
            raise SmokeFailure(f"server exited with {server_process.returncode}")
        server_stdout.close()
        server_stdout = None
        collect_logcat(serial, arguments.output)
        return 0
    except Exception:
        if shutil.which("adb") is not None:
            try:
                collect_logcat(serial, arguments.output)
            except (OSError, subprocess.SubprocessError) as error:
                print(f"warning: failed to collect logcat: {error}", file=sys.stderr)
        raise
    finally:
        if server_stdout is not None:
            server_stdout.close()
        stop_process(server_process)
        if shutil.which("adb") is not None:
            _ = subprocess.run(
                ["adb", "-s", serial, "emu", "kill"],
                capture_output=True,
                timeout=10,
                check=False,
            )
        stop_process(emulator_process, timeout=30)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeFailure as error:
        print(f"android smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
