#!/usr/bin/env python3

import argparse
import hashlib
import json
import os
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO, cast

SCHEMA_VERSION = 5
EMULATOR_HOST = "10.0.2.2"
EMULATOR_PORT = 5554
BOOT_TIMEOUT_SECONDS = 180
SERVER_TIMEOUT_SECONDS = 30
INSTRUMENTATION_TIMEOUT_SECONDS = 120
ADB_PROBE_TIMEOUT_SECONDS = 5
DATA_PARTITION_MIB = 4096
MIN_AVD_FREE_BYTES = 6 * 1024 * 1024 * 1024
EMULATOR_LOG_TAIL_LINES = 80
ANDROID_CA_DIRECTORY = "/system/etc/security/cacerts"
ANDROID_CA_SETUP_LOG = "android-ca-setup.jsonl"


class SmokeFailure(RuntimeError):
    pass


@dataclass(frozen=True)
class Arguments:
    server_id: str
    server_command: str
    app_apk: Path
    test_apk: Path
    output: Path
    work_dir: Path
    system_image: str


def parse_args() -> Arguments:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--server-id", required=True)
    _ = parser.add_argument("--server-command", required=True)
    _ = parser.add_argument("--app-apk", type=Path, required=True)
    _ = parser.add_argument("--test-apk", type=Path, required=True)
    _ = parser.add_argument("--output", type=Path, required=True)
    _ = parser.add_argument("--work-dir", type=Path, required=True)
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
        work_dir=require_path(values, "work_dir"),
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


def execute_command(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 60,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        timeout=timeout,
        env=env,
        check=False,
    )


def validate_command_result(
    command: list[str], result: subprocess.CompletedProcess[str]
) -> subprocess.CompletedProcess[str]:
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip()
        raise SmokeFailure(
            f"command failed with exit {result.returncode}: {' '.join(command)}\n{message}"
        )
    return result


def timeout_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode(errors="replace")
    return value


def execute_checked(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 60,
    env: dict[str, str] | None = None,
    log: TextIO | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = execute_command(
            command,
            input_text=input_text,
            timeout=timeout,
            env=env,
        )
    except subprocess.TimeoutExpired as error:
        if log is not None:
            write_command_record(
                log,
                command,
                returncode=None,
                stdout=timeout_output(error.stdout),
                stderr=timeout_output(error.stderr),
                timeout=timeout,
                error="timed_out",
            )
        raise SmokeFailure(
            f"command timed out after {timeout}s: {' '.join(command)}"
        ) from error
    except OSError as error:
        if log is not None:
            write_command_record(
                log,
                command,
                returncode=None,
                stdout="",
                stderr="",
                error=str(error),
            )
        raise SmokeFailure(
            f"could not run command: {' '.join(command)}\n{error}"
        ) from error
    if log is not None:
        write_command_record(
            log,
            command,
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
    return validate_command_result(command, result)


def run_checked(
    command: list[str],
    *,
    input_text: str | None = None,
    timeout: int = 60,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return execute_checked(
        command,
        input_text=input_text,
        timeout=timeout,
        env=env,
    )


def require_tools(names: list[str]) -> None:
    missing = [name for name in names if shutil.which(name) is None]
    if missing:
        raise SmokeFailure(f"required command(s) not found: {', '.join(missing)}")


def adb(
    serial: str, *arguments: str, timeout: int = 60
) -> subprocess.CompletedProcess[str]:
    return run_checked(["adb", "-s", serial, *arguments], timeout=timeout)


def write_command_record(
    log: TextIO,
    command: list[str],
    *,
    returncode: int | None,
    stdout: str,
    stderr: str,
    timeout: int | None = None,
    error: str | None = None,
) -> None:
    record: dict[str, object] = {
        "command": command,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": stderr,
    }
    if timeout is not None:
        record["timeout"] = timeout
    if error is not None:
        record["error"] = error
    _ = log.write(json.dumps(record, sort_keys=True) + "\n")
    log.flush()


def recorded_adb(
    serial: str,
    log: TextIO,
    *arguments: str,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    return execute_checked(
        ["adb", "-s", serial, *arguments],
        timeout=timeout,
        log=log,
    )


def adb_probe(serial: str, *arguments: str) -> subprocess.CompletedProcess[str] | None:
    try:
        return execute_command(
            ["adb", "-s", serial, *arguments],
            timeout=ADB_PROBE_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def emulator_log_tail(log_path: Path) -> str:
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as error:
        return f"unable to read emulator log: {error}"
    return "\n".join(lines[-EMULATOR_LOG_TAIL_LINES:])


def require_emulator_running(process: subprocess.Popen[bytes], log_path: Path) -> None:
    return_code = process.poll()
    if return_code is None:
        return
    tail = emulator_log_tail(log_path)
    message = (
        f"emulator exited with {return_code}; see {log_path}\n"
        + f"last emulator log lines:\n{tail}"
    )
    raise SmokeFailure(message)


def wait_for_root(
    serial: str,
    process: subprocess.Popen[bytes],
    log_path: Path,
    timeout: int = 60,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        require_emulator_running(process, log_path)
        result = adb_probe(serial, "shell", "id", "-u")
        if (
            result is not None
            and result.returncode == 0
            and result.stdout.strip() == "0"
        ):
            return
        time.sleep(1)
    require_emulator_running(process, log_path)
    raise SmokeFailure(
        f"emulator {serial} did not restart adbd as root within {timeout}s"
    )


def read_boot_id(serial: str) -> str | None:
    result = adb_probe(serial, "shell", "cat", "/proc/sys/kernel/random/boot_id")
    if result is None or result.returncode != 0:
        return None
    boot_id = result.stdout.strip()
    return boot_id or None


def wait_for_boot(
    serial: str,
    process: subprocess.Popen[bytes],
    log_path: Path,
    *,
    previous_boot_id: str | None = None,
    timeout: int = BOOT_TIMEOUT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        require_emulator_running(process, log_path)
        state = adb_probe(serial, "get-state")
        if (
            state is not None
            and state.returncode == 0
            and state.stdout.strip() == "device"
        ):
            boot_id = read_boot_id(serial)
            completed = adb_probe(serial, "shell", "getprop", "sys.boot_completed")
            if (
                completed is not None
                and completed.returncode == 0
                and completed.stdout.strip() == "1"
                and boot_id is not None
                and (previous_boot_id is None or boot_id != previous_boot_id)
            ):
                _ = adb(serial, "shell", "input", "keyevent", "82")
                return
        time.sleep(1)
    require_emulator_running(process, log_path)
    raise SmokeFailure(f"emulator {serial} did not finish booting within {timeout}s")


def record_disk_usage(path: Path) -> dict[str, int | str]:
    usage = shutil.disk_usage(path)
    return {
        "path": str(path),
        "total_bytes": usage.total,
        "used_bytes": usage.used,
        "free_bytes": usage.free,
    }


def prepare_work_dir(work_dir: Path, output: Path) -> None:
    if work_dir.resolve() == output.resolve():
        raise SmokeFailure("work directory must be separate from diagnostic output")
    work_dir.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.NamedTemporaryFile(dir=work_dir, prefix=".write-probe-") as probe:
            _ = probe.write(b"ok\n")
            probe.flush()
    except OSError as error:
        raise SmokeFailure(
            f"work directory is not writable: {work_dir}: {error}"
        ) from error

    work_usage = record_disk_usage(work_dir)
    output_usage = record_disk_usage(output)
    _ = (output / "filesystem-space.json").write_text(
        json.dumps({"work": work_usage, "output": output_usage}, indent=2) + "\n",
        encoding="utf-8",
    )
    free_bytes = work_usage["free_bytes"]
    assert isinstance(free_bytes, int)
    if free_bytes < MIN_AVD_FREE_BYTES:
        message = (
            f"work directory has {free_bytes} bytes free, "
            + f"but Android smoke requires at least {MIN_AVD_FREE_BYTES}: {work_dir}"
        )
        raise SmokeFailure(message)


def create_avd(
    work_dir: Path, output: Path, system_image: str
) -> tuple[str, dict[str, str]]:
    avd_home = work_dir / "avd"
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
    config_path = avd_home / f"{avd_name}.avd" / "config.ini"
    if config_path.is_file():
        _ = shutil.copyfile(config_path, output / "avd-config.ini")
    return avd_name, env


def emulator_command(avd_name: str) -> list[str]:
    return [
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
        "-partition-size",
        str(DATA_PARTITION_MIB),
        "-gpu",
        "swiftshader_indirect",
        "-no-metrics",
    ]


def start_emulator(
    avd_name: str,
    env: dict[str, str],
    log_path: Path,
    output: Path,
) -> tuple[subprocess.Popen[bytes], str]:
    command = emulator_command(avd_name)
    _ = (output / "emulator-command.json").write_text(
        json.dumps(command, indent=2) + "\n", encoding="utf-8"
    )
    log = log_path.open("wb")
    process = subprocess.Popen(
        command,
        stdout=log,
        stderr=subprocess.STDOUT,
        env=env,
        start_new_session=True,
    )
    log.close()
    return process, f"emulator-{EMULATOR_PORT}"


def install_ca(
    serial: str,
    process: subprocess.Popen[bytes],
    emulator_log: Path,
    ca_path: Path,
    work_dir: Path,
    output: Path,
) -> None:
    subject_hash = run_checked(
        ["openssl", "x509", "-subject_hash_old", "-noout", "-in", str(ca_path)]
    ).stdout.splitlines()[0]
    android_ca = work_dir / f"{subject_hash}.0"
    _ = shutil.copyfile(ca_path, android_ca)
    destination = f"{ANDROID_CA_DIRECTORY}/{android_ca.name}"
    probe_path = f"{ANDROID_CA_DIRECTORY}/.trevrpc-write-probe-{os.getpid()}"
    probe_command = (
        f"trap 'rm -f {probe_path}' 0 1 2 15; "
        + f"printf '%s\\n' trevrpc > {probe_path}"
    )
    expected_digest = hashlib.sha256(android_ca.read_bytes()).hexdigest()

    with (output / ANDROID_CA_SETUP_LOG).open("a", encoding="utf-8") as setup_log:

        def setup_adb(
            *arguments: str, timeout: int = 60
        ) -> subprocess.CompletedProcess[str]:
            return recorded_adb(
                serial,
                setup_log,
                *arguments,
                timeout=timeout,
            )

        _ = setup_adb("root")
        wait_for_root(serial, process, emulator_log)
        _ = setup_adb("disable-verity")
        verity_boot_id = read_boot_id(serial)
        if verity_boot_id is None:
            raise SmokeFailure("could not read emulator boot ID before verity reboot")
        _ = setup_adb("reboot")
        wait_for_boot(
            serial,
            process,
            emulator_log,
            previous_boot_id=verity_boot_id,
        )

        _ = setup_adb("root")
        wait_for_root(serial, process, emulator_log)
        _ = setup_adb("remount", timeout=90)
        _ = setup_adb("shell", probe_command)
        _ = setup_adb("push", str(android_ca), destination)
        _ = setup_adb("shell", "chmod", "644", destination)
        _ = setup_adb("shell", "chown", "0:0", destination)
        _ = setup_adb("shell", "restorecon", destination)

        activation_boot_id = read_boot_id(serial)
        if activation_boot_id is None:
            raise SmokeFailure(
                "could not read emulator boot ID before CA activation reboot"
            )
        _ = setup_adb("reboot")
        wait_for_boot(
            serial,
            process,
            emulator_log,
            previous_boot_id=activation_boot_id,
        )
        digest_result = setup_adb("shell", "sha256sum", destination)

    digest_parts = digest_result.stdout.split()
    actual_digest = digest_parts[0] if digest_parts else ""
    if actual_digest != expected_digest:
        raise SmokeFailure(
            f"installed Android CA digest is {actual_digest!r}, expected {expected_digest!r}"
        )


def parse_server_event(line: str, expected: str, log: TextIO) -> dict[str, object]:
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
                line = stdout.readline()
                if line != "":
                    return parse_server_event(line, expected, log)
                raise SmokeFailure(
                    f"server exited with {process.returncode} before {expected!r} event"
                )
            events = selector.select(min(1, deadline - time.monotonic()))
            if not events:
                continue
            line = stdout.readline()
            if line != "":
                return parse_server_event(line, expected, log)
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
    if "trevrpc_http3" not in advertised_roles:
        raise SmokeFailure(
            f"server does not advertise ordinary HTTP/3 support: {capabilities!r}"
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
            "trevrpc_http3",
            "--listen",
            "0.0.0.0:0",
            "--cert",
            str(certificate),
            "--key",
            str(private_key),
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
        return
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"warning: graceful process cleanup failed: {error}", file=sys.stderr)
    try:
        os.killpg(process.pid, signal.SIGKILL)
        _ = process.wait(timeout=timeout)
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"warning: forced process cleanup failed: {error}", file=sys.stderr)


def device_is_online(serial: str) -> bool:
    result = adb_probe(serial, "get-state")
    return (
        result is not None
        and result.returncode == 0
        and result.stdout.strip() == "device"
    )


def collect_logcat(
    serial: str,
    output: Path,
    process: subprocess.Popen[bytes] | None,
) -> None:
    log_path = output / "logcat.log"
    if process is None or process.poll() is not None or not device_is_online(serial):
        _ = log_path.write_text(
            f"logcat unavailable: {serial} is not an online emulator\n",
            encoding="utf-8",
        )
        return
    try:
        result = subprocess.run(
            ["adb", "-s", serial, "logcat", "-d", "-v", "threadtime"],
            text=True,
            capture_output=True,
            timeout=30,
            check=False,
        )
        contents = result.stdout + result.stderr
    except (OSError, subprocess.TimeoutExpired) as error:
        contents = f"logcat collection failed: {error}\n"
    _ = log_path.write_text(contents, encoding="utf-8")


def stop_emulator_with_adb(
    serial: str, process: subprocess.Popen[bytes] | None
) -> None:
    if process is None or process.poll() is not None or not device_is_online(serial):
        return
    try:
        _ = subprocess.run(
            ["adb", "-s", serial, "emu", "kill"],
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        print(f"warning: adb emulator cleanup failed: {error}", file=sys.stderr)


def main() -> int:
    arguments = parse_args()
    if arguments.output.exists():
        raise SmokeFailure(f"output path already exists: {arguments.output}")
    arguments.output.mkdir(parents=True)
    prepare_work_dir(arguments.work_dir, arguments.output)
    certificate_dir = arguments.work_dir / "certificates"
    certificate_dir.mkdir()
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
                str(certificate_dir),
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

        avd_name, emulator_env = create_avd(
            arguments.work_dir, arguments.output, arguments.system_image
        )
        emulator_log = arguments.output / "emulator.log"
        emulator_process, serial = start_emulator(
            avd_name,
            emulator_env,
            emulator_log,
            arguments.output,
        )
        wait_for_boot(serial, emulator_process, emulator_log)
        install_ca(
            serial,
            emulator_process,
            emulator_log,
            ca,
            arguments.work_dir,
            arguments.output,
        )
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
        collect_logcat(serial, arguments.output, emulator_process)
        return 0
    except Exception:
        if shutil.which("adb") is not None:
            try:
                collect_logcat(serial, arguments.output, emulator_process)
            except OSError as error:
                print(f"warning: failed to collect logcat: {error}", file=sys.stderr)
        raise
    finally:
        if server_stdout is not None:
            server_stdout.close()
        stop_process(server_process)
        if shutil.which("adb") is not None:
            stop_emulator_with_adb(serial, emulator_process)
        stop_process(emulator_process, timeout=30)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SmokeFailure as error:
        print(f"android smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
