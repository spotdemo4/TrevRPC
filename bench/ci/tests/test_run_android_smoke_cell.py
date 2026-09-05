#!/usr/bin/env python3

import hashlib
import io
import json
import os
import runpy
import shutil
import subprocess
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple, TextIO, cast, final
from unittest.mock import patch

SCRIPT = Path(
    os.environ.get(
        "ANDROID_SMOKE_RUNNER",
        Path(__file__).parents[1] / "run-android-smoke-cell.py",
    )
)
NAMESPACE = runpy.run_path(str(SCRIPT))

SmokeFailure = cast(type[RuntimeError], NAMESPACE["SmokeFailure"])
emulator_command = cast(Callable[[str], list[str]], NAMESPACE["emulator_command"])
prepare_work_dir = cast(Callable[[Path, Path], None], NAMESPACE["prepare_work_dir"])
create_avd = cast(
    Callable[[Path, Path, str], tuple[str, dict[str, str]]], NAMESPACE["create_avd"]
)
wait_for_boot = cast(Callable[..., None], NAMESPACE["wait_for_boot"])
wait_for_root = cast(Callable[..., None], NAMESPACE["wait_for_root"])
collect_logcat = cast(Callable[..., None], NAMESPACE["collect_logcat"])
read_event = cast(Callable[..., dict[str, object]], NAMESPACE["read_event"])
stop_process = cast(Callable[..., None], NAMESPACE["stop_process"])
run_checked = cast(
    Callable[..., subprocess.CompletedProcess[str]], NAMESPACE["run_checked"]
)
install_ca = cast(Callable[..., None], NAMESPACE["install_ca"])
recorded_adb = cast(
    Callable[..., subprocess.CompletedProcess[str]], NAMESPACE["recorded_adb"]
)
verify_server_capabilities = cast(
    Callable[[str, str], None], NAMESPACE["verify_server_capabilities"]
)
start_server = cast(
    Callable[..., tuple[subprocess.Popen[str], TextIO]], NAMESPACE["start_server"]
)
ANDROID_CA_DIRECTORY = cast(str, NAMESPACE["ANDROID_CA_DIRECTORY"])
ANDROID_CA_SETUP_LOG = cast(str, NAMESPACE["ANDROID_CA_SETUP_LOG"])


class DiskUsage(NamedTuple):
    total: int
    used: int
    free: int


@final
class FakeProcess:
    def __init__(self, return_code: int | None) -> None:
        self.return_code = return_code
        self.returncode = return_code
        self.pid = 1234
        self.stdout: TextIO | None = None

    def poll(self) -> int | None:
        return self.return_code

    def wait(self, timeout: int) -> int:
        del timeout
        if self.return_code is None:
            self.return_code = 0
            self.returncode = 0
        return self.return_code


def process_as_popen(process: FakeProcess) -> subprocess.Popen[bytes]:
    return cast(subprocess.Popen[bytes], cast(object, process))


def noop_wait_for_root(
    serial: str,
    process: subprocess.Popen[bytes],
    log_path: Path,
    timeout: int = 60,
) -> None:
    del serial, process, log_path, timeout


def noop_wait_for_boot(
    serial: str,
    process: subprocess.Popen[bytes],
    log_path: Path,
    *,
    previous_boot_id: str | None = None,
    timeout: int = 180,
) -> None:
    del serial, process, log_path, previous_boot_id, timeout


class AndroidSmokeRunnerTests(unittest.TestCase):
    def test_emulator_command_limits_userdata_partition(self) -> None:
        command = emulator_command("smoke")
        index = command.index("-partition-size")
        self.assertEqual(command[index + 1], "4096")

    def test_prepare_work_dir_requires_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"
            output.mkdir()
            usage = DiskUsage(10_000, 9_000, 1_000)
            with (
                patch.object(shutil, "disk_usage", return_value=usage),
                self.assertRaisesRegex(SmokeFailure, "requires at least"),
            ):
                prepare_work_dir(root / "work", output)
            self.assertTrue((output / "filesystem-space.json").is_file())

    def test_prepare_work_dir_preserves_existing_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()
            sentinel = work / ".write-probe"
            _ = sentinel.write_text("keep\n", encoding="utf-8")
            usage = DiskUsage(20_000_000_000, 1_000, 19_999_999_000)
            with patch.object(shutil, "disk_usage", return_value=usage):
                prepare_work_dir(work, output)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "keep\n")

    def test_create_avd_uses_separate_work_directory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()

            def fake_run_checked(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del command, input_text, timeout
                assert env is not None
                avd_home = Path(env["ANDROID_AVD_HOME"])
                avd_dir = avd_home / f"trevrpc-smoke-{os.getpid()}.avd"
                avd_dir.mkdir(parents=True)
                _ = (avd_dir / "config.ini").write_text(
                    "disk.dataPartition.size=10G\n", encoding="utf-8"
                )
                return subprocess.CompletedProcess([], 0, "", "")

            with patch.dict(create_avd.__globals__, {"run_checked": fake_run_checked}):
                _, env = create_avd(work, output, "system-image")

            self.assertEqual(Path(env["ANDROID_AVD_HOME"]), work / "avd")
            self.assertEqual(
                (output / "avd-config.ini").read_text(encoding="utf-8"),
                "disk.dataPartition.size=10G\n",
            )

    def test_emulator_exit_is_reported_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            log = Path(temporary) / "emulator.log"
            _ = log.write_text("fatal startup error\n", encoding="utf-8")
            process = process_as_popen(FakeProcess(1))
            with self.assertRaisesRegex(
                SmokeFailure, "(?s)emulator exited with 1.*fatal startup error"
            ):
                wait_for_boot("emulator-5554", process, log, timeout=180)

    def test_wait_for_root_polls_for_rooted_adbd(self) -> None:
        process = process_as_popen(FakeProcess(None))
        rooted = subprocess.CompletedProcess([], 0, "0\n", "")

        def rooted_probe(
            serial: str, *arguments: str
        ) -> subprocess.CompletedProcess[str]:
            self.assertEqual(serial, "emulator-5554")
            self.assertEqual(arguments, ("shell", "id", "-u"))
            return rooted

        with patch.dict(wait_for_root.__globals__, {"adb_probe": rooted_probe}):
            wait_for_root("emulator-5554", process, Path("emulator.log"), timeout=1)

    def test_wait_for_boot_accepts_completed_live_emulator(self) -> None:
        process = process_as_popen(FakeProcess(None))
        results: dict[tuple[str, ...], subprocess.CompletedProcess[str]] = {
            ("get-state",): subprocess.CompletedProcess([], 0, "device\n", ""),
            (
                "shell",
                "cat",
                "/proc/sys/kernel/random/boot_id",
            ): subprocess.CompletedProcess([], 0, "new-boot\n", ""),
            (
                "shell",
                "getprop",
                "sys.boot_completed",
            ): subprocess.CompletedProcess([], 0, "1\n", ""),
        }
        adb_calls: list[tuple[str, ...]] = []

        def fake_probe(
            serial: str, *arguments: str
        ) -> subprocess.CompletedProcess[str]:
            self.assertEqual(serial, "emulator-5554")
            return results[arguments]

        def fake_adb(
            serial: str, *arguments: str, timeout: int = 60
        ) -> subprocess.CompletedProcess[str]:
            del serial, timeout
            adb_calls.append(arguments)
            return subprocess.CompletedProcess([], 0, "", "")

        with patch.dict(
            wait_for_boot.__globals__,
            {"adb_probe": fake_probe, "adb": fake_adb},
        ):
            wait_for_boot(
                "emulator-5554",
                process,
                Path("emulator.log"),
                previous_boot_id="old-boot",
                timeout=1,
            )

        self.assertEqual(adb_calls, [("shell", "input", "keyevent", "82")])

    def test_verify_server_capabilities_requires_http3_server(self) -> None:
        def capabilities_result(
            command: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            del kwargs
            self.assertEqual(command, ["peer", "capabilities"])
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "schema_version": 5,
                        "event": "capabilities",
                        "peer": "c",
                        "roles": {
                            "client": ["trevrpc_native_quic"],
                            "server": ["trevrpc_http3"],
                        },
                    }
                ),
                "",
            )

        with patch.dict(
            verify_server_capabilities.__globals__, {"run_checked": capabilities_result}
        ):
            verify_server_capabilities("c", "peer")

    def test_verify_server_capabilities_rejects_webtransport_only(self) -> None:
        def capabilities_result(
            command: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            del kwargs
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps(
                    {
                        "schema_version": 5,
                        "event": "capabilities",
                        "peer": "c",
                        "roles": {"server": ["trevrpc_webtransport"]},
                    }
                ),
                "",
            )

        with (
            patch.dict(
                verify_server_capabilities.__globals__,
                {"run_checked": capabilities_result},
            ),
            self.assertRaisesRegex(SmokeFailure, "ordinary HTTP/3 support"),
        ):
            verify_server_capabilities("c", "peer")

    def test_start_server_selects_direct_http3_without_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            commands: list[list[str]] = []

            def fake_popen(
                command: list[str], **kwargs: object
            ) -> subprocess.Popen[str]:
                del kwargs
                commands.append(command)
                return cast(
                    subprocess.Popen[str],
                    cast(object, FakeProcess(None)),
                )

            with patch.object(subprocess, "Popen", side_effect=fake_popen):
                _, stdout_log = start_server(
                    "peer",
                    output / "server.pem",
                    output / "server-key.pem",
                    output,
                )
            stdout_log.close()

            self.assertEqual(len(commands), 1)
            command = commands[0]
            self.assertEqual(command[command.index("--stack") + 1], "trevrpc_http3")
            self.assertNotIn("--webtransport-origin", command)

    def test_recorded_adb_logs_nonzero_result_before_raising(self) -> None:
        log = io.StringIO()

        def failed_command(
            command: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            del kwargs
            return subprocess.CompletedProcess(
                command, 1, "probe stdout\n", "probe stderr\n"
            )

        with (
            patch.dict(recorded_adb.__globals__, {"execute_command": failed_command}),
            self.assertRaisesRegex(SmokeFailure, "probe stderr"),
        ):
            _ = recorded_adb("emulator-5554", log, "shell", "false")

        entry = cast(dict[str, object], cast(object, json.loads(log.getvalue())))
        self.assertEqual(entry["returncode"], 1)
        self.assertEqual(entry["stdout"], "probe stdout\n")
        self.assertEqual(entry["stderr"], "probe stderr\n")

    def test_recorded_adb_logs_timeout_before_raising(self) -> None:
        log = io.StringIO()

        def timed_out(
            command: list[str], **kwargs: object
        ) -> subprocess.CompletedProcess[str]:
            del kwargs
            raise subprocess.TimeoutExpired(
                command,
                7,
                output="partial stdout\n",
                stderr="partial stderr\n",
            )

        with (
            patch.dict(recorded_adb.__globals__, {"execute_command": timed_out}),
            self.assertRaisesRegex(SmokeFailure, "timed out after 7s"),
        ):
            _ = recorded_adb("emulator-5554", log, "shell", "sleep", "10", timeout=7)

        entry = cast(dict[str, object], cast(object, json.loads(log.getvalue())))
        self.assertEqual(entry["returncode"], None)
        self.assertEqual(entry["timeout"], 7)
        self.assertEqual(entry["error"], "timed_out")
        self.assertEqual(entry["stdout"], "partial stdout\n")
        self.assertEqual(entry["stderr"], "partial stderr\n")

    def test_install_ca_uses_complete_remount_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()
            ca = root / "ca.pem"
            ca_bytes = b"test CA\n"
            _ = ca.write_bytes(ca_bytes)
            _ = (output / ANDROID_CA_SETUP_LOG).write_text(
                '{"existing":true}\n', encoding="utf-8"
            )
            expected_digest = hashlib.sha256(ca_bytes).hexdigest()
            destination = f"{ANDROID_CA_DIRECTORY}/2b066fc1.0"
            process = process_as_popen(FakeProcess(None))
            events: list[tuple[object, ...]] = []
            boot_ids = ["before-verity-reboot", "before-activation-reboot"]

            def fake_run_checked(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del input_text, timeout, env
                self.assertEqual(
                    command,
                    [
                        "openssl",
                        "x509",
                        "-subject_hash_old",
                        "-noout",
                        "-in",
                        str(ca),
                    ],
                )
                return subprocess.CompletedProcess(command, 0, "2b066fc1\n", "")

            def fake_execute_command(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del input_text, env
                self.assertEqual(command[:3], ["adb", "-s", "emulator-5554"])
                arguments = tuple(command[3:])
                action = arguments[0]
                if (
                    len(arguments) == 2
                    and arguments[0] == "shell"
                    and ".trevrpc-write-probe-" in arguments[1]
                ):
                    action = "write-probe"
                    self.assertIn(ANDROID_CA_DIRECTORY, arguments[1])
                    self.assertIn("trap", arguments[1])
                elif arguments[:1] == ("push",):
                    action = "push"
                    self.assertEqual(
                        arguments[1:], (str(work / "2b066fc1.0"), destination)
                    )
                elif arguments[:2] == ("shell", "chmod"):
                    action = "chmod"
                    self.assertEqual(arguments[2:], ("644", destination))
                elif arguments[:2] == ("shell", "chown"):
                    action = "chown"
                    self.assertEqual(arguments[2:], ("0:0", destination))
                elif arguments[:2] == ("shell", "restorecon"):
                    action = "restorecon"
                    self.assertEqual(arguments[2:], (destination,))
                elif arguments[:2] == ("shell", "sha256sum"):
                    action = "sha256sum"
                    self.assertEqual(arguments[2:], (destination,))
                events.append((f"adb:{action}", timeout))
                stdout = f"{action} stdout\n"
                if action == "sha256sum":
                    stdout = f"{expected_digest}  {destination}\n"
                return subprocess.CompletedProcess(
                    command,
                    0,
                    stdout,
                    f"{action} stderr\n",
                )

            def fake_wait_for_root(
                serial: str,
                emulator_process: subprocess.Popen[bytes],
                emulator_log: Path,
                timeout: int = 60,
            ) -> None:
                del emulator_process, emulator_log
                self.assertEqual(serial, "emulator-5554")
                events.append(("wait-root", timeout))

            def fake_read_boot_id(serial: str) -> str:
                self.assertEqual(serial, "emulator-5554")
                boot_id = boot_ids.pop(0)
                events.append(("read-boot-id", boot_id))
                return boot_id

            def fake_wait_for_boot(
                serial: str,
                emulator_process: subprocess.Popen[bytes],
                emulator_log: Path,
                *,
                previous_boot_id: str | None = None,
                timeout: int = 180,
            ) -> None:
                del emulator_process, emulator_log, timeout
                self.assertEqual(serial, "emulator-5554")
                events.append(("wait-boot", previous_boot_id))

            with patch.dict(
                install_ca.__globals__,
                {
                    "run_checked": fake_run_checked,
                    "execute_command": fake_execute_command,
                    "wait_for_root": fake_wait_for_root,
                    "read_boot_id": fake_read_boot_id,
                    "wait_for_boot": fake_wait_for_boot,
                },
            ):
                install_ca(
                    "emulator-5554",
                    process,
                    root / "emulator.log",
                    ca,
                    work,
                    output,
                )

            self.assertEqual(
                events,
                [
                    ("adb:root", 60),
                    ("wait-root", 60),
                    ("adb:disable-verity", 60),
                    ("read-boot-id", "before-verity-reboot"),
                    ("adb:reboot", 60),
                    ("wait-boot", "before-verity-reboot"),
                    ("adb:root", 60),
                    ("wait-root", 60),
                    ("adb:remount", 90),
                    ("adb:write-probe", 60),
                    ("adb:push", 60),
                    ("adb:chmod", 60),
                    ("adb:chown", 60),
                    ("adb:restorecon", 60),
                    ("read-boot-id", "before-activation-reboot"),
                    ("adb:reboot", 60),
                    ("wait-boot", "before-activation-reboot"),
                    ("adb:sha256sum", 60),
                ],
            )
            lines = (
                (output / ANDROID_CA_SETUP_LOG).read_text(encoding="utf-8").splitlines()
            )
            self.assertEqual(lines[0], '{"existing":true}')
            self.assertEqual(len(lines), 13)
            first_entry = cast(dict[str, object], cast(object, json.loads(lines[1])))
            last_entry = cast(dict[str, object], cast(object, json.loads(lines[-1])))
            self.assertEqual(first_entry["stdout"], "root stdout\n")
            self.assertEqual(first_entry["stderr"], "root stderr\n")
            self.assertEqual(
                last_entry["command"],
                ["adb", "-s", "emulator-5554", "shell", "sha256sum", destination],
            )

    def test_install_ca_requires_boot_id_before_verity_reboot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()
            ca = root / "ca.pem"
            _ = ca.write_text("test CA\n", encoding="utf-8")
            commands: list[tuple[str, ...]] = []

            def fake_run_checked(
                command: list[str], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                del kwargs
                return subprocess.CompletedProcess(command, 0, "2b066fc1\n", "")

            def fake_execute_command(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del input_text, timeout, env
                arguments = tuple(command[3:])
                commands.append(arguments)
                return subprocess.CompletedProcess(command, 0, "", "")

            def missing_boot_id(serial: str) -> None:
                del serial

            with (
                patch.dict(
                    install_ca.__globals__,
                    {
                        "run_checked": fake_run_checked,
                        "execute_command": fake_execute_command,
                        "wait_for_root": noop_wait_for_root,
                        "read_boot_id": missing_boot_id,
                    },
                ),
                self.assertRaisesRegex(SmokeFailure, "before verity reboot"),
            ):
                install_ca(
                    "emulator-5554",
                    process_as_popen(FakeProcess(None)),
                    root / "emulator.log",
                    ca,
                    work,
                    output,
                )

            self.assertNotIn(("reboot",), commands)
            self.assertNotIn("push", [command[0] for command in commands])

    def test_install_ca_requires_boot_id_before_activation_reboot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()
            ca = root / "ca.pem"
            _ = ca.write_text("test CA\n", encoding="utf-8")
            commands: list[tuple[str, ...]] = []
            boot_ids: list[str | None] = ["before-verity-reboot", None]

            def fake_run_checked(
                command: list[str], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                del kwargs
                return subprocess.CompletedProcess(command, 0, "2b066fc1\n", "")

            def fake_execute_command(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del input_text, timeout, env
                arguments = tuple(command[3:])
                commands.append(arguments)
                return subprocess.CompletedProcess(command, 0, "", "")

            def fake_read_boot_id(serial: str) -> str | None:
                del serial
                return boot_ids.pop(0)

            with (
                patch.dict(
                    install_ca.__globals__,
                    {
                        "run_checked": fake_run_checked,
                        "execute_command": fake_execute_command,
                        "wait_for_root": noop_wait_for_root,
                        "read_boot_id": fake_read_boot_id,
                        "wait_for_boot": noop_wait_for_boot,
                    },
                ),
                self.assertRaisesRegex(SmokeFailure, "before CA activation reboot"),
            ):
                install_ca(
                    "emulator-5554",
                    process_as_popen(FakeProcess(None)),
                    root / "emulator.log",
                    ca,
                    work,
                    output,
                )

            self.assertEqual(commands.count(("reboot",)), 1)
            self.assertIn("push", [command[0] for command in commands])

    def test_install_ca_stops_when_write_probe_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            work = root / "work"
            output = root / "output"
            work.mkdir()
            output.mkdir()
            ca = root / "ca.pem"
            _ = ca.write_text("test CA\n", encoding="utf-8")
            commands: list[tuple[str, ...]] = []

            def fake_run_checked(
                command: list[str], **kwargs: object
            ) -> subprocess.CompletedProcess[str]:
                del kwargs
                return subprocess.CompletedProcess(command, 0, "2b066fc1\n", "")

            def fake_execute_command(
                command: list[str],
                *,
                input_text: str | None = None,
                timeout: int = 60,
                env: dict[str, str] | None = None,
            ) -> subprocess.CompletedProcess[str]:
                del input_text, timeout, env
                arguments = tuple(command[3:])
                commands.append(arguments)
                if (
                    len(arguments) == 2
                    and arguments[0] == "shell"
                    and ".trevrpc-write-probe-" in arguments[1]
                ):
                    return subprocess.CompletedProcess(
                        command,
                        1,
                        "probe stdout\n",
                        "Read-only file system\n",
                    )
                return subprocess.CompletedProcess(command, 0, "", "")

            def fixed_boot_id(serial: str) -> str:
                del serial
                return "before-verity-reboot"

            with (
                patch.dict(
                    install_ca.__globals__,
                    {
                        "run_checked": fake_run_checked,
                        "execute_command": fake_execute_command,
                        "wait_for_root": noop_wait_for_root,
                        "read_boot_id": fixed_boot_id,
                        "wait_for_boot": noop_wait_for_boot,
                    },
                ),
                self.assertRaisesRegex(SmokeFailure, "Read-only file system"),
            ):
                install_ca(
                    "emulator-5554",
                    process_as_popen(FakeProcess(None)),
                    root / "emulator.log",
                    ca,
                    work,
                    output,
                )

            self.assertNotIn("push", [command[0] for command in commands])
            lines = (
                (output / ANDROID_CA_SETUP_LOG).read_text(encoding="utf-8").splitlines()
            )
            failed = cast(dict[str, object], cast(object, json.loads(lines[-1])))
            self.assertEqual(failed["returncode"], 1)
            self.assertEqual(failed["stdout"], "probe stdout\n")
            self.assertEqual(failed["stderr"], "Read-only file system\n")
            self.assertIn(
                ".trevrpc-write-probe-", cast(list[str], failed["command"])[-1]
            )

    def test_collect_logcat_skips_offline_emulator(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            process = process_as_popen(FakeProcess(None))
            offline = subprocess.CompletedProcess([], 1, "offline\n", "")

            def offline_probe(
                serial: str, *arguments: str
            ) -> subprocess.CompletedProcess[str]:
                del serial, arguments
                return offline

            with patch.dict(collect_logcat.__globals__, {"adb_probe": offline_probe}):
                collect_logcat("emulator-5554", output, process)
            self.assertIn(
                "not an online emulator",
                (output / "logcat.log").read_text(encoding="utf-8"),
            )

    def test_read_event_drains_stdout_after_process_exit(self) -> None:
        read_fd, write_fd = os.pipe()
        with os.fdopen(write_fd, "w", encoding="utf-8") as writer:
            _ = writer.write('{"schema_version":5,"event":"stopped","peer":"c"}\n')
        stdout = os.fdopen(read_fd, "r", encoding="utf-8")
        process = FakeProcess(0)
        process.stdout = stdout
        log = io.StringIO()
        try:
            event = read_event(
                cast(subprocess.Popen[str], cast(object, process)),
                "stopped",
                log,
            )
        finally:
            stdout.close()
        self.assertEqual(event["peer"], "c")

    def test_cleanup_timeouts_do_not_escape(self) -> None:
        process = FakeProcess(None)

        def timeout_wait(timeout: int) -> int:
            raise subprocess.TimeoutExpired("process", timeout)

        process.wait = timeout_wait  # type: ignore[method-assign]
        with (
            patch.object(os, "killpg"),
            patch("sys.stderr", new=io.StringIO()),
        ):
            stop_process(process_as_popen(process), timeout=1)

    def test_run_checked_normalizes_timeouts(self) -> None:
        with (
            patch.object(
                subprocess,
                "run",
                side_effect=subprocess.TimeoutExpired(["command"], 3),
            ),
            self.assertRaisesRegex(SmokeFailure, "timed out after 3s"),
        ):
            _ = run_checked(["command"], timeout=3)


if __name__ == "__main__":
    _ = unittest.main()
