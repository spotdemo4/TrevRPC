#!/usr/bin/env python3

import io
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
            _ = writer.write('{"schema_version":4,"event":"stopped","peer":"c"}\n')
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
