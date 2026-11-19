"""Unit tests for the interop runner's host-side logic. Run with `python3 -I -m unittest`."""

import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
import zipfile
from pathlib import Path
from typing import cast
from unittest import mock

import run

SUCCESS = textwrap.dedent(
    """\
    INSTRUMENTATION_STATUS: class=zip.trev.trevrpc.interop.android.EchoScenarioTest
    INSTRUMENTATION_STATUS: numtests=3
    INSTRUMENTATION_STATUS: stream=
    zip.trev.trevrpc.interop.android.EchoScenarioTest:
    INSTRUMENTATION_STATUS: test=scenario[unary]
    INSTRUMENTATION_STATUS_CODE: 1
    INSTRUMENTATION_STATUS: numtests=3
    INSTRUMENTATION_STATUS: test=scenario[unary]
    INSTRUMENTATION_STATUS_CODE: 0
    INSTRUMENTATION_STATUS: test=scenario[unary-error]
    INSTRUMENTATION_STATUS_CODE: 1
    INSTRUMENTATION_STATUS: stack=java.lang.AssertionError: unary-error: expected NOT_FOUND
    \tat zip.trev.Test.run(Test.kt:1)
    INSTRUMENTATION_STATUS: test=scenario[unary-error]
    INSTRUMENTATION_STATUS_CODE: -2
    INSTRUMENTATION_STATUS: test=scenario[bidi]
    INSTRUMENTATION_STATUS_CODE: 1
    INSTRUMENTATION_STATUS: test=scenario[bidi]
    INSTRUMENTATION_STATUS_CODE: 0
    INSTRUMENTATION_RESULT: stream=

    Time: 1.5
    INSTRUMENTATION_CODE: -1
    """
)


class ParseInstrumentationTest(unittest.TestCase):
    def test_reports_each_scenario_with_first_line_of_failure(self) -> None:
        outcomes = run.parse_instrumentation(SUCCESS)

        self.assertEqual(
            [
                "ok unary",
                "fail unary-error: java.lang.AssertionError: unary-error: expected NOT_FOUND",
                "ok bidi",
            ],
            outcomes.lines(),
        )
        self.assertFalse(outcomes.passed)

    def test_passes_only_when_every_scenario_passes(self) -> None:
        text = SUCCESS.replace("-2", "0")

        self.assertTrue(run.parse_instrumentation(text).passed)

    def test_crash_fails_unfinished_scenario_and_instrumentation(self) -> None:
        text = textwrap.dedent(
            """\
            INSTRUMENTATION_STATUS: test=scenario[unary-large]
            INSTRUMENTATION_STATUS_CODE: 1
            INSTRUMENTATION_RESULT: shortMsg=Process crashed.
            INSTRUMENTATION_CODE: 0
            """
        )

        outcomes = run.parse_instrumentation(text)

        self.assertFalse(outcomes.passed)
        self.assertEqual(
            [
                "fail unary-large: the instrumentation process ended before the scenario finished",
                "fail instrumentation: Process crashed.",
            ],
            outcomes.lines(),
        )

    def test_no_scenarios_is_a_failure(self) -> None:
        outcomes = run.parse_instrumentation("INSTRUMENTATION_CODE: -1\n")

        self.assertFalse(outcomes.passed)
        self.assertEqual(
            ["fail instrumentation: the instrumented test ran no scenarios"],
            outcomes.lines(),
        )

    def test_truncated_output_is_a_failure(self) -> None:
        outcomes = run.parse_instrumentation(
            "INSTRUMENTATION_STATUS: test=scenario[unary]\nINSTRUMENTATION_STATUS_CODE: 0\n"
        )

        self.assertFalse(outcomes.passed)


def fake_server(root: Path, ports: str) -> list[str]:
    """Writes a server that logs its arguments, reports `ports`, and runs until stdin closes."""
    server = root / "fake-server"
    script = ["#!/bin/sh", 'echo "$@" >&2', f"echo '{ports}'", "cat >/dev/null"]
    _ = server.write_text("\n".join(script) + "\n", encoding="utf-8")
    server.chmod(server.stat().st_mode | stat.S_IXUSR)
    return [str(server)]


class StartServerTest(unittest.TestCase):
    def test_appends_tls_and_host_flags_and_reads_ports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = fake_server(root, '{"native":1,"http3":4434,"webtransport":1,"http2":4436}')
            certificates = run.Certificates(
                root / "ca.pem", root / "server.pem", root / "server.key"
            )

            dictionary = root / "interop.dict"
            server = run.start_server(command, certificates, "0.0.0.0", dictionary, root)
            problem = server.stop()

            self.assertIsNone(problem)
            self.assertEqual(4434, server.port("http3"))
            self.assertEqual(4436, server.port("http2"))
            self.assertEqual(
                f"server --cert {certificates.certificate} --key {certificates.key} --host 0.0.0.0"
                + f" --dictionary {dictionary}",
                server.log.read_text(encoding="utf-8").strip(),
            )

    def test_requires_a_port_for_every_transport(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = fake_server(root, '{"native":1,"http3":4434}')
            certificates = run.Certificates(
                root / "ca.pem", root / "server.pem", root / "server.key"
            )

            with self.assertRaisesRegex(run.RunnerError, "'http2'"):
                _ = run.start_server(command, certificates, "0.0.0.0", root / "interop.dict", root)


class EmulatorSerialsTest(unittest.TestCase):
    def test_lists_emulators_in_any_state_and_skips_other_lines(self) -> None:
        devices = (
            "List of devices attached\n"
            "emulator-5556\tdevice\n"
            "192.168.1.2:5555\tdevice\n"
            "emulator-5558\toffline\n"
            "\n"
        )

        self.assertEqual(["emulator-5556", "emulator-5558"], run.emulator_serials(devices))


def write_executable(path: Path, lines: list[str]) -> None:
    _ = path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def fake_android_tools(root: Path, *, adb_server_fails: bool = False) -> Path:
    """Writes avdmanager, emulator and adb stand-ins to ROOT/bin and returns that directory.

    The adb server listens on ANDROID_ADB_SERVER_PORT, or exits at once with `adb_server_fails`.
    It knows two emulators: emulator-5554 belongs to another run, and emulator-5556 answers to
    the AVD name the fake emulator was started with.
    """
    tools = root / "bin"
    tools.mkdir()
    write_executable(tools / "avdmanager", ["#!/bin/sh", "cat >/dev/null"])
    write_executable(
        tools / "emulator",
        [
            f"#!{sys.executable}",
            "import json, sys, time",
            "from pathlib import Path",
            f"root = Path({str(root)!r})",
            "import os",
            "_ = (root / 'emulator-args.json').write_text(json.dumps(sys.argv[1:]))",
            "_ = (root / 'emulator-tmpdir').write_text(os.environ['TMPDIR'])",
            "_ = (root / 'avd-name').write_text(sys.argv[sys.argv.index('-avd') + 1])",
            "time.sleep(600)",
        ],
    )
    write_executable(
        tools / "adb",
        [
            f"#!{sys.executable}",
            "import json, os, socket, sys, time",
            "from pathlib import Path",
            f"root = Path({str(root)!r})",
            "arguments = sys.argv[1:]",
            "if arguments == ['nodaemon', 'server']:",
            "    if " + repr(adb_server_fails) + ":",
            "        sys.exit('could not install *smartsocket* listener: Address already in use')",
            "    environment = {name: os.environ.get(name) for name in",
            "                   ('ANDROID_ADB_SERVER_PORT', 'ADB_LOCAL_TRANSPORT_MAX_PORT')}",
            "    _ = (root / 'adb-server-env.json').write_text(json.dumps(environment))",
            "    listener = socket.create_server(",
            "        ('localhost', int(os.environ['ANDROID_ADB_SERVER_PORT'])))",
            "    time.sleep(600)",
            "elif arguments == ['devices']:",
            "    print('List of devices attached')",
            "    print('emulator-5554\\tdevice')",
            "    print('emulator-5556\\tdevice')",
            "elif arguments[:1] == ['-s']:",
            "    serial, command = arguments[1], arguments[2:]",
            "    if command == ['emu', 'avd', 'name']:",
            "        mine = serial == 'emulator-5556'",
            "        print((root / 'avd-name').read_text() if mine else 'other')",
            "        print('OK')",
            "    elif command == ['get-state']:",
            "        print('device')",
            "    elif command[:3] == ['shell', 'getprop', 'sys.boot_completed']:",
            "        print('1')",
            "    elif command[:2] == ['shell', 'cat']:",
            "        print('boot-1')",
        ],
    )
    return tools


class EmulatorTest(unittest.TestCase):
    def test_runs_its_own_adb_server_and_finds_its_emulator_by_avd_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tools = fake_android_tools(root)
            path = f"{tools}{os.pathsep}{os.environ.get('PATH', '')}"
            with mock.patch.dict(os.environ, {"PATH": path}):
                emulator = run.Emulator(root, root, run.DEFAULT_SYSTEM_IMAGE)
                try:
                    emulator.wait_for_boot()
                finally:
                    emulator.shutdown()

            self.assertEqual("emulator-5556", emulator.serial)
            server_env = cast(
                dict[str, str],
                json.loads((root / "adb-server-env.json").read_text(encoding="utf-8")),
            )
            self.assertEqual(
                {
                    "ANDROID_ADB_SERVER_PORT": emulator.env["ANDROID_ADB_SERVER_PORT"],
                    "ADB_LOCAL_TRANSPORT_MAX_PORT": run.ADB_SCAN_MAX_PORT,
                },
                server_env,
            )
            emulator_args = cast(
                list[str],
                json.loads((root / "emulator-args.json").read_text(encoding="utf-8")),
            )
            self.assertNotIn("-port", emulator_args)
            self.assertEqual(
                str(root / "tmp"), (root / "emulator-tmpdir").read_text(encoding="utf-8")
            )
            self.assertIsNotNone(emulator.adb_server.poll())
            self.assertIsNotNone(emulator.process.poll())

    def test_fails_without_starting_the_emulator_when_the_adb_server_exits(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            tools = fake_android_tools(root, adb_server_fails=True)
            path = f"{tools}{os.pathsep}{os.environ.get('PATH', '')}"
            with (
                mock.patch.dict(os.environ, {"PATH": path}),
                self.assertRaisesRegex(run.RunnerError, "Address already in use"),
            ):
                _ = run.Emulator(root, root, run.DEFAULT_SYSTEM_IMAGE)

            self.assertFalse((root / "avd-name").exists())


class ExtractDictionaryTest(unittest.TestCase):
    def test_writes_the_dictionary_of_the_vectors_in_the_test_apk(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            apk = root / "test.apk"
            with zipfile.ZipFile(apk, "w") as archive:
                archive.writestr(
                    run.DICTIONARY_VECTORS,
                    "# vectors\ndict.d1.id = 1\ndict.d1.dictionary = 37a430ec\n",
                )

            path = run.extract_dictionary(apk, root)

            self.assertEqual(bytes.fromhex("37a430ec"), path.read_bytes())

    def test_reports_an_apk_without_the_vectors(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            apk = root / "test.apk"
            with zipfile.ZipFile(apk, "w") as archive:
                archive.writestr("other.txt", "")

            with self.assertRaisesRegex(run.RunnerError, run.DICTIONARY_VECTORS):
                _ = run.extract_dictionary(apk, root)


class CertificatesTest(unittest.TestCase):
    def test_server_certificate_validates_for_the_emulator_host_and_localhost(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            certificates = run.generate_certificates(Path(temporary))

            for name, value in [
                ("-verify_ip", run.EMULATOR_HOST),
                ("-verify_ip", "127.0.0.1"),
                ("-verify_hostname", "localhost"),
            ]:
                result = subprocess.run(
                    [
                        "openssl",
                        "verify",
                        "-CAfile",
                        str(certificates.ca),
                        name,
                        value,
                        "-x509_strict",
                        str(certificates.certificate),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(
                    0,
                    result.returncode,
                    f"{name} {value}: {result.stdout}{result.stderr}",
                )

            text = subprocess.run(
                [
                    "openssl",
                    "x509",
                    "-noout",
                    "-text",
                    "-in",
                    str(certificates.certificate),
                ],
                capture_output=True,
                text=True,
                check=True,
            ).stdout
            self.assertIn("ecdsa-with-SHA256", text)
            self.assertIn("prime256v1", text)
            self.assertIn("CA:FALSE", text)
            self.assertTrue(
                json.dumps(certificates.key.read_text(encoding="utf-8")).startswith(
                    '"-----BEGIN PRIVATE KEY'
                )
            )


if __name__ == "__main__":
    _ = unittest.main()
