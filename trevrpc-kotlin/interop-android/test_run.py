"""Unit tests for the interop runner's host-side logic. Run with `python3 -I -m unittest`."""

import contextlib
import json
import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
import zipfile
from collections.abc import Generator
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


def connectivity(default: str, *networks: str) -> str:
    """`dumpsys connectivity` output with the given default network and network agents, each
    given as `ID TYPE STATE`, in the format Android 13 prints."""
    agents = [
        f"  NetworkAgentInfo{{network{{{network_id}}}  handle{{432902426637}}  ni{{{kind} {state}"
        + " extra: } created=2026-11-19T01:03:26.855Z Score(Policies : TRANSPORT_PRIMARY)"
        for network_id, kind, state in (network.split() for network in networks)
    ]
    return "\n".join(["NetworkProviders for:", f"Active default network: {default}", *agents])


MOBILE_ONLY = connectivity("100", "100 MOBILE[HSPA] CONNECTED")


class MobileDataProblemTest(unittest.TestCase):
    def test_accepts_a_connected_mobile_default_network(self) -> None:
        self.assertIsNone(run.mobile_data_problem(MOBILE_ONLY))

    def test_accepts_a_wifi_network_that_has_disconnected(self) -> None:
        dumpsys = connectivity("100", "100 MOBILE[LTE] CONNECTED", "101 WIFI DISCONNECTED")

        self.assertIsNone(run.mobile_data_problem(dumpsys))

    def test_rejects_wifi_as_the_default_network(self) -> None:
        dumpsys = connectivity("101", "100 MOBILE[LTE] CONNECTED", "101 WIFI CONNECTED")

        self.assertEqual("Wi-Fi network 101 is CONNECTED", run.mobile_data_problem(dumpsys))

    def test_rejects_wifi_that_could_still_become_the_default(self) -> None:
        dumpsys = connectivity("100", "100 MOBILE[LTE] CONNECTED", "101 WIFI CONNECTING")

        self.assertEqual("Wi-Fi network 101 is CONNECTING", run.mobile_data_problem(dumpsys))

    def test_rejects_no_default_network(self) -> None:
        self.assertEqual(
            "there is no default network", run.mobile_data_problem(connectivity("none"))
        )
        self.assertEqual(
            "default network 100 is not a connected mobile network",
            run.mobile_data_problem(connectivity("100", "100 MOBILE[LTE] CONNECTING")),
        )


def write_executable(path: Path, lines: list[str]) -> None:
    _ = path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


# The adb stand-in. Its state lives in files in ROOT, its first argument; tests create or read them:
#   adb-calls.log      every call's arguments, one JSON list per line
#   adb-server-fails   if present, the server exits at once as if its port were taken
#   wifi_on            the wifi_on setting ("1" until `svc wifi disable` writes "0")
#   wifi-stuck         how many `svc wifi disable` calls to ignore ("forever" ignores them all)
#   dumpsys-0, -1, ... `dumpsys connectivity` prints these in turn, then repeats the last
#   ping-fails         if present, `ping` gets no answer
# The server knows two emulators: emulator-5554 belongs to another run, and emulator-5556 answers to
# the AVD name the fake emulator was started with. `reboot` changes the boot ID.
FAKE_ADB = """\
import hashlib, json, os, shutil, socket, sys, time
from pathlib import Path

root = Path(sys.argv[1])
arguments = sys.argv[2:]
with (root / "adb-calls.log").open("a") as log:
    print(json.dumps(arguments), file=log)


def read(name, default):
    path = root / name
    return path.read_text() if path.exists() else default


if arguments == ["nodaemon", "server"]:
    if (root / "adb-server-fails").exists():
        sys.exit("could not install *smartsocket* listener: Address already in use")
    environment = {
        name: os.environ.get(name)
        for name in ("ANDROID_ADB_SERVER_PORT", "ADB_LOCAL_TRANSPORT_MAX_PORT")
    }
    _ = (root / "adb-server-env.json").write_text(json.dumps(environment))
    listener = socket.create_server(("localhost", int(os.environ["ANDROID_ADB_SERVER_PORT"])))
    time.sleep(600)
elif arguments == ["devices"]:
    print("List of devices attached")
    print("emulator-5554\\tdevice")
    print("emulator-5556\\tdevice")
elif arguments[:1] == ["-s"]:
    serial, command = arguments[1], arguments[2:]
    if command == ["emu", "avd", "name"]:
        print(read("avd-name", "") if serial == "emulator-5556" else "other")
        print("OK")
    elif command == ["get-state"]:
        print("device")
    elif command == ["shell", "getprop", "sys.boot_completed"]:
        print("1")
    elif command == ["shell", "cat", "/proc/sys/kernel/random/boot_id"]:
        print("boot-" + read("boots", "0"))
    elif command == ["reboot"]:
        _ = (root / "boots").write_text(str(int(read("boots", "0")) + 1))
    elif command == ["shell", "id", "-u"]:
        print("0")
    elif command[:1] == ["push"]:
        _ = shutil.copyfile(command[1], root / "pushed")
    elif command[:2] == ["shell", "sha256sum"]:
        print(hashlib.sha256((root / "pushed").read_bytes()).hexdigest(), command[2])
    elif command == ["shell", "settings", "get", "global", "wifi_on"]:
        print(read("wifi_on", "1"))
    elif command == ["shell", "svc", "wifi", "disable"]:
        disables = int(read("wifi-disables", "0")) + 1
        _ = (root / "wifi-disables").write_text(str(disables))
        stuck = read("wifi-stuck", "0")
        if stuck != "forever" and disables > int(stuck):
            _ = (root / "wifi_on").write_text("0")
    elif command == ["shell", "dumpsys", "connectivity"]:
        outputs = sorted(root.glob("dumpsys-[0-9]*"), key=lambda path: int(path.name[8:]))
        calls = int(read("dumpsys-calls", "0"))
        _ = (root / "dumpsys-calls").write_text(str(calls + 1))
        print(outputs[min(calls, len(outputs) - 1)].read_text())
    elif command[:2] == ["shell", "ping"]:
        sys.exit(1 if (root / "ping-fails").exists() else 0)
"""


def fake_android_tools(root: Path) -> Path:
    """Writes avdmanager, emulator and adb stand-ins to ROOT/bin and returns that directory."""
    tools = root / "bin"
    tools.mkdir()
    write_executable(tools / "avdmanager", ["#!/bin/sh", "cat >/dev/null"])
    write_executable(
        tools / "emulator",
        [
            f"#!{sys.executable}",
            "import json, os, sys, time",
            "from pathlib import Path",
            f"root = Path({str(root)!r})",
            "_ = (root / 'emulator-args.json').write_text(json.dumps(sys.argv[1:]))",
            "_ = (root / 'emulator-tmpdir').write_text(os.environ['TMPDIR'])",
            "_ = (root / 'avd-name').write_text(sys.argv[sys.argv.index('-avd') + 1])",
            "time.sleep(600)",
        ],
    )
    _ = (root / "fake_adb.py").write_text(FAKE_ADB, encoding="utf-8")
    write_executable(
        tools / "adb", ["#!/bin/sh", f'exec {sys.executable} {root / "fake_adb.py"} {root} "$@"']
    )
    return tools


def adb_calls(root: Path) -> list[list[str]]:
    """The arguments of every call to the fake adb, without `-s SERIAL`."""
    calls = [
        cast(list[str], json.loads(line))
        for line in (root / "adb-calls.log").read_text(encoding="utf-8").splitlines()
    ]
    return [call[2:] if call[:1] == ["-s"] else call for call in calls]


@contextlib.contextmanager
def booted_emulator(root: Path) -> Generator[run.Emulator]:
    """An Emulator on the fake tools in ROOT that has booted; it is shut down afterwards."""
    tools = fake_android_tools(root)
    path = f"{tools}{os.pathsep}{os.environ.get('PATH', '')}"
    with mock.patch.dict(os.environ, {"PATH": path}):
        emulator = run.Emulator(root, root, run.DEFAULT_SYSTEM_IMAGE)
        try:
            emulator.wait_for_boot()
            yield emulator
        finally:
            emulator.shutdown()


class EmulatorTest(unittest.TestCase):
    def test_runs_its_own_adb_server_and_finds_its_emulator_by_avd_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with booted_emulator(root) as emulator:
                pass

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
            tools = fake_android_tools(root)
            (root / "adb-server-fails").touch()
            path = f"{tools}{os.pathsep}{os.environ.get('PATH', '')}"
            with (
                mock.patch.dict(os.environ, {"PATH": path}),
                self.assertRaisesRegex(run.RunnerError, "Address already in use"),
            ):
                _ = run.Emulator(root, root, run.DEFAULT_SYSTEM_IMAGE)

            self.assertFalse((root / "avd-name").exists())


class MobileDataTest(unittest.TestCase):
    def test_turns_wifi_off_until_the_setting_reads_off(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with booted_emulator(root) as emulator:
                emulator.use_mobile_data(timeout=5)

            self.assertIn(["shell", "svc", "wifi", "disable"], adb_calls(root))
            self.assertEqual("0", (root / "wifi_on").read_text(encoding="utf-8"))

    def test_asks_again_until_wifi_goes_off(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "wifi-stuck").write_text("2", encoding="utf-8")
            with booted_emulator(root) as emulator:
                emulator.use_mobile_data(timeout=60)

            self.assertEqual(3, adb_calls(root).count(["shell", "svc", "wifi", "disable"]))
            self.assertEqual("0", (root / "wifi_on").read_text(encoding="utf-8"))

    def test_fails_when_wifi_never_goes_off(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "wifi-stuck").write_text("forever", encoding="utf-8")
            with (
                booted_emulator(root) as emulator,
                self.assertRaisesRegex(run.WaitTimeout, "did not turn Wi-Fi off within 1s"),
            ):
                emulator.use_mobile_data(timeout=1)

    def test_polls_until_mobile_data_is_the_only_network(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "wifi_on").write_text("0", encoding="utf-8")
            outputs = [
                connectivity("101", "100 MOBILE[LTE] CONNECTED", "101 WIFI CONNECTED"),
                connectivity("none"),
                MOBILE_ONLY,
            ]
            for index, output in enumerate(outputs):
                _ = (root / f"dumpsys-{index}").write_text(output, encoding="utf-8")
            with booted_emulator(root) as emulator:
                emulator.wait_for_mobile_data(timeout=10)

            self.assertEqual("3", (root / "dumpsys-calls").read_text(encoding="utf-8"))

    def test_turns_wifi_off_again_if_it_comes_back(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "wifi_on").write_text("1", encoding="utf-8")
            _ = (root / "dumpsys-0").write_text(MOBILE_ONLY, encoding="utf-8")
            with booted_emulator(root) as emulator:
                emulator.wait_for_mobile_data(timeout=10)

            self.assertIn(["shell", "svc", "wifi", "disable"], adb_calls(root))

    def test_names_the_unmet_condition_and_keeps_the_last_dumpsys(self) -> None:
        wifi = connectivity("100", "100 MOBILE[LTE] CONNECTED", "101 WIFI CONNECTED")
        for dumpsys, ping_fails, problem in [
            (wifi, False, "Wi-Fi network 101 is CONNECTED"),
            (MOBILE_ONLY, True, f"{run.EMULATOR_HOST} did not answer a ping"),
        ]:
            with self.subTest(problem=problem), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                (root / "wifi_on").write_text("0", encoding="utf-8")
                _ = (root / "dumpsys-0").write_text(dumpsys, encoding="utf-8")
                if ping_fails:
                    (root / "ping-fails").touch()
                with (
                    booted_emulator(root) as emulator,
                    self.assertRaisesRegex(run.WaitTimeout, f"within 1s: {problem}$"),
                ):
                    emulator.wait_for_mobile_data(timeout=1)

                self.assertEqual(dumpsys, (root / "connectivity.log").read_text(encoding="utf-8"))

    def test_passes_an_emulator_exit_through_without_a_network_reason(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _ = (root / "dumpsys-0").write_text(MOBILE_ONLY, encoding="utf-8")
            with booted_emulator(root) as emulator:
                emulator.process.kill()
                _ = emulator.process.wait()
                with self.assertRaises(run.RunnerError) as raised:
                    emulator.wait_for_mobile_data(timeout=10)

            self.assertNotIsInstance(raised.exception, run.WaitTimeout)
            self.assertRegex(str(raised.exception), "^emulator exited with ")

    def test_prepare_turns_wifi_off_before_the_ca_reboots_and_waits_after_them(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _ = (root / "dumpsys-0").write_text(MOBILE_ONLY, encoding="utf-8")
            certificates = run.generate_certificates(root / "certificates")
            with booted_emulator(root) as emulator:
                emulator.prepare(certificates.ca, root)

            calls = adb_calls(root)
            last = {str(call): index for index, call in enumerate(calls)}
            first_disable = calls.index(["shell", "svc", "wifi", "disable"])
            self.assertLess(first_disable, calls.index(["disable-verity"]))
            self.assertGreater(
                last[str(["shell", "dumpsys", "connectivity"])], last[str(["reboot"])]
            )
            self.assertGreater(
                last[str(["shell", "ping", "-c", "1", "-W", "1", run.EMULATOR_HOST])],
                last[str(["reboot"])],
            )
            self.assertEqual("2", (root / "boots").read_text(encoding="utf-8"))


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
