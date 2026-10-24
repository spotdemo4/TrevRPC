"""Unit tests for the interop runner's host-side logic. Run with `python3 -I -m unittest`."""

import json
import stat
import subprocess
import tempfile
import textwrap
import unittest
import zipfile
from pathlib import Path

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

    def test_reports_a_server_that_exits_early(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            certificates = run.Certificates(
                root / "ca.pem", root / "server.pem", root / "server.key"
            )

            with self.assertRaisesRegex(run.RunnerError, "exited with 3"):
                _ = run.start_server(
                    ["sh", "-c", "exit 3", "--"],
                    certificates,
                    "0.0.0.0",
                    root / "interop.dict",
                    root,
                )


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
