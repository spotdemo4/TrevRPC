"""Unit tests for the runner harness. Run with `python3 -I -m unittest discover -s interop`."""

import argparse
import os
import stat
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import harness


def fake_server(root: Path, *lines: str) -> list[str]:
    """Writes an executable shell script and returns the command that runs it."""
    path = root / "fake-server"
    _ = path.write_text("#!/bin/sh\n" + "\n".join(lines) + "\n", encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)
    return [str(path)]


class ArgumentsTest(unittest.TestCase):
    def test_named_command_splits_at_the_first_equals_sign(self) -> None:
        self.assertEqual(
            harness.NamedCommand("go", ["trevrpc-interop-go", "--flag=a b"]),
            harness.named_command("go=trevrpc-interop-go '--flag=a b'"),
        )

    def test_named_command_rejects_missing_names(self) -> None:
        for value in ["trevrpc-interop-go", "=trevrpc-interop-go", "go="]:
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                _ = harness.named_command(value)

    def test_comma_list_drops_empty_items(self) -> None:
        self.assertEqual(["a", "b"], harness.comma_list("a,,b,"))
        self.assertEqual([], harness.comma_list(""))


class ParseWithDefaultsTest(unittest.TestCase):
    def parse(self, argv: list[str], defaults: str | None) -> argparse.Namespace:
        parser = argparse.ArgumentParser()
        _ = parser.add_argument("--server", action="append", default=[])
        _ = parser.add_argument("--browser", action="append", default=[])
        _ = parser.add_argument("--out")
        with mock.patch.dict(os.environ, clear=False) as environment:
            _ = environment.pop(harness.DEFAULTS_VARIABLE, None)
            if defaults is not None:
                environment[harness.DEFAULTS_VARIABLE] = defaults
            args = harness.parse_with_defaults(parser, argv, ("server", "browser"))
            self.assertNotIn(harness.DEFAULTS_VARIABLE, os.environ)
        return args

    def test_uses_the_defaults_alongside_other_options(self) -> None:
        args = self.parse(["--out", "dir"], "--server 'go=a b' --browser chromium")

        self.assertEqual((["go=a b"], ["chromium"], "dir"), (args.server, args.browser, args.out))

    def test_an_explicit_matrix_option_replaces_every_default(self) -> None:
        args = self.parse(["--browser", "safari"], "--server go=a --browser chromium")

        self.assertEqual(([], ["safari"]), (args.server, args.browser))

    def test_without_defaults_parses_as_usual(self) -> None:
        self.assertEqual([], self.parse([], None).server)


class RunWithDeadlineTest(unittest.TestCase):
    def test_captures_stdout_and_status(self) -> None:
        result = harness.run_with_deadline(
            ["sh", "-c", "echo out; exit 3"], 10, stdout=subprocess.PIPE
        )

        self.assertEqual(harness.Completed(3, "out\n"), result)

    def test_stops_the_whole_process_group_at_the_deadline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            marker = Path(temporary) / "survived"
            started = time.monotonic()

            # The grandchild would write the marker if it outlived the deadline.
            result = harness.run_with_deadline(
                ["sh", "-c", f"(sleep 2; touch {marker}) & sleep 30"], 0.2
            )
            time.sleep(2.5)

            self.assertIsNone(result.returncode)
            self.assertLess(time.monotonic() - started, 10)
            self.assertFalse(marker.exists())


class ParsePortsTest(unittest.TestCase):
    def test_accepts_the_readiness_line(self) -> None:
        self.assertEqual(
            {"native": 4433, "qmux": 4436}, harness.parse_ports('{"native":4433,"qmux":4436}')
        )

    def test_rejects_malformed_lines(self) -> None:
        for line in ["", "ready", "[]", "{}", '{"native":0}', '{"native":true}', '{"native":"1"}']:
            with self.subTest(line=line), self.assertRaises(harness.RunnerError):
                _ = harness.parse_ports(line)


class ServerTest(unittest.TestCase):
    def test_passes_tls_flags_and_reports_ports_until_stdin_closes(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = fake_server(
                root, 'echo "$@" >&2', "echo '{\"native\":4433}'", "cat >/dev/null"
            )
            certificate = harness.Certificate(root / "cert.pem", root / "key.pem")

            server = harness.Server("fake", command, certificate, root, ["--web-root", "web"])
            problem = server.stop()

            self.assertIsNone(problem)
            self.assertEqual(4433, server.port("native"))
            self.assertEqual('{"native":4433}\n', (root / "server-fake.ports").read_text())
            self.assertEqual(
                f"server --cert {certificate.cert} --key {certificate.key} --web-root web",
                server.log.read_text().strip(),
            )
            with self.assertRaisesRegex(harness.RunnerError, "'qmux'"):
                _ = server.port("qmux")

    def test_reports_a_server_that_exits_before_its_ports(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = fake_server(root, "echo broken >&2", "exit 3")
            certificate = harness.Certificate(root / "cert.pem", root / "key.pem")

            with self.assertRaisesRegex(harness.RunnerError, "exited with 3(.|\n)*broken"):
                _ = harness.Server("fake", command, certificate, root)

    def test_reports_an_unclean_exit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = fake_server(root, "echo '{\"native\":1}'", "cat >/dev/null", "exit 1")
            certificate = harness.Certificate(root / "cert.pem", root / "key.pem")

            server = harness.Server("fake", command, certificate, root)

            self.assertRegex(server.stop() or "", "exited with 1")


class FailureSummaryTest(unittest.TestCase):
    def test_keeps_what_explains_a_failure_and_drops_passing_scenarios(self) -> None:
        log = "\n".join(
            ["ok unary", "[browser error] Failed to load resource", "ok bidi", "",
             "fail compressed-stream: deadline exceeded", "ok refused-stream"]
        )  # fmt: skip

        self.assertEqual(
            [
                "[browser error] Failed to load resource",
                "fail compressed-stream: deadline exceeded",
            ],
            harness.failure_details(log),
        )
        self.assertEqual(["b"], harness.failure_details("a\nb", limit=1))

    def test_the_summary_lists_each_failure_with_its_reasons(self) -> None:
        summary = harness.failure_summary(
            [harness.Failure("chromium-websocket-to-go", ["fail bidi: reset"]),
             harness.Failure("server-rust", ["exited with status 1"])]
        )  # fmt: skip

        self.assertEqual(
            "failed: chromium-websocket-to-go\n    fail bidi: reset\n"
            "failed: server-rust\n    exited with status 1",
            summary,
        )


class CertificateTest(unittest.TestCase):
    def test_certificate_validates_for_localhost_and_loopback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            certificate = harness.generate_certificate(Path(temporary))

            for flag, value in [("-verify_hostname", "localhost"), ("-verify_ip", "127.0.0.1")]:
                result = subprocess.run(
                    [
                        "openssl", "verify", "-CAfile", str(certificate.cert),
                        flag, value, str(certificate.cert),
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )  # fmt: skip
                self.assertEqual(0, result.returncode, f"{flag} {value}: {result.stdout}")
            self.assertTrue(certificate.der().startswith(b"\x30"))


if __name__ == "__main__":
    _ = unittest.main()
