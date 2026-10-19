"""Unit tests for the benchmark's network emulation. Run with
`python3 -I -m unittest discover -s bench`."""

import argparse
import sys
import unittest
from pathlib import Path

import netem


class ProfileTest(unittest.TestCase):
    def test_normalizes_whitespace_and_quoting(self) -> None:
        self.assertEqual("delay 10ms loss 1%", netem.profile("  delay 10ms   'loss' 1% "))

    def test_rejects_empty_and_unparsable_profiles(self) -> None:
        for value in ["", "   ", "delay 'unterminated"]:
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                _ = netem.profile(value)


class CommandTest(unittest.TestCase):
    def test_reexecutes_the_script_in_new_user_and_network_namespaces(self) -> None:
        command = netem.namespace_command(Path("/bench/run.py"), ["--netem", "delay 1ms"])

        self.assertEqual(["unshare", "--user", "--map-root-user", "--net", "--"], command[:5])
        self.assertEqual(
            [sys.executable, "-I", "/bench/run.py", "--netem", "delay 1ms"], command[5:]
        )

    def test_segments_frames_to_the_mtu_before_netem(self) -> None:
        link, tbf = netem.link_commands()

        self.assertEqual(["ip", "link", "set", "lo", "mtu", "1500", "up"], link)
        self.assertEqual(["root", "handle", "1:", "tbf"], tbf[5:9])
        self.assertEqual("1514", tbf[tbf.index("burst") + 1])

    def test_profile_follows_the_default_limit_so_its_own_limit_wins(self) -> None:
        command = netem.profile_command("delay 10ms limit 50")

        self.assertEqual(["parent", "1:1", "handle", "10:", "netem"], command[5:10])
        self.assertEqual(["limit", "1000000", "delay", "10ms", "limit", "50"], command[10:])


if __name__ == "__main__":
    _ = unittest.main()
