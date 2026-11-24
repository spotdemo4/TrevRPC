"""Unit tests for the benchmark's network emulation. Run with
`python3 -I -m unittest discover -s bench`."""

import argparse
import sys
import threading
import unittest
from pathlib import Path

import netem
from harness import RunnerError


class ProfileTest(unittest.TestCase):
    def test_normalizes_whitespace_and_quoting(self) -> None:
        self.assertEqual("delay 10ms loss 1%", netem.profile("  delay 10ms   'loss' 1% "))

    def test_rejects_empty_and_unparsable_profiles(self) -> None:
        for value in [
            "",
            "   ",
            "delay 'unterminated",
            "delay 10ms up",
            "delay 10ms up rate 1mbit up rate 2mbit",
            "delay 10ms outage 1s",
            "delay 10ms outage 1s each 5s",
            "delay 10ms outage 5s every 5s",
            "delay 10ms outage 1 every 5s",
            "delay 10ms outage 1s every 5s outage 1s every 9s",
        ]:
            with self.subTest(value=value), self.assertRaises(argparse.ArgumentTypeError):
                _ = netem.profile(value)

    def test_a_profile_without_directions_shares_one_queue(self) -> None:
        parsed = netem.parse("delay 10ms rate 20mbit")

        self.assertEqual(("delay", "10ms", "rate", "20mbit"), parsed.up)
        self.assertEqual(parsed.up, parsed.down)
        self.assertFalse(parsed.split)
        self.assertIsNone(parsed.outage)

    def test_direction_sections_add_to_the_shared_arguments(self) -> None:
        parsed = netem.parse("delay 20ms up rate 5mbit limit 100 down rate 30mbit")

        self.assertEqual(("delay", "20ms", "rate", "5mbit", "limit", "100"), parsed.up)
        self.assertEqual(("delay", "20ms", "rate", "30mbit"), parsed.down)
        self.assertTrue(parsed.split)

    def test_one_direction_splits_the_queues_too(self) -> None:
        parsed = netem.parse("delay 20ms rate 30mbit up rate 5mbit")

        self.assertEqual(("delay", "20ms", "rate", "30mbit"), parsed.down)
        self.assertTrue(parsed.split)

    def test_outage_clause_may_come_anywhere(self) -> None:
        parsed = netem.parse("delay 20ms outage 500ms every 4s up rate 5mbit")

        self.assertEqual(netem.Outage(0.5, 4.0), parsed.outage)
        self.assertEqual(("delay", "20ms", "rate", "5mbit"), parsed.up)
        self.assertEqual(("delay", "20ms"), parsed.down)


class OutageTest(unittest.TestCase):
    def test_outages_start_half_a_period_in_and_end_inside_the_window(self) -> None:
        windows = list(netem.Outage(2.0, 5.0).windows(11.0))

        self.assertEqual([(2.5, 4.5), (7.5, 9.5)], windows)

    def test_no_outage_when_the_window_is_too_short(self) -> None:
        self.assertEqual([], list(netem.Outage(2.0, 5.0).windows(4.0)))


class CommandTest(unittest.TestCase):
    def test_reexecutes_the_script_in_new_user_and_network_namespaces(self) -> None:
        command = netem.namespace_command(Path("/bench/run.py"), ["--netem", "delay 1ms"])

        self.assertEqual(["unshare", "--user", "--map-root-user", "--net", "--"], command[:5])
        self.assertEqual(
            [sys.executable, "-I", "/bench/run.py", "--netem", "delay 1ms"], command[5:]
        )

    def test_segments_frames_to_the_mtu_before_the_directions_split(self) -> None:
        link, tbf, prio = netem.link_commands()

        self.assertEqual(["ip", "link", "set", "lo", "mtu", "1500", "up"], link)
        self.assertEqual(["root", "handle", "1:", "tbf"], tbf[5:9])
        self.assertEqual("1514", tbf[tbf.index("burst") + 1])
        self.assertEqual(["parent", "1:1", "handle", "10:", "prio", "bands", "2"], prio[5:12])
        # Everything goes to the first band unless a filter says otherwise.
        self.assertEqual(["priomap", *["0"] * 16], prio[12:])

    def test_profile_follows_the_default_limit_so_its_own_limit_wins(self) -> None:
        up, down = netem.netem_commands(netem.parse("delay 10ms limit 50 down rate 1mbit"))

        self.assertEqual(["parent", "10:1", "handle", "20:", "netem"], up[5:10])
        self.assertEqual(["limit", "1000000", "delay", "10ms", "limit", "50"], up[10:])
        self.assertEqual(["parent", "10:2", "handle", "30:", "netem"], down[5:10])
        self.assertEqual(
            ["limit", "1000000", "delay", "10ms", "limit", "50", "rate", "1mbit"], down[10:]
        )

    def test_direction_filter_matches_the_source_port_whatever_the_protocol(self) -> None:
        command = netem.direction_command(4433)

        self.assertEqual(["protocol", "ip", "prio", "2", "u32"], command[7:12])
        self.assertEqual(["match", "ip", "sport", "4433", "0xffff", "flowid", "10:2"], command[12:])

    def test_outage_drops_everything_ahead_of_the_direction_filters(self) -> None:
        self.assertEqual(["prio", "1", "matchall", "action", "drop"], netem.OUTAGE_START[9:])
        self.assertEqual(["prio", "1"], netem.OUTAGE_END[-2:])


class LinkTest(unittest.TestCase):
    def setUp(self) -> None:
        self.commands: list[list[str]] = []
        self.reserved: list[list[int]] = []
        self.link = netem.Link(self.commands.append, self.reserved.append)

    def filters(self) -> list[list[str]]:
        return [command for command in self.commands if command[1] == "filter"]

    def test_configure_sets_up_the_link_and_tries_every_profile(self) -> None:
        self.link.configure(["delay 1ms", "delay 2ms"])

        self.assertEqual(netem.link_commands(), self.commands[:3])
        self.assertEqual(
            [*netem.netem_commands(netem.parse("delay 1ms")),
             *netem.netem_commands(netem.parse("delay 2ms"))],
            self.commands[3:],
        )  # fmt: skip

    def test_configure_tries_the_filters_the_profiles_need_once(self) -> None:
        self.link.configure(
            ["delay 1ms up rate 1mbit", "delay 1ms down rate 1mbit outage 1s every 5s"]
        )

        delete = netem.delete_filters_command(netem.DIRECTION_PRIORITY)
        self.assertEqual(
            [netem.direction_command(netem.PROBE_PORT), delete,
             netem.OUTAGE_START, netem.OUTAGE_END],
            self.filters(),
        )  # fmt: skip
        self.assertEqual([[]], self.reserved)
        # The probe leaves no filter behind for apply to delete.
        self.assertNotIn(delete, self.commands[self.commands.index(netem.OUTAGE_END) :])

    def test_configure_tries_no_filter_for_shared_profiles(self) -> None:
        self.link.configure(["delay 1ms rate 1mbit"])

        self.assertEqual([], self.filters())
        self.assertEqual([], self.reserved)

    def test_a_shared_profile_adds_no_filter(self) -> None:
        self.link.serve([4433, 4434])
        self.link.apply("delay 10ms rate 20mbit")

        self.assertEqual([[4433, 4434]], self.reserved)
        self.assertEqual([], self.filters())

    def test_a_split_profile_filters_each_server_port_once(self) -> None:
        self.link.apply("delay 10ms up rate 5mbit")
        self.link.serve([4434, 4433, 4434])

        self.assertEqual([[4433, 4434]], self.reserved)
        self.assertEqual(
            [netem.direction_command(4433), netem.direction_command(4434)], self.filters()
        )

    def test_switching_profiles_and_servers_replaces_the_filters(self) -> None:
        self.link.serve([4433])
        self.link.apply("delay 10ms up rate 5mbit")
        self.link.serve([5000])
        self.link.apply("delay 10ms")
        self.link.apply("delay 10ms")

        delete = netem.delete_filters_command(netem.DIRECTION_PRIORITY)
        self.assertEqual(
            [netem.direction_command(4433), delete, netem.direction_command(5000), delete],
            self.filters(),
        )

    def test_a_profile_without_outages_runs_none(self) -> None:
        with self.link.outages("delay 10ms", 10.0):
            pass

        self.assertEqual([], self.commands)

    def test_outages_run_while_the_block_does_and_end_when_it_does(self) -> None:
        # One outage fits the 20 ms window, from 10 ms to 19 ms; the block may end before or
        # after it does, and the outage ends either way.
        started = threading.Event()
        commands: list[list[str]] = []

        def run(command: list[str]) -> None:
            commands.append(command)
            started.set()

        link = netem.Link(run, lambda _ports: None)
        with link.outages("delay 1ms outage 9ms every 20ms", 0.02):
            self.assertTrue(started.wait(5))

        self.assertEqual([netem.OUTAGE_START, netem.OUTAGE_END], commands)

    def test_outages_that_fail_fail_the_block(self) -> None:
        called = threading.Event()

        def run(command: list[str]) -> None:
            called.set()
            raise RunnerError(f"{command[1]} failed")

        link = netem.Link(run, lambda _ports: None)
        with self.assertRaises(RunnerError), link.outages("delay 1ms outage 1ms every 2ms", 1.0):
            # Once run has been called, the block may end: leaving it joins the thread, which
            # records the error before it exits.
            self.assertTrue(called.wait(5))


if __name__ == "__main__":
    _ = unittest.main()
