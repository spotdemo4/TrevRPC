"""Unit tests for the benchmark's network emulation. Run with
`python3 -I -m unittest discover -s bench`."""

import argparse
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

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
            "delay 10ms pause 1s every 1s",
            "delay 10ms episode 1s every 5s",
            "delay 10ms episode 1s every 5s rate 1mbit",
            "delay 10ms up rate 1mbit episode 1s every 5s rate 2mbit",
            "delay 10ms episode 1s every 5s delay 20ms distribution pareto",
            # A slot's distribution is no delay distribution.
            "delay 10ms slot distribution pareto 0.2ms 1ms"
            " episode 4s every 10s delay 30ms 9ms distribution pareto",
            "delay 10ms loss 1% episode 1s every 5s loss gemodel 1% 20% 60% 0%",
            "delay 10ms episode 1s every 5s ecn",
            "delay 10ms trace",
            "delay 10ms trace ../etc/passwd",
            "delay 10ms trace lte from",
            "delay 10ms trace lte trace lte",
            "delay 10ms rate 1mbit episode 1s every 5s rate 2mbit trace lte",
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

    def test_pause_and_episode_clauses(self) -> None:
        parsed = netem.parse(
            "delay 10ms slot 1ms 2ms pause 170ms every 5s episode 4s every 10s slot 10ms 60ms"
        )

        self.assertEqual(netem.Outage(0.17, 5.0), parsed.pause)
        self.assertEqual(netem.Outage(4.0, 10.0), parsed.episode)
        self.assertEqual(("slot", "10ms", "60ms"), parsed.episode_arguments)
        self.assertEqual(("delay", "10ms", "slot", "1ms", "2ms"), parsed.up)
        self.assertTrue(parsed.interrupted)
        self.assertFalse(parsed.split)

    def test_an_episode_ends_at_the_next_section(self) -> None:
        parsed = netem.parse("delay 10ms rate 5mbit episode 1s every 4s rate 1mbit up limit 9")

        self.assertEqual(("rate", "1mbit"), parsed.episode_arguments)
        self.assertEqual(("delay", "10ms", "rate", "5mbit", "limit", "9"), parsed.up)

    def test_a_timed_section_ends_an_episode_s_arguments(self) -> None:
        parsed = netem.parse(
            "delay 10ms slot 1ms 2ms episode 4s every 10s slot 5ms 9ms"
            " outage 1s every 5s rate 1mbit"
        )
        self.assertEqual(("slot", "5ms", "9ms"), parsed.episode_arguments)
        self.assertEqual(("delay", "10ms", "slot", "1ms", "2ms", "rate", "1mbit"), parsed.up)

        # Words go back where they went before the episode: here, to the up section.
        parsed = netem.parse(
            "delay 10ms slot 1ms 2ms up limit 5 episode 1s every 5s slot 3ms 4ms"
            " pause 1s every 5s rate 9mbit"
        )
        self.assertEqual(("slot", "3ms", "4ms"), parsed.episode_arguments)
        self.assertEqual(
            ("delay", "10ms", "slot", "1ms", "2ms", "limit", "5", "rate", "9mbit"), parsed.up
        )
        self.assertEqual(("delay", "10ms", "slot", "1ms", "2ms"), parsed.down)

    def test_sticky_options_tell_a_delay_distribution_from_a_slot_s(self) -> None:
        self.assertEqual(
            {"slot"}, netem.sticky_options(("delay", "10ms", "slot", "distribution", "pareto"))
        )
        self.assertEqual(
            {"delay distribution", "rate"},
            netem.sticky_options(
                ("delay", "10ms", "2ms", "distribution", "normal", "rate", "1mbit")
            ),
        )
        self.assertEqual(
            {"gemodel"}, netem.sticky_options(("loss", "gemodel", "1%", "distribution"))
        )

    def test_an_episode_may_change_a_sticky_option_the_profile_sets(self) -> None:
        parsed = netem.parse(
            "delay 10ms 2ms distribution normal"
            " episode 1s every 5s delay 30ms 9ms distribution pareto"
        )
        self.assertEqual(
            ("delay", "30ms", "9ms", "distribution", "pareto"), parsed.episode_arguments
        )

    def test_a_trace_splits_the_directions_and_may_start_later(self) -> None:
        parsed = netem.parse("delay 30ms trace lte from 157s up limit 450")

        self.assertEqual(("lte", 157.0), (parsed.trace, parsed.trace_offset))
        self.assertTrue(parsed.split)
        self.assertFalse(parsed.interrupted)
        self.assertEqual(0.0, netem.parse("delay 30ms trace lte").trace_offset)


class TraceTest(unittest.TestCase):
    def trace(self, *times: int) -> netem.Trace:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "t.up"
            _ = path.write_text("\n".join(map(str, times)) + "\n")
            return netem.Trace(path)

    def test_rates_count_the_opportunities_of_each_step(self) -> None:
        # 100 opportunities at 5 ms and 50 at 250 ms, the trace's last: the third 100 ms step
        # also holds the start of its next cycle, and the fourth nothing.
        trace = self.trace(*[5] * 100, *[250] * 50)

        self.assertEqual(
            [12_000_000, netem.TRACE_FLOOR, 18_000_000, netem.TRACE_FLOOR],
            [trace.rate(step) for step in range(4)],
        )

    def test_a_trace_repeats_after_its_last_millisecond(self) -> None:
        # One opportunity every millisecond from 1 ms, as Mahimahi reads a trace of just 1:
        # 12 Mbit/s, but for the first millisecond.
        self.assertEqual([11_880_000, 12_000_000], [self.trace(1).rate(i) for i in range(2)])
        # One every 3 ms: 33 in each of the first three steps.
        self.assertEqual([3_960_000] * 3, [self.trace(3).rate(i) for i in range(3)])
        # Mahimahi's traces start at 0 ms, which comes with the last of the cycle before: in the
        # first 100 ms of a 50 ms trace with both, at 0, 50 and 50.
        self.assertEqual(3 * 1500 * 8 * 10, self.trace(0, 50).rate(0))

    def test_unreadable_and_unordered_traces_fail(self) -> None:
        for times in ((5, 3), (-1, 3), (0,)):
            with self.subTest(times=times), self.assertRaises(RunnerError):
                _ = self.trace(*times)
        with self.assertRaises(RunnerError):
            _ = netem.Trace(Path("/nonexistent/t.up"))

    def test_steps_change_each_direction_s_rate_from_the_offset(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for direction, count in (("up", 10), ("down", 20)):
                lines = [
                    str(step * 100 + 1) for step in range(4) for _ in range(count * (step + 1))
                ]
                _ = (Path(directory) / f"lte.{direction}").write_text("\n".join(lines))
            with mock.patch.dict(os.environ, {netem.TRACES_VARIABLE: directory}):
                changes = netem.changes(netem.parse("delay 5ms trace lte from 200ms"), 0.25)

        self.assertEqual([0.0, 0.1, 0.2], [round(change.at, 6) for change in changes])
        # From the third step, up: 30 opportunities, then 40 at 301 ms and 10 at 1 ms of the next
        # cycle, then 20 at its 101 ms. Down has twice as many.
        up = [change.commands[0][-1] for change in changes]
        down = [change.commands[1][-1] for change in changes]
        self.assertEqual(["3600000bit", "6000000bit", "2400000bit"], up)
        self.assertEqual(["7200000bit", "12000000bit", "4800000bit"], down)
        self.assertEqual(
            netem.netem_command("change", "up", ("delay", "5ms", "rate", "3600000bit")),
            list(changes[0].commands[0]),
        )

    def test_a_trace_without_its_directory_fails(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=True), self.assertRaises(RunnerError):
            _ = netem.changes(netem.parse("delay 5ms trace lte"), 1.0)


class ChangesTest(unittest.TestCase):
    def test_pauses_block_and_release_both_plugs(self) -> None:
        changes = netem.changes(netem.parse("delay 1ms pause 1s every 4s"), 8.0)

        self.assertEqual([2.0, 3.0, 6.0, 7.0], [change.at for change in changes])
        self.assertEqual(tuple(map(tuple, netem.PAUSE_START)), changes[0].commands)
        self.assertEqual(changes[0].ending, changes[1].commands)
        self.assertEqual(tuple(map(tuple, netem.PAUSE_END)), changes[1].commands)
        self.assertTrue(all(change.interruption for change in changes))

    def test_episodes_change_the_netems_and_back(self) -> None:
        profile = "delay 1ms slot 1ms 2ms up rate 9mbit episode 1s every 4s slot 5ms 9ms"
        changes = netem.changes(netem.parse(profile), 4.0)

        start, end = changes
        self.assertEqual(
            (("delay", "1ms", "slot", "1ms", "2ms", "rate", "9mbit", "slot", "5ms", "9ms"),
             ("delay", "1ms", "slot", "1ms", "2ms", "slot", "5ms", "9ms")),
            tuple(command[12:] for command in start.commands),
        )  # fmt: skip
        self.assertEqual(
            (("delay", "1ms", "slot", "1ms", "2ms", "rate", "9mbit"),
             ("delay", "1ms", "slot", "1ms", "2ms")),
            tuple(command[12:] for command in end.commands),
        )  # fmt: skip
        self.assertFalse(start.interruption)

    def test_an_ending_comes_before_a_start_at_the_same_instant(self) -> None:
        changes = netem.changes(netem.parse("delay 1ms pause 1s every 2s outage 1s every 4s"), 4.0)

        # The first pause ends at 2 s, when the first outage starts.
        at_two = [change for change in changes if change.at == 2.0]
        self.assertEqual(2, len(at_two))
        self.assertIsNotNone(at_two[0].ends)
        self.assertIsNotNone(at_two[1].starts)


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

    def test_segments_frames_to_the_mtu(self) -> None:
        link, tbf = netem.link_commands()

        self.assertEqual(["ip", "link", "set", "lo", "mtu", "1500", "up"], link)
        self.assertEqual(["root", "handle", "1:", "tbf"], tbf[5:9])
        self.assertEqual("1514", tbf[tbf.index("burst") + 1])

    def test_the_directions_split_under_the_tbf(self) -> None:
        prio, *_ = netem.tree_commands(netem.parse("delay 10ms"), [])

        self.assertEqual(["add", "dev", "lo", "parent", "1:1", "handle", "10:"], prio[2:9])
        self.assertEqual(["prio", "bands", "2"], prio[9:12])
        # Everything goes to the first band unless a filter says otherwise.
        self.assertEqual(["priomap", *["0"] * 16], prio[12:])

    def test_profile_follows_the_default_limit_so_its_own_limit_wins(self) -> None:
        _, up, down = netem.tree_commands(netem.parse("delay 10ms limit 50 down rate 1mbit"), [])

        self.assertEqual(["add", "dev", "lo", "parent", "10:1", "handle", "20:", "netem"], up[2:10])
        self.assertEqual(["limit", "1000000", "delay", "10ms", "limit", "50"], up[10:])
        self.assertEqual(["parent", "10:2", "handle", "30:", "netem"], down[5:10])
        self.assertEqual(
            ["limit", "1000000", "delay", "10ms", "limit", "50", "rate", "1mbit"], down[10:]
        )
        # A split profile filters the server ports it is given.
        self.assertEqual(
            [netem.direction_command(4433)],
            netem.tree_commands(netem.parse("delay 1ms up rate 1mbit"), [4433])[3:],
        )
        self.assertEqual([], netem.tree_commands(netem.parse("delay 1ms"), [4433])[3:])

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

        self.assertEqual(
            [*netem.link_commands(),
             *netem.tree_commands(netem.parse("delay 1ms"), []),
             netem.REMOVE_TREE,
             *netem.tree_commands(netem.parse("delay 2ms"), [])],
            self.commands,
        )  # fmt: skip

    def test_each_profile_builds_its_qdiscs_anew(self) -> None:
        # Changing a netem in place would keep the rate and the slot for the second profile.
        self.link.apply("delay 10ms rate 100mbit slot 1ms 8ms")
        self.commands.clear()
        self.link.apply("delay 10ms")

        self.assertEqual(
            [netem.REMOVE_TREE, *netem.tree_commands(netem.parse("delay 10ms"), [])],
            self.commands,
        )

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

    def test_a_pausing_profile_puts_a_released_plug_under_each_netem(self) -> None:
        commands = netem.tree_commands(netem.parse("delay 1ms pause 1s every 5s"), [])

        plugs = [command for command in commands if "plug" in command]
        self.assertEqual(
            [["tc", "qdisc", "add", "dev", "lo", "parent", "20:1", "handle", netem.UP_PLUG,
              "plug", "limit", str(netem.PLUG_LIMIT)],
             netem.plug_command("up", "release_indefinite"),
             ["tc", "qdisc", "add", "dev", "lo", "parent", "30:1", "handle", netem.DOWN_PLUG,
              "plug", "limit", str(netem.PLUG_LIMIT)],
             netem.plug_command("down", "release_indefinite")],
            plugs,
        )  # fmt: skip
        self.assertEqual([], [c for c in netem.tree_commands(netem.parse("delay 1ms"), [])
                              if "plug" in c])  # fmt: skip

    def test_configure_tries_each_change_a_profile_makes_in_place(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            for direction in ("up", "down"):
                _ = (Path(directory) / f"lte.{direction}").write_text("1\n")
            with mock.patch.dict(os.environ, {netem.TRACES_VARIABLE: directory}):
                self.link.configure(
                    ["delay 1ms pause 1s every 5s",
                     "delay 1ms slot 1ms 2ms episode 1s every 5s slot 9ms 9ms",
                     "delay 1ms trace lte"]
                )  # fmt: skip

        # Building the pausing profile releases its plugs; then each change once.
        changes = [command for command in self.commands if command[2] == "change"]
        self.assertEqual(netem.PAUSE_END, changes[:2])
        self.assertEqual([*netem.PAUSE_START, *netem.PAUSE_END], changes[2:6])
        self.assertEqual(["slot", "9ms", "9ms"], changes[6][-3:])
        self.assertEqual(["slot", "1ms", "2ms"], changes[8][-3:])
        self.assertEqual(["rate", "11880000bit"], changes[10][-2:])
        self.assertEqual(12, len(changes))

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

    def test_a_new_server_rebuilds_the_profile_with_its_ports(self) -> None:
        self.link.serve([4433])
        self.link.apply("delay 10ms up rate 5mbit")
        self.commands.clear()
        self.link.serve([5000])

        self.assertEqual(
            [netem.REMOVE_TREE,
             *netem.tree_commands(netem.parse("delay 10ms up rate 5mbit"), [5000])],
            self.commands,
        )  # fmt: skip

    def test_a_server_before_any_profile_builds_nothing(self) -> None:
        self.link.serve([4433])

        self.assertEqual([], self.commands)

    def test_a_profile_without_outages_runs_none(self) -> None:
        with self.link.impairments("delay 10ms", 10.0):
            pass

        self.assertEqual([], self.commands)

    def test_round_trip_adds_each_direction_s_last_delay(self) -> None:
        self.assertEqual(0.02, netem.round_trip("delay 10ms loss 1%"))
        self.assertAlmostEqual(0.025, netem.round_trip("delay 5ms up delay 20ms") or 0)
        # Up keeps the shared 1 s, down overrides it with 500 µs.
        self.assertAlmostEqual(1.0005, netem.round_trip("delay 1s down delay 500us") or 0)
        self.assertIsNone(netem.round_trip("rate 1mbit"))

    def test_outages_run_while_the_block_does_and_end_when_it_does(self) -> None:
        # One outage fits the 20 ms window, from 10 ms to 19 ms; the block may end before or
        # after it does, and the outage ends either way.
        started = threading.Event()
        commands: list[list[str]] = []

        def run(command: list[str]) -> None:
            commands.append(command)
            started.set()

        link = netem.Link(run, lambda _ports: None)
        with link.impairments("delay 1ms outage 9ms every 20ms", 0.02) as times:
            self.assertTrue(started.wait(5))

        self.assertEqual([netem.OUTAGE_START, netem.OUTAGE_END], commands)
        # The outage that ran, by the wall clock.
        self.assertEqual(1, len(times))
        self.assertLessEqual(times[0][0], times[0][1])

    def test_pauses_block_and_release_and_count_as_interruptions(self) -> None:
        released = threading.Event()
        commands: list[list[str]] = []

        def run(command: list[str]) -> None:
            commands.append(command)
            if command == netem.PAUSE_END[-1]:
                released.set()

        link = netem.Link(run, lambda _ports: None)
        with link.impairments("delay 1ms pause 9ms every 20ms", 0.02) as times:
            self.assertTrue(released.wait(5))

        self.assertEqual([*netem.PAUSE_START, *netem.PAUSE_END], commands)
        self.assertEqual(1, len(times))

    def test_what_has_not_ended_ends_with_the_block(self) -> None:
        blocked = threading.Event()
        commands: list[list[str]] = []

        def run(command: list[str]) -> None:
            commands.append(command)
            if command == netem.PAUSE_START[-1]:
                blocked.set()

        link = netem.Link(run, lambda _ports: None)
        # The first pause runs from 1 s to 2.9 s: the block ends long before it would.
        with link.impairments("delay 1ms pause 1900ms every 2s", 10.0) as times:
            self.assertTrue(blocked.wait(5))

        self.assertEqual([*netem.PAUSE_START, *netem.PAUSE_END], commands)
        self.assertEqual(1, len(times))

    def test_outages_that_fail_fail_the_block(self) -> None:
        called = threading.Event()

        def run(command: list[str]) -> None:
            called.set()
            raise RunnerError(f"{command[1]} failed")

        link = netem.Link(run, lambda _ports: None)
        with (
            self.assertRaises(RunnerError),
            link.impairments("delay 1ms outage 1ms every 2ms", 1.0),
        ):
            # Once run has been called, the block may end: leaving it joins the thread, which
            # records the error before it exits.
            self.assertTrue(called.wait(5))


if __name__ == "__main__":
    _ = unittest.main()
