"""Unit tests for the microbenchmark runner. Run with `python3 -I -m unittest discover -s bench`."""

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

import micro


def checkout(root: Path) -> Path:
    (root / "trevrpc-go").mkdir()
    _ = (root / "trevrpc-go" / "go.mod").write_text("module x\n", encoding="utf-8")
    (root / "trevrpc-rust").mkdir()
    _ = (root / "trevrpc-rust" / "Cargo.toml").write_text("", encoding="utf-8")
    return root


class FindRootTest(unittest.TestCase):
    def test_finds_the_checkout_from_a_subdirectory(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = checkout(Path(temporary).resolve())
            (root / "bench" / "tools").mkdir(parents=True)

            self.assertEqual(root, micro.find_root(root / "bench" / "tools"))

    def test_rejects_a_directory_outside_a_checkout(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, self.assertRaises(micro.RunnerError):
            _ = micro.find_root(Path(temporary))


class TeeTest(unittest.TestCase):
    def test_copies_stdout_to_the_terminal_and_the_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "out.txt"
            terminal = io.StringIO()

            with contextlib.redirect_stdout(terminal):
                micro.tee(["sh", "-c", "echo one; echo two"], output)

            self.assertEqual("one\ntwo\n", output.read_text(encoding="utf-8"))
            self.assertTrue(terminal.getvalue().endswith("one\ntwo\n"))

    def test_raises_when_the_command_fails(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temporary,
            contextlib.redirect_stdout(io.StringIO()),
            self.assertRaisesRegex(micro.RunnerError, "exited 3"),
        ):
            micro.tee(["sh", "-c", "exit 3"], Path(temporary) / "out.txt")


class CommandsTest(unittest.TestCase):
    def test_go_runs_only_benchmarks_in_every_package(self) -> None:
        command = micro.go_command(Path("/src"), "BenchmarkUnary", 10)

        self.assertEqual(
            ["go", "-C", "/src/trevrpc-go", "test", "-run", "^$", "-bench", "BenchmarkUnary"],
            command[:8],
        )
        self.assertEqual(["-count", "10", "./..."], command[8:])

    def test_benchstat_compares_the_baseline_with_the_new_run(self) -> None:
        command = micro.benchstat_command(Path("old/go.txt"), Path("new/go.txt"))

        self.assertEqual(["benchstat", "old/go.txt", "new/go.txt"], command)


class BaselineTest(unittest.TestCase):
    def run_micro(self, *argv: str) -> None:
        with contextlib.redirect_stdout(io.StringIO()):
            _ = micro.execute(micro.parse_args(list(argv)))

    def test_requires_go_results_in_the_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(micro.RunnerError, "go.txt does not exist"):
                self.run_micro("--baseline", temporary)
            with self.assertRaisesRegex(micro.RunnerError, "needs --runtime go"):
                self.run_micro("--baseline", temporary, "--runtime", "rust")

    def test_refuses_to_overwrite_the_baseline(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = checkout(Path(temporary))
            _ = (root / "go.txt").write_text("", encoding="utf-8")

            with self.assertRaisesRegex(micro.RunnerError, "must differ"):
                self.run_micro("--root", str(root), "--baseline", str(root), "--out", str(root))


if __name__ == "__main__":
    _ = unittest.main()
