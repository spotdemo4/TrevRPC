#!/usr/bin/env python3

import io
import json
import os
import runpy
import unittest
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import NamedTuple, TextIO, TypedDict, cast

SCRIPT = Path(
    os.environ.get(
        "SMOKE_CELL_DISCOVERY",
        Path(__file__).parents[1] / "discover-smoke-cells.py",
    )
)
NAMESPACE = runpy.run_path(str(SCRIPT))


class Cell(TypedDict):
    id: str
    client: str
    server: str
    stack: str


class CompatibilityExclusion(NamedTuple):
    reason: str
    detail: str


compatibility_exclusion = cast(
    Callable[[str, Cell], CompatibilityExclusion | None],
    NAMESPACE["compatibility_exclusion"],
)
Matrix = dict[str, dict[str, list[dict[str, str]]]]
discover = cast(Callable[[Path], Matrix], NAMESPACE["discover"])
validate_rule_matches = cast(
    Callable[[Mapping[tuple[str, str, str, str], int]], None],
    NAMESPACE["validate_rule_matches"],
)
write_github_outputs = cast(
    Callable[[Matrix, TextIO], None], NAMESPACE["write_github_outputs"]
)
REPOSITORY_ROOT = Path(
    os.environ.get("SMOKE_REPOSITORY_ROOT", Path(__file__).parents[3])
)


class SmokeCellDiscoveryTests(unittest.TestCase):
    def test_discovers_expected_platform_matrices(self) -> None:
        matrices = discover(REPOSITORY_ROOT)

        self.assertEqual(len(matrices["linux"]["include"]), 48)
        self.assertEqual(len(matrices["android"]["include"]), 6)
        self.assertEqual(
            [cell["cell"] for cell in matrices["webkit"]["include"]],
            [
                "webkit-to-c",
                "webkit-to-cpp",
                "webkit-to-js",
                "webkit-to-kotlin",
            ],
        )

    def test_preserves_campaign_and_package_mapping(self) -> None:
        matrices = discover(REPOSITORY_ROOT)
        cells = {cell["cell"]: cell for cell in matrices["linux"]["include"]}

        self.assertEqual(
            cells["go-to-c"],
            {
                "cell": "go-to-c",
                "campaign": "bench/campaigns/native-smoke.example.json",
                "client_package": "trevrpc-go",
                "server_package": "trevrpc-c",
            },
        )
        self.assertEqual(
            cells["chromium-to-rust"],
            {
                "cell": "chromium-to-rust",
                "campaign": "bench/campaigns/chromium-smoke.example.json",
                "client_package": "trevrpc-browser-bench-peer",
                "server_package": "trevrpc-rust",
            },
        )
        self.assertEqual(cells["chromium-to-go"]["server_package"], "trevrpc-go")
        self.assertEqual(cells["firefox-to-go"]["server_package"], "trevrpc-go")
        self.assertEqual(cells["firefox-to-rust"]["server_package"], "trevrpc-rust")

    def test_darwin_exclusions_are_pair_specific(self) -> None:
        go = Cell(
            id="webkit-to-go",
            client="webkit",
            server="go",
            stack="trevrpc_webtransport",
        )
        rust = Cell(
            id="webkit-to-rust",
            client="webkit",
            server="rust",
            stack="trevrpc_webtransport",
        )
        chromium_go = Cell(
            id="chromium-to-go",
            client="chromium",
            server="go",
            stack="trevrpc_webtransport",
        )

        go_exclusion = compatibility_exclusion("darwin", go)
        rust_exclusion = compatibility_exclusion("darwin", rust)
        self.assertIsNotNone(go_exclusion)
        self.assertIsNotNone(rust_exclusion)
        self.assertEqual(
            cast(CompatibilityExclusion, go_exclusion).reason,
            "go_native_transport_linux_only",
        )
        self.assertIn(
            "h3/issues/347", cast(CompatibilityExclusion, rust_exclusion).detail
        )
        self.assertIsNone(compatibility_exclusion("linux", go))
        self.assertIsNone(compatibility_exclusion("darwin", chromium_go))

    def test_rejects_unmatched_compatibility_rules(self) -> None:
        with self.assertRaisesRegex(ValueError, "matched 0 cells"):
            validate_rule_matches({})

    def test_github_outputs_round_trip(self) -> None:
        output = io.StringIO()
        write_github_outputs(discover(REPOSITORY_ROOT), output)
        values: Matrix = {
            name: cast(dict[str, list[dict[str, str]]], json.loads(value))
            for name, value in (
                line.split("=", 1) for line in output.getvalue().splitlines()
            )
        }

        self.assertEqual(set(values), {"android", "linux", "webkit"})
        self.assertEqual(len(values["linux"]["include"]), 48)
        self.assertEqual(len(values["webkit"]["include"]), 4)
        self.assertEqual(len(values["android"]["include"]), 6)


if __name__ == "__main__":
    _ = unittest.main()
