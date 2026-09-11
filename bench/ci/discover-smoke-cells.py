#!/usr/bin/env python3

import argparse
import json
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import NamedTuple, TextIO, TypedDict, cast


class Cell(TypedDict):
    id: str
    client: str
    server: str
    stack: str


class Peer(TypedDict):
    id: str
    command: list[str]


class Campaign(TypedDict):
    peers: list[Peer]
    cells: list[Cell]


class CampaignSpec(NamedTuple):
    output: str
    target_os: str
    path: Path


class CompatibilityRule(NamedTuple):
    reason: str
    detail: str


CompatibilityKey = tuple[str, str, str, str]
Matrix = dict[str, dict[str, list[dict[str, str]]]]

REPOSITORY_ROOT = Path(__file__).parents[2]
BROWSER_PEERS = frozenset({"chromium", "firefox", "webkit"})
CAMPAIGN_SPECS = (
    CampaignSpec("linux", "linux", Path("bench/campaigns/native-smoke.example.json")),
    CampaignSpec("linux", "linux", Path("bench/campaigns/chromium-smoke.example.json")),
    CampaignSpec("linux", "linux", Path("bench/campaigns/firefox-smoke.example.json")),
    CampaignSpec("webkit", "darwin", Path("bench/campaigns/webkit-smoke.example.json")),
)
NATIVE_CAMPAIGN = Path("bench/campaigns/native-smoke.example.json")
COMPATIBILITY_RULES: dict[CompatibilityKey, CompatibilityRule] = {
    (
        "darwin",
        "webkit",
        "go",
        "trevrpc_webtransport",
    ): CompatibilityRule(
        reason="go_native_transport_linux_only",
        detail=(
            "the bundled Go native transport currently requires cgo on "
            "glibc-based Linux amd64 or arm64"
        ),
    ),
    (
        "darwin",
        "webkit",
        "rust",
        "trevrpc_webtransport",
    ): CompatibilityRule(
        reason="rust_h3_draft02_vs_webkit_network_framework_legacy_profile",
        detail=(
            "the Rust h3 server advertises draft-02 WebTransport settings, while "
            "Cocoa WebKit uses Network.framework's legacy profile; see "
            "https://github.com/hyperium/h3/issues/347"
        ),
    ),
}


def require_string(value: object, description: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{description} must be a non-empty string")
    return value


def load_campaign(root: Path, relative_path: Path) -> Campaign:
    path = root / relative_path
    with path.open(encoding="utf-8") as campaign_file:
        raw_document = cast(object, json.load(campaign_file))
    if not isinstance(raw_document, dict):
        raise TypeError(f"{relative_path} must contain a JSON object")
    document = cast(Mapping[str, object], raw_document)

    peers_value = document.get("peers")
    cells_value = document.get("cells")
    if not isinstance(peers_value, list) or not isinstance(cells_value, list):
        raise TypeError(f"{relative_path} must contain peer and cell arrays")
    raw_peers = cast(list[object], peers_value)
    raw_cells = cast(list[object], cells_value)

    peers: list[Peer] = []
    peer_ids: set[str] = set()
    for index, value in enumerate(raw_peers):
        if not isinstance(value, dict):
            raise TypeError(f"{relative_path} peer {index} must be an object")
        peer = cast(Mapping[str, object], value)
        peer_id = require_string(peer.get("id"), f"{relative_path} peer {index} id")
        command_value = peer.get("command")
        if not isinstance(command_value, list) or not command_value:
            raise ValueError(
                f"{relative_path} peer {peer_id} command must be a non-empty array"
            )
        raw_command = cast(list[object], command_value)
        command = [
            require_string(argument, f"{relative_path} peer {peer_id} command")
            for argument in raw_command
        ]
        if peer_id in peer_ids:
            raise ValueError(f"{relative_path} contains duplicate peer {peer_id}")
        peer_ids.add(peer_id)
        peers.append(Peer(id=peer_id, command=command))

    cells: list[Cell] = []
    cell_ids: set[str] = set()
    for index, value in enumerate(raw_cells):
        if not isinstance(value, dict):
            raise TypeError(f"{relative_path} cell {index} must be an object")
        cell_value = cast(Mapping[str, object], value)
        cell = Cell(
            id=require_string(cell_value.get("id"), f"{relative_path} cell {index} id"),
            client=require_string(
                cell_value.get("client"), f"{relative_path} cell {index} client"
            ),
            server=require_string(
                cell_value.get("server"), f"{relative_path} cell {index} server"
            ),
            stack=require_string(
                cell_value.get("stack"), f"{relative_path} cell {index} stack"
            ),
        )
        if cell["id"] in cell_ids:
            raise ValueError(f"{relative_path} contains duplicate cell {cell['id']}")
        if cell["client"] not in peer_ids:
            raise ValueError(
                f"{relative_path} cell {cell['id']} references missing client {cell['client']}"
            )
        if cell["server"] not in peer_ids:
            raise ValueError(
                f"{relative_path} cell {cell['id']} references missing server {cell['server']}"
            )
        cell_ids.add(cell["id"])
        cells.append(cell)

    return Campaign(peers=peers, cells=cells)


def compatibility_key(target_os: str, cell: Cell) -> CompatibilityKey:
    return (target_os, cell["client"], cell["server"], cell["stack"])


def compatibility_exclusion(target_os: str, cell: Cell) -> CompatibilityRule | None:
    return COMPATIBILITY_RULES.get(compatibility_key(target_os, cell))


def peer_package(peer: str) -> str:
    if peer in BROWSER_PEERS:
        return "trevrpc-browser-bench-peer"
    return f"trevrpc-{peer}"


def expand_campaign(
    root: Path,
    spec: CampaignSpec,
    matched_rules: dict[CompatibilityKey, int],
    seen_cells: set[str],
) -> list[dict[str, str]]:
    campaign = load_campaign(root, spec.path)
    discovered: list[dict[str, str]] = []
    for cell in campaign["cells"]:
        if cell["id"] in seen_cells:
            raise ValueError(f"duplicate generated smoke check name: {cell['id']}")
        seen_cells.add(cell["id"])

        key = compatibility_key(spec.target_os, cell)
        rule = COMPATIBILITY_RULES.get(key)
        if rule is not None:
            matched_rules[key] = matched_rules.get(key, 0) + 1
            print(
                f"excluding {cell['id']} on {spec.target_os}: {rule.reason}: {rule.detail}",
                file=sys.stderr,
            )
            continue
        discovered.append(
            {
                "cell": cell["id"],
                "campaign": spec.path.as_posix(),
                "client_package": peer_package(cell["client"]),
                "server_package": peer_package(cell["server"]),
            }
        )
    return discovered


def validate_rule_matches(matched_rules: Mapping[CompatibilityKey, int]) -> None:
    invalid = [
        (key, matched_rules.get(key, 0))
        for key in COMPATIBILITY_RULES
        if matched_rules.get(key, 0) != 1
    ]
    if invalid:
        details = ", ".join(f"{key!r} matched {count} cells" for key, count in invalid)
        raise ValueError(f"stale or ambiguous smoke compatibility rules: {details}")


def discover(root: Path = REPOSITORY_ROOT) -> Matrix:
    matrices: Matrix = {
        "linux": {"include": []},
        "webkit": {"include": []},
        "android": {"include": []},
    }
    matched_rules: dict[CompatibilityKey, int] = {}
    seen_cells: set[str] = set()
    for spec in CAMPAIGN_SPECS:
        matrices[spec.output]["include"].extend(
            expand_campaign(root, spec, matched_rules, seen_cells)
        )
    validate_rule_matches(matched_rules)

    native = load_campaign(root, NATIVE_CAMPAIGN)
    matrices["android"]["include"].extend(
        {
            "server": peer["id"],
            "server_command": peer["command"][0],
            "server_package": peer_package(peer["id"]),
        }
        for peer in native["peers"]
    )
    return matrices


def write_github_outputs(matrices: Matrix, stream: TextIO) -> None:
    for name in ("linux", "webkit", "android"):
        value = json.dumps(matrices[name], separators=(",", ":"))
        _ = stream.write(f"{name}={value}\n")


def main() -> None:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument(
        "--repository-root",
        type=Path,
        default=REPOSITORY_ROOT,
        help="repository containing bench/campaigns",
    )
    arguments = parser.parse_args()
    repository_root = cast(Path, arguments.repository_root)
    write_github_outputs(discover(repository_root), sys.stdout)


if __name__ == "__main__":
    main()
