#!/usr/bin/env python3
"""Validate the immutable bundled MsQuic provider release contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROVIDER = ROOT / "trevrpc-c" / "provider" / "msquic"
MODULE_PATH = "trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
PARENT_PATH = "trev.zip/llc/trevrpc/trevrpc-c"


def fail(message: str) -> None:
    print(f"MsQuic provider release contract: {message}", file=sys.stderr)
    raise SystemExit(1)


def required_match(path: Path, pattern: str, description: str) -> re.Match[str]:
    match = re.search(pattern, path.read_text(), flags=re.MULTILINE)
    if match is None:
        fail(f"{description} is missing from {path.relative_to(ROOT)}")
    return match


def archive_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", help="full Git tag being released")
    args = parser.parse_args()

    version = required_match(
        PROVIDER / "provider.go",
        r'^const Version = "([0-9]+\.[0-9]+\.[0-9]+-trevrpc\.[0-9]+)"$',
        "provider Version constant",
    ).group(1)
    upstream_version, _, revision = version.partition("-trevrpc.")
    if not revision:
        fail("provider Version must contain a -trevrpc.N revision")

    module_file = PROVIDER / "go.mod"
    if (
        required_match(
            module_file, rf"^module {re.escape(MODULE_PATH)}$", "provider module path"
        )
        is None
    ):
        fail("unreachable")
    parent_requirement = required_match(
        module_file,
        rf"^require {re.escape(PARENT_PATH)} (v[^\s]+)$",
        "provider parent module requirement",
    ).group(1)
    if not re.fullmatch(
        r"v[0-9]+\.[0-9]+\.[0-9]+(?:[-.][0-9A-Za-z.-]+)?", parent_requirement
    ):
        fail(f"invalid parent module version {parent_requirement!r}")

    expected_tag = f"trevrpc-c/provider/msquic/v{version}"
    if args.tag is not None and args.tag != expected_tag:
        fail(f"tag {args.tag!r} must equal {expected_tag!r}")

    expected_sums: list[str] = []
    for target in ("linux_amd64", "linux_arm64"):
        archive = PROVIDER / "lib" / target / "libmsquic.a"
        provenance_path = PROVIDER / "provenance" / f"{target}.json"
        record = json.loads(provenance_path.read_text())
        expected_target = target.replace("_", "/", 1)
        checksum = archive_checksum(archive)
        if record.get("providerVersion") != version:
            fail(
                f"{provenance_path.relative_to(ROOT)} providerVersion disagrees with {version}"
            )
        if record.get("version") != upstream_version:
            fail(
                f"{provenance_path.relative_to(ROOT)} version disagrees with {upstream_version}"
            )
        if record.get("target") != expected_target:
            fail(
                f"{provenance_path.relative_to(ROOT)} target must be {expected_target}"
            )
        if record.get("archiveSHA256") != checksum:
            fail(
                f"{provenance_path.relative_to(ROOT)} archiveSHA256 does not match its archive"
            )
        expected_sums.append(
            f"{checksum}  trevrpc-c/provider/msquic/lib/{target}/libmsquic.a"
        )

    actual_sums = (PROVIDER / "provenance" / "SHA256SUMS").read_text().splitlines()
    if sorted(actual_sums) != sorted(expected_sums):
        fail(
            "provenance/SHA256SUMS must contain exactly the committed provider archives"
        )

    dependent_pins = {
        ROOT
        / "trevrpc-go"
        / "go.mod": rf"{re.escape(MODULE_PATH)} v{re.escape(version)}",
        ROOT
        / "trevrpc-go"
        / "go.work": rf"{re.escape(MODULE_PATH)} v{re.escape(version)} =>",
        ROOT
        / "trevrpc-go"
        / "default.nix": rf"{re.escape(MODULE_PATH)}@v{re.escape(version)}=",
        ROOT
        / "trevrpc-c"
        / "tests"
        / "go-provider-spike"
        / "consumer"
        / "go.mod": rf"{re.escape(MODULE_PATH)} v{re.escape(version)}",
    }
    for path, pattern in dependent_pins.items():
        if re.search(pattern, path.read_text()) is None:
            fail(f"{path.relative_to(ROOT)} does not pin provider version {version}")

    print(
        f"validated {MODULE_PATH}@v{version}; parent requirement {parent_requirement}"
    )


if __name__ == "__main__":
    main()
