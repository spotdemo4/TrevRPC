"""Checks that trevrpc's Gleam packaging is in step with its rebar3 files.

rebar3's files are the source of truth: trevrpc.app.src for the version and
description, rebar.config for the dependencies' ranges, rebar.lock for their
versions. gleam.toml, with which gleam publish builds the Hex package,
manifest.toml, and protoc-gen-trevrpc-gleam must say the same, or the release
bump or a dependency update missed one:

- default.nix, gleam.toml and protoc-gen-trevrpc-gleam have the runtime's
  version, and gleam.toml its description;
- gleam.toml has the ranges rebar.config has, `~> X.Y.Z` as
  `>= X.Y.Z and < X.(Y+1).0`, for the same packages, and besides them only
  the Gleam binding's own dependencies (GLEAM_ONLY);
- manifest.toml locks the Hex packages rebar.lock does, at the same
  versions, and no others built with rebar3;
- protoc-gen-trevrpc-gleam, whose escript carries protoc-gen-trevrpc-erlang's
  modules but builds them with its own dependencies, requires and locks gpb
  as protoc-gen-trevrpc-erlang does, so that it runs the gpb those modules
  are tested with.

Run from the repository's root.
"""

import re
import sys
import tomllib
from pathlib import Path

# The dependencies of the Gleam binding that the runtime's modules do not use,
# which rebar.config leaves out.
GLEAM_ONLY = {"gleam_stdlib"}


def text(path: str) -> str:
    return Path(path).read_text()


def toml(path: str) -> dict:
    return tomllib.loads(text(path))


def one(pattern: str, path: str) -> str:
    matches = re.findall(pattern, text(path), re.MULTILINE)
    if len(matches) != 1:
        sys.exit(f"{path}: expected one match of {pattern!r}, found {len(matches)}")
    return matches[0]


def main() -> None:
    errors = []
    app_src = "trevrpc-erlang/src/trevrpc.app.src"
    version = one(r'\{vsn, "([^"]+)"\}', app_src)
    description = one(r'\{description, "([^"]+)"\}', app_src)
    gleam = toml("trevrpc-erlang/gleam.toml")
    manifest = toml("trevrpc-erlang/manifest.toml")
    packages = {package["name"]: package for package in manifest["packages"]}

    gleam_plugin = "trevrpc-erlang/protoc-gen-trevrpc-gleam"
    versions = {
        "trevrpc-erlang/default.nix": one(r'^  version = "([^"]+)";', "trevrpc-erlang/default.nix"),
        "trevrpc-erlang/gleam.toml": gleam["version"],
        f"{gleam_plugin}/default.nix": one(
            r'^  version = "([^"]+)";', f"{gleam_plugin}/default.nix"
        ),
        "protoc_gen_trevrpc_gleam.app.src": one(
            r'\{vsn, "([^"]+)"\}', f"{gleam_plugin}/src/protoc_gen_trevrpc_gleam.app.src"
        ),
    }
    for path, found in versions.items():
        if found != version:
            errors.append(f"{path} has version {found}, trevrpc.app.src {version}")
    if gleam["description"] != description:
        errors.append(
            f"trevrpc-erlang/gleam.toml has description {gleam['description']!r}, "
            f"trevrpc.app.src {description!r}"
        )

    # {name, "~> X.Y.Z"} or {app, "~> X.Y.Z", {pkg, name}}
    ranges = {}
    for app, requirement, package in re.findall(
        r'\{(\w+), "~> ([0-9.]+)"(?:, \{pkg, (\w+)\})?\}', text("trevrpc-erlang/rebar.config")
    ):
        major, minor, _patch = requirement.split(".")
        ranges[package or app] = f">= {requirement} and < {major}.{int(minor) + 1}.0"
    gleam_ranges = {
        name: requirement
        for name, requirement in gleam.get("dependencies", {}).items()
        if name not in GLEAM_ONLY
    }
    for name in sorted(ranges.keys() | gleam_ranges.keys()):
        want, found = ranges.get(name), gleam_ranges.get(name)
        if want != found:
            errors.append(
                f"trevrpc-erlang/gleam.toml requires {name} {found!r}, rebar.config {want!r}"
            )

    locked = dict(
        re.findall(r'\{pkg,<<"(\w+)">>,<<"([^"]+)">>\}', text("trevrpc-erlang/rebar.lock"))
    )
    rebar3 = {
        name: package["version"]
        for name, package in packages.items()
        if package["source"] == "hex" and package.get("build_tools") == ["rebar3"]
    }
    for name in sorted(locked.keys() | rebar3.keys()):
        if locked.get(name) != rebar3.get(name):
            errors.append(
                f"trevrpc-erlang/manifest.toml locks {name} {rebar3.get(name)}, "
                f"rebar.lock {locked.get(name)}"
            )

    erlang_plugin = "trevrpc-erlang/protoc-gen-trevrpc-erlang"
    gpb = r'\{gpb, "([^"]+)"\}'
    if one(gpb, f"{erlang_plugin}/rebar.config") != one(gpb, f"{gleam_plugin}/rebar.config"):
        errors.append(
            f"{gleam_plugin}/rebar.config requires gpb {one(gpb, f'{gleam_plugin}/rebar.config')}, "
            f"{erlang_plugin}/rebar.config {one(gpb, f'{erlang_plugin}/rebar.config')}"
        )
    if text(f"{erlang_plugin}/rebar.lock") != text(f"{gleam_plugin}/rebar.lock"):
        errors.append(f"{gleam_plugin}/rebar.lock differs from {erlang_plugin}/rebar.lock")

    if errors:
        sys.exit("\n".join(errors))
    print("trevrpc's Gleam packaging matches its rebar3 files")


if __name__ == "__main__":
    main()
