"""Checks that trevrpc-gleam's packaging is in step with trevrpc-erlang's.

rebar3's files are the source of truth: trevrpc.app.src for the version,
rebar.config for the dependencies' ranges, rebar.lock for their versions.
trevrpc-erlang/gleam.toml lets Gleam build the runtime, trevrpc-gleam's
manifest.toml locks it by path, and both, with protoc-gen-trevrpc-gleam,
must say the same, or the release bump or a dependency update missed one:

- every version of the Gleam packages is the runtime's;
- trevrpc-erlang/gleam.toml has the ranges rebar.config has, `~> X.Y.Z` as
  `>= X.Y.Z and < X.(Y+1).0`, for the same packages;
- trevrpc-gleam's manifest locks the Hex packages rebar.lock does, at the
  same versions, and no others built with rebar3;
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
    version = one(r'\{vsn, "([^"]+)"\}', "trevrpc-erlang/src/trevrpc.app.src")
    manifest = toml("trevrpc-gleam/manifest.toml")
    packages = {package["name"]: package for package in manifest["packages"]}

    versions = {
        "trevrpc-erlang/gleam.toml": toml("trevrpc-erlang/gleam.toml")["version"],
        "trevrpc-gleam/gleam.toml": toml("trevrpc-gleam/gleam.toml")["version"],
        "trevrpc-gleam/manifest.toml (trevrpc)": packages["trevrpc"]["version"],
        "trevrpc-gleam/default.nix": one(r'^  version = "([^"]+)";', "trevrpc-gleam/default.nix"),
        "trevrpc-gleam/protoc-gen-trevrpc-gleam/default.nix": one(
            r'^  version = "([^"]+)";', "trevrpc-gleam/protoc-gen-trevrpc-gleam/default.nix"
        ),
        "protoc_gen_trevrpc_gleam.app.src": one(
            r'\{vsn, "([^"]+)"\}',
            "trevrpc-gleam/protoc-gen-trevrpc-gleam/src/protoc_gen_trevrpc_gleam.app.src",
        ),
    }
    for path, found in versions.items():
        if found != version:
            errors.append(f"{path} has version {found}, trevrpc.app.src {version}")

    # {name, "~> X.Y.Z"} or {app, "~> X.Y.Z", {pkg, name}}
    ranges = {}
    for app, requirement, package in re.findall(
        r'\{(\w+), "~> ([0-9.]+)"(?:, \{pkg, (\w+)\})?\}', text("trevrpc-erlang/rebar.config")
    ):
        major, minor, _patch = requirement.split(".")
        ranges[package or app] = f">= {requirement} and < {major}.{int(minor) + 1}.0"
    gleam_ranges = toml("trevrpc-erlang/gleam.toml").get("dependencies", {})
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
                f"trevrpc-gleam/manifest.toml locks {name} {rebar3.get(name)}, "
                f"rebar.lock {locked.get(name)}"
            )

    erlang_plugin = "trevrpc-erlang/protoc-gen-trevrpc-erlang"
    gleam_plugin = "trevrpc-gleam/protoc-gen-trevrpc-gleam"
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
    print("trevrpc-gleam's packaging matches trevrpc-erlang's")


if __name__ == "__main__":
    main()
