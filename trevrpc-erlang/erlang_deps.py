"""Keeps trevrpc's rebar3 files in step with gleam.toml and manifest.toml.

gleam publish builds the Hex package from gleam.toml, and Renovate's Gleam
manager updates gleam.toml and manifest.toml. Nothing updates rebar3's files,
which the runtime's development build, its Nix package and the interop
executable use, so they take their Hex dependencies from the Gleam files:

- rebar.config requires each package with gleam.toml's range, verbatim, as
  rebar3 reads Hex's requirement syntax, and each of gleam.toml's path
  dependencies, such as qmux, by its bare name, as rebar3 takes those from
  _checkouts, which links them, and never locks them;
- rebar.lock locks what manifest.toml locks for those packages and the ones
  they depend on, at their depth below the runtime, with the outer checksum
  manifest.toml has and the inner one from the package's tarball;
- interop/rebar.lock is that lock one level deeper, as the runtime is a
  checkout there, which rebar3 does not lock;
- protoc-gen-trevrpc-gleam and protoc-gen-trevrpc-elixir, whose escripts carry
  protoc-gen-trevrpc-erlang's modules, require and lock gpb as
  protoc-gen-trevrpc-erlang does.

`sync` rewrites the rebar3 files, downloading the tarballs of the packages
rebar.lock does not lock yet at their versions. `check` fails, without the
network, when sync would change anything. Adding or removing a dependency
takes both gleam.toml and rebar.config. Run from the repository's root, or
test with `python3 -I -m unittest discover -s trevrpc-erlang`.
"""

import hashlib
import io
import re
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, NoReturn

# The dependencies of the Gleam binding that the runtime's modules do not use,
# which rebar.config leaves out.
GLEAM_ONLY = {"gleam_stdlib"}

# The protoc plugins that carry protoc-gen-trevrpc-erlang's modules, and so its gpb.
PLUGINS_ON_ERLANG = ("protoc-gen-trevrpc-gleam", "protoc-gen-trevrpc-elixir")

# {app, "requirement"} or {app, "requirement", {pkg, package}}
DEPENDENCY = re.compile(r'\{(\w+), "([^"]*)"(?:, \{pkg, (\w+)\})?\}')
# A dependency by its bare name, once the others and the comments are gone.
BARE = re.compile(r"\b([a-z]\w*)\b")
COMMENT = re.compile(r"%.*$", re.MULTILINE)
GPB = re.compile(r'\{gpb, "([^"]*)"\}')
LOCKED = re.compile(r'\{<<"(\w+)">>,\{pkg,<<"(\w+)">>,<<"([^"]+)">>\},(\d+)\}')
HASH = re.compile(r'\{<<"(\w+)">>, <<"([0-9A-F]+)">>\}')

# Attempts at downloading a tarball, and the seconds before each retry.
DOWNLOAD_RETRIES = (2, 5)


@dataclass(frozen=True)
class Locked:
    app: str
    package: str
    version: str
    level: int
    inner: str
    outer: str


class Unlocked(Exception):
    """A package that rebar.lock does not lock at its version, whose inner
    checksum only its tarball has."""


def fail(message: str) -> NoReturn:
    sys.exit(f"erlang_deps: {message}")


def synced_config(path: Path, ranges: dict[str, str], local: set[str]) -> tuple[str, list[str]]:
    """rebar.config with gleam.toml's ranges, and the Hex packages it
    requires. The `local` path dependencies must be required by name."""
    text = path.read_text()
    start = text.find("{deps, [")
    end = text.find("]}.", start)
    if start < 0 or end < 0:
        fail(f"{path} has no {{deps, [...]}}.")
    bare = set(
        BARE.findall(DEPENDENCY.sub("", COMMENT.sub("", text[start + len("{deps, [") : end])))
    )
    for name in sorted(bare - local):
        fail(f"{path} requires {name} by name, which gleam.toml does not take from a path")
    for name in sorted(local - bare):
        fail(f"gleam.toml takes {name} from a path, which {path} does not require by name")
    required: list[str] = []

    def with_range(match: re.Match[str]) -> str:
        app, requirement, package = match.groups()
        name = package or app
        if name not in ranges:
            fail(f"{path} requires {name}, which gleam.toml does not")
        required.append(name)
        return match.group(0).replace(f'"{requirement}"', f'"{ranges[name]}"', 1)

    block = DEPENDENCY.sub(with_range, text[start:end])
    for name in sorted(ranges.keys() - set(required)):
        fail(f"gleam.toml requires {name}, which {path} does not")
    return text[:start] + block + text[end:], required


def read_lock(path: Path) -> dict[str, Locked]:
    text = path.read_text()
    before, _, after = text.partition("{pkg_hash_ext,")
    inner, outer = dict(HASH.findall(before)), dict(HASH.findall(after))
    return {
        app: Locked(app, package, version, int(level), inner.get(app, ""), outer.get(app, ""))
        for app, package, version, level in LOCKED.findall(text)
    }


def render_lock(locked: list[Locked]) -> str:
    """A lock as rebar3 writes it."""
    locked = sorted(locked, key=lambda entry: entry.app)
    entries = ",\n ".join(
        f'{{<<"{e.app}">>,{{pkg,<<"{e.package}">>,<<"{e.version}">>}},{e.level}}}' for e in locked
    )
    inner = ",\n ".join(f'{{<<"{e.app}">>, <<"{e.inner}">>}}' for e in locked)
    outer = ",\n ".join(f'{{<<"{e.app}">>, <<"{e.outer}">>}}' for e in locked)
    return (
        f'{{"1.2.0",\n[{entries}]}}.\n'
        f"[\n{{pkg_hash,[\n {inner}]}},\n{{pkg_hash_ext,[\n {outer}]}}\n].\n"
    )


def download(url: str) -> bytes:
    """`url`'s body, retrying after a network or server error."""
    for delay in (*DOWNLOAD_RETRIES, None):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return response.read()
        except OSError as error:
            # A client error, such as a missing tarball, won't go away.
            client = isinstance(error, urllib.error.HTTPError) and error.code < 500
            if delay is None or client:
                fail(f"downloading {url}: {error}")
            time.sleep(delay)
    raise AssertionError("unreachable")


def inner_checksum(package: str, version: str, outer: str) -> str:
    """The inner checksum, from the tarball whose sha256 is `outer`."""
    url = f"https://repo.hex.pm/tarballs/{package}-{version}.tar"
    tarball = download(url)
    if hashlib.sha256(tarball).hexdigest().upper() != outer:
        fail(f"{url} does not have manifest.toml's checksum {outer}")
    try:
        with tarfile.open(fileobj=io.BytesIO(tarball)) as archive:
            member = archive.extractfile("CHECKSUM")
            if member is None:
                fail(f"{url} has no CHECKSUM")
            return member.read().decode().strip()
    except (tarfile.TarError, KeyError) as error:
        fail(f"reading {url}: {error}")


def lock(
    manifest: dict[str, Any], required: list[str], current: dict[str, Locked], offline: bool
) -> list[Locked]:
    """What manifest.toml locks for `required` and their dependencies, each
    at the depth rebar3 finds it at, its shallowest."""
    packages: dict[str, dict[str, Any]] = {p["name"]: p for p in manifest.get("packages", [])}
    levels: dict[str, int] = {}
    frontier, level = sorted(required), 0
    while frontier:
        deeper: list[str] = []
        for name in frontier:
            if name in levels:
                continue
            package = packages.get(name)
            if package is None:
                fail(f"manifest.toml locks no {name}: run `gleam deps download`")
            if package["source"] != "hex":
                fail(f"manifest.toml takes {name} from {package['source']}, not Hex")
            # deps.nix builds every locked package with buildRebar3.
            if "rebar3" not in package.get("build_tools", []):
                fail(f"{name}, which rebar3 would lock, is not built with rebar3")
            levels[name] = level
            deeper.extend(package.get("requirements", []))
        frontier, level = sorted(set(deeper)), level + 1

    locked: list[Locked] = []
    for name, level in levels.items():
        package = packages[name]
        app, version = package.get("otp_app", name), package["version"]
        outer = package["outer_checksum"]
        known = current.get(app)
        same = known and (known.package, known.version, known.outer) == (name, version, outer)
        if known and same and known.inner:
            inner = known.inner
        elif offline:
            raise Unlocked(f"{name} {version}")
        else:
            inner = inner_checksum(name, version, outer)
        locked.append(Locked(app, name, version, level, inner, outer))
    return locked


def synced(root: Path, offline: bool) -> dict[Path, str]:
    """Every rebar3 file this keeps under `root`, the runtime's directory, as
    it should read."""
    gleam = tomllib.loads((root / "gleam.toml").read_text())
    manifest = tomllib.loads((root / "manifest.toml").read_text())
    ranges: dict[str, str] = {}
    local: set[str] = set()
    for name, requirement in gleam.get("dependencies", {}).items():
        if name in GLEAM_ONLY:
            continue
        if isinstance(requirement, dict) and requirement.keys() == {"path"}:
            local.add(name)
        elif isinstance(requirement, str):
            ranges[name] = requirement
        else:
            fail(f"gleam.toml requires {name} from somewhere other than Hex or a path")

    files: dict[Path, str] = {}
    files[root / "rebar.config"], required = synced_config(root / "rebar.config", ranges, local)
    locked = lock(manifest, required, read_lock(root / "rebar.lock"), offline)
    files[root / "rebar.lock"] = render_lock(locked)
    files[root / "interop" / "rebar.lock"] = render_lock(
        [replace(entry, level=entry.level + 1) for entry in locked]
    )

    erlang_plugin = root / "protoc-gen-trevrpc-erlang"
    gpb = GPB.findall((erlang_plugin / "rebar.config").read_text())
    for plugin in PLUGINS_ON_ERLANG:
        config = (root / plugin / "rebar.config").read_text()
        if len(gpb) != 1 or len(GPB.findall(config)) != 1:
            fail("each protoc plugin's rebar.config must require gpb once")
        files[root / plugin / "rebar.config"] = GPB.sub(f'{{gpb, "{gpb[0]}"}}', config)
        files[root / plugin / "rebar.lock"] = (erlang_plugin / "rebar.lock").read_text()
    return files


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in ("sync", "check"):
        sys.exit(f"usage: {sys.argv[0]} sync|check")
    check = sys.argv[1] == "check"
    try:
        files = synced(Path("trevrpc-erlang"), offline=check)
    except Unlocked as unlocked:
        fail(f"rebar.lock does not lock {unlocked}: run `nix run .#sync-erlang-deps`")
    stale = sorted(path for path, text in files.items() if path.read_text() != text)
    if check:
        if stale:
            fail(
                "out of step with gleam.toml and manifest.toml: "
                + ", ".join(map(str, stale))
                + ": run `nix run .#sync-erlang-deps`"
            )
        print("trevrpc's rebar3 files match gleam.toml and manifest.toml")
        return
    for path in stale:
        path.write_text(files[path])
        print(f"updated {path}")
    if not stale:
        print("nothing to update")


if __name__ == "__main__":
    main()
