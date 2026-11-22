"""Unit tests for erlang_deps. Run with `python3 -I -m unittest discover -s trevrpc-erlang`."""

import hashlib
import io
import tarfile
import tempfile
import unittest
import urllib.error
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest import mock

import erlang_deps
from erlang_deps import Locked

HERE = Path(__file__).parent

GLEAM_TOML = """\
name = "trevrpc"
version = "1.0.0"

[dependencies]
alpha = ">= 1.2.0 and < 1.3.0"
beta_pkg = ">= 0.4.0 and < 0.5.0"
gleam_stdlib = ">= 0.44.0 and < 2.0.0"
"""

REBAR_CONFIG = """\
{minimum_otp_vsn, "29"}.

{deps, [
    {alpha, "~> 1.2.0"},
    %% beta's Hex package is beta_pkg.
    {beta, "~> 0.4.0", {pkg, beta_pkg}}
]}.
"""

PLUGIN_CONFIG = '{{deps, [{{gpb, "{}"}}, protoc_gen_trevrpc_erlang]}}.\n'


def package(name: str, version: str, outer: str, **fields: Any) -> dict[str, Any]:
    return {
        "name": name,
        "version": version,
        "build_tools": ["rebar3"],
        "requirements": [],
        "source": "hex",
        "outer_checksum": outer,
        **fields,
    }


def manifest_toml(packages: list[dict[str, Any]]) -> str:
    def value(item: Any) -> str:
        if isinstance(item, list):
            return "[" + ", ".join(f'"{x}"' for x in item) + "]"  # pyright: ignore[reportUnknownVariableType]
        return f'"{item}"'

    rows = [
        "  { " + ", ".join(f"{key} = {value(item)}" for key, item in p.items()) + " },"
        for p in packages
    ]
    return "packages = [\n" + "\n".join(rows) + "\n]\n"


# alpha and beta_pkg are direct; gamma is beta_pkg's and also alpha's
# dependency's dependency, so rebar3 finds it at its shallowest, level 1.
PACKAGES = [
    package("alpha", "1.2.3", "A0", requirements=["delta"]),
    package("beta_pkg", "0.4.1", "B0", otp_app="beta", requirements=["gamma"]),
    package("delta", "2.0.0", "D0", requirements=["gamma"]),
    package("gamma", "3.0.0", "C0"),
    package("gleam_stdlib", "1.0.5", "E0", build_tools=["gleam"]),
]

LOCKED = [
    Locked("alpha", "alpha", "1.2.3", 0, "A1", "A0"),
    Locked("beta", "beta_pkg", "0.4.1", 0, "B1", "B0"),
    Locked("delta", "delta", "2.0.0", 1, "D1", "D0"),
    Locked("gamma", "gamma", "3.0.0", 1, "C1", "C0"),
]


class Runtime:
    """A runtime directory with the files erlang_deps reads."""

    def __init__(self, root: Path, packages: list[dict[str, Any]], locked: list[Locked]) -> None:
        self.root = root
        for directory in ("interop", "protoc-gen-trevrpc-erlang", *erlang_deps.PLUGINS_ON_ERLANG):
            (root / directory).mkdir()
        (root / "gleam.toml").write_text(GLEAM_TOML)
        (root / "manifest.toml").write_text(manifest_toml(packages))
        (root / "rebar.config").write_text(REBAR_CONFIG)
        (root / "rebar.lock").write_text(erlang_deps.render_lock(locked))
        (root / "interop" / "rebar.lock").write_text("")
        (root / "protoc-gen-trevrpc-erlang" / "rebar.config").write_text(
            PLUGIN_CONFIG.format("~> 5.0.0")
        )
        (root / "protoc-gen-trevrpc-erlang" / "rebar.lock").write_text("the gpb lock\n")
        for plugin in erlang_deps.PLUGINS_ON_ERLANG:
            (root / plugin / "rebar.config").write_text(PLUGIN_CONFIG.format("~> 4.0.0"))
            (root / plugin / "rebar.lock").write_text("")

    def synced(self, offline: bool = False) -> dict[Path, str]:
        return erlang_deps.synced(self.root, offline)


class TestCase(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.tmp = Path(directory.name)
        # No test may reach the network unless it stubs inner_checksum itself.
        patcher = mock.patch.object(erlang_deps, "inner_checksum", side_effect=AssertionError)
        self.inner_checksum = patcher.start()
        self.addCleanup(patcher.stop)

    def runtime(
        self, packages: list[dict[str, Any]] | None = None, locked: list[Locked] | None = None
    ) -> Runtime:
        return Runtime(self.tmp, PACKAGES if packages is None else packages, locked or LOCKED)


class RenderTest(TestCase):
    def test_renders_the_repository_lock_as_rebar3_wrote_it(self) -> None:
        for path in (HERE / "rebar.lock", HERE / "interop" / "rebar.lock"):
            text = path.read_text()
            self.assertEqual(
                erlang_deps.render_lock(list(erlang_deps.read_lock(path).values())), text
            )

    def test_round_trips_through_read_lock(self) -> None:
        path = self.tmp / "rebar.lock"
        path.write_text(erlang_deps.render_lock(LOCKED))
        self.assertEqual(
            sorted(erlang_deps.read_lock(path).values(), key=str), sorted(LOCKED, key=str)
        )


class SyncTest(TestCase):
    def test_locks_each_package_at_its_shallowest_depth_under_its_application(self) -> None:
        # The current lock's levels are wrong; its checksums are reused.
        runtime = self.runtime(locked=[replace(entry, level=9) for entry in LOCKED])
        files = runtime.synced()
        self.assertEqual(files[self.tmp / "rebar.lock"], erlang_deps.render_lock(LOCKED))
        self.inner_checksum.assert_not_called()

    def test_interop_locks_one_level_deeper(self) -> None:
        files = self.runtime().synced()
        interop = self.tmp / "interop" / "rebar.lock"
        interop.write_text(files[interop])
        self.assertEqual(
            {app: entry.level for app, entry in erlang_deps.read_lock(interop).items()},
            {"alpha": 1, "beta": 1, "delta": 2, "gamma": 2},
        )

    def test_copies_gleam_ranges_into_rebar_config(self) -> None:
        files = self.runtime().synced()
        self.assertEqual(
            files[self.tmp / "rebar.config"],
            REBAR_CONFIG.replace('"~> 1.2.0"', '">= 1.2.0 and < 1.3.0"').replace(
                '"~> 0.4.0"', '">= 0.4.0 and < 0.5.0"'
            ),
        )

    def test_gives_the_plugins_on_the_erlang_plugin_its_gpb(self) -> None:
        files = self.runtime().synced()
        for name in ("protoc-gen-trevrpc-gleam", "protoc-gen-trevrpc-elixir"):
            plugin = self.tmp / name
            self.assertEqual(files[plugin / "rebar.config"], PLUGIN_CONFIG.format("~> 5.0.0"))
            self.assertEqual(files[plugin / "rebar.lock"], "the gpb lock\n")

    def test_downloads_the_inner_checksum_of_a_new_version_only(self) -> None:
        packages = [*PACKAGES]
        packages[3] = package("gamma", "3.0.1", "C2")
        runtime = self.runtime(packages)
        self.inner_checksum.side_effect = None
        self.inner_checksum.return_value = "C3"
        files = runtime.synced()
        self.inner_checksum.assert_called_once_with("gamma", "3.0.1", "C2")
        self.assertIn(
            erlang_deps.render_lock(
                [*LOCKED[:3], Locked("gamma", "gamma", "3.0.1", 1, "C3", "C2")]
            ),
            files.values(),
        )

    def test_check_cannot_lock_a_new_version_offline(self) -> None:
        packages = [*PACKAGES]
        packages[0] = package("alpha", "1.2.4", "A2", requirements=["delta"])
        runtime = self.runtime(packages)
        with self.assertRaisesRegex(erlang_deps.Unlocked, "alpha 1.2.4"):
            runtime.synced(offline=True)

    def test_fails_on_a_package_rebar3_cannot_build(self) -> None:
        packages = [*PACKAGES]
        packages[3] = package("gamma", "3.0.0", "C0", requirements=["gleam_stdlib"])
        runtime = self.runtime(packages)
        with self.assertRaisesRegex(
            SystemExit, "gleam_stdlib, which rebar3 would lock, is not built with rebar3"
        ):
            runtime.synced()

    def test_fails_on_a_package_manifest_does_not_lock(self) -> None:
        runtime = self.runtime([p for p in PACKAGES if p["name"] != "delta"])
        with self.assertRaisesRegex(SystemExit, "manifest.toml locks no delta"):
            runtime.synced()

    def test_requires_path_dependencies_by_name_without_locking_them(self) -> None:
        runtime = self.runtime(
            [
                *PACKAGES,
                {
                    "name": "local_dep",
                    "version": "1.0.0",
                    "build_tools": ["gleam"],
                    "requirements": [],
                    "source": "local",
                    "path": "local_dep",
                },
            ]
        )
        (self.tmp / "gleam.toml").write_text(GLEAM_TOML + 'local_dep = { path = "local_dep" }\n')
        config = REBAR_CONFIG.replace("{alpha,", "local_dep,\n    {alpha,")
        (self.tmp / "rebar.config").write_text(config)
        files = runtime.synced()
        self.assertIn("local_dep,\n", files[self.tmp / "rebar.config"])
        self.assertEqual(files[self.tmp / "rebar.lock"], erlang_deps.render_lock(LOCKED))
        (self.tmp / "rebar.config").write_text(REBAR_CONFIG)
        with self.assertRaisesRegex(SystemExit, "takes local_dep from a path, which .* does not"):
            runtime.synced()
        (self.tmp / "gleam.toml").write_text(GLEAM_TOML)
        (self.tmp / "rebar.config").write_text(config)
        with self.assertRaisesRegex(SystemExit, "requires local_dep by name, which gleam.toml"):
            runtime.synced()

    def test_fails_when_gleam_and_rebar3_require_different_packages(self) -> None:
        runtime = self.runtime()
        (self.tmp / "gleam.toml").write_text(GLEAM_TOML + 'epsilon = ">= 1.0.0 and < 2.0.0"\n')
        with self.assertRaisesRegex(SystemExit, "gleam.toml requires epsilon, which .* does not"):
            runtime.synced()
        (self.tmp / "gleam.toml").write_text(GLEAM_TOML.replace("alpha", "zeta"))
        with self.assertRaisesRegex(SystemExit, "requires alpha, which gleam.toml does not"):
            runtime.synced()


def tarball(checksum: str) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as archive:
        data = checksum.encode()
        info = tarfile.TarInfo("CHECKSUM")
        info.size = len(data)
        archive.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


class InnerChecksumTest(unittest.TestCase):
    def setUp(self) -> None:
        patcher = mock.patch.object(erlang_deps.time, "sleep")
        self.sleep = patcher.start()
        self.addCleanup(patcher.stop)

    def respond(self, *responses: bytes | OSError) -> mock.Mock:
        def open_url(url: str, timeout: float) -> Any:
            response = next(iterator)
            if isinstance(response, OSError):
                raise response
            return io.BytesIO(response)

        iterator = iter(responses)
        patcher = mock.patch.object(erlang_deps.urllib.request, "urlopen", side_effect=open_url)
        self.addCleanup(patcher.stop)
        return patcher.start()

    def test_reads_checksum_from_a_tarball_with_the_outer_checksum(self) -> None:
        data = tarball("INNER\n")
        self.respond(data)
        outer = hashlib.sha256(data).hexdigest().upper()
        self.assertEqual(erlang_deps.inner_checksum("quic", "3.1.2", outer), "INNER")

    def test_fails_on_a_tarball_with_another_checksum(self) -> None:
        self.respond(tarball("INNER"))
        with self.assertRaisesRegex(SystemExit, "quic-3.1.2.tar does not have .* checksum BAD"):
            erlang_deps.inner_checksum("quic", "3.1.2", "BAD")

    def test_retries_network_errors_then_names_the_url(self) -> None:
        urlopen = self.respond(*[urllib.error.URLError("unreachable")] * 3)
        with self.assertRaisesRegex(SystemExit, r"downloading https://repo\.hex\.pm/tarballs/quic"):
            erlang_deps.inner_checksum("quic", "3.1.2", "X")
        self.assertEqual(urlopen.call_count, 3)

    def test_retries_until_the_download_works(self) -> None:
        data = tarball("INNER")
        self.respond(urllib.error.URLError("reset"), data)
        outer = hashlib.sha256(data).hexdigest().upper()
        self.assertEqual(erlang_deps.inner_checksum("quic", "3.1.2", outer), "INNER")

    def test_does_not_retry_a_missing_tarball(self) -> None:
        missing = urllib.error.HTTPError("url", 404, "Not Found", mock.Mock(), io.BytesIO())
        self.addCleanup(missing.close)
        urlopen = self.respond(missing)
        with self.assertRaisesRegex(SystemExit, "404"):
            erlang_deps.inner_checksum("quic", "9.9.9", "X")
        self.assertEqual(urlopen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
