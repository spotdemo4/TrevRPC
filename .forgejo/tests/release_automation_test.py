#!/usr/bin/env python3
"""Focused contracts for module release automation.

Hook tests never resolve or mutate the checkout's unreleased Go module graph.
"""

from __future__ import annotations

import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / ".forgejo" / "scripts"


def run(
    *args: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args),
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def json5_to_json(text: str) -> str:
    """Remove the JSON5 comments/trailing commas used by Renovate config."""
    result: list[str] = []
    in_string = False
    escaped = False
    in_comment = False
    index = 0
    while index < len(text):
        char = text[index]
        if in_comment:
            if char == "\n":
                in_comment = False
                result.append(char)
            index += 1
            continue
        if in_string:
            result.append(char)
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            index += 1
            continue
        if char == '"':
            in_string = True
            result.append(char)
        elif char == "/" and index + 1 < len(text) and text[index + 1] == "/":
            in_comment = True
            index += 1
            continue
        else:
            result.append(char)
        index += 1
    return re.sub(r",\s*([}\]])", r"\1", "".join(result))


def test_renovate_config_contract() -> None:
    config_path = ROOT / ".forgejo" / "renovate.json"
    config = json.loads(json5_to_json(config_path.read_text()))
    commands = config["allowedCommands"]
    assert any("update-go-module-artifacts" in command for command in commands)
    custom = config["customManagers"][0]
    assert custom["managerFilePatterns"] == ["/^trevrpc-go\\/go\\.work$/"]
    patterns = "\n".join(custom["matchStrings"])
    assert "trevrpc-c/provider/msquic/v2" in patterns
    assert "\\.\\./trevrpc-c/provider/msquic" in patterns
    assert "\\.\\./trevrpc-c" in patterns
    workspace_rule = next(
        rule
        for rule in config["packageRules"]
        if rule.get("groupSlug") == "trevrpc-workspace-dependency"
    )
    assert "trevrpc-go/go.mod" in workspace_rule["matchFileNames"]
    filters = config["lockFileMaintenance"]["postUpgradeTasks"]["fileFilters"]
    assert "trevrpc-go/go.work" in filters
    assert "trevrpc-go/go.mod" in filters
    assert "trevrpc-c/go.mod" in filters
    assert "trevrpc-c/provider/msquic/go.mod" in filters
    provider_rule = next(
        rule
        for rule in config["packageRules"]
        if rule.get("matchFileNames") == ["trevrpc-c/provider/msquic/go.mod"]
    )
    provider_filters = provider_rule["postUpgradeTasks"]["fileFilters"]
    assert "trevrpc-c/provider/msquic/provider.go" in provider_filters
    assert "trevrpc-go/default.nix" in provider_filters
    assert "trevrpc-c/tests/go-provider-spike/consumer/go.mod" in provider_filters


def test_release_module_seeding_without_source_checkout() -> None:
    script = SCRIPTS / "release-go-module.sh"
    assert script.stat().st_mode & stat.S_IXUSR
    with tempfile.TemporaryDirectory() as directory:
        directory_path = Path(directory)
        curl = directory_path / "curl"
        calls = directory_path / "calls"
        curl.write_text('#!/usr/bin/env bash\nprintf \'%s\\n\' "$@" >> "$CALLS"\n')
        curl.chmod(0o755)

        def seed(tag: str) -> list[str]:
            env = os.environ.copy()
            env.update(
                {
                    "GITHUB_REF_NAME": tag,
                    "GITHUB_REF": f"refs/tags/{tag}",
                    "CURL_BIN": str(curl),
                    "CALLS": str(calls),
                    "GO_PROXY_URL": "https://proxy.invalid",
                    "GO_SUMDB_URL": "https://sumdb.invalid",
                }
            )
            result = run(str(script), env=env)
            assert result.returncode == 0, result.stderr
            return calls.read_text().splitlines()

        parent_calls = seed("trevrpc-c/v0.2.2")
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/@v/v0.2.2.info"
            in parent_calls
        )
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/@v/v0.2.2.mod"
            in parent_calls
        )
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/@v/v0.2.2.zip"
            in parent_calls
        )

        calls.unlink()
        provider_calls = seed("trevrpc-c/provider/msquic/v2.6.0-trevrpc.1")
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2/@v/v2.6.0-trevrpc.1.info"
            in provider_calls
        )
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2/@v/v2.6.0-trevrpc.1.mod"
            in provider_calls
        )
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2/@v/v2.6.0-trevrpc.1.zip"
            in provider_calls
        )
        assert (
            "https://proxy.invalid/trev.zip/llc/trevrpc/trevrpc-c/@v/v0.3.0.info"
            in provider_calls
        )

        invalid = os.environ.copy()
        invalid.update(
            {
                "GITHUB_REF_NAME": "trevrpc-c/provider/msquic/v2.6.0",
                "GITHUB_REF": "refs/tags/trevrpc-c/provider/msquic/v2.6.0",
                "CURL_BIN": str(curl),
                "CALLS": str(calls),
            }
        )
        assert run(str(script), env=invalid).returncode != 0


def test_msquic_provider_release_contract() -> None:
    script = SCRIPTS / "verify-msquic-provider-release.py"
    tag = "trevrpc-c/provider/msquic/v2.6.0-trevrpc.1"
    result = run("python3", str(script), "--tag", tag)
    assert result.returncode == 0, result.stderr

    result = run(
        "python3", str(script), "--tag", "trevrpc-c/provider/msquic/v2.6.0-trevrpc.2"
    )
    assert result.returncode != 0

    release = (SCRIPTS / "release-go-module.sh").read_text()
    assert "verify-msquic-provider-release.py --tag" in release
    assert "wait_for_module" in release
    assert "dependency ready:" in release


def test_artifact_hooks_are_explicit_and_source_independent() -> None:
    script = SCRIPTS / "update-go-module-artifacts.sh"
    with tempfile.TemporaryDirectory() as directory:
        marker = Path(directory) / "marker"
        env = os.environ.copy()
        env["TREVRPC_SKIP_GO_MOD_TIDY"] = "1"
        env["TREVRPC_PROVIDER_VERSION_SOURCE_COMMAND"] = (
            f'printf "%s\\n" "$TREVRPC_MODULE_PATH" > "{marker}"'
        )
        env["TREVRPC_ARTIFACT_UPDATE_COMMAND"] = (
            f'printf "%s\\n" "$TREVRPC_MODULE_DIR" >> "{marker}"'
        )
        result = run(str(script), "trevrpc-c/provider/msquic", env=env)
        assert result.returncode == 0, result.stderr
        assert marker.read_text().splitlines() == [
            "trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2",
            "trevrpc-c/provider/msquic",
        ]


def test_workflow_contracts() -> None:
    release_scripts = [
        (ROOT / ".forgejo" / "workflows" / "release.yaml").read_text(),
        (ROOT / ".github" / "workflows" / "release.yaml").read_text(),
    ]
    for workflow in release_scripts:
        provider = "refs/tags/trevrpc-c/provider/msquic/v2."
        parent = "refs/tags/trevrpc-c/v"
        assert provider in workflow
        assert parent in workflow
        assert workflow.index(provider) < workflow.index(parent)
        assert ".forgejo/scripts/release-go-module.sh" in workflow
        pkg_job = workflow[workflow.index("  pkg:\n") :]
        assert "    needs: check\n" in pkg_job

    vulnerable = [
        (ROOT / ".forgejo" / "workflows" / "vulnerable.yaml").read_text(),
        (ROOT / ".github" / "workflows" / "vulnerable.yaml").read_text(),
    ]
    for workflow in vulnerable:
        for module_dir in ("trevrpc-c", "trevrpc-c/provider/msquic"):
            position = workflow.index(f"working-directory: {module_dir}")
            step = workflow[position : position + 180]
            assert 'GOWORK: "off"' in step
            assert "run: govulncheck ./..." in step


def test_shell_syntax() -> None:
    for path in SCRIPTS.glob("*.sh"):
        result = run("bash", "-n", str(path))
        assert result.returncode == 0, f"{path}: {result.stderr}"


if __name__ == "__main__":
    tests = [value for name, value in globals().items() if name.startswith("test_")]
    for test in tests:
        test()
    print(f"{len(tests)} automation contract tests passed")
