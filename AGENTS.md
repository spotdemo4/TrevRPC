# AGENTS.md

## Task Completion Requirements

- Use focused language-native tests (`go test`, `cargo test`, `npm test`, `gradle check`), `interop/run.py`, or individual Nix derivation builds while implementation is in progress.
- Do not repeatedly run the full `nix flake check` during intermediate implementation or review stages; it is intentionally reserved for the final completion gate. Run it earlier only when diagnosing a specific Nix integration failure that cannot be isolated more narrowly.
- Once the implementation and focused verification have stabilized, intent-to-add (`git add -N`) every new file so Nix can see it, run `nix fmt`, and run the full `nix flake check` exactly once before considering the task completed.
- `nix fmt` must be used for formatting.
- When a test fails in a way your change can't explain (for example, it passes on a rerun of the same code, or fails in code you didn't touch), treat it as flaky and file an issue straight away, without asking first. Search open issues first. If one already covers the test, comment on it with the new occurrence instead of filing another. Either way, include the test name, the command you ran, the commit, the failure output, how often it failed across reruns, and anything relevant about the environment, such as machine load or running under `nix flake check`. Don't disable, skip or loosen the test to get it passing, and don't fix it in the current change unless the change caused it. Mention the issue in your final summary.

## Version Control Requirements

- Commit messages created by agents must follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).
- Commits created by agents must include an `Assisted-by: [tool name] ([primary model name and version])` Git commit trailer.
- Branch names created by agents must follow [Conventional Branch 1.0.0](https://conventionalbranch.org/v1.0.0/).
- Issues created by agents must follow `.forgejo/ISSUE_TEMPLATE/issue.md`, including its title format and labeling instructions.
- `wiki/` is a Git submodule for the project wiki. When an agent changes it, commit and push those changes in the submodule to its `main` branch, then commit the updated submodule pointer in this repository along with the related changes.

## Template Update Requirements

- If `.template.json` exists, this project was created from the template at `url`, and `commit` is the latest template commit whose changes have been applied.
- To update from the template, port each commit in `<commit>..main` of the template to this project, adapting names, URLs and paths. Skip dependency updates (`chore(deps)`) that this project's Renovate already handles, and changes to `init.sh`, `init.test.sh`, `template.schema.json`, the `templates` flake output and the `init` check, which are not part of this project.
- Set `commit` to the template commit that was synced to in the same commit that applies its changes.

## Project Snapshot

TrevRPC is a Remote Procedure Call (RPC) framework like gRPC, but uses QUIC as the transport protocol instead of HTTP/2.

Scope is deliberately small: Go and Rust servers (native QUIC, HTTP/3, WebTransport), Go and Rust native-QUIC clients, a browser WebTransport client in JavaScript, and an Android Cronet HTTP/3 client in Kotlin. QUIC is the primary transport; TCP fallbacks cover networks that block UDP and browsers without WebTransport: QMux over TLS (native clients), HTTP/2 (Cronet), and QMux over WebSocket (browsers), all served from one TCP listener. Each language uses exactly one QUIC stack and one QMux implementation (in-tree for Go and JavaScript, the qmux crate for Rust). `interop/` tests every client against every server.

This repository is a VERY EARLY WIP. Proposing sweeping changes that improve long-term maintainability is encouraged.

## Core Priorities

1. Performance first.
2. Reliability first.
3. Keep behavior predictable under load and during failures (session restarts, reconnects, partial streams).

If a tradeoff is required, choose correctness and robustness over short-term convenience.

## Maintainability

Long term maintainability is a core priority. If you add new functionality, first check if there is shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.
