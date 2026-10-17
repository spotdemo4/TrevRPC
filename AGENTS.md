# AGENTS.md

## Task Completion Requirements

- Use focused language-native tests (`go test`, `cargo test`, `npm test`, `gradle check`), `interop/run.sh`, or individual Nix derivation builds while implementation is in progress.
- Do not repeatedly run the full `nix flake check` during intermediate implementation or review stages; it is intentionally reserved for the final completion gate. Run it earlier only when diagnosing a specific Nix integration failure that cannot be isolated more narrowly.
- Once the implementation and focused verification have stabilized, intent-to-add (`git add -N`) every new file so Nix can see it, run `nix fmt`, and run the full `nix flake check` exactly once before considering the task completed.
- `nix fmt` must be used for formatting.

## Version Control Requirements

- Commit messages created by agents must follow [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/).
- Commits created by agents must include an `Assisted-by: [tool name] ([primary model name and version])` Git commit trailer.
- Branch names created by agents must follow [Conventional Branch 1.0.0](https://conventionalbranch.org/v1.0.0/).

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
