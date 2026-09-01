# AGENTS.md

## Task Completion Requirements

- Use focused CMake, CTest, sanitizer, installed-consumer, or individual Nix derivation checks while implementation is in progress.
- Do not repeatedly run the full `nix flake check` during intermediate implementation or review stages; it is intentionally reserved for the final completion gate. Run it earlier only when diagnosing a specific Nix integration failure that cannot be isolated more narrowly.
- Once the implementation and focused verification have stabilized, intent-to-add (`git add -N`) every new file so Nix can see it, run `nix fmt`, and run the full `nix flake check` exactly once before considering the task completed.

## Project Snapshot

TrevRPC is a Remote Procedure Call (RPC) framework like gRPC, but uses QUIC as the transport protocol instead of HTTP/2.

This repository is a VERY EARLY WIP. Proposing sweeping changes that improve long-term maintainability is encouraged.

## Core Priorities

1. Performance first.
2. Reliability first.
3. Keep behavior predictable under load and during failures (session restarts, reconnects, partial streams).

If a tradeoff is required, choose correctness and robustness over short-term convenience.

## Maintainability

Long term maintainability is a core priority. If you add new functionality, first check if there is shared logic that can be extracted to a separate module. Duplicate logic across multiple files is a code smell and should be avoided. Don't be afraid to change existing code. Don't take shortcuts by just adding local logic to solve a problem.
