---
name: Issue
about: Report a bug, a flaky test, a performance problem or a missing feature.
---

<!--
Title: name the component, then what happens, e.g.
  "Go server: cancelled calls are recorded with the wrong outcome"
  "Rust QMux: dropping a receive stream leaks flow-control credit"
  "Flaky Go test: TestX loses a burst under CPU load"

Labels: add one label for each implementation that is affected or has to change.
An issue that spans several gets all of them (e.g. go + rust).

  trevrpc-go/      Go server, client, QMux, codegen            go
  trevrpc-rust/    Rust server, client, codegen                rust
  trevrpc-js/      browser client, JS QMux, codegen (*.js)     javascript
  trevrpc-js/      type declarations, type tests (*.ts)        typescript
  package.json     npm packaging and dependencies              node
  trevrpc-kotlin/  Android Cronet client                       kotlin
  proto/, *.proto  protocol definitions, generated-code shape  proto
  interop/, bench/ Python harnesses                            python
  *.nix            flake, packaging, checks                    nix
  .forgejo/workflows/, .github/workflows/  CI                  actions
  vulnerabilities                                              security

A JS change that also changes the typings (e.g. a new client option) gets
both `javascript` and `typescript`. These match .forgejo/labeler.yaml, which
labels pull requests the same way. `dep` is reserved for Renovate. If you can't set labels, name the affected areas in
the issue and a maintainer will add them.
-->

## What happens

<!--
For a bug: what you did, what you expected, and what happened instead.
Include the TrevRPC version or commit, the client and server languages, and
the transport (native QUIC, HTTP/3, WebTransport, QMux over TLS, QMux over
WebSocket or HTTP/2). Paste logs, errors or a failing test.

For a feature: what's missing and the use case it blocks.
-->

## Why

<!-- Optional: the root cause if you know it, with file:line references. -->

## Suggested fix

<!-- Optional: how to fix it, alternatives, and the regression test it needs. -->
