# TrevRPC shared conformance

The Rust controller is the language-neutral authority for corpus validation, process supervision, comparison, and bounded artifacts. Milestone 0 remains a 63-case Go-only diagnostic suite. Milestone 3 defines the shared 90-case process-isolated gate for the lexical peer set `c`, `cpp`, `go`, `js`, `kotlin`, and `rust`, producing 540 result records when all peers are available.

## Run locally

Build each peer, then pass absolute executable paths. M3 intentionally rejects PATH fallback, missing overrides, duplicate or unknown peer IDs, relative paths, non-regular files, and non-executable files.

```sh
cargo run --locked --manifest-path bench/Cargo.toml --bin trevrpc-conformance -- \
  validate conformance/suites/m3.json

cargo run --locked --manifest-path bench/Cargo.toml --bin trevrpc-conformance -- \
  run conformance/suites/m3.json --out /tmp/trevrpc-conformance-m3 \
  --peer c=/absolute/path/trevrpc-conformance-c \
  --peer cpp=/absolute/path/trevrpc-conformance-cpp \
  --peer go=/absolute/path/trevrpc-conformance-go \
  --peer js=/absolute/path/trevrpc-conformance-js \
  --peer kotlin=/absolute/path/trevrpc-conformance-kotlin \
  --peer rust=/absolute/path/trevrpc-conformance-rust
```

The run writes `manifest.json`, `results.jsonl`, `summary.json`, bounded raw peer output under `raw/<peer>/attempt-NNNN.stdout|stderr`, and exact replay inputs under `inputs/`. The manifest records replayable artifact-relative suite, corpus, and golden paths with hashes; canonical executable paths and hashes; source identity; resolution and allowance policies; and all byte/time/crash limits.

## Suites

- `suites/m0.json`: 63 cases, Go only, practical PATH-or-absolute-override resolution, peer-specific diagnostic allowances.
- `suites/m3.json`: 90 cases, six exact absolute overrides, `allowance_policy:"forbid"`, 262,144-byte commands, 65,536-byte events, bounded retained output, one allowed process crash, and the four M0 corpora plus `m3-normative.json` and `resource-limits.json`.

The Linux `checks.conformance-m3` first preserves the unchanged M0 baseline at 63/63 against Go, then runs the canonical M3 suite unchanged against all six packaged peers. M3 requires 90 passes per peer, 540 passes overall, zero allowed/failed/skipped results, clean shutdown, absolute hashed executables, and hash-verified replay inputs.

## Authority and scope

`../testdata/wire-golden-vectors.txt` remains the byte oracle for valid deterministic encodings. Malformed, normalization, framing, resource-limit, and stream-state expectations live in the versioned corpus. Adapters must use production codec, framing, metadata, status, and state paths; private or hidden scripted seams are acceptable only where production lacks an injectable source.

M3 is runtime conformance, not transport interoperability. It does not publish process fields or peer packages, change the C ABI, redesign public async APIs, migrate Rust to later APIs, stabilize JavaScript exports, add Kotlin Maven/Android work, or test network ordering.
