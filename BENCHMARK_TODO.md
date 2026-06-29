# Benchmark Apples-To-Apples TODO

Goal: make the published trevRPC, gRPC, and ConnectRPC benchmark comparisons representative of a production RPC path:

`structured data -> serialization -> encryption -> wire -> decryption -> deserialization -> structured data`

The current benchmark tables are useful, but they mix encrypted trevRPC QUIC/WebTransport rows with plaintext gRPC/ConnectRPC baselines and use very small payloads. Some C throughput paths also pre-encode repeated payload bytes. Treat the current tables as transport/framework microbenchmarks until the items below are complete.

## 1. Document The Current Baseline

- Add an explicit "Security and payload model" subsection to `wiki/Benchmarks.md` for each benchmark family.
- State that trevRPC split and WebTransport rows use QUIC/WebTransport encryption, usually with local self-signed certificates and skipped or pinned verification.
- State that current Go gRPC, Rust tonic, and browser ConnectRPC baseline rows are plaintext.
- State that connection setup and handshake are outside the measured steady-state samples because the clients are warmed before timing.
- State the default payload shape and size: one protobuf message with a single string field, usually `"TrevRPC benchmark"` or a count string.
- State which rows pre-encode repeated payload bytes, especially C throughput paths.

## 2. Add TLS To gRPC Baselines

- Update `trevrpc-go/cmd/trevrpc-rpc-split-bench/main.go` so `-transport grpc` can run with TLS.
- Reuse the existing benchmark certificate files passed by `bench/run_rpc_split.sh`.
- Replace `grpc.WithTransportCredentials(insecure.NewCredentials())` with TLS credentials when cert paths are provided.
- Keep a deliberate plaintext option only if it is separately named and excluded from apples-to-apples tables.
- Update `trevrpc-rust/examples/rpc_split_bench.rs` so tonic can run over TLS.
- Reuse the same certificate generation or file-passing pattern used by the trevRPC QUIC paths.
- Prefer self-signed local certificates with verification pinned to the generated cert, not a production CA requirement.
- Ensure the measured sample still excludes handshake/setup unless a separate handshake benchmark is intentionally added.
- Add a clear failure if a gRPC baseline row is requested without TLS in apples-to-apples mode.

## 3. Add TLS To ConnectRPC Baselines

- Update the Go ConnectRPC benchmark server in `trevrpc-go/cmd/trevrpc-rpc-split-bench/main.go` to support HTTPS.
- Update `bench/run_webtransport.sh` to start the ConnectRPC baseline with cert/key paths.
- Update `trevrpc-js/bench/webtransport_browser.js` so ConnectRPC fetches can target `https://127.0.0.1:...`.
- Make browser trust behavior explicit. Prefer launching Chromium with a scoped benchmark-only trust mechanism or cert exception that does not affect the user profile.
- If browser HTTPS trust cannot be made robust, keep ConnectRPC plaintext in a separately labeled table and do not compare it directly against encrypted WebTransport rows.

## 4. Make Serialization Work Comparable

- Audit every benchmark shape for whether each request/response message is encoded from a structured object inside the measured section.
- Remove or gate C pre-encoded repeated-body fast paths when running apples-to-apples mode.
- For C throughput rows, encode each repeated `HelloRequest` and `HelloReply` from protobuf-c structures unless the table is explicitly labeled as a wire/pre-encoded throughput benchmark.
- Keep decode validation in the measured receive path for every implementation.
- Avoid optimizing away reply bodies; continue checking fields and accumulating sinks where needed.
- Add a report field that records whether payloads were `per-message-serialized` or `pre-encoded-reused`.

## 5. Add Realistic Payload Profiles

- Add benchmark settings for payload profiles instead of only the current tiny single-string message.
- Suggested profiles:
- `tiny`: current single string payload for microbenchmark continuity.
- `small`: several scalar fields plus short strings, roughly 256 B encoded.
- `medium`: nested/repeated fields and metadata, roughly 4 KiB encoded.
- `large`: repeated bytes/string fields, roughly 64 KiB encoded.
- `mixed`: deterministic sequence of varied request sizes to exercise allocator and framing behavior.
- Keep payload contents deterministic so results are reproducible.
- Make every implementation use the same protobuf schema and values for each profile.
- Update generated bindings where needed for C, Go, Rust, and JS.
- Record the selected payload profile and approximate encoded request/response bytes in `wiki/Benchmarks.md`.

## 6. Include Metadata And Status Overhead

- Add an optional metadata profile to measure common production headers.
- Include representative user metadata, deadline/timeout settings, and a small response metadata field.
- Ensure gRPC and trevRPC carry comparable metadata semantics.
- Keep the default apples-to-apples profile simple, but document whether metadata is enabled.

## 7. Separate Steady-State From Handshake Benchmarks

- Keep the existing warmed steady-state benchmark family.
- Add a separate handshake/connect benchmark only if useful.
- For handshake benchmarks, measure:
- client construction/dial/connect,
- TLS handshake,
- first unary RPC,
- clean close.
- Report handshake results separately from steady-state RPC tables.
- Do not mix handshake-inclusive and warmed rows in the same sorted table.

## 8. Normalize Transport Settings

- Review transport settings that materially affect throughput and latency.
- Document stream limits, idle timeouts, keepalive, TCP_NODELAY, QUIC flow control, batching, and buffering.
- Make settings equivalent where possible.
- Where equivalence is impossible, record the difference in the report.
- Decide whether benchmark-specific batching is part of the production representative profile or belongs in a separate optimized-throughput profile.

## 9. Improve Report Generation

- Extend `bench/run_rpc_split.sh` and `bench/run_webtransport.sh` to emit a configuration summary that includes:
- transport security mode,
- certificate verification mode,
- payload profile,
- encoded request/response sizes,
- serialization mode,
- metadata profile,
- handshake inclusion mode,
- batching settings.
- Add labels such as `encrypted`, `plaintext`, `tls-pinned`, `tls-skip-verify`, `per-message-serialized`, and `pre-encoded-reused` to normalized CSV rows.
- Avoid inserting plaintext baseline rows into encrypted apples-to-apples tables unless the table explicitly says it is not apples-to-apples.
- Preserve raw command output and command logs as the scripts do today.

## 10. Add Guardrail Tests

- Add tests or script assertions that apples-to-apples mode refuses plaintext gRPC/ConnectRPC baselines.
- Add tests that report metadata accurately reflects selected TLS and serialization modes.
- Add a small smoke benchmark with `SPLIT_ITERATIONS=10` and `WEBTRANSPORT_ITERATIONS=10` to catch broken benchmark wiring.
- Ensure new generated protobuf schemas are checked by existing language test suites.

## 11. Rerun And Publish New Tables

- Run benchmarks on the `bench` host following the `ssh-bench` workflow.
- Build and dependency setup must happen outside measured commands.
- Record host context, commit/tree state, commands, environment, settings, failed samples, and anomalies.
- Update only the relevant `wiki/Benchmarks.md` sections after each run.
- Keep old history or notes for previous non-apples-to-apples runs so readers can understand why results changed.

## Suggested Execution Order

1. Update report documentation to label current limitations.
2. Add TLS support to Go gRPC and Rust tonic split baselines.
3. Add report fields and guardrails for security mode.
4. Add payload profile support and remove/gate pre-encoded C throughput shortcuts.
5. Add HTTPS ConnectRPC support or split it into a clearly labeled plaintext baseline table.
6. Add smoke tests for benchmark wiring.
7. Run focused low-iteration validation locally or on `bench`.
8. Run full benchmark suites on `bench` and update `wiki/Benchmarks.md`.

## Completion Criteria

- Encrypted apples-to-apples tables contain only rows that encrypt data in transit.
- Every included row serializes structured request data before sending and deserializes response data after receiving inside the measured operation.
- Payload profile and metadata profile are identical across compared implementations.
- The report clearly distinguishes steady-state RPC results from handshake-inclusive results.
- `nix fmt` and `nix flake check` pass.
