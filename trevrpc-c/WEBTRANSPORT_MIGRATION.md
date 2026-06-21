# Native WebTransport Migration

This tracks replacing the `libwtf` dependency with an internal WebTransport implementation that fits TrevRPC's MsQuic transport model.

## Goals

- Remove the external `libwtf` dependency from `trevrpc-c`.
- Keep the public `trevrpc_wt_*` API stable while internals are replaced.
- Serve native QUIC and WebTransport from one logical `trevrpc_server` and, eventually, one shared MsQuic listener where practical.
- Support high-level unary and streaming RPCs over WebTransport with the same wire frames and runtime policy as native QUIC.

## Non-Goals For The First Cut

- Browser-grade full HTTP/3 feature coverage.
- WebTransport datagrams.
- Multiple WebTransport sessions per QUIC connection unless required by tests.
- Draft-15 support before draft-07 parity is stable.
- Replacing MsQuic itself.

## Implementation Plan

1. Remove the `libwtf` build dependency.
2. Replace `src/trevrpc_webtransport.c` with an internal implementation skeleton that preserves all exported `trevrpc_wt_*` symbols.
3. Keep unsupported paths explicit with stable `TREV_WT_ERR_*` errors while functionality is rebuilt.
4. Add a minimal HTTP/3 layer over MsQuic:
   - ALPN `h3`.
   - Control stream setup.
   - SETTINGS encode/decode for WebTransport support.
   - Minimal QPACK/HEADERS support for CONNECT requests and responses.
5. Add server-side WebTransport session handling:
   - Validate path/origin from `trevrpc_wt_config`.
   - Accept CONNECT `:protocol = webtransport`.
   - Queue accepted sessions through `trevrpc_wt_listener_accept_session`.
6. Add WebTransport bidirectional stream handling:
   - Map accepted bidirectional WebTransport streams to `trevrpc_wt_stream`.
   - Preserve existing blocking read/write/frame APIs.
   - Propagate resets/close as `TREV_WT_ERR_CLOSED`.
7. Add client-side WebTransport support:
   - Open HTTP/3 connection with `h3` ALPN.
   - Send CONNECT request for configured URL/path/origin.
   - Open bidirectional WebTransport streams.
8. Add integration coverage:
   - Native WebTransport unary CTest.
   - Server-streaming, client-streaming, and bidirectional streaming CTests.
   - Shutdown/reset/partial stream failure cases.
9. Remove obsolete libwtf-specific documentation and draft workaround text.

## Current Status

- Multi-listener high-level `trevrpc_server` support exists.
- `trevrpc_wt_*` is still backed by `libwtf` until this migration is completed.
- Native WebTransport round-trip CTests are blocked on the internal implementation.

## Risks

- HTTP/3 and QPACK correctness under malformed peers.
- Flow-control behavior under load.
- Compatibility with browser WebTransport implementations.
- Keeping shutdown deterministic while QUIC streams/sessions are being re-routed through the high-level runtime.
