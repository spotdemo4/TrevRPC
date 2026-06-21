# Native WebTransport Migration

This tracks replacing the `libwtf` dependency with an internal WebTransport implementation that fits TrevRPC's MsQuic transport model.

## Goals

- Remove the external `libwtf` dependency from `trevrpc-c`.
- Keep the public `trevrpc_wt_*` API stable while internals are replaced.
- Serve native QUIC and WebTransport from one logical `trevrpc_server` and, where desired, one shared MsQuic listener.
- Support high-level unary and streaming RPCs over WebTransport with the same wire frames and runtime policy as native QUIC.

## Non-Goals For The First Cut

- Browser-grade full HTTP/3 feature coverage.
- WebTransport datagrams.
- Multiple WebTransport sessions per QUIC connection unless required by tests.
- Draft-15 support before draft-07 parity is stable.
- Replacing MsQuic itself.

## Completed Work

- Removed the `libwtf` build dependency.
- Replaced `src/trevrpc_webtransport.c` with an internal MsQuic-backed implementation while preserving the exported `trevrpc_wt_*` API shape.
- Added the TrevRPC-focused HTTP/3 subset over MsQuic:
  - ALPN `h3`.
  - Control stream setup.
  - SETTINGS encode/decode for WebTransport support.
  - Minimal literal-only QPACK/HEADERS support for CONNECT requests and responses.
- Added server-side WebTransport session handling:
  - Validate path/origin from `trevrpc_wt_config`.
  - Accept CONNECT `:protocol = webtransport`.
  - Queue accepted sessions through `trevrpc_wt_listener_accept_session`.
- Added WebTransport bidirectional stream handling:
  - Map accepted bidirectional WebTransport streams to `trevrpc_wt_stream`.
  - Preserve existing blocking read/write/frame APIs.
  - Propagate resets/close as `TREV_WT_ERR_CLOSED`.
- Added client-side WebTransport support:
  - Open HTTP/3 connection with `h3` ALPN.
  - Send CONNECT request for configured URL/path/origin.
  - Open bidirectional WebTransport streams.
- Added integration coverage for native WebTransport unary, server-streaming, client-streaming, bidirectional streaming, and low-level shutdown/close unblock behavior.
- Added malformed-peer coverage for HTTP/3 control stream type, missing/malformed SETTINGS, malformed QPACK blocks, and invalid CONNECT pseudo-headers.
- Added high-level WebTransport partial request close failure coverage.
- Added shared MsQuic listener support that routes native TrevRPC and WebTransport connections by negotiated ALPN.
- Removed obsolete libwtf-specific documentation and draft workaround text.

## Current Status

- Multi-listener high-level `trevrpc_server` support exists.
- `trevrpc_wt_*` is backed by the internal MsQuic/HTTP3/WebTransport implementation.
- `trevrpc_server_listen` serves native QUIC and WebTransport on one UDP port.
- High-level WebTransport RPC CTests cover unary, all streaming shapes, partial requests, and close failure paths.
- Low-level WebTransport CTests cover session establishment, stream I/O, malformed handshakes, path rejection, and shutdown/close unblock behavior.

## Remaining Work

- None for the first-cut native WebTransport migration.

## Risks

- HTTP/3 and QPACK correctness under malformed peers.
- Flow-control behavior under load.
- Compatibility with browser WebTransport implementations.
- Keeping shutdown deterministic while QUIC streams/sessions are being re-routed through the high-level runtime.
