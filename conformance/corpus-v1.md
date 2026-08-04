# Conformance corpus version 1

Every corpus file is a strict JSON object with exactly `schema_version`, `kind`, and `cases`. Version 1 uses `schema_version: 1`. Implementations reject unknown fields, duplicate case IDs, IDs outside `[a-z0-9._-]+`, unknown operation or message-type tokens, unresolved golden references, uppercase or odd-length hex, non-decimal numeric strings, and unsorted normalized metadata.

Arbitrary text and bytes are lowercase hexadecimal. Unsigned 64-bit values are canonical decimal strings (`0` or a non-zero digit followed by digits); protobuf `uint32` fields must also fit that narrower domain. Metadata is a key-sorted array of `{key_hex,value_hex}`. Production metadata limits are 64 entries, 128 key bytes, 8,192 value bytes, and 65,536 aggregate key-plus-value bytes. Normalized response and stream status values preserve `status_raw` while deriving canonical `status_code`; unsupported raw statuses map to Unknown/2 at public status boundaries. Stream `kind` must match `kind_raw`.

Duplicate protobuf scalar and map fields normalize last-wins. Unknown additive fields are omitted by canonical codec re-encoding, but a known field encoded with an incompatible protobuf wire type is malformed. State operations decode every message body as `StatePayload { bytes body = 3; }`; successful state output is its canonical re-encoding with unknown fields omitted, and field 3 with any wire type other than length-delimited is `malformed_protobuf/13`. Framing decode is byte-preserving: each `bodies_hex` value is the exact length-prefixed body, without protobuf decoding or canonicalization.

The closed operations are `codec.encode`, `codec.decode`, `framing.encode`, `framing.decode_stream`, `state.server_stream`, and `state.client_stream`. Message types are `rpc_request`, `rpc_response`, and `rpc_stream_frame`. Request kind tokens are `unary`, `client_stream`, `server_stream`, and `bidi`; stream kind tokens are `message` and `status`.

`rpc_request` is request-side. `rpc_response`, `rpc_stream_frame`, and typed response payloads are response-side. Direction changes the public status for malformed protobuf and invalid metadata: request-side failures use InvalidArgument/3; response-side failures use Internal/13.

The closed error taxonomy is:

| category                   |  canonical status code |
| -------------------------- | ---------------------: |
| `malformed_protobuf`       | request 3, response 13 |
| `invalid_metadata`         | request 3, response 13 |
| `unsupported_wire_version` |                      9 |
| `unsupported_rpc_kind`     |                      3 |
| `unsupported_frame_kind`   |                      3 |
| `frame_too_large`          |                      8 |
| `incomplete_frame`         |                     13 |
| `remote_status`            |  canonical remote code |
| `missing_terminal_status`  |                     13 |
| `response_cardinality`     |                     13 |
| `trailing_frame`           |                     13 |

Stream state validates every frame and terminal metadata. After a syntactically valid terminal status, the source must reach clean FIN before the terminal result is exposed. Any later frame is `trailing_frame/13`, including after non-OK status. With clean FIN, non-OK remote status takes precedence over response cardinality. With clean OK, server-streaming exposes stable repeated EOF and client-streaming requires exactly one response message. FIN before terminal status is `missing_terminal_status/13`. Server state closes its transport exactly once on every success and error path.

A decode case may carry `peer_allowances`, keyed by suite peer ID, only for an explicitly documented production deviation. `expected_error` remains the language-neutral normative result, and a matching deviation is visible as `status:"allowed"`. Suites declare either `allowance_policy:"forbid"` or `allowance_policy:"peer_specific"`; forbidden suites reject included allowances during loading. Allowances never change expected behavior and must be removed with the runtime defect.

Exact encoded bytes are authoritative when resolved through a golden reference or when a successful case contains at most one normalized metadata entry. Multi-entry resource-limit cases are error-only because protobuf map encoders do not promise a shared entry order.
