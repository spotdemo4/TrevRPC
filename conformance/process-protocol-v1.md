# Adapter process protocol version 1

Start a peer as `trevrpc-conformance-<peer> --protocol 1`, where the closed Milestone 3 peer IDs are `c`, `cpp`, `go`, `js`, `kotlin`, and `rust`. Standard output is protocol-only NDJSON, standard error is diagnostics, and every event is flushed. The first event is exactly one strict ready object with `schema_version:1`, `event:"ready"`, the configured `peer`, a non-zero numeric `pid`, and exactly these sorted capabilities: `codec.decode`, `codec.encode`, `framing.decode_stream`, `framing.encode`, `state.client_stream`, `state.server_stream`.

Controller input is serial LF-terminated tab-delimited ASCII. It is either `STOP` or `RUN<TAB>sequence<TAB>case_id<TAB>operation<TAB>...`. A command contains at most 262,144 bytes before LF. Sequence numbers and numeric fields are canonical unsigned decimal strings. Variable bytes/text are lowercase even-length hex. Metadata is a decimal count followed by repeated key/value hex fields, sorted by key and without duplicates.

Peers reject missing LF, CR, non-ASCII bytes, noncanonical decimal, uppercase or odd-length hex, invalid IDs, unknown tokens, unsorted or duplicate metadata, missing or extra fields, and commands over the cap. Rejection emits exactly one flushed object containing only `schema_version`, `event:"fatal"`, `peer`, and non-empty `message`, then exits 2. `STOP` exits 0 without another event.

Operation fields:

- `codec.encode`: message type then normalized message fields. Request fields are `service_hex method_hex body_hex metadata_count (key_hex value_hex)* kind version timeout_nanos`; response fields are `status_raw message_hex body_hex metadata_count ...`; stream-frame fields are `kind_raw status_raw message_hex body_hex metadata_count ...`.
- `codec.decode`: `message_type body_hex`.
- `framing.encode`: `message_type max_frame_size` followed by normalized fields.
- `framing.decode_stream`: `message_type max_frame_size chunk_count chunk_hex...`.
- `state.server_stream` and `state.client_stream`: `frame_count frame_body_hex...`; clean EOF after the final scripted frame represents peer FIN. Message frames carry `StatePayload { bytes body = 3; }`. Peers accept valid unknown payload fields but omit them from successful canonical state output; field 3 with a non-length-delimited wire type is `malformed_protobuf/13`.

There is exactly one result per RUN and each result line is at most 65,536 bytes. Common fields are `schema_version`, `event:"result"`, `peer`, decimal-string `sequence`, `case_id`, `operation`, and `outcome`. Codec encode success adds `body_hex` and `frame_hex`; codec decode adds normalized `message` and `canonical_body_hex`; framing decode adds ordered byte-preserving `bodies_hex` and terminal `eof`; server-state success adds ordered events, optional terminal status, and decimal-string `transport_close_count`; client-state success adds `response_body_hex`. Error outcomes add only the closed `category` and numeric `status_code`; every `state.server_stream` error also requires decimal-string `transport_close_count`. Results never contain native categories or diagnostic text.

A controller treats malformed, oversized, extra, mismatched, or late output as a process-protocol failure, kills the process group, fails the current case, and restarts for the next case while the crash budget remains. A structurally valid but semantically wrong result fails only that case.
