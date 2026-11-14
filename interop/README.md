# Interop

Cross-runtime interoperability tests. Every server is exercised by every client
over every transport that client speaks. QUIC is the primary transport; each
client also has a TCP fallback for networks that block UDP.

| Client                   | Primary transport | TCP fallback        | Servers          |
| ------------------------ | ----------------- | ------------------- | ---------------- |
| Go                       | native QUIC       | QMux over TLS       | Go, Rust, Erlang |
| Rust                     | native QUIC       | QMux over TLS       | Go, Rust, Erlang |
| Erlang                   | native QUIC       | QMux over TLS       | Go, Rust, Erlang |
| JavaScript (Chromium)    | WebTransport      | QMux over WebSocket | Go, Rust, Erlang |
| JavaScript (Firefox)     | WebTransport      | QMux over WebSocket | Go, Rust, Erlang |
| Kotlin (Cronet, Android) | HTTP/3            | HTTP/2              | Go, Rust         |
| JavaScript (Safari)      | WebTransport      | QMux over WebSocket | Go, Rust         |

The Erlang runtime ([#477](https://trev.zip/llc/TrevRPC/issues/477)) serves native QUIC, QMux
over TLS, QMux over WebSocket and the HTTP mapping over HTTP/3 and HTTP/2, and calls native QUIC
and QMux over TLS, so far. It takes part in what it implements; see
[Partial runtimes](#partial-runtimes). Browsers reach it over the websocket and fallback lanes.
Its server has no HTTP fallback to serve the app lanes' page, so it leaves its `http3` and
`http2` ports out, and the Android client, which runs every scenario, is not run against it.

`run.py` drives the desktop matrix. `nix run .#trevrpc-interop` runs it with the
packaged Go, Rust and Erlang servers and clients and, on Linux, Chromium and Firefox,
unless the command line names any `--server`, `--client`, or `--browser`, which
then replace all three. `nix flake check` runs that default matrix. CI runs the Android client on an emulator because it needs
KVM (once over HTTP/3 and once over HTTP/2 with QUIC disabled), and runs Safari
on macOS through `safaridriver` (not Playwright's WebKit, whose builds decide on
their own whether to expose WebTransport). Every Safari lane must pass against both servers.

Each cell's output is printed as it finishes and kept in `CELL.log` under
`--out`. A run that fails ends with a summary of every failed cell and the lines
that explain it (its `fail` lines, browser errors, timeouts, or whatever a
crashing client printed), so the reason survives logs cut to their last lines,
as CI and Nix print them.

### Partial runtimes

A runtime that does not implement every transport or scenario yet takes part in
what it does, and what it skips is reported, never counted as passed:

- A server leaves the ports of the transports it does not serve out of its
  readiness line, and `run.py` skips the cells that need them.
- A server that supports only some scenarios lists them in its readiness line,
  and `run.py` passes that list to native and browser clients as `--scenarios`,
  so they skip the rest.
- A client given a transport it does not speak exits `77`, and its cell is
  skipped.
- A client prints `skip <name>: <reason>` for each scenario it does not run.

Only the cells of runtimes named with `--may-skip` (a client, browser or server
name) may skip; a skipped cell or scenario anywhere else fails the run, so a
regression in a complete runtime cannot pass as a skip. The packaged matrix
passes `--may-skip erlang`. Every run that skipped anything ends with a summary
of the skipped cells and of the passing cells that skipped scenarios, and says
which runtimes those skips were allowed for instead of that every cell passed.

## Cells

`run.py` runs one cell per client, server, and transport (`X` is a native client,
`B` a browser, `S` a server):

| Cell                      | Client   | Transport                                                                                       |
| ------------------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `X-to-S`                  | Go, Rust | QUIC, compressing with the interop dictionary                                                   |
| `X-qmux-to-S`             | Go, Rust | QMux over TLS on the server's TCP port, compressing with the interop dictionary                 |
| `X-fallback-to-S`         | Go, Rust | `auto`, with the QUIC address pointing at UDP port 24999, which nothing answers: must fall back |
| `B-webtransport-to-S`     | Browser  | WebTransport                                                                                    |
| `B-websocket-to-S`        | Browser  | QMux over WebSocket (`wss://`)                                                                  |
| `B-fallback-to-S`         | Browser  | `auto`, with the WebTransport URL pointing at UDP port 24999: must fall back to WebSocket       |
| `B-app-webtransport-to-S` | Browser  | WebTransport, from a page the server serves through its HTTP fallback (over HTTP/3 in Chromium) |
| `B-app-websocket-to-S`    | Browser  | QMux over WebSocket, from a page the server serves through its HTTP fallback over HTTP/2        |

Every server holds the interop compression dictionary: the `dict.d1` dictionary of
[`testdata/zstd-golden-vectors.txt`](../testdata/zstd-golden-vectors.txt), which `run.py` writes to a
file and passes as `--dictionary`, for the message types `trevrpc.interop.v1.EchoRequest` and
`trevrpc.interop.v1.EchoResponse`. The native QUIC and QMux cells, the browser webtransport,
websocket and app-webtransport cells, and the Android client pass it to the client too, so their
calls negotiate it and compress every message that shrinks (see
[Compressed Messages](../wiki/Protocol-and-Wire-Format.md#compressed-messages)). The native
fallback cells and the browser fallback and app-websocket cells hold no dictionary and get
uncompressed messages from the same servers.

`--browser-lanes webtransport,websocket,fallback,app-webtransport,app-websocket` selects
which browser lanes run (default: all five); each browser cell is named after its
lane, `B-<lane>-to-S`. Each cell asserts the transport it actually used
(`--expect-transport`), so a cell fails if it connects over the wrong
transport.

The app lanes check that one port serves both a web application and TrevRPC.
`run.py` writes the interop page with `trevrpc-interop-browser --write-page`,
starts each server with `--web-root` on that directory, and has the browser load
the page from the server. The scenarios then run from the server's own origin, to
its WebTransport or WebSocket endpoint.

`B-app-webtransport-to-S` loads the page over HTTP/3 from the server's UDP port in
Chromium, and `B-app-websocket-to-S` over HTTP/2 from its TCP port, as on a network that blocks
UDP. Both assert the protocol (`--page-protocol`). Chromium uses HTTP/3 only when
it trusts the certificate by its public key and is forced to use QUIC for the
origin: it ignores `Alt-Svc` from a connection with certificate errors. Firefox
does not use HTTP/3 with an overridden certificate, so both of its app lanes load
over HTTP/2. Safari's app lanes do not assert a protocol.

## Service

[`proto/interop.proto`](proto/interop.proto) defines `trevrpc.interop.v1.Echo`.
Servers implement it exactly as follows.

| Method         | Behavior                                                                                                                                                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Unary`        | If `fail_code != 0`, fail with status `fail_code` / `fail_message`. Otherwise respond `{message: request.message + suffix, index: 0}`, where `suffix` is the UTF-8 value of request metadata key `interop-suffix`, or empty. |
| `ClientStream` | Read requests until EOF. If a request has `fail_code != 0`, fail immediately with that status. At EOF respond `{message: concatenation of every request.message, index: number of requests}`.                                |
| `ServerStream` | Send `count` responses `{message: request.message, index: i}` for `i` in `0..count`. Then end with status `fail_code` / `fail_message` if `fail_code != 0`, otherwise `OK`.                                                  |
| `Bidi`         | For the `i`-th request: if `fail_code != 0`, end with that status; otherwise send `{message: request.message, index: i}`. End with `OK` at request EOF.                                                                      |

Every failure of `Echo` with `fail_code` 8 (`RESOURCE_EXHAUSTED`) also carries a
[`google.rpc.RetryInfo`](../wiki/Operations-Limits-and-Security.md#rate-limiting) status detail
whose `retry_delay` is `{seconds: 1, nanos: 500000000}`, so that every client reads a retry delay
from every server. A failure with any other code has no details.

[`proto/unreliable.proto`](proto/unreliable.proto) defines
`trevrpc.interop.v1.UnreliableEcho`, whose one method marks both directions
unreliable (see [Datagram Messages](../wiki/Protocol-and-Wire-Format.md#datagram-messages)).
It is a separate file so the gRPC baseline, which compiles only `interop.proto`,
needs no TrevRPC options. Both files import from the include roots
`interop/proto` and `proto`.

| Method                | Behavior                                                                                                                                                                                                                                                                                                                                  |
| --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UnreliableEcho.Bidi` | For each request, send `count` responses `{message: request.message, index: i}` for `i` in `0..count`, as the request arrives. At request EOF end with `OK` and trailing metadata `interop-received` (the number of requests received) and `interop-datagrams-received` (how many of them arrived as datagrams), both as decimal strings. |

[`proto/compressed.proto`](proto/compressed.proto) defines
`trevrpc.interop.v1.CompressedEcho`, whose methods allow
[stream compression](../wiki/Protocol-and-Wire-Format.md#stream-compression)
(`COMPRESSION_STREAM`) or
[compression without a dictionary](../wiki/Protocol-and-Wire-Format.md#compression-without-a-dictionary)
(`COMPRESSION_MESSAGE`), in a separate file for the same reason, and
`trevrpc.interop.v1.RefusingEcho`, whose schema allows it too but which servers
register by hand without stream compression, as a server built from a schema
without `COMPRESSION_STREAM` would. Servers therefore refuse every
[optimistic](../wiki/Protocol-and-Wire-Format.md#optimistic-stream-compression)
call to it, and clients must replay the call.

| Method                        | Behavior                                                                                                                                                                          |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CompressedEcho.Bidi`         | As `Echo.Bidi`, and its `OK` status carries trailing metadata `interop-stream-compressed-received`: how many requests arrived as ZSTD_STREAM_MESSAGE frames, as a decimal string. |
| `CompressedEcho.ClientStream` | As `Echo.ClientStream`, and its `OK` status carries trailing metadata `interop-stream-compressed-received` as `CompressedEcho.Bidi`'s does.                                       |
| `CompressedEcho.Unary`        | As `Echo.Unary`, allowing compression without a dictionary, for the load benchmarks.                                                                                              |
| `CompressedEcho.Plain`        | Echo `{text}` back, ending with `OK` and trailing metadata `interop-plain-compressed-received`: `1` if the request arrived as a ZSTD_MESSAGE without a dictionary, `0` otherwise. |
| `RefusingEcho.ClientStream`   | As `Echo.ClientStream`, registered without stream compression.                                                                                                                    |

[`proto/validated.proto`](proto/validated.proto) defines
`trevrpc.interop.v1.ValidatedEcho`, whose request type carries
[`buf.validate`](https://github.com/bufbuild/protovalidate) rules: `message` must not be empty
(`string.min_len`), and a CEL rule, `message.not_forbidden`, rejects the message `"forbidden"`. It
imports `buf/validate/validate.proto`, of which `proto/buf/validate` holds a copy (protovalidate
v1.2.2). Servers check its requests with protovalidate before the handler runs (see
[Request Validation](../wiki/Go-Guide.md#request-validation)), so a request that breaks a rule
fails with `InvalidArgument` and a `buf.validate.Violations` status detail. It is a separate file
for the same reason as `unreliable.proto`.

| Method                       | Behavior                                                                                                              |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `ValidatedEcho.Unary`        | Respond `{message: request.message, index: 0}`.                                                                       |
| `ValidatedEcho.ClientStream` | Read requests until EOF, then respond `{message: concatenation of every request.message, index: number of requests}`. |

[`proto/validation_bench.proto`](proto/validation_bench.proto) defines
`trevrpc.interop.v1.ValidationBench`, which the load benchmarks call to measure what validation
costs (see [`bench/README.md`](../bench/README.md#validation)). Its request types `BenchRequest`,
`StandardBenchRequest` and `CelBenchRequest` have the same fields; the first has no rules, the
second standard rules, and the third the same standard rules and two CEL rules. Servers check
them as they check `ValidatedEcho`'s, and answer every method as the `ValidatedEcho` method of
its shape: its unary methods (`Unary`, `StandardUnary`, `CelUnary`) as `ValidatedEcho.Unary`, and
its client-streaming methods (`ClientStream`, `StandardClientStream`, `CelClientStream`) as
`ValidatedEcho.ClientStream`. No interop scenario calls it.

Every method also handles headers: when request metadata key `interop-header`
is present with value `v`, the server sends response headers
`{interop-header: v}` before its first response message, and its `OK` status
carries trailing metadata `{interop-trailer: v}`. Without that key the server
sends no headers and no trailing metadata.

## Scenarios

Clients run these in order, each with a 10 second deadline.

| Name                    | Call                                                                                                                                                                                                                                                                                                                     | Expected                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `unary`                 | `Unary {message: "hello"}`                                                                                                                                                                                                                                                                                               | `{message: "hello", index: 0}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `unary-metadata`        | `Unary {message: "hello"}` with metadata `interop-suffix: "!"`                                                                                                                                                                                                                                                           | `{message: "hello!"}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `unary-error`           | `Unary {fail_code: 5, fail_message: "missing"}`                                                                                                                                                                                                                                                                          | status `NotFound` (5), message `"missing"`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `unary-retry-delay`     | `Unary {fail_code: 8, fail_message: "slow down"}`, then `Unary {fail_code: 5, fail_message: "missing"}`                                                                                                                                                                                                                  | `ResourceExhausted` (8) `"slow down"` whose retry delay is 1.5 s; then `NotFound` (5) `"missing"` with no retry delay                                                                                                                                                                                                                                                                                                                                                                                        |
| `unary-large`           | `Unary {message: "x" × 1048576}`                                                                                                                                                                                                                                                                                         | same 1 MiB message echoed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `client-stream`         | `ClientStream` sending `"a"`, `"b"`, `"c"`                                                                                                                                                                                                                                                                               | `{message: "abc", index: 3}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `server-stream`         | `ServerStream {message: "s", count: 3}`                                                                                                                                                                                                                                                                                  | `("s", 0)`, `("s", 1)`, `("s", 2)`, then `OK`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `server-stream-error`   | `ServerStream {message: "s", count: 2, fail_code: 9, fail_message: "stop"}`                                                                                                                                                                                                                                              | `("s", 0)`, `("s", 1)`, then `FailedPrecondition` (9) `"stop"`                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `bidi`                  | `Bidi` sending `"1"`, `"2"`, `"3"`, awaiting each echo before the next send, then closing the send side                                                                                                                                                                                                                  | `("1", 0)`, `("2", 1)`, `("3", 2)`, then `OK`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `bidi-error`            | `Bidi` sending `"1"` (await its echo), then `{fail_code: 10, fail_message: "abort"}`                                                                                                                                                                                                                                     | `("1", 0)`, then `Aborted` (10) `"abort"`; the second send may fail with the same status, since the server can end the call first                                                                                                                                                                                                                                                                                                                                                                            |
| `unary-headers`         | `Unary {message: "hello"}` with metadata `interop-header: "h"`                                                                                                                                                                                                                                                           | `{message: "hello"}`, headers `{interop-header: "h"}`, trailing metadata `{interop-trailer: "h"}`                                                                                                                                                                                                                                                                                                                                                                                                            |
| `server-stream-headers` | `ServerStream {message: "s", count: 2}` with metadata `interop-header: "h"`; read headers before the first message                                                                                                                                                                                                       | headers `{interop-header: "h"}`, `("s", 0)`, `("s", 1)`, then `OK` with trailing metadata `{interop-trailer: "h"}`                                                                                                                                                                                                                                                                                                                                                                                           |
| `unreliable`            | `UnreliableEcho.Bidi` sending `{message: "0", count: 1}` … `{message: "49", count: 1}` without waiting, then, once the first response arrived, `{message: <4096 characters>, count: 1}`, then closing the send side; read every response until the status                                                                | `OK`; every response message is one that was sent, at most once; the 4096-byte echo is present. On a reliable transport (QMux, WebSocket, HTTP/3 and HTTP/2 from Cronet): all 51 responses, `interop-received: 51`, `interop-datagrams-received: 0`. On native QUIC and WebTransport: `interop-received` between 1 and 51 and `interop-datagrams-received` at least 1                                                                                                                                        |
| `compressed`            | `Unary` twice with messages like `{"service":"search","region":"us-east-1","status":404,"latency_ms":100}` (`latency_ms` 100, 101), then `Bidi` sending three such messages (100 … 102) without waiting, then closing the send side                                                                                      | each message echoed, the stream ones with index 0, 1, 2, then `OK`. With the interop dictionary on both sides the messages travel compressed                                                                                                                                                                                                                                                                                                                                                                 |
| `compressed-stream`     | `CompressedEcho.Bidi` sending eight messages like those of `compressed` (`latency_ms` 100 … 107), awaiting each echo before the next send, then closing the send side, with metadata `interop-header: "h"`; then `CompressedEcho.ClientStream` sending the same eight messages without waiting and closing the send side | headers `{interop-header: "h"}`, the bidi messages echoed with index 0 … 7, then `OK` with trailing metadata `interop-trailer: "h"`, since a handler of a method whose requests are stream-compressed sends headers like any other; then `{message: <the eight messages concatenated>, index: 8}` and `OK`. Each `OK` carries trailing metadata `interop-stream-compressed-received` of at least 6: the server received the third message and every later one as ZSTD_STREAM_MESSAGE frames                  |
| `refused-stream`        | `RefusingEcho.ClientStream` sending the eight messages of `compressed-stream` without waiting and closing the send side; then `compressed-stream`'s `CompressedEcho.Bidi` call; then its `CompressedEcho.ClientStream` call                                                                                              | `{message: <the eight messages concatenated>, index: 8}` and `OK`, each message received once: the server refused the optimistic call and the client replayed it. Then the bidi messages echoed with index 0 … 7 and `OK` with `interop-stream-compressed-received` of at least 1, since that call is not optimistic and compresses once the answer arrives; then the client-stream echo and `OK` with `interop-stream-compressed-received` of at least 6, since the answer made the client optimistic again |
| `compressed-plain`      | `CompressedEcho.Plain` twice with `text` the 315-byte JSON record of `plain.records` in [`zstd-golden-vectors.txt`](../testdata/zstd-golden-vectors.txt), twice over (630 characters)                                                                                                                                    | `{text}` echoed and `OK` both times. The second call's `interop-plain-compressed-received` is `1`: the first call's answer let the client compress without a dictionary. The Kotlin client, which never compresses unary requests, sees `0`                                                                                                                                                                                                                                                                  |
| `connection-metadata`   | On a new channel whose connection metadata is `interop-suffix: "!"`: `Unary` with messages `"a"`, `"b"`, `"c"`, awaiting each before the next; then `"d"` with call metadata `interop-suffix: "?"`; then, after replacing the connection metadata with `interop-suffix: "#"`, `"e"`                                      | `{message: "a!"}`, `{message: "b!"}`, `{message: "c!"}`, `{message: "d?"}`, `{message: "e#"}`: the server merges the connection metadata into every call, the later ones referring to the set it confirmed (a client that cannot tell which calls share a connection, such as Kotlin over Cronet, offers it on every call), and the call's own entry wins                                                                                                                                                    |
| `validation`            | `ValidatedEcho.Unary {message: "hello"}`, then `{message: ""}`, then `{message: "forbidden"}`; then `ValidatedEcho.ClientStream` sending `"a"`, then `""`, and closing the send side (the second send may fail with the call's status)                                                                                   | `{message: "hello", index: 0}`; then each of the three calls fails with `InvalidArgument` and a `buf.validate.Violations` detail holding one violation: rule `string.min_len` on the field path `message`, rule `message.not_forbidden` with an empty field path, and rule `string.min_len` on `message`. Go and Rust servers report the same violations, CEL ones included                                                                                                                                  |

Every client runs `compressed-stream`, `refused-stream`, and `compressed-plain`
and holds compression settings for them, so it stream-compresses, and compresses
without a dictionary, on every lane. Clients compress their
requests optimistically from the first call, so nothing waits for the server's
answer; after the refusal in `refused-stream` they stop until a call is answered,
which the bidi call of that scenario is, through the ANSWER the server sends at
dispatch.

The 4096-character message of `unreliable` must not fit a datagram, so it must
not compress below about 1200 bytes either: the Go, Rust and browser clients
send 4096 base64 digits from a fixed xorshift generator. The Android client,
whose Cronet transport has no datagrams, may send `"x"` × 4096.

`unreliable` waits for the first response before the rest, so that at least
one request datagram is known to have arrived before the request ends. The
stream can overtake the datagrams the client still has queued (QUIC stacks send
a DATAGRAM frame only where it fits, and stream data goes ahead of it), and the
server waits for those its DATAGRAM_COUNT announced for at most 10 ms, which a
loaded machine can outlast. The Android client, which has no datagrams to lose,
may send every request at once.

Native clients running `unreliable` over `--transport quic` also check that
their connection received QUIC DATAGRAM frames, which proves the
server-to-client datagram path; `interop-datagrams-received` proves the other
direction for every client.

## Command-line contract

Each runtime ships one interop executable. The browser and Android clients
have their own entry points but follow the same output contract.

### Server

```text
<binary> server --cert <cert.pem> --key <key.pem> [--host 127.0.0.1] [--port 0]
                [--web-root <dir>] [--dictionary <file>]
```

- Serves `Echo` over native QUIC (ALPN `trevrpc/1`), ordinary HTTP/3
  (`POST /trevrpc`), and WebTransport (`/trevrpc`) on a UDP port, and over QMux
  over TLS (ALPN `qmux-02.trevrpc-1`), HTTP/2 (`POST /trevrpc`), and QMux over
  WebSocket (`/trevrpc`, subprotocol `qmux-02.trevrpc-1`) on a TCP port.
- Once every listener is accepting, prints exactly one JSON line to stdout
  and flushes it:
  `{"native":<P>,"http3":<P>,"webtransport":<P>,"qmux":<T>,"http2":<T>,"websocket":<T>}`.
  `P` is the UDP port and `T` the TCP port. The ports within each group are
  equal when one socket serves several protocols. The Go server uses the same
  number for `P` and `T`; the Rust server may use different ones.
- A server that does not serve a transport yet leaves its port out, and one
  that supports only some scenarios lists them under `scenarios`, such as
  `{"native":<P>,"scenarios":["unary","bidi"]}` (see
  [Partial runtimes](#partial-runtimes)). It accepts every flag above even when
  it cannot use one yet.
- With `--dictionary`, holds that zstd dictionary for compressed messages of
  the types `trevrpc.interop.v1.EchoRequest` and
  `trevrpc.interop.v1.EchoResponse`.
- With `--web-root`, serves the files in that directory to every other HTTP
  request, over HTTP/3, HTTP/2 and HTTP/1.1, through the HTTP fallback
  (`/` serves `index.html`).
- Runs until stdin reaches EOF or it receives `SIGTERM`/`SIGINT`, then shuts
  down gracefully and exits `0`. `run.py` closes stdin after the server's cells
  and fails the run unless the server exits `0` within 10 seconds. A server that
  has not exited by then gets `SIGQUIT`, on which a Go server prints every
  goroutine's stack, and is killed 5 seconds later.
- Diagnostics go to stderr: at least the server's warnings and errors, such as
  handlers that outlive their calls, and a line for each failed TLS handshake
  on the TCP listener and each WebSocket upgrade, so that a browser that cannot
  connect shows how far it got. The Rust server also logs when each connection
  opens, what it negotiates, when it closes, and when shutdown starts and
  finishes draining. A connection that held up a shutdown shows as one that
  never closed. A failed cell lists what the server logged while it ran, and a
  server that does not exit cleanly prints the end of its log.

### Client

```text
<binary> client --addr <host:port> --ca <cert.pem> [--server-name localhost]
                [--transport quic|tcp|auto] [--tcp-addr <host:port>]
                [--expect-transport quic|qmux] [--dictionary <file>]
                [--scenarios <name,...>]
```

- `--transport` selects QUIC only (`quic`, the default), QMux over TLS only
  (`tcp`), or QUIC with a QMux fallback (`auto`). `--tcp-addr` is the server's
  TCP address and defaults to `--addr`.
- `--expect-transport` fails the run unless the channel connected over that
  transport.
- `--dictionary` holds that zstd dictionary for compressed messages, as the
  server's flag does.
- Runs every scenario above in order, or with `--scenarios`, only the ones it
  names; an unknown name is a usage error.
- Prints `ok <name>` or `fail <name>: <reason>` per scenario it runs to stdout,
  and `skip <name>: <reason>` for the others, those left out of `--scenarios`
  and those the client does not implement yet.
- Exits `0` only if every scenario it ran passed, and `77` when it does not
  support the `--transport` it was given.

### Browser client

```text
trevrpc-interop-browser --browser chromium|firefox|safari --url <https://host:port/trevrpc>
                        --cert-hash <hex sha256> [--transport webtransport|websocket|auto]
                        [--ws-url <wss://host:port/trevrpc>]
                        [--expect-transport webtransport|websocket] [--dictionary <file>]
                        [--page-url <https://host:port/> --cert <cert.pem> --key <key.pem>
                         [--page-protocol h2|h3]] [--scenarios <name,...>]
trevrpc-interop-browser --write-page <dir>
```

It follows the same output contract. `--transport` defaults to `webtransport`;
`--ws-url` defaults to `--url` with the `wss:` scheme. `--expect-transport`
fails the run unless the channel connected over that transport. `--dictionary`
makes the channel compress Echo messages with that zstd dictionary. `--scenarios` runs only the
scenarios it names, as the native clients' flag does. Chromium and
Firefox ignore certificate errors, because WSS cannot pin a certificate hash.
Safari is driven through `safaridriver`, which needs `sudo safaridriver --enable`
once, and cannot ignore certificate errors (see [Certificates](#certificates)).

The CLI normally serves the page itself from a loopback origin. `--write-page`
writes it to a directory (`index.html` and `page.js`) instead and exits.
`--page-url` then opens it from a server that serves that directory. The page
reports to the CLI at the origin in its `report` query parameter, and the CLI
serves that origin over HTTPS with `--cert` and `--key`, and answers cross-origin
requests from the page. HTTPS keeps an HTTPS page's reports from being blocked as
mixed content. `--page-protocol` has the browser load the page over that HTTP
version and fails every scenario unless the page and its script arrived over it
(their `nextHopProtocol`). Chromium supports `h2` and `h3`, Firefox only `h2`, and
Safari neither.

### Android client

`trevrpc-interop-android` accepts `--transport http3|http2` (default `http3`).
`http2` runs with Cronet's QUIC disabled, so the scenarios use the server's
HTTP/2 mapping on its TCP port. Over either, the server holds the interop
dictionary, which the runner reads from the test APK's golden vectors, and the
instrumented test compresses with it.

### Certificates

`run.py` generates one self-signed ECDSA P-256 certificate valid for 10 days
with SANs `DNS:localhost` and `IP:127.0.0.1`, `CA:FALSE` (rustls rejects a CA
certificate as a server identity) and the `serverAuth` extended key usage (macOS
requires it of TLS server certificates). It is both the server certificate and the
client trust root. Browsers trust it through WebTransport
`serverCertificateHashes`, which requires ECDSA and at most 14 days validity.
The WSS lanes cannot pin a hash, so Chromium and Firefox ignore certificate
errors instead. Safari cannot, so with `--browser safari` `run.py` adds the
certificate to the macOS System keychain as a trusted root through `sudo` and
removes it when the run ends. It never prompts: `sudo` must not need a password,
as on CI runners, or must already be authenticated (`sudo -v`).
