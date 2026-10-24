# Interop

Cross-runtime interoperability tests. Every server is exercised by every client
over every transport that client speaks. QUIC is the primary transport; each
client also has a TCP fallback for networks that block UDP.

| Client                   | Primary transport | TCP fallback        | Servers  |
| ------------------------ | ----------------- | ------------------- | -------- |
| Go                       | native QUIC       | QMux over TLS       | Go, Rust |
| Rust                     | native QUIC       | QMux over TLS       | Go, Rust |
| JavaScript (Chromium)    | WebTransport      | QMux over WebSocket | Go, Rust |
| JavaScript (Firefox)     | WebTransport      | QMux over WebSocket | Go, Rust |
| Kotlin (Cronet, Android) | HTTP/3            | HTTP/2              | Go, Rust |
| JavaScript (Safari)      | WebTransport      | QMux over WebSocket | Go, Rust |

`run.py` drives the desktop matrix. `nix run .#trevrpc-interop` runs it with the
packaged Go and Rust servers and clients and, on Linux, Chromium and Firefox,
unless the command line names any `--server`, `--client`, or `--browser`, which
then replace all three. `nix flake check` runs that default matrix. CI runs the Android client on an emulator because it needs
KVM (once over HTTP/3 and once over HTTP/2 with QUIC disabled), and runs Safari
on macOS through `safaridriver` (not Playwright's WebKit, whose builds decide on
their own whether to expose WebTransport). Every Safari lane must pass against both servers.

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

[`proto/unreliable.proto`](proto/unreliable.proto) defines
`trevrpc.interop.v1.UnreliableEcho`, whose one method marks both directions
unreliable (see [Datagram Messages](../wiki/Protocol-and-Wire-Format.md#datagram-messages)).
It is a separate file so the gRPC baseline, which compiles only `interop.proto`,
needs no TrevRPC options. Both files import from the include roots
`interop/proto` and `proto`.

| Method                | Behavior                                                                                                                                                                                                                                                                                                                                                              |
| --------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `UnreliableEcho.Bidi` | For each request, send `count` responses `{message: request.message, index: i}` for `i` in `0..count`, either as the request arrives or after request EOF. At request EOF end with `OK` and trailing metadata `interop-received` (the number of requests received) and `interop-datagrams-received` (how many of them arrived as datagrams), both as decimal strings. |

[`proto/compressed.proto`](proto/compressed.proto) defines
`trevrpc.interop.v1.CompressedEcho`, whose methods allow
[stream compression](../wiki/Protocol-and-Wire-Format.md#stream-compression)
(`compress_stream`), in a separate file for the same reason, and
`trevrpc.interop.v1.RefusingEcho`, whose schema allows it too but which servers
register by hand without stream compression, as a server built from a schema
without `compress_stream` would. Servers therefore refuse every
[optimistic](../wiki/Protocol-and-Wire-Format.md#optimistic-stream-compression)
call to it, and clients must replay the call.

| Method                        | Behavior                                                                                                                                                                          |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `CompressedEcho.Bidi`         | As `Echo.Bidi`, and its `OK` status carries trailing metadata `interop-stream-compressed-received`: how many requests arrived as ZSTD_STREAM_MESSAGE frames, as a decimal string. |
| `CompressedEcho.ClientStream` | As `Echo.ClientStream`, and its `OK` status carries trailing metadata `interop-stream-compressed-received` as `CompressedEcho.Bidi`'s does.                                       |
| `RefusingEcho.ClientStream`   | As `Echo.ClientStream`, registered without stream compression.                                                                                                                    |

Every method also handles headers: when request metadata key `interop-header`
is present with value `v`, the server sends response headers
`{interop-header: v}` before its first response message, and its `OK` status
carries trailing metadata `{interop-trailer: v}`. Without that key the server
sends no headers and no trailing metadata.

## Scenarios

Clients run these in order, each with a 10 second deadline.

| Name                    | Call                                                                                                                                                                                                                                                                                | Expected                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `unary`                 | `Unary {message: "hello"}`                                                                                                                                                                                                                                                          | `{message: "hello", index: 0}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `unary-metadata`        | `Unary {message: "hello"}` with metadata `interop-suffix: "!"`                                                                                                                                                                                                                      | `{message: "hello!"}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `unary-error`           | `Unary {fail_code: 5, fail_message: "missing"}`                                                                                                                                                                                                                                     | status `NotFound` (5), message `"missing"`                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| `unary-large`           | `Unary {message: "x" × 1048576}`                                                                                                                                                                                                                                                    | same 1 MiB message echoed                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `client-stream`         | `ClientStream` sending `"a"`, `"b"`, `"c"`                                                                                                                                                                                                                                          | `{message: "abc", index: 3}`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| `server-stream`         | `ServerStream {message: "s", count: 3}`                                                                                                                                                                                                                                             | `("s", 0)`, `("s", 1)`, `("s", 2)`, then `OK`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `server-stream-error`   | `ServerStream {message: "s", count: 2, fail_code: 9, fail_message: "stop"}`                                                                                                                                                                                                         | `("s", 0)`, `("s", 1)`, then `FailedPrecondition` (9) `"stop"`                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `bidi`                  | `Bidi` sending `"1"`, `"2"`, `"3"`, awaiting each echo before the next send, then closing the send side                                                                                                                                                                             | `("1", 0)`, `("2", 1)`, `("3", 2)`, then `OK`                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `bidi-error`            | `Bidi` sending `"1"` (await its echo), then `{fail_code: 10, fail_message: "abort"}`                                                                                                                                                                                                | `("1", 0)`, then `Aborted` (10) `"abort"`; the second send may fail with the same status, since the server can end the call first                                                                                                                                                                                                                                                                                                                                                                            |
| `unary-headers`         | `Unary {message: "hello"}` with metadata `interop-header: "h"`                                                                                                                                                                                                                      | `{message: "hello"}`, headers `{interop-header: "h"}`, trailing metadata `{interop-trailer: "h"}`                                                                                                                                                                                                                                                                                                                                                                                                            |
| `server-stream-headers` | `ServerStream {message: "s", count: 2}` with metadata `interop-header: "h"`; read headers before the first message                                                                                                                                                                  | headers `{interop-header: "h"}`, `("s", 0)`, `("s", 1)`, then `OK` with trailing metadata `{interop-trailer: "h"}`                                                                                                                                                                                                                                                                                                                                                                                           |
| `unreliable`            | `UnreliableEcho.Bidi` sending `{message: "0", count: 1}` … `{message: "49", count: 1}` without waiting, then `{message: <4096 characters>, count: 1}`, then closing the send side; read every response until the status                                                             | `OK`; every response message is one that was sent, at most once; the 4096-byte echo is present. On a reliable transport (QMux, WebSocket, HTTP/3 and HTTP/2 from Cronet): all 51 responses, `interop-received: 51`, `interop-datagrams-received: 0`. On native QUIC and WebTransport: `interop-received` between 1 and 51 and `interop-datagrams-received` at least 1                                                                                                                                        |
| `compressed`            | `Unary` twice with messages like `{"service":"search","region":"us-east-1","status":404,"latency_ms":100}` (`latency_ms` 100, 101), then `Bidi` sending three such messages (100 … 102) without waiting, then closing the send side                                                 | each message echoed, the stream ones with index 0, 1, 2, then `OK`. With the interop dictionary on both sides the messages travel compressed                                                                                                                                                                                                                                                                                                                                                                 |
| `compressed-stream`     | `CompressedEcho.Bidi` sending eight messages like those of `compressed` (`latency_ms` 100 … 107), awaiting each echo before the next send, then closing the send side; then `CompressedEcho.ClientStream` sending the same eight messages without waiting and closing the send side | the bidi messages echoed with index 0 … 7, then `OK`; then `{message: <the eight messages concatenated>, index: 8}` and `OK`. Each `OK` carries trailing metadata `interop-stream-compressed-received` of at least 6: the server received the third message and every later one as ZSTD_STREAM_MESSAGE frames                                                                                                                                                                                                |
| `refused-stream`        | `RefusingEcho.ClientStream` sending the eight messages of `compressed-stream` without waiting and closing the send side; then `compressed-stream`'s `CompressedEcho.Bidi` call; then its `CompressedEcho.ClientStream` call                                                         | `{message: <the eight messages concatenated>, index: 8}` and `OK`, each message received once: the server refused the optimistic call and the client replayed it. Then the bidi messages echoed with index 0 … 7 and `OK` with `interop-stream-compressed-received` of at least 1, since that call is not optimistic and compresses once the answer arrives; then the client-stream echo and `OK` with `interop-stream-compressed-received` of at least 6, since the answer made the client optimistic again |

Every client runs `compressed-stream` and `refused-stream` and holds compression
settings for them, so it stream-compresses on every lane. Clients compress their
requests optimistically from the first call, so nothing waits for the server's
answer; after the refusal in `refused-stream` they stop until a call is answered,
which the bidi call of that scenario is, through the HEADERS the server sends at
dispatch.

The 4096-character message of `unreliable` must not fit a datagram, so it must
not compress below about 1200 bytes either: the Go, Rust and browser clients
send 4096 base64 digits from a fixed xorshift generator. The Android client,
whose Cronet transport has no datagrams, may send `"x"` × 4096.

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
- With `--dictionary`, holds that zstd dictionary for compressed messages of
  the types `trevrpc.interop.v1.EchoRequest` and
  `trevrpc.interop.v1.EchoResponse`.
- With `--web-root`, serves the files in that directory to every other HTTP
  request, over HTTP/3, HTTP/2 and HTTP/1.1, through the HTTP fallback
  (`/` serves `index.html`).
- Runs until stdin reaches EOF or it receives `SIGTERM`/`SIGINT`, then shuts
  down gracefully and exits `0`. `run.py` closes stdin after the server's cells
  and fails the run unless the server exits `0` within 10 seconds.
- Diagnostics go to stderr.

### Client

```text
<binary> client --addr <host:port> --ca <cert.pem> [--server-name localhost]
                [--transport quic|tcp|auto] [--tcp-addr <host:port>]
                [--expect-transport quic|qmux] [--dictionary <file>]
```

- `--transport` selects QUIC only (`quic`, the default), QMux over TLS only
  (`tcp`), or QUIC with a QMux fallback (`auto`). `--tcp-addr` is the server's
  TCP address and defaults to `--addr`.
- `--expect-transport` fails the run unless the channel connected over that
  transport.
- `--dictionary` holds that zstd dictionary for compressed messages, as the
  server's flag does.
- Runs every scenario above in order.
- Prints `ok <name>` or `fail <name>: <reason>` per scenario to stdout.
- Exits `0` only if every scenario passed.

### Browser client

```text
trevrpc-interop-browser --browser chromium|firefox|safari --url <https://host:port/trevrpc>
                        --cert-hash <hex sha256> [--transport webtransport|websocket|auto]
                        [--ws-url <wss://host:port/trevrpc>]
                        [--expect-transport webtransport|websocket] [--dictionary <file>]
                        [--page-url <https://host:port/> --cert <cert.pem> --key <key.pem>
                         [--page-protocol h2|h3]]
trevrpc-interop-browser --write-page <dir>
```

It follows the same output contract. `--transport` defaults to `webtransport`;
`--ws-url` defaults to `--url` with the `wss:` scheme. `--expect-transport`
fails the run unless the channel connected over that transport. `--dictionary`
makes the channel compress Echo messages with that zstd dictionary. Chromium and
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
