# Interop

Cross-runtime interoperability tests. Every server is exercised by every client
over every transport that client speaks. QUIC is the primary transport; each
client also has a TCP fallback for networks that block UDP.

| Client                   | Primary transport | TCP fallback        | Servers                                |
| ------------------------ | ----------------- | ------------------- | -------------------------------------- |
| Go                       | native QUIC       | QMux over TLS       | Go, Rust                               |
| Rust                     | native QUIC       | QMux over TLS       | Go, Rust                               |
| JavaScript (Chromium)    | WebTransport      | QMux over WebSocket | Go, Rust                               |
| JavaScript (Firefox)     | WebTransport      | QMux over WebSocket | Go, Rust                               |
| Kotlin (Cronet, Android) | HTTP/3            | HTTP/2              | Go, Rust                               |
| JavaScript (Safari)      | WebTransport      | QMux over WebSocket | Go; Rust WebTransport expected to fail |

`run.sh` (packaged with its tools as `nix run .#trevrpc-interop`) drives the desktop matrix, which `nix flake check` runs on Linux for
Chromium and Firefox. CI runs the Android client on an emulator because it needs
KVM (once over HTTP/3 and once over HTTP/2 with QUIC disabled), and runs Safari
on macOS through `safaridriver` (not Playwright's WebKit, whose builds decide on
their own whether to expose WebTransport). Safari passes every lane against the Go server. Against the Rust
server it passes the WebSocket and fallback lanes; Rust's `h3` does not yet
speak the WebTransport draft Safari uses, so the Rust WebTransport lane is
expected to fail. It is an allowed failure in the `safari` workflow, so it stays
visible without failing the workflow.

## Cells

`run.sh` runs one cell per client, server, and transport (`X` is a native client,
`B` a browser, `S` a server):

| Cell              | Client   | Transport                                                                                       |
| ----------------- | -------- | ----------------------------------------------------------------------------------------------- |
| `X-to-S`          | Go, Rust | QUIC                                                                                            |
| `X-qmux-to-S`     | Go, Rust | QMux over TLS on the server's TCP port                                                          |
| `X-fallback-to-S` | Go, Rust | `auto`, with the QUIC address pointing at UDP port 24999, which nothing answers: must fall back |
| `B-to-S`          | Browser  | WebTransport                                                                                    |
| `B-wss-to-S`      | Browser  | QMux over WebSocket (`wss://`)                                                                  |
| `B-fallback-to-S` | Browser  | `auto`, with the WebTransport URL pointing at UDP port 24999: must fall back to WebSocket       |

`--browser-lanes webtransport,websocket,fallback` selects which browser lanes
run (default: all three). Each cell asserts the transport it actually used
(`--expect-transport`), so a cell fails if it connects over the wrong
transport.

## Service

[`proto/interop.proto`](proto/interop.proto) defines `trevrpc.interop.v1.Echo`.
Servers implement it exactly as follows.

| Method         | Behavior                                                                                                                                                                                                                     |
| -------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Unary`        | If `fail_code != 0`, fail with status `fail_code` / `fail_message`. Otherwise respond `{message: request.message + suffix, index: 0}`, where `suffix` is the UTF-8 value of request metadata key `interop-suffix`, or empty. |
| `ClientStream` | Read requests until EOF. If a request has `fail_code != 0`, fail immediately with that status. At EOF respond `{message: concatenation of every request.message, index: number of requests}`.                                |
| `ServerStream` | Send `count` responses `{message: request.message, index: i}` for `i` in `0..count`. Then end with status `fail_code` / `fail_message` if `fail_code != 0`, otherwise `OK`.                                                  |
| `Bidi`         | For the `i`-th request: if `fail_code != 0`, end with that status; otherwise send `{message: request.message, index: i}`. End with `OK` at request EOF.                                                                      |

## Scenarios

Clients run these in order, each with a 10 second deadline.

| Name                  | Call                                                                                                    | Expected                                                       |
| --------------------- | ------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------- |
| `unary`               | `Unary {message: "hello"}`                                                                              | `{message: "hello", index: 0}`                                 |
| `unary-metadata`      | `Unary {message: "hello"}` with metadata `interop-suffix: "!"`                                          | `{message: "hello!"}`                                          |
| `unary-error`         | `Unary {fail_code: 5, fail_message: "missing"}`                                                         | status `NotFound` (5), message `"missing"`                     |
| `unary-large`         | `Unary {message: "x" × 1048576}`                                                                        | same 1 MiB message echoed                                      |
| `client-stream`       | `ClientStream` sending `"a"`, `"b"`, `"c"`                                                              | `{message: "abc", index: 3}`                                   |
| `server-stream`       | `ServerStream {message: "s", count: 3}`                                                                 | `("s", 0)`, `("s", 1)`, `("s", 2)`, then `OK`                  |
| `server-stream-error` | `ServerStream {message: "s", count: 2, fail_code: 9, fail_message: "stop"}`                             | `("s", 0)`, `("s", 1)`, then `FailedPrecondition` (9) `"stop"` |
| `bidi`                | `Bidi` sending `"1"`, `"2"`, `"3"`, awaiting each echo before the next send, then closing the send side | `("1", 0)`, `("2", 1)`, `("3", 2)`, then `OK`                  |
| `bidi-error`          | `Bidi` sending `"1"` (await its echo), then `{fail_code: 10, fail_message: "abort"}`                    | `("1", 0)`, then `Aborted` (10) `"abort"`                      |

## Command-line contract

Each runtime ships one interop executable. The browser and Android clients
have their own entry points but follow the same output contract.

### Server

```text
<binary> server --cert <cert.pem> --key <key.pem> [--host 127.0.0.1] [--port 0]
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
- Runs until stdin reaches EOF or it receives `SIGTERM`/`SIGINT`, then shuts
  down gracefully and exits `0`.
- Diagnostics go to stderr.

### Client

```text
<binary> client --addr <host:port> --ca <cert.pem> [--server-name localhost]
                [--transport quic|tcp|auto] [--tcp-addr <host:port>]
                [--expect-transport quic|qmux]
```

- `--transport` selects QUIC only (`quic`, the default), QMux over TLS only
  (`tcp`), or QUIC with a QMux fallback (`auto`). `--tcp-addr` is the server's
  TCP address and defaults to `--addr`.
- `--expect-transport` fails the run unless the channel connected over that
  transport.
- Runs every scenario above in order.
- Prints `ok <name>` or `fail <name>: <reason>` per scenario to stdout.
- Exits `0` only if every scenario passed.

### Browser client

```text
trevrpc-interop-browser --browser chromium|firefox|safari --url <https://host:port/trevrpc>
                        --cert-hash <hex sha256> [--transport webtransport|websocket|auto]
                        [--ws-url <wss://host:port/trevrpc>]
                        [--expect-transport webtransport|websocket]
```

It follows the same output contract. `--transport` defaults to `webtransport`;
`--ws-url` defaults to `--url` with the `wss:` scheme. `--expect-transport`
fails the run unless the channel connected over that transport. Chromium and
Firefox ignore certificate errors, because WSS cannot pin a certificate hash.
Safari is driven through `safaridriver`, which needs `sudo safaridriver --enable`
once, and cannot ignore certificate errors (see [Certificates](#certificates)).

### Android client

`trevrpc-interop-android` accepts `--transport http3|http2` (default `http3`).
`http2` runs with Cronet's QUIC disabled, so the scenarios use the server's
HTTP/2 mapping on its TCP port.

### Certificates

`run.sh` generates one self-signed ECDSA P-256 certificate valid for 10 days
with SANs `DNS:localhost` and `IP:127.0.0.1`, `CA:FALSE` (rustls rejects a CA
certificate as a server identity) and the `serverAuth` extended key usage (macOS
requires it of TLS server certificates). It is both the server certificate and the
client trust root. Browsers trust it through WebTransport
`serverCertificateHashes`, which requires ECDSA and at most 14 days validity.
The WSS lanes cannot pin a hash, so Chromium and Firefox ignore certificate
errors instead. Safari cannot, so with `--browser safari` `run.sh` adds the
certificate to the macOS System keychain as a trusted root through `sudo` and
removes it when the run ends. It never prompts: `sudo` must not need a password,
as on CI runners, or must already be authenticated (`sudo -v`).
