# Interop

Cross-runtime interoperability tests. Every server is exercised by every client
over the transport that client speaks:

| Client                   | Transport    | Servers                     |
| ------------------------ | ------------ | --------------------------- |
| Go                       | native QUIC  | Go, Rust                    |
| Rust                     | native QUIC  | Go, Rust                    |
| JavaScript (Chromium)    | WebTransport | Go, Rust                    |
| JavaScript (Firefox)     | WebTransport | Go, Rust                    |
| Kotlin (Cronet, Android) | HTTP/3       | Go, Rust                    |
| JavaScript (Safari)      | WebTransport | Go, Rust (expected to fail) |

`run.sh` drives the desktop matrix, which `nix flake check` runs on Linux for
Chromium and Firefox. CI runs the Android client on an emulator because it needs
KVM, and runs Safari on macOS. Safari's WebTransport profile does not yet
interoperate with either server, so that lane is expected to fail; it lives in
its own workflow so the failure stays visible without blocking the required
checks.

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
  (`POST /trevrpc`), and WebTransport (`/trevrpc`).
- Once every listener is accepting, prints exactly one JSON line to stdout
  and flushes it: `{"native":<port>,"http3":<port>,"webtransport":<port>}`.
  The ports may be equal when one UDP socket serves several protocols.
- Runs until stdin reaches EOF or it receives `SIGTERM`/`SIGINT`, then shuts
  down gracefully and exits `0`.
- Diagnostics go to stderr.

### Client

```text
<binary> client --addr <host:port> --ca <cert.pem> [--server-name localhost]
```

- Runs every scenario above in order.
- Prints `ok <name>` or `fail <name>: <reason>` per scenario to stdout.
- Exits `0` only if every scenario passed.

### Certificates

`run.sh` generates one self-signed ECDSA P-256 certificate valid for 10 days
with SANs `DNS:localhost` and `IP:127.0.0.1` and `CA:FALSE` (rustls rejects a CA
certificate as a server identity). It is both the server certificate and the
client trust root. Browsers trust it through WebTransport
`serverCertificateHashes`, which requires ECDSA and at most 14 days validity.
