# trevrpc-js

The browser client for [TrevRPC](https://trev.zip/llc/TrevRPC), an RPC framework like gRPC built on QUIC instead of HTTP/2: a WebTransport runtime with a QMux over WebSocket fallback for where WebTransport is missing or UDP is blocked, for the messages and service descriptors that [protobuf-es](https://github.com/bufbuild/protobuf-es) generates. It has no Node.js runtime, and it talks to TrevRPC servers written in Go, Rust or Erlang.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.12.0 works with `trevrpc-go` 1.12.0, `trevrpc-js` 1.12.0, `trevrpc-kotlin`
> 1.12.0, and the Erlang `trevrpc` 1.12.0. Compatibility across different versions isn't guaranteed yet; that's a
> milestone for 2.0.

## Packages

| Package                                                                                          | Install                                                      | Purpose                                                                      |
| ------------------------------------------------------------------------------------------------ | ------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| [`@trevrpc/trevrpc-js`](https://www.npmjs.com/package/@trevrpc/trevrpc-js)                       | `npm install @trevrpc/trevrpc-js @bufbuild/protobuf`         | Browser client runtime                                                       |
| [`@trevrpc/protoc-gen-trevrpc-js`](https://www.npmjs.com/package/@trevrpc/protoc-gen-trevrpc-js) | `npm install --save-dev @trevrpc/protoc-gen-trevrpc-js`      | Optional `protoc` plugin: message codecs, faster than protobuf-es            |
| [`@trevrpc/effect`](https://www.npmjs.com/package/@trevrpc/effect)                               | `npm install @trevrpc/effect effect`                         | [Effect](https://effect.website) clients whose calls are effects and streams |
| [`@trevrpc/protovalidate`](https://www.npmjs.com/package/@trevrpc/protovalidate)                 | `npm install @trevrpc/protovalidate @bufbuild/protovalidate` | Checks requests against protovalidate rules before sending                   |

The `@trevrpc/trevrpc-js/zstd` entry point provides `createCompression()` for [compressed messages](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide#compressed-messages); it loads libzstd as WebAssembly (about 85 KB gzipped), so applications without compression never load it.

The `trevrpc-js` Nix package provides the plugin: `nix shell git+https://trev.zip/llc/TrevRPC.git#trevrpc-js`.

## Example

Generate the messages and the service descriptor for a service with `protoc-gen-es`:

```sh
npm install @trevrpc/trevrpc-js @bufbuild/protobuf
npm install --save-dev @bufbuild/protoc-gen-es
protoc \
  --plugin=protoc-gen-es=node_modules/.bin/protoc-gen-es \
  --es_out=src/gen --es_opt=target=ts \
  greeter.proto
```

Connect and call it:

```js
import { connect, createClient } from "@trevrpc/trevrpc-js";

import { Greeter } from "./gen/greeter_pb.js";

const channel = await connect("https://example.com:4433/trevrpc");
const client = createClient(Greeter, channel, { timeoutMs: 5_000 });

const reply = await client.sayHello({ name: "TrevRPC" });
console.log(reply.message);

channel.close();
```

## Documentation

The [JavaScript guide](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide) covers generated messages and codecs, calls and streams, errors, interceptors, request validation, Effect, compression, channels, and the WebSocket fallback. The rest of the [wiki](https://trev.zip/llc/TrevRPC/wiki) covers the transports, the protocol, compression, and operations.

## Development

```sh
npm ci
npm run check      # format check, lint, typecheck, tests
npm run build      # generate the interop and test descriptors and bundle the interop and benchmark pages into build/interop/
npm test           # the tests alone, which read what npm run build generates
npm run interop -- --browser chromium --url ... --cert-hash ...
```

`nix build .#trevrpc-js` runs the same checks and installs the library, `protoc-gen-trevrpc-js`, and the wrapped `trevrpc-interop-browser` with matching Playwright browsers. The pinned `playwright` version must equal nixpkgs' `playwright-driver`; `nix run .#sync-playwright` updates it.

Tests (`node --test`) cover the wire format against `testdata/wire-golden-vectors.txt` (frame bytes, and the frame reader against the shared byte streams), the QMux frames against `testdata/qmux-golden-vectors.txt`, framing, the `trevrpc.method` option, and the client — deadlines, cancellation, stream termination and reconnection — against in-memory fake servers, including a fake Echo server that runs all interop scenarios.

`zstd-wasm/` is a Rust crate that is not published: it builds libzstd as the WebAssembly module that `npm run build` embeds for the `@trevrpc/trevrpc-js/zstd` entry point. The Effect and protovalidate adapters and the plugin are packages of their own in `effect/`, `protovalidate/` and `protoc-gen-trevrpc-js/`.

### Interop client

`trevrpc-interop-browser` runs the cross-runtime interop scenarios from [`interop/README.md`](https://trev.zip/llc/TrevRPC/src/branch/main/interop/README.md) in a real browser over WebTransport, QMux over WebSocket, or the fallback between them. It is built from `interop/proto/interop.proto` with protoc-gen-es, so it also serves as a complete example of a client. `interop/run.py` runs it against every server; see [Interop Tests](https://trev.zip/llc/TrevRPC/wiki/Local-Development#interop-tests).

```sh
trevrpc-interop-browser --browser chromium|firefox|safari \
  --url https://127.0.0.1:4433/trevrpc \
  --cert-hash <hex sha256 of the server certificate DER> \
  [--transport webtransport|websocket|auto] [--ws-url wss://127.0.0.1:4433/trevrpc] \
  [--expect-transport webtransport|websocket] [--dictionary <file>]
```

It prints `ok <name>` or `fail <name>: <reason>` per scenario and exits 0 only if all scenarios passed. `--dictionary` makes the channel compress Echo messages with that zstd dictionary, the one the interop servers hold. `--scenarios <name,...>` runs only the scenarios it names, for a server that supports only some, and prints `skip <name>: <reason>` for the others. `trevrpc-interop-browser bench` runs the load benchmark in a browser instead (see [`bench/README.md`](https://trev.zip/llc/TrevRPC/src/branch/main/bench/README.md#browsers)). Chromium and Firefox are launched through Playwright from `PLAYWRIGHT_BROWSERS_PATH`, or from `TREVRPC_BROWSER_CHROMIUM` and `TREVRPC_BROWSER_FIREFOX` when those name an executable, and ignore certificate errors because WSS cannot pin a certificate hash. Safari is driven through `safaridriver` (`TREVRPC_BROWSER_SAFARI` overrides its path), which needs `sudo safaridriver --enable` once; Safari cannot ignore certificate errors, so the server certificate must be trusted by the system, as `interop/run.py` does. The CLI serves the page from a loopback origin; the page fetches its input from there and posts each result back, so where the browser has no WebTransport at all every scenario fails with a clear reason. `--write-page <dir>` writes that page to a directory instead, and `--page-url <url> --cert <pem> --key <pem>` opens it from a TrevRPC server that serves the directory through its HTTP fallback; the page then reports to the CLI over HTTPS, at the origin in its `report` query parameter. `--page-protocol h2|h3` has the browser load that page over HTTP/2 or HTTP/3 and fails every scenario unless it did (`h3` in Chromium only).

See [Local Development](https://trev.zip/llc/TrevRPC/wiki/Local-Development) for the repository-wide checks.
