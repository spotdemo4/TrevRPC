# @trevrpc/protoc-gen-trevrpc-js

A `protoc` plugin that generates a codec for each message, for the TrevRPC browser client, [`@trevrpc/trevrpc-js`](https://www.npmjs.com/package/@trevrpc/trevrpc-js). It runs after [`protoc-gen-es`](https://github.com/bufbuild/protobuf-es) and writes `<file>_trevrpc.js` next to its `<file>_pb.js`. A codec encodes and decodes the same protobuf-es messages as `toBinary` and `fromBinary`, to the same bytes, with code written for its message type instead of protobuf-es's generic per-field dispatch.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: generated codecs need the `@trevrpc/trevrpc-js` of the plugin's version or later. Compatibility
> across different versions isn't guaranteed yet; that's a milestone for 2.0.

```sh
npm install @trevrpc/trevrpc-js @bufbuild/protobuf
npm install --save-dev @bufbuild/protoc-gen-es @trevrpc/protoc-gen-trevrpc-js
```

Add the plugin to `buf.gen.yaml` after `protoc-gen-es`, with the same `target` and `import_extension`:

```yaml
version: v2
plugins:
  - local: protoc-gen-es
    out: src/gen
    opt: target=ts
  - local: protoc-gen-trevrpc-js
    out: src/gen
    opt: target=ts
```

Pass a service's codecs to `createClient`:

```ts
import { createClient } from "@trevrpc/trevrpc-js";

import { Greeter } from "./gen/greeter_pb.js";
import { GreeterCodecs } from "./gen/greeter_trevrpc.js";

const greeter = createClient(Greeter, channel, { codecs: GreeterCodecs });
```

- Each message gets a codec, such as `GreetRequestCodec`, and each service the codecs of its request and response types, such as `GreeterCodecs`. Combine the codecs of several services in one array when a client needs them.
- Messages are the usual protobuf-es messages, with the same property order and `$unknown` for unknown fields, so `create`, `equals`, `toJson` and the generated types work on them as before.
- A codec fails where protobuf-es fails, with protobuf-es's error: invalid values such as an `int32` of 1.5, invalid UTF-8 in a string, truncated input, and messages nested more than 100 deep.
- Decoding differs on two kinds of malformed input: a known field sent with another wire type is kept in `$unknown`, as the Go and Rust runtimes keep it, where `fromBinary` reads it as its declared type anyway; and a nested message whose last field runs past its length makes the codec fail and fall back to `fromBinary`, which accepts it, so such input is decoded twice.
- Methods whose types have no codec use protobuf-es, as do clients without the `codecs` option. A codec applies only to the schema it was generated with, the one its `_trevrpc.js` imports, so regenerate both files together.
- `GreetRequestCodec.encode(message)` and `GreetRequestCodec.decode(bytes)` run a codec directly, like `toBinary` and `fromBinary`.

The generated code covers proto3 messages. Messages of proto2 and editions files, and messages with a field that protobuf-es represents specially (a wrapper type such as `google.protobuf.StringValue`, which it unboxes, or `google.protobuf.Struct`, which is a JSON object), get a codec that runs protobuf-es, as do fields of message types that weren't generated in the same run, such as the other well-known types. Their encoding is unchanged; only their speed is protobuf-es's.

The plugin takes protobuf-es's plugin options: `target` (`js`, `ts`, `dts`, or a combination like `js+dts`), `import_extension` and `js_import_style`.

## Development

The plugin runs from this directory with the dependencies of trevrpc-js, whose tests generate codecs with it and check them against protobuf-es (`test/codec.test.js`):

```sh
npm ci --prefix ..
npm run build --prefix ..
npm test --prefix ..
npm run check      # format check, lint
```

`nix build .#trevrpc-js.protocGen` checks the published files and runs the installed plugin; `nix build .#trevrpc-js` builds it too and puts it next to `trevrpc-interop-browser`.
