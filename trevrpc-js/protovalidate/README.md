# @trevrpc/protovalidate

[protovalidate](https://github.com/bufbuild/protovalidate) request validation for the TrevRPC browser client, [`@trevrpc/trevrpc-js`](https://www.npmjs.com/package/@trevrpc/trevrpc-js): requests that break their [`buf.validate`](https://buf.build/bufbuild/protovalidate) rules fail before they are sent. It is a separate package so that `@trevrpc/trevrpc-js` itself does not depend on protovalidate or CEL.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.9.0 works with `trevrpc-go` 1.9.0, `trevrpc-js` 1.9.0, and `trevrpc-kotlin`
> 1.9.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

```sh
npm install @trevrpc/protovalidate @trevrpc/trevrpc-js @bufbuild/protobuf @bufbuild/protovalidate
```

Generate the messages with `protoc-gen-es` as usual, with `buf/validate/validate.proto` on the include path (or a `buf.build/bufbuild/protovalidate` dependency in `buf.yaml`), and pass a request validator to `createClient`:

```ts
import { createClient } from "@trevrpc/trevrpc-js";
import { createRequestValidator } from "@trevrpc/protovalidate";
import { ViolationsSchema } from "@bufbuild/protovalidate/gen/buf/validate/validate_pb.js";

import { Users } from "./gen/users/v1/users_pb.js";

const users = createClient(Users, channel, { validator: createRequestValidator() });

try {
  await users.create({ name: "", email: "ada@example.com" });
} catch (error) {
  // Code.InvalidArgument, whether the client or a validating server rejected it.
  for (const { violations } of error.findDetails(ViolationsSchema)) {
    for (const violation of violations) console.log(violation.field, violation.message);
  }
}
```

- A request message that breaks its rules is not sent, and fails with `InvalidArgument` and a `buf.validate.Violations` status detail, as the Go and Rust servers report it, so one error handler serves both. The message names the first violation and counts the rest. The request of a unary or server-streaming call fails the call, after its interceptors ran; a message passed to `send()` of a client-streaming or bidirectional call rejects that send, and the call goes on.
- A rule that cannot be evaluated for a message fails it with `Internal`.
- `createClient` compiles the rules of each method's request type, and throws a `TrevRpcError` with `Code.Internal` when one cannot compile.
- Methods whose request type has no rules, directly or in a message it holds, are not checked and cost nothing.
- `createRequestValidator(validator?)` takes a protovalidate validator, by default one from `createValidator()`. Pass `createValidator({ registry })`, with a registry that holds their extensions, to enforce predefined rules, or `createValidator({ failFast: true })` to stop at the first violation.

To check one message as the server would, for a test, call the function that `validateFunc` returns for its schema: `createRequestValidator().validateFunc(CreateRequestSchema)?.(message)` throws the error the call would fail with.

Checking requests on the client saves a round trip and shows the violations at once. It does not replace checking them on the server, with [`protovalidatetrevrpc`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-go/protovalidatetrevrpc) or [`trevrpc-protovalidate`](https://trev.zip/llc/TrevRPC/src/branch/main/trevrpc-rust/crates/trevrpc-protovalidate), since clients can skip it.

See [Request Validation](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide#request-validation).

## Development

The package is developed against the trevrpc-js of this checkout, which it links as `file:..`, so install that first:

```sh
npm ci --prefix ..
npm ci
npm run check      # format check, lint, typecheck, tests
```

The tests run the real client against the in-memory server of trevrpc-js's own tests. `nix build .#trevrpc-js-protovalidate` runs them against the built trevrpc-js, and checks the published files.
