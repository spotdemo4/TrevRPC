# protovalidatetrevrpc

[protovalidate](https://github.com/bufbuild/protovalidate) request validation for [TrevRPC](https://trev.zip/llc/TrevRPC) Go servers: requests that break their [`buf.validate`](https://buf.build/bufbuild/protovalidate) rules fail before the handler runs. It is a separate module so that TrevRPC itself does not depend on protovalidate or CEL.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.8.0 works with `trevrpc-go` 1.8.0, `trevrpc-js` 1.8.0, and `trevrpc-kotlin`
> 1.8.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

```sh
go get trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc
```

```go
// nil uses protovalidate.GlobalValidator; pass a protovalidate.Validator to configure one.
server.SetValidator(protovalidatetrevrpc.New(nil))
```

- A request that breaks its rules fails with `InvalidArgument` and a `buf.validate.Violations` status detail, which every client runtime can decode field by field. The detail lists at most `MaxDetailViolations` (100) violations, and the message names the first and counts the rest, so a request that breaks a rule on each of many items still gets a status that fits a frame. On a streaming route, the request message that breaks them fails the `Recv` that returned it.
- Rules are compiled when the server starts, and a rule that cannot compile makes `Listen` fail. The route it belongs to fails every call with `Internal` instead of skipping validation.
- Routes whose request type has no rules, directly or in a message it holds, skip validation and cost nothing.
- Clients can check the requests they send with the same validator, without a round trip: `trevrpc.DialOptions{Validator: protovalidatetrevrpc.New(nil)}`. A request that breaks its rules fails locally with the same status, and a streamed message fails its `Send` and leaves the call open.
- `protovalidatetrevrpc.Validate(msg)` checks one message as the server checks a request of its type, and returns the `*trevrpc.Status` the call would fail with, or nil, for handler tests. Rules compile once per message type.

See [Request Validation](https://trev.zip/llc/TrevRPC/wiki/Go-Guide#request-validation).
