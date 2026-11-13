# trevrpc-protovalidate

[protovalidate](https://github.com/bufbuild/protovalidate) request validation for [TrevRPC](https://trev.zip/llc/TrevRPC) servers: requests that break their [`buf.validate`](https://buf.build/bufbuild/protovalidate) rules fail before the handler runs. It is a separate crate so that `trevrpc` itself does not depend on protovalidate or CEL.

> **Compatibility:** every TrevRPC package shares one version, and packages of the same version are compatible
> with each other: `trevrpc-rust` 1.11.0 works with `trevrpc-go` 1.11.0, `trevrpc-js` 1.11.0, and `trevrpc-kotlin`
> 1.11.0. Compatibility across different versions isn't guaranteed yet; that's a milestone for 2.0.

Generate the services with `protoc-gen-trevrpc-rust`'s `validate=true` option, with `buf/validate/validate.proto` on the include path, and depend on this crate:

```toml
[dependencies]
trevrpc-protovalidate = "1.11.0"
```

- The generated registration checks each request message of the methods whose request type carries rules, directly or in a message it holds. The other methods are generated as without the option and cost nothing.
- A request that breaks its rules fails with `InvalidArgument` and a `buf.validate.Violations` status detail, which every client runtime can decode field by field. The detail lists at most `MAX_DETAIL_VIOLATIONS` (100) violations, and the message names the first and counts the rest, so a request that breaks a rule on each of many items still gets a status that fits a frame. On a streaming route, the request message that breaks them fails the stream's item.
- Validation runs on the request's encoded bytes with prost-protovalidate's runtime engine, which enforces CEL rules as protovalidate does in every language, so the request types need no `prost-reflect` support. The generated code embeds the descriptors it needs. Validation runs inline on the task serving the call, as decoding does, so keep rules cheap to evaluate.
- Rules are compiled when the services are registered: registration panics on a rule that cannot compile.
- Generated clients check the requests they send the same way, without a round trip, when they opt in: `NumberServiceClient::new(channel).with_validation()`. A request that breaks its rules fails locally with the same status; a streamed message fails its `send()` and leaves the call open.
- `MessageValidator::validate_message(&message)` checks one message as the server checks a request of its type, for handler tests.

See [Request Validation](https://trev.zip/llc/TrevRPC/wiki/Rust-Guide#request-validation).
