# protoc-gen-trevrpc-py

Generates [TrevRPC](https://trev.zip/llc/TrevRPC) clients for Python, on the messages
[protoc-gen-py](https://github.com/bufbuild/protobuf-py) generates. Run both plugins into the same
directory: for each `.proto` file with services, it writes `<file>_trevrpc.py` next to
protoc-gen-py's `<file>_pb.py`.

```yaml
# buf.gen.yaml
version: v2
plugins:
  - local: protoc-gen-py
    out: gen
  - local: protoc-gen-trevrpc-py
    out: gen
```

Each service `Greeter` gets a `GreeterClient` that takes a `trevrpc.Channel`, with one method per
RPC named in snake_case, and a `trevrpc.Method` descriptor per RPC (`GREETER_SAY_HELLO`) that
carries its `(trevrpc.method)` options. Unary methods are coroutines; streaming methods return the
call object at once. See the [trevrpc](https://pypi.org/project/trevrpc) package for the runtime.

`src/protoc_gen_trevrpc_py/_options_pb.py` is what protoc-gen-py generates from
`proto/trevrpc/options.proto`, so the plugin reads the options without other generated code.
