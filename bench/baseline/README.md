# gRPC and Connect baselines

`trevrpc-bench-baseline` provides the gRPC and Connect baselines for the [load benchmarks](../README.md), so TrevRPC's numbers can be compared with stock grpc-go and connect-go on the same machine. It is a separate Go module that does not depend on `trevrpc-go`. It generates `echopb` (and `echopb/echopbconnect`) from the shared [`interop.proto`](../../interop/proto/interop.proto) with `protoc-gen-go`, `protoc-gen-go-grpc`, and `protoc-gen-connect-go` (pinned as `tool` directives in `go.mod`); a test regenerates the code and fails on drift, and `go test ./echopb -update` rewrites it.

`server --cert --key [--host] [--port] [--connect-port]` serves `trevrpc.interop.v1.Echo` with the behavior in [`interop/README.md`](../../interop/README.md) twice, each over HTTP/2 with TLS on its own port: over gRPC with grpc-go, and over the Connect protocol with connect-go on `net/http`. It prints `{"grpc":<port>,"connect":<port>}` once both are accepting, and exits when stdin reaches EOF.

`bench --transport grpc|connect` implements the client contract: all workers share one connection, each with one call in flight (`bidi` keeps one stream per worker open), and it prints the same JSON line with `transport` set to `grpc` or `connect`. `--metadata-size N` adds one `bench-pad` request metadata entry of N `x` bytes to every call, as gRPC metadata or a Connect request header. `--tcp-addr` is accepted and ignored. `wire_bytes_per_msg` counts the bytes of the client's TCP connection (TLS records, HTTP/2 framing included) through a counting `net.Conn`, the analogue of the UDP payload measure used for QUIC.

Both use their library's defaults, with no tuning, except where Connect needs two changes to be comparable:

- **gRPC** uses one `grpc.ClientConn`, with grpc-go's default HTTP/2 flow control (64 KiB initial windows with dynamic BDP estimation) and no compression.
- **Connect** uses the Connect protocol with the binary Protobuf codec over one `net/http` HTTP/2 connection. Compression is off: connect-go otherwise gzips every response, since its client accepts gzip and its handlers compress messages of any size. The client also waits for a free stream instead of opening a second connection when the server's limit of 250 concurrent streams is reached, so more than 250 workers still share one connection.
