# TrevRPC schema options

Options for [TrevRPC](https://trev.zip/llc/TrevRPC), a protobuf RPC framework over QUIC, HTTP/3, and WebTransport.

`trevrpc/options.proto` defines `(trevrpc.method)`. It marks the streamed requests or responses of a method as unreliable, so they may travel as QUIC or WebTransport datagrams (see [Datagram Messages](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#datagram-messages)), and sets how a method's messages may be compressed without a dictionary (`compression`): one at a time (`COMPRESSION_MESSAGE`; see [Compression Without a Dictionary](https://trev.zip/llc/TrevRPC/wiki/Compression#compression-without-a-dictionary)), or also with one zstd context per call on a streaming method (`COMPRESSION_STREAM`; see [Stream Compression](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format#stream-compression)):

```proto
import "trevrpc/options.proto";

service Telemetry {
  rpc Track(stream Position) returns (stream Position) {
    option (trevrpc.method) = {unreliable_requests: true, unreliable_responses: true};
  }
  rpc Subscribe(SubscribeRequest) returns (stream MetricSample) {
    option (trevrpc.method) = {compression: COMPRESSION_STREAM};
  }
  rpc GetReport(GetReportRequest) returns (Report) {
    option (trevrpc.method) = {compression: COMPRESSION_MESSAGE};
  }
}
```

Add the module to your `buf.yaml` and run `buf dep update`:

```yaml
version: v2
deps:
  - buf.build/trevrpc/trevrpc
```

Go code for these options ships in `trev.zip/llc/trevrpc/trevrpc-go/trevrpcpb`, so Go users generate only their own files. See the [Getting Started](https://trev.zip/llc/TrevRPC/wiki/Getting-Started) guide for `buf.gen.yaml` examples with the TrevRPC generators.
