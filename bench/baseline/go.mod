module trev.zip/llc/trevrpc/bench/baseline

go 1.26.0

require (
	connectrpc.com/connect/v2 v2.0.2
	github.com/klauspost/compress v1.20.3
	github.com/quic-go/quic-go v0.64.1
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/crypto v0.54.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.6.2 // indirect
)

tool (
	connectrpc.com/connect/v2/cmd/protoc-gen-connect-go
	google.golang.org/grpc/cmd/protoc-gen-go-grpc
	google.golang.org/protobuf/cmd/protoc-gen-go
)
