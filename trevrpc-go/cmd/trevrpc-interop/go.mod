module trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop

go 1.26.0

require (
	buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go v1.36.12-20260825204119-511051f7f437.2
	github.com/quic-go/quic-go v0.64.1
	google.golang.org/protobuf v1.36.12
	trev.zip/llc/trevrpc/trevrpc-go v1.9.0
	trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc v1.9.0
)

require (
	buf.build/go/protovalidate v1.4.0 // indirect
	cel.dev/cel-go v0.32.0 // indirect
	cel.dev/expr v0.25.3 // indirect
	github.com/DataDog/zstd v1.5.7 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/coder/websocket v1.8.15 // indirect
	github.com/dunglas/httpsfv v1.1.1 // indirect
	github.com/klauspost/compress v1.20.4 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	github.com/quic-go/webtransport-go v0.13.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.59.0 // indirect
	golang.org/x/exp v0.0.0-20260820142414-ca536658362e // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.50.0 // indirect
	golang.org/x/text v0.44.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260819154853-08b0e4226688 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260819154853-08b0e4226688 // indirect
)

replace (
	trev.zip/llc/trevrpc/trevrpc-go => ../../
	trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc => ../../protovalidatetrevrpc
)

tool google.golang.org/protobuf/cmd/protoc-gen-go
