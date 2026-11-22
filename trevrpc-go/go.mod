module trev.zip/llc/trevrpc/trevrpc-go

go 1.26.0

require (
	github.com/DataDog/zstd v1.5.7
	github.com/coder/websocket v1.8.15
	github.com/klauspost/compress v1.20.5
	github.com/quic-go/quic-go v0.64.1
	github.com/quic-go/webtransport-go v0.13.0
	golang.org/x/crypto v0.59.0
	golang.org/x/net v0.63.0
	golang.org/x/sys v0.50.0
	google.golang.org/protobuf v1.36.12
	trev.zip/llc/trevrpc/trevrpc-go/qmux v1.12.0
)

require (
	github.com/dunglas/httpsfv v1.1.1 // indirect
	github.com/quic-go/qpack v0.6.0 // indirect
	golang.org/x/text v0.44.0 // indirect
)

replace trev.zip/llc/trevrpc/trevrpc-go/qmux => ./qmux

tool google.golang.org/protobuf/cmd/protoc-gen-go
