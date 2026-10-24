package main

import (
	"bytes"
	"io"

	"connectrpc.com/connect"
	"github.com/klauspost/compress/zstd"
	connectcompress "github.com/klauspost/connect-compress/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	grpcgzip "google.golang.org/grpc/encoding/gzip"
)

// The --compression modes of bench/README.md. Each compresses every message
// on its own, which is all gRPC and Connect offer; gzip and zstd run the unary,
// client-stream and bidi scenarios.
var (
	compressions           = []string{"none", "gzip", "zstd"}
	compressedScenarios    = []string{"unary", "client-stream", "bidi"}
	connectZstdAtFastest   = connectcompress.WithNew(connectcompress.Zstandard, connectcompress.LevelFastest)
	grpcCompressorForModes = map[string]string{"gzip": grpcgzip.Name, "zstd": grpcZstdName}
)

// zstd runs at klauspost's fastest level, the closest to the zstd level 1
// TrevRPC compresses at; gzip keeps each library's default level.
func init() {
	encoding.RegisterCompressor(newGRPCZstd())
}

// grpcZstdName is the grpc-encoding of zstd.
const grpcZstdName = "zstd"

// grpcZstd is gRPC's zstd compressor, compressing each message on its own as
// grpc-go's gzip does. One encoder and one decoder serve every call:
// klauspost's EncodeAll and DecodeAll are safe for concurrent use, so there
// is no pool to refill and nothing waits on finalizers. The window is 512 KiB,
// more than any benchmark message, since klauspost's default of 8 MiB costs
// memory for every concurrent encode; and the decoder caps the memory a
// message may decompress to.
type grpcZstd struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func newGRPCZstd() *grpcZstd {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(512<<10))
	if err != nil {
		panic(err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		panic(err)
	}
	return &grpcZstd{encoder: encoder, decoder: decoder}
}

func (*grpcZstd) Name() string { return grpcZstdName }

func (c *grpcZstd) Compress(w io.Writer) (io.WriteCloser, error) {
	return &zstdMessageWriter{encoder: c.encoder, out: w}, nil
}

func (c *grpcZstd) Decompress(r io.Reader) (io.Reader, error) {
	compressed, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	message, err := c.decoder.DecodeAll(compressed, nil)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(message), nil
}

// zstdMessageWriter collects one message and compresses it on Close.
type zstdMessageWriter struct {
	encoder *zstd.Encoder
	out     io.Writer
	message bytes.Buffer
}

func (w *zstdMessageWriter) Write(p []byte) (int, error) { return w.message.Write(p) }

func (w *zstdMessageWriter) Close() error {
	_, err := w.out.Write(w.encoder.EncodeAll(w.message.Bytes(), nil))
	return err
}

// grpcCallOptions returns the call options that compress every request of a
// call in the compression mode. The server, which registers both compressors
// by importing their packages, answers with the request's encoding.
func grpcCallOptions(compression string) []grpc.CallOption {
	if name, ok := grpcCompressorForModes[compression]; ok {
		return []grpc.CallOption{grpc.UseCompressor(name)}
	}
	return nil
}

// connectClientOptions returns the connect-go client options of the
// compression mode. The client accepts only that encoding, so the handler
// compresses its responses with it; without compression it accepts none,
// since connect-go otherwise gzips every response.
func connectClientOptions(compression string) []connect.ClientOption {
	switch compression {
	case "gzip":
		return []connect.ClientOption{connect.WithSendGzip()}
	case "zstd":
		return []connect.ClientOption{
			connectZstdAtFastest,
			connect.WithSendCompression(connectcompress.Zstandard),
			connect.WithAcceptCompression("gzip", nil, nil),
		}
	default:
		return []connect.ClientOption{connect.WithAcceptCompression("gzip", nil, nil)}
	}
}
