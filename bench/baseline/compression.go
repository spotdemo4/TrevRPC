package main

import (
	"bytes"
	"io"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"github.com/klauspost/compress/zstd"
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
	grpcCompressorForModes = map[string]string{"gzip": grpcgzip.Name, "zstd": zstdName}
)

// zstd runs at klauspost's fastest level, the closest to the zstd level 1
// TrevRPC compresses at; gzip keeps each library's default level.
func init() {
	encoding.RegisterCompressor(grpcZstd{sharedZstd})
}

// zstdName is the grpc-encoding and Content-Encoding of zstd.
const zstdName = "zstd"

// sharedZstd compresses every zstd message of both baselines.
var sharedZstd = newZstdCodec()

// connectZstd is connect-go's zstd compressor.
var connectZstd connect.Compressor = connectZstdCompressor{sharedZstd}

// zstdCodec compresses each message on its own, as the libraries' gzip does.
// One encoder and one decoder serve every call: klauspost's EncodeAll and
// DecodeAll are safe for concurrent use, so there is no pool to refill and
// nothing waits on finalizers. The window is 512 KiB, more than any benchmark
// message, since klauspost's default of 8 MiB costs memory for every
// concurrent encode; and the decoder caps the memory a message may decompress
// to.
type zstdCodec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func newZstdCodec() *zstdCodec {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithWindowSize(512<<10))
	if err != nil {
		panic(err)
	}
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(64<<20))
	if err != nil {
		panic(err)
	}
	return &zstdCodec{encoder: encoder, decoder: decoder}
}

func (c *zstdCodec) compress(w io.Writer) io.WriteCloser {
	return &zstdMessageWriter{encoder: c.encoder, out: w}
}

func (c *zstdCodec) decompress(r io.Reader) (*bytes.Reader, error) {
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

// grpcZstd is gRPC's zstd compressor.
type grpcZstd struct{ codec *zstdCodec }

func (grpcZstd) Name() string { return zstdName }

func (c grpcZstd) Compress(w io.Writer) (io.WriteCloser, error) { return c.codec.compress(w), nil }

func (c grpcZstd) Decompress(r io.Reader) (io.Reader, error) { return c.codec.decompress(r) }

// connectZstdCompressor is connect-go's zstd compressor.
type connectZstdCompressor struct{ codec *zstdCodec }

func (connectZstdCompressor) Name() string { return zstdName }

func (c connectZstdCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return c.codec.compress(w), nil
}

func (c connectZstdCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	message, err := c.codec.decompress(r)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(message), nil
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

// connectClientOptions returns the connect-go transport options of the
// compression mode. The client registers only that encoding, so the handler
// compresses its responses with it; without compression it registers none,
// since connect-go otherwise gzips every response.
func connectClientOptions(compression string) []connecthttp.Option {
	switch compression {
	case "gzip":
		return []connecthttp.Option{connecthttp.WithSendGzip()}
	case "zstd":
		return []connecthttp.Option{
			connecthttp.WithCompressors(connectZstd),
			connecthttp.WithSendCompression(zstdName),
		}
	default:
		return []connecthttp.Option{connecthttp.WithCompressors()}
	}
}
