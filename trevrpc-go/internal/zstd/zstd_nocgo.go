//go:build !cgo

package zstd

// Available reports whether this binary can compress and decompress.
const Available = false

// Dict is a dictionary digested for both directions. Without cgo none can be
// created.
type Dict struct {
	ID          uint32
	Fingerprint [8]byte
}

// NewPlain returns a Dict without a dictionary, which cannot compress or
// decompress without cgo either.
func NewPlain(int) *Dict { return &Dict{} }

// NewDict always fails with ErrUnavailable: compression requires cgo.
func NewDict([]byte, int) (*Dict, error) { return nil, ErrUnavailable }

// Compress always fails with ErrUnavailable.
func (*Dict) Compress(dst, _ []byte) ([]byte, error) { return dst, ErrUnavailable }

// Decompress always fails with ErrUnavailable.
func (*Dict) Decompress(dst, _ []byte, _ Header) ([]byte, error) { return dst, ErrUnavailable }

// StreamEncoder compresses the messages of one direction of a call. Without
// cgo none can be created.
type StreamEncoder struct{}

// NewStreamEncoder always fails with ErrUnavailable: compression requires cgo.
func NewStreamEncoder(*Dict, int, int) (*StreamEncoder, error) { return nil, ErrUnavailable }

// Encode always fails with ErrUnavailable.
func (*StreamEncoder) Encode(dst, _ []byte) ([]byte, error) { return dst, ErrUnavailable }

// Close does nothing.
func (*StreamEncoder) Close() {}

// StreamDecoder decompresses the messages of one direction of a call. Without
// cgo none can be created.
type StreamDecoder struct{}

// NewStreamDecoder always fails with ErrUnavailable: compression requires cgo.
func NewStreamDecoder(int) (*StreamDecoder, error) { return nil, ErrUnavailable }

// Decode always fails with ErrUnavailable.
func (*StreamDecoder) Decode(dst, _ []byte, _ StreamChunk, _ *Dict, _ int) ([]byte, error) {
	return dst, ErrUnavailable
}

// Close does nothing.
func (*StreamDecoder) Close() {}

// StreamEncoderCost is zero: without cgo there are no encoders.
func StreamEncoderCost(int, int) int64 { return 0 }

// StreamDecoderCost is zero: without cgo there are no decoders.
func StreamDecoderCost(int) int64 { return 0 }
