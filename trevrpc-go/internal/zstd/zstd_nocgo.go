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

// NewDict always fails with ErrUnavailable: compression requires cgo.
func NewDict([]byte, int) (*Dict, error) { return nil, ErrUnavailable }

// Compress always fails with ErrUnavailable.
func (*Dict) Compress(dst, _ []byte) ([]byte, error) { return dst, ErrUnavailable }

// Decompress always fails with ErrUnavailable.
func (*Dict) Decompress(dst, _ []byte, _ Header) ([]byte, error) { return dst, ErrUnavailable }
