// Package zstd implements TrevRPC's ZSTD_MESSAGE payloads: magicless zstd frames
// compressed with a dictionary both peers hold (see the "Compressed Messages"
// section of the wire format).
//
// Frame validation is pure Go, so payloads are checked the same way with or
// without cgo. Compression and decompression call libzstd through cgo and are
// unavailable when cgo is disabled; see Available.
package zstd

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

// dictMagic opens every zstd-format dictionary (RFC 8878, section 5).
const dictMagic = 0xEC30A437

var (
	// ErrInvalid reports a payload that is not a single magicless zstd frame
	// with a Dictionary_ID and a Frame_Content_Size, or that fails to
	// decompress to exactly that size.
	ErrInvalid = errors.New("zstd: invalid ZSTD_MESSAGE payload")
	// ErrUnavailable reports that the binary was built without cgo, so it
	// cannot compress or decompress.
	ErrUnavailable = errors.New("zstd: compression requires cgo")
)

// Header is what a receiver needs from a ZSTD_MESSAGE payload before
// decompressing it.
type Header struct {
	// DictID is the Dictionary_ID; never zero.
	DictID uint32
	// ContentSize is the Frame_Content_Size: the size of the message.
	ContentSize uint64
}

// ParseHeader validates that payload is exactly one magicless zstd frame with a
// non-zero Dictionary_ID and a Frame_Content_Size, and returns both. It walks
// the block headers to find the frame's end, so trailing bytes, including a
// second frame, are rejected. Block contents are not checked; decompression
// does that.
func ParseHeader(payload []byte) (Header, error) {
	if len(payload) == 0 {
		return Header{}, ErrInvalid
	}
	fhd := payload[0]
	fcsFlag := fhd >> 6
	singleSegment := fhd&0x20 != 0
	if fhd&0x08 != 0 { // reserved bit
		return Header{}, ErrInvalid
	}
	checksum := fhd&0x04 != 0
	didSize := [4]int{0, 1, 2, 4}[fhd&0x03]
	fcsSize := [4]int{0, 2, 4, 8}[fcsFlag]
	if fcsFlag == 0 && singleSegment {
		fcsSize = 1
	}
	if didSize == 0 || fcsSize == 0 {
		return Header{}, ErrInvalid
	}
	pos := 1
	if !singleSegment {
		pos++ // Window_Descriptor
	}
	if len(payload) < pos+didSize+fcsSize {
		return Header{}, ErrInvalid
	}
	var h Header
	for i := range didSize {
		h.DictID |= uint32(payload[pos+i]) << (8 * i)
	}
	pos += didSize
	for i := range fcsSize {
		h.ContentSize |= uint64(payload[pos+i]) << (8 * i)
	}
	if fcsSize == 2 {
		h.ContentSize += 256
	}
	pos += fcsSize
	if h.DictID == 0 {
		return Header{}, ErrInvalid
	}
	for {
		if len(payload)-pos < 3 {
			return Header{}, ErrInvalid
		}
		bh := uint32(payload[pos]) | uint32(payload[pos+1])<<8 | uint32(payload[pos+2])<<16
		pos += 3
		size := int(bh >> 3)
		switch (bh >> 1) & 0x03 {
		case 0, 2: // Raw_Block, Compressed_Block
		case 1: // RLE_Block
			size = 1
		default:
			return Header{}, ErrInvalid
		}
		if len(payload)-pos < size {
			return Header{}, ErrInvalid
		}
		pos += size
		if bh&1 != 0 { // Last_Block
			break
		}
	}
	if checksum {
		pos += 4
	}
	if pos != len(payload) {
		return Header{}, ErrInvalid
	}
	return h, nil
}

// Fingerprint returns a dictionary's fingerprint: the first 8 bytes of its
// SHA-256 digest.
func Fingerprint(dict []byte) [8]byte {
	sum := sha256.Sum256(dict)
	return [8]byte(sum[:8])
}

// DictID returns the Dictionary_ID of a zstd-format dictionary, or an error if
// dict is not one (raw-content dictionaries have no ID) or its ID is zero.
func DictID(dict []byte) (uint32, error) {
	if len(dict) < 8 || binary.LittleEndian.Uint32(dict) != dictMagic {
		return 0, errors.New("zstd: not a zstd-format dictionary")
	}
	id := binary.LittleEndian.Uint32(dict[4:])
	if id == 0 {
		return 0, errors.New("zstd: dictionary ID is zero")
	}
	return id, nil
}

// CompressBound is the largest payload Compress writes for n bytes: libzstd's
// ZSTD_COMPRESSBOUND.
func CompressBound(n int) int {
	bound := n + n>>8
	if n < 128<<10 {
		bound += (128<<10 - n) >> 11
	}
	return bound
}
