// Package zstd implements TrevRPC's ZSTD_MESSAGE payloads: magicless zstd frames
// compressed with a dictionary both peers hold, or without one (see the
// "Compressed Messages" section of the wire format).
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
	// with a Frame_Content_Size, or that fails to decompress to exactly that
	// size.
	ErrInvalid = errors.New("zstd: invalid ZSTD_MESSAGE payload")
	// ErrUnavailable reports that the binary was built without cgo, so it
	// cannot compress or decompress.
	ErrUnavailable = errors.New("zstd: compression requires cgo")
)

// Header is what a receiver needs from a ZSTD_MESSAGE payload before
// decompressing it.
type Header struct {
	// DictID is the Dictionary_ID, zero for a frame without a dictionary.
	DictID uint32
	// ContentSize is the Frame_Content_Size: the size of the message.
	ContentSize uint64
}

// ParseHeader validates that payload is exactly one magicless zstd frame with a
// Frame_Content_Size, and returns it and the Dictionary_ID, which is zero when
// the frame has none or names zero. It walks
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
	if fcsSize == 0 {
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
	for {
		next, last, ok := nextBlock(payload, pos)
		if !ok {
			return Header{}, ErrInvalid
		}
		pos = next
		if last {
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

// nextBlock walks the block whose header starts at pos and returns where the
// next block starts and whether this one is the Last_Block. It reports false
// for a reserved block type or a block that does not fit in data. Block
// contents are not checked.
func nextBlock(data []byte, pos int) (next int, last bool, ok bool) {
	if len(data)-pos < 3 {
		return 0, false, false
	}
	bh := uint32(data[pos]) | uint32(data[pos+1])<<8 | uint32(data[pos+2])<<16
	pos += 3
	size := int(bh >> 3)
	switch (bh >> 1) & 0x03 {
	case 0, 2: // Raw_Block, Compressed_Block
	case 1: // RLE_Block
		size = 1
	default:
		return 0, false, false
	}
	if len(data)-pos < size {
		return 0, false, false
	}
	return pos + size, bh&1 != 0, true
}

// StreamWindowLog is the window, as a base-2 logarithm, that TrevRPC's stream
// compression contexts use and the largest its receivers decode: the wire
// format fixes it at 64 KiB.
const StreamWindowLog = 16

// StreamChunk is what a receiver learns from validating the zstd data of a
// ZSTD_STREAM_MESSAGE.
type StreamChunk struct {
	// StartsFrame reports that the chunk begins with a frame header, and
	// DictID is that frame's Dictionary_ID, zero for none.
	StartsFrame bool
	DictID      uint32
	// EndsFrame reports that the chunk's last block ends the frame, so the
	// next chunk starts a new one.
	EndsFrame bool
}

// ParseStreamChunk validates chunk, the zstd data of a ZSTD_STREAM_MESSAGE
// (see the "Stream Compression" section of the wire format). atFrameStart
// says that no frame is open, so a non-empty chunk must start with a frame
// header: one without Single_Segment_flag, Frame_Content_Size_flag,
// Content_Checksum_flag or the reserved bit, whose window is at most
// 2^maxWindowLog. It then walks the block headers: a chunk is whole blocks,
// and a Last_Block ends it. An empty chunk is an empty message and changes
// nothing. Block contents are not checked; decompression does that.
func ParseStreamChunk(chunk []byte, atFrameStart bool, maxWindowLog int) (StreamChunk, error) {
	var result StreamChunk
	if len(chunk) == 0 {
		return result, nil
	}
	pos := 0
	if atFrameStart {
		fhd := chunk[0]
		// Frame_Content_Size_flag (bits 7-6), Single_Segment_flag (bit 5), the
		// reserved bit (bit 3) and Content_Checksum_flag (bit 2) must be unset.
		if fhd&0xEC != 0 || len(chunk) < 2 {
			return StreamChunk{}, ErrInvalid
		}
		wd := chunk[1]
		base := uint64(1) << (10 + uint64(wd>>3))
		if base+base/8*uint64(wd&7) > uint64(1)<<maxWindowLog {
			return StreamChunk{}, ErrInvalid
		}
		didSize := [4]int{0, 1, 2, 4}[fhd&0x03]
		pos = 2 + didSize
		if len(chunk) < pos {
			return StreamChunk{}, ErrInvalid
		}
		for i := range didSize {
			result.DictID |= uint32(chunk[2+i]) << (8 * i)
		}
		result.StartsFrame = true
	}
	for pos < len(chunk) {
		next, last, ok := nextBlock(chunk, pos)
		if !ok || (last && next != len(chunk)) {
			return StreamChunk{}, ErrInvalid
		}
		pos = next
		result.EndsFrame = last
	}
	return result, nil
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
