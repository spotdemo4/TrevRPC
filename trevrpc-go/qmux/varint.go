package qmux

import (
	"errors"
	"fmt"
)

// maxVarint is the largest value a variable-length integer holds (RFC 9000
// §16).
const maxVarint = 1<<62 - 1

var errTruncatedVarint = errors.New("truncated variable-length integer")

// varintLen returns the encoded size of value.
func varintLen(value uint64) int {
	switch {
	case value <= 63:
		return 1
	case value <= 16383:
		return 2
	case value <= 1073741823:
		return 4
	case value <= maxVarint:
		return 8
	default:
		panic(fmt.Sprintf("qmux: %#x does not fit in a variable-length integer", value))
	}
}

// appendVarint appends value in its shortest encoding. It panics for a value
// above maxVarint.
func appendVarint(b []byte, value uint64) []byte {
	switch varintLen(value) {
	case 1:
		return append(b, byte(value))
	case 2:
		return append(b, byte(value>>8)|0x40, byte(value))
	case 4:
		return append(b, byte(value>>24)|0x80, byte(value>>16), byte(value>>8), byte(value))
	default:
		return append(b, byte(value>>56)|0xc0, byte(value>>48), byte(value>>40), byte(value>>32),
			byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
	}
}

// parseVarint decodes the variable-length integer at the start of b and
// returns it with its encoded size.
func parseVarint(b []byte) (uint64, int, error) {
	if len(b) == 0 {
		return 0, 0, errTruncatedVarint
	}
	n := 1 << (b[0] >> 6)
	if len(b) < n {
		return 0, 0, errTruncatedVarint
	}
	value := uint64(b[0] & 0x3f)
	for _, c := range b[1:n] {
		value = value<<8 | uint64(c)
	}
	return value, n, nil
}
