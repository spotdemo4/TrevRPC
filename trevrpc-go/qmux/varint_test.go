package qmux

import (
	"errors"
	"testing"
)

func TestVarintRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		value uint64
		size  int
	}{
		{0, 1}, {63, 1}, {64, 2}, {16383, 2}, {16384, 4},
		{1073741823, 4}, {1073741824, 8}, {maxVarint, 8},
	} {
		encoded := appendVarint([]byte{0xff}, tc.value)[1:]
		if len(encoded) != tc.size || varintLen(tc.value) != tc.size {
			t.Errorf("%d encodes in %d bytes (varintLen %d), want %d", tc.value, len(encoded), varintLen(tc.value), tc.size)
		}
		value, n, err := parseVarint(append(encoded, 0xaa))
		if err != nil || value != tc.value || n != tc.size {
			t.Errorf("%x decodes as %d, %d, %v", encoded, value, n, err)
		}
		if _, _, err := parseVarint(encoded[:len(encoded)-1]); !errors.Is(err, errTruncatedVarint) {
			t.Errorf("truncated %x decodes with %v", encoded, err)
		}
	}
}

func TestVarintTooLarge(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("encoding 2^62 does not panic")
		}
	}()
	appendVarint(nil, maxVarint+1)
}
