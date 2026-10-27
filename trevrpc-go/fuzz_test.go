package trevrpc

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/quic-go/quic-go/quicvarint"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

func FuzzFrameDecodeNeverPanics(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{},
		{0xff},
		{0xff, 0xff},
		[]byte("\x0a\x03svc\x12\x01m\x30\x01"),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, payload []byte) {
		for _, decode := range []func([]byte) error{
			func(payload []byte) error { return decodePayload(payload, &wireRequest{}) },
			func(payload []byte) error { _, err := decodeHeaders(payload); return err },
			func(payload []byte) error { _, err := decodeAnswer(payload); return err },
			func(payload []byte) error { _, err := decodeStatus(payload); return err },
		} {
			err := decode(payload)
			if err == nil {
				continue
			}

			if _, ok := errors.AsType[*frameDecodeError](err); !ok {
				t.Fatalf("expected frameDecodeError for malformed frame payload, got %T: %v", err, err)
			}
			if status := StatusFromError(err); status.Code != CodeInvalidArgument {
				t.Fatalf("expected invalid argument status, got %v", status)
			}
		}
	})
}

func FuzzFrameReaderNeverPanics(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{0x00, 0x00},
		{0x03, 0x00},
		{0x00, 0x02, 'h', 'i'},
		{0x21, 0x01, 0xff, 0x03, 0x00},
		{0x40, 0x40, 0x01, 0xff},
		{0xc0, 0, 0, 0, 0, 0, 0, 0},
		{0x00, 0xff},
		{0x04, 0x01, 0x00},
		{0x05, 0x01, 0x00},
		// A stream-compressed "hi" (see the stream vectors), then a second chunk.
		{0x05, 0x0a, 0x02, 0x00, 0x30, 0x10, 0x00, 0x00, 'h', 'i', 0x05, 0x04},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := newFrameReader(bytes.NewReader(data), 64, -1)
		// Odd inputs enable stream compression, so random chunks reach libzstd.
		if len(data) > 0 && data[len(data)-1]&1 == 1 && zstd.Available {
			reader.stream = newStreamInflater(nil, nil)
			defer reader.stream.close()
		}
		// Without accepted dictionaries a ZSTD_MESSAGE frame is a decode error.
		for range len(data) + 1 {
			next, err := reader.next()
			if err != nil {
				if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
					_, tooLarge := errors.AsType[*FrameTooLargeError](err)
					_, undecodable := errors.AsType[*frameDecodeError](err)
					if !tooLarge && !undecodable {
						t.Fatalf("unexpected frame reader error %T: %v", err, err)
					}
				}
				return
			}
			if !next.typ.known() || next.typ == frameTypeZstdMessage || next.typ == frameTypeZstdStreamMessage || len(next.payload) > 64 {
				t.Fatalf("frame reader returned %v with %d payload bytes", next.typ, len(next.payload))
			}
		}
	})
}

func FuzzFrameLengthParsing(f *testing.F) {
	for _, length := range []uint64{0, 1, 15, 16, 17, uint64(DefaultMaxFrameSize), uint64(DefaultMaxFrameSize + 1), 1<<62 - 1} {
		f.Add(length)
	}

	f.Fuzz(func(t *testing.T, length uint64) {
		length &= 1<<62 - 1
		header := quicvarint.Append([]byte{byte(frameTypeMessage)}, length)

		next, err := newFrameReader(bytes.NewReader(header), 16, -1).next()
		if length > 16 {
			if _, ok := errors.AsType[*FrameTooLargeError](err); !ok {
				t.Fatalf("expected FrameTooLargeError for length %d, got frame=%v err=%T %v", length, next, err, err)
			}
			return
		}

		if length == 0 {
			if err != nil || next.typ != frameTypeMessage || len(next.payload) != 0 {
				t.Fatalf("expected empty frame to decode, got %v err=%v", next, err)
			}
			return
		}

		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected incomplete in-range frame to return ErrUnexpectedEOF, got err=%v", err)
		}
	})
}

func FuzzMetadataValidationDeterministic(f *testing.F) {
	f.Add("authorization", []byte("Bearer token"))
	f.Add("Authorization", []byte("Bearer token"))
	f.Add("trevrpc-timeout", []byte("1"))
	f.Add("bad key", []byte("value"))
	f.Add("", []byte("value"))

	f.Fuzz(func(t *testing.T, key string, value []byte) {
		metadata := Metadata{key: value}
		first := StatusFromError(validateMetadata(metadata)).Code
		second := StatusFromError(validateMetadata(metadata)).Code
		if first != second {
			t.Fatalf("metadata validation was not deterministic for key %q: %v != %v", key, first, second)
		}
		if first != CodeOK && first != CodeInvalidArgument {
			t.Fatalf("metadata validation returned non-canonical status %v", first)
		}
	})
}
