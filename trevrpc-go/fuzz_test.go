package trevrpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func FuzzFrameDecodeNeverPanics(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		{},
		{0xff},
		{0xff, 0xff},
		[]byte("\x0a\x03svc\x12\x01m\x1a\x02hi\x30\x01"),
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, body []byte) {
		request := &wireRequest{}
		err := decodeFrame(body, request)
		if err == nil {
			return
		}

		var decodeErr *frameDecodeError
		if !errors.As(err, &decodeErr) {
			t.Fatalf("expected frameDecodeError for malformed frame body, got %T: %v", err, err)
		}
		if status := StatusFromError(err); status.Code != CodeInvalidArgument {
			t.Fatalf("expected invalid argument status, got %v", status)
		}
	})
}

func FuzzFrameLengthParsing(f *testing.F) {
	for _, length := range []uint32{0, 1, 15, 16, 17, uint32(DefaultMaxFrameSize), uint32(DefaultMaxFrameSize + 1), ^uint32(0)} {
		f.Add(length)
	}

	f.Fuzz(func(t *testing.T, length uint32) {
		header := make([]byte, 4)
		binary.BigEndian.PutUint32(header, length)

		read, err := readFrameOrEOF(bytes.NewReader(header), &wireRequest{}, 16)
		if length > 16 {
			var tooLarge *FrameTooLargeError
			if !errors.As(err, &tooLarge) {
				t.Fatalf("expected FrameTooLargeError for length %d, got read=%t err=%T %v", length, read, err, err)
			}
			return
		}

		if length == 0 {
			if !read || err != nil {
				t.Fatalf("expected empty frame to decode, got read=%t err=%v", read, err)
			}
			return
		}

		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("expected incomplete in-range frame to return EOF, got read=%t err=%v", read, err)
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
