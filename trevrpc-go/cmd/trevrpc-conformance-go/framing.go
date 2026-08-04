package main

import (
	"encoding/hex"
	"errors"
	"io"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type chunkReader struct {
	chunks [][]byte
	chunk  int
	offset int
}

func (r *chunkReader) Read(destination []byte) (int, error) {
	for r.chunk < len(r.chunks) {
		current := r.chunks[r.chunk]
		if r.offset == len(current) {
			r.chunk++
			r.offset = 0
			continue
		}
		read := copy(destination, current[r.offset:])
		r.offset += read
		return read, nil
	}
	return 0, io.EOF
}

func framingDecode(_ string, maxFrameSize int, chunks [][]byte) (map[string]any, *conformanceError) {
	reader := &chunkReader{chunks: chunks}
	bodies := make([]string, 0)
	for {
		body, read, nativeErr := trevrpc.ReadRawFrameOrEOF(reader, maxFrameSize)
		if nativeErr != nil {
			var tooLarge *trevrpc.FrameTooLargeError
			if errors.As(nativeErr, &tooLarge) {
				return nil, &conformanceError{category: "frame_too_large", statusCode: uint32(trevrpc.CodeResourceExhausted), native: nativeErr}
			}
			if errors.Is(nativeErr, io.ErrUnexpectedEOF) || errors.Is(nativeErr, io.EOF) {
				return nil, &conformanceError{category: "incomplete_frame", statusCode: uint32(trevrpc.CodeInternal), native: nativeErr}
			}
			return nil, malformedError("rpc_response", nativeErr)
		}
		if !read {
			return map[string]any{"bodies_hex": bodies, "eof": true}, nil
		}
		bodies = append(bodies, hex.EncodeToString(body))
	}
}
