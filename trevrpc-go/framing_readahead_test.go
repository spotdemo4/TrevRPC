package trevrpc

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
)

// readAllFrames reads frames until the reader fails and returns them with the
// error that ended the reading.
func readAllFrames(reader *frameReader) ([]frame, error) {
	var frames []frame
	for {
		next, err := reader.next()
		if err != nil {
			return frames, err
		}
		frames = append(frames, next)
	}
}

// TestFrameReaderReadAheadMatchesExactReads checks that reading ahead changes
// nothing a caller can see, whatever way the stream hands out its bytes: the
// frames, skipped unknown frames, and the error that ends the stream, including
// a stream cut inside a frame.
func TestFrameReaderReadAheadMatchesExactReads(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 3000 {
		var stream []byte
		for range rng.IntN(6) {
			size := []int{0, 1, 5, 40, 300}[rng.IntN(5)]
			payload := bytes.Repeat([]byte{byte('a' + rng.IntN(26))}, size)
			typ := []frameType{frameTypeMessage, frameTypeHeaders, 0x40, 0x1f*3 + 0x21}[rng.IntN(4)]
			stream = appendFrameHeader(stream, typ, size)
			stream = append(stream, payload...)
		}
		if rng.IntN(3) == 0 && len(stream) > 0 {
			stream = stream[:rng.IntN(len(stream))]
		}

		wrappers := map[string]func(io.Reader) io.Reader{
			"plain":     func(r io.Reader) io.Reader { return r },
			"one byte":  iotest.OneByteReader,
			"half":      iotest.HalfReader,
			"data+err":  iotest.DataErrReader,
			"one+error": func(r io.Reader) io.Reader { return iotest.DataErrReader(iotest.OneByteReader(r)) },
		}
		for name, wrap := range wrappers {
			exact := newFrameReader(wrap(bytes.NewReader(stream)), 64, 100)
			ahead := newFrameReader(wrap(bytes.NewReader(stream)), 64, 100)
			ahead.readAhead = true
			wantFrames, wantErr := readAllFrames(exact)
			gotFrames, gotErr := readAllFrames(ahead)
			if len(gotFrames) != len(wantFrames) {
				t.Fatalf("%s %x: read %d frames, want %d", name, stream, len(gotFrames), len(wantFrames))
			}
			for index := range wantFrames {
				if gotFrames[index].typ != wantFrames[index].typ || !bytes.Equal(gotFrames[index].payload, wantFrames[index].payload) {
					t.Fatalf("%s %x: frame %d = %v, want %v", name, stream, index, gotFrames[index], wantFrames[index])
				}
			}
			if !errors.Is(gotErr, wantErr) && gotErr.Error() != wantErr.Error() {
				t.Fatalf("%s %x: error = %v, want %v", name, stream, gotErr, wantErr)
			}
		}
	}
}

// TestFrameReaderReadAheadStaysWithinTheFrameUntilEnabled checks that a reader
// takes only the bytes it needs until readAhead is set.
func TestFrameReaderReadAheadStaysWithinTheFrameUntilEnabled(t *testing.T) {
	var stream []byte
	stream = appendFrameHeader(stream, frameTypeMessage, 3)
	stream = append(stream, "abc"...)
	stream = append(stream, "trailing"...)
	source := bytes.NewReader(stream)
	reader := newFrameReader(source, 64, -1)
	if _, err := reader.next(); err != nil {
		t.Fatal(err)
	}
	if source.Len() != len("trailing") {
		t.Fatalf("%d bytes left in the stream, want %d", source.Len(), len("trailing"))
	}
}
