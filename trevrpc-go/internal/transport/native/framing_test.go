package native

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func framedBytes(bodies ...[]byte) []byte {
	var encoded []byte
	for _, body := range bodies {
		var prefix [4]byte
		binary.BigEndian.PutUint32(prefix[:], uint32(len(body)))
		encoded = append(encoded, prefix[:]...)
		encoded = append(encoded, body...)
	}
	return encoded
}

func TestFrameWriterBuffersAndSplits(t *testing.T) {
	var sent [][]byte
	writer := frameWriter{
		maxFrameSize: 16,
		send: func(body []byte) error {
			sent = append(sent, append([]byte(nil), body...))
			return nil
		},
	}
	encoded := framedBytes([]byte("first"), []byte("second"))
	for _, chunk := range [][]byte{encoded[:2], encoded[2:11], encoded[11:]} {
		if written, err := writer.Write(chunk); err != nil || written != len(chunk) {
			t.Fatalf("Write() = %d, %v, want %d, nil", written, err, len(chunk))
		}
	}
	if err := writer.finish(); err != nil {
		t.Fatalf("finish() error = %v", err)
	}
	if len(sent) != 2 || !bytes.Equal(sent[0], []byte("first")) || !bytes.Equal(sent[1], []byte("second")) {
		t.Fatalf("sent frames = %q, want [first second]", sent)
	}
}

func TestFrameWriterRejectsPartialAndOversizedFrames(t *testing.T) {
	partial := frameWriter{maxFrameSize: 16, send: func([]byte) error { return nil }}
	if _, err := partial.Write([]byte{0, 0, 0}); err != nil {
		t.Fatalf("partial Write() error = %v", err)
	}
	if err := partial.finish(); err == nil {
		t.Fatal("partial finish() returned nil error")
	}

	oversized := frameWriter{maxFrameSize: 3, send: func([]byte) error { return nil }}
	encoded := framedBytes([]byte("four"))
	if written, err := oversized.Write(encoded); err == nil || written != len(encoded) {
		t.Fatalf("oversized Write() = %d, %v, want %d, error", written, err, len(encoded))
	}
}

func TestFrameWriterFailureDoesNotWaitForSend(t *testing.T) {
	sendStarted := make(chan struct{})
	releaseSend := make(chan struct{})
	writer := frameWriter{
		maxFrameSize: 16,
		send: func([]byte) error {
			close(sendStarted)
			<-releaseSend
			return nil
		},
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := writer.Write(framedBytes([]byte("body")))
		writeDone <- err
	}()
	<-sendStarted

	terminalErr := errors.New("stream terminated")
	failDone := make(chan struct{})
	go func() {
		writer.fail(terminalErr)
		close(failDone)
	}()
	<-failDone
	close(releaseSend)
	if err := <-writeDone; !errors.Is(err, terminalErr) {
		t.Fatalf("Write() error = %v, want terminal error", err)
	}
}

func TestFrameReaderCancelDiscardsReconstructedBytes(t *testing.T) {
	received := false
	reader := frameReader{
		maxFrameSize: 16,
		receive: func() ([]byte, error) {
			if received {
				return nil, io.EOF
			}
			received = true
			return []byte("body"), nil
		},
	}
	var prefix [2]byte
	if read, err := reader.Read(prefix[:]); err != nil || read != len(prefix) {
		t.Fatalf("initial Read() = %d, %v", read, err)
	}

	cancelErr := errors.New("read canceled")
	reader.cancel(cancelErr)
	buffer := make([]byte, 16)
	if read, err := reader.Read(buffer); read != 0 || !errors.Is(err, cancelErr) {
		t.Fatalf("Read() after cancel = %d, %v, want 0, cancellation", read, err)
	}
}

func TestFrameReaderSynthesizesPrefixes(t *testing.T) {
	bodies := [][]byte{[]byte("first"), []byte("second")}
	reader := frameReader{
		maxFrameSize: 16,
		receive: func() ([]byte, error) {
			if len(bodies) == 0 {
				return nil, io.EOF
			}
			body := bodies[0]
			bodies = bodies[1:]
			return body, nil
		},
	}

	var actual bytes.Buffer
	buffer := make([]byte, 3)
	for {
		read, err := reader.Read(buffer)
		actual.Write(buffer[:read])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read() error = %v", err)
		}
	}
	want := framedBytes([]byte("first"), []byte("second"))
	if !bytes.Equal(actual.Bytes(), want) {
		t.Fatalf("read bytes = %x, want %x", actual.Bytes(), want)
	}
}
