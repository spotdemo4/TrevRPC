package qmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// wantSessionError waits for session to end and checks that it ended with a
// transport error of code.
func wantSessionError(t *testing.T, session *Session, code TransportErrorCode) {
	t.Helper()
	<-session.Context().Done()
	var transportErr *TransportError
	if err := context.Cause(session.Context()); !errors.As(err, &transportErr) || transportErr.ErrorCode != code {
		t.Fatalf("session ended with %v, want %s", err, code)
	}
}

func TestUniStreamsCarryDataBothWays(t *testing.T) {
	client, server := defaultPair(t)
	payload := bytes.Repeat([]byte("unidirectional "), 40<<10)
	for _, pair := range [][2]*Session{{client, server}, {server, client}} {
		sender, receiver := pair[0], pair[1]
		stream, err := sender.OpenUniStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if streamDir(stream.StreamID()) != dirUni || !sender.isLocal(stream.StreamID()) {
			t.Fatalf("opened stream %d is not a local unidirectional stream", stream.StreamID())
		}
		go func() {
			_, _ = stream.Write(payload)
			_ = stream.Close()
		}()
		accepted, err := receiver.AcceptUniStream(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if accepted.StreamID() != stream.StreamID() {
			t.Fatalf("accepted stream %d, opened %d", accepted.StreamID(), stream.StreamID())
		}
		got, err := io.ReadAll(accepted)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("read %d bytes, %v; want %d", len(got), err, len(payload))
		}
		select {
		case <-stream.Context().Done():
		case <-time.After(5 * time.Second):
			t.Fatal("send stream context not cancelled after Close")
		}
	}
	// Bidirectional and unidirectional streams are counted apart.
	if _, err := client.OpenStream(); err != nil {
		t.Fatalf("bidirectional stream after unidirectional ones: %v", err)
	}
}

func TestUniStreamLimitRisesAsStreamsComplete(t *testing.T) {
	serverParams := testParameters()
	serverParams.InitialMaxStreamsUni = 1
	client, server := sessionPair(t, Config{Parameters: testParameters()}, Config{Parameters: serverParams})

	first, err := client.OpenUniStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenUniStream(); !errors.Is(err, ErrStreamLimitReached) {
		t.Fatalf("second stream opened beyond the limit: %v", err)
	}
	// The bidirectional limit is untouched.
	if _, err := client.OpenStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	accepted, err := server.AcceptUniStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(accepted); err != nil || string(got) != "x" {
		t.Fatalf("read %q, %v", got, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := client.OpenUniStreamSync(ctx); err != nil {
		t.Fatalf("stream limit did not rise after the first stream completed: %v", err)
	}
}

func TestUniStreamCancelReadStopsSender(t *testing.T) {
	client, server := defaultPair(t)
	stream, err := client.OpenUniStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	accepted, err := server.AcceptUniStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	accepted.CancelRead(9)
	select {
	case <-stream.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("STOP_SENDING did not cancel the send stream")
	}
	_, err = stream.Write([]byte("more"))
	var streamErr *StreamError
	if !errors.As(err, &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 9 {
		t.Fatalf("write after STOP_SENDING = %v, want remote stream error 9", err)
	}
}

func TestUniStreamCancelWriteResetsReceiver(t *testing.T) {
	client, server := defaultPair(t)
	stream, err := client.OpenUniStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	accepted, err := server.AcceptUniStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stream.CancelWrite(4)
	_, err = io.ReadAll(accepted)
	var streamErr *StreamError
	if !errors.As(err, &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 4 {
		t.Fatalf("read after reset = %v, want remote stream error 4", err)
	}
}

// TestUniStreamDirectionViolations checks that frames only the other side of
// a unidirectional stream may send end the session with STREAM_STATE_ERROR
// (RFC 9000 §19).
func TestUniStreamDirectionViolations(t *testing.T) {
	for _, tc := range []struct {
		name string
		// local opens a client unidirectional stream (ID 2) first.
		local bool
		frame []byte
	}{
		{"STREAM on a send-only stream", true, appendStreamFrame(nil, 2, 0, []byte("x"), false)},
		{"RESET_STREAM on a send-only stream", true, appendResetStream(nil, 2, 1, 0)},
		{"STREAM_DATA_BLOCKED on a send-only stream", true, []byte{frameTypeStreamDataBlocked, 2, 0}},
		{"MAX_STREAM_DATA on a receive-only stream", false, appendMaxStreamData(nil, 3, 100)},
		{"STOP_SENDING on a receive-only stream", false, appendStopSending(nil, 3, 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, conn := openRaw(t, Config{Parameters: testParameters()})
			if tc.local {
				if _, err := session.OpenUniStream(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := conn.Write(record(tc.frame)); err != nil {
				t.Fatal(err)
			}
			wantSessionError(t, session, StreamStateError)
		})
	}
}

func TestUniStreamBeyondLimitIsStreamLimitError(t *testing.T) {
	params := testParameters()
	params.InitialMaxStreamsUni = 0
	session, conn := openRaw(t, Config{Parameters: params})
	// Server-initiated unidirectional stream 3 exceeds a limit of zero.
	if _, err := conn.Write(record(appendStreamFrame(nil, 3, 0, []byte("x"), false))); err != nil {
		t.Fatal(err)
	}
	wantSessionError(t, session, StreamLimitError)
}

func TestOpenStreamFailsAtTheLimit(t *testing.T) {
	serverParams := testParameters()
	serverParams.InitialMaxStreamsBidi = 1
	client, _ := sessionPair(t, Config{Parameters: testParameters()}, Config{Parameters: serverParams})
	if _, err := client.OpenStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.OpenStream(); !errors.Is(err, ErrStreamLimitReached) {
		t.Fatalf("second stream = %v, want ErrStreamLimitReached", err)
	}
}
