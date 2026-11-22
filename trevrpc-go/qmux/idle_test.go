package qmux

import (
	"context"
	"io"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// TestOneWaySenderStaysOpen checks that a session that only sends stays open while its peer reads
// and sends nothing back: as draft-ietf-quic-qmux-02 section 7.1 defines the idle timeout, the
// records it sends restart it.
func TestOneWaySenderStaysOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := testParameters()
		params.MaxIdleTimeout = time.Second
		clientConn, serverConn := net.Pipe()
		client, server := openPair(t, clientConn, serverConn, Config{Parameters: params}, Config{Parameters: testParameters()})
		go func() {
			stream, err := server.AcceptStream(t.Context())
			if err == nil {
				_, _ = io.Copy(io.Discard, stream)
			}
		}()
		stream, err := client.OpenStreamSync(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		// A byte every 100 ms for five idle timeouts never uses up the stream window, so the
		// server sends no MAX_STREAM_DATA and nothing else either.
		for range 50 {
			if _, err := stream.Write([]byte{1}); err != nil {
				t.Fatalf("write: %v (session: %v)", err, context.Cause(client.Context()))
			}
			time.Sleep(100 * time.Millisecond)
		}
		synctest.Wait()
		if err := client.Context().Err(); err != nil {
			t.Fatalf("sending session timed out: %v", context.Cause(client.Context()))
		}
	})
}
