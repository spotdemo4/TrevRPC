package qmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

func TestIdleClockCreditsOneSendPerReceipt(t *testing.T) {
	clock := newIdleClock(100)
	for _, step := range []struct {
		received, sent, want int64
	}{
		{100, 100, 100}, // nothing sent since the receipt
		{100, 150, 150}, // the first send after it restarts the timeout
		{100, 300, 150}, // later sends, such as keep-alives, don't
		{400, 300, 400}, // a receipt does, and earns another send
		{400, 500, 500},
		{400, 900, 500},
	} {
		if got := clock.observe(step.received, step.sent); got != step.want {
			t.Fatalf("observe(%d, %d) = %d, want %d", step.received, step.sent, got, step.want)
		}
	}
}

// TestKeepAliveDoesNotHideVanishedPeer checks that keep-alives to a peer that reads everything
// and answers nothing don't keep the session open: the idle timeout still fires.
func TestKeepAliveDoesNotHideVanishedPeer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		params := testParameters()
		params.MaxIdleTimeout = time.Second
		session, _ := openRaw(t, Config{
			Parameters:      params,
			KeepAlivePeriod: 100 * time.Millisecond,
			CloseTimeout:    50 * time.Millisecond,
		})
		// The first keep-alive restarts the timeout once; the ones after it don't.
		time.Sleep(time.Second)
		synctest.Wait()
		if err := session.Context().Err(); err != nil {
			t.Fatalf("session ended before its idle timeout: %v", context.Cause(session.Context()))
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if err := context.Cause(session.Context()); !errors.Is(err, ErrIdleTimeout) {
			t.Fatalf("session ended with %v, want an idle timeout", err)
		}
	})
}

// TestProbeKeepsOneWaySenderOpen checks that a session that only sends stays open while its peer
// is alive but has nothing to send back: the peer answers its QX_PING probes.
func TestProbeKeepsOneWaySenderOpen(t *testing.T) {
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
		// server sends no MAX_STREAM_DATA: only its answers to the probes reach the client.
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

// TestIdleSessionSendsNoProbe checks that a session idle in both directions times out without
// probing the peer, even when its last record only answered the peer's: a QX_PING response is no
// data waiting for the peer. It runs on the real clock, so that the response goes out after the
// ping arrived, as it does outside synctest's bubble, where time stands still while goroutines run.
func TestIdleSessionSendsNoProbe(t *testing.T) {
	params := testParameters()
	params.MaxIdleTimeout = 200 * time.Millisecond
	conn, result := rawPeer(t, Config{Parameters: params, CloseTimeout: 50 * time.Millisecond})
	var (
		mu       sync.Mutex
		received []byte
	)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			mu.Lock()
			received = append(received, buf[:n]...)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	if _, err := conn.Write(record(appendTransportParameters(nil, &Draft02, testParameters()), appendPing(nil, Draft02.PingType, 0))); err != nil {
		t.Fatal(err)
	}
	opened := <-result
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	select {
	case <-opened.session.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("idle session was not closed")
	}
	if err := context.Cause(opened.session.Context()); !errors.Is(err, ErrIdleTimeout) {
		t.Fatalf("session ended with %v, want an idle timeout", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !bytes.Contains(received, appendPing(nil, Draft02.PingResponseType, 0)) {
		t.Fatal("the peer's QX_PING was not answered")
	}
	if bytes.Contains(received, appendVarint(nil, Draft02.PingType)) {
		t.Fatal("an idle session sent a QX_PING")
	}
}
