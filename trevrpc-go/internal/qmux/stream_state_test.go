package qmux

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"slices"
	"testing"
	"time"
)

// completeServerStream has the raw peer send "abc" and a FIN on server-initiated
// stream 1, reads it to EOF and closes the send side, then waits until the
// session has released the stream's slot.
func completeServerStream(t *testing.T, session *Session, conn net.Conn) {
	t.Helper()
	if _, err := conn.Write(record(appendStreamFrame(nil, 1, 0, []byte("abc"), true))); err != nil {
		t.Fatal(err)
	}
	stream, err := session.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if got, err := io.ReadAll(stream); err != nil || string(got) != "abc" {
		t.Fatalf("read = %q, %v", got, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		session.mu.Lock()
		released := stream.removed
		session.mu.Unlock()
		if released {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("stream was never released")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestDataOnReleasedStreamIsFinalSizeError checks that STREAM data for a stream
// whose slot was released ends the session. The stream's final size is behind
// it, and the frame would otherwise skip the stream and connection flow
// control checks, letting a peer send without limit.
func TestDataOnReleasedStreamIsFinalSizeError(t *testing.T) {
	session, conn := openRaw(t, Config{Parameters: testParameters(), CloseTimeout: 10 * time.Millisecond})
	completeServerStream(t, session, conn)

	// Far more than the 1 MiB connection limit, so a session that accepts the
	// data cannot end for flow control reasons without having read all of it.
	chunk := make([]byte, 8<<10)
	total := uint64(4 << 20)
	for offset := uint64(3); offset < 3+total; offset += uint64(len(chunk)) {
		if _, err := conn.Write(record(appendStreamFrame(nil, 1, offset, chunk, false))); err != nil {
			break // the session closed the connection
		}
	}
	select {
	case <-session.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("data on a released stream was accepted")
	}
	var transportErr *TransportError
	if err := context.Cause(session.Context()); !errors.As(err, &transportErr) || transportErr.ErrorCode != FinalSizeError {
		t.Fatalf("session ended with %v, want FINAL_SIZE_ERROR", err)
	}
}

// TestEmptyFrameOnReleasedStreamIsIgnored checks that a repeated FIN or reset
// for a released stream is harmless: it carries no data to account for.
func TestEmptyFrameOnReleasedStreamIsIgnored(t *testing.T) {
	session, conn := openRaw(t, Config{Parameters: testParameters(), CloseTimeout: 10 * time.Millisecond})
	completeServerStream(t, session, conn)

	if _, err := conn.Write(record(appendStreamFrame(nil, 1, 3, nil, true), appendResetStream(nil, 1, 7, 3), appendStopSending(nil, 1, 7))); err != nil {
		t.Fatal(err)
	}
	// A frame after them proves the session processed them without ending.
	stream, err := session.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(record(appendMaxStreamData(nil, stream.id, 1<<20))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-session.Context().Done():
		t.Fatalf("session ended with %v", context.Cause(session.Context()))
	case <-time.After(50 * time.Millisecond):
	}
}

// checkStreamInvariantsLocked verifies the implicit send and receive state
// machines of every stream, and the credit accounting that ties them to the
// session. The caller holds s.mu. initialMaxRemote is the stream limit the
// session advertised at the start and removedRemote the number of peer streams
// it saw complete.
func checkStreamInvariantsLocked(s *Session, streams []*Stream, initialMaxRemote uint64) error {
	var unread uint64
	removedRemote := uint64(0)
	for _, st := range streams {
		switch {
		case st.finSent && st.resetSent:
			return fmt.Errorf("stream %d: finSent && resetSent", st.id)
		case st.finReceived && !st.finalKnown:
			return fmt.Errorf("stream %d: finReceived without finalKnown", st.id)
		case st.finalKnown && st.received != st.finalSize:
			return fmt.Errorf("stream %d: finalKnown but received %d != finalSize %d", st.id, st.received, st.finalSize)
		case st.read > st.received:
			return fmt.Errorf("stream %d: read %d > received %d", st.id, st.read, st.received)
		case st.readCancelled && st.resetReceived != nil:
			return fmt.Errorf("stream %d: readCancelled && resetReceived", st.id)
		case st.readCancelled && st.read != st.received:
			return fmt.Errorf("stream %d: readCancelled but %d bytes stay unread", st.id, st.received-st.read)
		case st.resetReceived != nil && (!st.finalKnown || st.read != st.finalSize || len(st.recvBuf) != st.recvHead):
			return fmt.Errorf("stream %d: resetReceived but data is still deliverable", st.id)
		case st.sendDone() && st.writeErr == nil:
			return fmt.Errorf("stream %d: send side done but Write still succeeds", st.id)
		case st.removed != (s.streams[st.id] == nil):
			return fmt.Errorf("stream %d: removed=%v but map entry present=%v", st.id, st.removed, s.streams[st.id] != nil)
		case st.removed && (!st.sendDone() || !st.recvDone()):
			return fmt.Errorf("stream %d: removed before both sides ended (send %v recv %v)", st.id, st.sendDone(), st.recvDone())
		case !st.removed && st.sendDone() && st.recvDone():
			return fmt.Errorf("stream %d: both sides ended but the slot was never released", st.id)
		case st.inReady && !slices.Contains(s.ready, st):
			return fmt.Errorf("stream %d: inReady but not queued", st.id)
		}
		if !s.isLocal(st.id) && st.removed {
			removedRemote++
		}
		unread += st.received - st.read
	}
	if got := s.connReceived - s.connConsumed; got != unread {
		return fmt.Errorf("connection credit: received-consumed = %d, but streams hold %d unread bytes", got, unread)
	}
	if s.maxRemote != initialMaxRemote+removedRemote {
		return fmt.Errorf("maxRemote = %d, want %d + %d completed", s.maxRemote, initialMaxRemote, removedRemote)
	}
	seen := map[*Stream]bool{}
	for _, st := range s.ready {
		if seen[st] {
			return fmt.Errorf("stream %d queued twice", st.id)
		}
		seen[st] = true
	}
	return nil
}

// TestStreamStateMachineInvariants drives one session with random peer frames
// (mostly valid, some not) interleaved with random local calls, and checks the
// state invariants after every step. Each run is seeded by its number, so a
// failure reproduces: the message names the run and lists the steps it took.
func TestStreamStateMachineInvariants(t *testing.T) {
	const runs = 1500
	removed, resets := 0, 0
	for run := range runs {
		rng := rand.New(rand.NewPCG(uint64(run), 42))
		session, _ := openRaw(t, Config{Parameters: testParameters(), CloseTimeout: 10 * time.Millisecond})
		var all []*Stream
		var trace []string
		known := map[uint64]*Stream{}
		note := func(format string, args ...any) { trace = append(trace, fmt.Sprintf(format, args...)) }
		track := func(st *Stream) {
			if _, ok := known[st.id]; !ok {
				known[st.id] = st
				all = append(all, st)
			}
		}
		fail := func(err error) {
			t.Fatalf("run %d: %v\ntrace:\n%v", run, err, trace)
		}
		cancelled, cancel := context.WithCancel(context.Background())
		cancel()
		// Every stream the session creates for the peer lands in the accept
		// queue, so tracking is: queue after each peer frame, local opens.
		syncTracked := func() {
			session.mu.Lock()
			for _, st := range session.streams {
				track(st)
			}
			session.mu.Unlock()
		}
		peerIDs := []uint64{1, 5, 9}
		steps := func() {
			for range 40 {
				// Choose a stream: peer-opened or local.
				var st *Stream
				var id uint64
				if rng.IntN(2) == 0 {
					id = peerIDs[rng.IntN(len(peerIDs))]
				} else {
					id = uint64(rng.IntN(3)) << 2
				}
				session.mu.Lock()
				st = session.streams[id]
				if st == nil {
					st = known[id]
				}
				session.mu.Unlock()
				var received, finalSize uint64
				var finalKnown bool
				if st != nil {
					session.mu.Lock()
					received, finalSize, finalKnown = st.received, st.finalSize, st.finalKnown
					session.mu.Unlock()
				}
				// The peer cannot name a local stream before it was opened.
				if st == nil && id&1 == 0 {
					id = peerIDs[rng.IntN(len(peerIDs))]
					st = known[id]
					if st != nil {
						session.mu.Lock()
						received, finalSize, finalKnown = st.received, st.finalSize, st.finalKnown
						session.mu.Unlock()
					}
				}
				switch op := rng.IntN(11); op {
				case 0, 1, 2:
					offset := received
					if rng.IntN(10) == 0 {
						offset += uint64(rng.IntN(3))
					}
					data := make([]byte, rng.IntN(6))
					if finalKnown {
						data = nil
					}
					fin := rng.IntN(4) == 0
					note("peer STREAM id=%d off=%d len=%d fin=%v", id, offset, len(data), fin)
					if err := session.handleRecord(appendStreamFrame(nil, id, offset, data, fin)); err != nil {
						note("  -> %v", err)
						return
					}
				case 3:
					size := received + uint64(rng.IntN(3))
					if finalKnown {
						size = finalSize
					}
					note("peer RESET_STREAM id=%d code=7 final=%d", id, size)
					if err := session.handleRecord(appendResetStream(nil, id, 7, size)); err != nil {
						note("  -> %v", err)
						return
					}
				case 4:
					note("peer STOP_SENDING id=%d", id)
					if err := session.handleRecord(appendStopSending(nil, id, 9)); err != nil {
						note("  -> %v", err)
						return
					}
				case 5:
					note("peer MAX_STREAM_DATA id=%d", id)
					if err := session.handleRecord(appendMaxStreamData(nil, id, 1<<20)); err != nil {
						note("  -> %v", err)
						return
					}
				case 6:
					if st != nil {
						note("local Write id=%d", id)
						_ = st.SetWriteDeadline(time.Now().Add(-time.Second))
						_, _ = st.Write([]byte("xyz"))
					}
				case 7:
					if st != nil {
						note("local Close id=%d", id)
						_ = st.Close()
					}
				case 8:
					if st != nil {
						note("local CancelWrite id=%d", id)
						st.CancelWrite(5)
					}
				case 9:
					if st != nil {
						note("local CancelRead id=%d", id)
						st.CancelRead(6)
					}
				case 10:
					if st != nil {
						note("local Read id=%d", id)
						_ = st.SetReadDeadline(time.Now().Add(-time.Second))
						_, _ = st.Read(make([]byte, 1+rng.IntN(4)))
					}
				}
				if rng.IntN(5) == 0 {
					if opened, err := session.OpenStreamSync(cancelled); err == nil {
						note("local open id=%d", opened.id)
						track(opened)
					}
				}
				// Let the writer frame pending FINs so that Data Sent is reached.
				for range 200 {
					session.mu.Lock()
					pending := false
					for _, st := range all {
						pending = pending || (st.finRequested && !st.resetSent && !st.finSent)
					}
					session.mu.Unlock()
					if !pending {
						break
					}
					time.Sleep(50 * time.Microsecond)
				}
				syncTracked()
				session.mu.Lock()
				err := checkStreamInvariantsLocked(session, all, testParameters().InitialMaxStreamsBidi)
				session.mu.Unlock()
				if err != nil {
					fail(err)
				}
			}
		}
		steps()
		syncTracked()
		session.mu.Lock()
		err := checkStreamInvariantsLocked(session, all, testParameters().InitialMaxStreamsBidi)
		session.mu.Unlock()
		if err != nil {
			fail(err)
		}
		session.mu.Lock()
		for _, st := range all {
			if st.removed {
				removed++
			}
			if st.resetReceived != nil {
				resets++
			}
		}
		session.mu.Unlock()
		_ = session.CloseWithError(0, "")
	}
	t.Logf("%d streams removed, %d with a received reset over %d runs", removed, resets, runs)
}
