package qmux

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func datagramParameters(maxFrameSize uint64) Parameters {
	params := testParameters()
	params.MaxDatagramFrameSize = maxFrameSize
	return params
}

func TestDatagramsArriveInOrder(t *testing.T) {
	client, server := sessionPair(t,
		Config{Parameters: datagramParameters(1200)}, Config{Parameters: datagramParameters(1200)})
	for _, pair := range [][2]*Session{{client, server}, {server, client}} {
		sender, receiver := pair[0], pair[1]
		for i := range 20 {
			if err := sender.SendDatagram(fmt.Appendf(nil, "datagram %d", i)); err != nil {
				t.Fatal(err)
			}
		}
		for i := range 20 {
			got, err := receiver.ReceiveDatagram(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if want := fmt.Sprintf("datagram %d", i); string(got) != want {
				t.Fatalf("received %q, want %q", got, want)
			}
		}
	}
}

func TestDatagramSizeLimits(t *testing.T) {
	client, server := sessionPair(t,
		Config{Parameters: datagramParameters(1 << 20)}, Config{Parameters: datagramParameters(100)})
	// 100 bytes of frame: one type byte, two length bytes, 97 bytes of payload.
	if got := client.MaxDatagramSize(); got != 97 {
		t.Fatalf("MaxDatagramSize = %d, want 97", got)
	}
	if err := client.SendDatagram(make([]byte, 98)); !errors.Is(err, ErrDatagramTooLarge) {
		t.Fatalf("oversized datagram = %v, want ErrDatagramTooLarge", err)
	}
	payload := bytes.Repeat([]byte{7}, 97)
	if err := client.SendDatagram(payload); err != nil {
		t.Fatal(err)
	}
	if got, err := server.ReceiveDatagram(t.Context()); err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("received %x, %v", got, err)
	}
	// A frame size above the peer's record size is bounded by the record.
	if got, want := server.MaxDatagramSize(), maxDatagramPayload(64<<10); got != want {
		t.Fatalf("MaxDatagramSize = %d, want %d", got, want)
	}
}

func TestDatagramsDisabledByThePeer(t *testing.T) {
	client, _ := sessionPair(t, Config{Parameters: datagramParameters(1200)}, Config{Parameters: testParameters()})
	if got := client.MaxDatagramSize(); got != -1 {
		t.Fatalf("MaxDatagramSize = %d, want -1", got)
	}
	if err := client.SendDatagram([]byte("x")); !errors.Is(err, ErrDatagramsDisabled) {
		t.Fatalf("SendDatagram = %v, want ErrDatagramsDisabled", err)
	}
}

func TestReceivedDatagramChecks(t *testing.T) {
	for _, tc := range []struct {
		name         string
		maxFrameSize uint64
		frame        []byte
		want         TransportErrorCode
	}{
		{"not negotiated", 0, appendDatagram(nil, []byte("x")), ProtocolViolation},
		{"larger than advertised", 100, appendDatagram(nil, make([]byte, 99)), ProtocolViolation},
		{"truncated", 100, []byte{frameTypeDatagramLen, 5, 1}, FrameEncodingError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, conn := openRaw(t, Config{Parameters: datagramParameters(tc.maxFrameSize)})
			if _, err := conn.Write(record(tc.frame)); err != nil {
				t.Fatal(err)
			}
			wantSessionError(t, session, tc.want)
		})
	}
}

func TestReceivedDatagramFormats(t *testing.T) {
	session, conn := openRaw(t, Config{Parameters: datagramParameters(100)})
	// A DATAGRAM frame with a length, then one that runs to the end of the
	// record; both exactly 100 bytes.
	withLength := appendDatagram(nil, bytes.Repeat([]byte{1}, 97))
	toEnd := append([]byte{frameTypeDatagram}, bytes.Repeat([]byte{2}, 99)...)
	if _, err := conn.Write(record(withLength, toEnd)); err != nil {
		t.Fatal(err)
	}
	for _, want := range [][]byte{withLength[3:], toEnd[1:]} {
		got, err := session.ReceiveDatagram(t.Context())
		if err != nil || !bytes.Equal(got, want) {
			t.Fatalf("received %x, %v; want %x", got, err, want)
		}
	}
}

func TestReceiveDatagramFailsWhenSessionEnds(t *testing.T) {
	client, server := sessionPair(t,
		Config{Parameters: datagramParameters(1200)}, Config{Parameters: datagramParameters(1200)})
	result := make(chan error, 1)
	go func() {
		_, err := server.ReceiveDatagram(context.Background())
		result <- err
	}()
	_ = client.CloseWithError(3, "done")
	select {
	case err := <-result:
		var appErr *ApplicationError
		if !errors.As(err, &appErr) || appErr.ErrorCode != 3 {
			t.Fatalf("ReceiveDatagram = %v, want application error 3", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReceiveDatagram did not return after the session ended")
	}
}

func TestMaxDatagramPayload(t *testing.T) {
	for frameSize, want := range map[int]int{0: -1, 1: -1, 2: 0, 65: 63, 66: 63, 67: 64, 16384: 16381} {
		got := maxDatagramPayload(frameSize)
		if got != want {
			t.Errorf("maxDatagramPayload(%d) = %d, want %d", frameSize, got, want)
		}
		if got >= 0 && datagramFrameSize(got) > frameSize {
			t.Errorf("payload %d does not fit in %d bytes", got, frameSize)
		}
	}
}

// recordSizes is a RecordConn that remembers the largest record written.
type recordSizes struct {
	RecordConn
	largest chan int
}

func (c *recordSizes) WriteRecord(frames []byte) error {
	select {
	case largest := <-c.largest:
		c.largest <- max(largest, len(frames))
	default:
	}
	return c.RecordConn.WriteRecord(frames)
}

func TestMaxSendRecordSize(t *testing.T) {
	clientConn, serverConn := tcpPair(t)
	sizes := &recordSizes{RecordConn: NewStreamConn(clientConn, 64<<10), largest: make(chan int, 1)}
	sizes.largest <- 0
	client, server := openRecordPair(t, sizes, NewStreamConn(serverConn, 64<<10),
		Config{Parameters: testParameters(), MaxSendRecordSize: 1}, Config{Parameters: testParameters()})
	go echo(server)
	roundTrip(t, client, bytes.Repeat([]byte("x"), 200<<10))
	if largest := <-sizes.largest; largest > DefaultMaxRecordSize || largest < DefaultMaxRecordSize/2 {
		t.Fatalf("largest record %d bytes, want close to %d", largest, DefaultMaxRecordSize)
	}
}

func TestLongCloseReasonIsCut(t *testing.T) {
	client, server := defaultPair(t)
	_ = client.CloseWithError(1, string(bytes.Repeat([]byte("r"), 100<<10)))
	<-server.Context().Done()
	var appErr *ApplicationError
	if err := context.Cause(server.Context()); !errors.As(err, &appErr) || len(appErr.ErrorMessage) != maxReasonLength {
		t.Fatalf("server ended with %v, want a %d-byte reason", err, maxReasonLength)
	}
}

// TestDatagramFloodDoesNotStallStreams checks that a producer that keeps the
// datagram queue full leaves room for stream data: datagrams and streams take
// turns at the front of each record.
func TestDatagramFloodDoesNotStallStreams(t *testing.T) {
	// Each client record takes a millisecond to write, as under TCP
	// backpressure, so the flood refills the queue while the writer waits.
	clientConn, serverConn := tcpPair(t)
	slow := &slowRecords{RecordConn: NewStreamConn(clientConn, 64<<10), delay: time.Millisecond}
	client, server := openRecordPair(t, slow, NewStreamConn(serverConn, 64<<10),
		Config{Parameters: datagramParameters(1200)}, Config{Parameters: datagramParameters(1200)})
	go echo(server)
	stop := make(chan struct{})
	flooded := make(chan struct{})
	go func() {
		defer close(flooded)
		payload := make([]byte, client.MaxDatagramSize())
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := client.SendDatagram(payload); err != nil {
				return
			}
		}
	}()
	defer func() {
		close(stop)
		<-flooded
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		roundTrip(t, client, bytes.Repeat([]byte("s"), 1<<20))
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stream round trip stalled behind a datagram flood")
	}
}

// slowRecords is a RecordConn whose writes each take delay.
type slowRecords struct {
	RecordConn
	delay time.Duration
}

func (c *slowRecords) WriteRecord(frames []byte) error {
	time.Sleep(c.delay)
	return c.RecordConn.WriteRecord(frames)
}
