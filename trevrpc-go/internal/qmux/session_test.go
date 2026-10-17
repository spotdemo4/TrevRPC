package qmux

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func testParameters() Parameters {
	return Parameters{
		InitialMaxData:                 1 << 20,
		InitialMaxStreamDataBidiLocal:  256 << 10,
		InitialMaxStreamDataBidiRemote: 256 << 10,
		InitialMaxStreamsBidi:          16,
		MaxRecordSize:                  64 << 10,
	}
}

// sessionPair opens a client and a server session over loopback TCP.
func sessionPair(t *testing.T, client, server Config) (*Session, *Session) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	clientConn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	serverConn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	return openPair(t, clientConn, serverConn, client, server)
}

func openPair(t *testing.T, clientConn, serverConn net.Conn, client, server Config) (*Session, *Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var serverSession *Session
	var serverErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		serverSession, serverErr = Open(ctx, NewStreamConn(serverConn, server.Parameters.maxRecordSize()), RoleServer, server)
	}()
	clientSession, err := Open(ctx, NewStreamConn(clientConn, client.Parameters.maxRecordSize()), RoleClient, client)
	<-done
	if err != nil || serverErr != nil {
		t.Fatalf("open: client %v, server %v", err, serverErr)
	}
	t.Cleanup(func() {
		_ = clientSession.CloseWithError(0, "")
		_ = serverSession.CloseWithError(0, "")
		<-clientSession.Done()
		<-serverSession.Done()
	})
	return clientSession, serverSession
}

func defaultPair(t *testing.T) (*Session, *Session) {
	t.Helper()
	return sessionPair(t, Config{Parameters: testParameters()}, Config{Parameters: testParameters()})
}

// echo serves streams on session by copying each stream's data back.
func echo(session *Session) {
	for {
		stream, err := session.AcceptStream(context.Background())
		if err != nil {
			return
		}
		go func() {
			_, _ = io.Copy(stream, stream)
			_ = stream.Close()
		}()
	}
}

func roundTrip(t *testing.T, session *Session, payload []byte) {
	t.Helper()
	stream, err := session.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	writeErr := make(chan error, 1)
	go func() {
		_, err := stream.Write(payload)
		if err == nil {
			err = stream.Close()
		}
		writeErr <- err
	}()
	got, err := io.ReadAll(stream)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if err := <-writeErr; err != nil {
		t.Fatalf("write: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("echo returned %d bytes, want %d", len(got), len(payload))
	}
}

func TestEcho(t *testing.T) {
	client, server := defaultPair(t)
	go echo(server)
	roundTrip(t, client, []byte("hello"))
	roundTrip(t, client, nil)
}

func TestFlowControlWindowsAreReplenished(t *testing.T) {
	params := Parameters{
		InitialMaxData:                 3000,
		InitialMaxStreamDataBidiLocal:  1000,
		InitialMaxStreamDataBidiRemote: 1000,
		InitialMaxStreamsBidi:          4,
	}
	client, server := sessionPair(t, Config{Parameters: params, StreamSendBuffer: 700}, Config{Parameters: params, StreamSendBuffer: 700})
	go echo(server)

	payload := bytes.Repeat([]byte("0123456789abcdef"), 64<<10)
	var wg sync.WaitGroup
	for range 3 {
		wg.Go(func() { roundTrip(t, client, payload) })
	}
	wg.Wait()
}

func TestStreamLimitBlocksUntilAStreamCompletes(t *testing.T) {
	serverParams := testParameters()
	serverParams.InitialMaxStreamsBidi = 1
	client, server := sessionPair(t, Config{Parameters: testParameters()}, Config{Parameters: serverParams})
	go echo(server)

	first, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if _, err := client.OpenStreamSync(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("second stream opened beyond the limit: %v", err)
	}

	if _, err := first.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	if _, err := io.ReadAll(first); err != nil {
		t.Fatal(err)
	}
	roundTrip(t, client, []byte("after the first stream completed"))
}

func TestPeerResetFailsRead(t *testing.T) {
	client, server := defaultPair(t)
	stream, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("partial")); err != nil {
		t.Fatal(err)
	}
	accepted, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stream.CancelWrite(7)

	_, err = io.ReadAll(accepted)
	var streamErr *StreamError
	if !errors.As(err, &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 7 {
		t.Fatalf("read after reset = %v, want remote stream error 7", err)
	}
}

func TestStopSendingFailsWriteAndCancelsContext(t *testing.T) {
	client, server := defaultPair(t)
	stream, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	accepted, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	accepted.CancelRead(3)

	select {
	case <-stream.Context().Done():
	case <-time.After(5 * time.Second):
		t.Fatal("stream context was not cancelled by STOP_SENDING")
	}
	_, err = stream.Write([]byte("more"))
	var streamErr *StreamError
	if !errors.As(err, &streamErr) || !streamErr.Remote || streamErr.ErrorCode != 3 {
		t.Fatalf("write after STOP_SENDING = %v, want remote stream error 3", err)
	}
}

func TestCloseCancelsStreamContext(t *testing.T) {
	client, _ := defaultPair(t)
	stream, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stream.Context().Err() != nil {
		t.Fatal("new stream context is already cancelled")
	}
	_ = stream.Close()
	if stream.Context().Err() == nil {
		t.Fatal("Close did not cancel the stream context")
	}
}

func TestReadDeadline(t *testing.T) {
	client, _ := defaultPair(t)
	stream, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read = %v, want deadline exceeded", err)
	}
}

func TestApplicationCloseReachesPeer(t *testing.T) {
	client, server := defaultPair(t)
	stream, err := client.OpenStreamSync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Write([]byte("last words")); err != nil {
		t.Fatal(err)
	}
	_ = stream.Close()
	_ = client.CloseWithError(42, "bye")

	accepted, err := server.AcceptStream(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(accepted)
	if err != nil || string(got) != "last words" {
		t.Fatalf("data written before close = %q, %v", got, err)
	}
	<-server.Context().Done()
	var appErr *ApplicationError
	if err := context.Cause(server.Context()); !errors.As(err, &appErr) || !appErr.Remote || appErr.ErrorCode != 42 || appErr.ErrorMessage != "bye" {
		t.Fatalf("server session ended with %v, want remote application error 42", err)
	}
	select {
	case <-client.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("closing session never released its connection")
	}
}

func TestIdleTimeout(t *testing.T) {
	params := testParameters()
	params.MaxIdleTimeout = 100 * time.Millisecond
	client, server := sessionPair(t, Config{Parameters: params}, Config{Parameters: testParameters()})
	for _, session := range []*Session{client, server} {
		select {
		case <-session.Context().Done():
		case <-time.After(5 * time.Second):
			t.Fatal("idle session was not closed")
		}
	}
	// Both sides share the negotiated timeout; whichever expires first ends
	// the connection for the other.
	clientErr, serverErr := context.Cause(client.Context()), context.Cause(server.Context())
	if !errors.Is(clientErr, ErrIdleTimeout) && !errors.Is(serverErr, ErrIdleTimeout) {
		t.Fatalf("sessions ended with %v and %v, want an idle timeout", clientErr, serverErr)
	}
}

func TestKeepAlivePreventsIdleTimeout(t *testing.T) {
	params := testParameters()
	params.MaxIdleTimeout = 150 * time.Millisecond
	client, _ := sessionPair(t, Config{Parameters: params}, Config{Parameters: testParameters(), KeepAlivePeriod: 40 * time.Millisecond})
	time.Sleep(500 * time.Millisecond)
	if err := client.Context().Err(); err != nil {
		t.Fatalf("session with keep-alives timed out: %v", context.Cause(client.Context()))
	}
}

type openResult struct {
	session *Session
	err     error
}

// rawPeer opens a client session whose server side the test drives byte by
// byte through the returned connection.
func rawPeer(t *testing.T, config Config) (net.Conn, <-chan openResult) {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	result := make(chan openResult, 1)
	go func() {
		session, err := Open(t.Context(), NewStreamConn(clientConn, DefaultMaxRecordSize), RoleClient, config)
		result <- openResult{session, err}
	}()
	t.Cleanup(func() { _ = serverConn.Close() })
	return serverConn, result
}

// openRaw completes the handshake of a rawPeer session.
func openRaw(t *testing.T, config Config) (*Session, net.Conn) {
	t.Helper()
	conn, result := rawPeer(t, config)
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	if _, err := conn.Write(record(appendTransportParameters(nil, &Draft02, testParameters()))); err != nil {
		t.Fatal(err)
	}
	opened := <-result
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	return opened.session, conn
}

func record(frames ...[]byte) []byte {
	var body []byte
	for _, frame := range frames {
		body = append(body, frame...)
	}
	return append(appendVarint(nil, uint64(len(body))), body...)
}

func TestFirstFrameMustBeTransportParameters(t *testing.T) {
	conn, result := rawPeer(t, Config{Parameters: testParameters()})
	go func() { _, _ = io.Copy(io.Discard, conn) }()
	if _, err := conn.Write(record(appendMaxData(nil, 10))); err != nil {
		t.Fatal(err)
	}
	err := (<-result).err
	var transportErr *TransportError
	if !errors.As(err, &transportErr) || transportErr.ErrorCode != ProtocolViolation {
		t.Fatalf("open = %v, want PROTOCOL_VIOLATION", err)
	}
}

func TestForbiddenFrameClosesWithFrameEncodingError(t *testing.T) {
	conn, result := rawPeer(t, Config{Parameters: testParameters(), CloseTimeout: 50 * time.Millisecond})
	received := make(chan []byte, 1)
	go func() { data, _ := io.ReadAll(conn); received <- data }()
	params := appendTransportParameters(nil, &Draft02, testParameters())
	if _, err := conn.Write(record(params)); err != nil {
		t.Fatal(err)
	}
	opened := <-result
	if opened.err != nil {
		t.Fatal(opened.err)
	}
	session := opened.session
	// ACK (0x02) is never valid in QMux.
	if _, err := conn.Write(record([]byte{0x02, 0, 0, 0, 0})); err != nil {
		t.Fatal(err)
	}
	<-session.Context().Done()
	var transportErr *TransportError
	if err := context.Cause(session.Context()); !errors.As(err, &transportErr) || transportErr.ErrorCode != FrameEncodingError {
		t.Fatalf("session ended with %v, want FRAME_ENCODING_ERROR", err)
	}
	data := <-received
	if !bytes.Contains(data, []byte{frameTypeConnectionClose, byte(FrameEncodingError)}) {
		t.Fatalf("peer did not receive CONNECTION_CLOSE: %x", data)
	}
}

func TestStreamDataGapIsProtocolViolation(t *testing.T) {
	session, conn := openRaw(t, Config{Parameters: testParameters()})
	// Server-initiated stream 1 starting at offset 5.
	if _, err := conn.Write(record(appendStreamFrame(nil, 1, 5, []byte("x"), false))); err != nil {
		t.Fatal(err)
	}
	<-session.Context().Done()
	var transportErr *TransportError
	if err := context.Cause(session.Context()); !errors.As(err, &transportErr) || transportErr.ErrorCode != ProtocolViolation {
		t.Fatalf("session ended with %v, want PROTOCOL_VIOLATION", err)
	}
}

func TestStreamBeyondLimitIsStreamLimitError(t *testing.T) {
	params := testParameters()
	params.InitialMaxStreamsBidi = 2
	session, conn := openRaw(t, Config{Parameters: params})
	// Server-initiated stream number 2 (ID 9) exceeds a limit of two.
	if _, err := conn.Write(record(appendStreamFrame(nil, 9, 0, []byte("x"), false))); err != nil {
		t.Fatal(err)
	}
	<-session.Context().Done()
	var transportErr *TransportError
	if err := context.Cause(session.Context()); !errors.As(err, &transportErr) || transportErr.ErrorCode != StreamLimitError {
		t.Fatalf("session ended with %v, want STREAM_LIMIT_ERROR", err)
	}
}

func TestConnectionLostWithoutClose(t *testing.T) {
	session, conn := openRaw(t, Config{Parameters: testParameters()})
	_ = conn.Close()
	<-session.Context().Done()
	if err := context.Cause(session.Context()); !errors.Is(err, ErrConnectionLost) {
		t.Fatalf("session ended with %v, want connection lost", err)
	}
}

func TestLargeWritesWithLargeRecordsComplete(t *testing.T) {
	params := testParameters()
	params.InitialMaxStreamDataBidiLocal = 4 << 20
	params.InitialMaxStreamDataBidiRemote = 4 << 20
	params.InitialMaxData = 16 << 20
	client, server := sessionPair(t, Config{Parameters: params}, Config{Parameters: params})
	go echo(server)
	roundTrip(t, client, bytes.Repeat([]byte("x"), 3<<20+17))
}
