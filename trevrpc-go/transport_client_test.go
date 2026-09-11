package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

type unaryTransportTestEndpoint struct {
	stream *unaryTransportTestStream
	done   chan struct{}
}

func (e *unaryTransportTestEndpoint) OpenStream(context.Context) (transportinternal.BidirectionalStream, error) {
	return e.stream, nil
}

func (*unaryTransportTestEndpoint) AcceptStream(context.Context) (transportinternal.BidirectionalStream, error) {
	return nil, errors.New("not implemented")
}

func (e *unaryTransportTestEndpoint) Done() <-chan struct{} { return e.done }
func (*unaryTransportTestEndpoint) Err() error              { return nil }
func (*unaryTransportTestEndpoint) Info() ConnectionInfo    { return ConnectionInfo{} }
func (*unaryTransportTestEndpoint) Close(TransportCloseReason) error {
	return nil
}

type unaryTransportTestStream struct {
	response      bytes.Reader
	written       bytes.Buffer
	eof           chan struct{}
	cancelled     chan struct{}
	waitingForEOF chan struct{}
	waitOnce      sync.Once
	cancelOnce    sync.Once
	cancelReads   atomic.Int32
	cancelWrites  atomic.Int32
	sendClosed    atomic.Bool
}

func newUnaryTransportTestStream(response []byte) *unaryTransportTestStream {
	stream := &unaryTransportTestStream{
		eof:           make(chan struct{}),
		cancelled:     make(chan struct{}),
		waitingForEOF: make(chan struct{}),
	}
	stream.response.Reset(response)
	return stream
}

func (s *unaryTransportTestStream) Read(data []byte) (int, error) {
	if s.response.Len() != 0 {
		return s.response.Read(data)
	}
	s.waitOnce.Do(func() { close(s.waitingForEOF) })
	select {
	case <-s.eof:
		return 0, io.EOF
	case <-s.cancelled:
		return 0, context.Canceled
	}
}

func (s *unaryTransportTestStream) Write(data []byte) (int, error) {
	return s.written.Write(data)
}

func (s *unaryTransportTestStream) Close() error {
	s.sendClosed.Store(true)
	return nil
}

func (s *unaryTransportTestStream) CancelRead(TransportCloseReason) {
	s.cancelReads.Add(1)
	s.cancelOnce.Do(func() { close(s.cancelled) })
}

func (s *unaryTransportTestStream) CancelWrite(TransportCloseReason) {
	s.cancelWrites.Add(1)
}

func encodeUnaryTestResponse(t *testing.T) []byte {
	t.Helper()
	frame, err := EncodeFrame(OKResponse([]byte("response")), DefaultMaxFrameSize)
	if err != nil {
		t.Fatalf("EncodeFrame() error = %v", err)
	}
	return frame
}

func callUnaryTestClient(
	t *testing.T,
	ctx context.Context,
	stream *unaryTransportTestStream,
) (*RpcResponse, error) {
	t.Helper()
	endpoint := &unaryTransportTestEndpoint{stream: stream, done: make(chan struct{})}
	client := newTransportStreamClient(endpoint, DefaultMaxFrameSize, nil)
	return client.Call(ctx, NewRpcRequest("service", "method", []byte("request")))
}

func TestTransportUnaryWaitsForCleanPeerEOF(t *testing.T) {
	stream := newUnaryTransportTestStream(encodeUnaryTestResponse(t))
	result := make(chan struct {
		response *RpcResponse
		err      error
	}, 1)
	go func() {
		response, err := callUnaryTestClient(t, context.Background(), stream)
		result <- struct {
			response *RpcResponse
			err      error
		}{response: response, err: err}
	}()

	select {
	case <-stream.waitingForEOF:
	case <-time.After(time.Second):
		t.Fatal("Call() did not reach peer EOF wait")
	}
	select {
	case completed := <-result:
		t.Fatalf("Call() returned before peer EOF: %#v", completed)
	default:
	}

	close(stream.eof)
	select {
	case completed := <-result:
		if completed.err != nil {
			t.Fatalf("Call() error = %v", completed.err)
		}
		if !bytes.Equal(completed.response.Body, []byte("response")) {
			t.Fatalf("Call() body = %q", completed.response.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("Call() did not return after peer EOF")
	}
	if got := stream.cancelReads.Load(); got != 0 {
		t.Fatalf("CancelRead() calls = %d, want 0", got)
	}
	if !stream.sendClosed.Load() {
		t.Fatal("Call() did not gracefully close the send direction")
	}
}

func TestTransportUnaryRejectsTrailingResponseData(t *testing.T) {
	response := append(encodeUnaryTestResponse(t), 0x7f)
	stream := newUnaryTransportTestStream(response)
	close(stream.eof)

	actual, err := callUnaryTestClient(t, context.Background(), stream)
	if actual != nil {
		t.Fatalf("Call() response = %#v, want nil", actual)
	}
	if status := StatusFromError(err); status.Code != CodeInvalidArgument {
		t.Fatalf("Call() status = %v, want InvalidArgument", status)
	}
	if got := stream.cancelReads.Load(); got != 1 {
		t.Fatalf("CancelRead() calls = %d, want 1", got)
	}
}

func TestTransportUnaryCancellationWhileAwaitingPeerEOF(t *testing.T) {
	stream := newUnaryTransportTestStream(encodeUnaryTestResponse(t))
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := callUnaryTestClient(t, ctx, stream)
		result <- err
	}()

	select {
	case <-stream.waitingForEOF:
	case <-time.After(time.Second):
		t.Fatal("Call() did not reach peer EOF wait")
	}
	cancel()
	select {
	case err := <-result:
		if status := StatusFromError(err); status.Code != CodeCancelled {
			t.Fatalf("Call() status = %v, want Cancelled", status)
		}
	case <-time.After(time.Second):
		t.Fatal("Call() did not return after context cancellation")
	}
	if got := stream.cancelReads.Load(); got == 0 {
		t.Fatal("Call() did not cancel the receive direction")
	}
	if got := stream.cancelWrites.Load(); got == 0 {
		t.Fatal("Call() did not cancel the send direction")
	}
}
