package trevrpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// A unary handler's context ends with its call: work the handler leaves
// behind that watches ctx stops once the handler returns.
func TestUnaryHandlerContextEndsWithItsCall(t *testing.T) {
	pki := newTestPKI(t)
	ended := make(chan error, 1)
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, request *testMessage) (*testMessage, error) {
			go func() {
				<-ctx.Done()
				ended <- ctx.Err()
			}()
			return &testMessage{Value: request.Value}, nil
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	if got := echoSuffix(t, channel); got != "v" {
		t.Fatalf("call = %q, want v", got)
	}
	select {
	case err := <-ended:
		if err == nil {
			t.Fatal("the handler's context ended without an error")
		}
	case <-time.After(rpcTestTimeout):
		t.Fatal("the handler's context was not canceled after its call ended")
	}
}

// releasingStream answers one message and, on Close, waits for the goroutine
// it started to see its context end, as a stream releasing a subscription
// when the call ends would.
type releasingStream struct {
	sent     bool
	released chan struct{}
}

func (s *releasingStream) Recv() (*testMessage, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	return &testMessage{Value: "a"}, nil
}

func (s *releasingStream) Close() error {
	<-s.released
	return nil
}

// A streaming handler's context ends when its response stream does, before
// the server closes the stream, so a Close that waits for it returns.
func TestStreamingHandlerContextEndsBeforeItsStreamCloses(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		RegisterServerStreaming(server, "/example.Greeter/Count", newTestMessage, func(ctx context.Context, _ *testMessage) (MessageStream[*testMessage], error) {
			stream := &releasingStream{released: make(chan struct{})}
			go func() {
				<-ctx.Done()
				close(stream.released)
			}()
			return stream, nil
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	callCtx, cancelCall := context.WithTimeout(ctx, 10*time.Second)
	defer cancelCall()
	started := time.Now()
	stream, err := ServerStreaming(callCtx, channel, "/example.Greeter/Count", &testMessage{Value: "c"}, newTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if reply, err := stream.Recv(); err != nil || reply.Value != "a" {
		t.Fatalf("Recv = %v, %v; want a", reply, err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want io.EOF", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("the call took %s: the server waited for its deadline to close the stream", elapsed)
	}
}
