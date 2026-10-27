package trevrpc

import (
	"context"
	"testing"
	"time"
)

// A unary handler's context lives until its call's response is written, not
// just until the handler returns: work the handler leaves behind that watches
// ctx stops then.
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
