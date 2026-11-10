package trevrpc

import (
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// resettableTestStream is a raw client stream whose request side a test resets
// with a chosen error code, as a browser does.
type resettableTestStream interface {
	io.ReadWriter
	resetRequest(code uint64)
}

type quicResettableStream struct{ *quic.Stream }

func (s quicResettableStream) resetRequest(code uint64) {
	s.CancelWrite(quic.StreamErrorCode(code))
}

type webTransportResettableStream struct{ *webtransport.Stream }

func (s webTransportResettableStream) resetRequest(code uint64) {
	s.CancelWrite(webtransport.StreamErrorCode(code))
}

type qmuxResettableStream struct{ *qmux.Stream }

func (s qmuxResettableStream) resetRequest(code uint64) { s.CancelWrite(code) }

// TestPeerResetOfTheRequestStreamCancelsTheCall covers a client that abandons
// a client-streaming call while its handler waits in Recv by resetting the
// request side only, as Chromium does with code 0 when a page cancels a
// WebTransport call: the handler's Recv and the call end as Cancelled, not as
// an Unavailable transport failure.
func TestPeerResetOfTheRequestStreamCancelsTheCall(t *testing.T) {
	pki := newTestPKI(t)
	received := make(chan struct{}, 1)
	recvErrs := make(chan error, 1)
	var capture *finishedCapture
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		capture = newFinishedCapture(server)
		RegisterClientStreaming(server, testPath("Upload"), newTestMessage, func(_ context.Context, requests MessageStream[*testMessage]) (*testMessage, error) {
			for {
				if _, err := requests.Recv(); err != nil {
					recvErrs <- err
					return nil, err
				}
				received <- struct{}{}
			}
		})
	})

	conn, err := quic.DialAddr(t.Context(), listener.Addr().String(), clientTLSConfig(pki.client, "localhost"), clientQUICConfig(nil, DefaultMaxFrameSize))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.CloseWithError(0, "") })
	wtResponse, wtSession, err := dialWebTransportTest(pki, listener, webTransportTestDial{origin: testOrigin})
	if err != nil {
		t.Fatalf("WebTransport dial: %v", err)
	}
	t.Cleanup(func() {
		_ = wtSession.CloseWithError(0, "")
		_ = wtResponse.Body.Close()
	})
	wsSession, err := dialWebSocketTestSession(t, pki, listener, testOrigin)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	t.Cleanup(func() { _ = wsSession.CloseWithError(0, "") })

	open := map[string]func(context.Context) (resettableTestStream, error){
		"quic": func(ctx context.Context) (resettableTestStream, error) {
			stream, err := conn.OpenStreamSync(ctx)
			return quicResettableStream{stream}, err
		},
		"webtransport": func(ctx context.Context) (resettableTestStream, error) {
			stream, err := wtSession.OpenStreamSync(ctx)
			return webTransportResettableStream{stream}, err
		},
		"websocket": func(ctx context.Context) (resettableTestStream, error) {
			stream, err := wsSession.OpenStreamSync(ctx)
			return qmuxResettableStream{stream}, err
		},
	}
	for name, openStream := range open {
		for _, code := range []uint64{0, streamErrorCancelled} {
			t.Run(fmt.Sprintf("%s/code %d", name, code), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
				defer cancel()
				stream, err := openStream(ctx)
				if err != nil {
					t.Fatal(err)
				}
				request := newWireRequest(testPath("Upload"))
				request.Kind = RPCKindClientStreaming
				if _, err := stream.Write(testRequestBytes(t, request, encodedTestMessage(t, "first"))); err != nil {
					t.Fatalf("write request: %v", err)
				}
				// A reset may discard request bytes the server has not read, so
				// it waits for the handler to wait in Recv for the next message.
				select {
				case <-received:
				case err := <-recvErrs:
					t.Fatalf("Recv of the first message = %v", err)
				case <-ctx.Done():
					t.Fatal("the handler never received the first message")
				}
				// The response side stays open, so the client does not cancel the
				// call's context before the reset reaches Recv.
				stream.resetRequest(code)

				select {
				case err := <-recvErrs:
					if status := StatusFromError(err); status.Code != CodeCancelled {
						t.Fatalf("Recv = %v, want Cancelled", err)
					}
				case <-ctx.Done():
					t.Fatal("Recv never failed")
				}
				if event := capture.next(t); event.Code != CodeCancelled || StatusFromError(event.Err).Code != CodeCancelled {
					t.Fatalf("finished %v with %v, want Cancelled", event.Code, event.Err)
				}
			})
		}
	}
}
