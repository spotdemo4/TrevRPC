package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

// The library ships native QUIC clients only. These test-only clients speak
// HTTP/3 and WebTransport so the server's handling of both stays covered. Both
// reuse the production streamClient by adapting their streams to clientStream.

// newHTTP3TestClient returns a Transport that sends every RPC as one
// POST /trevrpc request.
func newHTTP3TestClient(t *testing.T, pki testPKI, listener *Listener) Transport {
	t.Helper()
	roundTripper := &http3.Transport{TLSClientConfig: pki.client.Clone()}
	t.Cleanup(func() { _ = roundTripper.Close() })
	opener := http3TestOpener{
		roundTripper: roundTripper,
		url:          "https://" + listener.Addr().String() + DefaultHTTP3Path,
	}
	return newStreamClient(opener, DefaultMaxFrameSize)
}

type http3TestOpener struct {
	roundTripper *http3.Transport
	url          string
}

func (o http3TestOpener) openStream(ctx context.Context) (clientStream, error) {
	body, writer := io.Pipe()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", http3ContentType)
	stream := &http3TestStream{writer: writer, ready: make(chan struct{})}
	go func() {
		defer close(stream.ready)
		stream.response, stream.err = o.roundTripper.RoundTrip(request)
		if stream.err == nil && stream.response.StatusCode != http.StatusOK {
			status := stream.response.Status
			_ = stream.response.Body.Close()
			stream.err = fmt.Errorf("unexpected HTTP status %s", status)
		}
	}()
	return stream, nil
}

// http3TestStream writes the request body and reads the response body of one request.
type http3TestStream struct {
	writer   *io.PipeWriter
	ready    chan struct{}
	response *http.Response
	err      error
}

func (s *http3TestStream) Read(p []byte) (int, error) {
	<-s.ready
	if s.err != nil {
		return 0, s.err
	}
	return s.response.Body.Read(p)
}

func (s *http3TestStream) Write(p []byte) (int, error) { return s.writer.Write(p) }
func (s *http3TestStream) Close() error                { return s.writer.Close() }

func (s *http3TestStream) SetReadDeadline(time.Time) error { return nil }

func (s *http3TestStream) cancelRead() {
	select {
	case <-s.ready:
		if s.response != nil {
			_ = s.response.Body.Close()
		}
	default:
	}
}

func (s *http3TestStream) cancelWrite() { _ = s.writer.CloseWithError(errors.New("request cancelled")) }

// newWebTransportTestClient returns a Transport that runs RPCs on the streams
// of one WebTransport session to /trevrpc.
func newWebTransportTestClient(t *testing.T, pki testPKI, listener *Listener, origin string) Transport {
	t.Helper()
	return newStreamClient(newWebTransportTestOpener(t, pki, listener, origin), DefaultMaxFrameSize)
}

// newWebTransportTestOpener opens one WebTransport session and returns an
// opener for raw streams on it.
func newWebTransportTestOpener(t *testing.T, pki testPKI, listener *Listener, origin string) streamOpener {
	t.Helper()
	wt := &webtransport.Transport{
		TLSClientConfig: pki.client.Clone(),
		QUICConfig:      &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
	}
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	_, session, err := wt.Dial(ctx, "https://"+listener.Addr().String()+DefaultHTTP3Path, header)
	if err != nil {
		t.Fatalf("WebTransport dial: %v", err)
	}
	var closeOnce sync.Once
	t.Cleanup(func() { closeOnce.Do(func() { _ = session.CloseWithError(0, "test done") }) })
	return webTransportTestOpener{session}
}

type webTransportTestOpener struct{ session *webtransport.Session }

func (o webTransportTestOpener) openStream(ctx context.Context) (clientStream, error) {
	stream, err := o.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}
	return webTransportTestStream{stream}, nil
}

type webTransportTestStream struct{ *webtransport.Stream }

func (s webTransportTestStream) cancelRead()  { s.Stream.CancelRead(streamErrorCancelled) }
func (s webTransportTestStream) cancelWrite() { s.Stream.CancelWrite(streamErrorCancelled) }
