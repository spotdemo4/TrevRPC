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

	"github.com/coder/websocket"
	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/qmux"
)

// The library ships native QUIC and QMux clients only. These test-only
// clients speak HTTP/3, HTTP/2, WebTransport and QMux over WebSocket so the
// server's handling of each stays covered. They reuse the production
// streamClient by adapting their streams to clientStream.

// newHTTP3TestClient returns a Transport that sends every RPC as one
// POST /trevrpc request over HTTP/3.
func newHTTP3TestClient(t *testing.T, pki testPKI, listener *Listener) Transport {
	t.Helper()
	roundTripper := &http3.Transport{TLSClientConfig: pki.client.Clone()}
	t.Cleanup(func() { _ = roundTripper.Close() })
	opener := httpTestOpener{
		roundTripper: roundTripper,
		url:          "https://" + listener.Addr().String() + DefaultHTTPPath,
	}
	return newStreamClient(opener, DefaultMaxFrameSize)
}

// newHTTP2TestClient returns a Transport that sends every RPC as one
// POST /trevrpc request over HTTP/2 on the TCP listener.
func newHTTP2TestClient(t *testing.T, pki testPKI, listener *Listener) Transport {
	t.Helper()
	protocols := new(http.Protocols)
	protocols.SetHTTP2(true)
	roundTripper := &http.Transport{TLSClientConfig: pki.client.Clone(), Protocols: protocols}
	t.Cleanup(roundTripper.CloseIdleConnections)
	opener := httpTestOpener{
		roundTripper: roundTripper,
		url:          "https://" + listener.TCPAddr().String() + DefaultHTTPPath,
	}
	return newStreamClient(opener, DefaultMaxFrameSize)
}

// newQMuxTestClient returns a Channel over QMux over TLS on the TCP listener.
func newQMuxTestClient(t *testing.T, pki testPKI, listener *Listener) *Channel {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	connector := &qmuxConnector{
		addr:         listener.TCPAddr().String(),
		tls:          clientQMuxTLSConfig(pki.client, "localhost"),
		maxFrameSize: DefaultMaxFrameSize,
	}
	channel, err := dialChannel(ctx, connector, nil)
	if err != nil {
		t.Fatalf("QMux dial: %v", err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}

// newWebSocketTestClient returns a Transport that runs RPCs on the streams of
// one QMux over WebSocket session to /trevrpc, as the browser fallback does.
func newWebSocketTestClient(t *testing.T, pki testPKI, listener *Listener, origin string) Transport {
	t.Helper()
	session, err := dialWebSocketTestSession(t, pki, listener, origin)
	if err != nil {
		t.Fatalf("WebSocket dial: %v", err)
	}
	t.Cleanup(func() { _ = session.CloseWithError(0, "test done") })
	return newStreamClient(qmuxOpener{session}, DefaultMaxFrameSize)
}

func dialWebSocketTestSession(t *testing.T, pki testPKI, listener *Listener, origin string) (*qmux.Session, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: pki.client.Clone()}}
	conn, response, err := websocket.Dial(ctx, "wss://"+listener.TCPAddr().String()+DefaultHTTPPath, &websocket.DialOptions{
		HTTPClient:   httpClient,
		HTTPHeader:   header,
		Subprotocols: []string{QMuxALPN},
	})
	if err != nil {
		if response != nil {
			return nil, fmt.Errorf("%w (HTTP %d)", err, response.StatusCode)
		}
		return nil, err
	}
	conn.SetReadLimit(qmuxMaxRecordSize)
	return qmux.Open(ctx, qmux.NewMessageConn(newWebSocketMessages(conn), qmuxMaxRecordSize), qmux.RoleClient, clientQMuxConfig(DefaultMaxFrameSize))
}

type httpTestOpener struct {
	roundTripper http.RoundTripper
	url          string
}

func (o httpTestOpener) openStream(ctx context.Context) (clientStream, error) {
	body, writer := io.Pipe()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, o.url, body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Content-Type", httpContentType)
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
func newWebTransportTestClient(t *testing.T, pki testPKI, listener *Listener, origin string, config *webtransport.Config) Transport {
	t.Helper()
	return newStreamClient(newWebTransportTestOpener(t, pki, listener, origin, config), DefaultMaxFrameSize)
}

// newWebTransportTestOpener opens one WebTransport session and returns an
// opener for raw streams on it. A config with limits makes the client advertise
// WT_INITIAL_* SETTINGS, as Safari does, which enables session flow control.
func newWebTransportTestOpener(t *testing.T, pki testPKI, listener *Listener, origin string, config *webtransport.Config) streamOpener {
	t.Helper()
	wt := &webtransport.Transport{
		Config:          config,
		TLSClientConfig: pki.client.Clone(),
		QUICConfig:      &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true},
	}
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	_, session, err := wt.Dial(ctx, "https://"+listener.Addr().String()+DefaultHTTPPath, header)
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
