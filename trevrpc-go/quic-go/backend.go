package quicgo

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

type backend struct {
	d DialOptions
	o ListenOptions
}

var _ trevrpc.DialBackend = (*backend)(nil)
var _ trevrpc.ListenBackend = (*backend)(nil)

func (b *backend) Backend() trevrpc.TransportBackend { return trevrpc.TransportBackendQUICGo }

func (b *backend) NewConnector(target string, options trevrpc.BackendDialOptions) (trevrpc.BackendConnector, error) {
	o := b.d.clone()
	o.Transport, o.Limits, o.Credentials, o.MaxFrameSize = options.Transport, options.Limits, options.Credentials, options.MaxFrameSize
	o.WebTransport.RequestHeaders = options.WebTransport.RequestHeaders.Clone()
	o.WebTransport.StreamReorderingTimeout = options.WebTransport.StreamReorderingTimeout
	if strings.HasPrefix(target, "https://") {
		return newWebTransportConnector(target, o)
	}
	if strings.Contains(target, "://") {
		return nil, trevrpc.InvalidArgument("unsupported dial target scheme")
	}
	return newQUICConnector(target, o)
}

func (b *backend) Listen(addr string, options trevrpc.BackendListenOptions) (transportapi.Listener, error) {
	o := b.o.clone()
	o.Transport, o.Limits, o.Credentials = options.Transport, options.Limits, options.Credentials
	return newListener(addr, o, options.Server)
}

var _ trevrpc.BackendConnector = (*quicConnector)(nil)
var _ trevrpc.BackendConnector = (*webTransportConnector)(nil)

type quicConnector struct {
	target string
	tls    *tls.Config
	config *quic.Config
}

func newQUICConnector(target string, options DialOptions) (*quicConnector, error) {
	if _, err := validateTarget(target, false); err != nil {
		return nil, err
	}
	if err := validateCredentials(options.Credentials); err != nil {
		return nil, err
	}
	tlsConfig, err := tlsConfig(options.TLSConfig, options.Credentials, false, false)
	if err != nil {
		return nil, err
	}
	return &quicConnector{target: target, tls: tlsConfig, config: quicConfig(options.QUICConfig, options.Transport, options.Limits, options.MaxFrameSize, false, false)}, nil
}
func (c *quicConnector) Connect(ctx context.Context) (trevrpc.BackendConnection, error) {
	conn, err := quic.DialAddr(ctx, c.target, cloneTLSConfig(c.tls), c.config.Clone())
	if err != nil {
		return trevrpc.BackendConnection{}, mapDialStatus(ctx, err)
	}
	endpoint := newQUICEndpoint(conn, trevrpc.TransportBackendQUICGo)
	return trevrpc.BackendConnection{Endpoint: endpoint, Close: func() error {
		return endpoint.Close(trevrpc.TransportCloseReason{Local: true, Clean: true, Message: "client closed"})
	}, MapStatus: mapStatus, CloseReason: closeReason}, nil
}

type webTransportConnector struct {
	url     string
	tls     *tls.Config
	config  *quic.Config
	options WebTransportOptions
}

func newWebTransportConnector(target string, options DialOptions) (*webTransportConnector, error) {
	u, err := validateTarget(target, true)
	if err != nil {
		return nil, err
	}
	if err := validateCredentials(options.Credentials); err != nil {
		return nil, err
	}
	if err := validateHeaders(options.WebTransport.RequestHeaders); err != nil {
		return nil, err
	}
	tlsConfig, err := tlsConfig(options.TLSConfig, options.Credentials, false, true)
	if err != nil {
		return nil, err
	}
	return &webTransportConnector{url: u.String(), tls: tlsConfig, config: quicConfig(options.QUICConfig, options.Transport, options.Limits, options.MaxFrameSize, true, true), options: options.WebTransport}, nil
}
func (c *webTransportConnector) Connect(ctx context.Context) (trevrpc.BackendConnection, error) {
	headers := make(http.Header)
	for _, field := range c.options.RequestHeaders {
		headers.Add(field.Name, field.Value)
	}
	dialer := &webtransport.Transport{TLSClientConfig: cloneTLSConfig(c.tls), QUICConfig: c.config.Clone(), ApplicationProtocols: append([]string(nil), c.options.ApplicationProtocols...), StreamReorderingTimeout: c.options.StreamReorderingTimeout, DialAddr: quic.DialAddr}
	defer dialer.Close()
	_, session, err := dialer.Dial(ctx, c.url, headers)
	if err != nil {
		return trevrpc.BackendConnection{}, mapWebTransportStatus(ctx, err)
	}
	endpoint := newWebTransportEndpoint(session, trevrpc.TransportBackendQUICGo)
	return trevrpc.BackendConnection{Endpoint: endpoint, Close: func() error {
		return endpoint.Close(trevrpc.TransportCloseReason{Local: true, Clean: true, Message: "client closed"})
	}, MapStatus: mapWebTransportStatus, CloseReason: closeReason}, nil
}

func mapStatus(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return statusFromContext(ctx.Err())
	}
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return trevrpc.Cancelled("transport closed locally")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return trevrpc.DeadlineExceeded("transport deadline exceeded")
	}
	return trevrpc.Unavailable("transport unavailable: " + err.Error())
}
func mapDialStatus(ctx context.Context, err error) error { return mapStatus(ctx, err) }
func mapWebTransportStatus(ctx context.Context, err error) error {
	if ctx != nil && ctx.Err() != nil {
		return statusFromContext(ctx.Err())
	}
	if err == nil {
		return nil
	}
	var streamErr *webtransport.StreamError
	if errors.As(err, &streamErr) {
		return trevrpc.Cancelled(err.Error())
	}
	var sessionErr *webtransport.SessionError
	if errors.As(err, &sessionErr) {
		if sessionErr.Remote {
			return trevrpc.Unavailable("transport unavailable: " + err.Error())
		}
		return trevrpc.Cancelled("transport closed locally")
	}
	return mapStatus(ctx, err)
}
func statusFromContext(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return trevrpc.DeadlineExceeded("RPC deadline exceeded")
	}
	return trevrpc.Cancelled("RPC cancelled")
}
func validateCredentials(credentials *trevrpc.TransportCredentials) error {
	if credentials == nil {
		return nil
	}
	if (len(credentials.CertificateChainPEM) == 0) != (len(credentials.PrivateKeyPEM) == 0) {
		return trevrpc.InvalidArgument("transport credentials require both certificate chain and private key")
	}
	if credentials.RequireClientCertificate && len(credentials.RootCAPEM) == 0 {
		return trevrpc.InvalidArgument("client certificate verification requires a root CA")
	}
	return nil
}
func validateHeaders(fields trevrpc.HeaderFields) error {
	for _, field := range fields {
		if strings.TrimSpace(field.Name) == "" || strings.HasPrefix(field.Name, ":") || strings.ContainsAny(field.Name, "\r\n") || strings.ContainsAny(field.Value, "\r\n") {
			return trevrpc.InvalidArgument("invalid WebTransport request header")
		}
	}
	return nil
}

type quicListener struct {
	listener  *quic.Listener
	requested trevrpc.TransportBackend
}

func newListener(addr string, options ListenOptions, server trevrpc.BackendServerOptions) (transportapi.Listener, error) {
	if options.Limits.IncomingBidirectionalStreams <= 0 && server.MaxConcurrentStreamsPerConnection > 0 {
		options.Limits.IncomingBidirectionalStreams = int64(server.MaxConcurrentStreamsPerConnection) + 1
	}
	if err := validateCredentials(options.Credentials); err != nil {
		return nil, err
	}
	h3 := server.EnableHTTP3 || server.EnableWebTransport
	tlsConfig, err := tlsConfig(options.TLSConfig, options.Credentials, true, h3)
	if err != nil {
		return nil, err
	}
	config := quicConfig(options.QUICConfig, options.Transport, options.Limits, server.MaxFrameSize, server.EnableWebTransport, h3)
	if !h3 {
		config.MaxIncomingUniStreams = -1
	}
	listener, err := quic.ListenAddr(addr, tlsConfig, config)
	if err != nil {
		return nil, mapStatus(context.Background(), err)
	}
	return &quicListener{listener: listener, requested: trevrpc.TransportBackendQUICGo}, nil
}
func (l *quicListener) Accept(ctx context.Context) (transportapi.Connection, error) {
	conn, err := l.listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return newQUICEndpoint(conn, l.requested), nil
}
func (l *quicListener) Address() trevrpc.TransportAddress {
	return transportapi.AddressFromNet(l.listener.Addr())
}
func (l *quicListener) Close() error { return l.listener.Close() }

var _ transportapi.Listener = (*quicListener)(nil)

func (b *backend) ServeConnection(ctx context.Context, conn transportapi.Connection, server trevrpc.BackendServerConnection) {
	if conn.Info().Protocol != trevrpc.TransportProtocolHTTP3 {
		server.ServeNativeQUIC(ctx, conn)
		return
	}
	qconn, ok := conn.(*quicEndpoint)
	if !ok {
		_ = conn.Close(trevrpc.TransportCloseReason{Local: true, Message: "invalid HTTP/3 endpoint"})
		return
	}
	b.serveHTTP3(ctx, qconn, server)
}

func (b *backend) ListenerClosed(err error) bool { return errors.Is(err, net.ErrClosed) }
func (b *backend) MapStatus(err error) error     { return mapStatus(context.Background(), err) }

func (b *backend) serveHTTP3(ctx context.Context, endpoint *quicEndpoint, server trevrpc.BackendServerConnection) {
	opts := server.Options()
	h3Server := &http3.Server{ConnContext: func(connCtx context.Context, _ *quic.Conn) context.Context { return connCtx }}
	var wtServer *webtransport.Server
	if opts.EnableWebTransport {
		wtServer = &webtransport.Server{
			CheckOrigin: func(*http.Request) bool { return true },
			H3:          h3Server,
		}
		webtransport.ConfigureHTTP3Server(h3Server)
	}

	var sessionTasks sync.WaitGroup
	h3Server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if wtServer != nil && r.Method == http.MethodConnect {
			req := &webTransportRequest{
				http3Request: &http3Request{request: r, writer: w},
				server:       wtServer,
				requested:    trevrpc.TransportBackendQUICGo,
			}
			session, admitted := server.UpgradeWebTransport(req)
			if admitted {
				sessionTasks.Go(func() {
					server.ServeWebTransport(ctx, session)
				})
			}
			return
		}
		server.HandleHTTP3(&http3Request{request: r, writer: w})
	})

	serveDone := make(chan error, 1)
	go func() {
		if wtServer != nil {
			serveDone <- wtServer.ServeQUICConn(endpoint.conn)
			return
		}
		serveDone <- h3Server.ServeQUICConn(endpoint.conn)
	}()

	serveErr := waitForHTTP3Serve(ctx, endpoint, h3Server, wtServer, opts.GracefulShutdownTimeout, serveDone)
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		server.EmitDiagnostic(trevrpc.ServerDiagnostic{
			Phase:       trevrpc.ServerDiagnosticHTTP3ConnectionClosed,
			Err:         serveErr,
			Connection:  endpoint.Info(),
			CloseReason: closeReason(serveErr),
		})
	}

	if wtServer != nil {
		_ = wtServer.Close()
	} else {
		_ = h3Server.Close()
	}
	waitForSessionTasks(&sessionTasks, opts.GracefulShutdownTimeout)
}

func waitForHTTP3Serve(
	ctx context.Context,
	endpoint *quicEndpoint,
	h3Server *http3.Server,
	wtServer *webtransport.Server,
	gracefulTimeout time.Duration,
	serveDone <-chan error,
) error {
	select {
	case err := <-serveDone:
		return err
	case <-ctx.Done():
	}

	shutdownCtx := context.Background()
	cancel := func() {}
	if gracefulTimeout > 0 {
		shutdownCtx, cancel = context.WithTimeout(shutdownCtx, gracefulTimeout)
	}
	defer cancel()

	go func() {
		_ = endpoint.conn.CloseWithError(0, "server shutdown")
		if wtServer != nil {
			_ = wtServer.Close()
		} else {
			_ = h3Server.Shutdown(shutdownCtx)
		}
	}()

	select {
	case err := <-serveDone:
		return err
	case <-shutdownCtx.Done():
		_ = endpoint.conn.CloseWithError(0, "server shutdown timeout")
		return <-serveDone
	}
}

func waitForSessionTasks(tasks *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		tasks.Wait()
		close(done)
	}()
	if timeout <= 0 {
		<-done
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
	}
}
