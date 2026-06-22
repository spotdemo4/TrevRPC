//go:build trevrpc_webtransport_native && cgo

package trevrpc

/*
#cgo CFLAGS: -I${SRCDIR}/../trevrpc-c/include -I${SRCDIR}/../trevrpc-c/src
#cgo LDFLAGS: -lwtf -lpthread -lm -lrt -lcrypto
#include <stdlib.h>
#include "trevrpc_webtransport.c"
*/
import "C"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"
	"unsafe"
)

// NativeWebTransportConfig configures the experimental C libwtf WebTransport backend.
type NativeWebTransportConfig struct {
	Path                        string
	Origin                      string
	CertFile                    string
	KeyFile                     string
	CACertFile                  string
	SkipCertificateValidation   bool
	MaxSessionsPerConnection    int
	MaxStreamsPerSession        int
	MaxDataPerSession           uint64
	StreamReceiveWindow         uint32
	ConnectionFlowControlWindow uint32
	IdleTimeout                 time.Duration
	HandshakeTimeout            time.Duration
}

// NativeWebTransportListener accepts WebTransport sessions from trevrpc-c/libwtf.
type NativeWebTransportListener struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_wt_listener
	closeOnce sync.Once
}

// NativeWebTransportSession is an accepted or dialed native WebTransport session.
type NativeWebTransportSession struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_wt_session
	closeOnce sync.Once
}

// NativeWebTransportClient sends TrevRPC calls over an established native WebTransport session.
type NativeWebTransportClient struct {
	session      *NativeWebTransportSession
	maxFrameSize int
}

// ListenNativeWebTransport starts a native WebTransport listener backed by libwtf.
func ListenNativeWebTransport(addr string, config NativeWebTransportConfig) (*NativeWebTransportListener, error) {
	host, port, err := splitNativeWebTransportAddr(addr)
	if err != nil {
		return nil, err
	}
	var listener *C.trevrpc_wt_listener
	err = withNativeWebTransportConfig(config, host, "", port, func(cConfig *C.trevrpc_wt_config) error {
		code := C.trevrpc_wt_listen(cConfig, &listener)
		if code != 0 {
			return nativeWebTransportError(code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &NativeWebTransportListener{ptr: listener}, nil
}

// DialNativeWebTransport dials a native WebTransport session backed by libwtf.
func DialNativeWebTransport(ctx context.Context, url string, config NativeWebTransportConfig) (*NativeWebTransportSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	var session *C.trevrpc_wt_session
	err := withNativeWebTransportConfig(config, "", url, 0, func(cConfig *C.trevrpc_wt_config) error {
		code := C.trevrpc_wt_dial(cConfig, &session)
		if code != 0 {
			return nativeWebTransportOrContextStatus(ctx, code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &NativeWebTransportSession{ptr: session}, nil
}

// Accept accepts one native WebTransport session.
func (l *NativeWebTransportListener) Accept() (*NativeWebTransportSession, error) {
	ptr := l.cptr()
	if ptr == nil {
		return nil, Cancelled("native WebTransport listener closed")
	}
	var session *C.trevrpc_wt_session
	code := C.trevrpc_wt_listener_accept_session(ptr, &session)
	if code != 0 {
		return nil, nativeWebTransportError(code)
	}
	return &NativeWebTransportSession{ptr: session}, nil
}

// Close stops the listener and releases native resources.
func (l *NativeWebTransportListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		ptr := l.takePtr()
		if ptr != nil {
			C.trevrpc_wt_listener_close(ptr)
		}
	})
	return nil
}

func (l *NativeWebTransportListener) shutdown() {
	if l == nil {
		return
	}
	ptr := l.cptr()
	if ptr != nil {
		C.trevrpc_wt_listener_shutdown(ptr)
	}
}

func (l *NativeWebTransportListener) cptr() *C.trevrpc_wt_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ptr
}

func (l *NativeWebTransportListener) takePtr() *C.trevrpc_wt_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	ptr := l.ptr
	l.ptr = nil
	return ptr
}

// NewNativeWebTransportClient creates a TrevRPC client over an established native WebTransport session.
func NewNativeWebTransportClient(session *NativeWebTransportSession) *NativeWebTransportClient {
	return &NativeWebTransportClient{session: session, maxFrameSize: DefaultMaxFrameSize}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *NativeWebTransportClient) WithMaxFrameSize(maxFrameSize int) *NativeWebTransportClient {
	t.maxFrameSize = maxFrameSize
	return t
}

// Call sends a unary RPC request over native WebTransport and returns its response.
func (t *NativeWebTransportClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	stream, err := t.session.OpenStream(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.destroy()
	stopCancel := stream.trevrpcCancelReadOnContext(ctx)
	defer stopCancel()

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		return nil, Unavailable("native WebTransport write request: " + nativeWebTransportOrContextErr(ctx, err).Error())
	}
	if err := stream.Close(); err != nil {
		return nil, Unavailable("native WebTransport close request send: " + nativeWebTransportOrContextErr(ctx, err).Error())
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, Unavailable("native WebTransport read response: " + nativeWebTransportOrContextErr(ctx, err).Error())
	}
	return response, nil
}

// StreamingCall sends a streaming RPC request over native WebTransport and returns response frames.
func (t *NativeWebTransportClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	if err := streamCtx.Err(); err != nil {
		cancel()
		return nil, statusFromContextError(err)
	}
	stream, err := t.session.OpenStream(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}

	writerDone := make(chan error, 1)
	stopCancel := stream.trevrpcCancelReadOnContext(streamCtx)
	go func() {
		writerDone <- writeNativeWebTransportStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()
	return &nativeWebTransportResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, stopCancel: stopCancel, maxFrameSize: t.maxFrameSize}, nil
}

func writeNativeWebTransportStreamingRequest(ctx context.Context, stream *nativeWebTransportStream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	defer closeMessageStream(requestBody)
	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		return nativeWebTransportOrContextErr(ctx, err)
	}
	if err := writeRequestBodyFrames(ctx, stream, requestBody, maxFrameSize); err != nil {
		return nativeWebTransportOrContextErr(ctx, err)
	}
	return stream.Close()
}

type nativeWebTransportResponseStream struct {
	stream       *nativeWebTransportStream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *nativeWebTransportResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *nativeWebTransportResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, _, err := s.recvStreamFrameFields(true)
	if err != nil {
		return nil, err
	}
	return frame.rpcStreamFrame(), nil
}

func (s *nativeWebTransportResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error) {
	return s.recvStreamFrameFields(false)
}

func (s *nativeWebTransportResponseStream) recvStreamFrameFields(copyBytes bool) (streamFrameFields, func(), error) {
	if s.done {
		return streamFrameFields{}, nil, io.EOF
	}
	frame, release, read, err := s.stream.trevrpcReadStreamFrameWithCopy(s.maxFrameSize, copyBytes)
	if err != nil {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			if release != nil {
				release()
			}
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, release, nativeWebTransportErrorFromErr(err)
	}
	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, nil, writerErr
		}
		return streamFrameFields{}, nil, io.EOF
	}
	if frame.kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if frame.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				if release != nil {
					release()
				}
				return streamFrameFields{}, nil, err
			}
		} else {
			s.ignoreWriterError()
		}
	}
	return frame, release, nil
}

func (s *nativeWebTransportResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *nativeWebTransportResponseStream) finish(abortRead bool) {
	if s.done {
		return
	}
	s.done = true
	if s.stopCancel != nil {
		s.stopCancel()
		s.stopCancel = nil
	}
	if s.cancel != nil {
		s.cancel()
	}
	if abortRead {
		_ = s.stream.abort()
	}
}

func (s *nativeWebTransportResponseStream) writerError(ignoreCancelled bool) error {
	if s.writerDone == nil {
		s.stream.destroy()
		return nil
	}
	err := <-s.writerDone
	s.writerDone = nil
	s.stream.destroy()
	if err == nil {
		return nil
	}
	if ignoreCancelled && StatusFromError(err).Code == CodeCancelled {
		return nil
	}
	return err
}

func (s *nativeWebTransportResponseStream) ignoreWriterError() { _ = s.writerError(true) }

func (s *NativeWebTransportSession) OpenStream(ctx context.Context) (*nativeWebTransportStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	ptr := s.cptr()
	if ptr == nil {
		return nil, Cancelled("native WebTransport session closed")
	}
	var stream *C.trevrpc_wt_stream
	code := C.trevrpc_wt_session_open_stream(ptr, &stream)
	if code != 0 {
		return nil, nativeWebTransportOrContextStatus(ctx, code)
	}
	return &nativeWebTransportStream{ptr: stream}, nil
}

func (s *NativeWebTransportSession) AcceptStream(ctx context.Context) (*nativeWebTransportStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	ptr := s.cptr()
	if ptr == nil {
		return nil, Cancelled("native WebTransport session closed")
	}
	var stream *C.trevrpc_wt_stream
	code := C.trevrpc_wt_session_accept_stream(ptr, &stream)
	if code != 0 {
		return nil, nativeWebTransportOrContextStatus(ctx, code)
	}
	return &nativeWebTransportStream{ptr: stream}, nil
}

// Close closes the native WebTransport session.
func (s *NativeWebTransportSession) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		ptr := s.takePtr()
		if ptr != nil {
			C.trevrpc_wt_session_close(ptr)
		}
	})
	return nil
}

func (s *NativeWebTransportSession) cptr() *C.trevrpc_wt_session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ptr
}

func (s *NativeWebTransportSession) takePtr() *C.trevrpc_wt_session {
	s.mu.Lock()
	defer s.mu.Unlock()
	ptr := s.ptr
	s.ptr = nil
	return ptr
}

// ServeNativeWebTransport accepts native WebTransport sessions and serves TrevRPC until ctx is cancelled.
func ServeNativeWebTransport(ctx context.Context, listener *NativeWebTransportListener, server *Server) error {
	connectionLimit := newSemaphore(server.options.MaxConcurrentConnections)
	requestLimit := newSemaphore(server.options.MaxConcurrentRequests)
	sessionsCtx, stopSessions := context.WithCancel(context.Background())
	defer stopSessions()

	var sessionTasks sync.WaitGroup
	go func() {
		<-ctx.Done()
		stopSessions()
		listener.shutdown()
	}()

	for {
		session, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || sessionsCtx.Err() != nil {
				break
			}
			stopSessions()
			waitForWaitGroup(&sessionTasks, server.options.GracefulShutdownTimeout, func() {})
			return err
		}
		if !tryAcquire(connectionLimit) {
			_ = session.Close()
			continue
		}
		sessionTasks.Go(func() {
			defer release(connectionLimit)
			handleNativeWebTransportSession(sessionsCtx, session, server, requestLimit, true)
		})
	}

	stopSessions()
	waitForWaitGroup(&sessionTasks, server.options.GracefulShutdownTimeout, func() {})
	return nil
}

func handleNativeWebTransportSession(ctx context.Context, session *NativeWebTransportSession, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	streamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)
	var streamTasks sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = session.Close()
	}()

	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			break
		}
		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable("too many concurrent streams on native WebTransport session"), server.options.MaxFrameSize)
			stream.destroy()
			continue
		}
		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer stream.destroy()
			handleRPCStream(streamCtx, server, requestLimit, stream)
		})
	}

	waitForWaitGroup(&streamTasks, server.options.GracefulShutdownTimeout, func() {
		_ = session.Close()
	})
	if closeOnShutdown && ctx.Err() != nil {
		_ = session.Close()
	}
}

type nativeWebTransportStream struct {
	mu          sync.Mutex
	ptr         *C.trevrpc_wt_stream
	closeOnce   sync.Once
	destroyOnce sync.Once
}

func (s *nativeWebTransportStream) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	ptr := s.cptr()
	if ptr == nil {
		return 0, io.EOF
	}
	n := C.trevrpc_wt_stream_read(ptr, (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if n > 0 {
		return int(n), nil
	}
	if n == 0 {
		return 0, io.EOF
	}
	return 0, nativeWebTransportError(C.int(n))
}

func (s *nativeWebTransportStream) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	ptr := s.cptr()
	if ptr == nil {
		return 0, io.ErrClosedPipe
	}
	n := C.trevrpc_wt_stream_write(ptr, (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if n >= 0 {
		return int(n), nil
	}
	return 0, nativeWebTransportError(C.int(n))
}

func (s *nativeWebTransportStream) trevrpcReadFrame(message ProtoMessage, maxFrameSize int) (bool, error) {
	ptr := s.cptr()
	if ptr == nil {
		return false, nil
	}
	var body *C.uint8_t
	var bodyLen C.size_t
	result := C.trevrpc_wt_stream_read_frame(ptr, &body, &bodyLen, C.size_t(maxFrameSize))
	if result == 0 {
		return false, nil
	}
	if result < 0 {
		if result == C.TREV_WT_ERR_FRAME_TOO_LARGE {
			return false, &FrameTooLargeError{Len: int(bodyLen), Max: maxFrameSize}
		}
		return false, nativeWebTransportError(C.int(result))
	}
	if body != nil {
		defer C.trevrpc_wt_free(unsafe.Pointer(body))
	}
	var frameBody []byte
	if bodyLen > 0 {
		frameBody = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(bodyLen))
	}
	return true, DecodeFrame(frameBody, message)
}

func (s *nativeWebTransportStream) trevrpcReadStreamFrame(maxFrameSize int) (streamFrameFields, bool, error) {
	fields, release, read, err := s.trevrpcReadStreamFrameWithCopy(maxFrameSize, true)
	if release != nil {
		release()
	}
	return fields, read, err
}

func (s *nativeWebTransportStream) trevrpcReadStreamFrameReleasable(maxFrameSize int) (streamFrameFields, func(), bool, error) {
	return s.trevrpcReadStreamFrameWithCopy(maxFrameSize, false)
}

func (s *nativeWebTransportStream) trevrpcReadStreamFrameWithCopy(maxFrameSize int, copyBytes bool) (streamFrameFields, func(), bool, error) {
	ptr := s.cptr()
	if ptr == nil {
		return streamFrameFields{}, nil, false, nil
	}
	var body *C.uint8_t
	var bodyLen C.size_t
	result := C.trevrpc_wt_stream_read_frame(ptr, &body, &bodyLen, C.size_t(maxFrameSize))
	if result == 0 {
		return streamFrameFields{}, nil, false, nil
	}
	if result < 0 {
		if result == C.TREV_WT_ERR_FRAME_TOO_LARGE {
			return streamFrameFields{}, nil, false, &FrameTooLargeError{Len: int(bodyLen), Max: maxFrameSize}
		}
		return streamFrameFields{}, nil, false, nativeWebTransportError(C.int(result))
	}
	release := func() {}
	if body != nil {
		release = func() { C.trevrpc_wt_free(unsafe.Pointer(body)) }
	}
	var frameBody []byte
	if bodyLen > 0 {
		frameBody = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(bodyLen))
	}
	fields, err := parseStreamFrameFields(frameBody, copyBytes)
	if err != nil {
		release()
		return streamFrameFields{}, nil, false, err
	}
	if copyBytes {
		release()
		return fields, nil, true, nil
	}
	return fields, release, true, nil
}

func (s *nativeWebTransportStream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		ptr := s.cptr()
		if ptr == nil {
			return
		}
		code := C.trevrpc_wt_stream_shutdown_send(ptr)
		if code != 0 {
			err = nativeWebTransportError(code)
		}
	})
	return err
}

func (s *nativeWebTransportStream) abort() error {
	ptr := s.cptr()
	if ptr == nil {
		return nil
	}
	code := C.trevrpc_wt_stream_abort(ptr, 1)
	if code != 0 {
		return nativeWebTransportError(code)
	}
	return nil
}

func (s *nativeWebTransportStream) destroy() {
	s.destroyOnce.Do(func() {
		ptr := s.takePtr()
		if ptr != nil {
			C.trevrpc_wt_stream_close(ptr)
		}
	})
}

func (s *nativeWebTransportStream) cptr() *C.trevrpc_wt_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ptr
}

func (s *nativeWebTransportStream) takePtr() *C.trevrpc_wt_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	ptr := s.ptr
	s.ptr = nil
	return ptr
}

func (s *nativeWebTransportStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}
	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = s.abort()
		case <-done:
		}
	}()
	return func() { closeOnce.Do(func() { close(done) }) }
}

func withNativeWebTransportConfig(config NativeWebTransportConfig, host, url string, port C.uint16_t, fn func(*C.trevrpc_wt_config) error) error {
	var cHost *C.char
	if host != "" {
		cHost = C.CString(host)
		defer C.free(unsafe.Pointer(cHost))
	}
	var cURL *C.char
	if url != "" {
		cURL = C.CString(url)
		defer C.free(unsafe.Pointer(cURL))
	}
	path := config.Path
	if path == "" {
		path = "/trevrpc"
	}
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))
	var cOrigin *C.char
	if config.Origin != "" {
		cOrigin = C.CString(config.Origin)
		defer C.free(unsafe.Pointer(cOrigin))
	}
	var cCertFile *C.char
	if config.CertFile != "" {
		cCertFile = C.CString(config.CertFile)
		defer C.free(unsafe.Pointer(cCertFile))
	}
	var cKeyFile *C.char
	if config.KeyFile != "" {
		cKeyFile = C.CString(config.KeyFile)
		defer C.free(unsafe.Pointer(cKeyFile))
	}
	var cCAFile *C.char
	if config.CACertFile != "" {
		cCAFile = C.CString(config.CACertFile)
		defer C.free(unsafe.Pointer(cCAFile))
	}
	skipCert := C.int(0)
	if config.SkipCertificateValidation {
		skipCert = 1
	}
	cConfig := C.trevrpc_wt_config{
		host:                        cHost,
		port:                        port,
		url:                         cURL,
		path:                        cPath,
		origin:                      cOrigin,
		cert_file:                   cCertFile,
		key_file:                    cKeyFile,
		ca_cert_file:                cCAFile,
		skip_certificate_validation: skipCert,
		max_sessions_per_connection: C.uint32_t(config.MaxSessionsPerConnection),
		max_streams_per_session:     C.uint32_t(config.MaxStreamsPerSession),
		max_data_per_session:        C.uint64_t(config.MaxDataPerSession),
		stream_recv_window:          C.uint32_t(config.StreamReceiveWindow),
		conn_flow_control_window:    C.uint32_t(config.ConnectionFlowControlWindow),
		idle_timeout_ms:             C.uint32_t(nativeWebTransportDurationMillis(config.IdleTimeout)),
		handshake_timeout_ms:        C.uint32_t(nativeWebTransportDurationMillis(config.HandshakeTimeout)),
	}
	return fn(&cConfig)
}

func nativeWebTransportDurationMillis(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration / time.Millisecond)
}

func splitNativeWebTransportAddr(addr string) (string, C.uint16_t, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port < 0 || port > 65535 {
		return "", 0, InvalidArgument("native WebTransport address port is out of range")
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return host, C.uint16_t(port), nil
}

func nativeWebTransportError(code C.int) error {
	message := C.GoString(C.trevrpc_wt_error(code))
	if message == "closed" {
		return Cancelled("native WebTransport closed")
	}
	return Unavailable(fmt.Sprintf("native WebTransport unavailable: %s (%d)", message, int(code)))
}

func nativeWebTransportErrorFromErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable("native WebTransport unavailable: " + err.Error())
	}
	return transportStatus(err)
}

func nativeWebTransportOrContextStatus(ctx context.Context, code C.int) error {
	if err := ctx.Err(); err != nil {
		return statusFromContextError(err)
	}
	return nativeWebTransportError(code)
}

func nativeWebTransportOrContextErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	return nativeWebTransportErrorFromErr(err)
}
