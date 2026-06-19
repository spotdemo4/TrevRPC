//go:build trevrpc_msquic_native && cgo

package trevrpc

/*
#cgo pkg-config: msquic
#cgo CFLAGS: -I${SRCDIR}/../trevrpc-c/include -I${SRCDIR}/../trevrpc-c/src
#cgo LDFLAGS: -lpthread
#include <stdlib.h>
#include "trevrpc_msquic.c"
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

// NativeMsQuicConfig configures the experimental C MsQuic backend.
type NativeMsQuicConfig struct {
	CertFile                      string
	KeyFile                       string
	MaxIdleTimeout                time.Duration
	KeepAlive                     time.Duration
	PeerBidiStreamCount           int
	MaxStatelessOperations        int
	MaxBindingStatelessOperations int
}

// NativeMsQuicListener accepts native MsQuic connections from trevrpc-c.
type NativeMsQuicListener struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_msquic_listener
	closeOnce sync.Once
}

// NativeMsQuicConn is an accepted or dialed native MsQuic connection.
type NativeMsQuicConn struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_msquic_conn
	closeOnce sync.Once
}

// NativeMsQuicClient sends TrevRPC calls over an established native MsQuic connection.
type NativeMsQuicClient struct {
	conn         *NativeMsQuicConn
	maxFrameSize int
}

// ListenNativeMsQuic starts a native MsQuic listener.
func ListenNativeMsQuic(addr string, config NativeMsQuicConfig) (*NativeMsQuicListener, error) {
	host, port, err := splitNativeMsQuicAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var listener *C.trevrpc_msquic_listener
	err = withNativeMsQuicConfig(config, func(cConfig *C.trevrpc_msquic_config) error {
		code := C.trevrpc_msquic_listen(cHost, port, cConfig, &listener)
		if code != 0 {
			return nativeMsQuicError(code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &NativeMsQuicListener{ptr: listener}, nil
}

// DialNativeMsQuic dials a native MsQuic connection.
func DialNativeMsQuic(ctx context.Context, addr string, config NativeMsQuicConfig) (*NativeMsQuicConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	host, port, err := splitNativeMsQuicAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var conn *C.trevrpc_msquic_conn
	err = withNativeMsQuicConfig(config, func(cConfig *C.trevrpc_msquic_config) error {
		code := C.trevrpc_msquic_dial(cHost, port, cConfig, &conn)
		if code != 0 {
			return nativeMsQuicOrContextStatus(ctx, code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &NativeMsQuicConn{ptr: conn}, nil
}

// Accept accepts one native MsQuic connection.
func (l *NativeMsQuicListener) Accept() (*NativeMsQuicConn, error) {
	ptr := l.cptr()
	if ptr == nil {
		return nil, Cancelled("native MsQuic listener closed")
	}

	var conn *C.trevrpc_msquic_conn
	code := C.trevrpc_msquic_listener_accept(ptr, &conn)
	if code != 0 {
		return nil, nativeMsQuicError(code)
	}

	return &NativeMsQuicConn{ptr: conn}, nil
}

// Close stops the listener and releases native resources.
func (l *NativeMsQuicListener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		ptr := l.takePtr()
		if ptr != nil {
			C.trevrpc_msquic_listener_close(ptr)
		}
	})
	return nil
}

func (l *NativeMsQuicListener) shutdown() {
	if l == nil {
		return
	}
	ptr := l.cptr()
	if ptr != nil {
		C.trevrpc_msquic_listener_shutdown(ptr)
	}
}

func (l *NativeMsQuicListener) cptr() *C.trevrpc_msquic_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ptr
}

func (l *NativeMsQuicListener) takePtr() *C.trevrpc_msquic_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	ptr := l.ptr
	l.ptr = nil
	return ptr
}

// NewNativeMsQuicClient creates a TrevRPC client over an established native MsQuic connection.
func NewNativeMsQuicClient(conn *NativeMsQuicConn) *NativeMsQuicClient {
	return &NativeMsQuicClient{conn: conn, maxFrameSize: DefaultMaxFrameSize}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *NativeMsQuicClient) WithMaxFrameSize(maxFrameSize int) *NativeMsQuicClient {
	t.maxFrameSize = maxFrameSize
	return t
}

// Conn returns the underlying native MsQuic connection.
func (t *NativeMsQuicClient) Conn() *NativeMsQuicConn {
	return t.conn
}

// Call sends a unary RPC request over native MsQuic and returns its response.
func (t *NativeMsQuicClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	stream, err := t.conn.OpenStream(ctx)
	if err != nil {
		return nil, Unavailable("native MsQuic open stream: " + err.Error())
	}
	defer stream.destroy()
	stopCancel := stream.trevrpcCancelReadOnContext(ctx)
	defer stopCancel()

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		return nil, Unavailable("native MsQuic write request: " + nativeMsQuicOrContextErr(ctx, err).Error())
	}
	if err := stream.Close(); err != nil {
		return nil, Unavailable("native MsQuic close request send: " + nativeMsQuicOrContextErr(ctx, err).Error())
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, Unavailable("native MsQuic read response: " + nativeMsQuicOrContextErr(ctx, err).Error())
	}

	return response, nil
}

// StreamingCall sends a streaming RPC request over native MsQuic and returns response frames.
func (t *NativeMsQuicClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	if err := streamCtx.Err(); err != nil {
		cancel()
		return nil, statusFromContextError(err)
	}

	stream, err := t.conn.OpenStream(streamCtx)
	if err != nil {
		cancel()
		return nil, err
	}

	writerDone := make(chan error, 1)
	stopCancel := stream.trevrpcCancelReadOnContext(streamCtx)
	go func() {
		writerDone <- writeNativeMsQuicStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &nativeMsQuicResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, stopCancel: stopCancel, maxFrameSize: t.maxFrameSize}, nil
}

func (c *NativeMsQuicConn) OpenStream(ctx context.Context) (*nativeMsQuicStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	ptr := c.cptr()
	if ptr == nil {
		return nil, Cancelled("native MsQuic connection closed")
	}

	var stream *C.trevrpc_msquic_stream
	code := C.trevrpc_msquic_conn_open_stream(ptr, &stream)
	if code != 0 {
		return nil, nativeMsQuicOrContextStatus(ctx, code)
	}

	return &nativeMsQuicStream{ptr: stream}, nil
}

func (c *NativeMsQuicConn) AcceptStream(ctx context.Context) (*nativeMsQuicStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	ptr := c.cptr()
	if ptr == nil {
		return nil, Cancelled("native MsQuic connection closed")
	}

	var stream *C.trevrpc_msquic_stream
	code := C.trevrpc_msquic_conn_accept_stream(ptr, &stream)
	if code != 0 {
		return nil, nativeMsQuicOrContextStatus(ctx, code)
	}

	return &nativeMsQuicStream{ptr: stream}, nil
}

// Close closes the native MsQuic connection.
func (c *NativeMsQuicConn) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		ptr := c.takePtr()
		if ptr != nil {
			C.trevrpc_msquic_conn_close(ptr)
		}
	})
	return nil
}

func (c *NativeMsQuicConn) shutdown() {
	if c == nil {
		return
	}
	ptr := c.cptr()
	if ptr != nil {
		C.trevrpc_msquic_conn_shutdown(ptr)
	}
}

func (c *NativeMsQuicConn) cptr() *C.trevrpc_msquic_conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ptr
}

func (c *NativeMsQuicConn) takePtr() *C.trevrpc_msquic_conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	ptr := c.ptr
	c.ptr = nil
	return ptr
}

type nativeMsQuicResponseStream struct {
	stream       *nativeMsQuicStream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *nativeMsQuicResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *nativeMsQuicResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, err := s.trevrpcRecvStreamFrameFields()
	if err != nil {
		return nil, err
	}

	return frame.rpcStreamFrame(), nil
}

func (s *nativeMsQuicResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, error) {
	if s.done {
		return streamFrameFields{}, io.EOF
	}

	frame, read, err := readStreamFrameFieldsOrEOF(s.stream, s.maxFrameSize)
	if err != nil {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, writerErr
		}
		return streamFrameFields{}, nativeMsQuicErrorFromErr(err)
	}

	if !read {
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return streamFrameFields{}, writerErr
		}
		return streamFrameFields{}, io.EOF
	}

	if frame.kind == RpcStreamFrameKindStatus {
		s.finish(false)
		if frame.statusValue().IsOK() {
			if err := s.writerError(true); err != nil {
				return streamFrameFields{}, err
			}
		} else {
			s.ignoreWriterError()
		}
	}

	return frame, nil
}

func (s *nativeMsQuicResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *nativeMsQuicResponseStream) finish(abortRead bool) {
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
		_ = s.stream.abortRead()
	}
	s.stream.destroy()
}

func (s *nativeMsQuicResponseStream) writerError(ignoreCancelled bool) error {
	if s.writerDone == nil {
		return nil
	}

	err := <-s.writerDone
	s.writerDone = nil
	if err == nil {
		return nil
	}
	if ignoreCancelled && StatusFromError(err).Code == CodeCancelled {
		return nil
	}

	return err
}

func (s *nativeMsQuicResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func writeNativeMsQuicStreamingRequest(ctx context.Context, stream *nativeMsQuicStream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		return nativeMsQuicOrContextErr(ctx, err)
	}

	for {
		body, err := recvRequestBody(ctx, requestBody)
		if err == io.EOF {
			break
		}

		if err != nil {
			return nativeMsQuicOrContextErr(ctx, err)
		}

		if err := writeMessageStreamFrame(stream, body, maxFrameSize); err != nil {
			return nativeMsQuicOrContextErr(ctx, err)
		}
	}

	return stream.Close()
}

// ServeNativeMsQuic accepts native MsQuic connections and serves TrevRPC until ctx is cancelled.
func ServeNativeMsQuic(ctx context.Context, listener *NativeMsQuicListener, server *Server) error {
	connectionLimit := newSemaphore(server.options.MaxConcurrentConnections)
	requestLimit := newSemaphore(server.options.MaxConcurrentRequests)
	connectionsCtx, stopConnections := context.WithCancel(context.Background())
	defer stopConnections()

	var connectionTasks sync.WaitGroup

	go func() {
		<-ctx.Done()
		stopConnections()
		listener.shutdown()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || connectionsCtx.Err() != nil {
				break
			}

			stopConnections()
			waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {})
			return err
		}

		if !tryAcquire(connectionLimit) {
			_ = conn.Close()
			continue
		}

		connectionTasks.Go(func() {
			defer release(connectionLimit)
			handleNativeMsQuicConnection(connectionsCtx, conn, server, requestLimit, true)
		})
	}

	stopConnections()
	waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {})

	return nil
}

func handleNativeMsQuicConnection(ctx context.Context, conn *NativeMsQuicConn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
	streamLimit := newSemaphore(server.options.MaxConcurrentStreamsPerConnection)
	var streamTasks sync.WaitGroup

	go func() {
		<-ctx.Done()
		conn.shutdown()
	}()

	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			break
		}

		if !tryAcquire(streamLimit) {
			writeStatusResponse(stream, Unavailable("too many concurrent streams on connection"), server.options.MaxFrameSize)
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
		_ = conn.Close()
	})

	if closeOnShutdown && ctx.Err() != nil {
		_ = conn.Close()
	}
}

type nativeMsQuicStream struct {
	mu          sync.Mutex
	ptr         *C.trevrpc_msquic_stream
	closeOnce   sync.Once
	destroyOnce sync.Once
}

func (s *nativeMsQuicStream) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	ptr := s.cptr()
	if ptr == nil {
		return 0, io.EOF
	}

	n := C.trevrpc_msquic_stream_read(ptr, (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if n > 0 {
		return int(n), nil
	}
	if n == 0 {
		return 0, io.EOF
	}

	return 0, nativeMsQuicError(C.int(n))
}

func (s *nativeMsQuicStream) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	ptr := s.cptr()
	if ptr == nil {
		return 0, io.ErrClosedPipe
	}

	n := C.trevrpc_msquic_stream_write(ptr, (*C.uint8_t)(unsafe.Pointer(&data[0])), C.size_t(len(data)))
	if n >= 0 {
		return int(n), nil
	}

	return 0, nativeMsQuicError(C.int(n))
}

func (s *nativeMsQuicStream) trevrpcReadFrame(message ProtoMessage, maxFrameSize int) (bool, error) {
	ptr := s.cptr()
	if ptr == nil {
		return false, nil
	}

	var body *C.uint8_t
	var bodyLen C.size_t
	result := C.trevrpc_msquic_stream_read_frame(ptr, &body, &bodyLen, C.size_t(maxFrameSize))
	if result == 0 {
		return false, nil
	}
	if result < 0 {
		if result == C.TREV_MSQUIC_ERR_FRAME_TOO_LARGE {
			return false, &FrameTooLargeError{Len: int(bodyLen), Max: maxFrameSize}
		}
		return false, nativeMsQuicError(C.int(result))
	}
	if body != nil {
		defer C.trevrpc_msquic_free(unsafe.Pointer(body))
	}

	var frameBody []byte
	if bodyLen > 0 {
		frameBody = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(bodyLen))
	}
	return true, DecodeFrame(frameBody, message)
}

func (s *nativeMsQuicStream) trevrpcReadStreamFrame(maxFrameSize int) (streamFrameFields, bool, error) {
	ptr := s.cptr()
	if ptr == nil {
		return streamFrameFields{}, false, nil
	}

	var body *C.uint8_t
	var bodyLen C.size_t
	result := C.trevrpc_msquic_stream_read_frame(ptr, &body, &bodyLen, C.size_t(maxFrameSize))
	if result == 0 {
		return streamFrameFields{}, false, nil
	}
	if result < 0 {
		if result == C.TREV_MSQUIC_ERR_FRAME_TOO_LARGE {
			return streamFrameFields{}, false, &FrameTooLargeError{Len: int(bodyLen), Max: maxFrameSize}
		}
		return streamFrameFields{}, false, nativeMsQuicError(C.int(result))
	}
	if body != nil {
		defer C.trevrpc_msquic_free(unsafe.Pointer(body))
	}

	var frameBody []byte
	if bodyLen > 0 {
		frameBody = unsafe.Slice((*byte)(unsafe.Pointer(body)), int(bodyLen))
	}
	fields, err := parseStreamFrameFields(frameBody, true)
	return fields, true, err
}

func (s *nativeMsQuicStream) trevrpcWriteFrame(message ProtoMessage, maxFrameSize int) error {
	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = s.Write(frame)
	return err
}

func (s *nativeMsQuicStream) trevrpcWriteMessageStreamFrame(body []byte, maxFrameSize int) error {
	bodyLen := messageStreamFrameBodyLen(body)
	if bodyLen > maxFrameSize {
		return &FrameTooLargeError{Len: bodyLen, Max: maxFrameSize}
	}

	ptr := s.cptr()
	if ptr == nil {
		return io.ErrClosedPipe
	}

	var bodyPtr *C.uint8_t
	if len(body) > 0 {
		bodyPtr = (*C.uint8_t)(unsafe.Pointer(&body[0]))
	}
	result := C.trevrpc_msquic_stream_write_message_frame(ptr, bodyPtr, C.size_t(len(body)), C.size_t(maxFrameSize))
	if result >= 0 {
		return nil
	}
	if result == C.TREV_MSQUIC_ERR_FRAME_TOO_LARGE {
		return &FrameTooLargeError{Len: bodyLen, Max: maxFrameSize}
	}

	return nativeMsQuicError(C.int(result))
}

func (s *nativeMsQuicStream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		ptr := s.cptr()
		if ptr == nil {
			return
		}
		code := C.trevrpc_msquic_stream_shutdown_send(ptr)
		if code != 0 {
			err = nativeMsQuicError(code)
		}
	})
	return err
}

func (s *nativeMsQuicStream) abortRead() error {
	ptr := s.cptr()
	if ptr == nil {
		return nil
	}
	code := C.trevrpc_msquic_stream_abort_receive(ptr)
	if code != 0 {
		return nativeMsQuicError(code)
	}
	return nil
}

func (s *nativeMsQuicStream) destroy() {
	s.destroyOnce.Do(func() {
		ptr := s.takePtr()
		if ptr != nil {
			C.trevrpc_msquic_stream_close(ptr)
		}
	})
}

func (s *nativeMsQuicStream) cptr() *C.trevrpc_msquic_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ptr
}

func (s *nativeMsQuicStream) takePtr() *C.trevrpc_msquic_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	ptr := s.ptr
	s.ptr = nil
	return ptr
}

func (s *nativeMsQuicStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	done := make(chan struct{})
	var closeOnce sync.Once
	go func() {
		select {
		case <-ctx.Done():
			_ = s.abortRead()
		case <-done:
		}
	}()

	return func() { closeOnce.Do(func() { close(done) }) }
}

func withNativeMsQuicConfig(config NativeMsQuicConfig, fn func(*C.trevrpc_msquic_config) error) error {
	alpn := C.CString(ALPN)
	defer C.free(unsafe.Pointer(alpn))

	var certFile *C.char
	if config.CertFile != "" {
		certFile = C.CString(config.CertFile)
		defer C.free(unsafe.Pointer(certFile))
	}
	var keyFile *C.char
	if config.KeyFile != "" {
		keyFile = C.CString(config.KeyFile)
		defer C.free(unsafe.Pointer(keyFile))
	}

	cConfig := C.trevrpc_msquic_config{
		alpn:                             alpn,
		alpn_len:                         C.uint32_t(len(ALPN)),
		cert_file:                        certFile,
		key_file:                         keyFile,
		max_idle_timeout_ms:              C.uint64_t(durationMillis(config.MaxIdleTimeout)),
		keep_alive_ms:                    C.uint32_t(durationMillis(config.KeepAlive)),
		peer_bidi_stream_count:           C.uint16_t(config.PeerBidiStreamCount),
		max_stateless_operations:         C.uint32_t(config.MaxStatelessOperations),
		max_binding_stateless_operations: C.uint16_t(config.MaxBindingStatelessOperations),
	}

	return fn(&cConfig)
}

func splitNativeMsQuicAddr(addr string) (string, C.uint16_t, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port < 0 || port > 65535 {
		return "", 0, InvalidArgument("native MsQuic address port is out of range")
	}
	if host == "" {
		host = "0.0.0.0"
	}

	return host, C.uint16_t(port), nil
}

func durationMillis(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration / time.Millisecond)
}

func nativeMsQuicError(code C.int) error {
	message := C.GoString(C.trevrpc_msquic_error(code))
	if message == "closed" {
		return Cancelled("native MsQuic closed")
	}
	return Unavailable(fmt.Sprintf("native MsQuic transport unavailable: %s (%d)", message, int(code)))
}

func nativeMsQuicErrorFromErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable("native MsQuic transport unavailable: " + err.Error())
	}
	return transportStatus(err)
}

func nativeMsQuicOrContextStatus(ctx context.Context, code C.int) error {
	if err := ctx.Err(); err != nil {
		return statusFromContextError(err)
	}
	return nativeMsQuicError(code)
}

func nativeMsQuicOrContextErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	return nativeMsQuicErrorFromErr(err)
}
