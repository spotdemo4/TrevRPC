//go:build trevrpc_msquic && cgo

package trevrpc

/*
#cgo CFLAGS: -I${SRCDIR}/../trevrpc-c/include -I${SRCDIR}/../trevrpc-c/src
#cgo LDFLAGS: -lmsquic -lpthread
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

// MsQuicListener accepts MsQuic connections from trevrpc-c.
type MsQuicListener struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_msquic_listener
	addr      net.Addr
	closeOnce sync.Once
}

// MsQuicConn is an accepted or dialed MsQuic connection.
type MsQuicConn struct {
	mu        sync.Mutex
	ptr       *C.trevrpc_msquic_conn
	closeOnce sync.Once
}

// MsQuicClient sends TrevRPC calls over an established MsQuic connection.
type MsQuicClient struct {
	conn         *MsQuicConn
	maxFrameSize int
}

// ListenMsQuic starts a MsQuic listener.
func ListenMsQuic(addr string, config MsQuicConfig) (*MsQuicListener, error) {
	host, port, err := splitMsQuicAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var listener *C.trevrpc_msquic_listener
	err = withMsQuicConfig(config, func(cConfig *C.trevrpc_msquic_config) error {
		code := C.trevrpc_msquic_listen(cHost, port, cConfig, &listener)
		if code != 0 {
			return msQuicError(code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &MsQuicListener{ptr: listener, addr: msQuicListenerAddr(host, port, listener)}, nil
}

// DialMsQuic dials a MsQuic connection.
func DialMsQuic(ctx context.Context, addr string, config MsQuicConfig) (*MsQuicConn, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	host, port, err := splitMsQuicAddr(addr)
	if err != nil {
		return nil, err
	}
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))

	var conn *C.trevrpc_msquic_conn
	err = withMsQuicConfig(config, func(cConfig *C.trevrpc_msquic_config) error {
		code := C.trevrpc_msquic_dial(cHost, port, cConfig, &conn)
		if code != 0 {
			return msQuicOrContextStatus(ctx, code)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return &MsQuicConn{ptr: conn}, nil
}

// Accept accepts one MsQuic connection.
func (l *MsQuicListener) Accept() (*MsQuicConn, error) {
	ptr := l.cptr()
	if ptr == nil {
		return nil, Cancelled("MsQuic listener closed")
	}

	var conn *C.trevrpc_msquic_conn
	code := C.trevrpc_msquic_listener_accept(ptr, &conn)
	if code != 0 {
		return nil, msQuicError(code)
	}

	return &MsQuicConn{ptr: conn}, nil
}

// Addr returns the listener's local network address.
func (l *MsQuicListener) Addr() net.Addr {
	if l == nil {
		return nil
	}
	return l.addr
}

// Close stops the listener and releases native resources.
func (l *MsQuicListener) Close() error {
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

func (l *MsQuicListener) shutdown() {
	if l == nil {
		return
	}
	ptr := l.cptr()
	if ptr != nil {
		C.trevrpc_msquic_listener_shutdown(ptr)
	}
}

func (l *MsQuicListener) cptr() *C.trevrpc_msquic_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ptr
}

func (l *MsQuicListener) takePtr() *C.trevrpc_msquic_listener {
	l.mu.Lock()
	defer l.mu.Unlock()
	ptr := l.ptr
	l.ptr = nil
	return ptr
}

// NewMsQuicClient creates a TrevRPC client over an established MsQuic connection.
func NewMsQuicClient(conn *MsQuicConn) *MsQuicClient {
	return &MsQuicClient{conn: conn, maxFrameSize: DefaultMaxFrameSize}
}

// WithMaxFrameSize sets the maximum TrevRPC frame size for the client.
func (t *MsQuicClient) WithMaxFrameSize(maxFrameSize int) *MsQuicClient {
	t.maxFrameSize = maxFrameSize
	return t
}

// Conn returns the underlying MsQuic connection.
func (t *MsQuicClient) Conn() *MsQuicConn {
	return t.conn
}

// Close closes the underlying MsQuic connection.
func (t *MsQuicClient) Close() error {
	if t == nil || t.conn == nil {
		return nil
	}
	return t.conn.Close()
}

// Call sends a unary RPC request over MsQuic and returns its response.
func (t *MsQuicClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	stream, err := t.conn.OpenStream(ctx)
	if err != nil {
		return nil, Unavailable("MsQuic open stream: " + err.Error())
	}
	defer stream.destroy()
	stopCancel := stream.trevrpcCancelReadOnContext(ctx)
	defer stopCancel()

	if err := WriteFrame(stream, request, t.maxFrameSize); err != nil {
		return nil, Unavailable("MsQuic write request: " + msQuicOrContextErr(ctx, err).Error())
	}
	if err := stream.Close(); err != nil {
		return nil, Unavailable("MsQuic close request send: " + msQuicOrContextErr(ctx, err).Error())
	}

	response := &RpcResponse{}
	if err := ReadFrame(stream, response, t.maxFrameSize); err != nil {
		return nil, Unavailable("MsQuic read response: " + msQuicOrContextErr(ctx, err).Error())
	}

	return response, nil
}

// StreamingCall sends a streaming RPC request over MsQuic and returns response frames.
func (t *MsQuicClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
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
		writerDone <- writeMsQuicStreamingRequest(streamCtx, stream, request, requestBody, t.maxFrameSize)
	}()

	return &msQuicResponseStream{stream: stream, writerDone: writerDone, cancel: cancel, stopCancel: stopCancel, maxFrameSize: t.maxFrameSize}, nil
}

func (c *MsQuicConn) OpenStream(ctx context.Context) (*msQuicStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	ptr := c.cptr()
	if ptr == nil {
		return nil, Cancelled("MsQuic connection closed")
	}

	var stream *C.trevrpc_msquic_stream
	code := C.trevrpc_msquic_conn_open_stream(ptr, &stream)
	if code != 0 {
		return nil, msQuicOrContextStatus(ctx, code)
	}

	return &msQuicStream{ptr: stream}, nil
}

func (c *MsQuicConn) AcceptStream(ctx context.Context) (*msQuicStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}

	ptr := c.cptr()
	if ptr == nil {
		return nil, Cancelled("MsQuic connection closed")
	}

	var stream *C.trevrpc_msquic_stream
	code := C.trevrpc_msquic_conn_accept_stream(ptr, &stream)
	if code != 0 {
		return nil, msQuicOrContextStatus(ctx, code)
	}

	return &msQuicStream{ptr: stream}, nil
}

// Close closes the MsQuic connection.
func (c *MsQuicConn) Close() error {
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

func (c *MsQuicConn) shutdown() {
	if c == nil {
		return
	}
	ptr := c.cptr()
	if ptr != nil {
		C.trevrpc_msquic_conn_shutdown(ptr)
	}
}

func (c *MsQuicConn) cptr() *C.trevrpc_msquic_conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ptr
}

func (c *MsQuicConn) takePtr() *C.trevrpc_msquic_conn {
	c.mu.Lock()
	defer c.mu.Unlock()
	ptr := c.ptr
	c.ptr = nil
	return ptr
}

type msQuicResponseStream struct {
	stream       *msQuicStream
	writerDone   <-chan error
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
}

func (s *msQuicResponseStream) trevrpcContextCancelsRecv() bool { return true }

func (s *msQuicResponseStream) Recv() (*RpcStreamFrame, error) {
	frame, _, err := s.recvStreamFrameFields(true)
	if err != nil {
		return nil, err
	}

	return frame.rpcStreamFrame(), nil
}

func (s *msQuicResponseStream) trevrpcRecvStreamFrameFields() (streamFrameFields, func(), error) {
	return s.recvStreamFrameFields(false)
}

func (s *msQuicResponseStream) recvStreamFrameFields(copyBytes bool) (streamFrameFields, func(), error) {
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
		return streamFrameFields{}, release, msQuicErrorFromErr(err)
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

func (s *msQuicResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *msQuicResponseStream) finish(abortRead bool) {
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

func (s *msQuicResponseStream) writerError(ignoreCancelled bool) error {
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

func (s *msQuicResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func writeMsQuicStreamingRequest(ctx context.Context, stream *msQuicStream, request *RpcRequest, requestBody ByteStream, maxFrameSize int) error {
	defer closeMessageStream(requestBody)

	if err := WriteFrame(stream, request, maxFrameSize); err != nil {
		return msQuicOrContextErr(ctx, err)
	}

	if err := writeRequestBodyFrames(ctx, stream, requestBody, maxFrameSize); err != nil {
		return msQuicOrContextErr(ctx, err)
	}

	return stream.Close()
}

// ServeMsQuic accepts MsQuic connections and serves TrevRPC until ctx is cancelled.
func ServeMsQuic(ctx context.Context, listener *MsQuicListener, server *Server) error {
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
			handleMsQuicConnection(connectionsCtx, conn, server, requestLimit, true)
		})
	}

	stopConnections()
	waitForWaitGroup(&connectionTasks, server.options.GracefulShutdownTimeout, func() {})

	return nil
}

func handleMsQuicConnection(ctx context.Context, conn *MsQuicConn, server *Server, requestLimit semaphore, closeOnShutdown bool) {
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

type msQuicStream struct {
	mu          sync.Mutex
	ptr         *C.trevrpc_msquic_stream
	closeOnce   sync.Once
	destroyOnce sync.Once
}

func (s *msQuicStream) Read(data []byte) (int, error) {
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

	return 0, msQuicError(C.int(n))
}

func (s *msQuicStream) Write(data []byte) (int, error) {
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

	return 0, msQuicError(C.int(n))
}

func (s *msQuicStream) trevrpcReadFrame(message ProtoMessage, maxFrameSize int) (bool, error) {
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
		return false, msQuicError(C.int(result))
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

func (s *msQuicStream) trevrpcReadStreamFrame(maxFrameSize int) (streamFrameFields, bool, error) {
	fields, release, read, err := s.trevrpcReadStreamFrameWithCopy(maxFrameSize, true)
	if release != nil {
		release()
	}
	return fields, read, err
}

func (s *msQuicStream) trevrpcReadStreamFrameReleasable(maxFrameSize int) (streamFrameFields, func(), bool, error) {
	return s.trevrpcReadStreamFrameWithCopy(maxFrameSize, false)
}

func (s *msQuicStream) trevrpcReadStreamFrameWithCopy(maxFrameSize int, copyBytes bool) (streamFrameFields, func(), bool, error) {
	ptr := s.cptr()
	if ptr == nil {
		return streamFrameFields{}, nil, false, nil
	}

	var body *C.uint8_t
	var bodyLen C.size_t
	result := C.trevrpc_msquic_stream_read_frame(ptr, &body, &bodyLen, C.size_t(maxFrameSize))
	if result == 0 {
		return streamFrameFields{}, nil, false, nil
	}
	if result < 0 {
		if result == C.TREV_MSQUIC_ERR_FRAME_TOO_LARGE {
			return streamFrameFields{}, nil, false, &FrameTooLargeError{Len: int(bodyLen), Max: maxFrameSize}
		}
		return streamFrameFields{}, nil, false, msQuicError(C.int(result))
	}
	release := func() {}
	if body != nil {
		release = func() { C.trevrpc_msquic_free(unsafe.Pointer(body)) }
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

func (s *msQuicStream) trevrpcWriteFrame(message ProtoMessage, maxFrameSize int) error {
	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = s.Write(frame)
	return err
}

func (s *msQuicStream) trevrpcWriteMessageStreamFrame(body []byte, maxFrameSize int) error {
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

	return msQuicError(C.int(result))
}

func (s *msQuicStream) trevrpcWriteMessageStreamFrames(bodies [][]byte, maxFrameSize int) error {
	if len(bodies) == 0 {
		return nil
	}
	if len(bodies) == 1 {
		return s.trevrpcWriteMessageStreamFrame(bodies[0], maxFrameSize)
	}

	ptr := s.cptr()
	if ptr == nil {
		return io.ErrClosedPipe
	}

	var lengths [maxMessageFrameBatch]C.size_t
	totalBodyLen := 0
	for i, body := range bodies {
		if i >= len(lengths) {
			if err := s.trevrpcWriteMessageStreamFrames(bodies[:i], maxFrameSize); err != nil {
				return err
			}
			return s.trevrpcWriteMessageStreamFrames(bodies[i:], maxFrameSize)
		}

		bodyLen := messageStreamFrameBodyLen(body)
		if bodyLen > maxFrameSize {
			return &FrameTooLargeError{Len: bodyLen, Max: maxFrameSize}
		}
		lengths[i] = C.size_t(len(body))
		totalBodyLen += len(body)
	}

	var stack [256]byte
	packed := stack[:0]
	if totalBodyLen > len(stack) {
		packed = make([]byte, 0, totalBodyLen)
	}
	for _, body := range bodies {
		packed = append(packed, body...)
	}

	var bodyPtr *C.uint8_t
	if len(packed) > 0 {
		bodyPtr = (*C.uint8_t)(unsafe.Pointer(&packed[0]))
	}
	result := C.trevrpc_msquic_stream_write_message_frames(ptr, bodyPtr, &lengths[0], C.size_t(len(bodies)), C.size_t(maxFrameSize))
	if result >= 0 {
		return nil
	}
	if result == C.TREV_MSQUIC_ERR_FRAME_TOO_LARGE {
		return &FrameTooLargeError{Len: maxFrameSize + 1, Max: maxFrameSize}
	}

	return msQuicError(C.int(result))
}

func (s *msQuicStream) Close() error {
	var err error
	s.closeOnce.Do(func() {
		ptr := s.cptr()
		if ptr == nil {
			return
		}
		code := C.trevrpc_msquic_stream_shutdown_send(ptr)
		if code != 0 {
			err = msQuicError(code)
		}
	})
	return err
}

func (s *msQuicStream) abortRead() error {
	ptr := s.cptr()
	if ptr == nil {
		return nil
	}
	code := C.trevrpc_msquic_stream_abort_receive(ptr)
	if code != 0 {
		return msQuicError(code)
	}
	return nil
}

func (s *msQuicStream) destroy() {
	s.destroyOnce.Do(func() {
		ptr := s.takePtr()
		if ptr != nil {
			C.trevrpc_msquic_stream_close(ptr)
		}
	})
}

func (s *msQuicStream) cptr() *C.trevrpc_msquic_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ptr
}

func (s *msQuicStream) takePtr() *C.trevrpc_msquic_stream {
	s.mu.Lock()
	defer s.mu.Unlock()
	ptr := s.ptr
	s.ptr = nil
	return ptr
}

func (s *msQuicStream) trevrpcCancelReadOnContext(ctx context.Context) func() {
	if ctx.Done() == nil {
		return func() {}
	}

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

type msQuicServerListener struct {
	listener *MsQuicListener
	server   *Server
}

func listenMsQuic(addr string, server *Server, options ListenOptions) (ServerListener, error) {
	config := mergeMsQuicConfig(MsQuicConfigFromServerOptions(server.Options()), options.MsQuic)
	listener, err := ListenMsQuic(addr, config)
	if err != nil {
		return nil, err
	}
	return &msQuicServerListener{listener: listener, server: server}, nil
}

func dialMsQuic(ctx context.Context, addr string, options DialOptions, maxFrameSize int) (ClientTransport, error) {
	conn, err := DialMsQuic(ctx, addr, options.MsQuic)
	if err != nil {
		return nil, err
	}
	return NewMsQuicClient(conn).WithMaxFrameSize(maxFrameSize), nil
}

func (l *msQuicServerListener) Addr() net.Addr {
	return l.listener.Addr()
}

func (l *msQuicServerListener) Serve(ctx context.Context) error {
	return ServeMsQuic(ctx, l.listener, l.server)
}

func (l *msQuicServerListener) Close() error {
	return l.listener.Close()
}

func mergeMsQuicConfig(base, override MsQuicConfig) MsQuicConfig {
	if override.CertFile != "" {
		base.CertFile = override.CertFile
	}
	if override.KeyFile != "" {
		base.KeyFile = override.KeyFile
	}
	if override.MaxIdleTimeout > 0 {
		base.MaxIdleTimeout = override.MaxIdleTimeout
	}
	if override.KeepAlive > 0 {
		base.KeepAlive = override.KeepAlive
	}
	if override.PeerBidiStreamCount > 0 {
		base.PeerBidiStreamCount = override.PeerBidiStreamCount
	}
	if override.MaxStatelessOperations > 0 {
		base.MaxStatelessOperations = override.MaxStatelessOperations
	}
	if override.MaxBindingStatelessOperations > 0 {
		base.MaxBindingStatelessOperations = override.MaxBindingStatelessOperations
	}
	return base
}

func withMsQuicConfig(config MsQuicConfig, fn func(*C.trevrpc_msquic_config) error) error {
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
	keepAliveMs, err := msQuicDurationMillis32("keep alive", config.KeepAlive)
	if err != nil {
		return err
	}
	peerBidiStreamCount, err := msQuicUint16("peer bidi stream count", config.PeerBidiStreamCount)
	if err != nil {
		return err
	}
	maxStatelessOperations, err := msQuicUint32("max stateless operations", config.MaxStatelessOperations)
	if err != nil {
		return err
	}
	maxBindingStatelessOperations, err := msQuicUint16("max binding stateless operations", config.MaxBindingStatelessOperations)
	if err != nil {
		return err
	}

	cConfig := C.trevrpc_msquic_config{
		alpn:                             alpn,
		alpn_len:                         C.uint32_t(len(ALPN)),
		cert_file:                        certFile,
		key_file:                         keyFile,
		max_idle_timeout_ms:              C.uint64_t(durationMillis(config.MaxIdleTimeout)),
		keep_alive_ms:                    keepAliveMs,
		peer_bidi_stream_count:           peerBidiStreamCount,
		max_stateless_operations:         maxStatelessOperations,
		max_binding_stateless_operations: maxBindingStatelessOperations,
	}

	return fn(&cConfig)
}

func msQuicUint16(name string, value int) (C.uint16_t, error) {
	if value <= 0 {
		return 0, nil
	}
	if int64(value) > msQuicMaxUint16 {
		return 0, InvalidArgument(fmt.Sprintf("MsQuic %s exceeds %d", name, msQuicMaxUint16))
	}
	return C.uint16_t(value), nil
}

func msQuicUint32(name string, value int) (C.uint32_t, error) {
	if value <= 0 {
		return 0, nil
	}
	if int64(value) > msQuicMaxUint32 {
		return 0, InvalidArgument(fmt.Sprintf("MsQuic %s exceeds %d", name, msQuicMaxUint32))
	}
	return C.uint32_t(value), nil
}

func msQuicDurationMillis32(name string, duration time.Duration) (C.uint32_t, error) {
	millis := durationMillis(duration)
	if millis > uint64(msQuicMaxUint32) {
		return 0, InvalidArgument(fmt.Sprintf("MsQuic %s exceeds %dms", name, msQuicMaxUint32))
	}
	return C.uint32_t(millis), nil
}

func splitMsQuicAddr(addr string) (string, C.uint16_t, error) {
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return "", 0, err
	}
	if port < 0 || port > 65535 {
		return "", 0, InvalidArgument("MsQuic address port is out of range")
	}
	if host == "" {
		host = "0.0.0.0"
	}

	return host, C.uint16_t(port), nil
}

type transportAddr struct {
	network string
	address string
}

func (a transportAddr) Network() string { return a.network }

func (a transportAddr) String() string { return a.address }

func msQuicListenerAddr(host string, port C.uint16_t, listener *C.trevrpc_msquic_listener) net.Addr {
	var boundPort C.uint16_t
	if C.trevrpc_msquic_listener_port(listener, &boundPort) == 0 {
		port = boundPort
	}
	return transportAddr{network: "udp", address: net.JoinHostPort(host, strconv.Itoa(int(port)))}
}

func durationMillis(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration / time.Millisecond)
}

func msQuicError(code C.int) error {
	message := C.GoString(C.trevrpc_msquic_error(code))
	if message == "closed" {
		return Cancelled("MsQuic closed")
	}
	return Unavailable(fmt.Sprintf("MsQuic transport unavailable: %s (%d)", message, int(code)))
}

func msQuicErrorFromErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return Unavailable("MsQuic transport unavailable: " + err.Error())
	}
	return transportStatus(err)
}

func msQuicOrContextStatus(ctx context.Context, code C.int) error {
	if err := ctx.Err(); err != nil {
		return statusFromContextError(err)
	}
	return msQuicError(code)
}

func msQuicOrContextErr(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	return msQuicErrorFromErr(err)
}
