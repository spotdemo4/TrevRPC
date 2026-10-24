package trevrpc

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
)

// MessageStream yields messages until Recv returns io.EOF.
type MessageStream[T any] interface {
	// Recv returns the next message or io.EOF when the stream is complete.
	Recv() (T, error)
	// Close releases stream resources and cancels any pending work. It may be
	// called concurrently with Recv, including to unblock a pending Recv, so
	// implementations must be safe for that; the server closes a handler's
	// response stream that way when the call is cancelled or times out.
	Close() error
}

// Messages returns a single-use iterator over stream. It closes stream when
// iteration ends, including when the loop exits early. Close errors are not
// yielded; use Recv and Close directly when close errors must be observed.
func Messages[T any](stream MessageStream[T]) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		defer func() { _ = stream.Close() }()
		for {
			message, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				return
			}
			if !yield(message, err) || err != nil {
				return
			}
		}
	}
}

// byteStream is a stream of received message payloads.
type byteStream = MessageStream[[]byte]

// encodedStream is a stream of messages ready to send.
type encodedStream = MessageStream[encodedMessage]

// responseFrame is one known frame received on a response stream.
type responseFrame struct {
	kind frameType
	// body is the payload of a MESSAGE frame.
	body []byte
	// headers is the metadata of a HEADERS frame.
	headers Metadata
	// status is the content of a STATUS frame.
	status *Status
}

// frameStream is a stream of the known frames of one response.
type frameStream = MessageStream[responseFrame]

type nonBlockingStream interface {
	trevrpcNonBlockingStream() bool
}

type contextCancelsRecvStream interface {
	trevrpcContextCancelsRecv() bool
}

type contextCancelReadStream interface {
	trevrpcCancelReadOnContext(context.Context) func()
}

// messagePipe is a sendable message stream.
type messagePipe[T any] struct {
	ctx       context.Context
	messages  chan T
	done      chan struct{}
	closeOnce sync.Once
	errMu     sync.Mutex
	err       error
	// uploadErr is why the receiving side ended the upload, if it did.
	uploadErr error
}

// newMessagePipe returns a stream that yields messages sent with Send until closed.
func newMessagePipe[T any](ctx context.Context) *messagePipe[T] {
	if ctx == nil {
		ctx = context.Background()
	}

	return &messagePipe[T]{ctx: ctx, messages: make(chan T), done: make(chan struct{})}
}

// newBufferedMessagePipe is newMessagePipe with room for buffer messages, so
// that a sender can run ahead of the goroutine that writes them and the writer
// finds several messages ready at once. Messages in the buffer when the pipe
// is closed without an error are still yielded.
func newBufferedMessagePipe[T any](ctx context.Context, buffer int) *messagePipe[T] {
	pipe := newMessagePipe[T](ctx)
	pipe.messages = make(chan T, buffer)
	return pipe
}

// readyReceiver is implemented by the streams that can hand out a message that
// is already there without waiting.
type readyReceiver[T any] interface {
	// recvReady returns the next message if one is ready.
	recvReady() (T, bool, error)
}

func (s *messagePipe[T]) recvReady() (T, bool, error) {
	select {
	case message := <-s.messages:
		return message, true, nil
	default:
		var zero T
		return zero, false, nil
	}
}

// Send adds one message to the stream.
func (s *messagePipe[T]) Send(message T) error {
	if err := s.uploadError(); err != nil {
		return err
	}
	select {
	case <-s.done:
		return s.closedError()
	default:
	}

	select {
	case s.messages <- message:
		return nil
	case <-s.done:
		return s.closedError()
	case <-s.ctx.Done():
		return statusFromContextError(s.ctx.Err())
	}
}

func (s *messagePipe[T]) Recv() (T, error) {
	select {
	case message := <-s.messages:
		return message, nil
	case <-s.done:
		var zero T
		if err := s.closeError(); err != nil {
			return zero, err
		}
		select {
		case message := <-s.messages:
			return message, nil
		default:
		}
		return zero, io.EOF
	case <-s.ctx.Done():
		var zero T
		return zero, statusFromContextError(s.ctx.Err())
	}
}

// Close completes the stream successfully.
func (s *messagePipe[T]) Close() error {
	return s.CloseWithError(nil)
}

// CloseWithError completes the stream with err.
func (s *messagePipe[T]) CloseWithError(err error) error {
	s.closeOnce.Do(func() {
		s.errMu.Lock()
		s.err = err
		s.errMu.Unlock()
		close(s.done)
	})
	return nil
}

func (s *messagePipe[T]) closeError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.err
}

// abortUpload records why the consumer ended the upload. Send reports it from
// then on, in place of a closed pipe. The first reason wins.
func (s *messagePipe[T]) abortUpload(err error) {
	s.errMu.Lock()
	if s.uploadErr == nil {
		s.uploadErr = err
	}
	s.errMu.Unlock()
}

func (s *messagePipe[T]) uploadError() error {
	s.errMu.Lock()
	defer s.errMu.Unlock()
	return s.uploadErr
}

func (s *messagePipe[T]) closedError() error {
	if err := s.uploadError(); err != nil {
		return err
	}
	if err := s.closeError(); err != nil {
		return err
	}
	return io.ErrClosedPipe
}

type sliceStream[T any] struct {
	mu    sync.Mutex
	items []T
	next  int
}

// newSliceStream returns a stream that yields the provided items in order.
func newSliceStream[T any](items ...T) MessageStream[T] {
	return &sliceStream[T]{items: items}
}

func (s *sliceStream[T]) trevrpcNonBlockingStream() bool { return true }

func (s *sliceStream[T]) Recv() (T, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.next >= len(s.items) {
		var zero T
		return zero, io.EOF
	}

	item := s.items[s.next]
	s.next++
	return item, nil
}

func (s *sliceStream[T]) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next = len(s.items)
	return nil
}

type encodeStream[T ProtoMessage] struct {
	inner MessageStream[T]
	// compressor compresses the messages, or is nil.
	compressor *messageCompressor
	// stream, when set, stream-compresses the messages and falls back to its
	// own per-message compressor; see streamCompressor.
	stream *streamCompressor
	// upload, when set, encodes the messages of an optimistic call instead;
	// see optimisticUpload.
	upload *optimisticUpload
}

// newEncodeStream wraps a protobuf message stream and yields messages encoded
// as MESSAGE frames.
func newEncodeStream[T ProtoMessage](inner MessageStream[T]) encodedStream {
	return &encodeStream[T]{inner: inner}
}

// newCompressedEncodeStream is newEncodeStream whose messages compressor may
// compress; a nil compressor compresses none.
func newCompressedEncodeStream[T ProtoMessage](inner MessageStream[T], compressor *messageCompressor) encodedStream {
	return &encodeStream[T]{inner: inner, compressor: compressor}
}

// newResponseEncodeStream is newEncodeStream for the streamed response
// messages of the handler whose context is ctx, compressed as the call
// negotiated.
func newResponseEncodeStream[T ProtoMessage](ctx contextValuer, inner MessageStream[T]) encodedStream {
	return &encodeStream[T]{inner: inner, compressor: responseCompressorFromContext(ctx), stream: responseStreamsFromContext(ctx).responseCompressor()}
}

// setCompressors implements compressorSetter.
func (s *encodeStream[T]) setCompressors(compressor *messageCompressor, stream *streamCompressor) {
	s.compressor, s.stream = compressor, stream
}

// setUpload implements compressorSetter.
func (s *encodeStream[T]) setUpload(upload *optimisticUpload) {
	s.upload = upload
}

// compressorSetter is implemented by the encoded streams of client requests,
// whose compressors the connection that carries the call provides: the
// per-message one, and the call's stream compressor, either of which may be
// nil, or the optimistic upload that encodes with them.
type compressorSetter interface {
	setCompressors(*messageCompressor, *streamCompressor)
	setUpload(*optimisticUpload)
}

// encode encodes message as the stream's compressors decide.
func (s *encodeStream[T]) encode(message T) (encodedMessage, error) {
	if s.upload != nil {
		return s.upload.encode(message)
	}
	if s.stream != nil {
		return s.stream.encode(message)
	}
	return s.compressor.encode(message)
}

func (s *encodeStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *encodeStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

func (s *encodeStream[T]) Recv() (encodedMessage, error) {
	message, err := s.inner.Recv()
	if err != nil {
		return encodedMessage{}, err
	}

	return s.encode(message)
}

func (s *encodeStream[T]) recvReady() (encodedMessage, bool, error) {
	ready, ok := s.inner.(readyReceiver[T])
	if !ok {
		return encodedMessage{}, false, nil
	}
	message, ok, err := ready.recvReady()
	if !ok || err != nil {
		return encodedMessage{}, false, err
	}
	encoded, err := s.encode(message)
	return encoded, err == nil, err
}

func (s *encodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
}

// abortUpload passes the reason the upload ended to the wrapped stream.
func (s *encodeStream[T]) abortUpload(err error) {
	if aborter, ok := s.inner.(uploadAborter); ok {
		aborter.abortUpload(err)
	}
}

type decodeStream[T ProtoMessage] struct {
	inner      byteStream
	newMessage func() T
}

// newDecodeStream wraps a byte stream and yields decoded protobuf messages.
func newDecodeStream[T ProtoMessage](inner byteStream, newMessage func() T) MessageStream[T] {
	return &decodeStream[T]{inner: inner, newMessage: newMessage}
}

func (s *decodeStream[T]) Recv() (T, error) {
	body, err := s.inner.Recv()
	if err != nil {
		var zero T
		return zero, err
	}

	message := s.newMessage()
	if err := unmarshalMessage(body, message); err != nil {
		var zero T
		return zero, InvalidArgument("failed to decode message: " + err.Error())
	}

	return message, nil
}

func (s *decodeStream[T]) Close() error {
	return closeMessageStream(s.inner)
}

func (s *decodeStream[T]) trevrpcNonBlockingStream() bool {
	return isNonBlockingStream(s.inner)
}

func (s *decodeStream[T]) trevrpcContextCancelsRecv() bool {
	return streamContextCancelsRecv(s.inner)
}

type contextCloseStream[T any] struct {
	inner     MessageStream[T]
	done      chan struct{}
	closeOnce sync.Once
}

func closeStreamOnContext[T any](ctx context.Context, inner MessageStream[T]) MessageStream[T] {
	if inner == nil || isNonBlockingStream(inner) || streamContextCancelsRecv(inner) {
		return inner
	}

	stream := &contextCloseStream[T]{inner: inner, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			_ = stream.Close()
		case <-stream.done:
		}
	}()

	return stream
}

func (s *contextCloseStream[T]) trevrpcContextCancelsRecv() bool { return true }

func (s *contextCloseStream[T]) Recv() (T, error) {
	return s.inner.Recv()
}

func (s *contextCloseStream[T]) recvReady() (T, bool, error) {
	if ready, ok := s.inner.(readyReceiver[T]); ok {
		return ready.recvReady()
	}
	var zero T
	return zero, false, nil
}

func (s *contextCloseStream[T]) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		err = closeMessageStream(s.inner)
	})
	return err
}

func isNonBlockingStream(stream any) bool {
	if stream == nil {
		return false
	}
	nonBlocking, ok := stream.(nonBlockingStream)
	return ok && nonBlocking.trevrpcNonBlockingStream()
}

func streamContextCancelsRecv(stream any) bool {
	if stream == nil {
		return false
	}
	cancellable, ok := stream.(contextCancelsRecvStream)
	return ok && cancellable.trevrpcContextCancelsRecv()
}

// discardMessageStream closes stream where the close error cannot be acted on,
// such as when the call has already failed or the peer is gone.
func discardMessageStream(stream any) {
	_ = closeMessageStream(stream)
}

func closeMessageStream(stream any) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = Internal("stream close panicked")
		}
	}()

	if closer, ok := stream.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}
