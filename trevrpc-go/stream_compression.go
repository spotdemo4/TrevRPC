package trevrpc

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/proto"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// StreamCompression marks the directions of a streaming method whose messages
// may be compressed with one zstd context per direction of a call
// (ZSTD_STREAM_MESSAGE), so that later messages compress against earlier
// ones. Generated code passes the method's schema option, compress_stream,
// with a direction left out when its message type has a field marked
// debug_redact; hand-written code can pass it to WithStreamCompression
// (clients) and RouteStreamCompression (servers).
//
// Requests applies to client-streaming and bidirectional methods and Responses
// to server-streaming and bidirectional ones; a flag for a direction that does
// not stream has no effect. Both peers must allow a direction, and each call
// negotiates it, within the memory budget of the channel
// (DialOptions.StreamCompressionMemory) and of the server
// (ServerOptions.StreamCompressionMemory). A direction marked Unreliable keeps
// compressing per message, since datagrams cannot carry stream-compressed
// messages. Stream compression calls libzstd and needs cgo; without cgo it is
// off.
//
// A server sends HEADERS at dispatch, before the handler runs, on every call
// to a route whose request messages may be stream-compressed, so handlers of
// such routes cannot set response headers (see SetResponseHeader).
//
// Compressed sizes depend on earlier messages of the call, so do not mark a
// method whose streams carry data an attacker can influence together with
// secrets.
type StreamCompression struct {
	// Requests lets the request messages be stream-compressed.
	Requests bool
	// Responses lets the response messages be stream-compressed.
	Responses bool
}

// effective returns the flags that apply to a call of kind.
func (s StreamCompression) effective(kind RPCKind) StreamCompression {
	return StreamCompression{Requests: s.Requests && kind.streamsRequests(), Responses: s.Responses && kind.streamsResponses()}
}

// any reports whether either direction is marked.
func (s StreamCompression) any() bool { return s.Requests || s.Responses }

// WithStreamCompression lets the call stream-compress the directions s marks;
// see StreamCompression. Generated clients pass the method's schema option.
func WithStreamCompression(s StreamCompression) CallOption {
	return func(config *callConfig) {
		config.streamCompression = s
	}
}

// RouteStreamCompression lets the route's calls stream-compress the
// directions s marks; see StreamCompression. Generated registration passes the
// method's schema option. With Requests, the route's calls send HEADERS at
// dispatch and their handlers cannot set response headers (see
// SetResponseHeader).
func RouteStreamCompression(s StreamCompression) RouteOption {
	return func(config *routeConfig) {
		config.streamCompression = s
	}
}

// Default stream compression memory budgets; see ServerOptions and
// DialOptions.
const (
	defaultServerStreamCompressionMemory = 256 << 20
	defaultClientStreamCompressionMemory = 64 << 20
)

// streamCompressionStart is the message of a direction from which it is
// stream-compressed: the first ones go per message, so short streams never
// hold a context.
const streamCompressionStart = 3

// streamBudget bounds the memory of the stream compression contexts of one
// server or channel. A nil budget allows none.
type streamBudget struct {
	limit int64
	used  atomic.Int64
	// level is the zstd level of the compressing contexts, and encoderCost
	// and decoderCost what libzstd estimates one context of each to take.
	level       int
	encoderCost int64
	decoderCost int64
}

// newStreamBudget returns the budget of memory bytes, fallback when it is
// zero, or nil when it is negative or the binary cannot compress. Contexts
// compress at the level of compression, or 1 without it.
func newStreamBudget(memory, fallback int64, compression *compressionConfig) *streamBudget {
	if memory < 0 || !zstd.Available {
		return nil
	}
	if memory == 0 {
		memory = fallback
	}
	level := 1
	if compression != nil {
		level = compression.level
	}
	return &streamBudget{
		limit:       memory,
		level:       level,
		encoderCost: zstd.StreamEncoderCost(level, zstd.StreamWindowLog),
		decoderCost: zstd.StreamDecoderCost(zstd.StreamWindowLog),
	}
}

// reserve takes n bytes from the budget, or returns nil when they do not fit.
func (b *streamBudget) reserve(n int64) *streamReservation {
	if b == nil {
		return nil
	}
	for {
		used := b.used.Load()
		if used+n > b.limit {
			return nil
		}
		if b.used.CompareAndSwap(used, used+n) {
			return &streamReservation{budget: b, n: n}
		}
	}
}

// streamReservation is memory taken from a streamBudget. It is released at
// most once, however many paths try.
type streamReservation struct {
	budget *streamBudget
	n      int64
	once   sync.Once
}

// release returns the reservation; it is safe on nil.
func (r *streamReservation) release() {
	if r != nil {
		r.once.Do(func() { r.budget.used.Add(-r.n) })
	}
}

// streamOffer is what a peer sends to enable a direction it decodes.
func streamOffer() *streamCompressionInfo {
	return &streamCompressionInfo{MaxWindowLog: zstd.StreamWindowLog}
}

// senderWindowLog returns the window a sender uses for a peer that sent info,
// or zero if info enables nothing.
func senderWindowLog(info *streamCompressionInfo) int {
	if info == nil || info.MaxWindowLog < zstd.MinStreamWindowLog {
		return 0
	}
	return int(min(info.MaxWindowLog, zstd.StreamWindowLog))
}

// streamCompressor encodes the messages of one direction of a call. The first
// messages, and every message when the direction is not enabled or the budget
// is spent, go through per, the per-message compressor (nil for none). From
// streamCompressionStart on, once the direction is enabled, they go through
// one zstd context as ZSTD_STREAM_MESSAGE frames, for the rest of the call.
// One goroutine encodes; close may come from another.
type streamCompressor struct {
	budget *streamBudget
	per    *messageCompressor
	// windowLog is the peer's window once the direction is enabled, zero
	// until then. A client learns it from HEADERS, on another goroutine.
	windowLog atomic.Int32

	mu          sync.Mutex
	sent        int
	encoder     *zstd.StreamEncoder
	reservation *streamReservation
	// stopped is set once the direction stays per message: the budget was
	// spent, the encoder failed, or the call ended.
	stopped bool
}

func newStreamCompressor(budget *streamBudget, per *messageCompressor) *streamCompressor {
	return &streamCompressor{budget: budget, per: per}
}

// enable lets the direction be stream-compressed with a window of
// 2^windowLog bytes.
func (c *streamCompressor) enable(windowLog int) {
	c.windowLog.Store(int32(windowLog))
}

// encode marshals message into its frame.
func (c *streamCompressor) encode(message ProtoMessage) (encodedMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent++
	if c.encoder == nil && !c.stopped && c.sent >= streamCompressionStart {
		if windowLog := int(c.windowLog.Load()); windowLog > 0 {
			c.start(protoMessageV2(message), windowLog)
		}
	}
	if c.encoder == nil {
		return c.per.encode(message)
	}
	return c.encodeStream(message)
}

// start creates the direction's context, starting from the dictionary the
// per-message compressor would use for message, if the budget allows it.
func (c *streamCompressor) start(message proto.Message, windowLog int) {
	reservation := c.budget.reserve(c.budget.encoderCost)
	if reservation == nil {
		c.stopped = true
		return
	}
	var dict *zstd.Dict
	if d := c.per.dictFor(message); d != nil {
		dict = d.Dict
	}
	encoder, err := zstd.NewStreamEncoder(dict, c.budget.level, windowLog)
	if err != nil {
		reservation.release()
		c.stopped = true
		return
	}
	c.encoder, c.reservation = encoder, reservation
}

// streamFrameHeaderRoom is the room left before a chunk for the frame header
// and the message size varint.
const streamFrameHeaderRoom = compressedFrameHeaderRoom + 8

// encodeStream marshals message and compresses it with the context into a
// ZSTD_STREAM_MESSAGE frame.
func (c *streamCompressor) encodeStream(message ProtoMessage) (encodedMessage, error) {
	scratch := marshalScratch.Get().(*[]byte)
	defer func() {
		if cap(*scratch) <= maxRetainedMarshalScratch {
			marshalScratch.Put(scratch)
		}
	}()
	payload, err := proto.MarshalOptions{}.MarshalAppend((*scratch)[:0], protoMessageV2(message))
	if err != nil {
		return encodedMessage{}, err
	}
	*scratch = payload[:0]
	buffer := make([]byte, streamFrameHeaderRoom, streamFrameHeaderRoom+zstd.CompressBound(len(payload))+32)
	compressed, err := c.encoder.Encode(buffer, payload)
	if err != nil {
		// The context is unusable, and the peer's is now ahead of it.
		return encodedMessage{}, Internal("stream compression failed: " + err.Error())
	}
	chunk := len(compressed) - streamFrameHeaderRoom
	sizeLen := quicvarint.Len(uint64(len(payload)))
	length := sizeLen + chunk
	start := streamFrameHeaderRoom - sizeLen - frameHeaderLen(frameTypeZstdStreamMessage, length)
	frame := appendFrameHeader(compressed[start:start], frameTypeZstdStreamMessage, length)
	quicvarint.Append(frame, uint64(len(payload)))
	return encodedMessage{frame: compressed[start:], size: len(payload)}, nil
}

// close ends the direction: its context is freed and its memory returned.
func (c *streamCompressor) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	if c.encoder != nil {
		c.encoder.Close()
		c.encoder = nil
	}
	c.reservation.release()
}

// errUnenabledStreamCompression reports a ZSTD_STREAM_MESSAGE frame on a
// direction that did not enable stream compression.
var errUnenabledStreamCompression = errors.New("stream-compressed message where stream compression is not enabled")

// streamInflater decompresses the ZSTD_STREAM_MESSAGE frames of one direction
// of a call, in order. Its context is created by the first one. One goroutine
// reads; close may come from another.
type streamInflater struct {
	// dicts holds the dictionaries a frame may start from, nil for none.
	dicts dictionaryLookup
	// reservation is the memory the receiver set aside when it enabled the
	// direction.
	reservation *streamReservation
	// received counts the messages decompressed.
	received atomic.Int64

	mu           sync.Mutex
	decoder      *zstd.StreamDecoder
	inFrame      bool
	closed       bool
	failed       bool
	maxWindowLog int
}

func newStreamInflater(dicts dictionaryLookup, reservation *streamReservation) *streamInflater {
	return &streamInflater{dicts: dicts, reservation: reservation, maxWindowLog: zstd.StreamWindowLog}
}

// inflate decompresses the payload of a ZSTD_STREAM_MESSAGE frame. The message
// size is checked against maxSize, and passed to charge, before the chunk is
// decompressed; a size above maxSize is a FrameTooLargeError. Every other
// fault is a frameDecodeError, after which the direction cannot continue.
func (s *streamInflater) inflate(payload []byte, maxSize int, charge func(int) error) ([]byte, error) {
	size, n, err := quicvarint.Parse(payload)
	if err != nil {
		return nil, &frameDecodeError{Err: zstd.ErrInvalid}
	}
	if size > uint64(maxSize) {
		return nil, &FrameTooLargeError{Len: int(min(size, math.MaxInt)), Max: maxSize}
	}
	if err := charge(int(size)); err != nil {
		return nil, err
	}
	chunk := payload[n:]
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed {
		return nil, &frameDecodeError{Err: errUnenabledStreamCompression}
	}
	message, err := s.decode(chunk, int(size))
	if err != nil {
		s.failed = true
		return nil, &frameDecodeError{Err: err}
	}
	s.received.Add(1)
	return message, nil
}

func (s *streamInflater) decode(chunk []byte, size int) ([]byte, error) {
	info, err := zstd.ParseStreamChunk(chunk, !s.inFrame, s.maxWindowLog)
	if err != nil {
		return nil, err
	}
	var dict *zstd.Dict
	if info.StartsFrame && info.DictID != 0 {
		if s.dicts != nil {
			dict = s.dicts.lookup(info.DictID)
		}
		if dict == nil {
			return nil, fmt.Errorf("stream-compressed message uses dictionary %d, which is not accepted", info.DictID)
		}
	}
	if s.decoder == nil {
		if s.decoder, err = zstd.NewStreamDecoder(s.maxWindowLog); err != nil {
			return nil, err
		}
	}
	message, err := s.decoder.Decode(nil, chunk, info, dict, size)
	if err != nil {
		return nil, err
	}
	if len(chunk) > 0 {
		s.inFrame = !info.EndsFrame
	}
	return message, nil
}

// close ends the direction: its context is freed and its memory returned. It
// is safe on nil.
func (s *streamInflater) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.decoder != nil {
		s.decoder.Close()
		s.decoder = nil
	}
	s.reservation.release()
}

// serverStreams is the server's stream compression of one call.
type serverStreams struct {
	// answer goes out in HEADERS when the server decodes stream-compressed
	// requests, and requests then decompresses them; both are nil otherwise.
	answer   *streamCompressionInfo
	requests *streamInflater
	// responses encodes the response messages when they may be
	// stream-compressed, and is nil otherwise.
	responses *streamCompressor
}

// streamCompressesRequests reports whether the route's request messages may
// be stream-compressed. A direction marked unreliable is left out: its
// messages may be datagrams, which cannot carry stream-compressed messages.
// The calls of such a route send HEADERS at dispatch, so a client learns the
// answer while it is still uploading.
func (rt route) streamCompressesRequests() bool {
	return rt.streamCompression.Requests && !rt.unreliable.Requests
}

// negotiateStreams answers the request's stream_compression, if it has one,
// for the directions its route allows. It returns nil when nothing is
// enabled. A direction marked unreliable is left out: its messages may be
// datagrams, which cannot carry stream-compressed messages.
func (r *serverRuntime) negotiateStreams(request *wireRequest) *serverStreams {
	if request.StreamCompression == nil || r.streams == nil {
		return nil
	}
	route, ok := r.routes[request.Method]
	if !ok || route.kind != request.rpcKind() {
		return nil
	}
	streams := &serverStreams{}
	if route.streamCompressesRequests() {
		if reservation := r.streams.reserve(r.streams.decoderCost); reservation != nil {
			streams.answer = streamOffer()
			streams.requests = newStreamInflater(r.compression.lookupOrNil(), reservation)
		}
	}
	if windowLog := senderWindowLog(request.StreamCompression); windowLog > 0 && route.streamCompression.Responses && !route.unreliable.Responses {
		var per *messageCompressor
		if request.compression != nil {
			per = request.compression.responses
		}
		streams.responses = newStreamCompressor(r.streams, per)
		streams.responses.enable(windowLog)
	}
	if streams.answer == nil && streams.responses == nil {
		return nil
	}
	return streams
}

// requestInflater returns the decompressor of the request messages; nil-safe.
func (s *serverStreams) requestInflater() *streamInflater {
	if s == nil {
		return nil
	}
	return s.requests
}

// responseCompressor returns the encoder of the response messages; nil-safe.
func (s *serverStreams) responseCompressor() *streamCompressor {
	if s == nil {
		return nil
	}
	return s.responses
}

// close ends the call's stream compression; it is safe on nil.
func (s *serverStreams) close() {
	if s != nil {
		s.requests.close()
		s.responses.close()
	}
}

// clientStreams is a client's stream compression of one call.
type clientStreams struct {
	// requests encodes the request messages, nil when they are never
	// stream-compressed; it is enabled by the server's answer.
	requests *streamCompressor
	// responses decompresses the response messages, nil when the client did
	// not enable them.
	responses *streamInflater
}

// prepareStreams offers stream compression on request for the directions the
// call may stream-compress, within the channel's budget. accept holds the
// dictionaries the call accepts on its responses. It returns nil when it
// offers nothing. A direction marked unreliable is left out, as on servers.
func (c *streamClient) prepareStreams(request *wireRequest, accept *dictSet) *clientStreams {
	allowed := request.streamCompression
	if c.streams == nil || !allowed.any() {
		return nil
	}
	unreliable := request.unreliable.effective(request.rpcKind())
	streams := &clientStreams{}
	if allowed.Responses && !unreliable.Responses {
		// The server may start sending as soon as it reads the offer, so the
		// memory is set aside now; without it the call offers nothing.
		reservation := c.streams.reserve(c.streams.decoderCost)
		if reservation == nil {
			return nil
		}
		streams.responses = newStreamInflater(accept.lookupOrNil(), reservation)
	}
	if allowed.Requests && !unreliable.Requests {
		streams.requests = newStreamCompressor(c.streams, c.compression.requestCompressor())
	}
	if streams.requests == nil && streams.responses == nil {
		return nil
	}
	request.StreamCompression = streamOffer()
	return streams
}

// learn enables the request direction when the server's HEADERS answer the
// offer; it is safe on nil.
func (s *clientStreams) learn(answer *streamCompressionInfo) {
	if s == nil || s.requests == nil {
		return
	}
	if windowLog := senderWindowLog(answer); windowLog > 0 {
		s.requests.enable(windowLog)
	}
}

// requestCompressor returns the encoder of the request messages; nil-safe.
func (s *clientStreams) requestCompressor() *streamCompressor {
	if s == nil {
		return nil
	}
	return s.requests
}

// responseInflater returns the decompressor of the response messages;
// nil-safe.
func (s *clientStreams) responseInflater() *streamInflater {
	if s == nil {
		return nil
	}
	return s.responses
}

// close ends the call's stream compression; it is safe on nil.
func (s *clientStreams) close() {
	if s != nil {
		s.requests.close()
		s.responses.close()
	}
}
