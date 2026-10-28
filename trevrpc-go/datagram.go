package trevrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// This file is the datagram half of the runtime (see "Datagram Messages" in
// the protocol specification). A datagram message is the payload of one QUIC
// DATAGRAM frame or WebTransport datagram: the call's flow ID as a QUIC varint
// followed by the raw message.
//
// One datagramMux serves a native QUIC connection or a WebTransport session.
// It reads the connection's datagrams on one goroutine and hands each to the
// sink of its flow, sends datagrams on one goroutine, allocates the flows of a
// client and, on a server, holds the datagrams that overtake their REQUEST.

// Unreliable marks the directions of a streaming method whose messages may
// travel as datagrams: they can be lost or reordered, but are never duplicated
// or corrupted. Generated code passes the method's schema option, and
// hand-written code can pass it to WithUnreliable (clients) and RouteUnreliable
// (servers).
//
// Requests applies to client-streaming and bidirectional methods and Responses
// to server-streaming and bidirectional ones; a flag for a direction that does
// not stream has no effect. Datagrams are used only on native QUIC and
// WebTransport; on every other transport the messages stay on the stream.
type Unreliable struct {
	// Requests sends the request messages as datagrams. Servers ignore it: they
	// always accept request datagrams on a call that has a flow.
	Requests bool
	// Responses sends the response messages as datagrams. On a client it only
	// opens the call's flow, which lets the server send datagrams; clients
	// accept response datagrams on every call that has a flow.
	Responses bool
}

// effective returns the flags that apply to a call of kind.
func (u Unreliable) effective(kind RPCKind) Unreliable {
	return Unreliable{Requests: u.Requests && kind.streamsRequests(), Responses: u.Responses && kind.streamsResponses()}
}

// any reports whether either direction is marked.
func (u Unreliable) any() bool { return u.Requests || u.Responses }

// streamsRequests reports whether the request of k is a stream.
func (k RPCKind) streamsRequests() bool {
	return k == RPCKindClientStreaming || k == RPCKindBidirectionalStreaming
}

// streamsResponses reports whether the response of k is a stream.
func (k RPCKind) streamsResponses() bool {
	return k == RPCKindServerStreaming || k == RPCKindBidirectionalStreaming
}

// WithUnreliable lets the messages of the directions u marks travel as
// datagrams when the connection supports them. The call then also accepts the
// server's datagram messages whichever directions are marked. Generated
// clients pass the method's schema option; the option applies only to
// streaming directions.
func WithUnreliable(u Unreliable) CallOption {
	return func(config *callConfig) {
		config.unreliable = u
	}
}

// routeConfig holds the resolved options of one registered route.
type routeConfig struct {
	unreliable        Unreliable
	streamCompression StreamCompression
	plainCompression  PlainCompression
}

// RouteOption customizes a route registered with Register*.
type RouteOption func(*routeConfig)

// RouteUnreliable lets the handler's response messages travel as datagrams on
// calls that have a datagram flow, if u.Responses is set. Servers accept
// request datagrams on every call that has a flow, so u.Requests has no
// effect. Generated registration passes the method's schema option.
func RouteUnreliable(u Unreliable) RouteOption {
	return func(config *routeConfig) {
		config.unreliable = u
	}
}

func applyRouteOptions(options []RouteOption) routeConfig {
	var config routeConfig
	for _, option := range options {
		option(&config)
	}
	return config
}

// RequestDatagrams returns how many of the request messages of this call have
// arrived as datagrams so far, which is zero on transports without datagrams
// and on calls without a flow. It is meant for observing datagram delivery.
func (c HandlerContext) RequestDatagrams() int64 {
	if c.flow == nil {
		return 0
	}
	return c.flow.received.Load()
}

// appendDatagram appends the datagram payload that carries messages on flow:
// the flow ID as a minimal QUIC varint, then one MESSAGE frame per message.
func appendDatagram(dst []byte, flow uint64, messages [][]byte) []byte {
	dst = quicvarint.Append(dst, flow)
	for _, message := range messages {
		dst = appendFrameHeader(dst, frameTypeMessage, len(message))
		dst = append(dst, message...)
	}
	return dst
}

// parseDatagram splits a datagram payload into its flow and its frames, which
// alias datagram. It reports false for a payload that does not start with a
// varint, and for flow 0, which no call has.
func parseDatagram(datagram []byte) (flow uint64, frames []byte, ok bool) {
	flow, n, err := quicvarint.Parse(datagram)
	if err != nil || flow == 0 {
		return 0, nil, false
	}
	return flow, datagram[n:], true
}

// walkDatagramFrames calls fn, if there is one, with the type and payload of
// every MESSAGE and ZSTD_MESSAGE frame of frames in order, and skips the
// frames of every other type, known or not. Type and length varints need not
// be minimal. It reports false when the frames do not exactly fill their
// space, a truncated frame or a length past the end, or when they hold a
// ZSTD_STREAM_MESSAGE, which needs the order of a stream; the datagram is
// then dropped whole. fn may have been called for the frames before the
// fault, so callers that must not deliver any message first walk with a nil
// fn.
func walkDatagramFrames(frames []byte, fn func(typ frameType, payload []byte)) bool {
	for len(frames) > 0 {
		typ, n, err := quicvarint.Parse(frames)
		if err != nil {
			return false
		}
		frames = frames[n:]
		length, n, err := quicvarint.Parse(frames)
		if err != nil {
			return false
		}
		frames = frames[n:]
		if length > uint64(len(frames)) || frameType(typ) == frameTypeZstdStreamMessage {
			return false
		}
		if t := frameType(typ); (t == frameTypeMessage || t == frameTypeZstdMessage) && fn != nil {
			fn(t, frames[:length])
		}
		frames = frames[length:]
	}
	return true
}

// datagramMessages returns the payloads of a datagram's MESSAGE frames, or
// false if the datagram is dropped whole.
func datagramMessages(frames []byte) ([][]byte, bool) {
	if !walkDatagramFrames(frames, nil) {
		return nil, false
	}
	var messages [][]byte
	walkDatagramFrames(frames, func(typ frameType, message []byte) {
		if typ == frameTypeMessage {
			messages = append(messages, message)
		}
	})
	return messages, true
}

// datagramConn is the datagram surface of a QUIC connection or WebTransport
// session; *quic.Conn and *webtransport.Session both satisfy it.
type datagramConn interface {
	SendDatagram([]byte) error
	ReceiveDatagram(context.Context) ([]byte, error)
}

// Limits of the datagram path. Servers hold the datagrams of flows they have
// not seen yet, because QUIC packs DATAGRAM frames ahead of stream data and
// they usually overtake the REQUEST. The Rust runtime uses the same values.
const (
	// datagramSinkMessages and datagramSinkBytes bound the received messages
	// a call queues. Datagrams bypass flow control, so the queue is four times
	// the stream read-ahead's message count to absorb bursts.
	datagramSinkMessages = 128
	datagramSinkBytes    = 64 << 10
	// datagramHoldMessages and datagramHoldBytes bound the datagrams a
	// connection holds for flows it has not seen, and datagramHoldTime how
	// long each is held.
	datagramHoldMessages = 64
	datagramHoldBytes    = 64 << 10
	datagramHoldTime     = 500 * time.Millisecond
	// datagramSizeProbe is how long a datagram size limit learned from a send
	// error is trusted. The limit grows with path MTU discovery.
	datagramSizeProbe = time.Second
	// datagramSendQueue is the queue of the sender goroutine.
	datagramSendQueue = 32
)

// datagramMux demultiplexes the datagrams of one connection.
type datagramMux struct {
	conn datagramConn
	// overhead is what the transport adds to every datagram, the WebTransport
	// quarter stream ID; it counts against the datagram size limit.
	overhead int
	// server says that datagrams of unknown flows are held for their REQUEST.
	server bool
	ctx    context.Context
	cancel context.CancelFunc

	// hold limits, variables of the mux so tests can shrink them.
	holdMessages, holdBytes int
	holdTime                time.Duration

	mu    sync.Mutex
	flows map[uint64]*datagramFlow
	ended flowSet
	held  []heldDatagram
	// heldBytes is the size of the held datagrams. holdTimer expires them, and
	// is nil when none are held.
	heldBytes int
	holdTimer *time.Timer

	// nextFlow is the last flow ID a client allocated.
	nextFlow atomic.Uint64

	// rx is held for reading by the reader while it waits for a datagram and
	// dispatches it, and for writing by flush. waitCtx is the context of the
	// reader's current wait, which flush cancels (under waitMu), and flushMu
	// serializes the flushes.
	rx         sync.RWMutex
	flushMu    sync.Mutex
	waitMu     sync.Mutex
	waitCtx    context.Context
	waitCancel context.CancelFunc

	readOnce sync.Once
	sendOnce sync.Once
	out      chan datagramSend
	// sizeLimit is the largest datagram a send error reported, valid while
	// sizeLimitAt (Unix nanoseconds, zero for none) is recent.
	sizeLimit   atomic.Int64
	sizeLimitAt atomic.Int64
}

// heldDatagram is a datagram that arrived for a flow nobody registered yet.
type heldDatagram struct {
	flow     uint64
	frames   []byte
	size     int
	deadline time.Time
}

// newDatagramMux returns the demultiplexer of conn. A client passes
// server == false. endedCapacity is how many recently ended flows a server
// remembers.
func newDatagramMux(conn datagramConn, overhead int, server bool, endedCapacity int) *datagramMux {
	ctx, cancel := context.WithCancel(context.Background())
	waitCtx, waitCancel := context.WithCancel(ctx)
	return &datagramMux{
		waitCtx:      waitCtx,
		waitCancel:   waitCancel,
		conn:         conn,
		overhead:     overhead,
		server:       server,
		ctx:          ctx,
		cancel:       cancel,
		holdMessages: datagramHoldMessages,
		holdBytes:    datagramHoldBytes,
		holdTime:     datagramHoldTime,
		flows:        make(map[uint64]*datagramFlow),
		ended:        flowSet{capacity: endedCapacity},
	}
}

// close stops the goroutines of the mux. It is safe to call repeatedly, and on
// a nil mux.
func (m *datagramMux) close() {
	if m == nil {
		return
	}
	m.cancel()
	m.mu.Lock()
	m.held, m.heldBytes = nil, 0
	if m.holdTimer != nil {
		m.holdTimer.Stop()
		m.holdTimer = nil
	}
	m.mu.Unlock()
}

// datagramFlow is one call's registration on a mux. sink receives the
// call's datagram messages and is nil for a call whose receiving direction is
// not a stream.
type datagramFlow struct {
	mux  *datagramMux
	id   uint64
	sink *datagramSink
	// reply carries the result of the call's datagram sends, one at a time.
	reply chan error
	// received counts the messages the call took from sink.
	received    atomic.Int64
	releaseOnce sync.Once
}

// errDatagramFlowInUse is why a flow could not be registered.
func errDatagramFlowInUse(id uint64) error {
	return InvalidArgument(fmt.Sprintf("datagram flow %d is already in use", id))
}

// allocate registers the next flow ID of a client. receives says that the call
// receives a stream of messages on it, with messages up to maxMessage bytes.
// It returns nil when the connection is gone.
func (m *datagramMux) allocate(receives bool, maxMessage int, inflate dictionaryLookup) *datagramFlow {
	if m.ctx.Err() != nil {
		return nil
	}
	flow, err := m.register(m.nextFlow.Add(1), receives, maxMessage, inflate)
	if err != nil {
		return nil
	}
	return flow
}

// register makes id the flow of a call. It fails if id is live on the
// connection. Held datagrams of the flow are claimed. inflate holds the
// dictionaries the call accepts on compressed messages, nil for none.
func (m *datagramMux) register(id uint64, receives bool, maxMessage int, inflate dictionaryLookup) (*datagramFlow, error) {
	flow := &datagramFlow{mux: m, id: id, reply: make(chan error, 1)}
	if receives {
		flow.sink = newDatagramSink(maxMessage)
		flow.sink.inflate = inflate
	}
	m.mu.Lock()
	if _, live := m.flows[id]; live {
		m.mu.Unlock()
		return nil, errDatagramFlowInUse(id)
	}
	m.flows[id] = flow
	m.claimHeldLocked(flow)
	m.mu.Unlock()
	m.readOnce.Do(func() { go m.readLoop() })
	return flow, nil
}

// claimHeldLocked hands the datagrams held for flow to its sink.
func (m *datagramMux) claimHeldLocked(flow *datagramFlow) {
	if len(m.held) == 0 {
		return
	}
	kept := m.held[:0]
	for _, held := range m.held {
		if held.flow != flow.id {
			kept = append(kept, held)
			continue
		}
		m.heldBytes -= held.size
		if flow.sink != nil {
			flow.sink.pushFrames(held.frames)
		}
	}
	clear(m.held[len(kept):])
	m.held = kept
	if len(m.held) == 0 && m.holdTimer != nil {
		m.holdTimer.Stop()
		m.holdTimer = nil
	}
}

// release unregisters the flow once its call is over. Datagrams that still
// arrive for it are dropped at once. It is safe on a nil flow and to call
// repeatedly.
func (f *datagramFlow) release() {
	if f == nil {
		return
	}
	f.releaseOnce.Do(func() {
		m := f.mux
		m.mu.Lock()
		delete(m.flows, f.id)
		if m.server {
			m.ended.add(f.id)
		}
		m.mu.Unlock()
		if f.sink != nil {
			f.sink.shutdown()
		}
	})
}

// endedContext is a context that is already done. ReceiveDatagram of both
// quic-go and webtransport-go returns what is queued before it looks at the
// context, so passing it polls for datagrams without waiting.
var endedContext = func() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}()

// flush dispatches every datagram the connection received so far, including
// the one the reader may hold at this moment.
//
// A stream's end and the datagrams sent before it travel the same packets, but
// the reader and the goroutine that reads the stream run apart. Without the
// flush, a call whose stream ended could drop messages that arrived before the
// end, because the reader had popped them from the connection's queue and not
// dispatched them yet. flush cancels the reader's wait and takes the lock the
// reader holds while it waits and dispatches, so no dispatch is in flight when
// it polls the queue.
func (m *datagramMux) flush() {
	m.flushMu.Lock()
	defer m.flushMu.Unlock()
	// Until the reader is re-armed below, every wait it starts is cancelled.
	m.waitMu.Lock()
	m.waitCancel()
	m.waitMu.Unlock()

	m.rx.Lock()
	for {
		datagram, err := m.conn.ReceiveDatagram(endedContext)
		if err != nil {
			break
		}
		m.dispatch(datagram)
	}
	m.rx.Unlock()

	m.waitMu.Lock()
	m.waitCtx, m.waitCancel = context.WithCancel(m.ctx)
	m.waitMu.Unlock()
}

// endReceive ends the receiving direction of the call: the datagram messages
// that arrived so far are queued for the call and later ones are dropped. It
// is safe on a nil flow and on a flow that receives no stream.
func (f *datagramFlow) endReceive() {
	if f == nil || f.sink == nil {
		return
	}
	f.mux.flush()
	f.sink.close()
}

// readLoop dispatches the datagrams of the connection until it ends. It holds
// rx for reading while it waits for a datagram and dispatches it, and flush
// ends the wait to take rx for writing.
func (m *datagramMux) readLoop() {
	defer m.close()
	for {
		m.rx.RLock()
		m.waitMu.Lock()
		ctx := m.waitCtx
		m.waitMu.Unlock()
		datagram, err := m.conn.ReceiveDatagram(ctx)
		if err == nil {
			m.dispatch(datagram)
		}
		m.rx.RUnlock()
		if err != nil && ctx.Err() == nil {
			return // the connection ended
		}
		if m.ctx.Err() != nil {
			return
		}
	}
}

// dispatch hands a datagram to its flow's sink, holds it for a flow that is
// not registered yet, or drops it. It never blocks.
func (m *datagramMux) dispatch(datagram []byte) {
	id, frames, ok := parseDatagram(datagram)
	// A datagram whose frames do not exactly fill it delivers nothing.
	if !ok || !walkDatagramFrames(frames, nil) {
		return
	}
	m.mu.Lock()
	if flow, live := m.flows[id]; live {
		m.mu.Unlock()
		if flow.sink != nil {
			flow.sink.pushFrames(frames)
		}
		return
	}
	if !m.server || m.ended.has(id) || len(m.held) >= m.holdMessages || m.heldBytes+len(datagram) > m.holdBytes {
		m.mu.Unlock()
		return
	}
	m.held = append(m.held, heldDatagram{flow: id, frames: frames, size: len(datagram), deadline: time.Now().Add(m.holdTime)})
	m.heldBytes += len(datagram)
	if m.holdTimer == nil {
		m.holdTimer = time.AfterFunc(m.holdTime, m.expireHeld)
	}
	m.mu.Unlock()
}

// expireHeld drops the held datagrams that waited long enough.
func (m *datagramMux) expireHeld() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	expired := 0
	for expired < len(m.held) && !m.held[expired].deadline.After(now) {
		m.heldBytes -= m.held[expired].size
		expired++
	}
	m.held = append(m.held[:0], m.held[expired:]...)
	clear(m.held[len(m.held):cap(m.held)])
	if len(m.held) == 0 {
		m.holdTimer = nil
		return
	}
	if m.holdTimer != nil {
		m.holdTimer.Reset(max(time.Until(m.held[0].deadline), 0))
	}
}

// flowSet remembers the most recently added IDs, up to its capacity.
type flowSet struct {
	capacity int
	ring     []uint64
	next     int
	set      map[uint64]struct{}
}

func (s *flowSet) add(id uint64) {
	if s.capacity <= 0 {
		return
	}
	if s.set == nil {
		s.set = make(map[uint64]struct{}, s.capacity)
	}
	if _, present := s.set[id]; present {
		return
	}
	if len(s.ring) < s.capacity {
		s.ring = append(s.ring, id)
	} else {
		delete(s.set, s.ring[s.next])
		s.ring[s.next] = id
		s.next = (s.next + 1) % s.capacity
	}
	s.set[id] = struct{}{}
}

func (s *flowSet) has(id uint64) bool {
	_, present := s.set[id]
	return present
}

// datagramSink is the bounded queue of the messages that arrived for one
// call. Pushing never blocks: a full or closed sink drops the message.
type datagramSink struct {
	maxMessage int
	// inflate holds the dictionaries the call accepts on compressed
	// messages, nil for none. It is set before the sink is used.
	inflate dictionaryLookup
	// wake holds a token whenever messages may be queued.
	wake chan struct{}

	mu     sync.Mutex
	queue  [datagramSinkMessages][]byte
	head   int
	count  int
	bytes  int
	closed bool
}

func newDatagramSink(maxMessage int) *datagramSink {
	return &datagramSink{maxMessage: maxMessage, wake: make(chan struct{}, 1)}
}

// push queues message, and reports whether it did. A message is dropped when
// the direction has ended, the queue is full, or the message is larger than a
// frame may be.
func (s *datagramSink) push(message []byte) bool {
	s.mu.Lock()
	queued := s.pushLocked(message)
	s.mu.Unlock()
	if queued {
		notify(s.wake)
	}
	return queued
}

func (s *datagramSink) pushLocked(message []byte) bool {
	if s.closed || s.count == len(s.queue) || len(message) > s.maxMessage ||
		(s.count > 0 && s.bytes+len(message) > datagramSinkBytes) {
		return false
	}
	s.queue[(s.head+s.count)%len(s.queue)] = message
	s.count++
	s.bytes += len(message)
	return true
}

// pushFrames queues the messages of the frames of a datagram that was checked
// with walkDatagramFrames. The datagram is queued as a whole with respect to
// the end of the direction: when the stream ends meanwhile, either all of its
// messages are queued before the end or none.
//
// Compressed messages are decompressed first, outside the lock, and one that
// is rejected drops the whole datagram.
func (s *datagramSink) pushFrames(frames []byte) {
	compressed := false
	walkDatagramFrames(frames, func(typ frameType, _ []byte) {
		compressed = compressed || typ == frameTypeZstdMessage
	})
	var messages [][]byte
	if compressed {
		ok := true
		walkDatagramFrames(frames, func(typ frameType, payload []byte) {
			if !ok {
				return
			}
			if typ == frameTypeZstdMessage {
				var err error
				if payload, err = inflateMessage(s.inflate, payload, s.maxMessage); err != nil {
					ok = false
					return
				}
			}
			messages = append(messages, payload)
		})
		if !ok {
			return
		}
	}
	queued := false
	s.mu.Lock()
	if compressed {
		for _, message := range messages {
			queued = s.pushLocked(message) || queued
		}
	} else {
		walkDatagramFrames(frames, func(_ frameType, message []byte) {
			queued = s.pushLocked(message) || queued
		})
	}
	s.mu.Unlock()
	if queued {
		notify(s.wake)
	}
}

// pop takes the oldest queued message.
func (s *datagramSink) pop() ([]byte, bool) {
	s.mu.Lock()
	if s.count == 0 {
		s.mu.Unlock()
		return nil, false
	}
	message := s.queue[s.head]
	s.queue[s.head] = nil
	s.head = (s.head + 1) % len(s.queue)
	s.count--
	s.bytes -= len(message)
	more := s.count > 0
	s.mu.Unlock()
	if more {
		notify(s.wake)
	}
	return message, true
}

// close ends the direction: nothing more is queued, and what is queued can
// still be taken.
func (s *datagramSink) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// shutdown ends the direction and drops what is queued.
func (s *datagramSink) shutdown() {
	s.mu.Lock()
	s.closed = true
	clear(s.queue[:])
	s.head, s.count, s.bytes = 0, 0, 0
	s.mu.Unlock()
}

// datagramSend is one message for the sender goroutine.
type datagramSend struct {
	ctx     context.Context
	payload []byte
	reply   chan<- error
}

// datagramDefaultLimit is the datagram payload size assumed until a send error
// reports the real one: the smallest payload every QUIC path carries.
const datagramDefaultLimit = 1200

// sendBatch sends the messages of batch, which are encoded MESSAGE frames, as
// datagrams, packing the ones that are there into as few as fit and never
// waiting for more. It returns the messages that belong on the stream, in
// order, reusing batch: the ones that do not fit a datagram alone, and those
// of a datagram whose send failed. A non-nil error means the call's context
// ended while waiting for the send queue, and the call is over.
func (f *datagramFlow) sendBatch(ctx context.Context, batch []encodedMessage) ([]encodedMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	m := f.mux
	prefix := quicvarint.Len(f.id)
	kept := batch[:0]
	retried := false
	for i := 0; i < len(batch); {
		limit, known := m.datagramLimit()
		size, j := prefix, i
		for j < len(batch) && size+len(batch[j].frame) <= limit {
			size += len(batch[j].frame)
			j++
		}
		if j == i {
			if known {
				// Not even this message alone fits.
				kept = append(kept, batch[i])
				i++
				continue
			}
			// The limit is only a guess: try the message alone, and learn
			// the limit if it does not fit.
			j, size = i+1, prefix+len(batch[i].frame)
		}
		payload := quicvarint.Append(make([]byte, 0, size), f.id)
		for _, message := range batch[i:j] {
			payload = append(payload, message.frame...)
		}
		err := m.send(ctx, f.reply, payload)
		if err == nil {
			i, retried = j, false
			continue
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, statusFromContextError(ctxErr)
		}
		if _, tooLarge := errors.AsType[*quic.DatagramTooLargeError](err); tooLarge && !retried {
			// The error taught the limit: pack again under it.
			retried = true
			continue
		}
		kept = append(kept, batch[i:j]...)
		i, retried = j, false
	}
	clear(batch[len(kept):])
	return kept, nil
}

// send hands payload to the sender goroutine and waits for the result. The
// goroutine exists because quic-go's SendDatagram blocks without a context
// when the connection's queue of 32 datagrams is full: a call that waits for
// it stays cancellable, and a blocked queue never holds up anything else.
func (m *datagramMux) send(ctx context.Context, reply chan error, payload []byte) error {
	m.sendOnce.Do(func() {
		m.out = make(chan datagramSend, datagramSendQueue)
		go m.sendLoop()
	})
	// A result nobody collected belongs to a send that was given up on.
	select {
	case <-reply: //nolint:errcheck // its send was given up on
	default:
	}
	select {
	case m.out <- datagramSend{ctx: ctx, payload: payload, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ctx.Done():
		return errDatagramsClosed
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-m.ctx.Done():
		return errDatagramsClosed
	}
}

var errDatagramsClosed = errors.New("datagram connection closed")

func (m *datagramMux) sendLoop() {
	for {
		select {
		case request := <-m.out:
			var err error
			if err = request.ctx.Err(); err == nil {
				err = m.conn.SendDatagram(request.payload)
				m.noteSendError(err)
			}
			request.reply <- err
		case <-m.ctx.Done():
			return
		}
	}
}

// noteSendError remembers the datagram size limit a send error reports.
func (m *datagramMux) noteSendError(err error) {
	tooLarge, ok := errors.AsType[*quic.DatagramTooLargeError](err)
	if !ok {
		return
	}
	// A WebTransport session's error counts its quarter stream ID.
	m.sizeLimit.Store(max(tooLarge.MaxDatagramPayloadSize-int64(m.overhead), 0))
	m.sizeLimitAt.Store(time.Now().UnixNano())
}

// datagramLimit returns the largest datagram to send, and whether it was
// learned from a send error (otherwise it is a guess). A learned limit is
// trusted for a short while only, so a path MTU that grew is found.
func (m *datagramMux) datagramLimit() (limit int, known bool) {
	if at := m.sizeLimitAt.Load(); at != 0 {
		if time.Now().UnixNano()-at < int64(datagramSizeProbe) {
			return int(m.sizeLimit.Load()), true
		}
		m.sizeLimitAt.CompareAndSwap(at, 0)
	}
	return max(datagramDefaultLimit-m.overhead, 0), false
}

// datagramStream is implemented by the server streams of connections whose
// RPCs may carry datagram flows.
type datagramStream interface {
	trevrpcDatagrams() *datagramMux
}

// datagramSender sends the messages of one direction of a call as datagrams,
// and leaves to the stream those that do not fit or fail.
type datagramSender struct {
	flow *datagramFlow
	ctx  context.Context
}

// send sends the messages of batch as datagrams and returns those that must go
// on the stream; see datagramFlow.sendBatch.
func (s *datagramSender) send(batch []encodedMessage) ([]encodedMessage, error) {
	return s.flow.sendBatch(s.ctx, batch)
}
