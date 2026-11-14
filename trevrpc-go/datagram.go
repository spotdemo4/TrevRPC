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
	"google.golang.org/protobuf/reflect/protoreflect"
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
	method            protoreflect.MethodDescriptor
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
	// datagramCountWait is how long the end of a direction waits for the
	// datagrams its DATAGRAM_COUNT announced that have not arrived yet.
	datagramCountWait = 10 * time.Millisecond
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

	// hold limits and the wait for counted datagrams, variables of the mux so
	// tests can change them.
	holdMessages, holdBytes int
	holdTime                time.Duration
	countWait               time.Duration

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
	flow uint64
	// frames are the datagram's frames, if wellFormed; a malformed datagram
	// is held only to be counted.
	frames     []byte
	wellFormed bool
	size       int
	deadline   time.Time
	// unheld counts the datagrams of the flow that found no room while this
	// one was held. They are dropped, but count toward the call's
	// DATAGRAM_COUNT when it claims this one.
	unheld uint64
}

// newDatagramMux returns the demultiplexer of conn. A client passes
// server == false. endedCapacity is how many recently ended flows a server
// remembers.
func newDatagramMux(conn datagramConn, overhead int, server bool, endedCapacity int) *datagramMux {
	ctx, cancel := context.WithCancel(context.Background())
	return &datagramMux{
		conn:         conn,
		overhead:     overhead,
		server:       server,
		ctx:          ctx,
		cancel:       cancel,
		holdMessages: datagramHoldMessages,
		holdBytes:    datagramHoldBytes,
		holdTime:     datagramHoldTime,
		countWait:    datagramCountWait,
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
	received atomic.Int64
	// sent counts the datagrams of the call the transport accepted, which the
	// end of the sending direction announces in DATAGRAM_COUNT.
	sent        atomic.Uint64
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
		if flow.sink == nil {
			continue
		}
		if held.wellFormed {
			flow.sink.pushFrames(held.frames)
		} else {
			flow.sink.discard(1)
		}
		flow.sink.discard(held.unheld)
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

// endReceive ends the receiving direction of the call, whose stream announced
// count datagrams in its DATAGRAM_COUNT, zero if it had none. The end of a
// stream and the datagrams sent before it leave the peer together, but the
// datagrams can reach the call later: the network may reorder them, the peer's
// QUIC stack may have sent them after the end, and the reader dispatches them
// on a goroutine of its own, behind quic-go's HTTP/3 layer on WebTransport,
// which moves each datagram to its session on another. So the direction stays
// open until count datagrams arrived for the call, the mux's countWait passed,
// the call's ctx ended, or stop is closed. Then the messages that arrived stay
// queued for the call, and later ones are dropped. It is safe on a nil flow
// and on a flow that receives no stream.
func (f *datagramFlow) endReceive(ctx context.Context, count uint64, stop <-chan struct{}) {
	if f == nil || f.sink == nil {
		return
	}
	if arrived := f.sink.awaitArrivals(count); arrived != nil {
		timer := time.NewTimer(f.mux.countWait)
		select {
		case <-arrived:
		case <-timer.C:
		case <-ctx.Done():
		case <-stop:
		case <-f.mux.ctx.Done():
		}
		timer.Stop()
	}
	f.sink.close()
}

// readLoop dispatches the datagrams of the connection until it ends.
func (m *datagramMux) readLoop() {
	defer m.close()
	for {
		datagram, err := m.conn.ReceiveDatagram(m.ctx)
		if err != nil {
			return
		}
		m.dispatch(datagram)
	}
}

// dispatch hands a datagram to its flow's sink, holds it for a flow that is
// not registered yet, or drops it. It never blocks.
func (m *datagramMux) dispatch(datagram []byte) {
	id, frames, ok := parseDatagram(datagram)
	if !ok {
		return
	}
	// A datagram whose frames do not exactly fill it delivers nothing, but
	// counts toward its call's DATAGRAM_COUNT all the same.
	wellFormed := walkDatagramFrames(frames, nil)
	m.mu.Lock()
	if flow, live := m.flows[id]; live {
		m.mu.Unlock()
		switch {
		case flow.sink == nil:
		case wellFormed:
			flow.sink.pushFrames(frames)
		default:
			flow.sink.discard(1)
		}
		return
	}
	if !m.server || m.ended.has(id) {
		m.mu.Unlock()
		return
	}
	// A malformed datagram is held as a count of one, with no bytes.
	held := heldDatagram{flow: id, wellFormed: wellFormed, deadline: time.Now().Add(m.holdTime)}
	if wellFormed {
		held.frames, held.size = frames, len(datagram)
	}
	if len(m.held) >= m.holdMessages || m.heldBytes+held.size > m.holdBytes {
		// No room: the datagram is dropped, and counted on a datagram of
		// its flow that is held, if there is one.
		for i := range m.held {
			if m.held[i].flow == id {
				m.held[i].unheld++
				break
			}
		}
		m.mu.Unlock()
		return
	}
	m.held = append(m.held, held)
	m.heldBytes += held.size
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
	// arrived counts the datagrams of the call, whether or not they were
	// queued. Once it reaches awaited, reached is closed; see awaitArrivals.
	arrived uint64
	awaited uint64
	reached chan struct{}
}

func newDatagramSink(maxMessage int) *datagramSink {
	return &datagramSink{maxMessage: maxMessage, wake: make(chan struct{}, 1)}
}

// pushLocked queues message, and reports whether it did. A message is dropped
// when the direction has ended, the queue is full, or the message is larger
// than a frame may be.
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
// is rejected drops the whole datagram. Every datagram counts as arrived.
func (s *datagramSink) pushFrames(frames []byte) {
	compressed := false
	walkDatagramFrames(frames, func(typ frameType, _ []byte) {
		compressed = compressed || typ == frameTypeZstdMessage
	})
	var messages [][]byte
	ok := true
	if compressed {
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
	}
	queued := false
	s.mu.Lock()
	switch {
	case !ok:
	case compressed:
		for _, message := range messages {
			queued = s.pushLocked(message) || queued
		}
	default:
		walkDatagramFrames(frames, func(_ frameType, message []byte) {
			queued = s.pushLocked(message) || queued
		})
	}
	reached := s.arriveLocked(1)
	s.mu.Unlock()
	if queued {
		notify(s.wake)
	}
	if reached != nil {
		close(reached)
	}
}

// discard counts n datagrams of the call that deliver nothing.
func (s *datagramSink) discard(n uint64) {
	if n == 0 {
		return
	}
	s.mu.Lock()
	reached := s.arriveLocked(n)
	s.mu.Unlock()
	if reached != nil {
		close(reached)
	}
}

// arriveLocked counts n datagrams that arrived for the call, and returns the
// channel to close if they are the last ones awaitArrivals waits for.
func (s *datagramSink) arriveLocked(n uint64) chan struct{} {
	s.arrived += n
	reached := s.reached
	if reached == nil || s.arrived < s.awaited {
		return nil
	}
	s.reached = nil
	return reached
}

// awaitArrivals returns a channel that is closed once count datagrams arrived
// for the call, or nil if they have already.
func (s *datagramSink) awaitArrivals(count uint64) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.arrived >= count {
		return nil
	}
	s.awaited, s.reached = count, make(chan struct{})
	return s.reached
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
			f.sent.Add(1)
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
	case _ = <-reply:
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

// appendDatagramCount appends the DATAGRAM_COUNT that ends the sending
// direction of the call, if the call sent any datagrams. It is safe on a nil
// flow.
func (f *datagramFlow) appendDatagramCount(dst []byte) []byte {
	if f == nil {
		return dst
	}
	if sent := f.sent.Load(); sent > 0 {
		return appendDatagramCountFrame(dst, sent)
	}
	return dst
}

// send sends the messages of batch as datagrams and returns those that must go
// on the stream; see datagramFlow.sendBatch.
func (s *datagramSender) send(batch []encodedMessage) ([]encodedMessage, error) {
	return s.flow.sendBatch(s.ctx, batch)
}
