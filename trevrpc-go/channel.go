package trevrpc

import (
	"context"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultReconnectInitialBackoff = 100 * time.Millisecond
	defaultReconnectMaxBackoff     = 30 * time.Second
	defaultReconnectMultiplier     = 2
	defaultReconnectJitter         = 0.2
	channelEventQueueCapacity      = 64
)

// ChannelState describes a Channel's connectivity state.
type ChannelState uint8

const (
	// ChannelStateConnecting indicates that no connection has been established yet.
	ChannelStateConnecting ChannelState = iota
	// ChannelStateReady indicates that new calls can use the current connection.
	ChannelStateReady
	// ChannelStateReconnecting indicates that the current connection was lost.
	ChannelStateReconnecting
	// ChannelStateClosed indicates that the channel was explicitly closed.
	ChannelStateClosed
)

// String returns the state name.
func (s ChannelState) String() string {
	switch s {
	case ChannelStateConnecting:
		return "connecting"
	case ChannelStateReady:
		return "ready"
	case ChannelStateReconnecting:
		return "reconnecting"
	case ChannelStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ChannelSnapshot is a coherent connectivity state and connection generation.
type ChannelSnapshot struct {
	State      ChannelState
	Generation uint64
}

// ChannelEventType identifies a channel lifecycle event.
type ChannelEventType uint8

const (
	// ChannelEventReady indicates that a connection generation became ready.
	ChannelEventReady ChannelEventType = iota
	// ChannelEventDisconnected indicates that the ready connection was lost.
	ChannelEventDisconnected
	// ChannelEventReconnectFailed indicates that a redial failed and will be retried.
	ChannelEventReconnectFailed
	// ChannelEventClosed indicates that the channel was explicitly closed.
	ChannelEventClosed
)

// ChannelEvent describes a channel lifecycle transition or reconnect failure.
type ChannelEvent struct {
	Type       ChannelEventType
	State      ChannelState
	Generation uint64
	// Transport is the transport of the generation that became ready; it is
	// set on ChannelEventReady only.
	Transport      TransportKind
	Err            error
	ReconnectDelay time.Duration
}

type reconnectConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
}

func defaultReconnectConfig() reconnectConfig {
	return reconnectConfig{
		InitialBackoff: defaultReconnectInitialBackoff,
		MaxBackoff:     defaultReconnectMaxBackoff,
		Multiplier:     defaultReconnectMultiplier,
		Jitter:         defaultReconnectJitter,
	}
}

// Channel maintains a TrevRPC connection and reconnects after connection loss.
// Each RPC uses exactly one connection generation and is never retried or replayed.
type Channel struct {
	mu         sync.RWMutex
	state      ChannelState
	generation uint64
	current    channelGeneration
	ready      chan struct{}
	closed     chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	workerDone chan struct{}
	connector  channelConnector
	clock      reconnectClock
	backoff    reconnectBackoff
	events     *channelEventDispatcher
	// connectionMetadata is the channel's connection metadata, nil for a
	// channel built without Dial.
	connectionMetadata *connectionMetadataSource
}

// SetConnectionMetadata replaces the channel's connection metadata (see
// DialOptions.ConnectionMetadata), for example when a token is refreshed.
// Calls that start afterwards carry the new metadata; each connection offers
// it until the server confirms it. Empty metadata removes it.
func (c *Channel) SetConnectionMetadata(metadata Metadata) error {
	if c.connectionMetadata == nil {
		return FailedPrecondition("the channel has no connection metadata source")
	}
	return c.connectionMetadata.set(metadata)
}

func dialChannel(
	ctx context.Context,
	connector channelConnector,
	onEvent func(ChannelEvent),
) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	initial, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return newChannel(initial, connector, realReconnectClock{}, defaultReconnectConfig(), rand.Float64, onEvent), nil
}

// Ready reports whether new calls can snapshot a ready connection generation.
func (c *Channel) Ready() bool {
	return c != nil && c.State() == ChannelStateReady
}

// WaitUntilReady waits for a current or future connection generation to become ready.
func (c *Channel) WaitUntilReady(ctx context.Context) error {
	if c == nil {
		return Unavailable("channel is nil")
	}
	for {
		c.mu.RLock()
		state := c.state
		ready := c.ready
		closed := c.closed
		c.mu.RUnlock()

		switch state {
		case ChannelStateReady:
			return nil
		case ChannelStateClosed:
			return Unavailable("channel closed")
		}

		select {
		case <-ready:
		case <-closed:
			return Unavailable("channel closed")
		case <-ctx.Done():
			return statusFromContextError(ctx.Err())
		}
	}
}

// Transport returns the transport of the current connection, or "" while the
// channel is not ready.
func (c *Channel) Transport() TransportKind {
	generation, err := c.currentGeneration()
	if err != nil {
		return ""
	}
	return generation.transportKind()
}

// State returns the current connectivity state.
func (c *Channel) State() ChannelState {
	return c.Snapshot().State
}

// Generation returns the latest successfully established connection generation.
func (c *Channel) Generation() uint64 {
	return c.Snapshot().Generation
}

// DroppedEventCount returns the number of lifecycle events dropped by the bounded callback queue.
func (c *Channel) DroppedEventCount() uint64 {
	if c == nil || c.events == nil {
		return 0
	}
	return c.events.dropped.Load()
}

// Snapshot returns a coherent connectivity state and connection generation.
func (c *Channel) Snapshot() ChannelSnapshot {
	if c == nil {
		return ChannelSnapshot{State: ChannelStateClosed}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return ChannelSnapshot{State: c.state, Generation: c.generation}
}

// call starts a single-message RPC on the connection generation ready when the call starts.
func (c *Channel) call(ctx context.Context, request *wireRequest, message ProtoMessage) (frameStream, error) {
	generation, err := c.callGeneration(ctx)
	if err != nil {
		return nil, err
	}
	return generation.call(ctx, request, message)
}

// streamingCall starts a request-streaming RPC on the connection generation ready when the call starts.
func (c *Channel) streamingCall(ctx context.Context, request *wireRequest, requests encodedStream) (frameStream, error) {
	generation, err := c.callGeneration(ctx)
	if err != nil {
		return nil, err
	}
	return generation.streamingCall(ctx, request, requests)
}

// Close stops reconnecting and closes the current connection generation.
func (c *Channel) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if c.state == ChannelStateClosed {
		c.mu.Unlock()
		return nil
	}
	generation := c.current
	c.current = nil
	c.state = ChannelStateClosed
	close(c.closed)
	c.cancel()
	snapshot := ChannelSnapshot{State: c.state, Generation: c.generation}
	c.mu.Unlock()

	var err error
	if generation != nil {
		err = generation.Close()
	}
	<-c.workerDone
	c.events.close(ChannelEvent{
		Type:       ChannelEventClosed,
		State:      snapshot.State,
		Generation: snapshot.Generation,
		Err:        err,
	})
	return err
}

func (c *Channel) callGeneration(ctx context.Context) (channelGeneration, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	return c.currentGeneration()
}

func (c *Channel) currentGeneration() (channelGeneration, error) {
	if c == nil {
		return nil, Unavailable("channel is nil")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != ChannelStateReady || c.current == nil {
		return nil, Unavailable("channel " + c.state.String())
	}
	return c.current, nil
}

func newChannel(initial channelGeneration, connector channelConnector, clock reconnectClock, reconnect reconnectConfig, random func() float64, onEvent func(ChannelEvent)) *Channel {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	close(ready)
	client := &Channel{
		state:      ChannelStateReady,
		generation: 1,
		current:    initial,
		ready:      ready,
		closed:     make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
		workerDone: make(chan struct{}),
		connector:  connector,
		clock:      clock,
		backoff:    newReconnectBackoff(reconnect, random),
		events:     newChannelEventDispatcher(onEvent),
	}
	client.events.emit(ChannelEvent{Type: ChannelEventReady, State: ChannelStateReady, Generation: 1, Transport: initial.transportKind()})
	go client.run(initial, 1)
	return client
}

func (c *Channel) run(generation channelGeneration, number uint64) {
	defer close(c.workerDone)
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-generation.Done():
		}

		if !c.beginReconnect(number, generation.Err()) {
			return
		}
		c.backoff.Reset()
		delay := c.backoff.Next()
		for {
			if err := c.clock.Sleep(c.ctx, delay); err != nil {
				return
			}
			next, err := c.connector.Connect(c.ctx)
			if err == nil {
				published, nextNumber := c.publish(next)
				if !published {
					_ = next.Close()
					return
				}
				generation = next
				number = nextNumber
				break
			}
			if c.ctx.Err() != nil {
				return
			}

			delay = c.backoff.Next()
			c.events.emit(ChannelEvent{Type: ChannelEventReconnectFailed, State: ChannelStateReconnecting, Generation: number, Err: err, ReconnectDelay: delay})
		}
	}
}

func (c *Channel) beginReconnect(number uint64, err error) bool {
	c.mu.Lock()
	if c.state == ChannelStateClosed || c.generation != number {
		c.mu.Unlock()
		return false
	}
	c.current = nil
	c.state = ChannelStateReconnecting
	c.ready = make(chan struct{})
	snapshot := ChannelSnapshot{State: c.state, Generation: c.generation}
	c.mu.Unlock()
	c.events.emit(ChannelEvent{
		Type:       ChannelEventDisconnected,
		State:      snapshot.State,
		Generation: snapshot.Generation,
		Err:        err,
	})
	return true
}

func (c *Channel) publish(generation channelGeneration) (bool, uint64) {
	c.mu.Lock()
	if c.state == ChannelStateClosed {
		c.mu.Unlock()
		return false, 0
	}
	c.generation++
	number := c.generation
	c.current = generation
	c.state = ChannelStateReady
	close(c.ready)
	c.mu.Unlock()
	c.events.emit(ChannelEvent{Type: ChannelEventReady, State: ChannelStateReady, Generation: number, Transport: generation.transportKind()})
	return true, number
}

// channelGeneration is one established connection. It must close Done when the
// connection is lost and report why through Err.
type channelGeneration interface {
	Transport
	Close() error
	Done() <-chan struct{}
	Err() error
	transportKind() TransportKind
}

type channelConnector interface {
	Connect(context.Context) (channelGeneration, error)
}

type reconnectBackoff struct {
	config reconnectConfig
	next   time.Duration
	random func() float64
}

func newReconnectBackoff(config reconnectConfig, random func() float64) reconnectBackoff {
	return reconnectBackoff{config: config, next: config.InitialBackoff, random: random}
}

func (b *reconnectBackoff) Reset() {
	b.next = b.config.InitialBackoff
}

func (b *reconnectBackoff) Next() time.Duration {
	base := b.next
	next := float64(base) * b.config.Multiplier
	if next >= float64(b.config.MaxBackoff) {
		b.next = b.config.MaxBackoff
	} else {
		b.next = time.Duration(next)
	}
	if b.config.Jitter == 0 {
		return base
	}
	factor := 1 - b.config.Jitter + 2*b.config.Jitter*b.random()
	delay := float64(base) * factor
	if delay <= 0 {
		return 0
	}
	if delay >= float64(b.config.MaxBackoff) {
		return b.config.MaxBackoff
	}
	return time.Duration(delay)
}

type reconnectClock interface {
	Sleep(context.Context, time.Duration) error
}

type realReconnectClock struct{}

func (realReconnectClock) Sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type channelEventDispatcher struct {
	hook    func(ChannelEvent)
	mu      sync.Mutex
	queue   []ChannelEvent
	wake    chan struct{}
	closed  bool
	dropped atomic.Uint64
}

func newChannelEventDispatcher(hook func(ChannelEvent)) *channelEventDispatcher {
	dispatcher := &channelEventDispatcher{hook: hook}
	if hook != nil {
		dispatcher.wake = make(chan struct{}, 1)
		go dispatcher.run()
	}
	return dispatcher
}

func (d *channelEventDispatcher) emit(event ChannelEvent) {
	if d.hook == nil {
		return
	}
	d.mu.Lock()
	if !d.closed {
		if len(d.queue) == channelEventQueueCapacity {
			copy(d.queue, d.queue[1:])
			d.queue = d.queue[:channelEventQueueCapacity-1]
			d.dropped.Add(1)
		}
		d.queue = append(d.queue, event)
	}
	d.mu.Unlock()
	d.signal()
}

func (d *channelEventDispatcher) close(event ChannelEvent) {
	if d.hook == nil {
		return
	}
	d.mu.Lock()
	if !d.closed {
		if len(d.queue) == channelEventQueueCapacity {
			copy(d.queue, d.queue[1:])
			d.queue = d.queue[:channelEventQueueCapacity-1]
			d.dropped.Add(1)
		}
		d.queue = append(d.queue, event)
		d.closed = true
	}
	d.mu.Unlock()
	d.signal()
}

func (d *channelEventDispatcher) signal() {
	if d.hook == nil {
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *channelEventDispatcher) run() {
	for {
		d.mu.Lock()
		if len(d.queue) != 0 {
			event := d.queue[0]
			d.queue = d.queue[1:]
			d.mu.Unlock()
			func() {
				defer func() { _ = recover() }()
				d.hook(event)
			}()
			continue
		}
		closed := d.closed
		d.mu.Unlock()
		if closed {
			return
		}
		<-d.wake
	}
}

var _ Transport = (*Channel)(nil)
