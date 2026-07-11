package trevrpc

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	defaultReconnectInitialBackoff = 100 * time.Millisecond
	defaultReconnectMaxBackoff     = 30 * time.Second
	defaultReconnectMultiplier     = 2
	defaultReconnectJitter         = 0.2
	defaultChannelSessionCache     = 64
	defaultChannelTokenOrigins     = 32
	defaultChannelTokensPerOrigin  = 4
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
	Type           ChannelEventType
	State          ChannelState
	Generation     uint64
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
}

// Dial establishes the initial connection and returns a reconnecting Channel. A
// target with an https URL uses WebTransport; other targets use native QUIC.
// The context applies only to the initial dial; Close controls the Channel.
func Dial(ctx context.Context, target string, options DialOptions) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	connector, err := newChannelConnector(target, options)
	if err != nil {
		return nil, err
	}
	initial, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return newChannel(initial, connector, realReconnectClock{}, defaultReconnectConfig(), rand.Float64, options.OnEvent), nil
}

func newChannelConnector(target string, options DialOptions) (channelConnector, error) {
	if strings.HasPrefix(target, "https://") {
		return newWebTransportChannelConnector(target, options)
	}
	if scheme, _, ok := strings.Cut(target, "://"); ok {
		return nil, InvalidArgument("unsupported dial target scheme " + scheme)
	}
	return newNativeQUICConnector(target, options)
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

// State returns the current connectivity state.
func (c *Channel) State() ChannelState {
	return c.Snapshot().State
}

// Generation returns the latest successfully established connection generation.
func (c *Channel) Generation() uint64 {
	return c.Snapshot().Generation
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

// Call sends a unary RPC on the connection generation ready when the call starts.
func (c *Channel) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	generation, err := c.callGeneration(ctx)
	if err != nil {
		return nil, err
	}
	return generation.Call(ctx, request)
}

// StreamingCall starts a streaming RPC on the connection generation ready when the call starts.
func (c *Channel) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	generation, err := c.callGeneration(ctx)
	if err != nil {
		return nil, err
	}
	return generation.StreamingCall(ctx, request, requestBody)
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
	c.events.close(ChannelEvent{Type: ChannelEventClosed, State: snapshot.State, Generation: snapshot.Generation})
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
	client.events.emit(ChannelEvent{Type: ChannelEventReady, State: ChannelStateReady, Generation: 1})
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
	c.events.emit(ChannelEvent{Type: ChannelEventDisconnected, State: snapshot.State, Generation: snapshot.Generation, Err: err})
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
	c.events.emit(ChannelEvent{Type: ChannelEventReady, State: ChannelStateReady, Generation: number})
	return true, number
}

type channelGeneration interface {
	ClientTransport
	Done() <-chan struct{}
	Err() error
}

type nativeQUICGeneration struct {
	client *RawQUICClient
	conn   *quic.Conn
}

func (g *nativeQUICGeneration) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return g.client.Call(ctx, request)
}

func (g *nativeQUICGeneration) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return g.client.StreamingCall(ctx, request, requestBody)
}

func (g *nativeQUICGeneration) Close() error {
	return g.client.Close()
}

func (g *nativeQUICGeneration) Done() <-chan struct{} {
	return g.conn.Context().Done()
}

func (g *nativeQUICGeneration) Err() error {
	return context.Cause(g.conn.Context())
}

func (g *nativeQUICGeneration) AddPath(transport *quic.Transport) (*quic.Path, error) {
	return g.conn.AddPath(transport)
}

type channelConnector interface {
	Connect(context.Context) (channelGeneration, error)
}

type nativeQUICConnector struct {
	addr         string
	tlsConfig    *tls.Config
	quicConfig   *quic.Config
	maxFrameSize int
}

func newNativeQUICConnector(addr string, options DialOptions) (*nativeQUICConnector, error) {
	if options.TLSConfig == nil {
		return nil, InvalidArgument("quic-go dial requires TLSConfig")
	}

	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	tlsConfig := options.TLSConfig.Clone()
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(defaultChannelSessionCache)
	}
	quicConfig := QUICClientConfig(maxFrameSize, options.QUICConfig)
	applyDefaultQUICTransportConfig(quicConfig, options.Transport)
	if quicConfig.TokenStore == nil {
		quicConfig.TokenStore = quic.NewLRUTokenStore(defaultChannelTokenOrigins, defaultChannelTokensPerOrigin)
	}

	return &nativeQUICConnector{addr: addr, tlsConfig: tlsConfig, quicConfig: quicConfig, maxFrameSize: maxFrameSize}, nil
}

func (c *nativeQUICConnector) Connect(ctx context.Context) (channelGeneration, error) {
	// DialAddr completes the handshake before returning, so RPC bytes are never sent as 0-RTT data.
	conn, err := quic.DialAddr(ctx, c.addr, c.tlsConfig, c.quicConfig)
	if err != nil {
		return nil, transportOrContextStatus(ctx, err)
	}
	return &nativeQUICGeneration{client: newRawQUICClient(conn).WithMaxFrameSize(c.maxFrameSize), conn: conn}, nil
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
	hook   func(ChannelEvent)
	mu     sync.Mutex
	queue  []ChannelEvent
	wake   chan struct{}
	closed bool
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
			d.hook(event)
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

var _ ClientTransport = (*Channel)(nil)
