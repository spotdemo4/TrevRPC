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
	Type           ChannelEventType
	State          ChannelState
	Generation     uint64
	Err            error
	ReconnectDelay time.Duration
	Connection     ConnectionInfo
	CloseReason    TransportCloseReason
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

// Dial establishes a reconnecting Channel with the built-in native C backend.
// An https target uses WebTransport; other targets use native QUIC. The context
// applies only to the initial dial; Close controls the Channel.
func Dial(ctx context.Context, target string, options DialOptions) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	connector, err := newChannelConnector(target, options)
	if err != nil {
		return nil, err
	}
	return dialChannel(ctx, connector, options.OnEvent)
}

// DialWithBackend establishes a reconnecting Channel with an explicit optional backend.
// Backend packages normally wrap this function with their own typed options.
func DialWithBackend(
	ctx context.Context,
	target string,
	options DialOptions,
	backend DialBackend,
) (*Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContextError(err)
	}
	connector, err := newBackendChannelConnector(target, options, backend)
	if err != nil {
		return nil, err
	}
	return dialChannel(ctx, connector, options.OnEvent)
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

func newChannelConnector(target string, options DialOptions) (channelConnector, error) {
	return newBackendChannelConnector(
		target,
		options,
		defaultNativeBackend(),
	)
}

func newBackendChannelConnector(
	target string,
	options DialOptions,
	backend DialBackend,
) (channelConnector, error) {
	if backend == nil {
		return nil, InvalidArgument("dial backend is nil")
	}
	if _, err := resolveExplicitBackend(backend.Backend()); err != nil {
		return nil, err
	}
	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	connector, err := backend.NewConnector(target, BackendDialOptions{
		Transport:    options.Transport,
		Limits:       options.Limits,
		Credentials:  cloneTransportCredentials(options.Credentials),
		MaxFrameSize: maxFrameSize,
		WebTransport: cloneWebTransportOptions(options.WebTransport),
	})
	if err != nil {
		return nil, err
	}
	if connector == nil {
		return nil, InvalidArgument("dial backend returned a nil connector")
	}
	return backendChannelConnector{
		connector:    connector,
		maxFrameSize: maxFrameSize,
	}, nil
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
	c.events.close(ChannelEvent{
		Type:       ChannelEventClosed,
		State:      snapshot.State,
		Generation: snapshot.Generation,
		Connection: generationConnectionInfo(generation),
		CloseReason: TransportCloseReason{
			Local:   true,
			Clean:   err == nil,
			Message: "channel closed",
			Err:     err,
		},
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
	client.events.emit(ChannelEvent{
		Type:       ChannelEventReady,
		State:      ChannelStateReady,
		Generation: 1,
		Connection: generationConnectionInfo(initial),
	})
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

		if !c.beginReconnect(number, generation, generation.Err()) {
			return
		}
		if releaser, ok := generation.(disconnectedGenerationReleaser); ok {
			releaser.releaseDisconnected()
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

func (c *Channel) beginReconnect(
	number uint64,
	generation channelGeneration,
	err error,
) bool {
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
		Type:        ChannelEventDisconnected,
		State:       snapshot.State,
		Generation:  snapshot.Generation,
		Err:         err,
		Connection:  generationConnectionInfo(generation),
		CloseReason: generationCloseReason(generation, err),
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
	c.events.emit(ChannelEvent{
		Type:       ChannelEventReady,
		State:      ChannelStateReady,
		Generation: number,
		Connection: generationConnectionInfo(generation),
	})
	return true, number
}

type channelGeneration interface {
	ClientTransport
	Done() <-chan struct{}
	Err() error
	Info() ConnectionInfo
	CloseReason(error) TransportCloseReason
}

type disconnectedGenerationReleaser interface {
	releaseDisconnected()
}

func generationConnectionInfo(generation channelGeneration) ConnectionInfo {
	if generation == nil {
		return ConnectionInfo{}
	}
	return generation.Info()
}

func generationCloseReason(
	generation channelGeneration,
	err error,
) TransportCloseReason {
	if generation == nil {
		return TransportCloseReason{Err: err}
	}
	return generation.CloseReason(err)
}

type channelConnector interface {
	Connect(context.Context) (channelGeneration, error)
}

type backendChannelConnector struct {
	connector    BackendConnector
	maxFrameSize int
}

func (c backendChannelConnector) Connect(ctx context.Context) (channelGeneration, error) {
	connection, err := c.connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if connection.Endpoint == nil {
		if connection.Close != nil {
			_ = connection.Close()
		}
		return nil, InvalidArgument("dial backend returned a nil endpoint")
	}
	return &backendChannelGeneration{
		connection: connection,
		client: newTransportStreamClient(
			connection.Endpoint,
			c.maxFrameSize,
			connection.MapStatus,
		),
	}, nil
}

type backendChannelGeneration struct {
	connection BackendConnection
	client     *transportStreamClient
	closeOnce  sync.Once
	closeErr   error
}

func (g *backendChannelGeneration) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return g.client.Call(ctx, request)
}

func (g *backendChannelGeneration) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return g.client.StreamingCall(ctx, request, requestBody)
}

func (g *backendChannelGeneration) Close() error {
	g.closeOnce.Do(func() {
		if g.connection.Close != nil {
			g.closeErr = g.connection.Close()
			return
		}
		g.closeErr = g.connection.Endpoint.Close(TransportCloseReason{
			Local:   true,
			Clean:   true,
			Message: "client closed",
		})
	})
	return g.closeErr
}

func (g *backendChannelGeneration) releaseDisconnected() {
	_ = g.Close()
}

func (g *backendChannelGeneration) Done() <-chan struct{} {
	return g.connection.Endpoint.Done()
}

func (g *backendChannelGeneration) Err() error {
	return g.connection.Endpoint.Err()
}

func (g *backendChannelGeneration) Info() ConnectionInfo {
	return g.connection.Endpoint.Info()
}

func (g *backendChannelGeneration) CloseReason(err error) TransportCloseReason {
	if g.connection.CloseReason != nil {
		return g.connection.CloseReason(err)
	}
	if mapper, ok := g.connection.Endpoint.(interface {
		CloseReason(error) TransportCloseReason
	}); ok {
		return mapper.CloseReason(err)
	}
	return TransportCloseReason{Err: err, Message: errorString(err)}
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

var _ ClientTransport = (*Channel)(nil)
