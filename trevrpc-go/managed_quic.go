package trevrpc

import (
	"context"
	"crypto/tls"
	"math"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
)

const (
	defaultManagedInitialBackoff  = 100 * time.Millisecond
	defaultManagedMaxBackoff      = 30 * time.Second
	defaultManagedMultiplier      = 2
	defaultManagedJitter          = 0.2
	defaultManagedSessionCache    = 64
	defaultManagedTokenOrigins    = 32
	defaultManagedTokensPerOrigin = 4
	managedEventQueueCapacity     = 64
)

// ManagedClientState describes the connectivity state of a ManagedQuicClient.
type ManagedClientState uint8

const (
	// ManagedClientStateConnecting indicates that no connection has been established yet.
	ManagedClientStateConnecting ManagedClientState = iota
	// ManagedClientStateReady indicates that new calls can use the current connection.
	ManagedClientStateReady
	// ManagedClientStateReconnecting indicates that the current connection was lost.
	ManagedClientStateReconnecting
	// ManagedClientStateClosed indicates that the client was explicitly closed.
	ManagedClientStateClosed
)

// String returns the state name.
func (s ManagedClientState) String() string {
	switch s {
	case ManagedClientStateConnecting:
		return "connecting"
	case ManagedClientStateReady:
		return "ready"
	case ManagedClientStateReconnecting:
		return "reconnecting"
	case ManagedClientStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ManagedClientSnapshot is a coherent connectivity state and connection generation.
type ManagedClientSnapshot struct {
	State      ManagedClientState
	Generation uint64
}

// ManagedClientEventType identifies a managed client lifecycle event.
type ManagedClientEventType uint8

const (
	// ManagedClientEventReady indicates that a connection generation became ready.
	ManagedClientEventReady ManagedClientEventType = iota
	// ManagedClientEventDisconnected indicates that the ready connection was lost.
	ManagedClientEventDisconnected
	// ManagedClientEventReconnectFailed indicates that a redial failed and will be retried.
	ManagedClientEventReconnectFailed
	// ManagedClientEventClosed indicates that the client was explicitly closed.
	ManagedClientEventClosed
)

// ManagedClientEvent describes a managed client lifecycle transition or reconnect failure.
type ManagedClientEvent struct {
	Type       ManagedClientEventType
	State      ManagedClientState
	Generation uint64
	Err        error
	RetryDelay time.Duration
}

// ManagedReconnectConfig configures bounded exponential reconnect delays.
type ManagedReconnectConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
	Jitter         float64
}

// DefaultManagedReconnectConfig returns the default reconnect policy.
func DefaultManagedReconnectConfig() ManagedReconnectConfig {
	return ManagedReconnectConfig{
		InitialBackoff: defaultManagedInitialBackoff,
		MaxBackoff:     defaultManagedMaxBackoff,
		Multiplier:     defaultManagedMultiplier,
		Jitter:         defaultManagedJitter,
	}
}

// ManagedDialOptions configures DialManaged.
type ManagedDialOptions struct {
	DialOptions
	Reconnect ManagedReconnectConfig
	// OnEvent receives lifecycle events asynchronously and in order. It must return promptly.
	OnEvent func(ManagedClientEvent)
}

// ManagedQuicClient reconnects a native QUIC TrevRPC client after connection loss.
// Each RPC snapshots exactly one connection generation and is never replayed.
type ManagedQuicClient struct {
	mu         sync.RWMutex
	state      ManagedClientState
	generation uint64
	current    managedQuicGeneration
	ready      chan struct{}
	closed     chan struct{}
	ctx        context.Context
	cancel     context.CancelFunc
	workerDone chan struct{}
	connector  managedQuicConnector
	clock      managedReconnectClock
	backoff    managedBackoff
	events     *managedEventDispatcher
}

// DialManaged establishes the initial native QUIC connection and manages later reconnects.
// The provided context applies only to the initial dial; Close controls the returned client.
func DialManaged(ctx context.Context, addr string, options ManagedDialOptions) (*ManagedQuicClient, error) {
	connector, reconnect, err := newNativeManagedQuicConnector(addr, options)
	if err != nil {
		return nil, err
	}
	initial, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return newManagedQuicClient(initial, connector, managedRealClock{}, reconnect, rand.Float64, options.OnEvent), nil
}

// Ready reports whether new calls can snapshot a ready connection generation.
func (c *ManagedQuicClient) Ready() bool {
	return c != nil && c.State() == ManagedClientStateReady
}

// WaitUntilReady waits for a current or future connection generation to become ready.
func (c *ManagedQuicClient) WaitUntilReady(ctx context.Context) error {
	if c == nil {
		return Unavailable("managed QUIC transport is nil")
	}
	for {
		c.mu.RLock()
		state := c.state
		ready := c.ready
		closed := c.closed
		c.mu.RUnlock()

		switch state {
		case ManagedClientStateReady:
			return nil
		case ManagedClientStateClosed:
			return Unavailable("managed QUIC transport closed")
		}

		select {
		case <-ready:
		case <-closed:
			return Unavailable("managed QUIC transport closed")
		case <-ctx.Done():
			return statusFromContextError(ctx.Err())
		}
	}
}

// State returns the current connectivity state.
func (c *ManagedQuicClient) State() ManagedClientState {
	return c.Snapshot().State
}

// Generation returns the latest successfully established connection generation.
func (c *ManagedQuicClient) Generation() uint64 {
	return c.Snapshot().Generation
}

// Snapshot returns a coherent connectivity state and connection generation.
func (c *ManagedQuicClient) Snapshot() ManagedClientSnapshot {
	if c == nil {
		return ManagedClientSnapshot{State: ManagedClientStateClosed}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return ManagedClientSnapshot{State: c.state, Generation: c.generation}
}

// Call sends a unary RPC on the connection generation ready when the call starts.
func (c *ManagedQuicClient) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	generation, err := c.callGeneration()
	if err != nil {
		return nil, err
	}
	return generation.Call(ctx, request)
}

// StreamingCall starts a streaming RPC on the connection generation ready when the call starts.
func (c *ManagedQuicClient) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	generation, err := c.callGeneration()
	if err != nil {
		return nil, err
	}
	return generation.StreamingCall(ctx, request, requestBody)
}

// AddPath adds a migration path to the current connection generation. The caller
// must probe and switch the returned path according to quic-go's Path API.
func (c *ManagedQuicClient) AddPath(transport *quic.Transport) (*quic.Path, error) {
	if transport == nil {
		return nil, InvalidArgument("QUIC migration transport is nil")
	}
	generation, err := c.callGeneration()
	if err != nil {
		return nil, err
	}
	return generation.AddPath(transport)
}

// Close stops reconnecting and closes the current connection generation.
func (c *ManagedQuicClient) Close() error {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	if c.state == ManagedClientStateClosed {
		c.mu.Unlock()
		return nil
	}
	generation := c.current
	c.current = nil
	c.state = ManagedClientStateClosed
	close(c.closed)
	c.cancel()
	snapshot := ManagedClientSnapshot{State: c.state, Generation: c.generation}
	c.mu.Unlock()

	var err error
	if generation != nil {
		err = generation.Close()
	}
	<-c.workerDone
	c.events.close(ManagedClientEvent{Type: ManagedClientEventClosed, State: snapshot.State, Generation: snapshot.Generation})
	return err
}

func (c *ManagedQuicClient) callGeneration() (managedQuicGeneration, error) {
	if c == nil {
		return nil, Unavailable("managed QUIC transport is nil")
	}
	c.mu.RLock()
	generation := c.current
	state := c.state
	c.mu.RUnlock()
	if state != ManagedClientStateReady || generation == nil {
		return nil, Unavailable("managed QUIC transport " + state.String())
	}
	return generation, nil
}

func newManagedQuicClient(initial managedQuicGeneration, connector managedQuicConnector, clock managedReconnectClock, reconnect ManagedReconnectConfig, random func() float64, onEvent func(ManagedClientEvent)) *ManagedQuicClient {
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{})
	close(ready)
	client := &ManagedQuicClient{
		state:      ManagedClientStateReady,
		generation: 1,
		current:    initial,
		ready:      ready,
		closed:     make(chan struct{}),
		ctx:        ctx,
		cancel:     cancel,
		workerDone: make(chan struct{}),
		connector:  connector,
		clock:      clock,
		backoff:    newManagedBackoff(reconnect, random),
		events:     newManagedEventDispatcher(onEvent),
	}
	client.events.emit(ManagedClientEvent{Type: ManagedClientEventReady, State: ManagedClientStateReady, Generation: 1})
	go client.run(initial, 1)
	return client
}

func (c *ManagedQuicClient) run(generation managedQuicGeneration, number uint64) {
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
		for {
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

			delay := c.backoff.Next()
			c.events.emit(ManagedClientEvent{Type: ManagedClientEventReconnectFailed, State: ManagedClientStateReconnecting, Generation: number, Err: err, RetryDelay: delay})
			if err := c.clock.Sleep(c.ctx, delay); err != nil {
				return
			}
		}
	}
}

func (c *ManagedQuicClient) beginReconnect(number uint64, err error) bool {
	c.mu.Lock()
	if c.state == ManagedClientStateClosed || c.generation != number {
		c.mu.Unlock()
		return false
	}
	c.current = nil
	c.state = ManagedClientStateReconnecting
	c.ready = make(chan struct{})
	snapshot := ManagedClientSnapshot{State: c.state, Generation: c.generation}
	c.mu.Unlock()
	c.events.emit(ManagedClientEvent{Type: ManagedClientEventDisconnected, State: snapshot.State, Generation: snapshot.Generation, Err: err})
	return true
}

func (c *ManagedQuicClient) publish(generation managedQuicGeneration) (bool, uint64) {
	c.mu.Lock()
	if c.state == ManagedClientStateClosed {
		c.mu.Unlock()
		return false, 0
	}
	c.generation++
	number := c.generation
	c.current = generation
	c.state = ManagedClientStateReady
	close(c.ready)
	c.mu.Unlock()
	c.events.emit(ManagedClientEvent{Type: ManagedClientEventReady, State: ManagedClientStateReady, Generation: number})
	return true, number
}

type managedQuicGeneration interface {
	ClientTransport
	Done() <-chan struct{}
	Err() error
	AddPath(*quic.Transport) (*quic.Path, error)
}

type nativeManagedQuicGeneration struct {
	client *QuicClient
	conn   *quic.Conn
}

func (g *nativeManagedQuicGeneration) Call(ctx context.Context, request *RpcRequest) (*RpcResponse, error) {
	return g.client.Call(ctx, request)
}

func (g *nativeManagedQuicGeneration) StreamingCall(ctx context.Context, request *RpcRequest, requestBody ByteStream) (FrameStream, error) {
	return g.client.StreamingCall(ctx, request, requestBody)
}

func (g *nativeManagedQuicGeneration) Close() error {
	return g.client.Close()
}

func (g *nativeManagedQuicGeneration) Done() <-chan struct{} {
	return g.conn.Context().Done()
}

func (g *nativeManagedQuicGeneration) Err() error {
	return context.Cause(g.conn.Context())
}

func (g *nativeManagedQuicGeneration) AddPath(transport *quic.Transport) (*quic.Path, error) {
	return g.conn.AddPath(transport)
}

type managedQuicConnector interface {
	Connect(context.Context) (managedQuicGeneration, error)
}

type nativeManagedQuicConnector struct {
	addr         string
	tlsConfig    *tls.Config
	quicConfig   *quic.Config
	maxFrameSize int
}

func newNativeManagedQuicConnector(addr string, options ManagedDialOptions) (*nativeManagedQuicConnector, ManagedReconnectConfig, error) {
	if options.TLSConfig == nil {
		return nil, ManagedReconnectConfig{}, InvalidArgument("quic-go dial requires TLSConfig")
	}
	reconnect, err := normalizeManagedReconnectConfig(options.Reconnect)
	if err != nil {
		return nil, ManagedReconnectConfig{}, err
	}

	maxFrameSize := options.MaxFrameSize
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	tlsConfig := options.TLSConfig.Clone()
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(defaultManagedSessionCache)
	}
	quicConfig := QUICClientConfig(maxFrameSize, options.QUICConfig)
	applyDefaultQUICTransportConfig(quicConfig, options.Transport)
	if quicConfig.TokenStore == nil {
		quicConfig.TokenStore = quic.NewLRUTokenStore(defaultManagedTokenOrigins, defaultManagedTokensPerOrigin)
	}

	return &nativeManagedQuicConnector{addr: addr, tlsConfig: tlsConfig, quicConfig: quicConfig, maxFrameSize: maxFrameSize}, reconnect, nil
}

func (c *nativeManagedQuicConnector) Connect(ctx context.Context) (managedQuicGeneration, error) {
	conn, err := quic.DialAddr(ctx, c.addr, c.tlsConfig, c.quicConfig)
	if err != nil {
		return nil, transportOrContextStatus(ctx, err)
	}
	return &nativeManagedQuicGeneration{client: NewQuicClient(conn).WithMaxFrameSize(c.maxFrameSize), conn: conn}, nil
}

func normalizeManagedReconnectConfig(config ManagedReconnectConfig) (ManagedReconnectConfig, error) {
	if config == (ManagedReconnectConfig{}) {
		return DefaultManagedReconnectConfig(), nil
	}
	if config.InitialBackoff == 0 {
		config.InitialBackoff = defaultManagedInitialBackoff
	}
	if config.MaxBackoff == 0 {
		config.MaxBackoff = defaultManagedMaxBackoff
	}
	if config.Multiplier == 0 {
		config.Multiplier = defaultManagedMultiplier
	}
	if config.InitialBackoff < 0 || config.MaxBackoff < config.InitialBackoff {
		return ManagedReconnectConfig{}, InvalidArgument("managed reconnect backoff bounds are invalid")
	}
	if config.Multiplier < 1 || math.IsNaN(config.Multiplier) || math.IsInf(config.Multiplier, 0) {
		return ManagedReconnectConfig{}, InvalidArgument("managed reconnect multiplier must be finite and at least 1")
	}
	if config.Jitter < 0 || config.Jitter > 1 || math.IsNaN(config.Jitter) {
		return ManagedReconnectConfig{}, InvalidArgument("managed reconnect jitter must be between 0 and 1")
	}
	return config, nil
}

type managedBackoff struct {
	config ManagedReconnectConfig
	next   time.Duration
	random func() float64
}

func newManagedBackoff(config ManagedReconnectConfig, random func() float64) managedBackoff {
	return managedBackoff{config: config, next: config.InitialBackoff, random: random}
}

func (b *managedBackoff) Reset() {
	b.next = b.config.InitialBackoff
}

func (b *managedBackoff) Next() time.Duration {
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

type managedReconnectClock interface {
	Sleep(context.Context, time.Duration) error
}

type managedRealClock struct{}

func (managedRealClock) Sleep(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

type managedEventDispatcher struct {
	hook   func(ManagedClientEvent)
	mu     sync.Mutex
	queue  []ManagedClientEvent
	wake   chan struct{}
	closed bool
}

func newManagedEventDispatcher(hook func(ManagedClientEvent)) *managedEventDispatcher {
	dispatcher := &managedEventDispatcher{hook: hook}
	if hook != nil {
		dispatcher.wake = make(chan struct{}, 1)
		go dispatcher.run()
	}
	return dispatcher
}

func (d *managedEventDispatcher) emit(event ManagedClientEvent) {
	if d.hook == nil {
		return
	}
	d.mu.Lock()
	if !d.closed {
		if len(d.queue) == managedEventQueueCapacity {
			copy(d.queue, d.queue[1:])
			d.queue = d.queue[:managedEventQueueCapacity-1]
		}
		d.queue = append(d.queue, event)
	}
	d.mu.Unlock()
	d.signal()
}

func (d *managedEventDispatcher) close(event ManagedClientEvent) {
	if d.hook == nil {
		return
	}
	d.mu.Lock()
	if !d.closed {
		if len(d.queue) == managedEventQueueCapacity {
			copy(d.queue, d.queue[1:])
			d.queue = d.queue[:managedEventQueueCapacity-1]
		}
		d.queue = append(d.queue, event)
		d.closed = true
	}
	d.mu.Unlock()
	d.signal()
}

func (d *managedEventDispatcher) signal() {
	if d.hook == nil {
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *managedEventDispatcher) run() {
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

var _ ClientTransport = (*ManagedQuicClient)(nil)
