package trevrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
)

const managedTestTimeout = time.Second

func TestManagedQuicClientNativeRoundTrip(t *testing.T) {
	running := startTestQUICServer(t, func(*Server) {})
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	client, err := DialManaged(ctx, running.addr, ManagedDialOptions{DialOptions: DialOptions{TLSConfig: running.clientTLS}})
	if err != nil {
		t.Fatalf("dial managed client: %v", err)
	}

	reply, err := Unary(ctx, client, testServiceName, "SayHello", &testMessage{Value: "managed"}, func() *testMessage { return &testMessage{} })
	if err != nil {
		client.Close()
		t.Fatalf("managed RPC: %v", err)
	}
	if reply.Value != "hello, managed" {
		t.Fatalf("managed RPC = %q, want hello, managed", reply.Value)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close managed client: %v", err)
	}
}

func TestManagedQuicClientDoesNotReplayAndRecoversFutureCalls(t *testing.T) {
	initial := newFakeManagedGeneration(true, "initial")
	recovered := newFakeManagedGeneration(false, "recovered")
	connector := newFakeManagedConnector()
	events := make(chan ManagedClientEvent, 8)
	client := newTestManagedQuicClient(initial, connector, managedRealClock{}, func(event ManagedClientEvent) {
		events <- event
	})
	defer client.Close()

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), &RpcRequest{})
		callDone <- err
	}()
	waitManagedSignal(t, initial.callStarted, "initial call did not start")
	initial.fail(errors.New("connection lost"))
	attempt := waitManagedAttempt(t, connector)
	attempt.result <- managedConnectResult{generation: recovered}

	ctx, cancel := context.WithTimeout(context.Background(), managedTestTimeout)
	defer cancel()
	if err := client.WaitUntilReady(ctx); err != nil {
		t.Fatalf("wait for recovered generation: %v", err)
	}
	if err := waitManagedError(t, callDone); StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("in-flight call error = %v, want unavailable", err)
	}
	if calls := recovered.calls.Load(); calls != 0 {
		t.Fatalf("recovered generation calls = %d, want no replay", calls)
	}

	response, err := client.Call(context.Background(), &RpcRequest{})
	if err != nil {
		t.Fatalf("future call: %v", err)
	}
	if response.Message != "recovered" {
		t.Fatalf("future call response = %q, want recovered", response.Message)
	}
	if snapshot := client.Snapshot(); snapshot.State != ManagedClientStateReady || snapshot.Generation != 2 {
		t.Fatalf("recovered snapshot = %+v, want ready generation 2", snapshot)
	}

	wantEvents := []ManagedClientEventType{ManagedClientEventReady, ManagedClientEventDisconnected, ManagedClientEventReady}
	for _, want := range wantEvents {
		if event := waitManagedEvent(t, events); event.Type != want {
			t.Fatalf("event type = %v, want %v", event.Type, want)
		}
	}
}

func TestManagedQuicClientFailsFastWhileReconnecting(t *testing.T) {
	initial := newFakeManagedGeneration(false, "initial")
	recovered := newFakeManagedGeneration(false, "recovered")
	connector := newFakeManagedConnector()
	client := newTestManagedQuicClient(initial, connector, managedRealClock{}, nil)
	defer client.Close()

	initial.fail(errors.New("connection lost"))
	attempt := waitManagedAttempt(t, connector)
	if snapshot := client.Snapshot(); snapshot.State != ManagedClientStateReconnecting || snapshot.Generation != 1 {
		t.Fatalf("reconnecting snapshot = %+v, want reconnecting generation 1", snapshot)
	}

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), &RpcRequest{})
		callDone <- err
	}()
	select {
	case err := <-callDone:
		if StatusFromError(err).Code != CodeUnavailable {
			t.Fatalf("call error = %v, want unavailable", err)
		}
	case <-time.After(50 * time.Millisecond):
		t.Fatal("call blocked while reconnecting")
	}
	if calls := initial.calls.Load(); calls != 0 {
		t.Fatalf("initial generation calls = %d, want 0", calls)
	}

	waitDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), managedTestTimeout)
		defer cancel()
		waitDone <- client.WaitUntilReady(ctx)
	}()
	attempt.result <- managedConnectResult{generation: recovered}
	if err := waitManagedError(t, waitDone); err != nil {
		t.Fatalf("wait until ready: %v", err)
	}
}

func TestManagedQuicClientCloseStopsReconnect(t *testing.T) {
	initial := newFakeManagedGeneration(false, "initial")
	connector := newFakeManagedConnector()
	client := newTestManagedQuicClient(initial, connector, managedRealClock{}, nil)

	initial.fail(errors.New("connection lost"))
	_ = waitManagedAttempt(t, connector)
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if snapshot := client.Snapshot(); snapshot.State != ManagedClientStateClosed || snapshot.Generation != 1 {
		t.Fatalf("closed snapshot = %+v, want closed generation 1", snapshot)
	}
	if connector.active.Load() != 0 {
		t.Fatalf("active reconnect attempts = %d, want 0", connector.active.Load())
	}
	if closes := initial.closes.Load(); closes != 0 {
		t.Fatalf("already-dead generation closes = %d, want 0", closes)
	}
	if err := client.WaitUntilReady(context.Background()); StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("wait after close = %v, want unavailable", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}

	readyGeneration := newFakeManagedGeneration(false, "ready")
	readyClient := newTestManagedQuicClient(readyGeneration, newFakeManagedConnector(), managedRealClock{}, nil)
	if err := readyClient.Close(); err != nil {
		t.Fatalf("close ready client: %v", err)
	}
	if closes := readyGeneration.closes.Load(); closes != 1 {
		t.Fatalf("ready generation closes = %d, want 1", closes)
	}
}

func TestManagedQuicClientBackoffIsExponentialAndBounded(t *testing.T) {
	initial := newFakeManagedGeneration(false, "initial")
	recovered := newFakeManagedGeneration(false, "recovered")
	connector := newFakeManagedConnector()
	clock := newFakeManagedClock()
	config := ManagedReconnectConfig{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 25 * time.Millisecond, Multiplier: 2}
	client := newManagedQuicClient(initial, connector, clock, config, func() float64 { return 0.5 }, nil)
	defer client.Close()

	initial.fail(errors.New("connection lost"))
	wantDelays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for _, want := range wantDelays {
		attempt := waitManagedAttempt(t, connector)
		attempt.result <- managedConnectResult{err: errors.New("dial failed")}
		sleep := waitManagedSleep(t, clock)
		if sleep.delay != want {
			t.Fatalf("reconnect delay = %s, want %s", sleep.delay, want)
		}
		if connector.maxActive.Load() != 1 {
			t.Fatalf("maximum concurrent reconnects = %d, want 1", connector.maxActive.Load())
		}
		close(sleep.release)
	}
	attempt := waitManagedAttempt(t, connector)
	attempt.result <- managedConnectResult{generation: recovered}

	ctx, cancel := context.WithTimeout(context.Background(), managedTestTimeout)
	defer cancel()
	if err := client.WaitUntilReady(ctx); err != nil {
		t.Fatalf("wait for recovery: %v", err)
	}
}

func TestManagedQuicBackoffAppliesBoundedJitter(t *testing.T) {
	randomValues := []float64{0, 1}
	backoff := newManagedBackoff(ManagedReconnectConfig{
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     15 * time.Millisecond,
		Multiplier:     1,
		Jitter:         1,
	}, func() float64 {
		value := randomValues[0]
		randomValues = randomValues[1:]
		return value
	})
	if delay := backoff.Next(); delay != 0 {
		t.Fatalf("minimum jittered delay = %s, want 0", delay)
	}
	backoff.Reset()
	if delay := backoff.Next(); delay != 15*time.Millisecond {
		t.Fatalf("maximum jittered delay = %s, want bounded 15ms", delay)
	}
}

func TestManagedQuicBackoffLargeJitterDoesNotWrap(t *testing.T) {
	maximum := time.Duration(math.MaxInt64)
	backoff := newManagedBackoff(ManagedReconnectConfig{
		InitialBackoff: maximum,
		MaxBackoff:     maximum,
		Multiplier:     1,
		Jitter:         1,
	}, func() float64 { return 1 })
	if delay := backoff.Next(); delay != maximum {
		t.Fatalf("maximum jittered delay = %s, want %s", delay, maximum)
	}
}

func TestManagedQuicClientAddPathUsesCurrentGeneration(t *testing.T) {
	initial := newFakeManagedGeneration(false, "initial")
	connector := newFakeManagedConnector()
	client := newTestManagedQuicClient(initial, connector, managedRealClock{}, nil)
	defer client.Close()

	if _, err := client.AddPath(nil); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("nil migration transport error = %v, want invalid argument", err)
	}
	if _, err := client.AddPath(&quic.Transport{}); err != nil {
		t.Fatalf("add path: %v", err)
	}
	if paths := initial.addPaths.Load(); paths != 1 {
		t.Fatalf("add path calls = %d, want 1", paths)
	}
	if generation := client.Generation(); generation != 1 {
		t.Fatalf("generation after migration = %d, want 1", generation)
	}

	initial.fail(errors.New("connection lost"))
	_ = waitManagedAttempt(t, connector)
	if _, err := client.AddPath(&quic.Transport{}); StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("add path while reconnecting = %v, want unavailable", err)
	}
	if paths := initial.addPaths.Load(); paths != 1 {
		t.Fatalf("add path calls after disconnect = %d, want 1", paths)
	}
}

func TestManagedEventDispatcherBoundsSlowConsumerQueue(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	dispatcher := newManagedEventDispatcher(func(ManagedClientEvent) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	})
	dispatcher.emit(ManagedClientEvent{Type: ManagedClientEventReady})
	waitManagedSignal(t, started, "event callback did not start")

	for generation := uint64(1); generation <= 1000; generation++ {
		dispatcher.emit(ManagedClientEvent{Type: ManagedClientEventReconnectFailed, Generation: generation})
	}
	dispatcher.mu.Lock()
	queued := len(dispatcher.queue)
	dispatcher.mu.Unlock()
	if queued != managedEventQueueCapacity {
		t.Fatalf("queued lifecycle events = %d, want %d", queued, managedEventQueueCapacity)
	}

	close(release)
}

func TestManagedQuicConnectorReusesSessionAndTokenStores(t *testing.T) {
	baseTLS := &tls.Config{}
	baseQUIC := &quic.Config{}
	connector, _, err := newNativeManagedQuicConnector("127.0.0.1:1", ManagedDialOptions{DialOptions: DialOptions{TLSConfig: baseTLS, QUICConfig: baseQUIC}})
	if err != nil {
		t.Fatalf("create connector: %v", err)
	}
	if connector.tlsConfig == baseTLS || connector.quicConfig == baseQUIC {
		t.Fatal("managed connector should clone caller-owned configs")
	}
	if connector.tlsConfig.ClientSessionCache == nil {
		t.Fatal("managed connector did not install a long-lived TLS session cache")
	}
	if connector.quicConfig.TokenStore == nil {
		t.Fatal("managed connector did not install a long-lived QUIC token store")
	}
	if baseTLS.ClientSessionCache != nil || baseQUIC.TokenStore != nil {
		t.Fatal("managed connector mutated caller-owned configs")
	}

	sessionCache := tls.NewLRUClientSessionCache(2)
	tokenStore := quic.NewLRUTokenStore(2, 2)
	connector, _, err = newNativeManagedQuicConnector("127.0.0.1:1", ManagedDialOptions{DialOptions: DialOptions{
		TLSConfig:  &tls.Config{ClientSessionCache: sessionCache},
		QUICConfig: &quic.Config{TokenStore: tokenStore},
	}})
	if err != nil {
		t.Fatalf("create connector with stores: %v", err)
	}
	if connector.tlsConfig.ClientSessionCache != sessionCache || connector.quicConfig.TokenStore != tokenStore {
		t.Fatal("managed connector did not preserve configured long-lived stores")
	}
}

func newTestManagedQuicClient(initial managedQuicGeneration, connector managedQuicConnector, clock managedReconnectClock, onEvent func(ManagedClientEvent)) *ManagedQuicClient {
	config := ManagedReconnectConfig{InitialBackoff: time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	return newManagedQuicClient(initial, connector, clock, config, func() float64 { return 0.5 }, onEvent)
}

type fakeManagedGeneration struct {
	done        chan struct{}
	callStarted chan struct{}
	blockCalls  bool
	message     string
	doneOnce    sync.Once
	errMu       sync.Mutex
	err         error
	calls       atomic.Int64
	closes      atomic.Int64
	addPaths    atomic.Int64
}

func newFakeManagedGeneration(blockCalls bool, message string) *fakeManagedGeneration {
	return &fakeManagedGeneration{done: make(chan struct{}), callStarted: make(chan struct{}, 1), blockCalls: blockCalls, message: message}
}

func (g *fakeManagedGeneration) Call(ctx context.Context, _ *RpcRequest) (*RpcResponse, error) {
	g.calls.Add(1)
	select {
	case g.callStarted <- struct{}{}:
	default:
	}
	if !g.blockCalls {
		return &RpcResponse{Message: g.message}, nil
	}
	select {
	case <-g.done:
		return nil, Unavailable("fake connection lost")
	case <-ctx.Done():
		return nil, statusFromContextError(ctx.Err())
	}
}

func (g *fakeManagedGeneration) StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error) {
	g.calls.Add(1)
	return nil, Unavailable("fake streaming call unavailable")
}

func (g *fakeManagedGeneration) Close() error {
	g.closes.Add(1)
	g.fail(context.Canceled)
	return nil
}

func (g *fakeManagedGeneration) Done() <-chan struct{} {
	return g.done
}

func (g *fakeManagedGeneration) Err() error {
	g.errMu.Lock()
	defer g.errMu.Unlock()
	return g.err
}

func (g *fakeManagedGeneration) AddPath(*quic.Transport) (*quic.Path, error) {
	g.addPaths.Add(1)
	return nil, nil
}

func (g *fakeManagedGeneration) fail(err error) {
	g.doneOnce.Do(func() {
		g.errMu.Lock()
		g.err = err
		g.errMu.Unlock()
		close(g.done)
	})
}

type managedConnectResult struct {
	generation managedQuicGeneration
	err        error
}

type fakeManagedConnectAttempt struct {
	result chan managedConnectResult
}

type fakeManagedConnector struct {
	attempts  chan *fakeManagedConnectAttempt
	active    atomic.Int64
	maxActive atomic.Int64
}

func newFakeManagedConnector() *fakeManagedConnector {
	return &fakeManagedConnector{attempts: make(chan *fakeManagedConnectAttempt, 8)}
}

func (c *fakeManagedConnector) Connect(ctx context.Context) (managedQuicGeneration, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maximum := c.maxActive.Load()
		if active <= maximum || c.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	attempt := &fakeManagedConnectAttempt{result: make(chan managedConnectResult, 1)}
	select {
	case c.attempts <- attempt:
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
	select {
	case result := <-attempt.result:
		return result.generation, result.err
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

type fakeManagedSleep struct {
	delay   time.Duration
	release chan struct{}
}

type fakeManagedClock struct {
	sleeps chan fakeManagedSleep
}

func newFakeManagedClock() *fakeManagedClock {
	return &fakeManagedClock{sleeps: make(chan fakeManagedSleep, 8)}
}

func (c *fakeManagedClock) Sleep(ctx context.Context, delay time.Duration) error {
	sleep := fakeManagedSleep{delay: delay, release: make(chan struct{})}
	select {
	case c.sleeps <- sleep:
	case <-ctx.Done():
		return context.Cause(ctx)
	}
	select {
	case <-sleep.release:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func waitManagedAttempt(t *testing.T, connector *fakeManagedConnector) *fakeManagedConnectAttempt {
	t.Helper()
	select {
	case attempt := <-connector.attempts:
		return attempt
	case <-time.After(managedTestTimeout):
		t.Fatal("timed out waiting for reconnect attempt")
		return nil
	}
}

func waitManagedSleep(t *testing.T, clock *fakeManagedClock) fakeManagedSleep {
	t.Helper()
	select {
	case sleep := <-clock.sleeps:
		return sleep
	case <-time.After(managedTestTimeout):
		t.Fatal("timed out waiting for reconnect sleep")
		return fakeManagedSleep{}
	}
}

func waitManagedSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(managedTestTimeout):
		t.Fatal(message)
	}
}

func waitManagedError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(managedTestTimeout):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func waitManagedEvent(t *testing.T, events <-chan ManagedClientEvent) ManagedClientEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(managedTestTimeout):
		t.Fatal("timed out waiting for lifecycle event")
		return ManagedClientEvent{}
	}
}
