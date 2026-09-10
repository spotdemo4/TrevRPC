package trevrpc

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const channelTestTimeout = time.Second

func TestChannelDoesNotReplayAndRecoversFutureCalls(t *testing.T) {
	initial := newFakeChannelGeneration(true, "initial")
	recovered := newFakeChannelGeneration(false, "recovered")
	connector := newFakeChannelConnector()
	events := make(chan ChannelEvent, 8)
	client := newTestChannel(initial, connector, realReconnectClock{}, func(event ChannelEvent) {
		events <- event
	})
	defer client.Close()

	callDone := make(chan error, 1)
	go func() {
		_, err := client.Call(context.Background(), &RpcRequest{})
		callDone <- err
	}()
	waitChannelSignal(t, initial.callStarted, "initial call did not start")
	initial.fail(errors.New("connection lost"))
	attempt := waitChannelAttempt(t, connector)
	attempt.result <- channelConnectResult{generation: recovered}

	ctx, cancel := context.WithTimeout(context.Background(), channelTestTimeout)
	defer cancel()
	if err := client.WaitUntilReady(ctx); err != nil {
		t.Fatalf("wait for recovered generation: %v", err)
	}
	if err := waitChannelError(t, callDone); StatusFromError(err).Code != CodeUnavailable {
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
	if snapshot := client.Snapshot(); snapshot.State != ChannelStateReady || snapshot.Generation != 2 {
		t.Fatalf("recovered snapshot = %+v, want ready generation 2", snapshot)
	}

	wantEvents := []ChannelEventType{ChannelEventReady, ChannelEventDisconnected, ChannelEventReady}
	for _, want := range wantEvents {
		if event := waitChannelEvent(t, events); event.Type != want {
			t.Fatalf("event type = %v, want %v", event.Type, want)
		}
	}
}

func TestChannelCallsFailFastWhileReconnecting(t *testing.T) {
	initial := newFakeChannelGeneration(false, "initial")
	recovered := newFakeChannelGeneration(false, "recovered")
	connector := newFakeChannelConnector()
	client := newTestChannel(initial, connector, realReconnectClock{}, nil)
	defer client.Close()

	initial.fail(errors.New("connection lost"))
	attempt := waitChannelAttempt(t, connector)
	if snapshot := client.Snapshot(); snapshot.State != ChannelStateReconnecting || snapshot.Generation != 1 {
		t.Fatalf("reconnecting snapshot = %+v, want reconnecting generation 1", snapshot)
	}

	if _, err := client.Call(context.Background(), &RpcRequest{}); StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("call while reconnecting = %v, want unavailable", err)
	}
	if calls := initial.calls.Load(); calls != 0 {
		t.Fatalf("initial generation calls = %d, want 0", calls)
	}

	attempt.result <- channelConnectResult{generation: recovered}
	if err := client.WaitUntilReady(context.Background()); err != nil {
		t.Fatalf("wait after reconnect: %v", err)
	}
	if _, err := client.Call(context.Background(), &RpcRequest{}); err != nil {
		t.Fatalf("call after reconnect: %v", err)
	}
}

func TestChannelCloseStopsReconnect(t *testing.T) {
	initial := newFakeChannelGeneration(false, "initial")
	connector := newFakeChannelConnector()
	client := newTestChannel(initial, connector, realReconnectClock{}, nil)

	initial.fail(errors.New("connection lost"))
	_ = waitChannelAttempt(t, connector)
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if snapshot := client.Snapshot(); snapshot.State != ChannelStateClosed || snapshot.Generation != 1 {
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

	readyGeneration := newFakeChannelGeneration(false, "ready")
	readyClient := newTestChannel(readyGeneration, newFakeChannelConnector(), realReconnectClock{}, nil)
	if err := readyClient.Close(); err != nil {
		t.Fatalf("close ready client: %v", err)
	}
	if closes := readyGeneration.closes.Load(); closes != 1 {
		t.Fatalf("ready generation closes = %d, want 1", closes)
	}
}

func TestChannelBackoffIsExponentialAndBounded(t *testing.T) {
	initial := newFakeChannelGeneration(false, "initial")
	recovered := newFakeChannelGeneration(false, "recovered")
	connector := newFakeChannelConnector()
	clock := newFakeChannelClock()
	config := reconnectConfig{InitialBackoff: 10 * time.Millisecond, MaxBackoff: 25 * time.Millisecond, Multiplier: 2}
	client := newChannel(initial, connector, clock, config, func() float64 { return 0.5 }, nil)
	defer client.Close()

	initial.fail(errors.New("connection lost"))
	wantDelays := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 25 * time.Millisecond, 25 * time.Millisecond}
	for index, want := range wantDelays {
		sleep := waitChannelSleep(t, clock)
		if sleep.delay != want {
			t.Fatalf("reconnect delay = %s, want %s", sleep.delay, want)
		}
		close(sleep.release)
		attempt := waitChannelAttempt(t, connector)
		if connector.maxActive.Load() != 1 {
			t.Fatalf("maximum concurrent reconnects = %d, want 1", connector.maxActive.Load())
		}
		if index == len(wantDelays)-1 {
			attempt.result <- channelConnectResult{generation: recovered}
		} else {
			attempt.result <- channelConnectResult{err: errors.New("dial failed")}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), channelTestTimeout)
	defer cancel()
	if err := client.WaitUntilReady(ctx); err != nil {
		t.Fatalf("wait for recovery: %v", err)
	}
}

func TestChannelBackoffAppliesBoundedJitter(t *testing.T) {
	randomValues := []float64{0, 1}
	backoff := newReconnectBackoff(reconnectConfig{
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

func TestChannelBackoffLargeJitterDoesNotWrap(t *testing.T) {
	maximum := time.Duration(math.MaxInt64)
	backoff := newReconnectBackoff(reconnectConfig{
		InitialBackoff: maximum,
		MaxBackoff:     maximum,
		Multiplier:     1,
		Jitter:         1,
	}, func() float64 { return 1 })
	if delay := backoff.Next(); delay != maximum {
		t.Fatalf("maximum jittered delay = %s, want %s", delay, maximum)
	}
}

func TestChannelEventDispatcherBoundsSlowConsumerQueue(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	dispatcher := newChannelEventDispatcher(func(ChannelEvent) {
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
	})
	dispatcher.emit(ChannelEvent{Type: ChannelEventReady})
	waitChannelSignal(t, started, "event callback did not start")

	for generation := uint64(1); generation <= 1000; generation++ {
		dispatcher.emit(ChannelEvent{Type: ChannelEventReconnectFailed, Generation: generation})
	}
	dispatcher.mu.Lock()
	queued := len(dispatcher.queue)
	dispatcher.mu.Unlock()
	if queued != channelEventQueueCapacity {
		t.Fatalf("queued lifecycle events = %d, want %d", queued, channelEventQueueCapacity)
	}
	if dropped := dispatcher.dropped.Load(); dropped == 0 {
		t.Fatal("bounded event queue drops were not observable")
	}

	close(release)
}

func TestChannelEventDispatcherRecoversCallbackPanicAndContinues(t *testing.T) {
	delivered := make(chan ChannelEvent, 1)
	var calls atomic.Int64
	dispatcher := newChannelEventDispatcher(func(event ChannelEvent) {
		if calls.Add(1) == 1 {
			panic("callback boom")
		}
		delivered <- event
	})
	dispatcher.emit(ChannelEvent{Type: ChannelEventReady})
	dispatcher.close(ChannelEvent{Type: ChannelEventClosed})
	select {
	case event := <-delivered:
		if event.Type != ChannelEventClosed {
			t.Fatalf("event after panic = %v, want closed", event.Type)
		}
	case <-time.After(channelTestTimeout):
		t.Fatal("event dispatcher stopped after callback panic")
	}
}

func newTestChannel(initial channelGeneration, connector channelConnector, clock reconnectClock, onEvent func(ChannelEvent)) *Channel {
	config := reconnectConfig{InitialBackoff: time.Millisecond, MaxBackoff: time.Second, Multiplier: 2}
	return newChannel(initial, connector, clock, config, func() float64 { return 0.5 }, onEvent)
}

type fakeChannelGeneration struct {
	done        chan struct{}
	callStarted chan struct{}
	blockCalls  bool
	message     string
	doneOnce    sync.Once
	errMu       sync.Mutex
	err         error
	calls       atomic.Int64
	closes      atomic.Int64
}

func newFakeChannelGeneration(blockCalls bool, message string) *fakeChannelGeneration {
	return &fakeChannelGeneration{done: make(chan struct{}), callStarted: make(chan struct{}, 1), blockCalls: blockCalls, message: message}
}

func (g *fakeChannelGeneration) Call(ctx context.Context, _ *RpcRequest) (*RpcResponse, error) {
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

func (g *fakeChannelGeneration) StreamingCall(context.Context, *RpcRequest, ByteStream) (FrameStream, error) {
	g.calls.Add(1)
	return nil, Unavailable("fake streaming call unavailable")
}

func (g *fakeChannelGeneration) Close() error {
	g.closes.Add(1)
	g.fail(context.Canceled)
	return nil
}

func (g *fakeChannelGeneration) Done() <-chan struct{} {
	return g.done
}

func (g *fakeChannelGeneration) Err() error {
	g.errMu.Lock()
	defer g.errMu.Unlock()
	return g.err
}

func (g *fakeChannelGeneration) Info() ConnectionInfo {
	return ConnectionInfo{Provider: "fake"}
}

func (g *fakeChannelGeneration) CloseReason(err error) TransportCloseReason {
	return TransportCloseReason{Err: err, Message: errorString(err)}
}

func (g *fakeChannelGeneration) fail(err error) {
	g.doneOnce.Do(func() {
		g.errMu.Lock()
		g.err = err
		g.errMu.Unlock()
		close(g.done)
	})
}

type channelConnectResult struct {
	generation channelGeneration
	err        error
}

type fakeChannelConnectAttempt struct {
	result chan channelConnectResult
}

type fakeChannelConnector struct {
	attempts  chan *fakeChannelConnectAttempt
	active    atomic.Int64
	maxActive atomic.Int64
}

func newFakeChannelConnector() *fakeChannelConnector {
	return &fakeChannelConnector{attempts: make(chan *fakeChannelConnectAttempt, 8)}
}

func (c *fakeChannelConnector) Connect(ctx context.Context) (channelGeneration, error) {
	active := c.active.Add(1)
	defer c.active.Add(-1)
	for {
		maximum := c.maxActive.Load()
		if active <= maximum || c.maxActive.CompareAndSwap(maximum, active) {
			break
		}
	}
	attempt := &fakeChannelConnectAttempt{result: make(chan channelConnectResult, 1)}
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

type fakeChannelSleep struct {
	delay   time.Duration
	release chan struct{}
}

type fakeChannelClock struct {
	sleeps chan fakeChannelSleep
}

func newFakeChannelClock() *fakeChannelClock {
	return &fakeChannelClock{sleeps: make(chan fakeChannelSleep, 8)}
}

func (c *fakeChannelClock) Sleep(ctx context.Context, delay time.Duration) error {
	sleep := fakeChannelSleep{delay: delay, release: make(chan struct{})}
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

func waitChannelAttempt(t *testing.T, connector *fakeChannelConnector) *fakeChannelConnectAttempt {
	t.Helper()
	select {
	case attempt := <-connector.attempts:
		return attempt
	case <-time.After(channelTestTimeout):
		t.Fatal("timed out waiting for reconnect attempt")
		return nil
	}
}

func waitChannelSleep(t *testing.T, clock *fakeChannelClock) fakeChannelSleep {
	t.Helper()
	select {
	case sleep := <-clock.sleeps:
		return sleep
	case <-time.After(channelTestTimeout):
		t.Fatal("timed out waiting for reconnect sleep")
		return fakeChannelSleep{}
	}
}

func waitChannelSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(channelTestTimeout):
		t.Fatal(message)
	}
}

func waitChannelError(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(channelTestTimeout):
		t.Fatal("timed out waiting for result")
		return nil
	}
}

func waitChannelEvent(t *testing.T, events <-chan ChannelEvent) ChannelEvent {
	t.Helper()
	select {
	case event := <-events:
		return event
	case <-time.After(channelTestTimeout):
		t.Fatal("timed out waiting for lifecycle event")
		return ChannelEvent{}
	}
}
