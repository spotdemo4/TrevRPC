package trevrpc

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedConnector connects after delay with generation, or fails with err.
// A connect still waiting when its context ends counts as cancelled.
type scriptedConnector struct {
	delay     time.Duration
	err       error
	kind      TransportKind
	attempts  atomic.Int32
	closed    atomic.Int32
	cancelled atomic.Int32
}

func (c *scriptedConnector) Connect(ctx context.Context) (channelGeneration, error) {
	c.attempts.Add(1)
	select {
	case <-time.After(c.delay):
	case <-ctx.Done():
		c.cancelled.Add(1)
		return nil, ctx.Err()
	}
	if c.err != nil {
		return nil, c.err
	}
	done := make(chan struct{})
	return &streamGeneration{
		kind:  c.kind,
		close: func() error { c.closed.Add(1); return nil },
		done:  done,
		err:   func() error { return nil },
	}, nil
}

func connectKind(t *testing.T, connector channelConnector) TransportKind {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	generation, err := connector.Connect(ctx)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return generation.transportKind()
}

// promptly bounds how long a fallback that needs no wait may take. It is far
// below the delays the tests rule out, an hour, and far above any scheduling
// delay of a loaded machine.
const promptly = 5 * time.Second

func TestRaceUsesQUICWhenItConnectsInTime(t *testing.T) {
	quic := &scriptedConnector{delay: time.Millisecond, kind: TransportQUIC}
	tcp := &scriptedConnector{kind: TransportQMux}
	// The fallback would start after an hour, so QUIC wins however slowly it
	// is scheduled.
	if kind := connectKind(t, newRacingConnector(quic, tcp, time.Hour, 0)); kind != TransportQUIC {
		t.Fatalf("transport = %s, want quic", kind)
	}
	if tcp.attempts.Load() != 0 {
		t.Fatal("fallback was attempted although QUIC connected in time")
	}
}

func TestRaceFallsBackWhenQUICIsSilent(t *testing.T) {
	quic := &scriptedConnector{delay: time.Hour, kind: TransportQUIC}
	tcp := &scriptedConnector{kind: TransportQMux}
	started := time.Now()
	if kind := connectKind(t, newRacingConnector(quic, tcp, 20*time.Millisecond, 0)); kind != TransportQMux {
		t.Fatalf("transport = %s, want qmux", kind)
	}
	if elapsed := time.Since(started); elapsed > promptly {
		t.Fatalf("fallback took %v", elapsed)
	}
}

func TestRaceFallsBackAtOnceWhenQUICFails(t *testing.T) {
	quic := &scriptedConnector{err: errors.New("no route"), kind: TransportQUIC}
	tcp := &scriptedConnector{kind: TransportQMux}
	started := time.Now()
	if kind := connectKind(t, newRacingConnector(quic, tcp, time.Hour, 0)); kind != TransportQMux {
		t.Fatalf("transport = %s, want qmux", kind)
	}
	if elapsed := time.Since(started); elapsed > promptly {
		t.Fatalf("fallback after a QUIC failure took %v", elapsed)
	}
}

func TestRaceReportsBothFailures(t *testing.T) {
	quic := &scriptedConnector{err: errors.New("udp blocked")}
	tcp := &scriptedConnector{err: errors.New("tcp refused")}
	_, err := newRacingConnector(quic, tcp, time.Millisecond, 0).Connect(context.Background())
	status := StatusFromError(err)
	if status.Code != CodeUnavailable || !strings.Contains(status.Message, "udp blocked") || !strings.Contains(status.Message, "tcp refused") {
		t.Fatalf("error = %v, want both failures", err)
	}
}

func TestRaceClosesTheLoser(t *testing.T) {
	// QUIC never connects on its own, so the fallback wins however the
	// goroutines are scheduled, and the race must cancel the QUIC attempt.
	quic := &scriptedConnector{delay: time.Hour, kind: TransportQUIC}
	tcp := &scriptedConnector{kind: TransportQMux}
	if kind := connectKind(t, newRacingConnector(quic, tcp, 10*time.Millisecond, 0)); kind != TransportQMux {
		t.Fatalf("transport = %s, want qmux", kind)
	}
	waitFor(t, "the losing QUIC attempt to be cancelled", func() bool { return quic.cancelled.Load() > 0 })
}

func TestRaceClosesALoserThatAlsoConnects(t *testing.T) {
	// The primary ignores cancellation and connects only once the fallback
	// has won, which the test decides instead of a timer.
	quic := &uncancellableConnector{release: make(chan struct{})}
	tcp := &scriptedConnector{kind: TransportQMux}
	if kind := connectKind(t, newRacingConnector(quic, tcp, 10*time.Millisecond, 0)); kind != TransportQMux {
		t.Fatalf("transport = %s, want qmux", kind)
	}
	close(quic.release)
	waitFor(t, "the losing QUIC connection to be closed", func() bool { return quic.closed.Load() > 0 })
}

// uncancellableConnector ignores its context and connects once release is
// closed.
type uncancellableConnector struct {
	release chan struct{}
	closed  atomic.Int32
}

func (c *uncancellableConnector) Connect(context.Context) (channelGeneration, error) {
	<-c.release
	return &streamGeneration{
		kind:  TransportQUIC,
		close: func() error { c.closed.Add(1); return nil },
		done:  make(chan struct{}),
		err:   func() error { return nil },
	}, nil
}

func TestRaceRemembersTheFallbackUntilReprobe(t *testing.T) {
	quic := &scriptedConnector{err: errors.New("udp blocked"), kind: TransportQUIC}
	tcp := &scriptedConnector{kind: TransportQMux}
	connector := newRacingConnector(quic, tcp, time.Hour, time.Minute)
	now := time.Now()
	connector.now = func() time.Time { return now }

	connectKind(t, connector)
	connectKind(t, connector)
	if attempts := quic.attempts.Load(); attempts != 1 {
		t.Fatalf("QUIC attempts = %d, want 1 while the fallback is remembered", attempts)
	}
	now = now.Add(2 * time.Minute)
	connectKind(t, connector)
	if attempts := quic.attempts.Load(); attempts != 2 {
		t.Fatalf("QUIC attempts = %d, want a reprobe after the interval", attempts)
	}
}

func TestDialFallsBackToQMuxWhenUDPIsUnreachable(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	// Nothing answers QUIC on this UDP port.
	var listenConfig net.ListenConfig
	dead, err := listenConfig.ListenPacket(t.Context(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, dead)

	events := make(chan ChannelEvent, 8)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, dead.LocalAddr().String(), DialOptions{
		TLSConfig:     pki.client,
		TCPAddr:       listener.TCPAddr().String(),
		FallbackDelay: 50 * time.Millisecond,
		OnEvent:       func(event ChannelEvent) { events <- event },
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	closeOnCleanup(t, channel)
	if transport := channel.Transport(); transport != TransportQMux {
		t.Fatalf("Transport = %q, want qmux", transport)
	}
	if event := <-events; event.Type != ChannelEventReady || event.Transport != TransportQMux {
		t.Fatalf("first event = %+v, want ready over qmux", event)
	}
	exerciseRPCShapes(t, channel)
}

// TestDialDefaultsPreferQUIC checks what Dial does without options: race
// QMux over TLS against QUIC once QUIC has had defaultFallbackDelay. The race
// itself is tested with scripted connectors above; a race over real sockets
// would depend on how fast a loaded machine finishes a handshake.
func TestDialDefaultsPreferQUIC(t *testing.T) {
	var options DialOptions
	if options.Transport != TransportAuto {
		t.Fatalf("default Transport = %v, want TransportAuto", options.Transport)
	}
	connector := newRacingConnector(nil, nil, options.FallbackDelay, options.ReprobeInterval)
	if connector.delay != defaultFallbackDelay || connector.reprobe != defaultReprobeInterval {
		t.Fatalf("default delay %v and reprobe %v, want %v and %v", connector.delay, connector.reprobe, defaultFallbackDelay, defaultReprobeInterval)
	}
}

func TestDialTCPOnly(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportTCPOnly})
	if err != nil {
		t.Fatal(err)
	}
	closeOnCleanup(t, channel)
	if transport := channel.Transport(); transport != TransportQMux {
		t.Fatalf("Transport = %q, want qmux", transport)
	}
}
