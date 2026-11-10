package ratelimit

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	loginMethod  = "/example.auth.Auth/Login"
	searchMethod = "/example.search.Search/Query"
	otherMethod  = "/example.search.Search/Other"
)

// clock is a settable clock.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *clock { return &clock{now: time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// call runs the interceptor for a call to method from the client at addr, an
// IP address without a port, or empty for an unknown address.
func call(l *Limiter, method, addr string) error {
	info := trevrpc.HandlerContext{FullMethod: method}
	if addr != "" {
		info.RemoteAddr = netip.AddrPortFrom(netip.MustParseAddr(addr), 4433)
	}
	ctx := context.Background()
	got, finish, err := l.Intercept(ctx, info)
	if got != ctx || finish != nil {
		panic("the limiter must return the context it got and no finish function")
	}
	return err
}

// expectAllowed fails the test unless every call is allowed.
func expectAllowed(t *testing.T, l *Limiter, method, addr string, calls int) {
	t.Helper()
	for i := range calls {
		if err := call(l, method, addr); err != nil {
			t.Fatalf("call %d to %s from %q: %v", i+1, method, addr, err)
		}
	}
}

// expectRejected fails the test unless the next call fails with
// ResourceExhausted and says to retry after delay.
func expectRejected(t *testing.T, l *Limiter, method, addr string, delay time.Duration) {
	t.Helper()
	err := call(l, method, addr)
	if err == nil {
		t.Fatalf("call to %s from %q was allowed, want rejection", method, addr)
	}
	status := trevrpc.StatusFromError(err)
	if status.Code != trevrpc.CodeResourceExhausted {
		t.Fatalf("status = %v, want ResourceExhausted", status)
	}
	if got, ok := trevrpc.RetryDelay(err); !ok || got != delay {
		t.Fatalf("retry delay = %v, %v, want %v", got, ok, delay)
	}
}

func TestBurstThenRefillAtTheConfiguredRate(t *testing.T) {
	clock := newClock()
	// One token every second, up to three.
	l := New(Options{Default: &Policy{Requests: 3, Window: 3 * time.Second}, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "192.0.2.1", 3)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Second)

	// A rejection does not spend anything, and the delay shrinks as time passes.
	clock.Advance(400 * time.Millisecond)
	expectRejected(t, l, searchMethod, "192.0.2.1", 600*time.Millisecond)
	clock.Advance(600 * time.Millisecond)
	expectAllowed(t, l, searchMethod, "192.0.2.1", 1)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Second)

	// Two tokens refill in two seconds.
	clock.Advance(2 * time.Second)
	expectAllowed(t, l, searchMethod, "192.0.2.1", 2)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Second)

	// An idle client gets the burst again, and no more.
	clock.Advance(time.Hour)
	expectAllowed(t, l, searchMethod, "192.0.2.1", 3)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Second)
}

func TestTheRetryDelayRoundsUpToAMillisecond(t *testing.T) {
	clock := newClock()
	// One token every 333,333,333 ns.
	l := New(Options{Default: &Policy{Requests: 3, Window: time.Second}, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "192.0.2.1", 3)
	expectRejected(t, l, searchMethod, "192.0.2.1", 334*time.Millisecond)
	// Waiting for the delay is always enough.
	clock.Advance(334 * time.Millisecond)
	expectAllowed(t, l, searchMethod, "192.0.2.1", 1)
}

func TestARejectionSaysWhyAndWhenToRetry(t *testing.T) {
	clock := newClock()
	l := New(Options{Policies: map[string]Policy{loginMethod: {Requests: 1, Window: time.Minute}}, Now: clock.Now})
	expectAllowed(t, l, loginMethod, "192.0.2.1", 1)

	status := trevrpc.StatusFromError(call(l, loginMethod, "192.0.2.1"))
	const want = "rate limit exceeded: 1 requests per 1m0s"
	if status.Code != trevrpc.CodeResourceExhausted || status.Message != want {
		t.Fatalf("status = %v %q, want ResourceExhausted %q", status.Code, status.Message, want)
	}
	if len(status.Details) != 1 || status.Details[0].Type != "google.rpc.RetryInfo" || len(status.Metadata) != 0 {
		t.Fatalf("details = %+v, metadata = %v", status.Details, status.Metadata)
	}
}

func TestPoliciesApplyToTheirMethodsAndTheDefaultToTheRest(t *testing.T) {
	clock := newClock()
	l := New(Options{
		Policies: map[string]Policy{loginMethod: {Requests: 1, Window: time.Minute}},
		Default:  &Policy{Requests: 2, Window: time.Minute},
		Now:      clock.Now,
	})

	expectAllowed(t, l, loginMethod, "192.0.2.1", 1)
	expectRejected(t, l, loginMethod, "192.0.2.1", time.Minute)

	// Methods under the default have a bucket of their own each.
	expectAllowed(t, l, searchMethod, "192.0.2.1", 2)
	expectRejected(t, l, searchMethod, "192.0.2.1", 30*time.Second)
	expectAllowed(t, l, otherMethod, "192.0.2.1", 2)
	expectRejected(t, l, otherMethod, "192.0.2.1", 30*time.Second)

	// So does every client.
	expectAllowed(t, l, loginMethod, "192.0.2.2", 1)
	expectAllowed(t, l, searchMethod, "192.0.2.2", 2)
}

func TestMethodsWithoutAPolicyAreUnlimitedWithoutADefault(t *testing.T) {
	l := New(Options{Policies: map[string]Policy{loginMethod: {Requests: 1, Window: time.Minute}}})

	expectAllowed(t, l, searchMethod, "192.0.2.1", 1000)
	if n := l.bucketCount(); n != 0 {
		t.Fatalf("unlimited calls made %d buckets", n)
	}
}

func TestIPv4ClientsAreLimitedByAddress(t *testing.T) {
	clock := newClock()
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "192.0.2.1", 1)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Minute)
	// A neighbour in the same /24 has its own bucket.
	expectAllowed(t, l, searchMethod, "192.0.2.2", 1)
	// An IPv4-mapped IPv6 address is the IPv4 address.
	expectRejected(t, l, searchMethod, "::ffff:192.0.2.1", time.Minute)
	expectRejected(t, l, searchMethod, "::ffff:192.0.2.2", time.Minute)
	expectAllowed(t, l, searchMethod, "::ffff:192.0.2.3", 1)
	expectRejected(t, l, searchMethod, "192.0.2.3", time.Minute)
}

func TestIPv6ClientsShareABucketPerPrefix(t *testing.T) {
	clock := newClock()
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "2001:db8:1:2::1", 1)
	// Other addresses of the same /64, including ones that differ in the
	// interface identifier only, and an address with a zone.
	expectRejected(t, l, searchMethod, "2001:db8:1:2::2", time.Minute)
	expectRejected(t, l, searchMethod, "2001:db8:1:2:ffff:ffff:ffff:ffff", time.Minute)
	expectRejected(t, l, searchMethod, "2001:db8:1:2::1%eth0", time.Minute)
	// A neighbouring /64 has its own bucket.
	expectAllowed(t, l, searchMethod, "2001:db8:1:3::1", 1)
	expectRejected(t, l, searchMethod, "2001:db8:1:3::99", time.Minute)
	// IPv4 and IPv6 do not mix.
	expectAllowed(t, l, searchMethod, "192.0.2.1", 1)
}

func TestTheIPv6PrefixIsConfigurable(t *testing.T) {
	clock := newClock()
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, IPv6Prefix: 48, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "2001:db8:1:2::1", 1)
	expectRejected(t, l, searchMethod, "2001:db8:1:3::1", time.Minute)
	expectRejected(t, l, searchMethod, "2001:db8:1:ffff::1", time.Minute)
	expectAllowed(t, l, searchMethod, "2001:db8:2::1", 1)

	// A /128 limits single addresses.
	l = New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, IPv6Prefix: 128, Now: clock.Now})
	expectAllowed(t, l, searchMethod, "2001:db8:1:2::1", 1)
	expectAllowed(t, l, searchMethod, "2001:db8:1:2::2", 1)
	expectRejected(t, l, searchMethod, "2001:db8:1:2::2", time.Minute)
}

func TestCallsWithoutAnAddressShareOneBucketPerMethod(t *testing.T) {
	clock := newClock()
	l := New(Options{Default: &Policy{Requests: 2, Window: time.Minute}, Now: clock.Now})

	expectAllowed(t, l, searchMethod, "", 2)
	expectRejected(t, l, searchMethod, "", 30*time.Second)
	expectAllowed(t, l, otherMethod, "", 1)
	// Known clients are not affected.
	expectAllowed(t, l, searchMethod, "192.0.2.1", 2)
}

func TestAKeyFunctionReplacesTheAddress(t *testing.T) {
	clock := newClock()
	l := New(Options{
		Default: &Policy{Requests: 1, Window: time.Minute},
		Key:     func(info trevrpc.HandlerContext) string { return string(info.Metadata["x-api-key"]) },
		Now:     clock.Now,
	})
	callAs := func(key, addr string) error {
		info := trevrpc.HandlerContext{
			FullMethod: searchMethod,
			Metadata:   trevrpc.Metadata{"x-api-key": []byte(key)},
			RemoteAddr: netip.AddrPortFrom(netip.MustParseAddr(addr), 1),
		}
		_, _, err := l.Intercept(context.Background(), info)
		return err
	}

	if err := callAs("alice", "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
	// The same key from another address is the same client, another key from
	// the same address is another.
	if err := callAs("alice", "192.0.2.2"); err == nil {
		t.Fatal("alice's second call was allowed")
	}
	if err := callAs("bob", "192.0.2.1"); err != nil {
		t.Fatal(err)
	}
}

func TestBucketsAreBounded(t *testing.T) {
	clock := newClock()
	for _, maxBuckets := range []int{1, 5, 16, 100, 1000} {
		t.Run(fmt.Sprint(maxBuckets), func(t *testing.T) {
			l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, MaxBuckets: maxBuckets, Now: clock.Now})
			for i := range 5000 {
				addr := netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)})
				expectAllowed(t, l, searchMethod, addr.String(), 1)
			}
			if n := l.bucketCount(); n > maxBuckets {
				t.Fatalf("%d buckets, want at most %d", n, maxBuckets)
			}
			l.checkInvariants(t)
		})
	}

	// The default bound.
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, Now: clock.Now})
	for i := range 3 * DefaultMaxBuckets {
		l.take(bucketKey{method: searchMethod, custom: fmt.Sprint(i)}, l.def)
	}
	if n := l.bucketCount(); n > DefaultMaxBuckets {
		t.Fatalf("%d buckets, want at most %d", n, DefaultMaxBuckets)
	}
	l.checkInvariants(t)
}

func TestTheLeastRecentlyUsedBucketMakesRoom(t *testing.T) {
	clock := newClock()
	// One shard with room for two buckets.
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Minute}, MaxBuckets: 1, Now: clock.Now})
	l.shards[0].capacity = 2

	expectAllowed(t, l, searchMethod, "192.0.2.1", 1)
	expectAllowed(t, l, searchMethod, "192.0.2.2", 1)
	// Using the first bucket again makes the second the oldest.
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Minute)
	expectAllowed(t, l, searchMethod, "192.0.2.3", 1)
	expectRejected(t, l, searchMethod, "192.0.2.1", time.Minute)
	// The second was forgotten, so its client starts with a full bucket.
	expectAllowed(t, l, searchMethod, "192.0.2.2", 1)
	l.checkInvariants(t)
}

// checkInvariants fails the test if a shard's list and table disagree or hold
// more buckets than the shard may.
func (l *Limiter) checkInvariants(t *testing.T) {
	t.Helper()
	for i, s := range l.shards {
		s.mu.Lock()
		count := 0
		for b := s.root.next; b != &s.root; b = b.next {
			count++
			if s.buckets[b.key] != b || b.next.prev != b {
				t.Errorf("shard %d: bucket %v is not linked consistently", i, b.key)
			}
			if count > len(s.buckets) {
				break
			}
		}
		if count != len(s.buckets) || count > s.capacity {
			t.Errorf("shard %d: list has %d buckets, table %d, capacity %d", i, count, len(s.buckets), s.capacity)
		}
		s.mu.Unlock()
	}
}

func TestConcurrentCallsShareBucketsSafely(t *testing.T) {
	clock := newClock()
	const burst, clients, goroutines, perGoroutine = 50, 8, 16, 200
	l := New(Options{Default: &Policy{Requests: burst, Window: time.Hour}, MaxBuckets: 64, Now: clock.Now})

	var allowed [clients]atomic.Int64
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				client := (g + i) % clients
				if call(l, searchMethod, fmt.Sprintf("192.0.2.%d", client)) == nil {
					allowed[client].Add(1)
				}
			}
		})
	}
	wg.Wait()

	// The clock never moved, so every client got exactly its burst.
	for client := range allowed {
		if got := allowed[client].Load(); got != burst {
			t.Errorf("client %d: %d calls allowed, want %d", client, got, burst)
		}
	}
	l.checkInvariants(t)
}

func TestConcurrentCallsWithEvictionKeepTheBound(t *testing.T) {
	l := New(Options{Default: &Policy{Requests: 2, Window: time.Second}, MaxBuckets: 32})
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				_ = call(l, searchMethod, fmt.Sprintf("2001:db8:%x:%x::1", g, i))
			}
		})
	}
	wg.Wait()
	if n := l.bucketCount(); n > 32 {
		t.Fatalf("%d buckets, want at most 32", n)
	}
	l.checkInvariants(t)
}

func TestInvalidOptionsPanic(t *testing.T) {
	good := Policy{Requests: 1, Window: time.Second}
	for name, options := range map[string]Options{
		"no requests":          {Default: &Policy{Window: time.Second}},
		"negative requests":    {Default: &Policy{Requests: -1, Window: time.Second}},
		"no window":            {Default: &Policy{Requests: 1}},
		"negative window":      {Default: &Policy{Requests: 1, Window: -time.Second}},
		"window too short":     {Default: &Policy{Requests: 3, Window: 2}},
		"no requests listed":   {Policies: map[string]Policy{loginMethod: {Window: time.Second}}},
		"method not a path":    {Policies: map[string]Policy{"Login": good}},
		"method no service":    {Policies: map[string]Policy{"//Login": good}},
		"method no name":       {Policies: map[string]Policy{"/example.auth.Auth/": good}},
		"method extra path":    {Policies: map[string]Policy{"/example.auth.Auth/Login/x": good}},
		"negative IPv6 prefix": {IPv6Prefix: -1},
		"IPv6 prefix 129":      {IPv6Prefix: 129},
		"negative buckets":     {MaxBuckets: -1},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				recovered := recover()
				if message, _ := recovered.(string); !strings.HasPrefix(message, "ratelimit: ") {
					t.Fatalf("New panicked with %v, want a ratelimit message", recovered)
				}
			}()
			New(options)
		})
	}
}

func TestARejectedCallReachesTheClientWithItsRetryDelay(t *testing.T) {
	// End to end: a real server, a real client, and the client reads the delay.
	l := New(Options{Policies: map[string]Policy{echoMethod: {Requests: 2, Window: time.Hour}}})
	server := trevrpc.NewServer()
	server.SetInterceptors(l)
	var handled atomic.Int64
	trevrpc.RegisterUnary(server, echoMethod, func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} },
		func(_ context.Context, request *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
			handled.Add(1)
			return request, nil
		})
	channel := serve(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	newReply := func() *wrapperspb.StringValue { return &wrapperspb.StringValue{} }
	for range 2 {
		if _, err := trevrpc.Unary(ctx, channel, echoMethod, wrapperspb.String("hi"), newReply); err != nil {
			t.Fatalf("Echo: %v", err)
		}
	}
	_, err := trevrpc.Unary(ctx, channel, echoMethod, wrapperspb.String("hi"), newReply)

	status := trevrpc.StatusFromError(err)
	if status.Code != trevrpc.CodeResourceExhausted || !strings.HasPrefix(status.Message, "rate limit exceeded: 2 requests per 1h0m0s") {
		t.Fatalf("third call: %v", err)
	}
	// One token takes 30 minutes to refill; some of that may have passed.
	if delay, ok := trevrpc.RetryDelay(err); !ok || delay <= 29*time.Minute || delay > 30*time.Minute {
		t.Fatalf("RetryDelay = %v, %v, want about 30m", delay, ok)
	}
	if n := handled.Load(); n != 2 {
		t.Fatalf("handler ran %d times, want 2", n)
	}
	// The bucket belongs to the client's address.
	if n := l.bucketCount(); n != 1 {
		t.Fatalf("%d buckets, want 1", n)
	}
}

const echoMethod = "/example.Echo/Echo"

// serve serves server on a local listener until the test ends and returns a
// channel to it.
func serve(t *testing.T, server *trevrpc.Server) *trevrpc.Channel {
	t.Helper()
	certificate, err := trevrpc.NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM, err := certificate.PEM()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("PEM holds no certificates")
	}
	listener, err := trevrpc.Listen("127.0.0.1:0", server, trevrpc.ListenOptions{TLSConfig: certificate.TLSConfig()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- listener.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("Serve: %v", err)
		}
	})

	channel, err := trevrpc.Dial(ctx, listener.Addr().String(), trevrpc.DialOptions{
		TLSConfig: &tls.Config{RootCAs: roots, ServerName: "localhost"},
		Transport: trevrpc.TransportQUICOnly,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = channel.Close() })
	return channel
}
