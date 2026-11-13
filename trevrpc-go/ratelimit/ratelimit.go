// Package ratelimit limits how often clients may call the methods of a
// trevrpc.Server, with a token bucket per method and client.
//
// A Limiter is a trevrpc.Interceptor, not an authorizer: the server runs an
// authorizer again for a call the client replays after the server refused its
// optimistic stream compression, so an authorizer would charge such a call
// two tokens, while an interceptor runs once per handler invocation.
//
//	limiter := ratelimit.New(ratelimit.Options{
//		Policies: map[string]ratelimit.Policy{
//			"/example.auth.Auth/Login": {Requests: 5, Window: time.Minute},
//		},
//		Default: &ratelimit.Policy{Requests: 100, Window: time.Second},
//	})
//	server.SetInterceptors(limiter)
//
// A call that finds its bucket empty fails with ResourceExhausted and a
// google.rpc.RetryInfo status detail that says when a token is next
// available; every TrevRPC client reads it (see trevrpc.RetryDelay).
package ratelimit

import (
	"context"
	"fmt"
	"hash/maphash"
	"net/netip"
	"strings"
	"sync"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// Defaults of Options fields left at zero.
const (
	// DefaultIPv6Prefix is the length of the IPv6 prefix that shares a bucket:
	// the /64 that most residential and cloud networks give a customer.
	DefaultIPv6Prefix = 64
	// DefaultMaxBuckets bounds the memory a Limiter holds.
	DefaultMaxBuckets = 10_000
)

// maxShards is the number of independently locked bucket tables a Limiter
// splits its buckets over, at most.
const maxShards = 16

// Policy is a token bucket: a client may make Requests calls at once, and the
// bucket refills at Requests per Window, one token every Window/Requests.
type Policy struct {
	// Requests is the size of the bucket, which is also the burst a client
	// that has been idle may send. It must be positive.
	Requests int
	// Window is the time in which an empty bucket refills completely. It must
	// be positive, and at least one nanosecond for each request.
	Window time.Duration
}

// Options configure a Limiter.
type Options struct {
	// Policies limit the methods they name, by full method name, such as
	// "/example.auth.Auth/Login". Every method has a bucket of its own for
	// each client, even when several methods have the same policy.
	Policies map[string]Policy
	// Default limits every method that Policies does not name, with a bucket of
	// its own for each method and client. Nil leaves those methods unlimited.
	Default *Policy
	// Key names the client a call is counted against, in place of the
	// default, the client's network address. Use it to limit by identity, such
	// as the user an authorizer verified or an API key in the metadata. The
	// key is kept as long as the bucket is, so it must not be unbounded text
	// the client chooses without a check. When Key is set IPv6Prefix is not
	// used.
	Key func(info trevrpc.HandlerContext) string
	// IPv6Prefix is the length in bits of the IPv6 prefix whose addresses share
	// a bucket, from 1 to 128. Zero means DefaultIPv6Prefix. A client with a
	// /64 can use any of its 2^64 addresses, so limiting single addresses
	// would not limit it. IPv4 addresses, with IPv4-mapped IPv6 addresses
	// counted as IPv4, always have a bucket of their own.
	IPv6Prefix int
	// MaxBuckets is the number of buckets a Limiter keeps at most. When it is
	// reached the bucket that was used longest ago makes room for a new one,
	// which starts full. Zero means DefaultMaxBuckets.
	MaxBuckets int
	// Now returns the current time. Nil means time.Now. Tests inject a clock.
	Now func() time.Time
}

// Limiter limits calls with a token bucket for each method and client. It is
// a trevrpc.Interceptor, and safe for concurrent use.
//
// By default a client is its address, HandlerContext.RemoteAddr, which names
// the real client behind a proxy only when the server trusts the proxy to say
// so (ListenOptions.TrustedTCPProxies and TrustedUDPProxies); otherwise
// every client behind it shares the proxy's address and one set of buckets.
// A call whose address is not known shares one bucket per method with the
// others of its kind.
//
// Buckets live in a table that holds at most Options.MaxBuckets of them,
// split over shards by key hash with a lock of its own and a
// least-recently-used order each. A new client starts with a full bucket, so
// forgetting a bucket never refuses a call, but a client that makes a bucket
// forgotten while it is empty, such as by calling from many networks, gets a
// full one: set the bucket count large enough for the clients you expect, and
// limit abusive networks at the edge as well.
type Limiter struct {
	policies map[string]*policy
	def      *policy
	key      func(trevrpc.HandlerContext) string
	ipv6Bits int
	now      func() time.Time
	// epoch is the time bucket state is measured from; time.Time subtraction
	// uses the monotonic clock, so a wall clock step does not change buckets.
	epoch  time.Time
	seed   maphash.Seed
	shards []*shard
}

// policy is a Policy in the form the buckets use.
type policy struct {
	// interval is the time one token takes to refill, in nanoseconds.
	interval int64
	// burst is the time a full bucket takes to refill: interval * Requests.
	burst int64
	// message is the status message of a rejected call.
	message string
}

// bucketKey identifies a bucket: a method, and either the client's network or
// the text of Options.Key. It is comparable, so a call looks its bucket up
// without allocating.
type bucketKey struct {
	method string
	addr   netip.Addr
	custom string
}

// bucket is the state of a bucket, linked into the recently-used list of its
// shard.
type bucket struct {
	key bucketKey
	// tat is the theoretical arrival time of the generic cell rate algorithm:
	// the time at which the bucket is full again. A bucket with tat <= now is
	// full; one that holds n tokens has tat = now + (Requests-n) * interval.
	tat        int64
	prev, next *bucket
}

// shard is one lock and table of buckets.
type shard struct {
	mu       sync.Mutex
	buckets  map[bucketKey]*bucket
	capacity int
	// root is the list's sentinel: root.next is the most recently used
	// bucket and root.prev the least recently used.
	root bucket
}

// New returns a Limiter for options. It panics when options are invalid: a
// policy with no requests or no window, a method name that is not
// "/<service>/<method>", or an IPv6Prefix or MaxBuckets out of range.
func New(options Options) *Limiter {
	l := &Limiter{
		policies: make(map[string]*policy, len(options.Policies)),
		key:      options.Key,
		ipv6Bits: options.IPv6Prefix,
		now:      options.Now,
		seed:     maphash.MakeSeed(),
	}
	for method, p := range options.Policies {
		if !validMethod(method) {
			panic(fmt.Sprintf("ratelimit: policy method %q must be /<service>/<method>", method))
		}
		l.policies[method] = newPolicy(method, p)
	}
	if options.Default != nil {
		l.def = newPolicy("default", *options.Default)
	}
	if l.ipv6Bits == 0 {
		l.ipv6Bits = DefaultIPv6Prefix
	}
	if l.ipv6Bits < 1 || l.ipv6Bits > 128 {
		panic("ratelimit: IPv6Prefix must be between 1 and 128")
	}
	if l.now == nil {
		l.now = time.Now
	}
	l.epoch = l.now()

	maxBuckets := options.MaxBuckets
	if maxBuckets == 0 {
		maxBuckets = DefaultMaxBuckets
	}
	if maxBuckets < 0 {
		panic("ratelimit: MaxBuckets must not be negative")
	}
	// Split the capacity so that the shards hold exactly maxBuckets together.
	shards := min(maxShards, maxBuckets)
	l.shards = make([]*shard, shards)
	for i := range l.shards {
		capacity := maxBuckets / shards
		if i < maxBuckets%shards {
			capacity++
		}
		s := &shard{buckets: make(map[bucketKey]*bucket), capacity: capacity}
		s.root.prev, s.root.next = &s.root, &s.root
		l.shards[i] = s
	}
	return l
}

// validMethod reports whether method is "/<service>/<method>".
func validMethod(method string) bool {
	service, name, ok := strings.Cut(strings.TrimPrefix(method, "/"), "/")
	return strings.HasPrefix(method, "/") && ok && service != "" && name != "" && !strings.Contains(name, "/")
}

func newPolicy(name string, p Policy) *policy {
	if p.Requests <= 0 {
		panic(fmt.Sprintf("ratelimit: policy %s: Requests must be positive", name))
	}
	if p.Window <= 0 {
		panic(fmt.Sprintf("ratelimit: policy %s: Window must be positive", name))
	}
	interval := int64(p.Window) / int64(p.Requests)
	if interval < 1 {
		panic(fmt.Sprintf("ratelimit: policy %s: Window must be at least a nanosecond for each request", name))
	}
	return &policy{
		interval: interval,
		burst:    interval * int64(p.Requests),
		message:  fmt.Sprintf("rate limit exceeded: %d requests per %s", p.Requests, p.Window),
	}
}

// Intercept counts the call against the bucket of its method and client, and
// fails it with ResourceExhausted and a RetryInfo detail when the bucket is
// empty. A call to a method without a policy, when there is no default, costs
// one map lookup.
func (l *Limiter) Intercept(ctx context.Context, info trevrpc.HandlerContext) (context.Context, func(trevrpc.RPCFinished), error) {
	p := l.policies[info.FullMethod]
	if p == nil {
		if p = l.def; p == nil {
			return ctx, nil, nil
		}
	}

	key := bucketKey{method: info.FullMethod}
	if l.key != nil {
		key.custom = l.key(info)
	} else {
		key.addr = l.network(info.RemoteAddr.Addr())
	}
	if wait := l.take(key, p); wait > 0 {
		return ctx, nil, rejection(p, wait)
	}
	return ctx, nil, nil
}

// network returns the address that names addr's bucket: an IPv4 address (an
// IPv4-mapped IPv6 address as IPv4) as is, an IPv6 address masked to the
// configured prefix, and the zero Addr for an unknown address.
func (l *Limiter) network(addr netip.Addr) netip.Addr {
	if !addr.IsValid() {
		return netip.Addr{}
	}
	addr = addr.Unmap().WithZone("")
	if addr.Is4() {
		return addr
	}
	prefix, err := addr.Prefix(l.ipv6Bits)
	if err != nil {
		return netip.Addr{}
	}
	return prefix.Addr()
}

// take removes a token from the bucket of key and returns zero, or leaves the
// bucket as it is and returns how long until a token is available.
func (l *Limiter) take(key bucketKey, p *policy) time.Duration {
	now := int64(l.now().Sub(l.epoch))
	s := l.shards[maphash.Comparable(l.seed, key)%uint64(len(l.shards))]
	s.mu.Lock()
	defer s.mu.Unlock()

	b := s.buckets[key]
	switch {
	case b != nil:
		s.moveToFront(b)
	case len(s.buckets) < s.capacity:
		b = &bucket{key: key, tat: now}
		s.buckets[key] = b
		s.pushFront(b)
	default:
		// Reuse the least recently used bucket, which has usually refilled.
		b = s.root.prev
		delete(s.buckets, b.key)
		b.key, b.tat = key, now
		s.buckets[key] = b
		s.moveToFront(b)
	}

	// The generic cell rate algorithm: a call advances the bucket's full time
	// by one interval, and is allowed unless that leaves the bucket more than
	// full-time-to-empty ahead of now.
	tat := max(b.tat, now) + p.interval
	if allowAt := tat - p.burst; allowAt > now {
		return time.Duration(allowAt - now)
	}
	b.tat = tat
	return 0
}

// rejection returns the status of a call that has to wait. The delay is
// rounded up to a whole millisecond, which is what JavaScript clients count
// in, so a client that waits it never retries too soon.
func rejection(p *policy, wait time.Duration) error {
	delay := wait.Truncate(time.Millisecond)
	if delay < wait {
		delay += time.Millisecond
	}
	return &trevrpc.Status{Code: trevrpc.CodeResourceExhausted, Message: p.message, Details: []trevrpc.Detail{trevrpc.RetryInfoDetail(delay)}}
}

func (s *shard) pushFront(b *bucket) {
	b.prev, b.next = &s.root, s.root.next
	b.prev.next, b.next.prev = b, b
}

func (s *shard) moveToFront(b *bucket) {
	if s.root.next == b {
		return
	}
	b.prev.next, b.next.prev = b.next, b.prev
	s.pushFront(b)
}

var _ trevrpc.Interceptor = (*Limiter)(nil)
