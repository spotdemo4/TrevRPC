package trevrpc

import (
	"net"
	"sync"
	"time"
)

// addressValidator returns the quic.Transport VerifySourceAddress callback for
// options, which decides whether a connection attempt from an address QUIC has
// not validated yet must first prove it with a Retry, or nil to never ask.
func addressValidator(options ServerOptions) func(net.Addr) bool {
	switch {
	case options.ValidateAllAddresses:
		return func(net.Addr) bool { return true }
	case options.AddressValidationRate > 0:
		return newRetryLimiter(options.AddressValidationRate, time.Now).validate
	default:
		return nil
	}
}

// retryLimiter lets rate connection attempts from unvalidated addresses a
// second through without a Retry, and bursts of up to a second's worth, as a
// token bucket. The attempts past it are asked to validate their address.
type retryLimiter struct {
	now  func() time.Time
	rate float64

	mu     sync.Mutex
	tokens float64
	last   time.Time
}

func newRetryLimiter(rate int, now func() time.Time) *retryLimiter {
	return &retryLimiter{now: now, rate: float64(rate), tokens: float64(rate), last: now()}
}

// validate reports whether the attempt must validate its address.
func (l *retryLimiter) validate(net.Addr) bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if elapsed := now.Sub(l.last); elapsed > 0 {
		l.tokens = min(l.rate, l.tokens+elapsed.Seconds()*l.rate)
		l.last = now
	}
	if l.tokens < 1 {
		return true
	}
	l.tokens--
	return false
}
