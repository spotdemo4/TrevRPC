package trevrpc

import (
	"math"
	"math/bits"
	"testing"
)

func TestStreamLimitsLeaveRoomForReturningCreditOnlyWhereItLags(t *testing.T) {
	options := ServerOptions{MaxConcurrentStreamsPerConnection: 10}
	if got := serverStreamLimit(options); got != 40 {
		t.Errorf("serverStreamLimit = %d, want 40: QUIC, WebTransport, and QMux return a stream's credit two round trips after it opens", got)
	}
	if got := http2StreamLimit(options); got != 11 {
		t.Errorf("http2StreamLimit = %d, want 11: HTTP/2 frees streams at once and needs only a rejection slot", got)
	}
}

func TestStreamLimitsSaturate(t *testing.T) {
	options := ServerOptions{MaxConcurrentStreamsPerConnection: math.MaxInt}
	// A 32-bit int stays below the QUIC maximum even with spare credit.
	hi, product := bits.Mul64(uint64(math.MaxInt), streamCreditFactor)
	if hi != 0 {
		product = maxStreamCount
	}
	wantStreams := int64(min(product, maxStreamCount))
	if got := serverStreamLimit(options); got != wantStreams {
		t.Errorf("serverStreamLimit = %d, want %d, at most the QUIC maximum", got, wantStreams)
	}
	wantHTTP2 := int(min(uint64(math.MaxUint32), math.MaxInt))
	if got := http2StreamLimit(options); got != wantHTTP2 {
		t.Errorf("http2StreamLimit = %d, want the HTTP/2 maximum %d that fits in int", got, wantHTTP2)
	}
}
