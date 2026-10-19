package trevrpc

import (
	"math"
	"testing"
)

func TestStreamLimitsLeaveRoomForReturningCreditOnlyWhereItLags(t *testing.T) {
	options := ServerOptions{MaxConcurrentStreamsPerConnection: 10}
	if got := serverStreamLimit(options); got != 20 {
		t.Errorf("serverStreamLimit = %d, want 20: QUIC, WebTransport, and QMux return a stream's credit a round trip after it closes", got)
	}
	if got := http2StreamLimit(options); got != 11 {
		t.Errorf("http2StreamLimit = %d, want 11: HTTP/2 frees streams at once and needs only a rejection slot", got)
	}
}

func TestStreamLimitsSaturate(t *testing.T) {
	options := ServerOptions{MaxConcurrentStreamsPerConnection: math.MaxInt}
	if got := serverStreamLimit(options); got != maxStreamCount {
		t.Errorf("serverStreamLimit = %d, want the QUIC maximum %d", got, int64(maxStreamCount))
	}
	if got := http2StreamLimit(options); got != math.MaxUint32 {
		t.Errorf("http2StreamLimit = %d, want the HTTP/2 maximum %d", got, uint32(math.MaxUint32))
	}
}
