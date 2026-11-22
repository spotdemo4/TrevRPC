package trevrpc

import (
	"testing"
	"time"
)

func TestTCPUserTimeoutDefaults(t *testing.T) {
	for _, tc := range []struct {
		name              string
		timeout, fallback time.Duration
		disabled          bool
		want              time.Duration
	}{
		{"zero takes the fallback", 0, 30 * time.Second, false, 30 * time.Second},
		{"an explicit value wins", 5 * time.Second, 30 * time.Second, false, 5 * time.Second},
		{"disabled means none", 5 * time.Second, 30 * time.Second, true, 0},
		{"no fallback means none", 0, 0, false, 0},
	} {
		got, err := resolveTCPUserTimeout("TCPUserTimeout", tc.timeout, tc.fallback, tc.disabled)
		if err != nil || got != tc.want {
			t.Errorf("%s: got %v, %v; want %v", tc.name, got, err, tc.want)
		}
	}
}

func TestNegativeTCPUserTimeoutIsRejected(t *testing.T) {
	pki := newTestPKI(t)
	_, err := Listen("127.0.0.1:0", NewServer(), ListenOptions{TLSConfig: pki.server, TCPUserTimeout: -time.Second})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("Listen: error %v, want InvalidArgument", err)
	}
	_, err = Dial(t.Context(), "127.0.0.1:1", DialOptions{TCPUserTimeout: -time.Second})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("Dial: error %v, want InvalidArgument", err)
	}
}
