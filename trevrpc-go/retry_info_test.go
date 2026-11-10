package trevrpc

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestRetryInfoGoldenEncoding(t *testing.T) {
	for _, test := range []struct {
		name  string
		delay time.Duration
		hex   string
	}{
		// RetryInfo{retry_delay: Duration{seconds: 1, nanos: 2}}, the same bytes
		// every runtime's tests check.
		{"seconds and nanos", time.Second + 2, "0a0408011002"},
		{"seconds only", 30 * time.Second, "0a02081e"},
		{"nanos only", 500 * time.Millisecond, "0a061080cab5ee01"},
		{"seconds and fraction", 1500 * time.Millisecond, "0a0808011080cab5ee01"},
		{"zero", 0, "0a00"},
		{"negative is zero", -time.Second, "0a00"},
	} {
		t.Run(test.name, func(t *testing.T) {
			status := ResourceExhausted("slow down").WithRetryDelay(test.delay)
			if len(status.Details) != 1 || status.Details[0].Type != "google.rpc.RetryInfo" {
				t.Fatalf("details = %+v", status.Details)
			}
			if got := hex.EncodeToString(status.Details[0].Value); got != test.hex {
				t.Fatalf("encoding = %s, want %s", got, test.hex)
			}
			want := max(test.delay, 0)
			if got, ok := status.RetryDelay(); !ok || got != want {
				t.Fatalf("RetryDelay() = %v, %v, want %v", got, ok, want)
			}
		})
	}
}

func TestRetryDelayDecodesWhatOtherEncodersWrite(t *testing.T) {
	for _, test := range []struct {
		name  string
		hex   string
		delay time.Duration
		ok    bool
	}{
		{"seconds and nanos", "0a0408011002", time.Second + 2, true},
		{"unknown fields are skipped", "1a03616263" + "0a0408011002" + "2000", time.Second + 2, true},
		{"unknown duration fields are skipped", "0a06" + "1a00" + "0801" + "1002", time.Second + 2, true},
		{"the last value wins", "0a02080a" + "0a02080b", 11 * time.Second, true},
		{"too long for a Duration", "0a0a08ffffffffffffffff7f", math.MaxInt64, true},
		{"empty", "", 0, false},
		{"no delay", "1a03616263", 0, false},
		{"truncated tag", "0a", 0, false},
		{"truncated delay", "0a04" + "0801", 0, false},
		{"truncated duration varint", "0a01" + "08", 0, false},
		{"wrong wire type for the delay", "0801", 0, false},
		{"wrong wire type for seconds", "0a050d00000000", 0, false},
		{"negative seconds", "0a0b" + "08ffffffffffffffffff01", 0, false},
		{"negative nanos", "0a0d" + "0801" + "10ffffffffffffffffff01", 0, false},
		{"nanos of a second", "0a08" + "0801" + "108094ebdc03", 0, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, err := hex.DecodeString(test.hex)
			if err != nil {
				t.Fatal(err)
			}
			delay, ok := parseRetryInfo(value)
			if delay != test.delay || ok != test.ok {
				t.Fatalf("parseRetryInfo(%s) = %v, %v, want %v, %v", test.hex, delay, ok, test.delay, test.ok)
			}
		})
	}
}

func TestStatusRetryDelayIgnoresMalformedDetails(t *testing.T) {
	status := ResourceExhausted("slow down").WithDetails(
		Detail{Type: "google.rpc.RetryInfo", Value: []byte{0x0a, 0x09}},
		Detail{Type: "other.Type", Value: []byte{0x0a, 0x02, 0x08, 0x09}},
		Detail{Type: "google.rpc.RetryInfo", Value: []byte{0x0a, 0x02, 0x08, 0x03}},
	)
	if delay, ok := status.RetryDelay(); !ok || delay != 3*time.Second {
		t.Fatalf("RetryDelay() = %v, %v, want 3s", delay, ok)
	}
	if delay, ok := status.WithDetails().RetryDelay(); ok {
		t.Fatalf("RetryDelay() without details = %v, %v", delay, ok)
	}
	var none *Status
	if _, ok := none.RetryDelay(); ok {
		t.Fatal("a nil status has a retry delay")
	}
}

func TestWithRetryDelayKeepsOtherDetailsAndReplacesItself(t *testing.T) {
	other := Detail{Type: "x.Y", Value: []byte{1}}
	original := ResourceExhausted("slow down").WithMetadata(Metadata{"k": []byte("v")}).WithDetails(other)
	first := original.WithRetryDelay(time.Second)
	second := first.WithRetryDelay(5 * time.Second)

	if len(original.Details) != 1 {
		t.Fatalf("WithRetryDelay changed its receiver: %+v", original.Details)
	}
	if len(second.Details) != 2 || second.Details[0].Type != "x.Y" || second.Details[1].Type != "google.rpc.RetryInfo" {
		t.Fatalf("details = %+v", second.Details)
	}
	if delay, _ := first.RetryDelay(); delay != time.Second {
		t.Fatalf("first delay = %v", delay)
	}
	if delay, _ := second.RetryDelay(); delay != 5*time.Second {
		t.Fatalf("second delay = %v", delay)
	}
	if second.Code != CodeResourceExhausted || second.Message != "slow down" || string(second.Metadata["k"]) != "v" {
		t.Fatalf("status = %+v", second)
	}
	var none *Status
	if delay, ok := none.WithRetryDelay(time.Second).RetryDelay(); !ok || delay != time.Second {
		t.Fatalf("nil status RetryDelay() = %v, %v", delay, ok)
	}
}

func TestRetryDelayFindsAStatusInTheErrorChain(t *testing.T) {
	err := fmt.Errorf("call failed: %w", ResourceExhausted("slow down").WithRetryDelay(40*time.Second))
	if delay, ok := RetryDelay(err); !ok || delay != 40*time.Second {
		t.Fatalf("RetryDelay() = %v, %v, want 40s", delay, ok)
	}
	if _, ok := RetryDelay(ResourceExhausted("no hint")); ok {
		t.Fatal("a status without a detail has a retry delay")
	}
	if _, ok := RetryDelay(errors.New("plain")); ok {
		t.Fatal("a plain error has a retry delay")
	}
	if _, ok := RetryDelay(nil); ok {
		t.Fatal("a nil error has a retry delay")
	}
}

func TestRetryDelaySurvivesTheWire(t *testing.T) {
	status := ResourceExhausted("slow down").WithRetryDelay(1500 * time.Millisecond)
	decoded := statusFromWire(status.wire())
	if delay, ok := decoded.RetryDelay(); !ok || delay != 1500*time.Millisecond {
		t.Fatalf("RetryDelay() = %v, %v", delay, ok)
	}
}
