package native

import (
	"errors"
	"testing"
)

func TestBuildAvailability(t *testing.T) {
	if Available() {
		return
	}

	if _, err := CheckABI(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckABI() error = %v, want ErrUnavailable", err)
	}
	if _, err := CheckTransportABI(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("CheckTransportABI() error = %v, want ErrUnavailable", err)
	}
	if _, err := NewTransport(EngineConfig{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewTransport() error = %v, want ErrUnavailable", err)
	}
	if _, err := Open(); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Open() error = %v, want ErrUnavailable", err)
	}
}
