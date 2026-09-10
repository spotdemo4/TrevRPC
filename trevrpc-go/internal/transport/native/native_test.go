package native

import (
	"errors"
	"testing"
)

func TestBuildAvailability(t *testing.T) {
	if Available() {
		return
	}

	if _, err := NewEngine(nil, EngineConfig{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewEngine() error = %v, want ErrUnavailable", err)
	}
	if _, err := NewTransport(nil, EngineConfig{}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewTransport() error = %v, want ErrUnavailable", err)
	}
}
