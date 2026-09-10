//go:build cgo && linux && amd64

package consumer

import (
	"testing"

	msquic "trev.zip/llc/trevrpc/trevrpc-c/provider/msquic/v2"
)

func TestConsumerAdoptsProviderWithoutRegistration(t *testing.T) {
	engine, err := msquic.NewEngine(101)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	transport, err := msquic.NewTransport(202)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if engine.Identity() != 101 || transport.Identity() != 202 {
		t.Fatalf("unexpected provider identities: engine=%d transport=%d", engine.Identity(), transport.Identity())
	}
}
