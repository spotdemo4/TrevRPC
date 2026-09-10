//go:build cgo && linux && amd64

package msquic

import "testing"

func TestStaticArchiveProviderCreatesIndependentRuntimes(t *testing.T) {
	engineA, err := NewEngine(11)
	if err != nil {
		t.Fatal(err)
	}
	defer engineA.Close()
	engineB, err := NewEngine(22)
	if err != nil {
		t.Fatal(err)
	}
	defer engineB.Close()
	transport, err := NewTransport(33)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	if engineA.Identity() != 11 || engineB.Identity() != 22 || transport.Identity() != 33 {
		t.Fatalf("unexpected identities: %d %d %d", engineA.Identity(), engineB.Identity(), transport.Identity())
	}
}

func TestStaticArchiveProviderRejectsDescriptorABIMismatches(t *testing.T) {
	for _, mutation := range []uint32{1, 2, 3, 4} {
		if _, err := newEngine(44, mutation); err == nil {
			t.Fatalf("mutation %d unexpectedly succeeded", mutation)
		}
	}
}
