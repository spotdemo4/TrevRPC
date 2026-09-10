package cabi

import (
	"testing"

	"trev.zip/llc/trevrpc/trevrpc-c/internal/providerabi"
)

func TestRejectsInvalidDescriptorSizeAndVersion(t *testing.T) {
	for _, descriptor := range []providerabi.Descriptor{
		{StructSize: 1, StructVersion: providerabi.StructVersion1, Kind: providerabi.EngineKind},
		{StructSize: 64, StructVersion: 2, Kind: providerabi.EngineKind},
	} {
		if _, err := AdoptEngine(descriptor); err == nil {
			t.Fatalf("AdoptEngine(%+v) unexpectedly succeeded", descriptor)
		}
	}
}
