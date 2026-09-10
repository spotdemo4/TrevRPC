package providerabi

import "unsafe"

const (
	StructVersion1 uint32 = 1
	EngineKind     uint32 = 1
	TransportKind  uint32 = 2
)

// Descriptor carries opaque C-owned provider state between first-party modules.
// It is internal so pointer-bearing fields cannot leak into the public Go API.
type Descriptor struct {
	StructSize    uint32
	StructVersion uint32
	Kind          uint32
	Reserved      uint32
	Operations    unsafe.Pointer
	Context       unsafe.Pointer
}
