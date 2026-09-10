package providerabi

import "unsafe"

const StructVersion1 uint32 = 1

type EngineHost struct {
	StructSize    uint32
	StructVersion uint32
	Operations    unsafe.Pointer
}

type EngineDescriptor struct {
	StructSize    uint32
	StructVersion uint32
	Operations    unsafe.Pointer
	Context       unsafe.Pointer
	OwnerCookie   uint64
	Reserved      [5]uint64
}

func NewEngineDescriptor(operations, context unsafe.Pointer, ownerCookie uint64) EngineDescriptor {
	return EngineDescriptor{
		StructSize:    uint32(unsafe.Sizeof(EngineDescriptor{})),
		StructVersion: StructVersion1,
		Operations:    operations,
		Context:       context,
		OwnerCookie:   ownerCookie,
	}
}

type TransportDescriptor struct {
	StructSize    uint32
	StructVersion uint32
	Operations    unsafe.Pointer
	Context       unsafe.Pointer
	Reserved      [6]uint64
}

func NewTransportDescriptor(operations, context unsafe.Pointer) TransportDescriptor {
	return TransportDescriptor{
		StructSize:    uint32(unsafe.Sizeof(TransportDescriptor{})),
		StructVersion: StructVersion1,
		Operations:    operations,
		Context:       context,
	}
}
