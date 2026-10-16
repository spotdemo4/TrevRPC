package trevrpc

import (
	"fmt"
	"strings"
)

const (
	// ALPN is the QUIC application protocol negotiated by TrevRPC peers.
	ALPN = "trevrpc/1"
	// wireVersion is the current TrevRPC wire protocol version.
	wireVersion = uint32(1)

	// maxMetadataEntries is the maximum number of metadata entries per message.
	maxMetadataEntries = 64
	// maxMetadataKeyLen is the maximum metadata key length in bytes.
	maxMetadataKeyLen = 128
	// maxMetadataValueLen is the maximum metadata value length in bytes.
	maxMetadataValueLen = 8 * 1024
	// maxMetadataTotalSize is the maximum combined metadata key and value size in bytes.
	maxMetadataTotalSize = 64 * 1024

	// reservedMetadataPrefix is reserved for runtime-owned metadata keys.
	reservedMetadataPrefix = "trevrpc-"
)

// Metadata stores request or response metadata as byte values.
//
// Keys are non-empty lowercase ASCII (letters, digits, '.', '_' and '-') of at
// most 128 bytes and must not start with the reserved prefix "trevrpc-";
// NormalizeMetadataKey lowercases a key. A message carries at most 64 entries,
// each value is at most 8 KiB, and keys and values total at most 64 KiB.
type Metadata map[string][]byte

// RpcKind identifies the shape of an RPC.
type RpcKind int32

const (
	// RpcKindUnary is a single request and a single response.
	RpcKindUnary RpcKind = 0
	// RpcKindClientStreaming is a request stream and a single response.
	RpcKindClientStreaming RpcKind = 1
	// RpcKindServerStreaming is a single request and a response stream.
	RpcKindServerStreaming RpcKind = 2
	// RpcKindBidirectionalStreaming is a request stream and a response stream.
	RpcKindBidirectionalStreaming RpcKind = 3
)

// wireFrameKind identifies whether a streaming frame carries a message or terminal status.
type wireFrameKind int32

const (
	wireFrameMessage wireFrameKind = 0
	wireFrameStatus  wireFrameKind = 1
)

// wireRequest is the wire representation of an RPC request.
type wireRequest struct {
	Service      string   `protobuf:"bytes,1,opt,name=service,proto3" json:"service,omitempty"`
	Method       string   `protobuf:"bytes,2,opt,name=method,proto3" json:"method,omitempty"`
	Body         []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata     Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Kind         RpcKind  `protobuf:"varint,5,opt,name=kind,proto3,enum=trevrpc.RpcKind" json:"kind,omitempty"`
	Version      uint32   `protobuf:"varint,6,opt,name=version,proto3" json:"version,omitempty"`
	TimeoutNanos uint64   `protobuf:"varint,7,opt,name=timeout_nanos,json=timeoutNanos,proto3" json:"timeout_nanos,omitempty"`
}

// newWireRequest creates a unary RPC request with the current wire version.
func newWireRequest(service, method string, body []byte) *wireRequest {
	return &wireRequest{
		Service:  service,
		Method:   method,
		Body:     body,
		Metadata: Metadata{},
		Kind:     RpcKindUnary,
		Version:  wireVersion,
	}
}

// Reset clears the request for protobuf compatibility.
func (m *wireRequest) Reset() { *m = wireRequest{} }

// String returns the request as a debug string for protobuf compatibility.
func (m *wireRequest) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireRequest as a protobuf message.
func (*wireRequest) ProtoMessage() {}

// validateProtocol validates the request wire version and RPC kind.
func (m *wireRequest) validateProtocol() error {
	if m.Version != wireVersion {
		return FailedPrecondition(fmt.Sprintf("unsupported TrevRPC wire version %d; expected %d", m.Version, wireVersion))
	}

	if !m.Kind.isValid() {
		return InvalidArgument(fmt.Sprintf("unsupported TrevRPC RPC kind %d", m.Kind))
	}

	return nil
}

// isValid reports whether k is a supported RPC kind.
func (k RpcKind) isValid() bool {
	switch k {
	case RpcKindUnary, RpcKindClientStreaming, RpcKindServerStreaming, RpcKindBidirectionalStreaming:
		return true
	default:
		return false
	}
}

// rpcKind returns the request RPC kind, defaulting to unary for invalid values.
func (m *wireRequest) rpcKind() RpcKind {
	if m.Kind.isValid() {
		return m.Kind
	}

	return RpcKindUnary
}

// wireResponse is the wire representation of an RPC response.
type wireResponse struct {
	Status   uint32   `protobuf:"varint,1,opt,name=status,proto3" json:"status,omitempty"`
	Message  string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

// Reset clears the response for protobuf compatibility.
func (m *wireResponse) Reset() { *m = wireResponse{} }

// String returns the response as a debug string for protobuf compatibility.
func (m *wireResponse) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireResponse as a protobuf message.
func (*wireResponse) ProtoMessage() {}

// wireStreamFrame is the wire representation of one streaming RPC frame.
type wireStreamFrame struct {
	Kind     wireFrameKind `protobuf:"varint,1,opt,name=kind,proto3,enum=trevrpc.RpcStreamFrameKind" json:"kind,omitempty"`
	Status   uint32        `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
	Message  string        `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte        `protobuf:"bytes,4,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata      `protobuf:"bytes,5,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

// messageFrame creates a stream message frame carrying a protobuf body.
func messageFrame(body []byte) *wireStreamFrame {
	return &wireStreamFrame{
		Kind:     wireFrameMessage,
		Status:   uint32(CodeOK),
		Body:     body,
		Metadata: Metadata{},
	}
}

// statusFrame creates a terminal status frame without metadata.
func statusFrame(status *Status) *wireStreamFrame {
	if status == nil || len(status.Metadata) == 0 {
		return statusFrameWithMetadata(status, Metadata{})
	}

	return statusFrameWithMetadata(status, status.Metadata)
}

// statusFrameWithMetadata creates a terminal status frame with metadata.
func statusFrameWithMetadata(status *Status, metadata Metadata) *wireStreamFrame {
	if status == nil {
		status = OK()
	}

	return &wireStreamFrame{
		Kind:     wireFrameStatus,
		Status:   uint32(status.Code),
		Message:  status.Message,
		Metadata: metadata,
	}
}

// Reset clears the stream frame for protobuf compatibility.
func (m *wireStreamFrame) Reset() { *m = wireStreamFrame{} }

// String returns the stream frame as a debug string for protobuf compatibility.
func (m *wireStreamFrame) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireStreamFrame as a protobuf message.
func (*wireStreamFrame) ProtoMessage() {}

// NormalizeMetadataKey lowercases a metadata key, as the runtime requires of
// keys in Metadata maps it receives.
func NormalizeMetadataKey(key string) string {
	return strings.ToLower(key)
}

// validateMetadata validates metadata key syntax, value sizes, and total metadata limits.
func validateMetadata(metadata Metadata) error {
	if len(metadata) > maxMetadataEntries {
		return InvalidArgument(fmt.Sprintf("metadata has %d entries, maximum is %d", len(metadata), maxMetadataEntries))
	}

	totalSize := 0
	for key, value := range metadata {
		if err := validateMetadataKey(key); err != nil {
			return err
		}

		if len(value) > maxMetadataValueLen {
			return InvalidArgument(fmt.Sprintf("metadata value %q is %d bytes, maximum is %d", key, len(value), maxMetadataValueLen))
		}

		totalSize += len(key) + len(value)
	}

	if totalSize > maxMetadataTotalSize {
		return InvalidArgument(fmt.Sprintf("metadata is %d bytes, maximum is %d", totalSize, maxMetadataTotalSize))
	}

	return nil
}

func validateMetadataKey(key string) error {
	if key == "" {
		return InvalidArgument("metadata key is empty")
	}

	if len(key) > maxMetadataKeyLen {
		return InvalidArgument(fmt.Sprintf("metadata key %q is %d bytes, maximum is %d", key, len(key), maxMetadataKeyLen))
	}

	if strings.HasPrefix(key, reservedMetadataPrefix) {
		return InvalidArgument(fmt.Sprintf("metadata key %q uses reserved prefix %q", key, reservedMetadataPrefix))
	}

	for i := 0; i < len(key); i++ {
		if !isMetadataKeyByte(key[i]) {
			return InvalidArgument(fmt.Sprintf("metadata key %q must use lowercase ASCII letters, digits, '.', '_' or '-'", key))
		}
	}

	return nil
}

func isMetadataKeyByte(value byte) bool {
	return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '.' || value == '_' || value == '-'
}
