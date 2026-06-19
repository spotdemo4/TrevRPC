package trevrpc

import (
	"fmt"
	"strings"
)

const (
	// ALPN is the QUIC application protocol negotiated by TrevRPC peers.
	ALPN = "trevrpc/1"
	// WireVersion is the current TrevRPC wire protocol version.
	WireVersion = uint32(1)

	// MaxMetadataEntries is the maximum number of metadata entries per message.
	MaxMetadataEntries = 64
	// MaxMetadataKeyLen is the maximum metadata key length in bytes.
	MaxMetadataKeyLen = 128
	// MaxMetadataValueLen is the maximum metadata value length in bytes.
	MaxMetadataValueLen = 8 * 1024
	// MaxMetadataTotalSize is the maximum combined metadata key and value size in bytes.
	MaxMetadataTotalSize = 64 * 1024

	// ReservedMetadataPrefix is reserved for runtime-owned metadata keys.
	ReservedMetadataPrefix = "trevrpc-"
)

// Metadata stores request or response metadata as byte values keyed by normalized names.
type Metadata map[string][]byte

// RpcKind identifies whether an RPC is unary or streaming.
type RpcKind int32

const (
	RpcKindUnary                  RpcKind = 0
	RpcKindClientStreaming        RpcKind = 1
	RpcKindServerStreaming        RpcKind = 2
	RpcKindBidirectionalStreaming RpcKind = 3
)

// RpcStreamFrameKind identifies whether a streaming frame carries a message or terminal status.
type RpcStreamFrameKind int32

const (
	RpcStreamFrameKindMessage RpcStreamFrameKind = 0
	RpcStreamFrameKindStatus  RpcStreamFrameKind = 1
)

// RpcRequest is the wire representation of an RPC request.
type RpcRequest struct {
	Service      string   `protobuf:"bytes,1,opt,name=service,proto3" json:"service,omitempty"`
	Method       string   `protobuf:"bytes,2,opt,name=method,proto3" json:"method,omitempty"`
	Body         []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata     Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Kind         RpcKind  `protobuf:"varint,5,opt,name=kind,proto3,enum=trevrpc.RpcKind" json:"kind,omitempty"`
	Version      uint32   `protobuf:"varint,6,opt,name=version,proto3" json:"version,omitempty"`
	TimeoutNanos uint64   `protobuf:"varint,7,opt,name=timeout_nanos,json=timeoutNanos,proto3" json:"timeout_nanos,omitempty"`
}

// NewRpcRequest creates a unary RPC request with the current wire version.
func NewRpcRequest(service, method string, body []byte) *RpcRequest {
	return &RpcRequest{
		Service:  service,
		Method:   method,
		Body:     body,
		Metadata: Metadata{},
		Kind:     RpcKindUnary,
		Version:  WireVersion,
	}
}

// Reset clears the request for protobuf compatibility.
func (m *RpcRequest) Reset() { *m = RpcRequest{} }

// String returns the request as a debug string for protobuf compatibility.
func (m *RpcRequest) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks RpcRequest as a protobuf message.
func (*RpcRequest) ProtoMessage() {}

// ValidateProtocol validates the request wire version and RPC kind.
func (m *RpcRequest) ValidateProtocol() error {
	if m.Version != WireVersion {
		return FailedPrecondition(fmt.Sprintf("unsupported TrevRPC wire version %d; expected %d", m.Version, WireVersion))
	}

	if !m.Kind.IsValid() {
		return InvalidArgument(fmt.Sprintf("unsupported TrevRPC RPC kind %d", m.Kind))
	}

	return nil
}

// IsValid reports whether k is a supported RPC kind.
func (k RpcKind) IsValid() bool {
	switch k {
	case RpcKindUnary, RpcKindClientStreaming, RpcKindServerStreaming, RpcKindBidirectionalStreaming:
		return true
	default:
		return false
	}
}

// RPCKind returns the request RPC kind, defaulting to unary for invalid values.
func (m *RpcRequest) RPCKind() RpcKind {
	if m.Kind.IsValid() {
		return m.Kind
	}

	return RpcKindUnary
}

// RpcResponse is the wire representation of an RPC response.
type RpcResponse struct {
	Status   uint32   `protobuf:"varint,1,opt,name=status,proto3" json:"status,omitempty"`
	Message  string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

// OKResponse creates a successful RPC response with the provided body.
func OKResponse(body []byte) *RpcResponse {
	return OK().IntoResponse(body)
}

// Reset clears the response for protobuf compatibility.
func (m *RpcResponse) Reset() { *m = RpcResponse{} }

// String returns the response as a debug string for protobuf compatibility.
func (m *RpcResponse) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks RpcResponse as a protobuf message.
func (*RpcResponse) ProtoMessage() {}

// RpcStreamFrame is the wire representation of one streaming RPC frame.
type RpcStreamFrame struct {
	Kind     RpcStreamFrameKind `protobuf:"varint,1,opt,name=kind,proto3,enum=trevrpc.RpcStreamFrameKind" json:"kind,omitempty"`
	Status   uint32             `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
	Message  string             `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte             `protobuf:"bytes,4,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata           `protobuf:"bytes,5,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

// MessageFrame creates a stream message frame carrying a protobuf body.
func MessageFrame(body []byte) *RpcStreamFrame {
	return &RpcStreamFrame{
		Kind:     RpcStreamFrameKindMessage,
		Status:   uint32(CodeOK),
		Body:     body,
		Metadata: Metadata{},
	}
}

// StatusFrame creates a terminal status frame without metadata.
func StatusFrame(status *Status) *RpcStreamFrame {
	return StatusFrameWithMetadata(status, Metadata{})
}

// StatusFrameWithMetadata creates a terminal status frame with metadata.
func StatusFrameWithMetadata(status *Status, metadata Metadata) *RpcStreamFrame {
	if status == nil {
		status = OK()
	}

	return &RpcStreamFrame{
		Kind:     RpcStreamFrameKindStatus,
		Status:   uint32(status.Code),
		Message:  status.Message,
		Metadata: metadata,
	}
}

// Reset clears the stream frame for protobuf compatibility.
func (m *RpcStreamFrame) Reset() { *m = RpcStreamFrame{} }

// String returns the stream frame as a debug string for protobuf compatibility.
func (m *RpcStreamFrame) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks RpcStreamFrame as a protobuf message.
func (*RpcStreamFrame) ProtoMessage() {}

// FrameKind returns the decoded stream frame kind.
func (m *RpcStreamFrame) FrameKind() (RpcStreamFrameKind, bool) {
	switch m.Kind {
	case RpcStreamFrameKindMessage, RpcStreamFrameKindStatus:
		return m.Kind, true
	default:
		return 0, false
	}
}

// StatusValue returns the status represented by the frame status fields.
func (m *RpcStreamFrame) StatusValue() *Status {
	return NewStatus(CodeFromUint32(m.Status), m.Message)
}

// NormalizeMetadataKey normalizes a metadata key to lowercase ASCII.
func NormalizeMetadataKey(key string) string {
	return strings.ToLower(key)
}

// ValidateMetadata validates metadata key syntax, value sizes, and total metadata limits.
func ValidateMetadata(metadata Metadata) error {
	if len(metadata) > MaxMetadataEntries {
		return InvalidArgument(fmt.Sprintf("metadata has %d entries, maximum is %d", len(metadata), MaxMetadataEntries))
	}

	totalSize := 0
	for key, value := range metadata {
		if err := validateMetadataKey(key); err != nil {
			return err
		}

		if len(value) > MaxMetadataValueLen {
			return InvalidArgument(fmt.Sprintf("metadata value %q is %d bytes, maximum is %d", key, len(value), MaxMetadataValueLen))
		}

		totalSize += len(key) + len(value)
	}

	if totalSize > MaxMetadataTotalSize {
		return InvalidArgument(fmt.Sprintf("metadata is %d bytes, maximum is %d", totalSize, MaxMetadataTotalSize))
	}

	return nil
}

func validateMetadataKey(key string) error {
	if key == "" {
		return InvalidArgument("metadata key is empty")
	}

	if len(key) > MaxMetadataKeyLen {
		return InvalidArgument(fmt.Sprintf("metadata key %q is %d bytes, maximum is %d", key, len(key), MaxMetadataKeyLen))
	}

	if strings.HasPrefix(key, ReservedMetadataPrefix) {
		return InvalidArgument(fmt.Sprintf("metadata key %q uses reserved prefix %q", key, ReservedMetadataPrefix))
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
