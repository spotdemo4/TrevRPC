package trevrpc

import (
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
)

const (
	ALPN        = "trevrpc/1"
	WireVersion = uint32(1)

	MaxMetadataEntries   = 64
	MaxMetadataKeyLen    = 128
	MaxMetadataValueLen  = 8 * 1024
	MaxMetadataTotalSize = 64 * 1024

	ReservedMetadataPrefix = "trevrpc-"
)

type Metadata map[string][]byte

type RpcKind int32

const (
	RpcKindUnary                  RpcKind = 0
	RpcKindClientStreaming        RpcKind = 1
	RpcKindServerStreaming        RpcKind = 2
	RpcKindBidirectionalStreaming RpcKind = 3
)

type RpcStreamFrameKind int32

const (
	RpcStreamFrameKindMessage RpcStreamFrameKind = 0
	RpcStreamFrameKindStatus  RpcStreamFrameKind = 1
)

type RpcRequest struct {
	Service           string   `protobuf:"bytes,1,opt,name=service,proto3" json:"service,omitempty"`
	Method            string   `protobuf:"bytes,2,opt,name=method,proto3" json:"method,omitempty"`
	Body              []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata          Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Kind              RpcKind  `protobuf:"varint,5,opt,name=kind,proto3,enum=trevrpc.RpcKind" json:"kind,omitempty"`
	Version           uint32   `protobuf:"varint,6,opt,name=version,proto3" json:"version,omitempty"`
	DeadlineUnixNanos uint64   `protobuf:"varint,7,opt,name=deadline_unix_nanos,json=deadlineUnixNanos,proto3" json:"deadline_unix_nanos,omitempty"`
}

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

func (m *RpcRequest) Reset()         { *m = RpcRequest{} }
func (m *RpcRequest) String() string { return proto.CompactTextString(m) }
func (*RpcRequest) ProtoMessage()    {}

func (m *RpcRequest) ValidateProtocol() error {
	if m.Version != WireVersion {
		return FailedPrecondition(fmt.Sprintf("unsupported TrevRPC wire version %d; expected %d", m.Version, WireVersion))
	}

	return nil
}

func (m *RpcRequest) RPCKind() RpcKind {
	switch m.Kind {
	case RpcKindUnary, RpcKindClientStreaming, RpcKindServerStreaming, RpcKindBidirectionalStreaming:
		return m.Kind
	default:
		return RpcKindUnary
	}
}

type RpcResponse struct {
	Status   uint32   `protobuf:"varint,1,opt,name=status,proto3" json:"status,omitempty"`
	Message  string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte   `protobuf:"bytes,3,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func OKResponse(body []byte) *RpcResponse {
	return OK().IntoResponse(body)
}

func (m *RpcResponse) Reset()         { *m = RpcResponse{} }
func (m *RpcResponse) String() string { return proto.CompactTextString(m) }
func (*RpcResponse) ProtoMessage()    {}

type RpcStreamFrame struct {
	Kind     RpcStreamFrameKind `protobuf:"varint,1,opt,name=kind,proto3,enum=trevrpc.RpcStreamFrameKind" json:"kind,omitempty"`
	Status   uint32             `protobuf:"varint,2,opt,name=status,proto3" json:"status,omitempty"`
	Message  string             `protobuf:"bytes,3,opt,name=message,proto3" json:"message,omitempty"`
	Body     []byte             `protobuf:"bytes,4,opt,name=body,proto3" json:"body,omitempty"`
	Metadata Metadata           `protobuf:"bytes,5,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func MessageFrame(body []byte) *RpcStreamFrame {
	return &RpcStreamFrame{
		Kind:     RpcStreamFrameKindMessage,
		Status:   uint32(CodeOK),
		Body:     body,
		Metadata: Metadata{},
	}
}

func StatusFrame(status *Status) *RpcStreamFrame {
	return StatusFrameWithMetadata(status, Metadata{})
}

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

func (m *RpcStreamFrame) Reset()         { *m = RpcStreamFrame{} }
func (m *RpcStreamFrame) String() string { return proto.CompactTextString(m) }
func (*RpcStreamFrame) ProtoMessage()    {}

func (m *RpcStreamFrame) FrameKind() (RpcStreamFrameKind, bool) {
	switch m.Kind {
	case RpcStreamFrameKindMessage, RpcStreamFrameKindStatus:
		return m.Kind, true
	default:
		return 0, false
	}
}

func (m *RpcStreamFrame) StatusValue() *Status {
	return NewStatus(CodeFromUint32(m.Status), m.Message)
}

func NormalizeMetadataKey(key string) string {
	return strings.ToLower(key)
}

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
