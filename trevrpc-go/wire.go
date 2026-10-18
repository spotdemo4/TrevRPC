package trevrpc

import (
	"fmt"
	"strings"
)

const (
	// ALPN is the QUIC application protocol negotiated by TrevRPC peers.
	ALPN = "trevrpc/1"
	// QMuxALPN identifies TrevRPC over QMux (draft-ietf-quic-qmux-02). It is the
	// TLS application protocol of QMux over TLS and the WebSocket subprotocol of
	// QMux over WebSocket.
	QMuxALPN = "qmux-02.trevrpc-1"

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

// RPCKind identifies the shape of an RPC.
type RPCKind int32

const (
	// RPCKindUnary is a single request and a single response.
	RPCKindUnary RPCKind = 0
	// RPCKindClientStreaming is a request stream and a single response.
	RPCKindClientStreaming RPCKind = 1
	// RPCKindServerStreaming is a single request and a response stream.
	RPCKindServerStreaming RPCKind = 2
	// RPCKindBidirectionalStreaming is a request stream and a response stream.
	RPCKindBidirectionalStreaming RPCKind = 3
)

// wireRequest is the payload of a REQUEST frame. Fields 2 (the method name),
// 3 (the request body) and 6 (the wire version) are reserved: the method is the
// full path in field 1, the request message travels in a MESSAGE frame, and
// transports fix the wire version while negotiating.
type wireRequest struct {
	// Method is the method path "/package.Service/Method".
	Method       string   `protobuf:"bytes,1,opt,name=method,proto3" json:"method,omitempty"`
	Metadata     Metadata `protobuf:"bytes,4,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	Kind         RPCKind  `protobuf:"varint,5,opt,name=kind,proto3,enum=trevrpc.RpcKind" json:"kind,omitempty"`
	TimeoutNanos uint64   `protobuf:"varint,7,opt,name=timeout_nanos,json=timeoutNanos,proto3" json:"timeout_nanos,omitempty"`
	// Extensions lists the extension IDs the client supports (field 8). The
	// registry is empty, so clients send none and servers ignore the field.
	Extensions []uint64 `protobuf:"varint,8,rep,packed,name=extensions,proto3" json:"extensions,omitempty"`
}

// newWireRequest creates a unary RPC request for the method path
// "/package.Service/Method".
func newWireRequest(method string) *wireRequest {
	return &wireRequest{
		Method:   method,
		Metadata: Metadata{},
		Kind:     RPCKindUnary,
	}
}

// splitMethodPath splits a method path "/service/method" into its parts, which
// are substrings of path. A path is well-formed when it has exactly two
// slashes, the first at index 0, and both parts are non-empty.
func splitMethodPath(path string) (service, method string, ok bool) {
	if path == "" || path[0] != '/' {
		return "", "", false
	}
	rest := path[1:]
	slash := strings.IndexByte(rest, '/')
	if slash <= 0 || slash == len(rest)-1 || strings.IndexByte(rest[slash+1:], '/') >= 0 {
		return "", "", false
	}
	return rest[:slash], rest[slash+1:], true
}

// serviceAndMethod returns the service and method parts of the request's
// method path, or empty strings when the path is malformed.
func (m *wireRequest) serviceAndMethod() (service, method string) {
	service, method, _ = splitMethodPath(m.Method)
	return service, method
}

// Reset clears the request for protobuf compatibility.
func (m *wireRequest) Reset() { *m = wireRequest{} }

// String returns the request as a debug string for protobuf compatibility.
func (m *wireRequest) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireRequest as a protobuf message.
func (*wireRequest) ProtoMessage() {}

// validateProtocol validates the request method path and RPC kind.
func (m *wireRequest) validateProtocol() error {
	if _, _, ok := splitMethodPath(m.Method); !ok {
		return InvalidArgument("malformed RPC method path; expected /<service>/<method>")
	}

	if !m.Kind.isValid() {
		return InvalidArgument(fmt.Sprintf("unsupported TrevRPC RPC kind %d", m.Kind))
	}

	return nil
}

// isValid reports whether k is a supported RPC kind.
func (k RPCKind) isValid() bool {
	switch k {
	case RPCKindUnary, RPCKindClientStreaming, RPCKindServerStreaming, RPCKindBidirectionalStreaming:
		return true
	default:
		return false
	}
}

// rpcKind returns the request RPC kind, defaulting to unary for invalid values.
func (m *wireRequest) rpcKind() RPCKind {
	if m.Kind.isValid() {
		return m.Kind
	}

	return RPCKindUnary
}

// wireHeaders is the payload of a HEADERS frame.
type wireHeaders struct {
	Metadata Metadata `protobuf:"bytes,1,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
	// Extensions lists the extension IDs the server accepts (field 2). The
	// registry is empty, so servers send none and clients ignore the field.
	Extensions []uint64 `protobuf:"varint,2,rep,packed,name=extensions,proto3" json:"extensions,omitempty"`
}

// Reset clears the headers for protobuf compatibility.
func (m *wireHeaders) Reset() { *m = wireHeaders{} }

// String returns the headers as a debug string for protobuf compatibility.
func (m *wireHeaders) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireHeaders as a protobuf message.
func (*wireHeaders) ProtoMessage() {}

// wireStatus is the payload of a STATUS frame.
type wireStatus struct {
	Code     uint32   `protobuf:"varint,1,opt,name=code,proto3" json:"code,omitempty"`
	Message  string   `protobuf:"bytes,2,opt,name=message,proto3" json:"message,omitempty"`
	Metadata Metadata `protobuf:"bytes,3,rep,name=metadata,proto3" json:"metadata,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

// Reset clears the status for protobuf compatibility.
func (m *wireStatus) Reset() { *m = wireStatus{} }

// String returns the status as a debug string for protobuf compatibility.
func (m *wireStatus) String() string { return fmt.Sprintf("%+v", *m) }

// ProtoMessage marks wireStatus as a protobuf message.
func (*wireStatus) ProtoMessage() {}

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
