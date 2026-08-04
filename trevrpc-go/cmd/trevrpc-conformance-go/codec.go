package main

import (
	"encoding/hex"
	"errors"
	"sort"
	"strconv"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type conformanceError struct {
	category   string
	statusCode uint32
	native     error
}

func (e *conformanceError) Error() string { return e.native.Error() }

func codecEncode(message any, maxFrameSize int) (map[string]any, *conformanceError) {
	protoMessage, ok := message.(trevrpc.ProtoMessage)
	if !ok {
		return nil, internalError(errors.New("message does not implement ProtoMessage"))
	}
	if err := validateMessage(message); err != nil {
		return nil, err
	}
	body, nativeErr := trevrpc.MarshalMessage(protoMessage)
	if nativeErr != nil {
		return nil, malformedError(messageTypeOf(message), nativeErr)
	}
	frame, nativeErr := trevrpc.EncodeFrame(protoMessage, maxFrameSize)
	if nativeErr != nil {
		return nil, classifyNativeError(nativeErr)
	}
	return map[string]any{"body_hex": hex.EncodeToString(body), "frame_hex": hex.EncodeToString(frame)}, nil
}

func codecDecode(messageType string, body []byte) (map[string]any, *conformanceError) {
	message, err := newWireMessage(messageType)
	if err != nil {
		return nil, internalError(err)
	}
	if nativeErr := trevrpc.DecodeFrame(body, message); nativeErr != nil {
		return nil, malformedError(messageType, nativeErr)
	}
	if validation := validateMessage(message); validation != nil {
		return nil, validation
	}
	canonical, nativeErr := trevrpc.MarshalMessage(message)
	if nativeErr != nil {
		return nil, malformedError(messageType, nativeErr)
	}
	return map[string]any{
		"message":            normalizeMessage(message),
		"canonical_body_hex": hex.EncodeToString(canonical),
	}, nil
}

func newWireMessage(messageType string) (trevrpc.ProtoMessage, error) {
	switch messageType {
	case "rpc_request":
		return &trevrpc.RpcRequest{}, nil
	case "rpc_response":
		return &trevrpc.RpcResponse{}, nil
	case "rpc_stream_frame":
		return &trevrpc.RpcStreamFrame{}, nil
	default:
		return nil, errors.New("unknown message type")
	}
}

func validateMessage(message any) *conformanceError {
	switch message := message.(type) {
	case *trevrpc.RpcRequest:
		if err := message.ValidateProtocol(); err != nil {
			status := trevrpc.StatusFromError(err)
			if status.Code == trevrpc.CodeFailedPrecondition {
				return &conformanceError{category: "unsupported_wire_version", statusCode: uint32(status.Code), native: err}
			}
			return &conformanceError{category: "unsupported_rpc_kind", statusCode: uint32(status.Code), native: err}
		}
		if err := trevrpc.ValidateMetadata(message.Metadata); err != nil {
			return &conformanceError{category: "invalid_metadata", statusCode: uint32(trevrpc.CodeInvalidArgument), native: err}
		}
	case *trevrpc.RpcResponse:
		if err := trevrpc.ValidateMetadata(message.Metadata); err != nil {
			return &conformanceError{category: "invalid_metadata", statusCode: uint32(trevrpc.CodeInternal), native: err}
		}
	case *trevrpc.RpcStreamFrame:
		if _, ok := message.FrameKind(); !ok {
			return &conformanceError{category: "unsupported_frame_kind", statusCode: uint32(trevrpc.CodeInvalidArgument), native: trevrpc.InvalidArgument("unsupported frame kind")}
		}
		if err := trevrpc.ValidateMetadata(message.Metadata); err != nil {
			return &conformanceError{category: "invalid_metadata", statusCode: uint32(trevrpc.CodeInternal), native: err}
		}
	default:
		return internalError(errors.New("unknown wire message"))
	}
	return nil
}

func normalizeMessage(message any) any {
	switch message := message.(type) {
	case *trevrpc.RpcRequest:
		return normalizedRequest{
			Type: "rpc_request", ServiceHex: hex.EncodeToString([]byte(message.Service)), MethodHex: hex.EncodeToString([]byte(message.Method)),
			BodyHex: hex.EncodeToString(message.Body), Metadata: normalizeMetadata(message.Metadata), Kind: rpcKindToken(message.Kind),
			Version: decimal(uint64(message.Version)), TimeoutNanos: decimal(message.TimeoutNanos),
		}
	case *trevrpc.RpcResponse:
		return normalizedResponse{
			Type: "rpc_response", StatusRaw: decimal(uint64(message.Status)), StatusCode: uint32(trevrpc.CodeFromUint32(message.Status)),
			MessageHex: hex.EncodeToString([]byte(message.Message)), BodyHex: hex.EncodeToString(message.Body), Metadata: normalizeMetadata(message.Metadata),
		}
	case *trevrpc.RpcStreamFrame:
		kind, _ := message.FrameKind()
		return normalizedStreamFrame{
			Type: "rpc_stream_frame", Kind: frameKindToken(kind), KindRaw: decimal(uint64(uint32(message.Kind))),
			StatusRaw: decimal(uint64(message.Status)), StatusCode: uint32(trevrpc.CodeFromUint32(message.Status)),
			MessageHex: hex.EncodeToString([]byte(message.Message)), BodyHex: hex.EncodeToString(message.Body), Metadata: normalizeMetadata(message.Metadata),
		}
	default:
		return nil
	}
}

func normalizeMetadata(metadata trevrpc.Metadata) []metadataEntry {
	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]metadataEntry, 0, len(keys))
	for _, key := range keys {
		entries = append(entries, metadataEntry{KeyHex: hex.EncodeToString([]byte(key)), ValueHex: hex.EncodeToString(metadata[key])})
	}
	return entries
}

func rpcKindToken(kind trevrpc.RpcKind) string {
	switch kind {
	case trevrpc.RpcKindUnary:
		return "unary"
	case trevrpc.RpcKindClientStreaming:
		return "client_stream"
	case trevrpc.RpcKindServerStreaming:
		return "server_stream"
	case trevrpc.RpcKindBidirectionalStreaming:
		return "bidi"
	default:
		return ""
	}
}

func frameKindToken(kind trevrpc.RpcStreamFrameKind) string {
	if kind == trevrpc.RpcStreamFrameKindStatus {
		return "status"
	}
	return "message"
}

func decimal(value uint64) string { return strconv.FormatUint(value, 10) }

func classifyNativeError(err error) *conformanceError {
	var tooLarge *trevrpc.FrameTooLargeError
	if errors.As(err, &tooLarge) {
		return &conformanceError{category: "frame_too_large", statusCode: uint32(trevrpc.CodeResourceExhausted), native: err}
	}
	return internalError(err)
}

func messageTypeOf(message any) string {
	switch message.(type) {
	case *trevrpc.RpcRequest:
		return "rpc_request"
	case *trevrpc.RpcResponse:
		return "rpc_response"
	case *trevrpc.RpcStreamFrame:
		return "rpc_stream_frame"
	default:
		return ""
	}
}

func malformedError(messageType string, err error) *conformanceError {
	code := trevrpc.CodeInternal
	if messageType == "rpc_request" {
		code = trevrpc.CodeInvalidArgument
	}
	return &conformanceError{category: "malformed_protobuf", statusCode: uint32(code), native: err}
}

func internalError(err error) *conformanceError {
	return &conformanceError{category: "malformed_protobuf", statusCode: uint32(trevrpc.CodeInvalidArgument), native: err}
}
