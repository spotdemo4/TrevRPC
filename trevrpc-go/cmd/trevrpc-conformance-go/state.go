package main

import (
	"context"
	"encoding/hex"
	"errors"
	"io"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

type scriptedTransport struct {
	frameBodies     [][]byte
	deliveredFrames []*trevrpc.RpcStreamFrame
	closeCount      int
}

func (t *scriptedTransport) Call(context.Context, *trevrpc.RpcRequest) (*trevrpc.RpcResponse, error) {
	return nil, errors.New("unexpected unary call")
}

func (t *scriptedTransport) StreamingCall(context.Context, *trevrpc.RpcRequest, trevrpc.ByteStream) (trevrpc.FrameStream, error) {
	return &scriptedFrameStream{transport: t, frameBodies: t.frameBodies}, nil
}

type scriptedFrameStream struct {
	transport   *scriptedTransport
	frameBodies [][]byte
	next        int
	closed      bool
}

func (s *scriptedFrameStream) Recv() (*trevrpc.RpcStreamFrame, error) {
	if s.next >= len(s.frameBodies) {
		return nil, io.EOF
	}
	body := s.frameBodies[s.next]
	s.next++
	frame := &trevrpc.RpcStreamFrame{}
	if err := trevrpc.DecodeFrame(body, frame); err != nil {
		return nil, err
	}
	s.transport.deliveredFrames = append(s.transport.deliveredFrames, frame)
	return frame, nil
}

func (s *scriptedFrameStream) Close() error {
	if !s.closed {
		s.closed = true
		s.transport.closeCount++
	}
	return nil
}

type stateEvent struct {
	Event   string `json:"event"`
	BodyHex string `json:"body_hex,omitempty"`
}

type normalizedStatus struct {
	StatusRaw  string          `json:"status_raw"`
	StatusCode uint32          `json:"status_code"`
	MessageHex string          `json:"message_hex"`
	Metadata   []metadataEntry `json:"metadata"`
}

func runServerState(frameBodies [][]byte) (map[string]any, *conformanceError) {
	transport := &scriptedTransport{frameBodies: frameBodies}
	stream, err := trevrpc.ServerStreamingResponse[*trevrpc.RpcRequest, *StatePayload](
		context.Background(), transport, "conformance", "server", &trevrpc.RpcRequest{}, func() *StatePayload { return &StatePayload{} },
		trevrpc.WithoutTimeout(), trevrpc.WithoutStreamIdleTimeout(),
	)
	if err != nil {
		return nil, internalError(err)
	}
	events := make([]stateEvent, 0)
	for {
		message, recvErr := stream.Recv()
		if recvErr == nil {
			body, marshalErr := marshalCanonicalStatePayload(message)
			if marshalErr != nil {
				return nil, malformedError("state_payload", marshalErr)
			}
			events = append(events, stateEvent{Event: "message", BodyHex: hex.EncodeToString(body)})
			continue
		}
		if recvErr == io.EOF {
			events = append(events, stateEvent{Event: "eof"})
			_, againErr := stream.Recv()
			if againErr != io.EOF {
				return nil, internalError(errors.New("terminal Recv was not stable EOF"))
			}
			events = append(events, stateEvent{Event: "eof"})
			result := map[string]any{"events": events, "transport_close_count": decimal(uint64(transport.closeCount))}
			if status, ok := stream.TerminalStatus(); ok {
				result["terminal_status"] = normalizedStatus{StatusRaw: decimal(uint64(status.Code)), StatusCode: uint32(status.Code), MessageHex: hex.EncodeToString([]byte(status.Message)), Metadata: normalizeMetadata(status.Metadata)}
			}
			return result, nil
		}
		_, _ = stream.Recv()
		failure := classifyStateError(recvErr)
		return map[string]any{"transport_close_count": decimal(uint64(transport.closeCount))}, failure
	}
}

func runClientState(frameBodies [][]byte) (map[string]any, *conformanceError) {
	transport := &scriptedTransport{frameBodies: frameBodies}
	response, err := trevrpc.ClientStreamingFromStream[*trevrpc.RpcRequest, *StatePayload](
		context.Background(), transport, "conformance", "client", trevrpc.EmptyStream[*trevrpc.RpcRequest](), func() *StatePayload { return &StatePayload{} },
		trevrpc.WithoutTimeout(), trevrpc.WithoutStreamIdleTimeout(),
	)
	if err != nil {
		return nil, classifyStateError(err)
	}
	body, err := marshalCanonicalStatePayload(response)
	if err != nil {
		return nil, malformedError("state_payload", err)
	}
	return map[string]any{"response_body_hex": hex.EncodeToString(body)}, nil
}

func marshalCanonicalStatePayload(message *StatePayload) ([]byte, error) {
	return trevrpc.MarshalMessage(&StatePayload{Body: append([]byte(nil), message.GetBody()...)})
}

func classifyStateError(err error) *conformanceError {
	reason, ok := trevrpc.ResponseStreamFailureReason(err)
	if !ok {
		return &conformanceError{category: "malformed_protobuf", statusCode: uint32(trevrpc.StatusFromError(err).Code), native: err}
	}
	return &conformanceError{category: reason, statusCode: uint32(trevrpc.StatusFromError(err).Code), native: err}
}
