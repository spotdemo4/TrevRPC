package main

import (
	"context"
	"errors"
	"io"
	"log"
	"math"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

type benchmarkServiceErrorCode uint8

const (
	serviceInvalidArgument benchmarkServiceErrorCode = iota
	serviceResourceExhausted
)

type benchmarkServiceError struct {
	code    benchmarkServiceErrorCode
	message string
}

func (e *benchmarkServiceError) Error() string { return e.message }

func benchmarkUnary(request *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error) {
	if request == nil {
		return nil, invalidBenchmarkArgument("missing benchmark request")
	}
	if err := validateRequestPayload(request.Payload); err != nil {
		return nil, err
	}
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	return benchmarkResponse(request.Sequence, request.ResponseBytes), nil
}

func summarizeClientStream(recv func() (*benchmarkpb.BenchmarkRequest, error)) (*benchmarkpb.BenchmarkSummary, error) {
	var messageCount uint64
	var payloadBytes uint64
	for {
		request, err := recv()
		if err == io.EOF {
			return &benchmarkpb.BenchmarkSummary{MessageCount: messageCount, PayloadBytes: payloadBytes}, nil
		}
		if err != nil {
			return nil, err
		}
		if request == nil {
			return nil, invalidBenchmarkArgument("client stream contained a missing request")
		}
		if err := validateRequestPayload(request.Payload); err != nil {
			return nil, err
		}
		if messageCount >= maxBenchmarkMessagesPerStream {
			return nil, exhaustedBenchmarkResource("client stream exceeds the benchmark message limit")
		}
		if math.MaxUint64-payloadBytes < uint64(len(request.Payload)) {
			return nil, invalidBenchmarkArgument("client stream payload byte count overflow")
		}
		messageCount++
		payloadBytes += uint64(len(request.Payload))
	}
}

func newServerResponseStream(request *benchmarkpb.StreamRequest) (*serverResponseStream, error) {
	if request == nil {
		return nil, invalidBenchmarkArgument("missing server stream request")
	}
	if err := validateRequestPayload(request.Payload); err != nil {
		return nil, err
	}
	if request.MessageCount == 0 || request.MessageCount > maxBenchmarkMessagesPerStream {
		return nil, invalidBenchmarkArgument("server stream message count is outside the benchmark limit")
	}
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	return &serverResponseStream{remaining: request.MessageCount, responseBytes: request.ResponseBytes}, nil
}

type serverResponseStream struct {
	remaining     uint32
	sequence      uint64
	responseBytes uint32
}

func (s *serverResponseStream) Recv() (*benchmarkpb.BenchmarkResponse, error) {
	if s.remaining == 0 {
		return nil, io.EOF
	}
	response := benchmarkResponse(s.sequence, s.responseBytes)
	s.sequence++
	s.remaining--
	return response, nil
}

func (s *serverResponseStream) Close() error {
	s.remaining = 0
	return nil
}

type bidiResponder struct {
	messages uint32
}

func (s *bidiResponder) respond(request *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error) {
	if request == nil {
		return nil, invalidBenchmarkArgument("bidi stream contained a missing request")
	}
	if err := validateRequestPayload(request.Payload); err != nil {
		return nil, err
	}
	if s.messages >= maxBenchmarkMessagesPerStream {
		return nil, exhaustedBenchmarkResource("bidi stream exceeds the benchmark message limit")
	}
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	s.messages++
	return benchmarkResponse(request.Sequence, request.ResponseBytes), nil
}

func benchmarkResponse(sequence uint64, size uint32) *benchmarkpb.BenchmarkResponse {
	return &benchmarkpb.BenchmarkResponse{Sequence: sequence, Payload: make([]byte, int(size))}
}

func validateRequestPayload(payload []byte) error {
	if len(payload) > maxBenchmarkPayloadBytes {
		return invalidBenchmarkArgument("request payload exceeds the benchmark limit")
	}
	return nil
}

func validateResponseBytes(size uint32) error {
	if size > maxBenchmarkPayloadBytes {
		return invalidBenchmarkArgument("response payload exceeds the benchmark limit")
	}
	return nil
}

func invalidBenchmarkArgument(message string) error {
	return &benchmarkServiceError{code: serviceInvalidArgument, message: message}
}

func exhaustedBenchmarkResource(message string) error {
	return &benchmarkServiceError{code: serviceResourceExhausted, message: message}
}

func nativeServiceError(err error) error {
	var serviceError *benchmarkServiceError
	if !errors.As(err, &serviceError) {
		return err
	}
	switch serviceError.code {
	case serviceInvalidArgument:
		return trevrpc.InvalidArgument(serviceError.message)
	case serviceResourceExhausted:
		return trevrpc.ResourceExhausted(serviceError.message)
	default:
		return trevrpc.Internal(serviceError.message)
	}
}

type nativeBenchmarkService struct{}

func (nativeBenchmarkService) Unary(_ context.Context, request *benchmarkpb.BenchmarkRequest) (*trevrpc.Response[*benchmarkpb.BenchmarkResponse], error) {
	response, err := benchmarkUnary(request)
	if err != nil {
		return nil, nativeServiceError(err)
	}
	return trevrpc.NewResponse(response), nil
}

func (nativeBenchmarkService) ClientStream(_ context.Context, requests trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]) (*trevrpc.Response[*benchmarkpb.BenchmarkSummary], error) {
	response, err := summarizeClientStream(requests.Recv)
	if err != nil {
		return nil, nativeServiceError(err)
	}
	return trevrpc.NewResponse(response), nil
}

func (nativeBenchmarkService) ServerStream(_ context.Context, request *benchmarkpb.StreamRequest) (trevrpc.ResponseStream[*benchmarkpb.BenchmarkResponse], error) {
	responses, err := newServerResponseStream(request)
	if err != nil {
		return nil, nativeServiceError(err)
	}
	return trevrpc.NewResponseStream[*benchmarkpb.BenchmarkResponse](responses), nil
}

func (nativeBenchmarkService) Bidi(_ context.Context, requests trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]) (trevrpc.ResponseStream[*benchmarkpb.BenchmarkResponse], error) {
	return trevrpc.NewResponseStream[*benchmarkpb.BenchmarkResponse](&nativeBidiResponseStream{requests: requests}), nil
}

type nativeBidiResponseStream struct {
	requests  trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]
	responder bidiResponder
}

func (s *nativeBidiResponseStream) Recv() (*benchmarkpb.BenchmarkResponse, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	response, err := s.responder.respond(request)
	return response, nativeServiceError(err)
}

func (s *nativeBidiResponseStream) Close() error { return s.requests.Close() }

func newNativeBenchmarkServer() *trevrpc.Server {
	server := trevrpc.NewServer()
	options := server.Options()
	options.MaxFrameSize = maxBenchmarkFrameSize
	options.MaxConcurrentConnections = 16
	options.MaxConcurrentStreamsPerConnection = maxBenchmarkConcurrency
	options.MaxConcurrentRequests = maxBenchmarkConcurrency * 2
	options.MaxStreamMessages = maxBenchmarkMessagesPerStream
	options.MaxStreamBodySize = -1
	server.SetOptions(options)
	server.SetDiagnostics(func(event trevrpc.ServerDiagnostic) {
		if event.Message != "" || event.Err != nil {
			log.Printf("server diagnostic: phase=%s message=%q error=%v", event.Phase, event.Message, event.Err)
		}
	})
	benchmarkpb.RegisterNativeBenchmarkServiceServer(server, nativeBenchmarkService{})
	return server
}

var _ benchmarkpb.NativeBenchmarkServiceServer = nativeBenchmarkService{}
