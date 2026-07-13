package main

import (
	"context"
	"io"
	"math"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

type benchmarkService struct{}

func (benchmarkService) Unary(_ context.Context, request *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error) {
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	return benchmarkResponse(request.Sequence, request.ResponseBytes), nil
}

func (benchmarkService) ClientStream(_ context.Context, requests trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]) (*benchmarkpb.BenchmarkSummary, error) {
	var messageCount uint64
	var payloadBytes uint64
	for {
		request, err := requests.Recv()
		if err == io.EOF {
			return &benchmarkpb.BenchmarkSummary{MessageCount: messageCount, PayloadBytes: payloadBytes}, nil
		}
		if err != nil {
			return nil, err
		}
		if messageCount >= maxBenchmarkMessagesPerStream {
			return nil, trevrpc.ResourceExhausted("client stream exceeds the benchmark message limit")
		}
		if math.MaxUint64-payloadBytes < uint64(len(request.Payload)) {
			return nil, trevrpc.InvalidArgument("client stream payload byte count overflow")
		}
		messageCount++
		payloadBytes += uint64(len(request.Payload))
	}
}

func (benchmarkService) ServerStream(_ context.Context, request *benchmarkpb.StreamRequest) (trevrpc.MessageStream[*benchmarkpb.BenchmarkResponse], error) {
	if request.MessageCount == 0 || request.MessageCount > maxBenchmarkMessagesPerStream {
		return nil, trevrpc.InvalidArgument("server stream message count is outside the benchmark limit")
	}
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	return &serverResponseStream{remaining: request.MessageCount, responseBytes: request.ResponseBytes}, nil
}

func (benchmarkService) Bidi(_ context.Context, requests trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]) (trevrpc.MessageStream[*benchmarkpb.BenchmarkResponse], error) {
	return &bidiResponseStream{requests: requests}, nil
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

type bidiResponseStream struct {
	requests trevrpc.MessageStream[*benchmarkpb.BenchmarkRequest]
	messages uint32
}

func (s *bidiResponseStream) Recv() (*benchmarkpb.BenchmarkResponse, error) {
	request, err := s.requests.Recv()
	if err != nil {
		return nil, err
	}
	if s.messages >= maxBenchmarkMessagesPerStream {
		return nil, trevrpc.ResourceExhausted("bidi stream exceeds the benchmark message limit")
	}
	if err := validateResponseBytes(request.ResponseBytes); err != nil {
		return nil, err
	}
	s.messages++
	return benchmarkResponse(request.Sequence, request.ResponseBytes), nil
}

func (s *bidiResponseStream) Close() error {
	return s.requests.Close()
}

func benchmarkResponse(sequence uint64, size uint32) *benchmarkpb.BenchmarkResponse {
	return &benchmarkpb.BenchmarkResponse{Sequence: sequence, Payload: make([]byte, int(size))}
}

func validateResponseBytes(size uint32) error {
	if size > maxBenchmarkPayloadBytes {
		return trevrpc.InvalidArgument("response payload exceeds the benchmark limit")
	}
	return nil
}

func newBenchmarkServer() *trevrpc.Server {
	server := trevrpc.NewServer()
	options := server.Options()
	options.MaxFrameSize = maxBenchmarkFrameSize
	options.MaxConcurrentConnections = 16
	options.MaxConcurrentStreamsPerConnection = maxBenchmarkConcurrency
	options.MaxConcurrentRequests = maxBenchmarkConcurrency * 2
	options.MaxStreamMessages = -1
	options.MaxStreamBodySize = -1
	server.SetOptions(options)
	benchmarkpb.RegisterBenchmarkServiceServer(server, benchmarkService{})
	return server
}

var _ benchmarkpb.BenchmarkServiceServer = benchmarkService{}
