package main

import (
	"context"
	"io"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

func TestNativeBenchmarkServiceRejectsOversizedRequestPayloads(t *testing.T) {
	oversized := &benchmarkpb.BenchmarkRequest{Payload: make([]byte, maxBenchmarkPayloadBytes+1)}
	valid := &benchmarkpb.BenchmarkRequest{}
	service := nativeBenchmarkService{}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "unary",
			call: func() error {
				_, err := service.Unary(context.Background(), oversized)
				return err
			},
		},
		{
			name: "client stream message",
			call: func() error {
				_, err := service.ClientStream(context.Background(), newBenchmarkRequestStream(valid, oversized))
				return err
			},
		},
		{
			name: "server stream",
			call: func() error {
				_, err := service.ServerStream(context.Background(), &benchmarkpb.StreamRequest{
					MessageCount: 1,
					Payload:      oversized.Payload,
				})
				return err
			},
		},
		{
			name: "bidi message",
			call: func() error {
				responses, err := service.Bidi(context.Background(), newBenchmarkRequestStream(valid, oversized))
				if err != nil {
					return err
				}
				if _, err := responses.Recv(); err != nil {
					return err
				}
				_, err = responses.Recv()
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if code := trevrpc.StatusFromError(test.call()).Code; code != trevrpc.CodeInvalidArgument {
				t.Fatalf("status = %v, want %v", code, trevrpc.CodeInvalidArgument)
			}
		})
	}
}

func TestGRPCBenchmarkServiceRejectsOversizedRequestPayloads(t *testing.T) {
	oversized := &benchmarkpb.BenchmarkRequest{Payload: make([]byte, maxBenchmarkPayloadBytes+1)}
	valid := &benchmarkpb.BenchmarkRequest{}
	service := grpcBenchmarkService{}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "unary",
			call: func() error {
				_, err := service.Unary(context.Background(), oversized)
				return err
			},
		},
		{
			name: "client stream message",
			call: func() error {
				return service.ClientStream(newGRPCBenchmarkStream(valid, oversized))
			},
		},
		{
			name: "server stream",
			call: func() error {
				return service.ServerStream(&benchmarkpb.StreamRequest{
					MessageCount: 1,
					Payload:      oversized.Payload,
				}, newGRPCBenchmarkStream())
			},
		},
		{
			name: "bidi message",
			call: func() error {
				stream := newGRPCBenchmarkStream(valid, oversized)
				err := service.Bidi(stream)
				if len(stream.responses) != 1 {
					t.Fatalf("responses before oversized message = %d, want 1", len(stream.responses))
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if code := status.Code(test.call()); code != codes.InvalidArgument {
				t.Fatalf("status = %v, want %v", code, codes.InvalidArgument)
			}
		})
	}
}

type benchmarkRequestStream struct {
	requests []*benchmarkpb.BenchmarkRequest
	next     int
}

func newBenchmarkRequestStream(requests ...*benchmarkpb.BenchmarkRequest) *benchmarkRequestStream {
	return &benchmarkRequestStream{requests: requests}
}

func (s *benchmarkRequestStream) Recv() (*benchmarkpb.BenchmarkRequest, error) {
	if s.next == len(s.requests) {
		return nil, io.EOF
	}
	request := s.requests[s.next]
	s.next++
	return request, nil
}

func (*benchmarkRequestStream) Close() error { return nil }

type grpcBenchmarkStream struct {
	grpc.ServerStream
	requests  []*benchmarkpb.BenchmarkRequest
	responses []*benchmarkpb.BenchmarkResponse
	next      int
}

func newGRPCBenchmarkStream(requests ...*benchmarkpb.BenchmarkRequest) *grpcBenchmarkStream {
	return &grpcBenchmarkStream{requests: requests}
}

func (s *grpcBenchmarkStream) Recv() (*benchmarkpb.BenchmarkRequest, error) {
	if s.next == len(s.requests) {
		return nil, io.EOF
	}
	request := s.requests[s.next]
	s.next++
	return request, nil
}

func (s *grpcBenchmarkStream) Send(response *benchmarkpb.BenchmarkResponse) error {
	s.responses = append(s.responses, response)
	return nil
}

func (*grpcBenchmarkStream) SendAndClose(*benchmarkpb.BenchmarkSummary) error { return nil }
