package main

import (
	"context"
	"io"
	"testing"

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
