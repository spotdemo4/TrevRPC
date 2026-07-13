package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

type operationCounts struct {
	requestMessages  uint64
	responseMessages uint64
}

type benchmarkOperation func(context.Context, uint64) (operationCounts, error)

func newBenchmarkOperation(client *benchmarkpb.BenchmarkServiceClient, config clientConfig) benchmarkOperation {
	switch config.rpc {
	case rpcUnary:
		return func(ctx context.Context, sequence uint64) (operationCounts, error) {
			request := newBenchmarkRequest(sequence, config)
			response, err := client.Unary(ctx, request, trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize))
			if err != nil {
				return operationCounts{}, err
			}
			if err := validateBenchmarkResponse(response, sequence, config.responseBytes); err != nil {
				return operationCounts{}, err
			}
			return operationCounts{requestMessages: 1, responseMessages: 1}, nil
		}
	case rpcClientStream:
		return func(ctx context.Context, _ uint64) (operationCounts, error) {
			call, err := client.ClientStream(ctx)
			if err != nil {
				return operationCounts{}, err
			}
			defer call.Close()
			for index := range config.messagesPerStream {
				if err := call.Send(newBenchmarkRequest(uint64(index), config)); err != nil {
					return operationCounts{}, err
				}
			}
			summary, err := call.CloseAndRecv()
			if err != nil {
				return operationCounts{}, err
			}
			expectedPayloadBytes := uint64(config.messagesPerStream) * uint64(config.requestBytes)
			if summary.MessageCount != uint64(config.messagesPerStream) || summary.PayloadBytes != expectedPayloadBytes {
				return operationCounts{}, fmt.Errorf("client stream summary = (%d, %d), want (%d, %d)", summary.MessageCount, summary.PayloadBytes, config.messagesPerStream, expectedPayloadBytes)
			}
			return operationCounts{requestMessages: uint64(config.messagesPerStream), responseMessages: 1}, nil
		}
	case rpcServerStream:
		return func(ctx context.Context, _ uint64) (operationCounts, error) {
			request := &benchmarkpb.StreamRequest{
				MessageCount:  config.messagesPerStream,
				Payload:       make([]byte, int(config.requestBytes)),
				ResponseBytes: config.responseBytes,
			}
			responses, err := client.ServerStream(ctx, request,
				trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize),
				trevrpc.WithMaxResponseMessages(int(config.messagesPerStream)),
				trevrpc.WithoutMaxResponseStreamBodySize(),
			)
			if err != nil {
				return operationCounts{}, err
			}
			defer responses.Close()
			for index := range config.messagesPerStream {
				response, err := responses.Recv()
				if err != nil {
					return operationCounts{}, fmt.Errorf("server stream response %d: %w", index, err)
				}
				if err := validateBenchmarkResponse(response, uint64(index), config.responseBytes); err != nil {
					return operationCounts{}, err
				}
			}
			if _, err := responses.Recv(); err != io.EOF {
				return operationCounts{}, fmt.Errorf("server stream terminal result = %v, want EOF", err)
			}
			return operationCounts{requestMessages: 1, responseMessages: uint64(config.messagesPerStream)}, nil
		}
	case rpcBidi:
		return func(ctx context.Context, _ uint64) (operationCounts, error) {
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			call, err := client.Bidi(callCtx,
				trevrpc.WithMaxResponseBodySize(maxBenchmarkFrameSize),
				trevrpc.WithMaxResponseMessages(int(config.messagesPerStream)),
				trevrpc.WithoutMaxResponseStreamBodySize(),
			)
			if err != nil {
				return operationCounts{}, err
			}
			defer call.Close()
			sendResult := make(chan error, 1)
			go func() {
				for index := range config.messagesPerStream {
					if err := call.Send(newBenchmarkRequest(uint64(index), config)); err != nil {
						cancel()
						sendResult <- err
						return
					}
				}
				sendResult <- call.CloseSend()
			}()

			var responseCount uint32
			for {
				response, err := call.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					cancel()
					sendErr := <-sendResult
					return operationCounts{}, errors.Join(err, sendErr)
				}
				if responseCount >= config.messagesPerStream {
					cancel()
					<-sendResult
					return operationCounts{}, errors.New("bidi returned too many responses")
				}
				if err := validateBenchmarkResponse(response, uint64(responseCount), config.responseBytes); err != nil {
					cancel()
					<-sendResult
					return operationCounts{}, err
				}
				responseCount++
			}
			if err := <-sendResult; err != nil {
				return operationCounts{}, err
			}
			if responseCount != config.messagesPerStream {
				return operationCounts{}, fmt.Errorf("bidi response count = %d, want %d", responseCount, config.messagesPerStream)
			}
			messages := uint64(config.messagesPerStream)
			return operationCounts{requestMessages: messages, responseMessages: messages}, nil
		}
	default:
		panic("invalid RPC kind")
	}
}

func newBenchmarkRequest(sequence uint64, config clientConfig) *benchmarkpb.BenchmarkRequest {
	return &benchmarkpb.BenchmarkRequest{
		Sequence:      sequence,
		Payload:       make([]byte, int(config.requestBytes)),
		ResponseBytes: config.responseBytes,
	}
}

func validateBenchmarkResponse(response *benchmarkpb.BenchmarkResponse, sequence uint64, payloadBytes uint32) error {
	if response == nil {
		return errors.New("missing benchmark response")
	}
	if response.Sequence != sequence {
		return fmt.Errorf("response sequence = %d, want %d", response.Sequence, sequence)
	}
	if len(response.Payload) != int(payloadBytes) {
		return fmt.Errorf("response payload bytes = %d, want %d", len(response.Payload), payloadBytes)
	}
	for _, value := range response.Payload {
		if value != 0 {
			return errors.New("response payload contains non-zero data")
		}
	}
	return nil
}

type laneResult struct {
	completed        uint64
	failed           uint64
	requestMessages  uint64
	responseMessages uint64
	histogram        map[uint64]uint64
	err              error
}

type phaseResult struct {
	laneResult
	startedAt time.Time
	elapsed   time.Duration
}

type preparedPhase struct {
	ctx           context.Context
	operation     benchmarkOperation
	concurrency   int
	recordLatency bool
	gate          chan struct{}
	ready         sync.WaitGroup
	done          sync.WaitGroup
	results       chan laneResult
	startedAt     time.Time
	deadline      time.Time
}

func preparePhase(ctx context.Context, concurrency int, operation benchmarkOperation, recordLatency bool) *preparedPhase {
	phase := &preparedPhase{
		ctx:           ctx,
		operation:     operation,
		concurrency:   concurrency,
		recordLatency: recordLatency,
		gate:          make(chan struct{}),
		results:       make(chan laneResult, concurrency),
	}
	phase.ready.Add(concurrency)
	phase.done.Add(concurrency)
	for lane := range concurrency {
		go phase.runLane(lane)
	}
	phase.ready.Wait()
	return phase
}

func (p *preparedPhase) run(duration time.Duration) phaseResult {
	p.startedAt = time.Now()
	p.deadline = p.startedAt.Add(duration)
	close(p.gate)
	if remaining := time.Until(p.deadline); remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-timer.C:
		case <-p.ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
		}
	}
	p.done.Wait()
	result := phaseResult{startedAt: p.startedAt, elapsed: time.Since(p.startedAt)}
	for range p.concurrency {
		result.merge(<-p.results)
	}
	return result
}

func (p *preparedPhase) runLane(lane int) {
	defer p.done.Done()
	p.ready.Done()
	select {
	case <-p.gate:
	case <-p.ctx.Done():
		p.results <- laneResult{histogram: map[uint64]uint64{}}
		return
	}

	result := laneResult{histogram: map[uint64]uint64{}}
	sequence := uint64(lane)
	stride := uint64(p.concurrency)
	for {
		operationStarted := time.Now()
		if !operationStarted.Before(p.deadline) || p.ctx.Err() != nil {
			break
		}
		counts, err := p.operation(p.ctx, sequence)
		if err != nil {
			result.failed++
			result.err = err
			fmt.Fprintf(os.Stderr, "benchmark operation failed: %v\n", err)
			break
		}
		result.completed++
		result.requestMessages += counts.requestMessages
		result.responseMessages += counts.responseMessages
		if p.recordLatency {
			latency := uint64(max(time.Since(operationStarted).Nanoseconds(), 1))
			result.histogram[logLinearUpperBound(latency)]++
		}
		sequence += stride
	}
	p.results <- result
}

func (r *phaseResult) merge(lane laneResult) {
	r.completed += lane.completed
	r.failed += lane.failed
	r.requestMessages += lane.requestMessages
	r.responseMessages += lane.responseMessages
	if r.histogram == nil {
		r.histogram = map[uint64]uint64{}
	}
	for upperBound, count := range lane.histogram {
		r.histogram[upperBound] += count
	}
	if r.err == nil {
		r.err = lane.err
	}
}

func logLinearUpperBound(value uint64) uint64 {
	if value == 0 {
		value = 1
	}
	shift := max(bits.Len64(value)-1-9, 0)
	return (((value >> shift) + 1) << shift) - 1
}

func histogramEventBuckets(histogram map[uint64]uint64) []histogramBucket {
	upperBounds := make([]uint64, 0, len(histogram))
	for upperBound, count := range histogram {
		if count != 0 {
			upperBounds = append(upperBounds, upperBound)
		}
	}
	slices.Sort(upperBounds)
	buckets := make([]histogramBucket, 0, len(upperBounds))
	for _, upperBound := range upperBounds {
		buckets = append(buckets, histogramBucket{
			UpperBoundNS: strconv.FormatUint(upperBound, 10),
			Count:        strconv.FormatUint(histogram[upperBound], 10),
		})
	}
	return buckets
}

func sampleFromResult(config clientConfig, result phaseResult) sampleEvent {
	admissionNS := uint64(config.measurement)
	elapsedNS := uint64(max(result.elapsed.Nanoseconds(), 0))
	drainNS := uint64(0)
	if elapsedNS > admissionNS {
		drainNS = elapsedNS - admissionNS
	}
	return sampleEvent{
		SchemaVersion:    schemaVersion,
		Event:            "sample",
		Peer:             peerName,
		RPCKind:          config.rpc,
		AdmissionNS:      strconv.FormatUint(admissionNS, 10),
		ElapsedNS:        strconv.FormatUint(elapsedNS, 10),
		DrainNS:          strconv.FormatUint(drainNS, 10),
		Completed:        strconv.FormatUint(result.completed, 10),
		Failed:           strconv.FormatUint(result.failed, 10),
		RequestMessages:  strconv.FormatUint(result.requestMessages, 10),
		ResponseMessages: strconv.FormatUint(result.responseMessages, 10),
		Histogram:        histogramEventBuckets(result.histogram),
	}
}

func histogramCount(histogram map[uint64]uint64) uint64 {
	var total uint64
	for _, count := range histogram {
		if math.MaxUint64-total < count {
			return math.MaxUint64
		}
		total += count
	}
	return total
}
