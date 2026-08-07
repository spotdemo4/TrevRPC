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

	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-bench-peer/benchmarkpb"
)

type operationCounts struct {
	requestMessages  uint64
	responseMessages uint64
}

type benchmarkOperation func(context.Context, uint64) (operationCounts, error)

type benchmarkClient interface {
	Unary(context.Context, *benchmarkpb.BenchmarkRequest) (*benchmarkpb.BenchmarkResponse, error)
	ClientStream(context.Context) (benchmarkClientStream, error)
	ServerStream(context.Context, *benchmarkpb.StreamRequest) (benchmarkResponseStream, error)
	Bidi(context.Context) (benchmarkBidiStream, error)
}

type benchmarkClientStream interface {
	Send(*benchmarkpb.BenchmarkRequest) error
	CloseAndRecv() (*benchmarkpb.BenchmarkSummary, error)
}

type benchmarkResponseStream interface {
	Recv() (*benchmarkpb.BenchmarkResponse, error)
}

type benchmarkBidiStream interface {
	Send(*benchmarkpb.BenchmarkRequest) error
	Recv() (*benchmarkpb.BenchmarkResponse, error)
	CloseSend() error
}

func newBenchmarkOperation(client benchmarkClient, config clientConfig) benchmarkOperation {
	switch config.rpc {
	case rpcUnary:
		return func(ctx context.Context, sequence uint64) (operationCounts, error) {
			request := newBenchmarkRequest(sequence, config)
			response, err := client.Unary(ctx, request)
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
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			call, err := client.ClientStream(callCtx)
			if err != nil {
				return operationCounts{}, err
			}
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
			callCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			request := &benchmarkpb.StreamRequest{
				MessageCount:  config.messagesPerStream,
				Payload:       make([]byte, int(config.requestBytes)),
				ResponseBytes: config.responseBytes,
			}
			responses, err := client.ServerStream(callCtx, request)
			if err != nil {
				return operationCounts{}, err
			}
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
			call, err := client.Bidi(callCtx)
			if err != nil {
				return operationCounts{}, err
			}
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
	now           func() time.Time
}

func preparePhase(ctx context.Context, concurrency int, operation benchmarkOperation, recordLatency bool) *preparedPhase {
	return preparePhaseWithNow(ctx, concurrency, operation, recordLatency, nil)
}

func preparePhaseWithNow(ctx context.Context, concurrency int, operation benchmarkOperation, recordLatency bool, now func() time.Time) *preparedPhase {
	phase := &preparedPhase{
		ctx:           ctx,
		operation:     operation,
		concurrency:   concurrency,
		recordLatency: recordLatency,
		gate:          make(chan struct{}),
		results:       make(chan laneResult, concurrency),
		now:           now,
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
	now := p.now
	if now == nil {
		now = time.Now
	}
	p.startedAt = now()
	p.deadline = p.startedAt.Add(duration)
	close(p.gate)
	isRealClock := p.now == nil
	if isRealClock {
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
	} else {
		// Fake clock: admission window is driven by the injected clock.
		// Don't block on real time; lanes will observe deadline via p.now().
		select {
		case <-p.ctx.Done():
		default:
		}
	}
	p.done.Wait()
	elapsed := max(now().Sub(p.startedAt), time.Duration(0))
	// For the real clock, ensure wall time is at least as large as the
	// recorded elapsed (covers drain beyond the admission window).
	if isRealClock {
		elapsed = max(elapsed, time.Since(p.startedAt))
	}
	result := phaseResult{startedAt: p.startedAt, elapsed: elapsed}
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

	now := p.now
	if now == nil {
		now = time.Now
	}
	result := laneResult{histogram: map[uint64]uint64{}}
	sequence := uint64(lane)
	stride := uint64(p.concurrency)
	first := true
	for {
		operationStarted := now()
		if !first {
			if !operationStarted.Before(p.deadline) || p.ctx.Err() != nil {
				break
			}
		} else {
			if p.ctx.Err() != nil {
				break
			}
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
			// Use the same clock for latency measurement; for the real clock
			// this is equivalent to time.Since(operationStarted).
			latency := max(now().Sub(operationStarted).Nanoseconds(), int64(1))
			result.histogram[logLinearUpperBound(uint64(latency))]++
		}
		sequence += stride
		first = false
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
