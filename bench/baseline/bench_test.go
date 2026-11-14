package main

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// scriptedWorker runs the next of calls on each call.
type scriptedWorker struct {
	calls []func() error
}

func (w *scriptedWorker) do(context.Context) (int, error) {
	call := w.calls[0]
	w.calls = w.calls[1:]
	return 0, call()
}

func (*scriptedWorker) close() {}

// fail returns a call that fails with err, first running each of then.
func fail(err error, then ...func()) func() error {
	return func() error {
		for _, f := range then {
			f()
		}
		return err
	}
}

func TestRunWorkerKeepsTheFirstErrorInTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	first, later := errors.New("first"), errors.New("later")
	w := &scriptedWorker{calls: []func() error{fail(first), fail(later), fail(later, cancel)}}
	now := time.Now()
	result := runWorker(ctx, w, new(atomic.Uint64), now, now.Add(time.Hour))
	if result.errors != 3 {
		t.Fatalf("errors = %d, want 3", result.errors)
	}
	if !errors.Is(result.firstError, first) {
		t.Fatalf("firstError = %v, want %v", result.firstError, first)
	}
}

func TestRunWorkerIgnoresErrorsDuringWarmup(t *testing.T) {
	warmup, measured := errors.New("warmup"), errors.New("measured")
	// The first call starts during the warmup and returns once the window has
	// begun, so the second starts inside it. A first call that only got to run
	// once the window had begun did not start during the warmup, so that
	// attempt proves nothing and runs again with a new window.
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now().Add(100 * time.Millisecond)
		var startedInWarmup bool
		w := &scriptedWorker{calls: []func() error{
			fail(warmup, func() {
				startedInWarmup = time.Now().Before(start)
				time.Sleep(time.Until(start))
			}),
			fail(measured, cancel),
		}}
		result := runWorker(ctx, w, new(atomic.Uint64), start, start.Add(time.Hour))
		cancel()
		if !startedInWarmup {
			if attempt == 10 {
				t.Fatal("the first call never started during the warmup")
			}
			continue
		}
		if result.errors != 1 || !errors.Is(result.firstError, measured) {
			t.Fatalf("errors = %d, firstError = %v, want 1 and %v", result.errors, result.firstError, measured)
		}
		return
	}
}

func TestReportErrorsPrintsEachWorkersFirstError(t *testing.T) {
	var out strings.Builder
	reportErrors(&out, []workerResult{
		{errors: 3, firstError: errors.New("rpc error: code = Unavailable")},
		{latencies: []time.Duration{time.Millisecond}},
		{errors: 1, firstError: context.DeadlineExceeded},
	})
	want := "worker 0: 3 call(s) failed, first: rpc error: code = Unavailable\n" +
		"worker 2: 1 call(s) failed, first: context deadline exceeded\n"
	if out.String() != want {
		t.Fatalf("reportErrors printed\n%s\nwant\n%s", out.String(), want)
	}
}

// deliveringWorker delivers one message on each call.
type deliveringWorker struct{}

func (deliveringWorker) do(context.Context) (int, error) { return 1, nil }
func (deliveringWorker) close()                          {}

// TestRunWorkerCountsDeliveredMessagesOutsideTheWindow runs calls before the
// window starts: none is measured, but each counts as delivered, which is
// what snapshotWire reads at its instants.
func TestRunWorkerCountsDeliveredMessagesOutsideTheWindow(t *testing.T) {
	var delivered atomic.Uint64
	now := time.Now()
	result := runWorker(t.Context(), deliveringWorker{}, &delivered, now.Add(time.Hour), now.Add(10*time.Millisecond))
	if result.msgs != 0 {
		t.Fatalf("measured messages = %d, want 0 before the window", result.msgs)
	}
	if delivered.Load() == 0 {
		t.Fatal("delivered messages = 0, want every call's")
	}
}

// TestSnapshotWireTakesWhatChangedBetweenItsInstants samples a connection and
// workers that already carried bytes and messages before the window: the
// sample holds only what changed between its two instants, here nothing.
func TestSnapshotWireTakesWhatChangedBetweenItsInstants(t *testing.T) {
	var counter wireCounter
	counter.written.Store(500)
	delivered := make([]atomic.Uint64, 2)
	delivered[0].Store(7)
	delivered[1].Store(3)
	now := time.Now()
	sample, ok := snapshotWire(t.Context(), &counter, delivered, now, now)
	if !ok {
		t.Fatal("snapshotWire did not sample")
	}
	if sample != (wireSample{}) {
		t.Fatalf("sample = %+v, want nothing carried between its instants", sample)
	}
}

// TestSummarizeDividesWireBytesByTheSampledMessages divides the sample's bytes
// by the messages delivered between its instants, not by the window's msgs:
// the sampler wakes late on a busy machine, so the two cover different
// intervals.
func TestSummarizeDividesWireBytesByTheSampledMessages(t *testing.T) {
	results := []workerResult{{latencies: []time.Duration{time.Millisecond}, msgs: 10}}
	res, err := summarize(benchConfig{}, results, wireSample{bytes: 1000, msgs: 4})
	if err != nil {
		t.Fatal(err)
	}
	if res.WireBytesPerMsg == nil || *res.WireBytesPerMsg != 250 {
		t.Fatalf("wire bytes per message = %v, want 250", res.WireBytesPerMsg)
	}
	// A sampler starved past the window saw no message: the window's count is
	// the only one.
	res, err = summarize(benchConfig{}, results, wireSample{bytes: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if res.WireBytesPerMsg == nil || *res.WireBytesPerMsg != 100 {
		t.Fatalf("wire bytes per message with no message sampled = %v, want 100", res.WireBytesPerMsg)
	}
}
