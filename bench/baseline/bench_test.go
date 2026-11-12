package main

import (
	"context"
	"errors"
	"strings"
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
	result := runWorker(ctx, w, now, now.Add(time.Hour))
	if result.errors != 3 {
		t.Fatalf("errors = %d, want 3", result.errors)
	}
	if !errors.Is(result.firstError, first) {
		t.Fatalf("firstError = %v, want %v", result.firstError, first)
	}
}

func TestRunWorkerIgnoresErrorsDuringWarmup(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	warmup, measured := errors.New("warmup"), errors.New("measured")
	// The first call starts during the warmup and returns once the window has
	// begun, so the second starts inside it. The warmup leaves the first call
	// ample time to start.
	start := time.Now().Add(time.Second)
	w := &scriptedWorker{calls: []func() error{
		fail(warmup, func() { time.Sleep(time.Until(start)) }),
		fail(measured, cancel),
	}}
	result := runWorker(ctx, w, start, start.Add(time.Hour))
	if result.errors != 1 || !errors.Is(result.firstError, measured) {
		t.Fatalf("errors = %d, firstError = %v, want 1 and %v", result.errors, result.firstError, measured)
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
