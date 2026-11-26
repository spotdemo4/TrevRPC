package main

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
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
	result := runWorker(ctx, w, newWireSampler(new(wireCounter), 1, now, now.Add(time.Hour)), 0, now, now.Add(time.Hour), workerOptions{})
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
		result := runWorker(ctx, w, newWireSampler(new(wireCounter), 1, start, start.Add(time.Hour)), 0, start, start.Add(time.Hour), workerOptions{})
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

// deliveringWorker delivers one message on its call and then stops the run
// by cancelling it.
type deliveringWorker struct{ stop context.CancelFunc }

func (w deliveringWorker) do(context.Context) (int, error) {
	w.stop()
	return 1, nil
}
func (deliveringWorker) close() {}

// TestRunWorkerCountsDeliveredMessagesOutsideTheWindow runs a call before the
// window starts: it is not measured, but counts as delivered, which is what
// the wireSampler reads at the window's boundaries.
func TestRunWorkerCountsDeliveredMessagesOutsideTheWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	start := time.Now().Add(time.Hour)
	sampler := newWireSampler(new(wireCounter), 1, start, start.Add(time.Hour))
	result := runWorker(ctx, deliveringWorker{stop: cancel}, sampler, 0, start, start.Add(time.Hour), workerOptions{})
	if result.msgs != 0 {
		t.Fatalf("measured messages = %d, want 0 before the window", result.msgs)
	}
	if got := sampler.delivered[0].Load(); got != 1 {
		t.Fatalf("delivered messages = %d, want the call's 1", got)
	}
}

// TestWireSamplerTakesWhatChangedBetweenTheBoundaries samples a connection and
// workers that already carried bytes and messages before the window: the
// sample holds only what changed between the start and the end sample.
func TestWireSamplerTakesWhatChangedBetweenTheBoundaries(t *testing.T) {
	start := time.Now()
	end := start.Add(time.Second)
	var counter wireCounter
	sampler := newWireSampler(&counter, 2, start, end)
	counter.written.Store(500)
	sampler.deliver(0, 7)

	sampler.observe(start.Add(-time.Millisecond))
	counter.written.Store(600)
	sampler.observe(start)
	counter.written.Store(900)
	sampler.deliver(1, 3)
	sampler.observe(end)

	if got, want := sampler.sample(), (wireSample{bytes: 300, msgs: 3}); got != want {
		t.Fatalf("sample = %+v, want %+v", got, want)
	}
}

// TestWireSamplerKeepsTheFirstObservationOfEachBoundary has the start and the
// end observed again after their samples were taken, as the late main
// goroutine does after a worker saw them pass: the later observations change
// nothing, and an observation past the end takes a missing start sample first.
func TestWireSamplerKeepsTheFirstObservationOfEachBoundary(t *testing.T) {
	start := time.Now()
	end := start.Add(time.Second)
	var counter wireCounter
	sampler := newWireSampler(&counter, 1, start, end)
	sampler.observe(end.Add(time.Millisecond))
	counter.written.Store(100)
	sampler.deliver(0, 1)
	sampler.observe(start)
	sampler.observe(end)
	if got := sampler.sample(); got != (wireSample{}) {
		t.Fatalf("sample = %+v, want the first observation's, which saw nothing carried", got)
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

// TestTimelineCountsMessagesInTheBucketOfTheirInstant checks the buckets of a
// 250 ms window cut into 100 ms intervals: three, the last one short, and an
// instant before the window or at or after its end counts nowhere.
func TestTimelineCountsMessagesInTheBucketOfTheirInstant(t *testing.T) {
	start := time.Now()
	end := start.Add(250 * time.Millisecond)
	tl := newTimeline(start, end, 100*time.Millisecond)
	tl.add(start.Add(-time.Nanosecond), 7)
	tl.add(start, 1)
	tl.add(start.Add(99*time.Millisecond), 2)
	tl.add(start.Add(100*time.Millisecond), 4)
	tl.add(start.Add(249*time.Millisecond), 8)
	tl.add(end, 16)
	tl.add(end.Add(time.Second), 32)
	if want := []uint64{3, 4, 8}; !slices.Equal(tl.msgs, want) {
		t.Fatalf("buckets = %v, want %v", tl.msgs, want)
	}
	(*timeline)(nil).add(start, 1) // a nil timeline counts nothing

	for _, test := range []struct {
		window, bucket time.Duration
		want           int
	}{
		{300 * time.Millisecond, 100 * time.Millisecond, 3},
		{300*time.Millisecond + 3, 100 * time.Millisecond, 3},
		{300*time.Millisecond - 3, 100 * time.Millisecond, 3},
		{301 * time.Millisecond, 100 * time.Millisecond, 4},
		{50 * time.Millisecond, time.Second, 1},
	} {
		if got := len(newTimeline(start, start.Add(test.window), test.bucket).msgs); got != test.want {
			t.Errorf("%s window in %s buckets: %d buckets, want %d", test.window, test.bucket, got, test.want)
		}
	}
}

// delayWorker delivers one message after delay on every call.
type delayWorker struct{ delay time.Duration }

func (w delayWorker) do(ctx context.Context) (int, error) {
	if !sleepUntil(ctx, time.Now().Add(w.delay)) {
		return 0, ctx.Err()
	}
	return 1, nil
}
func (delayWorker) close() {}

// TestRunWorkerCountsSlowOperationsAndTimelineMessages runs calls of 20 ms
// back to back, the first beginning before the window and ending in it: it
// counts in the timeline when it ends, but is no measured operation.
func TestRunWorkerCountsSlowOperationsAndTimelineMessages(t *testing.T) {
	for _, test := range []struct {
		name     string
		cfg      benchConfig
		wantSlow func(ops int) int
	}{
		{"every operation is slow", benchConfig{slowUS: 1, timelineMS: 100}, func(ops int) int { return ops }},
		{"no operation is slow", benchConfig{slowUS: 3600 * 1e6, timelineMS: 100}, func(int) int { return 0 }},
		{"off", benchConfig{}, func(int) int { return 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			start := time.Now().Add(10 * time.Millisecond)
			end := start.Add(300 * time.Millisecond)
			ctx, cancel := context.WithDeadline(t.Context(), end.Add(time.Second))
			defer cancel()
			options := newWorkerOptions(test.cfg, start, end)
			result := runWorker(ctx, delayWorker{delay: 20 * time.Millisecond}, newWireSampler(new(wireCounter), 1, start, end), 0, start, end, options)
			if len(result.latencies) == 0 {
				t.Fatal("no operation was measured")
			}
			if want := uint64(test.wantSlow(len(result.latencies))); result.slow != want {
				t.Errorf("slow = %d of %d operations, want %d", result.slow, len(result.latencies), want)
			}
			if test.cfg.timelineMS == 0 {
				if result.timeline != nil {
					t.Errorf("timeline = %v, want none", result.timeline.msgs)
				}
				return
			}
			var total uint64
			for _, count := range result.timeline.msgs {
				total += count
			}
			if total != result.msgs+1 {
				t.Errorf("timeline %v holds %d messages, want the %d measured plus the one that began early", result.timeline.msgs, total, result.msgs)
			}
			if len(result.timeline.msgs) != 3 {
				t.Errorf("timeline has %d buckets, want 3", len(result.timeline.msgs))
			}
			// 20 ms calls fill every 100 ms interval.
			for i, count := range result.timeline.msgs {
				if count == 0 {
					t.Errorf("bucket %d of %v is empty", i, result.timeline.msgs)
				}
			}
		})
	}
}

// TestSummarizeAddsUpTimelinesAndOmitsFieldsThatAreOff checks the fields of
// the result line: the workers' timelines add up, and slow and timeline are
// absent without their flags.
func TestSummarizeAddsUpTimelinesAndOmitsFieldsThatAreOff(t *testing.T) {
	start := time.Now()
	end := start.Add(200 * time.Millisecond)
	cfg := benchConfig{duration: 200 * time.Millisecond, slowUS: 500, timelineMS: 100}
	first, second := newTimeline(start, end, 100*time.Millisecond), newTimeline(start, end, 100*time.Millisecond)
	first.add(start, 2)
	second.add(start.Add(150*time.Millisecond), 5)
	results := []workerResult{
		{latencies: []time.Duration{time.Millisecond}, msgs: 2, slow: 1, timeline: first},
		{latencies: []time.Duration{time.Millisecond}, msgs: 5, slow: 2, timeline: second},
	}
	res, err := summarize(cfg, results, wireSample{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Slow == nil || *res.Slow != 3 {
		t.Errorf("slow = %v, want 3", res.Slow)
	}
	want := &timelineResult{StartUnixUS: start.UnixMicro(), BucketMS: 100, Msgs: []uint64{2, 5}}
	if res.Timeline == nil || res.Timeline.StartUnixUS != want.StartUnixUS || res.Timeline.BucketMS != 100 || !slices.Equal(res.Timeline.Msgs, want.Msgs) {
		t.Errorf("timeline = %+v, want %+v", res.Timeline, want)
	}

	var line strings.Builder
	if err := res.write(&line); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(line.String(), `"slow":3`) || !strings.Contains(line.String(), `"timeline":{"start_unix_us":`+strconv.FormatInt(start.UnixMicro(), 10)+`,"bucket_ms":100,"msgs":[2,5]}`) {
		t.Errorf("result line %s lacks slow or timeline", line.String())
	}

	// A zero slowUS is off even when a worker counted slow calls, and a worker
	// without a timeline reports none.
	res, err = summarize(benchConfig{duration: time.Second}, []workerResult{{latencies: []time.Duration{time.Millisecond}, msgs: 1, slow: 1}}, wireSample{})
	if err != nil {
		t.Fatal(err)
	}
	line.Reset()
	if err := res.write(&line); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(line.String(), `"slow"`) || strings.Contains(line.String(), `"timeline"`) {
		t.Errorf("result line %s has slow or timeline without their flags", line.String())
	}
}

// TestBenchReportsSlowAndTimelineOnlyWhenAsked runs a bench over each
// transport with and without --slow-us and --timeline-ms.
func TestBenchReportsSlowAndTimelineOnlyWhenAsked(t *testing.T) {
	srv := startServer(t)
	for _, transport := range transports {
		t.Run(transport, func(t *testing.T) {
			t.Parallel()
			args := []string{
				"bench", "--addr", srv.addrs[transport], "--ca", srv.ca, "--transport", transport,
				"--scenario", "unary", "--concurrency", "2", "--duration", "0.22", "--warmup", "0.05",
			}
			var off map[string]json.RawMessage
			if err := json.Unmarshal(benchLine(t, args...), &off); err != nil {
				t.Fatal(err)
			}
			for _, field := range []string{"slow", "timeline"} {
				if _, ok := off[field]; ok {
					t.Errorf("result has %q without its flag", field)
				}
			}

			before := time.Now()
			var on struct {
				Ops, Msgs uint64
				Slow      *uint64
				Timeline  *timelineResult
			}
			if err := json.Unmarshal(benchLine(t, append(args, "--slow-us", "1", "--timeline-ms", "50")...), &on); err != nil {
				t.Fatal(err)
			}
			// Every call takes more than a microsecond.
			if on.Slow == nil || *on.Slow != on.Ops {
				t.Errorf("slow = %v, want all %d operations", on.Slow, on.Ops)
			}
			if on.Timeline == nil {
				t.Fatal("result has no timeline")
			}
			if on.Timeline.BucketMS != 50 || len(on.Timeline.Msgs) != 5 {
				t.Errorf("timeline = %d ms buckets, %d of them, want 50 ms and ceil(220/50) = 5", on.Timeline.BucketMS, len(on.Timeline.Msgs))
			}
			if got := time.UnixMicro(on.Timeline.StartUnixUS); got.Before(before) || got.After(time.Now().Add(time.Second)) {
				t.Errorf("timeline starts at %s, want between %s and a second after the run", got, before)
			}
			var total uint64
			for _, count := range on.Timeline.Msgs {
				total += count
			}
			if total < on.Msgs || total > on.Msgs+2 {
				t.Errorf("timeline holds %d messages, want %d measured plus at most the 2 that began before the window", total, on.Msgs)
			}

			var none struct{ Slow *uint64 }
			if err := json.Unmarshal(benchLine(t, append(args, "--slow-us", "3600000000")...), &none); err != nil {
				t.Fatal(err)
			}
			if none.Slow == nil || *none.Slow != 0 {
				t.Errorf("slow = %v with an hour's limit, want 0 present", none.Slow)
			}
		})
	}
}

func TestBenchRejectsNegativeSlowAndTimeline(t *testing.T) {
	for _, flag := range []string{"--slow-us", "--timeline-ms"} {
		args := []string{"--addr", "127.0.0.1:1", "--ca", "ca.pem", "--scenario", "unary", flag, "-1"}
		if _, err := parseBenchFlags(args); err == nil {
			t.Errorf("parseBenchFlags(%v) accepted a negative value", args)
		}
	}
}
