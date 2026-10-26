package trevrpc

import (
	"sync"
	"testing"
	"time"
)

func (p *streamWorkers) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.workers
}

func waitForWorkers(t *testing.T, p *streamWorkers, want int) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for p.count() != want {
		if time.Now().After(deadline) {
			t.Fatalf("workers = %d, want %d", p.count(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestStreamWorkersReuseIdleWorkers(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	p := newStreamWorkers(done, time.Hour)
	for range 100 {
		ran := make(chan struct{})
		p.run(func() { close(ran) })
		<-ran
		// Let the worker get back to waiting before the next stream.
		for p.count() == 0 {
			time.Sleep(time.Microsecond)
		}
		time.Sleep(100 * time.Microsecond)
	}
	if got := p.count(); got != 1 {
		t.Fatalf("workers = %d after streams run one at a time, want 1", got)
	}
}

func TestStreamWorkersGrowToConcurrentStreamsAndExitWhenIdle(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	p := newStreamWorkers(done, 20*time.Millisecond)
	release := make(chan struct{})
	var running sync.WaitGroup
	running.Add(8)
	for range 8 {
		p.run(func() {
			running.Done()
			<-release
		})
	}
	running.Wait()
	if got := p.count(); got != 8 {
		t.Fatalf("workers = %d with 8 streams running, want 8", got)
	}
	close(release)
	// Idle for two periods: the first marks them idle, the second ends them.
	waitForWorkers(t, p, 0)

	// The pool starts again afterwards.
	ran := make(chan struct{})
	p.run(func() { close(ran) })
	<-ran
	waitForWorkers(t, p, 0)
}

func TestStreamWorkersExitWhenTheListenerStops(t *testing.T) {
	done := make(chan struct{})
	p := newStreamWorkers(done, time.Hour)
	for range 4 {
		p.run(func() {})
	}
	waitForWorkersAtLeast := time.Now().Add(testTimeout)
	for p.count() == 0 && time.Now().Before(waitForWorkersAtLeast) {
		time.Sleep(time.Millisecond)
	}
	close(done)
	waitForWorkers(t, p, 0)
}
