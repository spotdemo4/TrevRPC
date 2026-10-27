package trevrpc

import (
	"context"
	"sync"
	"time"
)

// streamWorkerIdle is how long a stream worker may go without a stream before
// it exits.
const streamWorkerIdle = 10 * time.Second

// streamWorkers runs a listener's RPC streams, and the unary handlers and
// authorizers they start on goroutines of their own, on goroutines that
// outlive them. A new goroutine starts with a small stack, and every stream's
// and handler's grew it at least twice, copying it each time, which cost the
// server about a tenth of its CPU on small unary calls. A worker keeps the
// stack it grew for the next task. The pool grows to the most tasks that run
// at once, which the stream and request limits bound, and shrinks again as
// workers go idle.
//
// Idle workers wait on channels of their own, last in first out, so the next
// task goes to the worker whose stack is warmest, and a waiting worker does a
// plain receive: a select over a shared task channel, the idle period and
// shutdown cost the server 5% of its CPU.
type streamWorkers struct {
	// period is how long a worker may wait without a task before it exits.
	period time.Duration

	mu sync.Mutex
	// idle are the workers waiting for a task, the most recent last.
	idle    []*streamWorker
	workers int
	// tick counts the periods that passed; a worker remembers the tick it
	// went idle at.
	tick    uint64
	ticking bool
	// timer ends idle periods, nil until the first worker starts.
	timer *time.Timer
	// stopped is set once the listener shuts down: workers exit instead of
	// waiting for another task.
	stopped bool
}

// streamWorker is a worker waiting for its next task, or for nil to exit.
type streamWorker struct {
	tasks chan func()
	// idleSince is the tick at which it went idle.
	idleSince uint64
}

// newStreamWorkers returns a pool whose workers exit once ctx is done, or
// after waiting period for a task.
func newStreamWorkers(ctx context.Context, period time.Duration) *streamWorkers {
	p := &streamWorkers{period: period}
	context.AfterFunc(ctx, p.stop)
	return p
}

// run runs task on the most recently idle worker, or on a new one.
func (p *streamWorkers) run(task func()) {
	p.mu.Lock()
	if n := len(p.idle); n > 0 {
		worker := p.idle[n-1]
		p.idle[n-1] = nil
		p.idle = p.idle[:n-1]
		p.mu.Unlock()
		worker.tasks <- task
		return
	}
	p.workers++
	if !p.ticking && !p.stopped {
		p.ticking = true
		if p.timer == nil {
			p.timer = time.AfterFunc(p.period, p.expire)
		} else {
			p.timer.Reset(p.period)
		}
	}
	p.mu.Unlock()
	go p.work(task)
}

func (p *streamWorkers) work(task func()) {
	// A task that ends its goroutine (runtime.Goexit, as t.FailNow does) must
	// still leave the pool, or its count, and the idle timer, never settle.
	left := false
	defer func() {
		if !left {
			p.mu.Lock()
			p.workers--
			p.mu.Unlock()
		}
	}()
	worker := &streamWorker{tasks: make(chan func(), 1)}
	for task != nil {
		task()
		if !p.park(worker) {
			left = true
			return
		}
		task = <-worker.tasks
	}
}

// park makes worker idle, or reports false when the pool stopped, in which
// case the worker has left it.
func (p *streamWorkers) park(worker *streamWorker) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		p.workers--
		return false
	}
	worker.idleSince = p.tick
	p.idle = append(p.idle, worker)
	return true
}

// expire ends the workers that stayed idle for a whole period, and keeps the
// timer going while any workers remain.
func (p *streamWorkers) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.tick++
	// The least recently idle come first: retire them up to the first that
	// went idle within the last period.
	retired := 0
	for _, worker := range p.idle {
		if p.tick-worker.idleSince < 2 {
			break
		}
		worker.tasks <- nil
		retired++
	}
	remaining := copy(p.idle, p.idle[retired:])
	clear(p.idle[remaining:])
	p.idle = p.idle[:remaining]
	if p.stopped || p.workers == 0 {
		p.ticking = false
		return
	}
	p.timer.Reset(p.period)
}

// stop ends the idle workers and makes the others exit after their tasks.
func (p *streamWorkers) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	for _, worker := range p.idle {
		worker.tasks <- nil
	}
	p.idle = nil
	if p.timer != nil {
		p.timer.Stop()
	}
	p.ticking = false
}
