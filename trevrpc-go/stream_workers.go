package trevrpc

import (
	"sync"
	"sync/atomic"
	"time"
)

// streamWorkerIdle is how long a stream worker may go without a stream before
// it exits.
const streamWorkerIdle = 10 * time.Second

// streamWorkers runs a listener's RPC streams, and the unary handlers and
// authorizers they start on goroutines of their own, on goroutines that
// outlive them. A new goroutine
// starts with a small stack, and every stream's and handler's grew it at least
// twice, copying it each time, which cost the server about a tenth of its CPU
// on small unary calls. A worker keeps the stack it grew for the next task.
// The pool grows to the most tasks that run at once, which the stream and
// request limits bound, and shrinks again as workers go idle.
type streamWorkers struct {
	// tasks hands a stream to a worker waiting for one. It is unbuffered, so a
	// send succeeds only while a worker waits.
	tasks chan func()
	// done ends idle workers when the listener shuts down.
	done <-chan struct{}
	// period is how long a worker may wait without a stream before it exits.
	period time.Duration

	// idle is closed, and replaced, every period while workers run: a worker
	// that ran no stream since the last close exits.
	idle atomic.Pointer[chan struct{}]

	mu      sync.Mutex
	workers int
	ticking bool
	// tick is the timer that closes idle, nil until the first worker starts.
	tick *time.Timer
	// stopped is set once done is closed, so that the timer stays stopped.
	stopped bool
}

// newStreamWorkers returns a pool whose workers exit when done is closed or
// after waiting period for a stream.
func newStreamWorkers(done <-chan struct{}, period time.Duration) *streamWorkers {
	p := &streamWorkers{tasks: make(chan func()), done: done, period: period}
	idle := make(chan struct{})
	p.idle.Store(&idle)
	return p
}

// run runs task on a waiting worker, or on a new one.
func (p *streamWorkers) run(task func()) {
	select {
	case p.tasks <- task:
	default:
		p.mu.Lock()
		p.workers++
		if !p.ticking && !p.stopped {
			p.ticking = true
			if p.tick == nil {
				p.tick = time.AfterFunc(p.period, p.expire)
			} else {
				p.tick.Reset(p.period)
			}
		}
		p.mu.Unlock()
		go p.work(task)
	}
}

func (p *streamWorkers) work(task func()) {
	defer func() {
		p.mu.Lock()
		p.workers--
		p.mu.Unlock()
	}()
	task()
	busy := true
	for {
		select {
		case task := <-p.tasks:
			task()
			busy = true
		case <-*p.idle.Load():
			if !busy {
				return
			}
			busy = false
		case <-p.done:
			return
		}
	}
}

// expire tells the workers that another idle period passed, and keeps
// telling them while any remain.
func (p *streamWorkers) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	idle := make(chan struct{})
	close(*p.idle.Swap(&idle))
	select {
	case <-p.done:
		p.stopped = true
		p.ticking = false
		return
	default:
	}
	if p.workers == 0 {
		p.ticking = false
		return
	}
	p.tick.Reset(p.period)
}
