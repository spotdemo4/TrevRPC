package trevrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// handlerReadAheadMessages and handlerReadAheadBytes bound how far the
	// handler's response stream is read ahead of the network writes: up to
	// this many messages, or this many bytes of message payload, are read
	// ahead, plus the one message that crosses either limit. The count is also
	// the most messages one write carries. The Rust runtime queues the same.
	handlerReadAheadMessages = 32
	handlerReadAheadBytes    = 64 * 1024

	// readAheadFastRecv is how long a Recv may take, on average, for the
	// handler to count as producing messages on its own, such as a stream that
	// generates them, a slice, or a channel that is full. The goroutine reads
	// such a handler ahead of the writer. A handler that takes longer is
	// waiting for something, usually the peer, as in a call that goes back and
	// forth one message at a time. Reading it ahead gains nothing and costs
	// scheduling, so it is read only when the writer asks for a message.
	readAheadFastRecv = 20 * time.Microsecond
	// readAheadSample is how many messages of a handler that is read ahead
	// pass between two measurements of its speed.
	readAheadSample = 8
)

// cacheLinePad keeps what two goroutines write on different cache lines.
type cacheLinePad [64]byte

// responseReadAhead reads the response stream of a call on a goroutine of its
// own, ahead of the writer, so that the writer can send everything that has
// queued in one write. It also ends a call on time when the stream does not:
// the writer stops waiting for a message when the call context ends, or when
// the stream is idle for longer than the idle timeout. A Recv that was given
// up on keeps running, and keeps its call's admission, until the stream
// returns, as with streamReceiver.
//
// Only the goroutine calls Recv, so Recv never runs concurrently with itself.
// The goroutine stops after the item that ends the stream (a failure or
// io.EOF), when the queue is closed, and when the call context ends.
//
// The queue is a channel, so that a writer that waits for a message is handed
// it directly. What the goroutine and the writer both touch for every message
// is kept to the channels: each of them writes only its own fields, which sit
// on cache lines of their own.
//
// The writer side (next, close) belongs to one goroutine.
type responseReadAhead struct {
	stream      encodedStream
	ctx         context.Context
	idleTimeout time.Duration
	lease       *requestExecutionLease
	// fastRecv is readAheadFastRecv, which tests change.
	fastRecv time.Duration

	// items is the queue. The goroutine holds the message it is about to
	// queue while the queue is full, which makes up the last of the
	// handlerReadAheadMessages.
	items chan readAheadItem
	// space wakes a goroutine that waits for the queued bytes to drop below
	// the limit, and demand carries the writer's request for a message to a
	// goroutine that reads on request only (see readAheadFastRecv).
	space  chan struct{}
	demand chan struct{}
	// done is closed by close.
	done chan struct{}

	_ cacheLinePad
	// state says what the goroutine is doing, for close, which detaches a
	// Recv in progress: execution stands for it while state is
	// readAheadReceiving. producerWaiting says that the goroutine waits on
	// space, and parked that it waits for a request (see demand), which the
	// writer takes back when it makes the request. They are written by the
	// goroutine.
	state           atomic.Int32
	producerWaiting atomic.Bool
	parked          atomic.Bool
	execution       executionReference

	_ cacheLinePad
	// takenBytes is the payload bytes of the items the writer has taken,
	// written by the writer, and read by the goroutine only when the queue may
	// be full.
	takenBytes atomic.Int64

	// The rest belongs to the writer.
	taken     int64
	blocked   bool
	started   bool
	closeOnce sync.Once
	timer     *time.Timer
	abandoned error
}

// The states of the goroutine of a responseReadAhead.
const (
	readAheadIdle int32 = iota
	readAheadReceiving
	readAheadClosed
)

// readAheadItem is what one Recv returned.
type readAheadItem struct {
	message encodedMessage
	err     error
}

func newResponseReadAhead(ctx context.Context, stream encodedStream, idleTimeout time.Duration, lease *requestExecutionLease) *responseReadAhead {
	return &responseReadAhead{
		stream:      stream,
		ctx:         ctx,
		idleTimeout: idleTimeout,
		lease:       lease,
		fastRecv:    readAheadFastRecv,
		done:        make(chan struct{}),
	}
}

// start starts the goroutine that reads the stream. Nothing is read before.
func (q *responseReadAhead) start() {
	if q.started {
		return
	}
	q.started = true
	q.items = make(chan readAheadItem, handlerReadAheadMessages-1)
	q.space = make(chan struct{}, 1)
	q.demand = make(chan struct{}, 1)
	if q.idleTimeout > 0 {
		q.timer = time.NewTimer(q.idleTimeout)
		q.timer.Stop()
	}
	go q.run()
}

func notify(wake chan struct{}) {
	select {
	case wake <- struct{}{}:
	default:
	}
}

func (q *responseReadAhead) run() {
	// The first message is read before the writer asks for it, so the handler
	// works while HEADERS are written.
	ahead := true
	var mark time.Time
	count, sampleEvery := 0, 1
	var pushedBytes, takenBytes int64
	for {
		if !ahead {
			select {
			case <-q.demand:
			case <-q.done:
				return
			case <-q.ctx.Done():
				return
			}
		} else if q.ctx.Err() != nil {
			return
		}

		// The speed of the handler is measured on a message in every
		// readAheadSample: the time one Recv took when messages are read on
		// request, and the time the messages took in all when they are read
		// ahead.
		sample := count+1 >= sampleEvery
		var began time.Time
		if sample && (!ahead || mark.IsZero()) {
			began = time.Now()
			if ahead {
				mark = began
			}
		}

		execution := q.lease.retainAs(&q.execution, ServerDiagnosticResponseStreamPanic)
		if !q.state.CompareAndSwap(readAheadIdle, readAheadReceiving) {
			execution.finish()
			return
		}
		message, err := recvByte(q.stream, "response")
		execution.finish()
		if !q.state.CompareAndSwap(readAheadReceiving, readAheadIdle) {
			return
		}

		count++
		if sample {
			now := time.Now()
			if ahead {
				ahead = now.Sub(mark) < time.Duration(count)*q.fastRecv
			} else {
				ahead = now.Sub(began) < q.fastRecv
			}
			mark, count, sampleEvery = now, 0, readAheadSample
		}

		// Queuing the item readies the writer, so it is the last thing done
		// before the goroutine waits for the writer. The goroutine says it
		// waits for a request before the writer can have the item, so that
		// the request is made for exactly this wait.
		if !ahead {
			q.parked.Store(true)
		}
		item := readAheadItem{message: message, err: err}
		pushedBytes += int64(message.size)
		select {
		case q.items <- item:
		default:
			// The queue is full.
			select {
			case q.items <- item:
			case <-q.done:
				return
			case <-q.ctx.Done():
				return
			}
		}
		if err != nil {
			// The stream is over.
			return
		}
		for pushedBytes-takenBytes >= handlerReadAheadBytes {
			takenBytes = q.takenBytes.Load()
			if pushedBytes-takenBytes < handlerReadAheadBytes {
				break
			}
			q.producerWaiting.Store(true)
			// The writer may have made room before it could know to say so.
			takenBytes = q.takenBytes.Load()
			if pushedBytes-takenBytes < handlerReadAheadBytes {
				q.producerWaiting.Store(false)
				break
			}
			select {
			case <-q.space:
			case <-q.done:
				return
			case <-q.ctx.Done():
				return
			}
			q.producerWaiting.Store(false)
		}
	}
}

// next returns the next item of the stream. If none is queued, it waits for
// one when wait is set, and otherwise reports false. It returns an error when
// the call context ends or the stream is idle for too long, and then reads
// nothing more.
func (q *responseReadAhead) next(wait bool) (readAheadItem, bool, error) {
	if q.abandoned != nil {
		return readAheadItem{}, false, q.abandoned
	}
	// A writer that has just had to wait is likely to wait again, and the wait
	// takes what is queued in any case.
	if !wait || !q.blocked {
		select {
		case item := <-q.items:
			q.blocked = false
			q.take(item)
			if err := q.ctx.Err(); err != nil {
				// Whatever is queued is dropped with the call.
				q.abandon(statusFromContextError(err))
				return readAheadItem{}, false, q.abandoned
			}
			return item, true, nil
		default:
		}
	}
	if !wait {
		return readAheadItem{}, false, nil
	}
	return q.await()
}

// take makes room for the goroutine, which may wait for it.
func (q *responseReadAhead) take(item readAheadItem) {
	q.taken += int64(item.message.size)
	q.takenBytes.Store(q.taken)
	if q.producerWaiting.Load() {
		notify(q.space)
	}
}

// await waits for the goroutine to queue an item. It fails when the call ends
// or the stream is idle, after giving up the stream.
func (q *responseReadAhead) await() (readAheadItem, bool, error) {
	// A goroutine that reads on request reads now. One that is still reading
	// will deliver without being asked.
	if q.parked.CompareAndSwap(true, false) {
		notify(q.demand)
	}
	var idle <-chan time.Time
	if q.idleTimeout > 0 {
		q.timer.Reset(q.idleTimeout)
		idle = q.timer.C
	}
	var err error
	select {
	case item := <-q.items:
		if idle != nil {
			q.timer.Stop()
		}
		q.blocked = true
		q.take(item)
		return item, true, nil
	case <-q.done:
		err = statusFromContextError(context.Canceled)
	case <-q.ctx.Done():
		err = statusFromContextError(q.ctx.Err())
	case <-idle:
		err = Unavailable("response stream idle timeout")
	}
	if idle != nil {
		q.timer.Stop()
	}
	q.abandon(err)
	return readAheadItem{}, false, err
}

// abandon gives up the stream: the Recv in progress runs on, detached.
func (q *responseReadAhead) abandon(err error) {
	q.abandoned = err
	cancelStreamRead(q.stream)
	q.shutdown(true)
}

// close drops what is queued and ends the goroutine, which a Recv in progress
// delays until the stream returns. It is safe to call repeatedly.
func (q *responseReadAhead) close() {
	q.shutdown(false)
}

func (q *responseReadAhead) shutdown(detach bool) {
	q.closeOnce.Do(func() {
		receiving := q.state.Swap(readAheadClosed) == readAheadReceiving
		close(q.done)
		for q.items != nil {
			select {
			case <-q.items:
				continue
			default:
			}
			break
		}
		// A Recv that outlives a call that was cancelled is detached.
		if receiving && q.lease != nil && (detach || q.ctx.Err() != nil) {
			q.execution.detach()
		}
	})
}
