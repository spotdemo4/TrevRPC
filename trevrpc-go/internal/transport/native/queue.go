package native

import (
	"context"
	"sync"
)

type objectQueue[T any] struct {
	mu     sync.Mutex
	items  []T
	done   bool
	err    error
	notify chan struct{}
}

func newObjectQueue[T any]() objectQueue[T] {
	return objectQueue[T]{notify: make(chan struct{})}
}

func (q *objectQueue[T]) push(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.done {
		return false
	}
	q.items = append(q.items, item)
	q.signalLocked()
	return true
}

func (q *objectQueue[T]) pop(ctx context.Context) (T, error) {
	var zero T
	for {
		q.mu.Lock()
		if len(q.items) != 0 {
			item := q.items[0]
			var empty T
			q.items[0] = empty
			q.items = q.items[1:]
			q.mu.Unlock()
			return item, nil
		}
		if q.done {
			err := q.err
			q.mu.Unlock()
			return zero, err
		}
		notify := q.notify
		q.mu.Unlock()

		select {
		case <-notify:
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
}

func (q *objectQueue[T]) close(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.done {
		return
	}
	q.done = true
	q.err = err
	q.signalLocked()
}

func (q *objectQueue[T]) signalLocked() {
	close(q.notify)
	q.notify = make(chan struct{})
}
