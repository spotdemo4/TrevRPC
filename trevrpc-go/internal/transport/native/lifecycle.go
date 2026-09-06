package native

import "sync"

type lifecycle struct {
	once sync.Once
	mu   sync.Mutex
	err  error
	done chan struct{}
}

func newLifecycle() lifecycle {
	return lifecycle{done: make(chan struct{})}
}

func (l *lifecycle) finish(err error) {
	l.once.Do(func() {
		l.mu.Lock()
		l.err = err
		l.mu.Unlock()
		close(l.done)
	})
}

func (l *lifecycle) Done() <-chan struct{} {
	return l.done
}

func (l *lifecycle) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.err
}
