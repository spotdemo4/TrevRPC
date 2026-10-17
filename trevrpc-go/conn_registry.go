package trevrpc

import "sync"

// connRegistry tracks the live connections of a listener, whatever their
// transport, so that shutdown can force them closed.
type connRegistry struct {
	mu    sync.Mutex
	next  uint64
	conns map[uint64]func(message string)
}

// add registers a connection by its close function and returns the function
// that unregisters it.
func (r *connRegistry) add(closeConn func(message string)) (remove func()) {
	r.mu.Lock()
	if r.conns == nil {
		r.conns = map[uint64]func(string){}
	}
	id := r.next
	r.next++
	r.conns[id] = closeConn
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.conns, id)
		r.mu.Unlock()
	}
}

// closeAll closes every registered connection with message.
func (r *connRegistry) closeAll(message string) {
	r.mu.Lock()
	closers := make([]func(string), 0, len(r.conns))
	for _, closeConn := range r.conns {
		closers = append(closers, closeConn)
	}
	r.mu.Unlock()
	for _, closeConn := range closers {
		closeConn(message)
	}
}
