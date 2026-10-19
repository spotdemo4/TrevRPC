package trevrpc

import "net/http"

// serveFallback serves an HTTP request that TrevRPC does not claim with
// ServerOptions.HTTPFallback, or answers 404 without one. The request holds a
// stream slot of its connection, so an overloaded connection answers 503
// rather than exceeding the transport's stream limit.
func (l *Listener) serveFallback(w http.ResponseWriter, r *http.Request) {
	fallback := l.runtime.options.HTTPFallback
	if fallback == nil {
		http.NotFound(w, r)
		return
	}
	slot, ok := acquireHTTPStreamSlot(r)
	if !ok {
		http.Error(w, "too many concurrent requests on HTTP connection", http.StatusServiceUnavailable)
		return
	}
	defer slot.release()
	fallback.ServeHTTP(w, r)
}
