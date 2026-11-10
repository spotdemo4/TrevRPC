package trevrpc

import "net/http"

// serveUnclaimed serves an HTTP request that TrevRPC does not claim with
// ServerOptions.unclaimedHandler: the development certificate's hashes, or
// ServerOptions.HTTPFallback, or a 404 without one. The request holds a stream
// slot of its connection, so an overloaded connection answers 503 rather than
// exceeding the transport's stream limit.
func (l *Listener) serveUnclaimed(w http.ResponseWriter, r *http.Request) {
	handler := l.runtime.options.unclaimedHandler(r.URL.Path)
	if handler == nil {
		http.NotFound(w, r)
		return
	}
	slot, ok := acquireHTTPStreamSlot(r)
	if !ok {
		http.Error(w, "too many concurrent requests on HTTP connection", http.StatusServiceUnavailable)
		return
	}
	defer slot.release()
	handler.ServeHTTP(w, r)
}
