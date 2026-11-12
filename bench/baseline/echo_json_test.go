package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
)

// flushRecorder records a response and, at each flush, how many complete
// lines had been written.
type flushRecorder struct {
	*httptest.ResponseRecorder

	flushedLines []int
}

func (r *flushRecorder) Flush() {
	r.flushedLines = append(r.flushedLines, bytes.Count(r.Body.Bytes(), []byte("\n")))
	r.ResponseRecorder.Flush()
}

// TestJSONServerStreamFlushesEachLine checks that the server stream flushes
// after every line, as connect-go flushes every streamed message, rather
// than leaving lines in net/http's buffer until the handler returns.
func TestJSONServerStreamFlushesEachLine(t *testing.T) {
	mux := http.NewServeMux()
	mountJSON(mux)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, jsonServerStreamPath,
		strings.NewReader(`{"message":"s","count":5}`))
	mux.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("answered %d: %q", recorder.Code, recorder.Body)
	}
	if want := []int{1, 2, 3, 4, 5}; !slices.Equal(recorder.flushedLines, want) {
		t.Fatalf("flushed after lines %v, want after each of %v", recorder.flushedLines, want)
	}
}
