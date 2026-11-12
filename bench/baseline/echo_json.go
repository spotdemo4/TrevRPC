package main

import (
	"encoding/json"
	"net/http"
)

// The routes of the JSON API, which the browser benchmark's json lanes call
// with plain fetch: the way most web apps get data from their server, with no
// RPC framework on either end.
const (
	jsonUnaryPath        = "/json/unary"
	jsonServerStreamPath = "/json/server-stream"
)

// jsonRequest and jsonResponse are EchoRequest and EchoResponse as the JSON
// API spells them, with only the fields the benchmark uses.
type jsonRequest struct {
	Message string `json:"message"`
	Count   uint32 `json:"count,omitempty"`
}

type jsonResponse struct {
	Message string `json:"message"`
	Index   uint32 `json:"index"`
}

// mountJSON adds the JSON API to mux, written as a typical net/http service
// is: encoding/json in each direction, uncompressed. POST /json/unary echoes
// the request's message; POST /json/server-stream answers with count
// responses as newline-delimited JSON, flushing each line as it is written,
// as connect-go flushes each streamed message.
func mountJSON(mux *http.ServeMux) {
	mux.HandleFunc("POST "+jsonUnaryPath, func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeJSONRequest(w, r)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jsonResponse{Message: req.Message})
	})
	mux.HandleFunc("POST "+jsonServerStreamPath, func(w http.ResponseWriter, r *http.Request) {
		req, ok := decodeJSONRequest(w, r)
		if !ok {
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		encoder := json.NewEncoder(w)
		flusher := http.NewResponseController(w)
		resp := jsonResponse{Message: req.Message}
		for i := range req.Count {
			resp.Index = i
			if encoder.Encode(resp) != nil || flusher.Flush() != nil {
				return
			}
		}
	})
}

// decodeJSONRequest reads r's body as a jsonRequest, or answers 400 and
// reports false when it is not one.
func decodeJSONRequest(w http.ResponseWriter, r *http.Request) (jsonRequest, bool) {
	var req jsonRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return req, false
	}
	return req, true
}
