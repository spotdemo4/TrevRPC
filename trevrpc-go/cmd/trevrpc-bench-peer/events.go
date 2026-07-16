package main

import (
	"bufio"
	"encoding/json"
	"io"
	"sync"
)

const schemaVersion = 3
const peerName = "go"

type eventEmitter struct {
	mu      sync.Mutex
	writer  *bufio.Writer
	encoder *json.Encoder
}

func newEventEmitter(writer io.Writer) *eventEmitter {
	buffered := bufio.NewWriter(writer)
	return &eventEmitter{writer: buffered, encoder: json.NewEncoder(buffered)}
}

func (e *eventEmitter) emit(event any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.encoder.Encode(event); err != nil {
		return err
	}
	return e.writer.Flush()
}

type capabilitiesEvent struct {
	SchemaVersion int      `json:"schema_version"`
	Event         string   `json:"event"`
	Peer          string   `json:"peer"`
	Roles         []string `json:"roles"`
	RPCKinds      []string `json:"rpc_kinds"`
	Stacks        []string `json:"stacks"`
	Histogram     string   `json:"histogram"`
}

type readyEvent struct {
	SchemaVersion int    `json:"schema_version"`
	Event         string `json:"event"`
	Peer          string `json:"peer"`
	Address       string `json:"address"`
	PID           int    `json:"pid"`
}

type processEvent struct {
	SchemaVersion int    `json:"schema_version"`
	Event         string `json:"event"`
	Peer          string `json:"peer"`
	PID           int    `json:"pid,omitempty"`
}

type histogramBucket struct {
	UpperBoundNS string `json:"upper_bound_ns"`
	Count        string `json:"count"`
}

type sampleEvent struct {
	SchemaVersion    int               `json:"schema_version"`
	Event            string            `json:"event"`
	Peer             string            `json:"peer"`
	RPCKind          rpcKind           `json:"rpc_kind"`
	AdmissionNS      string            `json:"admission_ns"`
	ElapsedNS        string            `json:"elapsed_ns"`
	DrainNS          string            `json:"drain_ns"`
	Completed        string            `json:"completed"`
	Failed           string            `json:"failed"`
	RequestMessages  string            `json:"request_messages"`
	ResponseMessages string            `json:"response_messages"`
	Histogram        []histogramBucket `json:"histogram"`
}

type errorEvent struct {
	SchemaVersion int    `json:"schema_version"`
	Event         string `json:"event"`
	Peer          string `json:"peer"`
	Phase         string `json:"phase"`
	Code          string `json:"code"`
	Message       string `json:"message"`
}
