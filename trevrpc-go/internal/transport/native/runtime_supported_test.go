//go:build cgo && linux && (amd64 || arm64)

package native

import (
	"testing"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

type adapterTestRuntime struct {
	trevrpcc.Runtime
	event trevrpcc.Event
}

func (r *adapterTestRuntime) NextEvent() (trevrpcc.Event, bool, error) {
	return r.event, true, nil
}

type adapterTestAdmission struct {
	request  trevrpcc.AdmissionRequest
	statuses []uint16
}

func (a *adapterTestAdmission) Request() trevrpcc.AdmissionRequest {
	return a.request
}

func (a *adapterTestAdmission) Respond(status uint16) error {
	a.statuses = append(a.statuses, status)
	return nil
}

func TestProviderRuntimeForwardsDetachedEventsAndOwnsAdmissionResponse(t *testing.T) {
	admission := &adapterTestAdmission{request: trevrpcc.AdmissionRequest{
		Kind:    trevrpcc.EventHTTP3Admission,
		Headers: []trevrpcc.AdmissionHeader{{Name: "x-test", Value: "original"}},
	}}
	data := []byte("diagnostic")
	runtime, err := newProviderRuntime(&adapterTestRuntime{event: trevrpcc.Event{
		Kind:      trevrpcc.EventHTTP3Admission,
		Data:      data,
		Admission: admission,
	}})
	if err != nil {
		t.Fatalf("newProviderRuntime() error = %v", err)
	}

	event, present, err := runtime.nextEvent()
	if err != nil || !present {
		t.Fatalf("nextEvent() = (%+v, %v, %v)", event, present, err)
	}
	if &event.data[0] != &data[0] {
		t.Fatal("event data was copied by the provider runtime adapter")
	}
	if &event.admission.request.Headers[0] != &admission.request.Headers[0] {
		t.Fatal("admission headers were copied by the provider runtime adapter")
	}
	if err := event.admission.respond(204); err != nil {
		t.Fatalf("respond() error = %v", err)
	}
	if err := event.admission.respond(500); err != nil {
		t.Fatalf("second respond() error = %v", err)
	}
	if len(admission.statuses) != 1 || admission.statuses[0] != 204 {
		t.Fatalf("admission statuses = %v, want [204]", admission.statuses)
	}
}
