package main

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestServerStateTerminalStatusAndStableEOF(t *testing.T) {
	result, failure := runServerState([][]byte{mustHex(t, "22031a0161"), mustHex(t, "08012a0a0a057472616365120107")})
	if failure != nil {
		t.Fatalf("state: %v", failure)
	}
	events := result["events"].([]stateEvent)
	if len(events) != 3 || events[0].BodyHex != "1a0161" || events[1].Event != "eof" || events[2].Event != "eof" {
		t.Fatalf("events: %#v", events)
	}
	if result["transport_close_count"] != "1" {
		t.Fatalf("close count: %v", result)
	}
}

func TestStatePayloadUsesDeclaredSchema(t *testing.T) {
	descriptor := (&StatePayload{}).ProtoReflect().Descriptor()
	if descriptor.FullName() != "trevrpc.conformance.StatePayload" {
		t.Fatalf("StatePayload full name = %q", descriptor.FullName())
	}
	body := descriptor.Fields().ByNumber(3)
	if body == nil || body.Name() != "body" || body.Kind() != protoreflect.BytesKind {
		t.Fatalf("StatePayload body descriptor = %#v", body)
	}
}

func TestStatePayloadCanonicalizationOmitsUnknownFields(t *testing.T) {
	frames := [][]byte{mustHex(t, "22070a01781a026f6b"), mustHex(t, "0801")}

	server, failure := runServerState(frames)
	if failure != nil {
		t.Fatalf("server state: %#v", failure)
	}
	events := server["events"].([]stateEvent)
	if len(events) == 0 || events[0].BodyHex != "1a026f6b" {
		t.Fatalf("server events: %#v", events)
	}

	client, failure := runClientState(frames)
	if failure != nil || client["response_body_hex"] != "1a026f6b" {
		t.Fatalf("client state: %#v %#v", client, failure)
	}
}

func TestServerStateFailures(t *testing.T) {
	for _, test := range []struct {
		frames   []string
		category string
		code     uint32
	}{
		{[]string{}, "missing_terminal_status", 13},
		{[]string{"0863"}, "unsupported_frame_kind", 3},
		{[]string{"08011003"}, "remote_status", 3},
		{[]string{"0801100d"}, "remote_status", 13},
		{[]string{"0801100e1a04646f776e"}, "remote_status", 14},
		{[]string{"22021801", "0801"}, "malformed_protobuf", 13},
		{[]string{"0801", "80"}, "trailing_frame", 13},
	} {
		frames := make([][]byte, len(test.frames))
		for index, frame := range test.frames {
			frames[index] = mustHex(t, frame)
		}
		_, failure := runServerState(frames)
		if failure == nil || failure.category != test.category || failure.statusCode != test.code {
			t.Fatalf("%v: %#v", test.frames, failure)
		}
	}
}

func TestClientStateCardinality(t *testing.T) {
	one, failure := runClientState([][]byte{mustHex(t, "22031a0161"), mustHex(t, "0801")})
	if failure != nil || one["response_body_hex"] != "1a0161" {
		t.Fatalf("one response: %v %#v", one, failure)
	}
	for _, frames := range [][][]byte{
		{mustHex(t, "0801")},
		{mustHex(t, "22031a0161"), mustHex(t, "22031a0162"), mustHex(t, "0801")},
	} {
		_, failure := runClientState(frames)
		if failure == nil || failure.category != "response_cardinality" {
			t.Fatalf("cardinality: %#v", failure)
		}
	}
}

func TestClientStateDrainsBeforeCardinality(t *testing.T) {
	for _, test := range []struct {
		name     string
		frames   []string
		category string
		code     uint32
	}{
		{name: "remote status", frames: []string{"22031a0161", "22031a0162", "0801100e1a04646f776e"}, category: "remote_status", code: 14},
		{name: "missing terminal", frames: []string{"22031a0161", "22031a0162"}, category: "missing_terminal_status", code: 13},
		{name: "malformed trailing read", frames: []string{"22031a0161", "22031a0162", "0801", "80"}, category: "trailing_frame", code: 13},
	} {
		t.Run(test.name, func(t *testing.T) {
			frames := make([][]byte, len(test.frames))
			for index, frame := range test.frames {
				frames[index] = mustHex(t, frame)
			}
			_, failure := runClientState(frames)
			if failure == nil || failure.category != test.category || failure.statusCode != test.code {
				t.Fatalf("failure = %#v, want %s/%d", failure, test.category, test.code)
			}
		})
	}
}

func TestClientStateRemoteStatusClassification(t *testing.T) {
	for _, code := range []string{"03", "0d", "0e"} {
		_, failure := runClientState([][]byte{mustHex(t, "080110"+code)})
		if failure == nil || failure.category != "remote_status" {
			t.Fatalf("remote code %s: %#v", code, failure)
		}
	}
}
