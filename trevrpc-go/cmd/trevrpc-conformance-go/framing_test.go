package main

import "testing"

func TestFramingAllCoalescedSplitPoints(t *testing.T) {
	sequence := mustHex(t, "00000000000000032201a5")
	for split := 0; split <= len(sequence); split++ {
		chunks := [][]byte{sequence[:split], sequence[split:]}
		result, failure := framingDecode("rpc_stream_frame", 16, chunks)
		if failure != nil {
			t.Fatalf("split %d: %v", split, failure)
		}
		bodies := result["bodies_hex"].([]string)
		if len(bodies) != 2 || bodies[0] != "" || bodies[1] != "2201a5" {
			t.Fatalf("split %d: %#v", split, bodies)
		}
	}
}

func TestFramingPreservesUnknownAndMalformedBodies(t *testing.T) {
	for _, test := range []struct {
		messageType string
		framedHex   string
		bodyHex     string
	}{
		{messageType: "rpc_request", framedHex: "0000000430014007", bodyHex: "30014007"},
		{messageType: "rpc_request", framedHex: "0000000180", bodyHex: "80"},
	} {
		result, failure := framingDecode(test.messageType, 16, [][]byte{mustHex(t, test.framedHex)})
		if failure != nil {
			t.Fatalf("%s: %v", test.bodyHex, failure)
		}
		bodies := result["bodies_hex"].([]string)
		if len(bodies) != 1 || bodies[0] != test.bodyHex {
			t.Fatalf("%s: %#v", test.bodyHex, bodies)
		}
	}
}

func TestFramingIncompleteAndOversized(t *testing.T) {
	_, incomplete := framingDecode("rpc_stream_frame", 16, [][]byte{mustHex(t, "000000032201")})
	if incomplete == nil || incomplete.category != "incomplete_frame" {
		t.Fatalf("incomplete: %#v", incomplete)
	}
	_, oversized := framingDecode("rpc_stream_frame", 16, [][]byte{mustHex(t, "00000011")})
	if oversized == nil || oversized.category != "frame_too_large" {
		t.Fatalf("oversized: %#v", oversized)
	}
}
