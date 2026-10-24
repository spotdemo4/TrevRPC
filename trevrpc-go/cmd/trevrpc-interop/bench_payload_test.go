package main

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

// TestRecordsPayloadMatchesTheReference checks the generator against
// bench/payload.py: the SHA-256 of the messages of sizes 64, 256, 1024 and 4096
// in order from worker 0's seed.
func TestRecordsPayloadMatchesTheReference(t *testing.T) {
	const golden = "1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98"
	payload := &recordsPayload{state: recordsWorkerSeed}
	digest := sha256.New()
	for _, size := range []int{64, 256, 1024, 4096} {
		payload.size = size
		message := payload.next()
		if len(message) != size {
			t.Fatalf("message has %d bytes, want %d", len(message), size)
		}
		digest.Write([]byte(message))
	}
	if got := hex.EncodeToString(digest.Sum(nil)); got != golden {
		t.Fatalf("records digest %s, want %s", got, golden)
	}
}

func TestPayloadsDifferPerWorkerAndPerMessage(t *testing.T) {
	first := newBenchPayload("records", 256, 0)
	second := newBenchPayload("records", 256, 1)
	a, b := first.next(), first.next()
	if a == b || a == second.next() {
		t.Fatal("records messages repeat")
	}
	repeat := newBenchPayload("repeat", 4, 0)
	if repeat.next() != "xxxx" || repeat.next() != "xxxx" {
		t.Fatal("repeat payload is not x repeated")
	}
}
