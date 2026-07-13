package benchmarkpb

import (
	"bytes"
	"testing"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

func TestBenchmarkRequestCanonicalWireEncoding(t *testing.T) {
	request := &BenchmarkRequest{Sequence: 7, Payload: []byte{1, 2}, ResponseBytes: 9}
	encoded, err := trevrpc.MarshalMessage(request)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x08, 0x07, 0x12, 0x02, 0x01, 0x02, 0x18, 0x09}
	if !bytes.Equal(encoded, want) {
		t.Fatalf("encoded request = %x, want %x", encoded, want)
	}

	decoded := &BenchmarkRequest{}
	if err := trevrpc.UnmarshalMessage(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Sequence != request.Sequence || decoded.ResponseBytes != request.ResponseBytes || !bytes.Equal(decoded.Payload, request.Payload) {
		t.Fatalf("decoded request = %+v, want %+v", decoded, request)
	}
}
