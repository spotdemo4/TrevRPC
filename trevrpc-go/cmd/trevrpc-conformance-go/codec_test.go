package main

import (
	"encoding/hex"
	"testing"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

func TestCodecUsesProductionGoldenEncoding(t *testing.T) {
	message := trevrpc.NewRpcRequest("svc", "m", []byte("hi"))
	result, failure := codecEncode(message, trevrpc.DefaultMaxFrameSize)
	if failure != nil {
		t.Fatalf("encode: %v", failure)
	}
	if result["body_hex"] != "0a0373766312016d1a0268693001" {
		t.Fatalf("unexpected body: %v", result)
	}
	decoded, failure := codecDecode("rpc_request", mustHex(t, result["body_hex"].(string)))
	if failure != nil {
		t.Fatalf("decode: %v", failure)
	}
	if decoded["canonical_body_hex"] != result["body_hex"] {
		t.Fatalf("canonical mismatch: %v", decoded)
	}
}

func TestCodecClassifiesValidationFailures(t *testing.T) {
	for _, test := range []struct{ body, category string }{
		{"80", "malformed_protobuf"},
		{"0a0373766312016d1a0268693002", "unsupported_wire_version"},
		{"0a0373766312016d22130a0d417574686f72697a6174696f6e12026f6b3001", "invalid_metadata"},
	} {
		_, failure := codecDecode("rpc_request", mustHex(t, test.body))
		if failure == nil || failure.category != test.category {
			t.Fatalf("%s: got %#v", test.body, failure)
		}
	}
}

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}
