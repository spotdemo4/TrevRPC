package main

import (
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
	"trev.zip/llc/trevrpc/trevrpc-go/protovalidatetrevrpc"
)

// benchValidationSizes are the message sizes of the validation microbenchmarks:
// the load benchmarks' small and large sizes.
var benchValidationSizes = []int{64, 4096}

// asRequestType decodes the encoding of request as the message type of into,
// as a server routing it to the method of that type does.
func asRequestType[T proto.Message](tb testing.TB, request *interoppb.BenchRequest, into T) T {
	tb.Helper()
	encoded, err := proto.Marshal(request)
	if err != nil {
		tb.Fatal(err)
	}
	if err := proto.Unmarshal(encoded, into); err != nil {
		tb.Fatal(err)
	}
	return into
}

// validateFunc returns the protovalidatetrevrpc check of message's type.
func validateFunc(tb testing.TB, message proto.Message) func(proto.Message) error {
	tb.Helper()
	validate, err := protovalidatetrevrpc.New(nil).ValidateFunc(message.ProtoReflect().Descriptor())
	if err != nil || validate == nil {
		tb.Fatalf("ValidateFunc(%T) = check %t, %v; want a check", message, validate != nil, err)
	}
	return validate
}

// TestBenchRequestKeepsEveryRule checks that the request the validation
// scenarios send passes the rules of every ValidationBench request type, that
// BenchRequest has none, and that each CEL rule really runs.
func TestBenchRequestKeepsEveryRule(t *testing.T) {
	request := newBenchRequest("hello")
	for _, message := range []proto.Message{&interoppb.StandardBenchRequest{}, &interoppb.CelBenchRequest{}} {
		if err := validateFunc(t, message)(asRequestType(t, request, message)); err != nil {
			t.Errorf("%T: %v", message, err)
		}
	}
	if validate, err := protovalidatetrevrpc.New(nil).ValidateFunc((&interoppb.BenchRequest{}).ProtoReflect().Descriptor()); validate != nil || err != nil {
		t.Errorf("BenchRequest: ValidateFunc = check %t, %v; want no check", validate != nil, err)
	}

	for _, tc := range []struct {
		rule, field string
		breakIt     func(*interoppb.BenchRequest)
	}{
		{"window.ordered", "", func(r *interoppb.BenchRequest) { r.EndMs = r.StartMs }},
		{"tags.not_region", "", func(r *interoppb.BenchRequest) { r.Tags = append(r.Tags, r.Region) }},
		{"string.email", "email", func(r *interoppb.BenchRequest) { r.Email = "alice" }},
	} {
		broken := newBenchRequest("hello")
		tc.breakIt(broken)
		cel := &interoppb.CelBenchRequest{}
		if err := expectViolation(tc.rule, validateFunc(t, cel)(asRequestType(t, broken, cel)), tc.rule, tc.field); err != nil {
			t.Error(err)
		}
	}
}

// BenchmarkValidateBenchRequest measures protovalidatetrevrpc's check of the
// request the validation scenarios send, against the rules of
// StandardBenchRequest and of CelBenchRequest, which a server runs after it
// decodes the request. BenchmarkDecodeBenchRequest is that decoding, for
// scale.
func BenchmarkValidateBenchRequest(b *testing.B) {
	for _, rules := range []struct {
		name    string
		message proto.Message
	}{
		{"standard", &interoppb.StandardBenchRequest{}},
		{"cel", &interoppb.CelBenchRequest{}},
	} {
		for _, size := range benchValidationSizes {
			b.Run(fmt.Sprintf("rules=%s/size=%d", rules.name, size), func(b *testing.B) {
				message := asRequestType(b, newBenchRequest(strings.Repeat("x", size)), rules.message)
				validate := validateFunc(b, message)
				b.ReportAllocs()
				for b.Loop() {
					if err := validate(message); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkDecodeBenchRequest(b *testing.B) {
	for _, size := range benchValidationSizes {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			encoded, err := proto.Marshal(newBenchRequest(strings.Repeat("x", size)))
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			for b.Loop() {
				if err := proto.Unmarshal(encoded, &interoppb.StandardBenchRequest{}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
