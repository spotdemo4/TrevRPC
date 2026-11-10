package main

import "trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"

// benchValidationMethods are the ValidationBench methods one --validation
// value calls.
type benchValidationMethods struct {
	unary        string
	clientStream string
}

// benchValidations are the --validation values: which rules the request type
// of the methods they call carries.
var benchValidations = map[string]benchValidationMethods{
	"none":     {interoppb.ValidationBench_Unary_FullMethodName, interoppb.ValidationBench_ClientStream_FullMethodName},
	"standard": {interoppb.ValidationBench_StandardUnary_FullMethodName, interoppb.ValidationBench_StandardClientStream_FullMethodName},
	"cel":      {interoppb.ValidationBench_CelUnary_FullMethodName, interoppb.ValidationBench_CelClientStream_FullMethodName},
}

// newBenchRequest returns the request the validation scenarios send: values
// that keep every rule of StandardBenchRequest and CelBenchRequest, and the
// payload in message. The Rust client sends the same values.
func newBenchRequest(message string) *interoppb.BenchRequest {
	return &interoppb.BenchRequest{
		RequestId: "0b6e8f3c-2d4a-4e7b-9c1f-5a8d7e6b3c20",
		Email:     "alice@example.com",
		Region:    "eu-west-1",
		Quantity:  3,
		Tags:      []string{"checkout", "mobile", "beta"},
		StartMs:   1_790_000_000_000,
		EndMs:     1_790_000_060_000,
		Message:   message,
	}
}
