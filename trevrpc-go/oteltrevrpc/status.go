package oteltrevrpc

import trevrpc "trev.zip/llc/trevrpc/trevrpc-go"

// statusCodes are the rpc.response.status_code values of the TrevRPC codes,
// named as gRPC names its codes.
var statusCodes = [...]string{
	trevrpc.CodeOK:                 "OK",
	trevrpc.CodeCancelled:          "CANCELLED",
	trevrpc.CodeUnknown:            "UNKNOWN",
	trevrpc.CodeInvalidArgument:    "INVALID_ARGUMENT",
	trevrpc.CodeDeadlineExceeded:   "DEADLINE_EXCEEDED",
	trevrpc.CodeNotFound:           "NOT_FOUND",
	trevrpc.CodeAlreadyExists:      "ALREADY_EXISTS",
	trevrpc.CodePermissionDenied:   "PERMISSION_DENIED",
	trevrpc.CodeResourceExhausted:  "RESOURCE_EXHAUSTED",
	trevrpc.CodeFailedPrecondition: "FAILED_PRECONDITION",
	trevrpc.CodeAborted:            "ABORTED",
	trevrpc.CodeOutOfRange:         "OUT_OF_RANGE",
	trevrpc.CodeUnimplemented:      "UNIMPLEMENTED",
	trevrpc.CodeInternal:           "INTERNAL",
	trevrpc.CodeUnavailable:        "UNAVAILABLE",
	trevrpc.CodeDataLoss:           "DATA_LOSS",
	trevrpc.CodeUnauthenticated:    "UNAUTHENTICATED",
}

// statusCode returns the rpc.response.status_code value of code; a code
// TrevRPC does not define is UNKNOWN, as TrevRPC treats it.
func statusCode(code trevrpc.Code) string {
	if code < trevrpc.Code(len(statusCodes)) {
		return statusCodes[code]
	}
	return statusCodes[trevrpc.CodeUnknown]
}

// serverError reports whether code means the server failed, rather than the
// client or the call: a server span with one has an error status, and its
// measurements an error.type, as the semantic conventions for gRPC servers
// specify for the same codes.
func serverError(code trevrpc.Code) bool {
	switch code {
	case trevrpc.CodeUnknown,
		trevrpc.CodeDeadlineExceeded,
		trevrpc.CodeUnimplemented,
		trevrpc.CodeInternal,
		trevrpc.CodeUnavailable,
		trevrpc.CodeDataLoss:
		return true
	default:
		return code >= trevrpc.Code(len(statusCodes))
	}
}
