package trevrpc

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// RouteMethodDescriptor gives the route the protobuf descriptor of its method,
// which authorizers, interceptors and the handler read as
// HandlerContext.MethodDescriptor, for example to read the options an
// application defines on its methods. Generated registration passes it, from
// the descriptor protoc-gen-go embeds. A nil descriptor leaves the route
// without one.
//
// Registering panics when the descriptor does not describe the route's
// method, such as one with another full name or another streaming shape.
func RouteMethodDescriptor(method protoreflect.MethodDescriptor) RouteOption {
	return func(config *routeConfig) {
		config.method = method
	}
}

// methodKind returns the shape of the RPC method describes.
func methodKind(method protoreflect.MethodDescriptor) RPCKind {
	switch {
	case method.IsStreamingClient() && method.IsStreamingServer():
		return RPCKindBidirectionalStreaming
	case method.IsStreamingClient():
		return RPCKindClientStreaming
	case method.IsStreamingServer():
		return RPCKindServerStreaming
	default:
		return RPCKindUnary
	}
}

// validateRouteMethod panics when method, the descriptor of the route at path
// with the given kind, describes another method. A nil method is valid.
func validateRouteMethod(path string, kind RPCKind, method protoreflect.MethodDescriptor) {
	if method == nil {
		return
	}
	if want := "/" + string(method.Parent().FullName()) + "/" + string(method.Name()); path != want {
		panic(fmt.Sprintf("trevrpc: route %q has the descriptor of method %s", path, want))
	}
	if got := methodKind(method); got != kind {
		panic(fmt.Sprintf("trevrpc: route %q is %s but its descriptor is %s", path, kind, got))
	}
}
