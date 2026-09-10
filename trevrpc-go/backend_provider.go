package trevrpc

import (
	"context"
	"time"

	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

// BackendDialOptions contains copied, backend-neutral dial configuration.
type BackendDialOptions struct {
	Transport    TransportConfig
	Limits       TransportLimits
	Credentials  *TransportCredentials
	MaxFrameSize int
	WebTransport WebTransportOptions
}

// BackendConnection is one connection generation returned by a dial backend.
type BackendConnection struct {
	Endpoint    transportapi.StreamEndpoint
	Close       func() error
	MapStatus   func(context.Context, error) error
	CloseReason func(error) TransportCloseReason
}

// BackendConnector creates a new connection generation for an initial dial or reconnect.
type BackendConnector interface {
	Connect(context.Context) (BackendConnection, error)
}

// DialBackend creates reconnectable client connectors for an optional transport backend.
type DialBackend interface {
	Backend() TransportBackend
	NewConnector(string, BackendDialOptions) (BackendConnector, error)
}

// BackendServerOptions is the server configuration visible to transport backends.
type BackendServerOptions struct {
	MaxFrameSize                      int
	MaxConcurrentConnections          int
	MaxConcurrentStreamsPerConnection int
	EnableHTTP3                       bool
	EnableWebTransport                bool
	HTTP3Path                         string
	GracefulShutdownTimeout           time.Duration
}

// BackendListenOptions contains copied, backend-neutral listener configuration.
type BackendListenOptions struct {
	Transport         TransportConfig
	Limits            TransportLimits
	Credentials       *TransportCredentials
	Server            BackendServerOptions
	AdmitHTTP3        func(HTTP3AdmissionRequest) (bool, error)
	AdmitWebTransport func(WebTransportAdmissionRequest) (bool, error)
}

// BackendServerConnection feeds one backend-owned connection into the shared server runtime.
type BackendServerConnection interface {
	Options() BackendServerOptions
	ServeNativeQUIC(context.Context, transportapi.Connection)
	ServeHTTP3(context.Context, transportapi.StreamEndpoint)
	HandleHTTP3(transportapi.HTTP3Request)
	UpgradeWebTransport(transportapi.WebTransportRequest) (transportapi.WebTransportSession, bool)
	ServeWebTransport(context.Context, transportapi.WebTransportSession)
	EmitDiagnostic(ServerDiagnostic)
}

// ListenBackend creates listeners and dispatches their accepted connections.
type ListenBackend interface {
	Backend() TransportBackend
	Listen(string, BackendListenOptions) (transportapi.Listener, error)
	ServeConnection(context.Context, transportapi.Connection, BackendServerConnection)
	ListenerClosed(error) bool
	MapStatus(error) error
}
