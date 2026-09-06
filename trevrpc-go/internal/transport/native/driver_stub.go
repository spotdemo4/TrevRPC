//go:build !trevrpc_native || !cgo || (!linux && !darwin) || (!amd64 && !arm64)

package native

import (
	"context"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

type Engine struct{}

type Listener struct{}

type Connection struct{}

type Stream struct{}

func NewEngine(EngineConfig) (*Engine, error) {
	return nil, ErrUnavailable
}

func NewTransport(EngineConfig) (*Engine, error) {
	return nil, ErrUnavailable
}

func (*Engine) Listen(context.Context, EndpointConfig) (*Listener, error) {
	return nil, ErrUnavailable
}

func (*Engine) Dial(context.Context, EndpointConfig) (*Connection, error) {
	return nil, ErrUnavailable
}

func (*Engine) Close() error {
	return nil
}

func (*Engine) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (*Engine) Err() error {
	return ErrUnavailable
}

func (*Listener) Accept(context.Context) (transportinternal.Connection, error) {
	return nil, ErrUnavailable
}

func (*Listener) Address() transportinternal.Address {
	return transportinternal.Address{}
}

func (*Listener) Close() error {
	return nil
}

func (*Connection) OpenStream(context.Context) (transportinternal.BidirectionalStream, error) {
	return nil, ErrUnavailable
}

func (*Connection) AcceptStream(context.Context) (transportinternal.BidirectionalStream, error) {
	return nil, ErrUnavailable
}

func (*Connection) Done() <-chan struct{} {
	done := make(chan struct{})
	close(done)
	return done
}

func (*Connection) Err() error {
	return ErrUnavailable
}

func (*Connection) Info() transportinternal.ConnectionInfo {
	return transportinternal.ConnectionInfo{}
}

func (*Connection) Close(transportinternal.CloseReason) error {
	return nil
}

func (*Stream) Read([]byte) (int, error) {
	return 0, ErrUnavailable
}

func (*Stream) Write([]byte) (int, error) {
	return 0, ErrUnavailable
}

func (*Stream) Close() error {
	return nil
}

func (*Stream) Context() context.Context {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrUnavailable)
	return ctx
}

func (*Stream) CancelRead(transportinternal.CloseReason) {}

func (*Stream) CancelWrite(transportinternal.CloseReason) {}
