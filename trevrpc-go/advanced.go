package trevrpc

import (
	"context"

	"github.com/quic-go/quic-go"
	webtransport "github.com/quic-go/webtransport-go"
)

// Advanced exposes low-level transports and connection-specific operations.
var Advanced AdvancedAPI

// AdvancedAPI is the namespace type for low-level transport operations.
type AdvancedAPI struct{}

// NewRawQUICClient creates a transport over one caller-owned QUIC connection.
// It does not reconnect, retry, or replay calls.
func (AdvancedAPI) NewRawQUICClient(conn *quic.Conn) *RawQUICClient {
	return newRawQUICClient(conn)
}

// DialRawWebTransport creates one WebTransport session without reconnecting it.
func (AdvancedAPI) DialRawWebTransport(ctx context.Context, url string, options RawWebTransportDialOptions) (*RawWebTransportClient, error) {
	return dialRawWebTransport(ctx, url, options)
}

// NewRawWebTransportClient creates a transport over one caller-owned WebTransport session.
func (AdvancedAPI) NewRawWebTransportClient(session *webtransport.Session) *RawWebTransportClient {
	return newRawWebTransportClient(session)
}

// Channel returns connection-specific operations for a Channel.
func (AdvancedAPI) Channel(channel *Channel) AdvancedChannel {
	return AdvancedChannel{channel: channel}
}

// AdvancedChannel exposes connection-specific operations on the current generation.
type AdvancedChannel struct {
	channel *Channel
}

// AddPath adds a migration path to the current native QUIC generation. The
// caller must probe and switch the returned path according to quic-go's Path API.
func (a AdvancedChannel) AddPath(transport *quic.Transport) (*quic.Path, error) {
	if transport == nil {
		return nil, InvalidArgument("QUIC migration transport is nil")
	}
	generation, err := a.channel.currentGeneration()
	if err != nil {
		return nil, err
	}
	quicGeneration, ok := generation.(interface {
		AddPath(*quic.Transport) (*quic.Path, error)
	})
	if !ok {
		return nil, Unimplemented("current channel transport does not support QUIC path migration")
	}
	return quicGeneration.AddPath(transport)
}

// RawWebTransportSession returns the current WebTransport session.
func (a AdvancedChannel) RawWebTransportSession() (*webtransport.Session, error) {
	generation, err := a.channel.currentGeneration()
	if err != nil {
		return nil, err
	}
	webTransportGeneration, ok := generation.(interface {
		RawWebTransportSession() *webtransport.Session
	})
	if !ok {
		return nil, Unimplemented("current channel transport is not WebTransport")
	}
	return webTransportGeneration.RawWebTransportSession(), nil
}
