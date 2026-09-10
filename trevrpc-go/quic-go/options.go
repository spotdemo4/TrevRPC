package quicgo

import (
	"context"
	"crypto/tls"
	"slices"
	"time"

	"github.com/quic-go/quic-go"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// DialOptions configures a QUIC-go backed TrevRPC dial.
// TLSConfig and QUICConfig are cloned before use and are never mutated by the
// caller after Dial returns.
type DialOptions struct {
	Transport    trevrpc.TransportConfig
	Limits       trevrpc.TransportLimits
	Credentials  *trevrpc.TransportCredentials
	MaxFrameSize int
	OnEvent      func(trevrpc.ChannelEvent)
	TLSConfig    *tls.Config
	QUICConfig   *quic.Config
	WebTransport WebTransportOptions
}

// ListenOptions configures a QUIC-go backed TrevRPC listener.
// TLSConfig and QUICConfig are cloned before use.
type ListenOptions struct {
	Transport   trevrpc.TransportConfig
	Limits      trevrpc.TransportLimits
	Credentials *trevrpc.TransportCredentials
	TLSConfig   *tls.Config
	QUICConfig  *quic.Config
}

// WebTransportOptions configures WebTransport client requests.
type WebTransportOptions struct {
	RequestHeaders          trevrpc.HeaderFields
	ApplicationProtocols    []string
	StreamReorderingTimeout time.Duration
}

func (o DialOptions) clone() DialOptions {
	o.Credentials = cloneCredentials(o.Credentials)
	if o.TLSConfig != nil {
		o.TLSConfig = cloneTLSConfig(o.TLSConfig)
	}
	if o.QUICConfig != nil {
		o.QUICConfig = o.QUICConfig.Clone()
		o.QUICConfig.Versions = slices.Clone(o.QUICConfig.Versions)
	}
	o.WebTransport.RequestHeaders = o.WebTransport.RequestHeaders.Clone()
	o.WebTransport.ApplicationProtocols = slices.Clone(o.WebTransport.ApplicationProtocols)
	return o
}

func (o ListenOptions) clone() ListenOptions {
	o.Credentials = cloneCredentials(o.Credentials)
	if o.TLSConfig != nil {
		o.TLSConfig = cloneTLSConfig(o.TLSConfig)
	}
	if o.QUICConfig != nil {
		o.QUICConfig = o.QUICConfig.Clone()
		o.QUICConfig.Versions = slices.Clone(o.QUICConfig.Versions)
	}
	return o
}

func cloneCredentials(credentials *trevrpc.TransportCredentials) *trevrpc.TransportCredentials {
	if credentials == nil {
		return nil
	}
	clone := credentials.Clone()
	return &clone
}

// Dial establishes a reconnecting TrevRPC channel using QUIC-go.
// Host:port targets use native QUIC. HTTPS targets use WebTransport.
func Dial(ctx context.Context, target string, options DialOptions) (*trevrpc.Channel, error) {
	o := options.clone()
	return trevrpc.DialWithBackend(ctx, target, trevrpc.DialOptions{
		Transport:    o.Transport,
		Limits:       o.Limits,
		Credentials:  o.Credentials,
		MaxFrameSize: o.MaxFrameSize,
		OnEvent:      o.OnEvent,
		WebTransport: trevrpc.WebTransportOptions{
			RequestHeaders:          o.WebTransport.RequestHeaders,
			StreamReorderingTimeout: o.WebTransport.StreamReorderingTimeout,
		},
	}, &backend{d: o})
}

// Listen binds a QUIC-go listener for native QUIC, HTTP/3, and WebTransport.
func Listen(addr string, server *trevrpc.Server, options ListenOptions) (trevrpc.ServerListener, error) {
	o := options.clone()
	return trevrpc.ListenWithBackend(addr, server, trevrpc.ListenOptions{
		Transport:   o.Transport,
		Limits:      o.Limits,
		Credentials: o.Credentials,
	}, &backend{o: o})
}
