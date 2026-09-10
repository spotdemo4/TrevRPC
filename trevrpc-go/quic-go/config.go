package quicgo

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"strings"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const (
	defaultSessionCacheSize = 64
	defaultTokenOrigins     = 32
	defaultTokensPerOrigin  = 4
)

func cloneTLSConfig(base *tls.Config) *tls.Config {
	if base == nil {
		return nil
	}
	clone := base.Clone()
	clone.NextProtos = append([]string(nil), base.NextProtos...)
	clone.CipherSuites = append([]uint16(nil), base.CipherSuites...)
	clone.CurvePreferences = append([]tls.CurveID(nil), base.CurvePreferences...)
	clone.Certificates = append([]tls.Certificate(nil), base.Certificates...)
	for i := range clone.Certificates {
		clone.Certificates[i].Certificate = cloneByteSlices(base.Certificates[i].Certificate)
	}
	return clone
}

func cloneByteSlices(values [][]byte) [][]byte {
	clone := make([][]byte, len(values))
	for i := range values {
		clone[i] = append([]byte(nil), values[i]...)
	}
	return clone
}

func tlsConfig(base *tls.Config, credentials *trevrpc.TransportCredentials, server bool, h3 bool) (*tls.Config, error) {
	var config *tls.Config
	if base == nil {
		config = &tls.Config{}
	} else {
		config = cloneTLSConfig(base)
	}
	if credentials != nil {
		if len(credentials.CertificateChainPEM) != 0 || len(credentials.PrivateKeyPEM) != 0 {
			certificate, err := tls.X509KeyPair(credentials.CertificateChainPEM, credentials.PrivateKeyPEM)
			if err != nil {
				return nil, trevrpc.InvalidArgument("invalid transport certificate credentials: " + err.Error())
			}
			config.Certificates = append([]tls.Certificate{certificate}, config.Certificates...)
		}
		if len(credentials.RootCAPEM) != 0 {
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(credentials.RootCAPEM) {
				return nil, trevrpc.InvalidArgument("transport root CA contains no certificates")
			}
			if server {
				config.ClientCAs = roots
			} else {
				config.RootCAs = roots
			}
		}
		if credentials.ServerName != "" {
			config.ServerName = credentials.ServerName
		}
		if credentials.InsecureSkipVerify {
			config.InsecureSkipVerify = true
		}
		if server && credentials.RequireClientCertificate {
			config.ClientAuth = tls.RequireAndVerifyClientCert
		}
	}
	if h3 {
		config.NextProtos = []string{http3.NextProtoH3}
	} else {
		config.NextProtos = []string{trevrpc.ALPN}
	}
	if server && h3 {
		// The HTTP/3 server uses the h3 ALPN only. Native QUIC and h3 are
		// classified by the same QUIC listener before dispatch.
		config.NextProtos = append([]string{trevrpc.ALPN}, config.NextProtos...)
	}
	if !server && config.ClientSessionCache == nil {
		config.ClientSessionCache = tls.NewLRUClientSessionCache(defaultSessionCacheSize)
	}
	return config, nil
}

func quicConfig(base *quic.Config, transport trevrpc.TransportConfig, limits trevrpc.TransportLimits, maxFrameSize int, webtransport bool, allowIncomingUni bool) *quic.Config {
	var config *quic.Config
	if base == nil {
		config = &quic.Config{}
	} else {
		config = base.Clone()
	}
	if config.MaxIdleTimeout == 0 && transport.MaxIdleTimeout > 0 {
		config.MaxIdleTimeout = transport.MaxIdleTimeout
	}
	if config.KeepAlivePeriod == 0 && transport.KeepAlive > 0 {
		config.KeepAlivePeriod = transport.KeepAlive
	}
	if maxFrameSize > 0 {
		window := uint64(maxFrameSize) + 4
		capWindow(&config.InitialStreamReceiveWindow, window)
		capWindow(&config.MaxStreamReceiveWindow, window)
		capWindow(&config.InitialConnectionReceiveWindow, window)
		capWindow(&config.MaxConnectionReceiveWindow, window)
	}
	if limits.StreamReceiveWindow > 0 {
		capWindow(&config.InitialStreamReceiveWindow, limits.StreamReceiveWindow)
		capWindow(&config.MaxStreamReceiveWindow, limits.StreamReceiveWindow)
	}
	if limits.ConnectionReceiveWindow > 0 {
		capWindow(&config.InitialConnectionReceiveWindow, limits.ConnectionReceiveWindow)
		capWindow(&config.MaxConnectionReceiveWindow, limits.ConnectionReceiveWindow)
	}
	if limits.IncomingBidirectionalStreams > 0 {
		if config.MaxIncomingStreams < 0 || config.MaxIncomingStreams == 0 || config.MaxIncomingStreams > limits.IncomingBidirectionalStreams {
			config.MaxIncomingStreams = limits.IncomingBidirectionalStreams
		}
	}
	if !allowIncomingUni && config.MaxIncomingUniStreams >= 0 {
		config.MaxIncomingUniStreams = -1
	}
	if webtransport {
		config.EnableDatagrams = true
		config.EnableStreamResetPartialDelivery = true
	}
	// TrevRPC deliberately does not attempt 0-RTT. This is enforced both here
	// and by using quic.Dial rather than quic.DialEarly.
	config.Allow0RTT = false
	if config.TokenStore == nil {
		config.TokenStore = quic.NewLRUTokenStore(defaultTokenOrigins, defaultTokensPerOrigin)
	}
	return config
}

func capWindow(value *uint64, limit uint64) {
	if *value == 0 || *value > limit {
		*value = limit
	}
}

func validateTarget(target string, webtransport bool) (*url.URL, error) {
	if webtransport {
		u, err := url.Parse(target)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, trevrpc.InvalidArgument("WebTransport target must be an https URL without user info, query, or fragment")
		}
		return u, nil
	}
	if target == "" || strings.Contains(target, "://") {
		return nil, trevrpc.InvalidArgument(fmt.Sprintf("invalid QUIC target %q", target))
	}
	return nil, nil
}
