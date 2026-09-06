package trevrpc

import (
	"crypto/tls"
	"net"
	"net/http"
	"testing"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	webtransport "github.com/quic-go/webtransport-go"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
)

func TestTransportBackendResolution(t *testing.T) {
	tests := []struct {
		name     string
		backend  TransportBackend
		resolved TransportBackend
		code     Code
	}{
		{name: "auto", backend: TransportBackendAuto, resolved: TransportBackendLegacy},
		{name: "legacy", backend: TransportBackendLegacy, resolved: TransportBackendLegacy},
		{name: "native", backend: TransportBackendNative, resolved: TransportBackendNative},
		{name: "invalid", backend: TransportBackend(99), code: CodeInvalidArgument},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolved, err := resolveTransportBackend(test.backend)
			if test.code != CodeOK {
				if StatusFromError(err).Code != test.code {
					t.Fatalf("resolve error = %v, want code %v", err, test.code)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if resolved != test.resolved {
				t.Fatalf("resolved = %v, want %v", resolved, test.resolved)
			}
		})
	}
}

func TestNativeBackendDoesNotFallBack(t *testing.T) {
	connector, err := newChannelConnector("127.0.0.1:1", DialOptions{
		Backend: TransportBackendNative,
	})
	if native.Available() {
		if err != nil {
			t.Fatalf("native connector error = %v", err)
		}
		if _, ok := connector.(*nativeEngineQUICConnector); !ok {
			t.Fatalf("native connector type = %T", connector)
		}
	} else if StatusFromError(err).Code != CodeUnimplemented {
		t.Fatalf("native connector error = %v, want Unimplemented", err)
	}

	_, err = newChannelConnector("127.0.0.1:1", DialOptions{
		Backend:   TransportBackendNative,
		TLSConfig: &tls.Config{},
	})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("native legacy-option error = %v, want InvalidArgument", err)
	}
}

func TestNativeListenerDoesNotFallBack(t *testing.T) {
	server := NewServer()
	certificate, key := testCertificateMaterial(t)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Backend: TransportBackendNative,
		Credentials: &TransportCredentials{
			CertificateChainPEM: certificate,
			PrivateKeyPEM:       key,
		},
	})
	if native.Available() {
		if err != nil {
			t.Fatalf("native listener error = %v", err)
		}
		if _, ok := listener.(*nativeQUICServerListener); !ok {
			t.Fatalf("native listener type = %T", listener)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("native listener close error = %v", err)
		}
	} else if StatusFromError(err).Code != CodeUnimplemented {
		t.Fatalf("native listener error = %v, want Unimplemented", err)
	}

	_, err = Listen("127.0.0.1:0", server, ListenOptions{
		Backend:   TransportBackendNative,
		TLSConfig: &tls.Config{},
	})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("native legacy-option error = %v, want InvalidArgument", err)
	}
}

func TestNativeWebTransportRejectsLegacyHeaders(t *testing.T) {
	_, err := newChannelConnector("https://example.test/trevrpc", DialOptions{
		Backend: TransportBackendNative,
		WebTransport: WebTransportOptions{
			RequestHeader: http.Header{"X-Test": {"value"}},
		},
	})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("native legacy-header error = %v, want InvalidArgument", err)
	}
}

func TestTransportCredentialsClone(t *testing.T) {
	original := &TransportCredentials{
		CertificateChainPEM: []byte("certificate"),
		PrivateKeyPEM:       []byte("key"),
		RootCAPEM:           []byte("root"),
		ServerName:          "example.test",
	}
	clone := cloneTransportCredentials(original)
	original.CertificateChainPEM[0] = 'x'
	original.PrivateKeyPEM[0] = 'x'
	original.RootCAPEM[0] = 'x'
	original.ServerName = "changed.test"
	if string(clone.CertificateChainPEM) != "certificate" || string(clone.PrivateKeyPEM) != "key" ||
		string(clone.RootCAPEM) != "root" || clone.ServerName != "example.test" {
		t.Fatalf("credentials clone changed with source: %+v", clone)
	}
}

func TestOrderedRequestHeaders(t *testing.T) {
	fields := HeaderFields{
		{Name: "x-first", Value: "one"},
		{Name: "x-repeat", Value: "two"},
		{Name: "x-repeat", Value: "three"},
	}
	headers, err := legacyRequestHeaders(fields, nil)
	if err != nil {
		t.Fatal(err)
	}
	fields[0].Value = "changed"
	if got := headers.Values("x-first"); len(got) != 1 || got[0] != "one" {
		t.Fatalf("converted headers = %v", headers)
	}
	if got := headers.Values("x-repeat"); len(got) != 2 || got[0] != "two" || got[1] != "three" {
		t.Fatalf("duplicate headers = %v", got)
	}

	_, err = legacyRequestHeaders(HeaderFields{{Name: "x", Value: "one"}}, http.Header{"X": {"two"}})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("ambiguous headers error = %v, want InvalidArgument", err)
	}
}

func TestTransportAddressCopiesNetAddress(t *testing.T) {
	address := transportinternal.AddressFromNet(&net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443, Zone: "eth0"})
	if address.Network != "tcp" || address.Host != "2001:db8::1" || address.Port != 443 || address.Zone != "eth0" {
		t.Fatalf("address = %+v", address)
	}
	if address.String() != "[2001:db8::1%eth0]:443" {
		t.Fatalf("address string = %q", address.String())
	}
}

func TestLegacyCloseReasonsPreserveProtocolMetadata(t *testing.T) {
	h3Reason := legacyQUICCloseReason(&http3.Error{
		Remote:       true,
		ErrorCode:    http3.ErrCodeNoError,
		ErrorMessage: "drained",
	})
	if !h3Reason.Peer || h3Reason.Local || !h3Reason.Clean ||
		h3Reason.ApplicationCode != uint64(http3.ErrCodeNoError) ||
		h3Reason.Message != "drained" {
		t.Fatalf("HTTP/3 close reason = %+v", h3Reason)
	}

	transportReason := webTransportCloseReason(&quic.TransportError{
		Remote:       true,
		ErrorCode:    0x0a,
		ErrorMessage: "protocol violation",
	})
	if !transportReason.Peer || transportReason.Local ||
		transportReason.TransportCode != 0x0a ||
		transportReason.Message != "protocol violation" {
		t.Fatalf("QUIC transport close reason = %+v", transportReason)
	}

	sessionReason := webTransportCloseReason(&webtransport.SessionError{
		Remote:    false,
		ErrorCode: 0,
		Message:   "complete",
	})
	if sessionReason.Peer || !sessionReason.Local || !sessionReason.Clean ||
		sessionReason.Message != "complete" {
		t.Fatalf("WebTransport close reason = %+v", sessionReason)
	}
}

func TestPositiveTransportLimitEnablesIncomingStreams(t *testing.T) {
	config := &quic.Config{MaxIncomingStreams: -1}
	applyQUICTransportLimits(config, TransportLimits{
		IncomingBidirectionalStreams: 4,
	})
	if config.MaxIncomingStreams != 4 {
		t.Fatalf("MaxIncomingStreams = %d, want 4", config.MaxIncomingStreams)
	}
}
