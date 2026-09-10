package trevrpc

import (
	"net"
	"testing"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/transport/native"
	transportapi "trev.zip/llc/trevrpc/trevrpc-go/transport"
)

func TestTransportBackendResolution(t *testing.T) {
	tests := []struct {
		name     string
		backend  TransportBackend
		resolved TransportBackend
		code     Code
	}{
		{name: "auto", backend: TransportBackendAuto, resolved: TransportBackendNative},
		{name: "native", backend: TransportBackendNative, resolved: TransportBackendNative},
		{name: "quic-go", backend: TransportBackendQUICGo, resolved: TransportBackendQUICGo},
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

func TestRootDialSelectsNativeWithoutFallback(t *testing.T) {
	connector, err := newChannelConnector("127.0.0.1:1", DialOptions{})
	if native.Available() {
		if err != nil {
			t.Fatalf("native connector error = %v", err)
		}
		backendConnector, ok := connector.(backendChannelConnector)
		if !ok {
			t.Fatalf("native connector type = %T", connector)
		}
		nativeConnector, ok := backendConnector.connector.(*nativeEngineQUICConnector)
		if !ok {
			t.Fatalf("native backend connector type = %T", backendConnector.connector)
		}
		if nativeConnector.requestedBackend != TransportBackendAuto {
			t.Fatalf("requested backend = %v, want Auto", nativeConnector.requestedBackend)
		}
	} else if StatusFromError(err).Code != CodeUnimplemented {
		t.Fatalf("native connector error = %v, want Unimplemented", err)
	}
}

func TestRootListenSelectsNativeWithoutFallback(t *testing.T) {
	server := NewServer()
	certificate, key := testCertificateMaterial(t)
	listener, err := Listen("127.0.0.1:0", server, ListenOptions{
		Credentials: &TransportCredentials{
			CertificateChainPEM: certificate,
			PrivateKeyPEM:       key,
		},
	})
	if native.Available() {
		if err != nil {
			t.Fatalf("native listener error = %v", err)
		}
		backendListener, ok := listener.(*backendServerListener)
		if !ok {
			t.Fatalf("native listener type = %T", listener)
		}
		if _, ok := backendListener.listener.(*nativeBackendListener); !ok {
			t.Fatalf("native backend listener type = %T", backendListener.listener)
		}
		if err := listener.Close(); err != nil {
			t.Fatalf("native listener close error = %v", err)
		}
	} else if StatusFromError(err).Code != CodeUnimplemented {
		t.Fatalf("native listener error = %v, want Unimplemented", err)
	}
}

func TestNativeQUICRejectsWebTransportOptions(t *testing.T) {
	if !native.Available() {
		t.Skip("native transport unavailable")
	}
	_, err := newChannelConnector("127.0.0.1:1", DialOptions{
		WebTransport: WebTransportOptions{
			RequestHeaders: HeaderFields{{Name: "X-Test", Value: "value"}},
		},
	})
	if StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("native WebTransport option error = %v, want InvalidArgument", err)
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

func TestWebTransportOptionsClone(t *testing.T) {
	original := WebTransportOptions{
		RequestHeaders: HeaderFields{{Name: "X-Test", Value: "value"}},
	}
	clone := cloneWebTransportOptions(original)
	original.RequestHeaders[0].Value = "changed"
	if clone.RequestHeaders[0].Value != "value" {
		t.Fatalf("options clone changed with source: %+v", clone)
	}
}

func TestHeaderFieldValidation(t *testing.T) {
	valid := HeaderFields{
		{Name: "x-first", Value: "one"},
		{Name: "x-repeat", Value: "two"},
		{Name: "x-repeat", Value: "three"},
	}
	if err := validateHeaderFields(valid); err != nil {
		t.Fatalf("valid fields error = %v", err)
	}
	if values := headerFieldValues(valid, "X-Repeat"); len(values) != 2 || values[0] != "two" || values[1] != "three" {
		t.Fatalf("duplicate header values = %v", values)
	}

	for _, fields := range []HeaderFields{
		{{Name: "", Value: "value"}},
		{{Name: ":authority", Value: "example.test"}},
		{{Name: "X-Test\nOther", Value: "value"}},
		{{Name: "X-Test", Value: "value\r\nOther"}},
	} {
		if StatusFromError(validateHeaderFields(fields)).Code != CodeInvalidArgument {
			t.Fatalf("invalid fields accepted: %+v", fields)
		}
	}
}

func TestTransportAddressCopiesNetAddress(t *testing.T) {
	address := transportapi.AddressFromNet(&net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443, Zone: "eth0"})
	if address.Network != "tcp" || address.Host != "2001:db8::1" || address.Port != 443 || address.Zone != "eth0" {
		t.Fatalf("address = %+v", address)
	}
	if address.String() != "[2001:db8::1%eth0]:443" {
		t.Fatalf("address string = %q", address.String())
	}
}
