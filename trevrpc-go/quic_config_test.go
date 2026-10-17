package trevrpc

import (
	"crypto/tls"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

func TestServerQUICConfigDerivesLimitsFromServerOptions(t *testing.T) {
	options := DefaultServerOptions()
	options.MaxFrameSize = 1024
	options.MaxStreamBodySize = 4096
	options.MaxConcurrentStreamsPerConnection = 10
	options.StreamIdleTimeout = 20 * time.Second

	config := serverQUICConfig(nil, options)

	if config.InitialStreamReceiveWindow != 1040 || config.MaxStreamReceiveWindow != 1040 {
		t.Fatalf("stream receive window should match one frame, got initial=%d max=%d", config.InitialStreamReceiveWindow, config.MaxStreamReceiveWindow)
	}
	if config.InitialConnectionReceiveWindow != 4096 || config.MaxConnectionReceiveWindow != 4096 {
		t.Fatalf("connection receive window should match the bounded stream budget, got initial=%d max=%d", config.InitialConnectionReceiveWindow, config.MaxConnectionReceiveWindow)
	}
	if config.MaxIncomingStreams != 11 {
		t.Fatalf("incoming bidirectional streams should include one rejection slot, got %d", config.MaxIncomingStreams)
	}
	if config.MaxIdleTimeout != 20*time.Second || config.KeepAlivePeriod != 10*time.Second {
		t.Fatalf("idle defaults should derive from the stream idle timeout, got idle=%s keepalive=%s", config.MaxIdleTimeout, config.KeepAlivePeriod)
	}
	if config.MaxIncomingUniStreams != -1 {
		t.Fatalf("native-only listeners should refuse unidirectional streams, got %d", config.MaxIncomingUniStreams)
	}
	if config.Allow0RTT || config.EnableDatagrams {
		t.Fatalf("unexpected 0-RTT or datagram support: %+v", config)
	}

	options.EnableWebTransport = true
	config = serverQUICConfig(nil, options)
	if config.MaxIncomingUniStreams < 0 || !config.EnableDatagrams || !config.EnableStreamResetPartialDelivery {
		t.Fatalf("WebTransport needs unidirectional streams, datagrams and partial reset delivery: %+v", config)
	}

	options.StreamIdleTimeout = -time.Second
	config = serverQUICConfig(nil, options)
	if config.MaxIdleTimeout != 0 || config.KeepAlivePeriod != 0 {
		t.Fatalf("non-positive stream idle timeout should leave QUIC idle defaults, got idle=%s keepalive=%s", config.MaxIdleTimeout, config.KeepAlivePeriod)
	}
}

func TestQUICConfigOnlyTightensCallerLimitsAndDoesNotMutateBase(t *testing.T) {
	options := DefaultServerOptions()
	options.MaxFrameSize = 128
	base := &quic.Config{
		Allow0RTT:                  true,
		InitialStreamReceiveWindow: 1 << 20,
		MaxStreamReceiveWindow:     64,
		MaxIncomingStreams:         2,
		MaxIdleTimeout:             time.Minute,
	}

	config := serverQUICConfig(base, options)
	if config == base || config.Allow0RTT {
		t.Fatalf("0-RTT must be disabled on a copy: %+v", config)
	}
	if config.InitialStreamReceiveWindow != 144 || config.MaxStreamReceiveWindow != 64 {
		t.Fatalf("caller windows may only shrink: initial=%d max=%d", config.InitialStreamReceiveWindow, config.MaxStreamReceiveWindow)
	}
	if config.MaxIncomingStreams != 2 || config.MaxIdleTimeout != time.Minute {
		t.Fatalf("tighter caller settings should survive: %+v", config)
	}
	if !base.Allow0RTT || base.InitialStreamReceiveWindow != 1<<20 {
		t.Fatalf("base config was mutated: %+v", base)
	}

	client := clientQUICConfig(nil, 128)
	if client.MaxIncomingStreams != -1 || client.MaxIncomingUniStreams != -1 || client.MaxStreamReceiveWindow != 144 {
		t.Fatalf("clients never accept streams and bound windows to one frame: %+v", client)
	}
}

func TestTLSConfigsSelectALPN(t *testing.T) {
	if _, err := serverTLSConfig(nil, DefaultServerOptions()); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("missing certificate error = %v", err)
	}
	base := &tls.Config{Certificates: []tls.Certificate{{}}, NextProtos: []string{"custom"}}

	config, err := serverTLSConfig(base, DefaultServerOptions())
	if err != nil {
		t.Fatal(err)
	}
	if len(config.NextProtos) != 1 || config.NextProtos[0] != ALPN {
		t.Fatalf("native-only ALPN = %v", config.NextProtos)
	}
	options := DefaultServerOptions()
	options.EnableHTTP = true
	config, err = serverTLSConfig(base, options)
	if err != nil {
		t.Fatal(err)
	}
	if len(config.NextProtos) != 2 || config.NextProtos[0] != ALPN || config.NextProtos[1] != http3.NextProtoH3 {
		t.Fatalf("HTTP/3 ALPN = %v", config.NextProtos)
	}
	if base.NextProtos[0] != "custom" {
		t.Fatal("base TLS config was mutated")
	}

	client := clientTLSConfig(&tls.Config{NextProtos: []string{"custom"}}, "example.test")
	if client.ServerName != "example.test" || len(client.NextProtos) != 1 || client.NextProtos[0] != ALPN || client.ClientSessionCache == nil {
		t.Fatalf("client TLS config = %+v", client)
	}
}
