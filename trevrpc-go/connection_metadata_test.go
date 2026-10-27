package trevrpc

import (
	"context"
	"testing"
)

func codeOf(err error) Code {
	if err == nil {
		return CodeOK
	}
	return StatusFromError(err).Code
}

// echoSuffix calls Echo, whose reply appends the request's "suffix" metadata.
func echoSuffix(t *testing.T, transport Transport, options ...CallOption) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	reply, err := Unary(ctx, transport, "/example.Greeter/Echo", &testMessage{Value: "v"}, newTestMessage, options...)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	return reply.Value
}

// confirmedConnectionMetadata returns the connection metadata set the server
// confirmed on the channel's current connection.
func confirmedConnectionMetadata(t *testing.T, channel *Channel) uint32 {
	t.Helper()
	channel.mu.RLock()
	defer channel.mu.RUnlock()
	return channel.current.(*streamGeneration).client.connectionMetadata.confirmed.Load()
}

func TestConnectionMetadataJoinsEveryCallAndIsReferencedOnceConfirmed(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	for _, tc := range []struct {
		name   string
		policy TransportPolicy
	}{
		{"quic", TransportQUICOnly},
		{"qmux", TransportTCPOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
			defer cancel()
			channel, err := Dial(ctx, listener.Addr().String(), DialOptions{
				TLSConfig:          pki.client,
				Transport:          tc.policy,
				TCPAddr:            listener.TCPAddr().String(),
				ConnectionMetadata: Metadata{"Suffix": []byte("!")},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = channel.Close() }()

			if got := echoSuffix(t, channel); got != "v!" {
				t.Fatalf("first call = %q, want v!", got)
			}
			if got := confirmedConnectionMetadata(t, channel); got != 1 {
				t.Fatalf("confirmed set = %d after the first call, want 1", got)
			}
			// Referenced from now on, and still merged by the server.
			for range 3 {
				if got := echoSuffix(t, channel); got != "v!" {
					t.Fatalf("referencing call = %q, want v!", got)
				}
			}
			// The call's own value wins.
			if got := echoSuffix(t, channel, WithMetadata("suffix", []byte("?"))); got != "v?" {
				t.Fatalf("overriding call = %q, want v?", got)
			}

			// A new set is offered again, then referenced.
			if err := channel.SetConnectionMetadata(Metadata{"suffix": []byte("#")}); err != nil {
				t.Fatal(err)
			}
			if got := echoSuffix(t, channel); got != "v#" {
				t.Fatalf("call after the change = %q, want v#", got)
			}
			if got := confirmedConnectionMetadata(t, channel); got != 2 {
				t.Fatalf("confirmed set = %d after the change, want 2", got)
			}
			if got := echoSuffix(t, channel); got != "v#" {
				t.Fatalf("referencing call after the change = %q, want v#", got)
			}

			// Removing it leaves calls with their own metadata only.
			if err := channel.SetConnectionMetadata(nil); err != nil {
				t.Fatal(err)
			}
			if got := echoSuffix(t, channel); got != "v" {
				t.Fatalf("call without connection metadata = %q, want v", got)
			}
		})
	}
}

func TestConnectionMetadataConfirmationReachesStreamingCalls(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{
		TLSConfig:          pki.client,
		Transport:          TransportQUICOnly,
		ConnectionMetadata: Metadata{"suffix": []byte("!")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()

	stream, err := ServerStreaming[*testMessage, *testMessage](ctx, channel, "/example.Greeter/Count", &testMessage{Value: "c"}, newTestMessage)
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	_ = stream.Close()
	if got := confirmedConnectionMetadata(t, channel); got != 1 {
		t.Fatalf("confirmed set = %d after a streaming call, want 1", got)
	}
	if got := echoSuffix(t, channel); got != "v!" {
		t.Fatalf("referencing call = %q, want v!", got)
	}
}

func TestConnectionMetadataSatisfiesTheAuthorizer(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		registerTestService(server)
		server.SetAuthorizer(BearerAuthorizer("secret"))
	})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{
		TLSConfig:          pki.client,
		Transport:          TransportQUICOnly,
		ConnectionMetadata: Metadata{"authorization": []byte("Bearer secret")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	for range 3 {
		if got := echoSuffix(t, channel); got != "v" {
			t.Fatalf("authorized call = %q, want v", got)
		}
	}
}

func TestHTTPAndWebTransportConnectionMetadata(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListener(t, pki, nil)
	source, err := newConnectionMetadataSource(Metadata{"suffix": []byte("!")})
	if err != nil {
		t.Fatal(err)
	}

	// The ordinary HTTP mapping uses each offer for its call alone.
	httpClient := newHTTP3TestClient(t, pki, listener).(*streamClient)
	httpClient.connectionMetadata = newClientConnectionMetadata(source)
	for range 2 {
		if got := echoSuffix(t, httpClient); got != "v!" {
			t.Fatalf("HTTP call = %q, want v!", got)
		}
	}
	if got := httpClient.connectionMetadata.confirmed.Load(); got != 0 {
		t.Fatalf("an HTTP server confirmed set %d", got)
	}

	// A WebTransport session keeps it like a connection.
	webTransport := newWebTransportTestClient(t, pki, listener, testOrigin, nil).(*streamClient)
	webTransport.connectionMetadata = newClientConnectionMetadata(source)
	for range 2 {
		if got := echoSuffix(t, webTransport); got != "v!" {
			t.Fatalf("WebTransport call = %q, want v!", got)
		}
	}
	if got := webTransport.connectionMetadata.confirmed.Load(); got != 1 {
		t.Fatalf("WebTransport confirmed set = %d, want 1", got)
	}
}

func TestConnectionMetadataStore(t *testing.T) {
	offer := func(id uint32, value string) *wireRequest {
		request := newWireRequest("/svc/m")
		request.ConnectionMetadata = &connectionMetadataInfo{ID: id, Metadata: Metadata{"k": []byte(value), "shared": []byte(value)}}
		return request
	}
	reference := func(id uint32) *wireRequest {
		request := newWireRequest("/svc/m")
		request.Metadata["shared"] = []byte("call")
		request.ConnectionMetadata = &connectionMetadataInfo{ID: id}
		return request
	}

	store := new(connectionMetadataStore)
	for id := uint32(1); id <= maxConnectionMetadataSets+1; id++ {
		request := offer(id, string(rune('a'+id)))
		if err := store.resolve(request); err != nil {
			t.Fatal(err)
		}
		if request.connectionMetadataConfirmed != id || string(request.Metadata["k"]) != string(rune('a'+id)) {
			t.Fatalf("offer %d: confirmed %d, metadata %q", id, request.connectionMetadataConfirmed, request.Metadata)
		}
	}
	// The oldest set is gone; the others merge under the call's entries.
	if err := store.resolve(reference(1)); codeOf(err) != CodeInvalidArgument {
		t.Fatalf("reference to an evicted set: %v, want InvalidArgument", err)
	}
	request := reference(2)
	if err := store.resolve(request); err != nil {
		t.Fatal(err)
	}
	if string(request.Metadata["k"]) != "c" || string(request.Metadata["shared"]) != "call" || request.connectionMetadataConfirmed != 0 {
		t.Fatalf("referencing call metadata = %q, confirmed %d", request.Metadata, request.connectionMetadataConfirmed)
	}

	// An invalid offer fails the call.
	bad := newWireRequest("/svc/m")
	bad.ConnectionMetadata = &connectionMetadataInfo{ID: 9, Metadata: Metadata{"trevrpc-x": []byte("1")}}
	if err := store.resolve(bad); codeOf(err) != CodeInvalidArgument {
		t.Fatalf("offer with a reserved key: %v, want InvalidArgument", err)
	}

	// Without a store, the HTTP mapping's, an offer serves its call only.
	var none *connectionMetadataStore
	request = offer(1, "x")
	if err := none.resolve(request); err != nil || string(request.Metadata["k"]) != "x" || request.connectionMetadataConfirmed != 0 {
		t.Fatalf("offer without a store: %v, %q, confirmed %d", err, request.Metadata, request.connectionMetadataConfirmed)
	}
	if err := none.resolve(reference(1)); codeOf(err) != CodeInvalidArgument {
		t.Fatalf("reference without a store: %v, want InvalidArgument", err)
	}
}

func TestHandlersCannotChangeTheConnectionMetadataOfLaterCalls(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, func(server *Server) {
		// Answers with the suffix it got, then overwrites it in place.
		RegisterUnary(server, "/example.Greeter/Echo", newTestMessage, func(ctx context.Context, request *testMessage) (*testMessage, error) {
			suffix := RequestMetadataFromContext(ctx)["suffix"]
			reply := &testMessage{Value: request.Value + string(suffix)}
			for i := range suffix {
				suffix[i] = 'x'
			}
			return reply, nil
		})
	})
	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	channel, err := Dial(ctx, listener.Addr().String(), DialOptions{
		TLSConfig:          pki.client,
		Transport:          TransportQUICOnly,
		ConnectionMetadata: Metadata{"suffix": []byte("!")},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = channel.Close() }()
	// The first call offers the set and the later ones refer to it: each still
	// gets the stored value, not what an earlier handler wrote.
	for range 3 {
		if got := echoSuffix(t, channel); got != "v!" {
			t.Fatalf("call = %q, want v!", got)
		}
	}
}

func TestALateConfirmationOfAReplacedSetIsIgnored(t *testing.T) {
	source, err := newConnectionMetadataSource(Metadata{"k": []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	client := newClientConnectionMetadata(source)
	first := client.prepare(newWireRequest("/svc/m"))
	if err := source.set(Metadata{"k": []byte("2")}); err != nil {
		t.Fatal(err)
	}
	second := client.prepare(newWireRequest("/svc/m"))
	client.learn(second, headersAnswer{connectionMetadata: second})
	// The first call's HEADERS arrive last.
	client.learn(first, headersAnswer{connectionMetadata: first})
	if got := client.confirmed.Load(); got != second {
		t.Fatalf("confirmed set = %d after a late confirmation of %d, want %d", got, first, second)
	}
}
