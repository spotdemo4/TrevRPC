package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// TestRefusalGoldenVectors checks the STATUS refusing an optimistic call; the
// optimistic REQUEST is checked with the other negotiation fields.
func TestRefusalGoldenVectors(t *testing.T) {
	vectors := loadZstdVectors(t)
	frame, err := appendStatusFrame(nil, &Status{Code: CodeResourceExhausted, refusedStreamCompression: true}, DefaultMaxFrameSize)
	if err != nil || !bytes.Equal(frame, vectors["status.refused_stream_compression.frame"]) {
		t.Fatalf("STATUS frame = %x, %v; want %x", frame, err, vectors["status.refused_stream_compression.frame"])
	}
	payload := vectors["status.refused_stream_compression.payload"]
	status, err := decodeStatus(payload)
	if err != nil || status.Code != CodeResourceExhausted || !status.refusedStreamCompression {
		t.Fatalf("decoded status %+v, %v", status, err)
	}
	var reflectedStatus wireStatus
	if err := decodePayload(payload, &reflectedStatus); err != nil || !reflectedStatus.RefusedStreamCompression {
		t.Fatalf("reflected status %+v, %v", reflectedStatus, err)
	}
}

// recordingAuthorizer counts the calls it authorizes, and denies them all
// when deny is set.
type recordingAuthorizer struct {
	calls atomic.Int64
	deny  bool
}

func (a *recordingAuthorizer) Authorize(context.Context, HandlerContext) error {
	a.calls.Add(1)
	if a.deny {
		return Unauthenticated("denied")
	}
	return nil
}

// TestServerRefusesOptimisticCallsAfterAuthorization checks that a server
// refuses an authorized optimistic call it cannot decode with a lone STATUS
// before the handler runs, that an unauthorized one fails authorization
// instead, and that one it can decode is answered at dispatch and decoded
// from its third request without the client waiting.
func TestServerRefusesOptimisticCallsAfterAuthorization(t *testing.T) {
	requests := RouteStreamCompression(StreamCompression{Requests: true})
	for _, test := range []struct {
		name    string
		method  string
		memory  int64
		deny    bool
		refuses bool
	}{
		{"accepted", "Upload", 0, false, false},
		{"route without request compression", "Plain", 0, false, true},
		{"stream compression off", "Upload", -1, false, true},
		{"unauthenticated", "Plain", 0, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer()
			options := server.Options()
			options.StreamCompressionMemory = test.memory
			server.SetOptions(options)
			authorizer := &recordingAuthorizer{deny: test.deny}
			server.SetAuthorizer(authorizer)
			var handled atomic.Int64
			upload := func(ctx context.Context, requests MessageStream[*wrapperspb.StringValue]) (*wrapperspb.StringValue, error) {
				handled.Add(1)
				return joinUpload(ctx, requests)
			}
			RegisterClientStreaming(server, testPath("Upload"), newStringValue, upload, requests)
			RegisterClientStreaming(server, testPath("Plain"), newStringValue, upload)

			request := requestFor(test.method, RPCKindClientStreaming)
			request.StreamCompression = streamCompressionOptimistic
			frames, err := appendRequestFrame(nil, request, DefaultMaxFrameSize)
			if err != nil {
				t.Fatal(err)
			}
			if !test.refuses && !test.deny {
				// Every request at once: the client does not wait.
				budget := newStreamBudget(0, defaultClientStreamCompressionMemory, nil)
				compressor := newStreamCompressor(budget, nil)
				compressor.enable()
				defer compressor.close()
				for i := range streamTestMessages {
					encoded, err := compressor.encode(compressionSample(i))
					if err != nil {
						t.Fatal(err)
					}
					frames = append(frames, encoded.frame...)
				}
			}
			pipe, client := io.Pipe()
			stream := &pipedRPCStream{pipe: pipe}
			done := make(chan struct{})
			go func() {
				handleRPCStream(context.Background(), server, stream, nil, nil)
				close(done)
			}()
			if _, err := client.Write(frames); err != nil {
				t.Fatal(err)
			}
			_ = client.Close()
			<-done

			if authorizer.calls.Load() != 1 {
				t.Fatalf("authorizer ran %d times, want once", authorizer.calls.Load())
			}
			if test.refuses || test.deny {
				status := refusedStreamCompression()
				if test.deny {
					status = Unauthenticated("denied")
				}
				want, err := appendStatusFrame(nil, status, DefaultMaxFrameSize)
				if err != nil {
					t.Fatal(err)
				}
				if got := stream.snapshot(); !bytes.Equal(got, want) {
					t.Fatalf("response %x, want the lone %x", got, want)
				}
				if handled.Load() != 0 {
					t.Fatalf("handler ran %d times", handled.Load())
				}
				return
			}
			got := parseRawFrames(t, stream.snapshot())
			if len(got) != 3 || got[0].typ != frameTypeAnswer || got[1].typ != frameTypeMessage || got[2].typ != frameTypeStatus {
				t.Fatalf("response frames %v, want ANSWER MESSAGE STATUS", got)
			}
			if answer, err := decodeAnswer(got[0].payload); err != nil || !answer.StreamCompression {
				t.Fatalf("ANSWER %+v, %v", answer, err)
			}
			if compressed := streamCompressedRequests.Load(); compressed != streamTestMessages-streamCompressionStart+1 {
				t.Fatalf("%d requests arrived stream-compressed, want %d", compressed, streamTestMessages-streamCompressionStart+1)
			}
		})
	}
}

// refusingAuthorizer authorizes every call of the refusing service, and
// plainUploads counts the calls of its PlainUpload handler.
var (
	refusingAuthorizer = &recordingAuthorizer{}
	plainUploads       atomic.Int64
)

// registerRefusingService registers the stream compression test routes, and
// copies of the upload and chat routes without stream compression, which
// refuse every optimistic call.
func registerRefusingService(server *Server) {
	registerStreamCompressionService(server)
	server.SetAuthorizer(refusingAuthorizer)
	RegisterClientStreaming(server, testPath("PlainUpload"), newStringValue, func(ctx context.Context, requests MessageStream[*wrapperspb.StringValue]) (*wrapperspb.StringValue, error) {
		plainUploads.Add(1)
		return joinUpload(ctx, requests)
	})
	RegisterBidirectionalStreaming(server, testPath("PlainChat"), newStringValue, func(ctx context.Context, requests MessageStream[*wrapperspb.StringValue]) (MessageStream[*wrapperspb.StringValue], error) {
		return &countingRequests{ctx: ctx, inner: requests}, nil
	})
}

// rawFrames returns the frames that crossed the stream, before any
// decompression.
func (s *recordedStream) rawFrames(t testing.TB) (sent, received []rawFrame) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	return parseRawFrames(t, bytes.Clone(s.sent.Bytes())), parseRawFrames(t, bytes.Clone(s.received.Bytes()))
}

// recordedRequest decodes the REQUEST a recorded stream starts with.
func recordedRequest(t testing.TB, stream *recordedStream) *wireRequest {
	t.Helper()
	sent, _ := stream.rawFrames(t)
	if len(sent) == 0 || sent[0].typ != frameTypeRequest {
		t.Fatalf("stream starts with %v, not REQUEST", sent)
	}
	request := &wireRequest{}
	if err := decodeRequest(sent[0].payload, request); err != nil {
		t.Fatal(err)
	}
	return request
}

func frameTypes(frames []rawFrame) string {
	types := make([]string, len(frames))
	for i, frame := range frames {
		types[i] = frame.typ.String()
	}
	return strings.Join(types, " ")
}

// optimisticTestClient is a channel's stream client whose streams are
// recorded, with a fresh budget, and optimistic again.
type optimisticTestClient struct {
	channel *Channel
	client  *streamClient
	mu      *sync.Mutex
	log     *[]*recordedStream
}

func (c *optimisticTestClient) streams() []*recordedStream {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*recordedStream(nil), *c.log...)
}

func (c *optimisticTestClient) reset() {
	c.mu.Lock()
	*c.log = nil
	c.mu.Unlock()
	c.client.pessimistic.Store(false)
}

func newOptimisticTestClient(t *testing.T, channel *Channel) *optimisticTestClient {
	t.Helper()
	generation, err := channel.callGeneration(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	client := generation.(*streamGeneration).client
	mu, log := streamClientFor(client, client.streams)
	return &optimisticTestClient{channel: channel, client: client, mu: mu, log: log}
}

// checkReplayed checks that a call went out on two streams: the optimistic
// one, which got only the refusal, and the replay, which sent the same REQUEST
// without optimistic and then every request per message.
func checkReplayed(t *testing.T, client *optimisticTestClient, requests int) {
	t.Helper()
	streams := client.streams()
	if len(streams) != 2 {
		t.Fatalf("%d streams, want the refused call and its replay", len(streams))
	}
	first, second := recordedRequest(t, streams[0]), recordedRequest(t, streams[1])
	if first.StreamCompression != streamCompressionOptimistic {
		t.Fatalf("first REQUEST %+v is not optimistic", first.StreamCompression)
	}
	if second.StreamCompression != streamCompressionEnabled || second.Method != first.Method {
		t.Fatalf("replayed REQUEST %s %+v", second.Method, second.StreamCompression)
	}
	if _, received := streams[0].rawFrames(t); len(received) != 1 || !isRefusal(frame{typ: received[0].typ, payload: received[0].payload}) {
		t.Fatalf("refused call received %s", frameTypes(received))
	}
	sent, _ := streams[1].rawFrames(t)
	if want := "REQUEST" + strings.Repeat(" MESSAGE", requests); frameTypes(sent) != want {
		t.Fatalf("replay sent %s, want %s", frameTypes(sent), want)
	}
	if !client.client.pessimistic.Load() {
		t.Fatal("the connection is still optimistic after a refusal")
	}
}

// uploadSamples sends the stream compression samples on a client-streaming
// call to method, all at once unless between is set, which runs after the
// first.
func uploadSamples(ctx context.Context, t *testing.T, transport Transport, method string, between func(), options ...CallOption) (*wrapperspb.StringValue, error) {
	t.Helper()
	options = append(options, WithStreamCompression(StreamCompression{Requests: true}))
	call, err := ClientStreaming[*wrapperspb.StringValue](ctx, transport, testPath(method), newStringValue, options...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = call.Close() }()
	for i := range streamTestMessages {
		if err := call.Send(compressionSample(i)); err != nil {
			return nil, fmt.Errorf("send %d: %w", i, err)
		}
		if i == 0 && between != nil {
			between()
		}
	}
	return call.CloseAndRecv()
}

func joinedSamples() string {
	var joined strings.Builder
	for i := range streamTestMessages {
		joined.WriteString(compressionSample(i).GetValue())
	}
	return joined.String()
}

// waitFor polls condition until it holds or a few seconds pass.
func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestOptimisticCallsReplayRefusals runs optimistic calls over the native
// transports against routes that refuse them, checking that the client
// replays each one transparently, and that it stops and resumes being
// optimistic as the server refuses and answers.
func TestOptimisticCallsReplayRefusals(t *testing.T) {
	pki := newTestPKI(t)
	listener := startTestListenerWith(t, pki, nil, registerRefusingService)
	native, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, Transport: TransportQUICOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = native.Close() })
	qmuxChannel, err := Dial(t.Context(), listener.Addr().String(), DialOptions{TLSConfig: pki.client, TCPAddr: listener.TCPAddr().String(), Transport: TransportTCPOnly})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = qmuxChannel.Close() })

	for name, channel := range map[string]*Channel{"native": native, "qmux": qmuxChannel} {
		client := newOptimisticTestClient(t, channel)
		t.Run(name+"/refused after the requests ended", func(t *testing.T) {
			client.reset()
			authorized, handled := refusingAuthorizer.calls.Load(), plainUploads.Load()
			got, err := uploadSamples(t.Context(), t, channel, "PlainUpload", nil)
			if err != nil || got.GetValue() != joinedSamples() {
				t.Fatalf("upload = %q, %v", got.GetValue(), err)
			}
			checkReplayed(t, client, streamTestMessages)
			// Each attempt is authorized; only the replay reaches the handler.
			if authorized, handled := refusingAuthorizer.calls.Load()-authorized, plainUploads.Load()-handled; authorized != 2 || handled != 1 {
				t.Fatalf("authorized %d times, handled %d times; want 2 and 1", authorized, handled)
			}
		})
		t.Run(name+"/refused while uploading", func(t *testing.T) {
			client.reset()
			got, err := uploadSamples(t.Context(), t, channel, "PlainUpload", func() {
				waitFor(t, "the replay", func() bool { return len(client.streams()) == 2 })
			})
			if err != nil || got.GetValue() != joinedSamples() {
				t.Fatalf("upload = %q, %v", got.GetValue(), err)
			}
			checkReplayed(t, client, streamTestMessages)
		})
		t.Run(name+"/bidirectional", func(t *testing.T) {
			client.reset()
			chat, err := BidirectionalStreaming[*wrapperspb.StringValue](t.Context(), channel, testPath("PlainChat"), newStringValue, WithStreamCompression(StreamCompression{Requests: true}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = chat.Close() }()
			for i := range streamTestMessages {
				if err := chat.Send(compressionSample(i)); err != nil {
					t.Fatal(err)
				}
				got, err := chat.Recv()
				if err != nil || got.GetValue() != compressionSample(i).GetValue() {
					t.Fatalf("message %d = %q, %v", i, got.GetValue(), err)
				}
			}
			if err := chat.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if _, err := chat.Recv(); !errors.Is(err, io.EOF) {
				t.Fatalf("end of chat: %v", err)
			}
			// The first request was replayed, the others sent one at a time.
			streams := client.streams()
			if len(streams) != 2 {
				t.Fatalf("%d streams, want 2", len(streams))
			}
			if sent, _ := streams[1].rawFrames(t); frameTypes(sent) != "REQUEST"+strings.Repeat(" MESSAGE", streamTestMessages) {
				t.Fatalf("replay sent %s", frameTypes(sent))
			}
		})
		t.Run(name+"/deadline", func(t *testing.T) {
			client.reset()
			if _, err := uploadSamples(t.Context(), t, channel, "PlainUpload", nil, WithTimeout(time.Minute)); err != nil {
				t.Fatal(err)
			}
			streams := client.streams()
			if len(streams) != 2 {
				t.Fatalf("%d streams, want 2", len(streams))
			}
			first, second := recordedRequest(t, streams[0]), recordedRequest(t, streams[1])
			if second.TimeoutNanos == 0 || second.TimeoutNanos > first.TimeoutNanos {
				t.Fatalf("replay timeout %d, after %d", second.TimeoutNanos, first.TimeoutNanos)
			}
		})
		t.Run(name+"/stop and resume", func(t *testing.T) {
			client.reset()
			if _, err := uploadSamples(t.Context(), t, channel, "PlainUpload", nil); err != nil {
				t.Fatal(err)
			}
			// After the refusal the next call offers without optimism...
			if _, err := uploadSamples(t.Context(), t, channel, "StreamUpload", nil); err != nil {
				t.Fatal(err)
			}
			streams := client.streams()
			if request := recordedRequest(t, streams[2]); request.StreamCompression != streamCompressionEnabled {
				t.Fatalf("call after the refusal offered %+v", request.StreamCompression)
			}
			// ...and its answer makes the one after optimistic again.
			if client.client.pessimistic.Load() {
				t.Fatal("still pessimistic after an answer")
			}
			got, err := uploadSamples(t.Context(), t, channel, "StreamUpload", nil)
			if err != nil || got.GetValue() != joinedSamples() {
				t.Fatalf("upload = %q, %v", got.GetValue(), err)
			}
			streams = client.streams()
			if len(streams) != 4 || recordedRequest(t, streams[3]).StreamCompression != streamCompressionOptimistic {
				t.Fatalf("%d streams; the last call is not optimistic", len(streams))
			}
			if compressed := streamCompressedRequests.Load(); compressed != streamTestMessages-streamCompressionStart+1 {
				t.Fatalf("%d requests arrived stream-compressed, want %d", compressed, streamTestMessages-streamCompressionStart+1)
			}
		})
		if name == "native" {
			t.Run(name+"/datagram flow", func(t *testing.T) {
				client.reset()
				chat, err := BidirectionalStreaming[*wrapperspb.StringValue](t.Context(), channel, testPath("StreamChat"), newStringValue,
					WithStreamCompression(StreamCompression{Requests: true, Responses: true}), WithUnreliable(Unreliable{Responses: true}))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = chat.Close() }()
				if err := chat.Send(compressionSample(0)); err != nil {
					t.Fatal(err)
				}
				if got, err := chat.Recv(); err != nil || got.GetValue() != compressionSample(0).GetValue() {
					t.Fatalf("echo = %q, %v", got.GetValue(), err)
				}
				// A call with a datagram flow offers, but waits for the answer.
				request := recordedRequest(t, client.streams()[0])
				if request.DatagramFlow == 0 || request.StreamCompression != streamCompressionEnabled {
					t.Fatalf("REQUEST flow %d, offer %+v; want a flow and an offer without optimism", request.DatagramFlow, request.StreamCompression)
				}
			})
		}
		t.Run(name+"/cancelled during the replay", func(t *testing.T) {
			client.reset()
			opener := client.client.opener
			blocking := &blockingOpener{inner: opener, opened: make(chan struct{}, 1)}
			client.client.opener = blocking
			defer func() { client.client.opener = opener }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			go func() {
				<-blocking.opened
				cancel()
			}()
			if _, err := uploadSamples(ctx, t, channel, "PlainUpload", nil); StatusFromError(err).Code != CodeCancelled {
				t.Fatalf("upload = %v, want Cancelled", err)
			}
			waitForEmptyBudget(t, "client", client.client.streams)
		})
	}
	waitForEmptyBudget(t, "server", listener.server.freeze().streams)
}

// blockingOpener opens the first stream, and blocks every later open until
// its context ends, reporting it on opened.
type blockingOpener struct {
	inner  streamOpener
	calls  atomic.Int64
	opened chan struct{}
}

func (o *blockingOpener) openStream(ctx context.Context) (clientStream, error) {
	if o.calls.Add(1) == 1 {
		return o.inner.openStream(ctx)
	}
	o.opened <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestOptimisticUploadWaitsForRoom checks that keeping a request waits while
// the copies would pass maxOptimisticCopies or the budget, until the upload
// settles or its call ends, and that settling returns the copies' memory.
func TestOptimisticUploadWaitsForRoom(t *testing.T) {
	newUpload := func(ctx context.Context, limit int64) (*optimisticUpload, *streamBudget) {
		budget := &streamBudget{limit: limit, level: 1, encoderCost: zstd.StreamEncoderCost(1, zstd.StreamWindowLog)}
		client := newStreamClient(nil, DefaultMaxFrameSize)
		client.streams = budget
		request := requestFor("Upload", RPCKindClientStreaming)
		return newOptimisticUpload(ctx, client, nil, request, nil, newStreamCompressor(budget, nil)), budget
	}
	encodeAsync := func(u *optimisticUpload) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, err := u.encode(compressionSample(0))
			result <- err
		}()
		return result
	}
	expectBlocked := func(t *testing.T, result <-chan error) {
		t.Helper()
		select {
		case err := <-result:
			t.Fatalf("encode returned %v while there was no room", err)
		case <-time.After(50 * time.Millisecond):
		}
	}

	t.Run("kept within the budget", func(t *testing.T) {
		u, budget := newUpload(t.Context(), 1<<20)
		if _, err := u.encode(compressionSample(0)); err != nil {
			t.Fatal(err)
		}
		if size := int64(proto.Size(compressionSample(0))); budget.used.Load() != size || len(u.kept) != 1 {
			t.Fatalf("budget holds %d, %d kept; want %d, 1", budget.used.Load(), len(u.kept), size)
		}
		u.settle()
		if budget.used.Load() != 0 {
			t.Fatalf("budget holds %d after settling", budget.used.Load())
		}
	})
	t.Run("budget", func(t *testing.T) {
		u, _ := newUpload(t.Context(), 1)
		result := encodeAsync(u)
		expectBlocked(t, result)
		u.settle()
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if len(u.kept) != 0 {
			t.Fatal("kept a request after settling")
		}
	})
	t.Run("cap", func(t *testing.T) {
		u, _ := newUpload(t.Context(), 1<<30)
		u.keptBytes = maxOptimisticCopies
		result := encodeAsync(u)
		expectBlocked(t, result)
		u.end()
		if err := <-result; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("call ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		u, _ := newUpload(ctx, 1)
		result := encodeAsync(u)
		expectBlocked(t, result)
		cancel()
		if err := <-result; StatusFromError(err).Code != CodeCancelled {
			t.Fatalf("encode = %v, want Cancelled", err)
		}
	})
}

// isRefusal reports whether frame is a STATUS refusing an optimistic call.
func isRefusal(frame frame) bool {
	if frame.typ != frameTypeStatus {
		return false
	}
	status, err := decodeStatus(frame.payload)
	return err == nil && status.refusedStreamCompression
}

// TestOptimisticCallNeedsAnAnswer checks that the response to an optimistic
// call that the server neither refused nor accepted with an ANSWER is a
// protocol error: the server would be decoding requests it was never asked
// to take.
func TestOptimisticCallNeedsAnAnswer(t *testing.T) {
	reply, err := encodeMessage(wrapperspb.String("x"))
	if err != nil {
		t.Fatal(err)
	}
	declined, err := appendAnswerFrame(nil, &wireAnswer{}, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	for name, response := range map[string][]byte{
		"HEADERS":                           concat(headersBytes(t, Metadata{"x-h": []byte("1")}), reply.frame, statusBytes(t, OK())),
		"MESSAGE":                           concat(reply.frame, statusBytes(t, OK())),
		"ANSWER without stream compression": concat(declined, reply.frame, statusBytes(t, OK())),
	} {
		t.Run(name, func(t *testing.T) {
			stream := newUnaryTransportTestStream(response)
			close(stream.eof)
			client := newStreamClient(unaryTestOpener{stream: stream}, DefaultMaxFrameSize)
			client.streams = newStreamBudget(0, defaultClientStreamCompressionMemory, nil)
			call, err := ClientStreaming[*wrapperspb.StringValue](t.Context(), client, testPath("Upload"), newStringValue, WithStreamCompression(StreamCompression{Requests: true}))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = call.Close() }()
			if _, err := call.CloseAndRecv(); StatusFromError(err).Code != CodeInternal {
				t.Fatalf("CloseAndRecv() = %v, want Internal", err)
			}
			if request := recordedRequestBytes(t, stream.written.Bytes()); request.StreamCompression != streamCompressionOptimistic {
				t.Fatalf("REQUEST %+v is not optimistic", request)
			}
		})
	}
}

// recordedRequestBytes decodes the REQUEST frame data starts with.
func recordedRequestBytes(t *testing.T, data []byte) *wireRequest {
	t.Helper()
	frames := readTestFrames(t, data, DefaultMaxFrameSize)
	if len(frames) == 0 || frames[0].Kind != frameTypeRequest {
		t.Fatalf("sent %#v, want a REQUEST first", frames)
	}
	return frames[0].Request
}
