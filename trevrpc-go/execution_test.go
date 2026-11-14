package trevrpc

import (
	"context"
	"errors"
	"iter"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// diagnosticRecorder collects the diagnostics of a server.
type diagnosticRecorder struct {
	mu     sync.Mutex
	events []ServerDiagnostic
}

func recordDiagnostics(server *Server) *diagnosticRecorder {
	recorder := &diagnosticRecorder{}
	server.SetDiagnostics(func(event ServerDiagnostic) {
		recorder.mu.Lock()
		recorder.events = append(recorder.events, event)
		recorder.mu.Unlock()
	})
	return recorder
}

func (r *diagnosticRecorder) phases() []ServerDiagnosticPhase {
	r.mu.Lock()
	defer r.mu.Unlock()
	phases := make([]ServerDiagnosticPhase, len(r.events))
	for i, event := range r.events {
		phases[i] = event.Phase
	}
	return phases
}

func TestDetachedExecutionThatReturnsPromptlyIsNotReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer()
		diagnostics := recordDiagnostics(server)
		runtime := server.freeze()
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()

		execution := lease.retain(ServerDiagnosticHandlerPanic)
		execution.detach()
		if got := runtime.detached.Load(); got != 1 {
			t.Fatalf("detached executions = %d, want 1", got)
		}
		time.Sleep(time.Millisecond)
		execution.finish()
		time.Sleep(2 * detachedExecutionReportDelay)
		synctest.Wait()

		if got := diagnostics.phases(); len(got) != 0 {
			t.Fatalf("diagnostics = %v, want none", got)
		}
		if got := runtime.detached.Load(); got != 0 {
			t.Fatalf("detached executions = %d, want 0", got)
		}
	})
}

func TestDetachedExecutionThatRunsOnIsReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer()
		diagnostics := recordDiagnostics(server)
		runtime := server.freeze()
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()

		execution := lease.retain(ServerDiagnosticHandlerPanic)
		execution.detach()
		time.Sleep(detachedExecutionReportDelay)
		synctest.Wait()
		if got, want := diagnostics.phases(), []ServerDiagnosticPhase{ServerDiagnosticExecutionDetached}; !slices.Equal(got, want) {
			t.Fatalf("diagnostics = %v, want %v", got, want)
		}
		diagnostics.mu.Lock()
		detached := diagnostics.events[0]
		diagnostics.mu.Unlock()
		if detached.Service != "service" || detached.Method != "method" || detached.DetachedExecutionCount != 1 {
			t.Fatalf("execution_detached = %+v, want service/method with 1 detached execution", detached)
		}

		execution.finish()
		synctest.Wait()
		if got, want := diagnostics.phases(), []ServerDiagnosticPhase{ServerDiagnosticExecutionDetached, ServerDiagnosticExecutionFinallyExited}; !slices.Equal(got, want) {
			t.Fatalf("diagnostics = %v, want %v", got, want)
		}
		if got := runtime.detached.Load(); got != 0 {
			t.Fatalf("detached executions = %d, want 0", got)
		}
	})
}

// TestReusedExecutionIsNotReportedForTheOneBefore reuses a reference, as the
// read-ahead does for each Recv: the execution before was detached, the one
// after is not, and runs on past the report delay.
func TestReusedExecutionIsNotReportedForTheOneBefore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer()
		diagnostics := recordDiagnostics(server)
		runtime := server.freeze()
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()

		var reference executionReference
		execution, ok := lease.tryRetainAs(&reference, ServerDiagnosticResponseStreamPanic)
		if !ok {
			t.Fatal("a held lease could not be retained")
		}
		execution.detach()
		execution.finish()
		if execution, ok = lease.tryRetainAs(&reference, ServerDiagnosticResponseStreamPanic); !ok {
			t.Fatal("a held lease could not be retained again")
		}
		time.Sleep(2 * detachedExecutionReportDelay)
		synctest.Wait()
		execution.finish()
		synctest.Wait()

		if got := diagnostics.phases(); len(got) != 0 {
			t.Fatalf("diagnostics = %v, want none", got)
		}
	})
}

// TestCancelledStreamFromSeqThatWatchesItsContextIsNotReported cancels a
// server-streaming call whose StreamFromSeq handler is waiting for its next
// message and returns when its context ends, as StreamFromSeq's documentation
// asks: the read-ahead stops waiting for it a moment before it returns, which
// is not worth reporting.
func TestCancelledStreamFromSeqThatWatchesItsContextIsNotReported(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		server := NewServer()
		diagnostics := recordDiagnostics(server)
		server.routeStreaming("/example.Greeter/Watch", RPCKindServerStreaming, func(ctx context.Context, _ []byte, _ byteStream) (encodedStream, error) {
			var seq iter.Seq2[encodedMessage, error] = func(func(encodedMessage, error) bool) {
				<-ctx.Done()
				// The seq returns a moment after the read-ahead gives up on it.
				time.Sleep(time.Millisecond)
			}
			return StreamFromSeq(seq), nil
		})
		request := newWireRequest("/example.Greeter/Watch")
		request.Kind = RPCKindServerStreaming
		response, ok := server.handleStreamingWriter(ctx, request, nil, newEmptyStream[[]byte]()).(*serverResponseStream)
		if !ok {
			t.Fatal("the call did not produce a streaming response")
		}

		finished := make(chan struct{})
		go func() {
			defer close(finished)
			_, _ = response.writeNext(ctx, &recordingWriter{}, DefaultMaxFrameSize, nil)
		}()
		// The read-ahead is waiting in Recv for the seq to yield.
		synctest.Wait()
		cancel()
		<-finished
		discardMessageStream(response)

		// The Recv waiting for the seq was detached.
		if got := server.freeze().detached.Load(); got != 1 {
			t.Fatalf("detached executions = %d, want 1", got)
		}

		time.Sleep(2 * detachedExecutionReportDelay)
		synctest.Wait()
		if got := diagnostics.phases(); len(got) != 0 {
			t.Fatalf("diagnostics = %v, want none", got)
		}
		if got := server.freeze().detached.Load(); got != 0 {
			t.Fatalf("detached executions = %d, want 0", got)
		}
	})
}

func TestRaceGuardedDetachesACallThatOutlivesItsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer()
		runtime := server.freeze()
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		release := make(chan struct{})
		abandoned := make(chan int, 1)
		call := func() (int, error) {
			<-release
			return 7, nil
		}
		result, err := raceGuarded(ctx, lease, ServerDiagnosticHandlerPanic, true, call, func(result int) { abandoned <- result })
		if result != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("raceGuarded = %d, %v, want 0 and the deadline", result, err)
		}
		if got := runtime.detached.Load(); got != 1 {
			t.Fatalf("detached executions while the call runs on = %d, want 1", got)
		}

		close(release)
		synctest.Wait()
		if got := <-abandoned; got != 7 {
			t.Fatalf("abandoned result = %d, want 7", got)
		}
		if got := runtime.detached.Load(); got != 0 {
			t.Fatalf("detached executions after the call returned = %d, want 0", got)
		}
	})
}

func TestARacedAuthorizerThatApprovesAfterTheDeadlineFailsTheCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := NewServer()
		runtime := server.freeze()
		lease, ok := runtime.tryRequestLease(newWireRequest("/service/method"))
		if !ok {
			t.Fatal("failed to acquire request lease")
		}
		defer lease.release()

		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		approved := AuthorizerFunc(func(ctx context.Context, _ HandlerContext) (context.Context, error) {
			time.Sleep(2 * time.Second)
			return ctx, nil
		})
		next, err := invokeAuthorizer(ctx, approved, HandlerContext{}, true, lease)
		if next != ctx || StatusFromError(err).Code != CodeDeadlineExceeded {
			t.Fatalf("invokeAuthorizer = %v, %v, want the call's context and DeadlineExceeded", next, err)
		}
		if got := runtime.detached.Load(); got != 1 {
			t.Fatalf("detached executions while the authorizer runs on = %d, want 1", got)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if got := runtime.detached.Load(); got != 0 {
			t.Fatalf("detached executions after the authorizer returned = %d, want 0", got)
		}
	})
}
