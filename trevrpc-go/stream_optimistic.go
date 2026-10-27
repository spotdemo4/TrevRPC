package trevrpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// maxOptimisticCopies bounds the request bytes an optimistic call keeps until
// the server accepts it. A send beyond it waits for the server's answer.
const maxOptimisticCopies = 1 << 20

// Generations of an optimistic upload: the first, optimistic call, and the
// call that replays it after a refusal.
const (
	optimisticGeneration = 1
	replayGeneration     = 2
)

// keptRequest is a request message an optimistic call keeps for a replay: its
// marshalled payload, and the dictionary it may be compressed with.
type keptRequest struct {
	payload []byte
	dict    *compressionDict
}

// optimisticUpload is the request side of a call that stream-compresses its
// requests without waiting for the server's answer (see "Optimistic Stream
// Compression" in the wire format). It keeps every request it sends until the
// server's ANSWER accepts the call. If the server refuses it instead, the
// upload replays the call on a new stream: the same REQUEST without
// optimistic, the kept requests, then the rest of the caller's requests, all
// without the caller noticing.
//
// The writer goroutine encodes and writes through it; the response pump
// settles it or replays it. Every message is stamped with the generation it
// was encoded in, so that one encoded for the refused call, and therefore kept
// and replayed, is never written to the new one.
type optimisticUpload struct {
	client *streamClient
	ctx    context.Context
	// request is the optimistic REQUEST, and accept the dictionaries the call
	// accepts on its responses, for the replay.
	request    *wireRequest
	accept     *dictSet
	compressor *streamCompressor
	per        *messageCompressor

	current    atomic.Pointer[streamRef]
	generation atomic.Uint32
	// ended is set when the call ends, so that a replay finishing later
	// aborts the stream it opened.
	ended atomic.Bool

	// writeMu orders writes against the replay; closed records under it that
	// the request side was finished.
	writeMu sync.Mutex
	closed  bool

	// stateMu guards encoding and the copies, which are kept while keeping is
	// set; settled is closed when it is cleared.
	stateMu      sync.Mutex
	keeping      bool
	kept         []keptRequest
	keptBytes    int
	reservations []*streamReservation
	settled      chan struct{}

	// decided is closed once the first frame of the optimistic call, or its
	// failure, is known; refused says it was a refusal. replayed is closed
	// when the replay is over, replayErr says how it went.
	decideOnce sync.Once
	decided    chan struct{}
	refused    atomic.Bool
	replayed   chan struct{}
	replayErr  error
}

// streamRef holds a clientStream for an atomic.Pointer.
type streamRef struct{ clientStream }

func newOptimisticUpload(ctx context.Context, client *streamClient, stream clientStream, request *wireRequest, accept *dictSet, compressor *streamCompressor) *optimisticUpload {
	u := &optimisticUpload{
		client:     client,
		ctx:        ctx,
		request:    request,
		accept:     accept,
		compressor: compressor,
		per:        client.compression.requestCompressor(request.plainCompression.Requests),
		keeping:    true,
		settled:    make(chan struct{}),
		decided:    make(chan struct{}),
		replayed:   make(chan struct{}),
	}
	u.current.Store(&streamRef{stream})
	u.generation.Store(optimisticGeneration)
	return u
}

// stream returns the stream the call currently uses.
func (u *optimisticUpload) stream() clientStream { return u.current.Load().clientStream }

// encode encodes a request message with the call's compressors and, until the
// server accepts the call, keeps it. Keeping waits while the copies would pass
// maxOptimisticCopies or the channel's budget has no room for them.
func (u *optimisticUpload) encode(message ProtoMessage) (encodedMessage, error) {
	protoMessage := protoMessageV2(message)
	size := proto.Size(protoMessage)
	var reservation *streamReservation
	u.stateMu.Lock()
	for u.keeping && size <= u.client.maxFrameSize {
		if u.keptBytes+size <= maxOptimisticCopies {
			if reservation = u.client.streams.reserve(int64(size)); reservation != nil {
				break
			}
		}
		settled := u.settled
		u.stateMu.Unlock()
		select {
		case <-settled:
		case <-u.ctx.Done():
			return encodedMessage{}, statusFromContextError(u.ctx.Err())
		}
		u.stateMu.Lock()
	}
	defer u.stateMu.Unlock()
	if !u.keeping {
		// Settled while waiting: nothing is kept any more.
		reservation.release()
		reservation = nil
	}
	var encoded encodedMessage
	var err error
	if reservation == nil {
		encoded, err = u.compressor.encode(message)
	} else {
		var payload []byte
		if payload, err = proto.Marshal(protoMessage); err == nil {
			encoded, err = u.compressor.encodeMarshalled(payload, protoMessage)
		}
		if err != nil {
			reservation.release()
		} else {
			u.kept = append(u.kept, keptRequest{payload: payload, dict: u.per.compressorFor(protoMessage)})
			u.keptBytes += size
			u.reservations = append(u.reservations, reservation)
		}
	}
	encoded.generation = u.generation.Load()
	return encoded, err
}

// settle stops keeping requests and frees the copies: the server accepted the
// call, or it ended. It is safe to call more than once.
func (u *optimisticUpload) settle() {
	u.stateMu.Lock()
	defer u.stateMu.Unlock()
	u.releaseCopies()
}

// releaseCopies ends keeping; stateMu must be held.
func (u *optimisticUpload) releaseCopies() {
	if !u.keeping {
		return
	}
	u.keeping = false
	close(u.settled)
	for _, reservation := range u.reservations {
		reservation.release()
	}
	u.kept, u.reservations, u.keptBytes = nil, nil, 0
}

// decide records the outcome of the optimistic call once its first frame, or
// its failure, is known.
func (u *optimisticUpload) decide(refused bool) {
	u.decideOnce.Do(func() {
		u.refused.Store(refused)
		close(u.decided)
	})
}

// undecided reports whether the first frame of the optimistic call is still
// to come.
func (u *optimisticUpload) undecided() bool {
	select {
	case <-u.decided:
		return false
	default:
		return true
	}
}

// awaitReplay waits for the outcome of the optimistic call, after a write to
// its stream failed. It reports whether the call was refused and replayed, in
// which case everything written to the failed stream was replayed.
func (u *optimisticUpload) awaitReplay() bool {
	select {
	case <-u.decided:
	case <-u.ctx.Done():
		return false
	}
	if !u.refused.Load() {
		return false
	}
	select {
	case <-u.replayed:
		return true
	case <-u.ctx.Done():
		return false
	}
}

// Write writes the REQUEST frame of a stream whose messages are written
// separately.
func (u *optimisticUpload) Write(data []byte) (int, error) {
	if err := u.writeMessageFrames(data, nil); err != nil {
		return 0, err
	}
	return len(data), nil
}

// writeMessageFrames writes header and the messages of the current
// generation. Those of the refused call, and its REQUEST, were replayed. A
// write that failed because the server refused the call succeeds once the
// call was replayed, since it was part of the replay.
func (u *optimisticUpload) writeMessageFrames(header []byte, messages []encodedMessage) error {
	u.writeMu.Lock()
	generation := u.generation.Load()
	if generation != optimisticGeneration {
		header = nil
	}
	current := messages[:0]
	for _, message := range messages {
		if message.generation == generation {
			current = append(current, message)
		}
	}
	var err error
	if len(header) > 0 || len(current) > 0 {
		err = writeFrames(u.stream(), header, current)
	}
	u.writeMu.Unlock()
	if err != nil && generation == optimisticGeneration && u.awaitReplay() {
		return u.replayErr
	}
	return err
}

// Close finishes the request side.
func (u *optimisticUpload) Close() error {
	u.writeMu.Lock()
	u.closed = true
	generation := u.generation.Load()
	err := u.stream().Close()
	u.writeMu.Unlock()
	if err != nil && generation == optimisticGeneration && u.awaitReplay() {
		return u.replayErr
	}
	return err
}

// cancelWrite aborts the request side of the current stream.
func (u *optimisticUpload) cancelWrite() { u.stream().cancelWrite() }

// cancel aborts both directions of the current stream.
func (u *optimisticUpload) cancel() {
	stream := u.stream()
	stream.cancelRead()
	stream.cancelWrite()
}

// end settles the upload when the call ends.
func (u *optimisticUpload) end() {
	u.ended.Store(true)
	u.settle()
}

// replay replaces the refused call with a new one on a new stream: the same
// REQUEST, no longer optimistic, with the time that remains, then the kept
// requests per message, then, from the writer, the rest. The writer is held
// off while it happens, and the old stream is aborted.
func (u *optimisticUpload) replay() (stream clientStream, err error) {
	defer func() {
		u.replayErr = err
		close(u.replayed)
	}()
	old := u.stream()
	// A write blocked on the refused stream holds writeMu; this ends it.
	old.cancelWrite()
	u.writeMu.Lock()
	defer u.writeMu.Unlock()
	defer old.cancelRead()

	u.stateMu.Lock()
	kept, reservations := u.kept, u.reservations
	u.reservations = nil
	u.releaseCopies()
	u.compressor.reset(len(kept))
	u.generation.Store(replayGeneration)
	u.stateMu.Unlock()
	defer func() {
		for _, reservation := range reservations {
			reservation.release()
		}
	}()

	request := *u.request
	request.StreamCompression = streamCompressionEnabled
	if deadline, ok := u.ctx.Deadline(); ok {
		request.TimeoutNanos = deadlineTimeoutNanos(clientDeadline{at: deadline, set: true}, time.Now)
	}
	frames, err := appendRequestFrame(nil, &request, u.client.maxFrameSize)
	if err != nil {
		return nil, err
	}
	for _, message := range kept {
		frames = append(frames, encodePayload(message.payload, message.dict).frame...)
	}

	if stream, err = u.client.opener.openStream(u.ctx); err != nil {
		return nil, callStatus(u.ctx, err)
	}
	u.current.Store(&streamRef{stream})
	if u.ended.Load() {
		// The call ended while the stream was opened.
		u.cancel()
		return nil, statusFromContextError(context.Canceled)
	}
	if _, err := stream.Write(frames); err != nil {
		return nil, callStatus(u.ctx, err)
	}
	if u.closed {
		if err := stream.Close(); err != nil {
			return nil, callStatus(u.ctx, err)
		}
	}
	return stream, nil
}

// requestSink is where a call's requests are written: its stream, or the
// optimistic upload that may replace the stream.
type requestSink interface {
	io.Writer
	Close() error
	cancelWrite()
}
