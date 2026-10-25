package trevrpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"
)

// clientStream is the stream surface the RPC client needs from a transport.
// Close closes only the send direction.
type clientStream interface {
	io.ReadWriteCloser
	SetReadDeadline(time.Time) error
	// cancelRead aborts the receive direction.
	cancelRead()
	// cancelWrite aborts the send direction.
	cancelWrite()
}

// streamOpener opens one bidirectional stream per RPC.
type streamOpener interface {
	openStream(context.Context) (clientStream, error)
}

// streamClient runs RPCs over bidirectional streams, one stream per call.
type streamClient struct {
	opener       streamOpener
	maxFrameSize int
	// datagrams carries the datagram flows of the calls, or is nil when the
	// connection has none. It is set before the client is used.
	datagrams *datagramMux
	// compression is the connection's compression state, or nil without
	// dictionaries. It is set before the client is used.
	compression *clientCompression
	// streams is the channel's stream compression budget, nil when stream
	// compression is off. It is set before the client is used.
	streams *streamBudget
	// pessimistic is set while calls on the connection do not stream-compress
	// their requests optimistically: from a refusal until a call's HEADERS
	// answer stream compression again.
	pessimistic atomic.Bool
}

func newStreamClient(opener streamOpener, maxFrameSize int) *streamClient {
	if maxFrameSize <= 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	return &streamClient{opener: opener, maxFrameSize: maxFrameSize}
}

// call sends the request and its single message in one write, finishes the
// request side, and returns the response frames.
func (c *streamClient) call(
	ctx context.Context,
	request *wireRequest,
	message ProtoMessage,
) (frameStream, error) {
	protoMessage := protoMessageV2(message)
	accept, queried := c.compression.prepare(request)
	// A server-streaming call may stream-compress its responses.
	streams := c.prepareStreams(request, accept)
	// A message that may be compressed is encoded on its own; any other is
	// marshalled straight behind the request.
	var compressed encodedMessage
	if compressor := c.compression.requestCompressor(request.plainCompression.Requests); compressor.compressorFor(protoMessage) != nil {
		var err error
		if compressed, err = compressor.encode(message); err != nil {
			streams.close()
			return nil, err
		}
	}
	messageSize, messageFrameLen := compressed.size, len(compressed.frame)
	if compressed.frame == nil {
		messageSize = proto.Size(protoMessage)
		messageFrameLen = frameHeaderLen(frameTypeMessage, messageSize) + messageSize
	}
	flow := c.openFlow(request, accept)
	requestSize, _ := request.payloadSize()
	// The frames are built in one allocation.
	frames := make([]byte, 0, frameHeaderSize+requestSize+messageFrameLen)
	frames, err := appendRequestFrame(frames, request, c.maxFrameSize)
	if err != nil {
		flow.release()
		streams.close()
		return nil, err
	}
	if messageSize > c.maxFrameSize {
		flow.release()
		streams.close()
		return nil, &FrameTooLargeError{Len: messageSize, Max: c.maxFrameSize}
	}
	if compressed.frame != nil {
		frames = append(frames, compressed.frame...)
	} else if frames, err = appendProtoFrame(frames, frameTypeMessage, protoMessage, messageSize); err != nil {
		flow.release()
		streams.close()
		return nil, err
	}

	stream, err := c.opener.openStream(ctx)
	if err != nil {
		flow.release()
		streams.close()
		return nil, callStatus(ctx, err)
	}
	response := &transportResponseStream{
		ctx:          ctx,
		stream:       stream,
		reader:       newResponseFrameReader(stream, c.maxFrameSize, accept, streams.responseInflater()),
		stopCancel:   cancelStreamOnContext(ctx, stream),
		maxFrameSize: c.maxFrameSize,
		flow:         flow,
		compression:  c.compression,
		queried:      queried,
		streams:      streams,
	}

	if _, err := stream.Write(frames); err != nil {
		stream.cancelWrite()
		response.finish(true)
		return nil, callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		response.finish(true)
		return nil, callStatus(ctx, err)
	}
	if flow != nil {
		// A call with a flow reads its response ahead, so that a pending Recv
		// wakes on a datagram as well as on a frame.
		response.startPump()
	}
	return response, nil
}

// openFlow gives the call a datagram flow if the connection has datagrams and
// the call asks for them, and records its ID in request. The decision is made
// here, inside the connection that carries the call. It returns nil for a call
// without a flow.
func (c *streamClient) openFlow(request *wireRequest, accept *dictSet) *datagramFlow {
	if c.datagrams == nil {
		return nil
	}
	kind := request.rpcKind()
	if !request.unreliable.effective(kind).any() {
		return nil
	}
	flow := c.datagrams.allocate(kind.streamsResponses(), c.maxFrameSize, accept.lookupOrNil())
	if flow == nil {
		return nil
	}
	request.DatagramFlow = flow.id
	return flow
}

func (c *streamClient) streamingCall(
	ctx context.Context,
	request *wireRequest,
	requests encodedStream,
) (frameStream, error) {
	accept, queried := c.compression.prepare(request)
	streams := c.prepareStreams(request, accept)
	setter, _ := requests.(compressorSetter)
	if setter != nil {
		setter.setCompressors(c.compression.requestCompressor(request.plainCompression.Requests), streams.requestCompressor())
	}
	flow := c.openFlow(request, accept)
	// A call that may stream-compress its requests starts without waiting for
	// the server's answer, unless the server of this connection refused that
	// since it last answered. A call with a datagram flow waits: a replay
	// would have to move its flow to the new call.
	optimistic := setter != nil && streams.requestCompressor() != nil && flow == nil && !c.pessimistic.Load()
	if optimistic {
		request.StreamCompression.Optimistic = true
		streams.requests.enable(int(request.StreamCompression.MaxWindowLog))
	}
	header, err := appendRequestFrame(nil, request, c.maxFrameSize)
	if err != nil {
		flow.release()
		streams.close()
		return nil, err
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := c.opener.openStream(streamCtx)
	if err != nil {
		statusErr := callStatus(streamCtx, err)
		cancel()
		flow.release()
		streams.close()
		return nil, statusErr
	}

	// The requests of an optimistic call go through its upload, which may
	// replace the stream.
	var sink requestSink = stream
	var upload *optimisticUpload
	stopCancel := cancelStreamOnContext
	if optimistic {
		upload = newOptimisticUpload(streamCtx, c, stream, request, accept, streams.requests)
		setter.setUpload(upload)
		sink = upload
		stopCancel = func(ctx context.Context, _ clientStream) func() { return watchContextCancellation(ctx, upload.cancel) }
	}

	// The upload has a context of its own, so it can be stopped when the
	// response ends while the response stream stays readable.
	uploadCtx, cancelUpload := context.WithCancel(streamCtx)
	writerDone := make(chan error, 1)
	response := &transportResponseStream{
		ctx:          streamCtx,
		stream:       stream,
		reader:       newResponseFrameReader(stream, c.maxFrameSize, accept, streams.responseInflater()),
		writerDone:   writerDone,
		cancelUpload: cancelUpload,
		cancel:       cancel,
		stopCancel:   stopCancel(streamCtx, stream),
		maxFrameSize: c.maxFrameSize,
		frames:       make(chan pumpedFrame, responseReadAheadFrames),
		drained:      make(chan struct{}, 1),
		pumpStop:     make(chan struct{}),
		outcome:      make(chan struct{}),
		flow:         flow,
		compression:  c.compression,
		queried:      queried,
		streams:      streams,
		optimistic:   upload,
		pessimistic:  &c.pessimistic,
	}
	if aborter, ok := requests.(uploadAborter); ok {
		response.abortUpload = aborter.abortUpload
	}
	var sender *datagramSender
	if flow != nil {
		response.sink = flow.sink
		if request.unreliable.effective(request.rpcKind()).Requests {
			sender = &datagramSender{flow: flow, ctx: uploadCtx}
		}
	}
	go func() {
		err := c.writeStreamingRequest(uploadCtx, sink, header, requests, sender, response.outcome)
		// The writer was the only user of the request compressor.
		streams.requestCompressor().close()
		writerDone <- err
	}()
	// The response is read while the upload runs, so that a terminal status
	// reaches a caller that is only sending.
	go response.pump()
	return response, nil
}

// newResponseFrameReader returns the reader of the response frames of a call,
// which accepts the dictionaries of accept on compressed messages and
// decompresses stream-compressed ones with inflater, if the call enabled them.
func newResponseFrameReader(stream io.Reader, maxFrameSize int, accept *dictSet, inflater *streamInflater) *frameReader {
	reader := newFrameReader(stream, maxFrameSize, -1)
	reader.readAhead = true
	reader.inflate = accept.lookupOrNil()
	reader.stream = inflater
	return reader
}

// writeStreamingRequest uploads the request and then the messages of requests.
// It closes requests when it returns, which fails the Send calls blocked on
// them. When the stream itself refuses the upload, because the server stopped
// reading, it first waits for outcome: the server's terminal status explains
// the failure, and Send reports it instead of a closed pipe.
func (c *streamClient) writeStreamingRequest(
	ctx context.Context,
	stream requestSink,
	header []byte,
	requests encodedStream,
	sender *datagramSender,
	outcome <-chan struct{},
) error {
	requests = closeStreamOnContext(ctx, requests)
	defer discardMessageStream(requests)

	writer := &trackedWriter{writer: stream}
	if err := writeRequestFrames(ctx, writer, header, requests, sender, c.maxFrameSize); err != nil {
		stream.cancelWrite()
		if writer.failed {
			select {
			case <-outcome:
			case <-ctx.Done():
			}
		}
		return callStatus(ctx, err)
	}
	if err := stream.Close(); err != nil {
		return callStatus(ctx, err)
	}
	return nil
}

// trackedWriter records whether a write to the stream failed, as opposed to
// the source of the data failing.
type trackedWriter struct {
	writer io.Writer
	failed bool
}

func (w *trackedWriter) Write(data []byte) (int, error) {
	n, err := w.writer.Write(data)
	if err != nil {
		w.failed = true
	}
	return n, err
}

func (w *trackedWriter) writeMessageFrames(header []byte, messages []encodedMessage) error {
	err := writeMessageFrames(w.writer, header, messages)
	if err != nil {
		w.failed = true
	}
	return err
}

// callStatus maps a transport failure observed during a call to a status. A
// finished call context always wins so cancellations and deadlines are never
// reported as transport failures.
func callStatus(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return statusFromContextError(ctx.Err())
	}
	// A read deadline set from the call deadline can fire before the context
	// reports that the deadline passed.
	if deadline, ok := ctx.Deadline(); ok && isTimeoutError(err) && !time.Now().Before(deadline) {
		return statusFromContextError(context.DeadlineExceeded)
	}
	return transportStatus(err)
}

// uploadAborter is implemented by request streams that can tell a blocked or
// later Send why the upload ended: the server's terminal status.
type uploadAborter interface {
	// abortUpload makes Send fail with err. io.EOF reports that the server
	// finished the call successfully.
	abortUpload(err error)
}

// pumpedFrame is what the pump read from the response: a frame, a decoded
// STATUS, or the error that ended the stream.
type pumpedFrame struct {
	frame     frame
	status    *Status
	statusErr error
	err       error
}

// transportResponseStream reads the response frames of one call from its
// stream. A call that streams its request has a writer goroutine, whose
// result is reported together with the response, and a pump that reads the
// response ahead of Recv.
type transportResponseStream struct {
	ctx          context.Context
	stream       clientStream
	reader       *frameReader
	writerDone   <-chan error
	cancelUpload context.CancelFunc // set while the request is streamed
	cancel       context.CancelFunc
	stopCancel   func()
	maxFrameSize int
	done         bool
	// compression is the connection's compression state, and queried says
	// the request asked the server for its dictionaries, so that the HEADERS
	// of the response answer it.
	compression *clientCompression
	queried     bool
	// streams is the call's stream compression, nil without it. HEADERS
	// enable its request direction.
	streams *clientStreams
	// optimistic is the upload of a call that stream-compresses its requests
	// optimistically, nil otherwise; it may replace stream. pessimistic is
	// the connection's streamClient.pessimistic.
	optimistic  *optimisticUpload
	pessimistic *atomic.Bool

	// The fields below are set for calls that stream their request.
	//
	// frames carries the pump's frames to Recv. The pump closes it after
	// delivering the error that ended the stream. queued counts the payload
	// bytes in it, and drained is signalled whenever Recv takes a frame.
	frames  chan pumpedFrame
	queued  atomic.Int64
	drained chan struct{}
	// pumpStop ends the pump when the call is finished.
	pumpStop chan struct{}
	stopPump sync.Once
	// outcome is closed once the terminal status or the failure that ended
	// the response is known.
	outcome        chan struct{}
	outcomeReached sync.Once
	// abortUpload reports why the upload ended to Send.
	abortUpload func(error)

	// flow is the call's datagram flow, nil without one. A call with a flow
	// is pumped, and its sink, if any, holds the response messages that
	// arrived as datagrams. The remaining fields belong to the consumer of
	// nextFrame.
	flow *datagramFlow
	sink *datagramSink
	// gateOpen is set once the first frame of the response was delivered:
	// datagram messages follow HEADERS, never precede it.
	gateOpen bool
	// held is the terminal frame that waits for the datagram messages queued
	// before it.
	held *pumpedFrame
}

func (s *transportResponseStream) trevrpcContextCancelsRecv() bool { return true }

// startPump makes the stream a pumped one, for a call whose response is read
// ahead of Recv without a writer goroutine.
func (s *transportResponseStream) startPump() {
	s.sink = s.flow.sink
	s.frames = make(chan pumpedFrame, responseReadAheadFrames)
	s.drained = make(chan struct{}, 1)
	s.pumpStop = make(chan struct{})
	s.outcome = make(chan struct{})
	go s.pump()
}

// SetReadDeadline sets the stream's read deadline. A pumped stream is read by
// the pump alone, and the call context bounds it through cancellation.
func (s *transportResponseStream) SetReadDeadline(deadline time.Time) error {
	if s.frames != nil {
		return nil
	}
	return s.stream.SetReadDeadline(deadline)
}

const (
	// responseReadAheadBytes and responseReadAheadFrames bound how much of a
	// response the pump reads ahead of Recv. Beyond that it stops reading, so
	// the transport's flow control applies to a caller that does not receive.
	responseReadAheadBytes  = 1 << 20
	responseReadAheadFrames = 32
)

// pump reads the response while the caller is busy uploading. As soon as the
// terminal STATUS arrives it stops the upload and tells Send why, even though
// Recv has not asked for the frame yet. Frames wait in a bounded queue.
func (s *transportResponseStream) pump() {
	defer close(s.frames)
	for {
		for s.queued.Load() >= responseReadAheadBytes {
			select {
			case <-s.drained:
			case <-s.pumpStop:
				return
			case <-s.ctx.Done():
				return
			}
		}
		next, err := s.reader.next()
		if upload := s.optimistic; upload != nil && upload.undecided() {
			if err == nil && isRefusal(next) {
				// The server ran nothing for the call: it is replayed, and
				// the refusal is not delivered.
				if err = s.replayRefused(upload); err == nil {
					continue
				}
				next = frame{}
			} else {
				upload.decide(false)
				upload.settle()
			}
		}
		item := pumpedFrame{frame: next, err: err}
		switch {
		case err == nil && next.typ == frameTypeHeaders && s.streams.requestCompressor() != nil:
			// The answer enables compressed requests, so it is read here, as
			// soon as it arrives, not when Recv gets to it.
			if _, answer, err := decodeHeaders(next.payload); err == nil {
				s.streams.learn(answer.stream)
				if answer.stream != nil {
					// The server decodes again: later calls are optimistic.
					s.pessimistic.Store(false)
				}
			}
		case err != nil:
			// The response direction ended: late datagrams are dropped.
			s.flow.endReceive()
			if !errors.Is(err, io.EOF) {
				s.endUpload(callStatus(s.ctx, err))
			} else {
				s.endUpload(nil)
			}
		case next.typ == frameTypeStatus:
			s.flow.endReceive()
			item.status, item.statusErr = decodeStatus(next.payload)
			if item.statusErr == nil {
				s.endUpload(uploadEndError(item.status))
			}
		}
		s.queued.Add(int64(len(next.payload)))
		select {
		case s.frames <- item:
		case <-s.pumpStop:
			return
		case <-s.ctx.Done():
			return
		}
		if err != nil {
			return
		}
	}
}

// replayRefused replays an optimistic call the server refused, and reads the
// response of the new call from then on. Calls on the connection stop being
// optimistic until a server answer.
func (s *transportResponseStream) replayRefused(upload *optimisticUpload) error {
	upload.decide(true)
	s.pessimistic.Store(true)
	stream, err := upload.replay()
	if err != nil {
		return err
	}
	s.reader = newResponseFrameReader(stream, s.maxFrameSize, upload.accept, s.streams.responseInflater())
	return nil
}

// currentStream returns the stream the call uses, which a replay may have
// replaced.
func (s *transportResponseStream) currentStream() clientStream {
	if s.optimistic != nil {
		return s.optimistic.stream()
	}
	return s.stream
}

// uploadEndError is the error Send reports once the server ended the call
// with status.
func uploadEndError(status *Status) error {
	if status.IsOK() {
		return io.EOF
	}
	return status
}

// endUpload stops uploading because the response has ended. err is why Send
// should fail, or nil if the stream gave no reason.
func (s *transportResponseStream) endUpload(err error) {
	if err != nil && s.abortUpload != nil {
		s.abortUpload(err)
	}
	s.stopUpload()
	s.outcomeReached.Do(func() { close(s.outcome) })
}

// nextFrame returns the next known frame of the response.
func (s *transportResponseStream) nextFrame() (pumpedFrame, error) {
	if s.frames == nil {
		next, err := s.reader.next()
		item := pumpedFrame{frame: next, err: err}
		if err == nil && next.typ == frameTypeStatus {
			item.status, item.statusErr = decodeStatus(next.payload)
		}
		return item, nil
	}
	if s.sink != nil {
		return s.nextMergedFrame()
	}
	select {
	case item, ok := <-s.frames:
		if !ok {
			// The pump stopped without a final frame because the call ended.
			if err := s.ctx.Err(); err != nil {
				return pumpedFrame{}, statusFromContextError(err)
			}
			return pumpedFrame{err: io.EOF}, nil
		}
		s.takeFrame(item)
		return item, nil
	case <-s.ctx.Done():
		return pumpedFrame{}, statusFromContextError(s.ctx.Err())
	}
}

// takeFrame accounts for a frame the consumer took from the pump.
func (s *transportResponseStream) takeFrame(item pumpedFrame) {
	s.queued.Add(-int64(len(item.frame.payload)))
	select {
	case s.drained <- struct{}{}:
	default:
	}
}

// nextMergedFrame returns the next frame of a call that also receives its
// response messages as datagrams: the pumped stream frames and the datagram
// messages in the order they arrive. The messages wait until the first frame,
// HEADERS if there is any, was delivered, and the messages queued when the
// terminal frame arrives go before it. Messages that arrive after the stream
// ended were dropped by the pump.
func (s *transportResponseStream) nextMergedFrame() (pumpedFrame, error) {
	for {
		if s.held != nil {
			if message, ok := s.sink.pop(); ok {
				return datagramMessageFrame(message), nil
			}
			item := *s.held
			s.held = nil
			return item, nil
		}
		var wake <-chan struct{}
		if s.gateOpen {
			wake = s.sink.wake
		}
		select {
		case item, ok := <-s.frames:
			if !ok {
				// The pump stopped without a final frame because the call ended.
				if err := s.ctx.Err(); err != nil {
					return pumpedFrame{}, statusFromContextError(err)
				}
				return pumpedFrame{err: io.EOF}, nil
			}
			s.takeFrame(item)
			s.gateOpen = true
			if item.err == nil && item.frame.typ != frameTypeStatus {
				return item, nil
			}
			if item.err != nil && !errors.Is(item.err, io.EOF) {
				// The stream failed: what is queued is not delivered.
				return item, nil
			}
			s.held = &item
		case <-wake:
			if message, ok := s.sink.pop(); ok {
				return datagramMessageFrame(message), nil
			}
		case <-s.ctx.Done():
			return pumpedFrame{}, statusFromContextError(s.ctx.Err())
		}
	}
}

func datagramMessageFrame(message []byte) pumpedFrame {
	return pumpedFrame{frame: frame{typ: frameTypeMessage, payload: message}}
}

// Recv returns the next known response frame. After the STATUS frame it keeps
// reading, skipping unknown frames, so the end of the stream can be told from
// more data: the next call returns io.EOF when the stream ended cleanly.
func (s *transportResponseStream) Recv() (responseFrame, error) {
	if s.done {
		return responseFrame{}, io.EOF
	}

	item, err := s.nextFrame()
	if err != nil {
		return responseFrame{}, err
	}
	next := item.frame
	if item.err != nil {
		statusErr := io.EOF
		if !errors.Is(item.err, io.EOF) {
			statusErr = callStatus(s.ctx, item.err)
		}
		s.finish(false)
		if writerErr := s.writerError(false); writerErr != nil {
			return responseFrame{}, writerErr
		}
		return responseFrame{}, statusErr
	}

	switch next.typ {
	case frameTypeMessage:
		return responseFrame{kind: frameTypeMessage, body: next.payload}, nil
	case frameTypeHeaders:
		headers, answer, err := decodeHeaders(next.payload)
		if err != nil {
			return responseFrame{}, err
		}
		if s.queried {
			s.queried = false
			s.compression.learn(answer.dictionaries)
		}
		return responseFrame{kind: frameTypeHeaders, headers: headers}, nil
	case frameTypeStatus:
		if item.statusErr != nil {
			return responseFrame{}, item.statusErr
		}
		status := item.status
		// The call is over: stop uploading and settle the writer. The context
		// watcher keeps guarding the wait for the end of the stream.
		s.stopUpload()
		if status.IsOK() {
			if err := s.writerError(true); err != nil {
				return responseFrame{}, err
			}
		} else {
			s.ignoreWriterError()
		}
		return responseFrame{kind: frameTypeStatus, status: status}, nil
	default:
		return responseFrame{kind: next.typ}, nil
	}
}

// stopUpload cancels the request side of a streamed request. The response
// side stays readable.
func (s *transportResponseStream) stopUpload() {
	if s.cancelUpload != nil {
		s.cancelUpload()
		s.currentStream().cancelWrite()
	}
}

func (s *transportResponseStream) Close() error {
	s.finish(true)
	return s.writerError(true)
}

func (s *transportResponseStream) finish(cancelRead bool) {
	if s.done {
		return
	}
	s.done = true
	if s.pumpStop != nil {
		s.stopPump.Do(func() { close(s.pumpStop) })
	}
	if s.stopCancel != nil {
		s.stopCancel()
	}
	s.stopUpload()
	if s.cancel != nil {
		s.cancel()
	}
	s.flow.release()
	if s.optimistic != nil {
		s.optimistic.end()
	}
	s.streams.close()
	if cancelRead {
		s.currentStream().cancelRead()
	}
}

func (s *transportResponseStream) writerError(ignoreCancelled bool) error {
	if s.writerDone == nil {
		return nil
	}
	err := <-s.writerDone
	s.writerDone = nil
	if err == nil {
		return nil
	}
	if ignoreCancelled && StatusFromError(err).Code == CodeCancelled {
		return nil
	}
	return err
}

func (s *transportResponseStream) ignoreWriterError() {
	s.writerDone = nil
}

func cancelStreamOnContext(ctx context.Context, stream clientStream) func() {
	return watchContextCancellation(ctx, func() {
		stream.cancelRead()
		stream.cancelWrite()
	})
}

// watchContextCancellation calls cancel, on a goroutine of its own, once ctx
// is done. The returned function stops the watch. Waiting costs no goroutine.
func watchContextCancellation(ctx context.Context, cancel func()) func() {
	if ctx.Done() == nil {
		return func() {}
	}

	stop := context.AfterFunc(ctx, cancel)
	return func() { stop() }
}
