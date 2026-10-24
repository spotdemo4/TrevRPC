package trevrpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/quicvarint"
)

// fakeDatagramConn is a datagramConn that tests drive by hand.
type fakeDatagramConn struct {
	incoming  chan []byte
	closed    chan struct{}
	closeOnce sync.Once

	mu        sync.Mutex
	sent      [][]byte
	sendCalls int
	// sendFunc, if set, decides the result of every send before it is
	// recorded. It may block.
	sendFunc func(payload []byte) error
}

func newFakeDatagramConn() *fakeDatagramConn {
	return &fakeDatagramConn{incoming: make(chan []byte, 1024), closed: make(chan struct{})}
}

func (c *fakeDatagramConn) SendDatagram(payload []byte) error {
	c.mu.Lock()
	c.sendCalls++
	sendFunc := c.sendFunc
	c.mu.Unlock()
	if sendFunc != nil {
		if err := sendFunc(payload); err != nil {
			return err
		}
	}
	c.mu.Lock()
	c.sent = append(c.sent, bytes.Clone(payload))
	c.mu.Unlock()
	return nil
}

func (c *fakeDatagramConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	// Like quic-go, a datagram that is queued is returned before the context
	// is looked at.
	select {
	case datagram := <-c.incoming:
		return datagram, nil
	default:
	}
	select {
	case datagram := <-c.incoming:
		return datagram, nil
	case <-c.closed:
		return nil, errFakeConnClosed
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

var errFakeConnClosed = errors.New("fake datagram connection closed")

func (c *fakeDatagramConn) close() { c.closeOnce.Do(func() { close(c.closed) }) }

// deliver makes the datagram flow||message arrive.
func (c *fakeDatagramConn) deliver(flow uint64, message string) {
	c.incoming <- appendDatagram(nil, flow, [][]byte{[]byte(message)})
}

func (c *fakeDatagramConn) sentDatagrams() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.sent...)
}

func (c *fakeDatagramConn) calls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sendCalls
}

// eventually polls cond until it holds.
func eventually(t *testing.T, description string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(testTimeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

// takeMessage takes the next message of sink, waiting for it.
func takeMessage(t *testing.T, sink *datagramSink) string {
	t.Helper()
	select {
	case <-sink.wake:
	case <-time.After(testTimeout):
		t.Fatal("no datagram message arrived")
	}
	message, ok := sink.pop()
	if !ok {
		t.Fatal("woken without a message")
	}
	return string(message)
}

// ended reports whether the direction of the sink ended.
func (s *datagramSink) ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func newServerMux(conn datagramConn) *datagramMux { return newDatagramMux(conn, 0, true, 8) }

func newClientMux(conn datagramConn) *datagramMux { return newDatagramMux(conn, 0, false, 0) }

func (m *datagramMux) heldCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.held)
}

func (m *datagramMux) liveFlows() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.flows)
}

func TestDatagramSinkQueuesInOrderAndDropsWhenFullOrClosed(t *testing.T) {
	sink := newDatagramSink(8)
	for i := range datagramSinkMessages {
		if !sink.push([]byte{byte(i)}) {
			t.Fatalf("push %d refused before the queue was full", i)
		}
	}
	if sink.push([]byte{0xff}) {
		t.Fatal("push into a full sink succeeded; it must drop instead of blocking")
	}
	for i := range datagramSinkMessages {
		message, ok := sink.pop()
		if !ok || !bytes.Equal(message, []byte{byte(i)}) {
			t.Fatalf("pop %d = %v, %v", i, message, ok)
		}
	}
	if _, ok := sink.pop(); ok {
		t.Fatal("pop from an empty sink succeeded")
	}
	if sink.push(make([]byte, 9)) {
		t.Fatal("a message larger than the frame size limit was queued")
	}

	// Closing ends the direction: what is queued stays, nothing more is queued.
	sink.push([]byte("a"))
	sink.close()
	if sink.push([]byte("b")) {
		t.Fatal("push after close succeeded")
	}
	if message, ok := sink.pop(); !ok || string(message) != "a" {
		t.Fatalf("pop after close = %q, %v; want the message queued before it", message, ok)
	}
	sink.push([]byte("c"))
	sink.shutdown()
	if _, ok := sink.pop(); ok {
		t.Fatal("shutdown left a message queued")
	}
}

func TestDatagramMuxDeliversToTheFlowsSink(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newClientMux(conn)
	defer mux.close()
	one := mux.allocate(true, 1024, nil)
	two := mux.allocate(true, 1024, nil)
	noReceive := mux.allocate(false, 1024, nil)
	if one.id != 1 || two.id != 2 || noReceive.id != 3 {
		t.Fatalf("flow IDs = %d, %d, %d; want 1, 2, 3", one.id, two.id, noReceive.id)
	}

	conn.deliver(2, "to two")
	conn.deliver(1, "to one")
	conn.deliver(3, "dropped: the call does not receive a stream")
	conn.deliver(9, "dropped: unknown flow")
	conn.incoming <- []byte{0x40}      // truncated flow ID
	conn.incoming <- []byte{0x00, 'x'} // flow 0 belongs to no call
	conn.deliver(1, "again")

	if got := takeMessage(t, two.sink); got != "to two" {
		t.Fatalf("flow 2 got %q", got)
	}
	if got := takeMessage(t, one.sink); got != "to one" {
		t.Fatalf("flow 1 got %q", got)
	}
	if got := takeMessage(t, one.sink); got != "again" {
		t.Fatalf("flow 1 got %q", got)
	}
	if mux.heldCount() != 0 {
		t.Fatal("a client held a datagram for a flow it does not know")
	}
}

func TestDatagramMuxFullReceiveQueueDropsInsteadOfBlocking(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newClientMux(conn)
	defer mux.close()
	slow := mux.allocate(true, 1024, nil)
	fast := mux.allocate(true, 1024, nil)

	// Far more datagrams than the sink queues, with nobody receiving.
	for range 10 * datagramSinkMessages {
		conn.deliver(slow.id, "x")
	}
	conn.deliver(fast.id, "still read")
	if got := takeMessage(t, fast.sink); got != "still read" {
		t.Fatalf("the reader was blocked by a full queue: got %q", got)
	}
	slow.sink.mu.Lock()
	queued := slow.sink.count
	slow.sink.mu.Unlock()
	if queued != datagramSinkMessages {
		t.Fatalf("slow call queued %d messages, want %d", queued, datagramSinkMessages)
	}
}

func TestDatagramMuxHoldsDatagramsThatOvertakeTheirRequest(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	// Start the reader without registering a flow of interest.
	other, err := mux.register(99, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer other.release()

	conn.deliver(5, "first")
	conn.deliver(6, "belongs to another call")
	conn.deliver(5, "second")
	eventually(t, "the datagrams to be held", func() bool { return mux.heldCount() == 3 })

	flow, err := mux.register(5, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := takeMessage(t, flow.sink); got != "first" {
		t.Fatalf("claimed %q first", got)
	}
	if got := takeMessage(t, flow.sink); got != "second" {
		t.Fatalf("claimed %q second", got)
	}
	if mux.heldCount() != 1 {
		t.Fatalf("held = %d after the claim, want only the other flow's datagram", mux.heldCount())
	}
	// From now on datagrams of the flow go straight to its sink.
	conn.deliver(5, "third")
	if got := takeMessage(t, flow.sink); got != "third" {
		t.Fatalf("got %q", got)
	}
}

func TestDatagramMuxHeldDatagramsExpire(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	mux.holdTime = 30 * time.Millisecond
	anchor, err := mux.register(99, false, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.release()

	conn.deliver(5, "too late")
	eventually(t, "the datagram to be held", func() bool { return mux.heldCount() == 1 })
	eventually(t, "the datagram to expire", func() bool { return mux.heldCount() == 0 })

	flow, err := mux.register(5, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if message, ok := flow.sink.pop(); ok {
		t.Fatalf("an expired datagram %q was claimed", message)
	}
	mux.mu.Lock()
	timer := mux.holdTimer
	mux.mu.Unlock()
	if timer != nil {
		t.Fatal("the hold timer outlived the held datagrams")
	}
}

func TestDatagramMuxHoldingAreaIsBounded(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	anchor, err := mux.register(1, false, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.release()

	for range datagramHoldMessages + 10 {
		conn.deliver(7, "x")
	}
	eventually(t, "the holding area to fill", func() bool { return mux.heldCount() == datagramHoldMessages })
	conn.deliver(8, "one more")
	conn.deliver(1, "") // flushes the reader past the datagram above
	time.Sleep(20 * time.Millisecond)
	if got := mux.heldCount(); got != datagramHoldMessages {
		t.Fatalf("held = %d, want at most %d", got, datagramHoldMessages)
	}

	// The byte bound holds as well.
	bytesMux := newServerMux(newFakeDatagramConn())
	defer bytesMux.close()
	bytesMux.holdBytes = 100
	for _, size := range []int{60, 60, 30} {
		bytesMux.dispatch(appendDatagram(nil, 3, [][]byte{make([]byte, size-3)}))
	}
	if got := bytesMux.heldCount(); got != 2 {
		t.Fatalf("held = %d datagrams under a 100 byte bound, want the 60 and the 30 byte ones", got)
	}
}

func TestDatagramMuxDropsLateDatagramsOfEndedFlowsAtOnce(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	flow, err := mux.register(4, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	flow.release()

	// The flow is over: its late datagrams are not held, so they cannot use up
	// the holding area, however many arrive.
	for range 3 * datagramHoldMessages {
		mux.dispatch(appendDatagram(nil, 4, [][]byte{[]byte("late")}))
	}
	if got := mux.heldCount(); got != 0 {
		t.Fatalf("held %d late datagrams of an ended flow", got)
	}
	mux.dispatch(appendDatagram(nil, 5, [][]byte{[]byte("early")}))
	if got := mux.heldCount(); got != 1 {
		t.Fatalf("held = %d, want the datagram of an unseen flow", got)
	}

	// The set of ended flows is bounded: the oldest is forgotten.
	for id := uint64(10); id < 10+8; id++ {
		ended, err := mux.register(id, false, 1024, nil)
		if err != nil {
			t.Fatal(err)
		}
		ended.release()
	}
	mux.dispatch(appendDatagram(nil, 4, [][]byte{[]byte("forgotten")}))
	if got := mux.heldCount(); got != 2 {
		t.Fatalf("held = %d, want the forgotten flow's datagram held as an unseen one", got)
	}
}

func TestDatagramMuxRejectsALiveFlowAndAcceptsItAfterTheCallEnds(t *testing.T) {
	mux := newServerMux(newFakeDatagramConn())
	defer mux.close()
	first, err := mux.register(3, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mux.register(3, true, 1024, nil); StatusFromError(err).Code != CodeInvalidArgument {
		t.Fatalf("registering a live flow = %v, want InvalidArgument", err)
	}
	if mux.liveFlows() != 1 {
		t.Fatal("the failed registration disturbed the live flow")
	}
	first.release()
	first.release()
	if mux.liveFlows() != 0 {
		t.Fatal("release did not unregister the flow")
	}
	var nilFlow *datagramFlow
	nilFlow.release()
}

func TestDatagramMuxDropsDatagramsOfDirectionsThatDoNotStream(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	flow, err := mux.register(2, false, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer flow.release()
	conn.deliver(2, "no sink")
	sentinel, err := mux.register(3, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	conn.deliver(3, "sentinel")
	takeMessage(t, sentinel.sink)
	if mux.heldCount() != 0 {
		t.Fatal("a live flow's datagram was held")
	}
}

// trySend sends one message of n zero bytes with sendBatch and reports whether
// it travelled as a datagram.
func trySend(ctx context.Context, flow *datagramFlow, n int) (bool, error) {
	kept, err := flow.sendBatch(ctx, []encodedMessage{rawMessage(make([]byte, n))})
	return err == nil && len(kept) == 0, err
}

func TestDatagramSendFallsBackToTheStreamWhenTooLarge(t *testing.T) {
	conn := newFakeDatagramConn()
	// The path allows 100 bytes, which a WebTransport session spends partly on
	// its quarter stream ID.
	conn.sendFunc = func(payload []byte) error {
		if len(payload)+3 > 100 {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 100}
		}
		return nil
	}
	mux := newDatagramMux(conn, 3, false, 0)
	defer mux.close()
	flow := mux.allocate(false, 1024, nil)
	ctx := context.Background()

	// The first large message learns the limit from the send error.
	if sent, err := trySend(ctx, flow, 2000); sent || err != nil {
		t.Fatalf("send of 2000 bytes = %v, %v; want it left to the stream", sent, err)
	}
	calls := conn.calls()
	// 97 bytes are left once the 3 byte prefix is counted. The flow ID takes
	// one and the frame header three (the length needs two bytes from 64): a 93
	// byte message fits and a 94 byte one does not.
	if sent, err := trySend(ctx, flow, 93); !sent || err != nil {
		t.Fatalf("send of 93 bytes = %v, %v; want a datagram", sent, err)
	}
	if sent, err := trySend(ctx, flow, 94); sent || err != nil {
		t.Fatalf("send of 94 bytes = %v, %v; want it left to the stream", sent, err)
	}
	if got := conn.calls() - calls; got != 1 {
		t.Fatalf("the connection was asked %d times, want 1: a message known not to fit is not sent", got)
	}
	datagrams := conn.sentDatagrams()
	if len(datagrams) != 1 || datagrams[0][0] != 1 || len(datagrams[0]) != 97 {
		t.Fatalf("sent datagrams = %x, want one of flow 1 and 97 bytes", datagrams)
	}

	// The limit is trusted for a while only, because the path MTU grows.
	mux.sizeLimitAt.Store(time.Now().Add(-2 * datagramSizeProbe).UnixNano())
	conn.sendFunc = nil
	if sent, err := trySend(ctx, flow, 200); !sent || err != nil {
		t.Fatalf("send of 200 bytes after the limit expired = %v, %v; want a datagram", sent, err)
	}
}

func TestDatagramSendPacksReadyMessagesIntoAsFewDatagramsAsFit(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(false, 1024, nil)
	// The first send teaches the mux that datagrams hold 100 bytes.
	conn.sendFunc = func(payload []byte) error {
		if len(payload) > 100 {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 100}
		}
		return nil
	}

	// Ten messages of 20 bytes are frames of 22: four fit with the flow ID.
	var batch []encodedMessage
	var want []string
	for i := range 10 {
		message := bytes.Repeat([]byte{byte('a' + i)}, 20)
		batch = append(batch, rawMessage(message))
		want = append(want, string(message))
	}
	// A message that cannot travel in any datagram stays on the stream, and
	// the ones around it still pack.
	huge := rawMessage(make([]byte, 500))
	batch = append(batch[:5], append([]encodedMessage{huge}, batch[5:]...)...)

	kept, err := flow.sendBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	if len(kept) != 1 || kept[0].size != 500 {
		t.Fatalf("kept %d messages for the stream, want only the 500 byte one", len(kept))
	}
	var got []string
	var sizes []int
	for _, datagram := range conn.sentDatagrams() {
		flowID, frames, ok := parseDatagram(datagram)
		messages, valid := datagramMessages(frames)
		if !ok || !valid || flowID != flow.id || len(datagram) > 100 {
			t.Fatalf("datagram %x is not a valid datagram of the flow within the limit", datagram)
		}
		sizes = append(sizes, len(messages))
		for _, message := range messages {
			got = append(got, string(message))
		}
	}
	if !slices.Equal(got, want) {
		t.Fatalf("datagrams carried %q, want the ten messages in order", got)
	}
	// The first attempt (everything up to 1200 bytes) was refused, and the
	// retry packed under the learned limit: 4, 1 (up to the huge one), 4, 1.
	if !slices.Equal(sizes, []int{4, 1, 4, 1}) {
		t.Fatalf("messages per datagram = %v, want [4 1 4 1]", sizes)
	}
}

func TestDatagramSendFailureLeavesTheWholeDatagramForTheStream(t *testing.T) {
	conn := newFakeDatagramConn()
	var calls int
	conn.sendFunc = func([]byte) error {
		calls++
		if calls == 2 {
			return errors.New("datagram support disabled")
		}
		return nil
	}
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(false, 1024, nil)
	// 30 messages of 100 bytes need three datagrams of 11, 11 and 8; the second
	// fails.
	batch := make([]encodedMessage, 30)
	for i := range batch {
		batch[i] = rawMessage(bytes.Repeat([]byte{byte(i)}, 100))
	}
	kept, err := flow.sendBatch(context.Background(), batch)
	if err != nil {
		t.Fatal(err)
	}
	datagrams := conn.sentDatagrams()
	if len(datagrams) != 2 || calls != 3 {
		t.Fatalf("sent %d datagrams in %d attempts, want 2 in 3", len(datagrams), calls)
	}
	if len(kept) != 11 || kept[0].frame[len(kept[0].frame)-1] != 11 {
		t.Fatalf("kept %d messages; the failed datagram's messages must go to the stream", len(kept))
	}
	// Every message is either in a datagram or kept, once.
	sent := 0
	for _, datagram := range datagrams {
		_, frames, _ := parseDatagram(datagram)
		messages, _ := datagramMessages(frames)
		sent += len(messages)
	}
	if sent+len(kept) != 30 {
		t.Fatalf("%d in datagrams and %d kept, want 30 in all", sent, len(kept))
	}
}

func TestDatagramSendErrorsFallBackToTheStream(t *testing.T) {
	conn := newFakeDatagramConn()
	conn.sendFunc = func([]byte) error { return errors.New("datagram support disabled") }
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(false, 1024, nil)
	if sent, err := trySend(context.Background(), flow, 1); sent || err != nil {
		t.Fatalf("send after a send error = %v, %v; want it left to the stream", sent, err)
	}
}

func TestDatagramSendWaitHonoursCancellationAndBlocksNoOtherCall(t *testing.T) {
	conn := newFakeDatagramConn()
	release := make(chan struct{})
	var blocked atomic.Int32
	// The connection's queue is full: SendDatagram blocks without a context.
	conn.sendFunc = func([]byte) error {
		blocked.Add(1)
		<-release
		return nil
	}
	mux := newClientMux(conn)
	defer mux.close()
	defer close(release)
	stuck := mux.allocate(false, 1024, nil)
	waiting := mux.allocate(false, 1024, nil)

	// The first send occupies the sender goroutine.
	stuckCtx, cancelStuck := context.WithCancel(context.Background())
	stuckResult := make(chan error, 1)
	go func() {
		_, err := trySend(stuckCtx, stuck, 1)
		stuckResult <- err
	}()
	eventually(t, "the send to block in the connection", func() bool { return blocked.Load() == 1 })

	// A second call waits behind it and is cancelled at once.
	waitingCtx, cancelWaiting := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelWaiting()
	began := time.Now()
	sent, err := trySend(waitingCtx, waiting, 1)
	if sent || StatusFromError(err).Code != CodeDeadlineExceeded {
		t.Fatalf("send behind a full queue = %v, %v; want DeadlineExceeded", sent, err)
	}
	if elapsed := time.Since(began); elapsed > 5*time.Second {
		t.Fatalf("the cancelled send took %v", elapsed)
	}

	// So is the call whose send is the one that blocks.
	cancelStuck()
	select {
	case err := <-stuckResult:
		if StatusFromError(err).Code != CodeCancelled {
			t.Fatalf("cancelled send = %v, want Cancelled", err)
		}
	case <-time.After(testTimeout):
		t.Fatal("a cancelled call stayed blocked on the datagram queue")
	}
}

func TestDatagramSenderStopsWhenTheConnectionCloses(t *testing.T) {
	conn := newFakeDatagramConn()
	conn.sendFunc = func([]byte) error { return nil }
	mux := newClientMux(conn)
	flow := mux.allocate(false, 1024, nil)
	if sent, err := trySend(context.Background(), flow, 1); !sent || err != nil {
		t.Fatalf("send = %v, %v", sent, err)
	}
	conn.close() // ends the reader, which closes the mux
	eventually(t, "the mux to close", func() bool { return mux.ctx.Err() != nil })
	if sent, err := trySend(context.Background(), flow, 1); sent || err != nil {
		t.Fatalf("send on a closed mux = %v, %v; want it left to the stream", sent, err)
	}
	if mux.allocate(true, 1024, nil) != nil {
		t.Fatal("a closed mux allocated a flow")
	}
}

// orderRecorder logs the writes of a request and the datagrams sent for it in
// the order they happened.
type orderRecorder struct {
	mu     sync.Mutex
	events []string
}

func (r *orderRecorder) record(event string) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func (r *orderRecorder) Write(data []byte) (int, error) {
	r.record("write " + string(bytes.Trim(data, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a\x0b\x0c")))
	return len(data), nil
}

func TestRequestDatagramsFollowTheFlushedRequestAndOversizedMessagesStayOnTheStream(t *testing.T) {
	recorder := &orderRecorder{}
	conn := newFakeDatagramConn()
	conn.sendFunc = func(payload []byte) error {
		if len(payload) > 20 {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 20}
		}
		_, frames, _ := parseDatagram(payload)
		messages, _ := datagramMessages(frames)
		recorder.record("datagram " + string(bytes.Join(messages, []byte("+"))))
		return nil
	}
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(false, 1024, nil)

	small, large := rawMessage([]byte("small")), rawMessage(bytes.Repeat([]byte("L"), 64))
	header := []byte("REQUEST")
	requests := newSliceStream(small, large, small)
	err := writeRequestFrames(context.Background(), recorder, header, requests, &datagramSender{flow: flow, ctx: context.Background()}, DefaultMaxFrameSize)
	if err != nil {
		t.Fatal(err)
	}
	recorder.mu.Lock()
	events := append([]string(nil), recorder.events...)
	recorder.mu.Unlock()
	if len(events) < 4 || events[0] != "write REQUEST" {
		t.Fatalf("events = %q; the REQUEST must be written before any datagram", events)
	}
	var datagrams, writes int
	for _, event := range events[1:] {
		switch {
		case event == "datagram small":
			datagrams++
		case len(event) > 5 && event[:5] == "write":
			writes++
		}
	}
	if datagrams != 2 || writes != 1 {
		t.Fatalf("events = %q; want two small messages as datagrams and the large one in one write", events)
	}
}

func TestRequestFramesStayOnTheStreamWithoutDatagrams(t *testing.T) {
	recorder := &orderRecorder{}
	requests := newSliceStream(rawMessage([]byte("a")), rawMessage([]byte("b")))
	if err := writeRequestFrames(context.Background(), recorder, []byte("REQUEST"), requests, nil, DefaultMaxFrameSize); err != nil {
		t.Fatal(err)
	}
	// The header and the messages of a stream that never blocks share a write.
	if len(recorder.events) != 1 {
		t.Fatalf("events = %q, want one batched write", recorder.events)
	}
}

func TestDatagramsInTheClientsResponseWaitForHeadersAndPrecedeTheStatus(t *testing.T) {
	newStream := func() *transportResponseStream {
		sink := newDatagramSink(1024)
		return &transportResponseStream{
			ctx:     context.Background(),
			frames:  make(chan pumpedFrame, 8),
			drained: make(chan struct{}, 1),
			sink:    sink,
		}
	}
	next := func(s *transportResponseStream) responseFrame {
		t.Helper()
		item, err := s.nextFrame()
		if err != nil || item.err != nil {
			t.Fatalf("nextFrame = %v, %v", item.err, err)
		}
		if item.frame.typ == frameTypeStatus {
			return responseFrame{kind: frameTypeStatus}
		}
		if item.frame.typ == frameTypeHeaders {
			return responseFrame{kind: frameTypeHeaders}
		}
		return responseFrame{kind: frameTypeMessage, body: item.frame.payload}
	}

	t.Run("messages follow HEADERS", func(t *testing.T) {
		s := newStream()
		s.sink.push([]byte("early"))
		s.frames <- pumpedFrame{frame: frame{typ: frameTypeHeaders}}
		if got := next(s); got.kind != frameTypeHeaders {
			t.Fatalf("first frame = %v, want HEADERS before the datagram message that arrived earlier", got.kind)
		}
		if got := next(s); got.kind != frameTypeMessage || string(got.body) != "early" {
			t.Fatalf("second frame = %v %q", got.kind, got.body)
		}
	})

	t.Run("queued messages precede the status", func(t *testing.T) {
		s := newStream()
		s.frames <- pumpedFrame{frame: frame{typ: frameTypeHeaders}}
		next(s)
		s.sink.push([]byte("one"))
		s.sink.push([]byte("two"))
		// The pump closes the sink when it reads the terminal frame.
		s.sink.close()
		s.frames <- pumpedFrame{frame: frame{typ: frameTypeStatus}}
		for _, want := range []string{"one", "two"} {
			if got := next(s); got.kind != frameTypeMessage || string(got.body) != want {
				t.Fatalf("got %v %q, want message %q", got.kind, got.body, want)
			}
		}
		if got := next(s); got.kind != frameTypeStatus {
			t.Fatalf("got %v, want the STATUS after the queued messages", got.kind)
		}
		if s.sink.push([]byte("late")) {
			t.Fatal("a message was queued after the status")
		}
	})

	t.Run("a failed stream ends the call", func(t *testing.T) {
		s := newStream()
		s.frames <- pumpedFrame{frame: frame{typ: frameTypeHeaders}}
		next(s)
		s.sink.push([]byte("queued"))
		s.sink.close()
		s.frames <- pumpedFrame{err: errors.New("reset")}
		// The queued message may arrive before the failure, as arrival order
		// has it, but the failure is reported and nothing follows it.
		for range 2 {
			item, err := s.nextFrame()
			if err != nil {
				t.Fatal(err)
			}
			if item.err != nil {
				if item.err.Error() != "reset" {
					t.Fatalf("failure = %v", item.err)
				}
				return
			}
		}
		t.Fatal("the failure was not reported")
	})

	t.Run("a message wakes a pending receive", func(t *testing.T) {
		s := newStream()
		s.frames <- pumpedFrame{frame: frame{typ: frameTypeHeaders}}
		next(s)
		go func() {
			time.Sleep(10 * time.Millisecond)
			s.sink.push([]byte("woken"))
		}()
		if got := next(s); got.kind != frameTypeMessage || string(got.body) != "woken" {
			t.Fatalf("got %v %q", got.kind, got.body)
		}
	})
}

// blockingReader blocks until released and then returns its error.
type blockingReader struct {
	release chan struct{}
	err     error
}

func (r *blockingReader) Read([]byte) (int, error) {
	<-r.release
	return 0, r.err
}

func newMergedRequests(t *testing.T, reader io.Reader) (*rpcRequestStream, *datagramFlow, *datagramMux) {
	t.Helper()
	mux := newServerMux(newFakeDatagramConn())
	t.Cleanup(mux.close)
	flow, err := mux.register(1, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(flow.release)
	requests := &rpcRequestStream{reader: newFrameReader(reader, DefaultMaxFrameSize, -1), merge: newRequestMerge(flow)}
	t.Cleanup(func() { _ = requests.Close() })
	return requests, flow, mux
}

func TestServerMergesRequestDatagramsWithTheStreamAndEndsAfterQueuedOnes(t *testing.T) {
	reader := &blockingReader{release: make(chan struct{}), err: io.EOF}
	requests, flow, _ := newMergedRequests(t, reader)

	flow.sink.push([]byte("a"))
	flow.sink.push([]byte("b"))
	for _, want := range []string{"a", "b"} {
		got, err := requests.Recv()
		if err != nil || string(got) != want {
			t.Fatalf("Recv = %q, %v; want %q", got, err, want)
		}
	}
	if got := flow.received.Load(); got != 2 {
		t.Fatalf("received = %d, want 2", got)
	}

	// A message is queued just before the request ends: it is delivered, and
	// then the stream ends. Datagrams after the end are dropped.
	flow.sink.push([]byte("c"))
	close(reader.release)
	eventually(t, "the request to end", flow.sink.ended)
	if flow.sink.push([]byte("late")) {
		t.Fatal("a datagram message was queued after the request ended")
	}
	var got []string
	for {
		message, err := requests.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(message))
	}
	if len(got) != 1 || got[0] != "c" {
		t.Fatalf("messages before the end = %q, want only c", got)
	}
	if !requests.finished() {
		t.Fatal("the request did not finish")
	}
}

func TestServerRequestFailureEndsTheCallAndDropsLaterDatagrams(t *testing.T) {
	reader := &blockingReader{release: make(chan struct{}), err: errors.New("stream reset")}
	requests, flow, _ := newMergedRequests(t, reader)
	close(reader.release)
	// The failure is reported, possibly after a message that was queued before.
	flow.sink.push([]byte("queued"))
	var err error
	for range 2 {
		if _, err = requests.Recv(); err != nil {
			break
		}
	}
	if StatusFromError(err).Code != CodeUnavailable {
		t.Fatalf("Recv = %v; the reset fails the call", err)
	}
	eventually(t, "the direction to end", flow.sink.ended)
	if flow.sink.push([]byte("late")) {
		t.Fatal("a datagram message was queued after the request failed")
	}
}

func TestServerCountsRequestDatagramsAgainstTheCallsLimitsAndIdleTimer(t *testing.T) {
	limits := streamLimits{maxMessages: 3, maxBodySize: -1}
	t.Run("message limit", func(t *testing.T) {
		requests, flow, _ := newMergedRequests(t, &blockingReader{release: make(chan struct{}), err: io.EOF})
		stream := limitByteStream(context.Background(), requests, limits, "request", nil, nil)
		defer discardMessageStream(stream)
		for range 3 {
			flow.sink.push([]byte("m"))
			if _, err := stream.Recv(); err != nil {
				t.Fatal(err)
			}
		}
		flow.sink.push([]byte("one too many"))
		if _, err := stream.Recv(); StatusFromError(err).Code != CodeResourceExhausted {
			t.Fatalf("Recv = %v, want ResourceExhausted", err)
		}
	})

	t.Run("byte limit", func(t *testing.T) {
		requests, flow, _ := newMergedRequests(t, &blockingReader{release: make(chan struct{}), err: io.EOF})
		stream := limitByteStream(context.Background(), requests, streamLimits{maxMessages: -1, maxBodySize: 10}, "request", nil, nil)
		defer discardMessageStream(stream)
		flow.sink.push(bytes.Repeat([]byte("x"), 6))
		if _, err := stream.Recv(); err != nil {
			t.Fatal(err)
		}
		flow.sink.push(bytes.Repeat([]byte("x"), 6))
		if _, err := stream.Recv(); StatusFromError(err).Code != CodeResourceExhausted {
			t.Fatalf("Recv = %v, want ResourceExhausted", err)
		}
	})

	t.Run("idle timer", func(t *testing.T) {
		requests, flow, _ := newMergedRequests(t, &blockingReader{release: make(chan struct{}), err: io.EOF})
		idle := 100 * time.Millisecond
		stream := limitByteStream(context.Background(), requests, streamLimits{maxMessages: -1, maxBodySize: -1, idleTimeout: idle}, "request", nil, nil)
		defer discardMessageStream(stream)

		// Datagram messages alone keep the call alive for longer than the idle
		// timeout...
		done := make(chan struct{})
		go func() {
			defer close(done)
			for range 8 {
				time.Sleep(idle / 2)
				flow.sink.push([]byte("tick"))
			}
		}()
		began := time.Now()
		for range 8 {
			if _, err := stream.Recv(); err != nil {
				t.Fatalf("Recv after %v: %v", time.Since(began), err)
			}
		}
		<-done
		if time.Since(began) < idle {
			t.Fatal("the test did not outlast the idle timeout")
		}
		// ...and silence ends it.
		if _, err := stream.Recv(); StatusFromError(err).Code != CodeUnavailable {
			t.Fatalf("Recv after silence = %v, want the idle timeout", err)
		}
	})
}

// polledConn hides its datagrams from the reader goroutine, which blocks, and
// hands them to whoever polls with a context that is already done.
type polledConn struct {
	fakeDatagramConn
	pending chan []byte
}

func (c *polledConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if ctx.Err() != nil {
		select {
		case datagram := <-c.pending:
			return datagram, nil
		default:
			return nil, context.Cause(ctx)
		}
	}
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case <-c.closed:
		return nil, errFakeConnClosed
	}
}

func TestEndingTheDirectionTakesTheDatagramsTheReaderHasNotDispatchedYet(t *testing.T) {
	conn := &polledConn{fakeDatagramConn: *newFakeDatagramConn(), pending: make(chan []byte, 8)}
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(true, 1024, nil)
	other := mux.allocate(true, 1024, nil)

	// The datagrams reached the connection before the end of the stream, but
	// the reader did not take them.
	conn.pending <- appendDatagram(nil, flow.id, [][]byte{[]byte("a")})
	conn.pending <- appendDatagram(nil, other.id, [][]byte{[]byte("for another call")})
	conn.pending <- appendDatagram(nil, flow.id, [][]byte{[]byte("b")})
	flow.endReceive()

	for _, want := range []string{"a", "b"} {
		if message, ok := flow.sink.pop(); !ok || string(message) != want {
			t.Fatalf("pop = %q, %v; want %q", message, ok, want)
		}
	}
	if message, ok := other.sink.pop(); !ok || string(message) != "for another call" {
		t.Fatalf("the other call's datagram = %q, %v", message, ok)
	}
	if flow.sink.push([]byte("late")) {
		t.Fatal("the direction accepts messages after it ended")
	}

	var noReceive *datagramFlow
	noReceive.endReceive()
	mux.allocate(false, 1024, nil).endReceive()
}

func TestMuxDeliversEveryMessageOfABatchedDatagramAndDropsMalformedOnesWhole(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newClientMux(conn)
	defer mux.close()
	flow := mux.allocate(true, 8, nil)

	batch := appendDatagram(nil, flow.id, [][]byte{[]byte("one"), nil, []byte("three")})
	// A truncated last frame drops the whole datagram, the messages before it
	// included.
	truncated := append(bytes.Clone(batch), 0x00, 0x05, 'x')
	// Frames of other types are skipped, with non-minimal varints.
	skipping := append(quicvarint.Append(nil, flow.id), 0x40, 0x03, 0x40, 0x01, 'z', 0x00, 0x01, 'a', 0x1f, 0x00)
	// A message over the frame size limit is dropped, the others are not.
	oversized := appendDatagram(nil, flow.id, [][]byte{[]byte("ok"), make([]byte, 9), []byte("fine")})

	for _, datagram := range [][]byte{truncated, batch, skipping, oversized} {
		conn.incoming <- datagram
	}
	want := []string{"one", "", "three", "a", "ok", "fine"}
	var got []string
	for range want {
		got = append(got, takeMessage(t, flow.sink))
	}
	if !slices.Equal(got, want) {
		t.Fatalf("messages = %q, want %q", got, want)
	}
	// Nothing else was queued: the truncated datagram delivered no message.
	conn.deliver(flow.id, "sentinel")
	if got := takeMessage(t, flow.sink); got != "sentinel" {
		t.Fatalf("next message = %q; a dropped datagram left messages behind", got)
	}
}

func TestHeldBatchedDatagramDeliversAllItsMessagesWhenClaimed(t *testing.T) {
	conn := newFakeDatagramConn()
	mux := newServerMux(conn)
	defer mux.close()
	anchor, err := mux.register(99, false, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer anchor.release()

	conn.incoming <- appendDatagram(nil, 5, [][]byte{[]byte("a"), []byte("b"), []byte("c")})
	eventually(t, "the datagram to be held", func() bool { return mux.heldCount() == 1 })
	flow, err := mux.register(5, true, 1024, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"a", "b", "c"} {
		if got := takeMessage(t, flow.sink); got != want {
			t.Fatalf("claimed %q, want %q", got, want)
		}
	}
}

func TestBufferedMessagePipeYieldsWhatIsReadyAndDrainsOnClose(t *testing.T) {
	pipe := newBufferedMessagePipe[int](context.Background(), 4)
	if _, ok, _ := pipe.recvReady(); ok {
		t.Fatal("an empty pipe has a message ready")
	}
	for i := range 3 {
		if err := pipe.Send(i); err != nil {
			t.Fatal(err)
		}
	}
	if got, ok, _ := pipe.recvReady(); !ok || got != 0 {
		t.Fatalf("recvReady = %d, %v; want 0", got, ok)
	}
	_ = pipe.Close()
	// A clean close ends the stream after the messages sent before it.
	for want := 1; want <= 2; want++ {
		if got, err := pipe.Recv(); err != nil || got != want {
			t.Fatalf("Recv = %d, %v; want %d", got, err, want)
		}
	}
	if _, err := pipe.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv at the end = %v, want io.EOF", err)
	}
	if err := pipe.Send(9); err == nil {
		t.Fatal("Send after Close succeeded")
	}
}
