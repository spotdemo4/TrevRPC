//go:build trevrpc_native && cgo && (linux || darwin) && (amd64 || arm64)

package native

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

const (
	eventDiagnostic       uint32 = 1
	eventStopped          uint32 = 2
	eventListenerStopped  uint32 = 3
	eventConnectionReady  uint32 = 4
	eventConnectionFailed uint32 = 5
	eventConnectionClosed uint32 = 6
	eventStreamReady      uint32 = 7
	eventStreamFailed     uint32 = 8
	eventStreamReadable   uint32 = 9
	eventReceiveFIN       uint32 = 10
	eventSendComplete     uint32 = 11
	eventStreamClosed     uint32 = 12
	eventSendStopped      uint32 = 15

	eventFlagFatal          uint32 = 1 << 0
	eventFlagTerminal       uint32 = 1 << 1
	eventFlagClient         uint32 = 1 << 2
	eventFlagServer         uint32 = 1 << 3
	eventFlagLocal          uint32 = 1 << 4
	eventFlagPeer           uint32 = 1 << 5
	eventFlagPeerReset      uint32 = 1 << 6
	eventFlagTransportError uint32 = 1 << 7
	eventFlagCleanFIN       uint32 = 1 << 8

	pollEngineReady  uint32 = 1 << 0
	pollCommandReady uint32 = 1 << 1
	pollTimeoutReady uint32 = 1 << 2
)

var errEngineClosed = errors.New("native Engine is closed")

type driverRuntime interface {
	WakeSources() ([]WakeSource, error)
	pollTimeoutMilliseconds() int
	transportABIVersion() uint32
	nextEvent() (nativeEvent, bool, error)
	listen(EndpointConfig) (nativeHandle, error)
	listenerPort(nativeHandle) (uint16, error)
	dial(EndpointConfig, uint64) (nativeHandle, error)
	cancelDial(nativeHandle) error
	openStream(nativeHandle, uint64) (nativeHandle, error)
	sendFrame(nativeHandle, uint64, []byte) error
	receiveFrame(nativeHandle) ([]byte, error)
	finishStreamSend(nativeHandle) error
	abortStreamRead(nativeHandle, uint64) error
	abortStreamWrite(nativeHandle, uint64) error
	abortStream(nativeHandle, uint64) error
	closeConnection(nativeHandle, uint64) error
	closeListener(nativeHandle) error
	releaseHandle(nativeHandle, uint32) error
	beginClose() error
	waitDrained() error
	release() (bool, error)
}

type driverCommand func(*driverState)

type nativeWakePoll func([]int, int, int) (uint32, error)

type admittedCommand struct {
	command  driverCommand
	rejected atomic.Bool
}

func (command *admittedCommand) invoke(state *driverState) {
	if command.rejected.Load() {
		return
	}
	command.command(state)
}

type Engine struct {
	runtime                   driverRuntime
	wakeDescriptors           []int
	commandRead               *os.File
	commandWrite              *os.File
	commandWriteFn            func(int, []byte) (int, error)
	pollNativeWakeFn          nativeWakePoll
	commandAdmissionAttemptFn func()
	commandPipeMu             sync.Mutex
	commandAdmission          sync.Mutex
	commands                  chan driverCommand
	done                      chan struct{}
	closing                   atomic.Bool
	closeOnce                 sync.Once
	closeMu                   sync.Mutex
	closeErr                  error
	capacityMu                sync.Mutex
	capacityChanged           chan struct{}
	maxQueuedCount            uint32
	maxQueuedBytes            uint64
	requestedBackend          transportinternal.Backend
}

type handleReleaseKey struct {
	handle nativeHandle
	kind   uint32
}

type driverState struct {
	engine             *Engine
	nextOperationID    uint64
	listeners          map[nativeHandle]*Listener
	connections        map[nativeHandle]*Connection
	streams            map[nativeHandle]*Stream
	earlySendStops     map[nativeHandle]error
	pendingDials       map[uint64]pendingDial
	pendingOpens       map[uint64]pendingOpen
	pendingSends       map[uint64]chan error
	pendingAdmissions  map[*transportAdmission]struct{}
	pendingReleases    map[handleReleaseKey]struct{}
	readable           []*Stream
	readableBlocked    []*Stream
	queuedReceiveCount uint32
	queuedReceiveBytes uint64
	closingStarted     bool
	stopped            bool
	released           bool
	terminalErr        error
}

type listenerResult struct {
	listener *Listener
	err      error
}

type connectionResult struct {
	connection *Connection
	err        error
}

type pendingDial struct {
	handle nativeHandle
	result chan connectionResult
}

type streamResult struct {
	stream *Stream
	err    error
}

type pendingOpen struct {
	handle nativeHandle
	result chan streamResult
}

type receiveResult struct {
	body []byte
	err  error
}

type readDeadlineResult struct {
	receive   receiveResult
	completed bool
}

type Listener struct {
	engine    *Engine
	handle    nativeHandle
	address   transportinternal.Address
	config    EndpointConfig
	accept    objectQueue[*Connection]
	lifecycle lifecycle
	closeOnce sync.Once
	closeErr  error
}

type Connection struct {
	engine       *Engine
	handle       nativeHandle
	info         transportinternal.ConnectionInfo
	maxFrameSize uint64
	accept       objectQueue[*Stream]
	lifecycle    lifecycle
	closeOnce    sync.Once
	closeErr     error
}

type Stream struct {
	engine          *Engine
	handle          nativeHandle
	info            transportinternal.ConnectionInfo
	lifecycle       lifecycle
	context         context.Context
	cancel          context.CancelCauseFunc
	reader          frameReader
	writer          frameWriter
	readDeadlineMu  sync.Mutex
	readDeadline    time.Time
	deadlineChanged chan struct{}
	closeOnce       sync.Once
	closeErr        error
	cancelReadOnce  sync.Once
	cancelWriteOnce sync.Once
	readCancelDone  chan struct{}
	readCancelErr   error
	writeCancelled  atomic.Bool
	writeCancelDone chan struct{}
	writeCancelErr  error
	queuedBodies    [][]byte
	queuedBytes     uint64
	queuedAccounted bool
	readWaiter      chan receiveResult
	readable        bool
	scheduled       bool
	readableBlocked bool
	readCancelled   bool
	receiveDone     bool
	receiveErr      error
	sendDone        bool
	terminalPending bool
	terminalErr     error
}

var (
	_ transportinternal.Listener            = (*Listener)(nil)
	_ transportinternal.Connection          = (*Connection)(nil)
	_ transportinternal.BidirectionalStream = (*Stream)(nil)
	_ transportinternal.StreamContext       = (*Stream)(nil)
	_ transportinternal.ReadCanceler        = (*Stream)(nil)
	_ transportinternal.WriteCanceler       = (*Stream)(nil)
	_ transportinternal.ReadDeadlineSetter  = (*Stream)(nil)
)

func NewEngine(config EngineConfig) (*Engine, error) {
	runtime, err := openRuntime(config)
	if err != nil {
		return nil, err
	}
	return newDriver(runtime, config, "Engine")
}

func NewTransport(config EngineConfig) (*Engine, error) {
	runtime, err := openTransportRuntime(config)
	if err != nil {
		return nil, err
	}
	return newDriver(runtime, config, "Transport")
}

func newDriver(runtime driverRuntime, config EngineConfig, runtimeName string) (*Engine, error) {
	cleanupError := func(cause error) (*Engine, error) {
		return nil, errors.Join(cause, closeUnstartedRuntime(runtime))
	}
	wakes, err := runtime.WakeSources()
	if err != nil {
		return cleanupError(err)
	}
	wakeDescriptors := make([]int, len(wakes))
	for index, wake := range wakes {
		if wake.Kind != WakeSourcePOSIXFD || !wake.Borrowed() || !wake.LevelTriggered() {
			return cleanupError(fmt.Errorf(
				"unsupported %s wake source kind=%d flags=%#x",
				runtimeName,
				wake.Kind,
				wake.Flags,
			))
		}
		wakeDescriptors[index] = int(wake.NativeHandle)
	}

	commandRead, commandWrite, err := os.Pipe()
	if err != nil {
		return cleanupError(fmt.Errorf("create native %s command pipe: %w", runtimeName, err))
	}
	if err := unix.SetNonblock(int(commandRead.Fd()), true); err != nil {
		_ = commandRead.Close()
		_ = commandWrite.Close()
		return cleanupError(fmt.Errorf("configure native %s command reader: %w", runtimeName, err))
	}
	if err := unix.SetNonblock(int(commandWrite.Fd()), true); err != nil {
		_ = commandRead.Close()
		_ = commandWrite.Close()
		return cleanupError(fmt.Errorf("configure native %s command writer: %w", runtimeName, err))
	}

	normalized := normalizeEngineConfig(config)
	engine := &Engine{
		runtime:          runtime,
		wakeDescriptors:  wakeDescriptors,
		commandRead:      commandRead,
		commandWrite:     commandWrite,
		commands:         make(chan driverCommand, normalized.EventCapacity),
		done:             make(chan struct{}),
		capacityChanged:  make(chan struct{}),
		maxQueuedCount:   normalized.MaxReceiveOwnedCount,
		maxQueuedBytes:   normalized.MaxReceiveOwnedBytes,
		requestedBackend: transportinternal.BackendNative,
	}
	go engine.run()
	return engine, nil
}

func closeUnstartedRuntime(runtime driverRuntime) error {
	var result error
	if err := runtime.beginClose(); err != nil {
		result = errors.Join(result, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	draining := true
	for draining {
		for {
			_, ok, err := runtime.nextEvent()
			if err != nil {
				result = errors.Join(result, err)
				draining = false
				break
			}
			if !ok {
				break
			}
		}
		if !draining {
			break
		}
		err := runtime.waitDrained()
		if err == nil {
			break
		}
		if !errors.Is(err, errNativeWouldBlock) {
			result = errors.Join(result, err)
			break
		}
		if time.Now().After(deadline) {
			result = errors.Join(result, errors.New("timed out draining unstarted native runtime"))
			break
		}
		time.Sleep(time.Millisecond)
	}
	consumed, releaseErr := runtime.release()
	if !consumed {
		startRuntimeReaper(runtime, nil)
	}
	return errors.Join(result, releaseErr)
}

const (
	runtimeReaperMinimumDelay = 10 * time.Millisecond
	runtimeReaperMaximumDelay = time.Second
	runtimeReaperMaxAttempts  = 4
)

// startRuntimeReaper transfers retained native ownership out of the operational
// driver so reconnect can proceed without spinning on failed release. If a
// child handle remains permanently unreleasable, the runtime is quarantined
// rather than freed behind that live handle. The reaper is deliberately
// bounded so a provider cannot keep a goroutine alive forever.
func startRuntimeReaper(
	runtime driverRuntime,
	pending map[handleReleaseKey]struct{},
) {
	startRuntimeReaperWithReport(runtime, pending, nil)
}

func startRuntimeReaperWithReport(
	runtime driverRuntime,
	pending map[handleReleaseKey]struct{},
	report func(error),
) {
	owned := make(map[handleReleaseKey]struct{}, len(pending))
	for key := range pending {
		owned[key] = struct{}{}
	}
	go func() {
		if err := reapRuntime(runtime, owned); err != nil && report != nil {
			report(err)
		}
	}()
}

func sortedReleaseKeys(pending map[handleReleaseKey]struct{}) []handleReleaseKey {
	keys := make([]handleReleaseKey, 0, len(pending))
	for key := range pending {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool {
		a, b := keys[left], keys[right]
		if a.handle.owner != b.handle.owner {
			return a.handle.owner < b.handle.owner
		}
		if a.handle.slot != b.handle.slot {
			return a.handle.slot < b.handle.slot
		}
		if a.handle.generation != b.handle.generation {
			return a.handle.generation < b.handle.generation
		}
		return a.kind < b.kind
	})
	return keys
}

func reapRuntime(
	runtime driverRuntime,
	pending map[handleReleaseKey]struct{},
) error {
	delay := runtimeReaperMinimumDelay
	releaseErrors := make(map[handleReleaseKey]error, len(pending))
	var waitDrainedErr error
	var runtimeReleaseErr error
	for attempt := 0; attempt < runtimeReaperMaxAttempts; attempt++ {
		timer := time.NewTimer(delay)
		<-timer.C
		progress := false

		for _, key := range sortedReleaseKeys(pending) {
			err := runtime.releaseHandle(key.handle, key.kind)
			if err == nil || nativeStatusIs(err, -int(unix.ESTALE)) {
				delete(pending, key)
				delete(releaseErrors, key)
				progress = true
			} else {
				releaseErrors[key] = err
			}
		}
		for {
			event, present, err := runtime.nextEvent()
			if err != nil {
				waitDrainedErr = err
				break
			}
			if !present {
				break
			}
			progress = true
			if event.admission != nil {
				_ = event.admission.respond(503)
			}
		}
		if len(pending) == 0 {
			waitDrainedErr = runtime.waitDrained()
			consumed, err := runtime.release()
			runtimeReleaseErr = err
			if consumed {
				if err != nil {
					return fmt.Errorf("native runtime release completed with error: %w", err)
				}
				return nil
			}
		}
		if attempt+1 == runtimeReaperMaxAttempts {
			break
		}
		if progress {
			delay = runtimeReaperMinimumDelay
		} else if delay < runtimeReaperMaximumDelay {
			delay *= 2
			if delay > runtimeReaperMaximumDelay {
				delay = runtimeReaperMaximumDelay
			}
		}
	}

	// Never release the runtime while child handles remain live. The provider
	// may safely release the runtime only after all child releases complete;
	// otherwise quarantine the runtime ownership and return a stable teardown
	// diagnostic instead of freeing memory behind those handles.
	causes := make([]error, 0, len(releaseErrors)+2)
	releaseErrorKeys := make(map[handleReleaseKey]struct{}, len(releaseErrors))
	for key := range releaseErrors {
		releaseErrorKeys[key] = struct{}{}
	}
	for _, key := range sortedReleaseKeys(releaseErrorKeys) {
		causes = append(causes, releaseErrors[key])
	}
	if waitDrainedErr != nil {
		causes = append(causes, waitDrainedErr)
	}
	if runtimeReleaseErr != nil {
		causes = append(causes, runtimeReleaseErr)
	}
	if len(pending) != 0 {
		causes = append(causes, fmt.Errorf("%d native handle release(s) remained pending", len(pending)))
	}
	if len(causes) == 0 {
		causes = append(causes, errors.New("native runtime release remained unconsumed"))
	}
	return fmt.Errorf(
		"native runtime cleanup exhausted after %d attempts: %w",
		runtimeReaperMaxAttempts,
		errors.Join(causes...),
	)
}

func normalizeEngineConfig(config EngineConfig) EngineConfig {
	defaults := DefaultEngineConfig()
	if config.EventCapacity == 0 {
		config.EventCapacity = defaults.EventCapacity
	}
	if config.ListenerCapacity == 0 {
		config.ListenerCapacity = defaults.ListenerCapacity
	}
	if config.ConnectionCapacity == 0 {
		config.ConnectionCapacity = defaults.ConnectionCapacity
	}
	if config.StreamCapacity == 0 {
		config.StreamCapacity = defaults.StreamCapacity
	}
	if config.MaxReceiveOwnedCount == 0 {
		config.MaxReceiveOwnedCount = defaults.MaxReceiveOwnedCount
	}
	if config.MaxReceiveOwnedBytes == 0 {
		config.MaxReceiveOwnedBytes = defaults.MaxReceiveOwnedBytes
	}
	return config
}

func normalizeEndpointConfig(config EndpointConfig) EndpointConfig {
	defaults := DefaultEndpointConfig()
	if config.Protocol == ProtocolAuto {
		config.Protocol = defaults.Protocol
	}
	if len(config.ALPN) == 0 && config.Protocol == ProtocolNative {
		config.ALPN = []byte("trevrpc/1")
	} else {
		config.ALPN = append([]byte(nil), config.ALPN...)
	}
	if config.PeerBidirectionalStreams == 0 {
		config.PeerBidirectionalStreams = defaults.PeerBidirectionalStreams
	}
	if config.WebTransportProfiles == 0 && config.Protocol != ProtocolMultiplexed {
		config.WebTransportProfiles = defaults.WebTransportProfiles
	}
	if config.MaxSessions == 0 && config.Protocol != ProtocolMultiplexed {
		config.MaxSessions = defaults.MaxSessions
	}
	if config.MaxPendingSendCount == 0 {
		config.MaxPendingSendCount = defaults.MaxPendingSendCount
	}
	if config.MaxPendingSendBytes == 0 {
		config.MaxPendingSendBytes = defaults.MaxPendingSendBytes
	}
	if config.MaxPendingReceiveCount == 0 {
		config.MaxPendingReceiveCount = defaults.MaxPendingReceiveCount
	}
	if config.MaxPendingReceiveBytes == 0 {
		config.MaxPendingReceiveBytes = defaults.MaxPendingReceiveBytes
	}
	if config.MaxFrameSize == 0 {
		config.MaxFrameSize = defaults.MaxFrameSize
	}
	if config.MaxFieldSectionSize == 0 {
		config.MaxFieldSectionSize = defaults.MaxFieldSectionSize
	}
	if config.UnresolvedStreamCount == 0 {
		config.UnresolvedStreamCount = defaults.UnresolvedStreamCount
	}
	if config.UnresolvedStreamBytes == 0 {
		config.UnresolvedStreamBytes = defaults.UnresolvedStreamBytes
	}
	if config.UnresolvedStreamTimeoutMS == 0 {
		config.UnresolvedStreamTimeoutMS = defaults.UnresolvedStreamTimeoutMS
	}
	return config
}

func (e *Engine) run() {
	state := &driverState{
		engine:            e,
		listeners:         make(map[nativeHandle]*Listener),
		connections:       make(map[nativeHandle]*Connection),
		streams:           make(map[nativeHandle]*Stream),
		earlySendStops:    make(map[nativeHandle]error),
		pendingDials:      make(map[uint64]pendingDial),
		pendingOpens:      make(map[uint64]pendingOpen),
		pendingSends:      make(map[uint64]chan error),
		pendingAdmissions: make(map[*transportAdmission]struct{}),
		pendingReleases:   make(map[handleReleaseKey]struct{}),
		nextOperationID:   1,
	}
	defer func() {
		e.commandPipeMu.Lock()
		_ = e.commandRead.Close()
		_ = e.commandWrite.Close()
		e.commandPipeMu.Unlock()
		close(e.done)
	}()

	for {
		if state.released {
			return
		}
		e.processCommands(state)
		if state.released {
			return
		}
		if e.closing.Load() && !state.closingStarted {
			state.startShutdown(nil)
		}
		if err := state.drainEvents(); err != nil {
			state.startShutdown(err)
		}
		if state.released {
			return
		}

		timeout := e.runtime.pollTimeoutMilliseconds()
		pollWake := e.pollNativeWakeFn
		if pollWake == nil {
			pollWake = pollNativeWake
		}
		ready, err := pollWake(
			e.wakeDescriptors,
			int(e.commandRead.Fd()),
			timeout,
		)
		if err != nil {
			state.startShutdown(err)
			continue
		}
		if ready&pollCommandReady != 0 {
			e.drainCommandWake()
		}
		if ready&(pollEngineReady|pollTimeoutReady) != 0 {
			if err := state.drainEvents(); err != nil {
				state.startShutdown(err)
			}
		}
	}
}

// processCommands shares commandAdmission with enqueue so a command cannot be
// observed by the driver until its wake signal has been accepted or rejected.
func (e *Engine) processCommands(state *driverState) {
	if attempt := e.commandAdmissionAttemptFn; attempt != nil {
		attempt()
	}
	e.commandAdmission.Lock()
	defer e.commandAdmission.Unlock()
	e.processCommandsLocked(state)
}

func (e *Engine) processCommandsLocked(state *driverState) {
	for {
		select {
		case command := <-e.commands:
			command(state)
			if state.released {
				return
			}
		default:
			return
		}
	}
}

func (e *Engine) drainCommandWake() {
	var signal [1]byte
	_, _ = unix.Read(int(e.commandRead.Fd()), signal[:])
}

func (e *Engine) enqueue(ctx context.Context, command driverCommand) error {
	if e == nil {
		return errEngineClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Keep admission held through the queue send and wake write. If signaling
	// fails, the ticket is rejected before the driver can observe it.
	e.commandAdmission.Lock()
	defer e.commandAdmission.Unlock()
	if e.closing.Load() {
		return errEngineClosed
	}
	admitted := &admittedCommand{command: command}
	select {
	case e.commands <- admitted.invoke:
	default:
		return errNativeWouldBlock
	}
	if err := e.signalCommands(); err != nil {
		admitted.rejected.Store(true)
		e.closing.Store(true)
		e.setCloseError(err)
		e.forceCommandWake()
		return err
	}
	return nil
}

// enqueueCleanup waits for driver capacity because an internal ownership transfer
// must not leak an object when ordinary command admission reports backpressure.
// Engine shutdown itself reclaims the object if the driver has already stopped.
func (e *Engine) enqueueCleanup(command driverCommand) {
	if e == nil {
		return
	}
	select {
	case e.commands <- command:
		if err := e.signalCommands(); err != nil {
			e.closing.Store(true)
			e.setCloseError(err)
			e.forceCommandWake()
		}
	case <-e.done:
	}
}

func (e *Engine) signalCommands() error {
	e.commandPipeMu.Lock()
	defer e.commandPipeMu.Unlock()
	if e.commandWrite == nil {
		return errors.New("signal native Engine command: command pipe is unavailable")
	}
	write := e.commandWriteFn
	if write == nil {
		write = unix.Write
	}
	var signal [1]byte
	for {
		written, err := write(int(e.commandWrite.Fd()), signal[:])
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err == nil {
			if written == len(signal) {
				return nil
			}
			return io.ErrShortWrite
		}
		if errors.Is(err, unix.EAGAIN) || errors.Is(err, unix.EWOULDBLOCK) {
			return nil
		}
		return fmt.Errorf("signal native Engine command: %w", err)
	}
}

func (e *Engine) forceCommandWake() {
	e.commandPipeMu.Lock()
	defer e.commandPipeMu.Unlock()
	if e.commandWrite != nil {
		_ = e.commandWrite.Close()
	}
}

func (e *Engine) Listen(ctx context.Context, config EndpointConfig) (*Listener, error) {
	config = normalizeEndpointConfig(config)
	result := make(chan listenerResult, 1)
	if err := e.enqueue(ctx, func(state *driverState) {
		handle, err := e.runtime.listen(config)
		if err != nil {
			result <- listenerResult{err: err}
			return
		}
		port, err := e.runtime.listenerPort(handle)
		if err != nil {
			_ = e.runtime.closeListener(handle)
			result <- listenerResult{err: err}
			return
		}
		config.Port = port
		listener := &Listener{
			engine:    e,
			handle:    handle,
			address:   endpointAddress(config.Host, port),
			config:    config,
			accept:    newObjectQueue[*Connection](),
			lifecycle: newLifecycle(),
		}
		state.listeners[handle] = listener
		result <- listenerResult{listener: listener}
	}); err != nil {
		return nil, err
	}

	select {
	case value := <-result:
		return value.listener, value.err
	case <-ctx.Done():
		discardListenerResult(result, e.done)
		return nil, ctx.Err()
	case <-e.done:
		return nil, e.stoppedError()
	}
}

func (e *Engine) Dial(ctx context.Context, config EndpointConfig) (*Connection, error) {
	config = normalizeEndpointConfig(config)
	result := make(chan connectionResult, 1)
	if err := e.enqueue(ctx, func(state *driverState) {
		operationID := state.nextID()
		handle, err := e.runtime.dial(config, operationID)
		if err != nil {
			result <- connectionResult{err: err}
			return
		}
		connection := newConnection(e, handle, config, false)
		state.connections[handle] = connection
		state.pendingDials[operationID] = pendingDial{handle: handle, result: result}
	}); err != nil {
		return nil, err
	}

	select {
	case value := <-result:
		return value.connection, value.err
	case <-ctx.Done():
		e.cancelPendingDial(result)
		discardConnectionResult(result, e.done)
		return nil, ctx.Err()
	case <-e.done:
		return nil, e.stoppedError()
	}
}

func (e *Engine) cancelPendingDial(result chan connectionResult) {
	e.enqueueCleanup(func(state *driverState) {
		for _, pending := range state.pendingDials {
			if pending.result != result {
				continue
			}
			_ = e.runtime.cancelDial(pending.handle)
			return
		}
	})
}

func discardListenerResult(result <-chan listenerResult, engineDone <-chan struct{}) {
	go func() {
		select {
		case value := <-result:
			if value.listener != nil {
				value.listener.engine.enqueueCleanup(func(*driverState) {
					_ = value.listener.engine.runtime.closeListener(value.listener.handle)
				})
			}
		case <-engineDone:
		}
	}()
}

func discardConnectionResult(result <-chan connectionResult, engineDone <-chan struct{}) {
	go func() {
		select {
		case value := <-result:
			if value.connection != nil {
				value.connection.engine.enqueueCleanup(func(*driverState) {
					_ = value.connection.engine.runtime.closeConnection(value.connection.handle, 1)
				})
			}
		case <-engineDone:
		}
	}()
}

func discardStreamResult(result <-chan streamResult, engineDone <-chan struct{}) {
	go func() {
		select {
		case value := <-result:
			if value.stream != nil {
				value.stream.engine.enqueueCleanup(func(*driverState) {
					_ = value.stream.engine.runtime.abortStream(value.stream.handle, 1)
				})
			}
		case <-engineDone:
		}
	}()
}

func (e *Engine) Close() error {
	if e == nil {
		return nil
	}
	e.closeOnce.Do(func() {
		e.commandAdmission.Lock()
		if !e.closing.Swap(true) {
			if err := e.signalCommands(); err != nil {
				e.setCloseError(err)
				e.forceCommandWake()
			}
		}
		e.commandAdmission.Unlock()
	})
	<-e.done
	return e.Err()
}

func (e *Engine) Done() <-chan struct{} {
	if e == nil {
		done := make(chan struct{})
		close(done)
		return done
	}
	return e.done
}

func (e *Engine) Err() error {
	if e == nil {
		return errEngineClosed
	}
	e.closeMu.Lock()
	defer e.closeMu.Unlock()
	return e.closeErr
}

func (e *Engine) stoppedError() error {
	if err := e.Err(); err != nil {
		return err
	}
	return errEngineClosed
}

func (e *Engine) setCloseError(err error) {
	if err == nil {
		return
	}
	e.closeMu.Lock()
	e.closeErr = errors.Join(e.closeErr, err)
	e.closeMu.Unlock()
}

func (e *Engine) capacityWaiter() <-chan struct{} {
	e.capacityMu.Lock()
	defer e.capacityMu.Unlock()
	return e.capacityChanged
}

func (e *Engine) signalCapacity() {
	e.capacityMu.Lock()
	close(e.capacityChanged)
	e.capacityChanged = make(chan struct{})
	e.capacityMu.Unlock()
}

func (state *driverState) nextID() uint64 {
	result := state.nextOperationID
	state.nextOperationID++
	if state.nextOperationID == 0 {
		state.nextOperationID = 1
	}
	return result
}

func (state *driverState) drainEvents() error {
	const maxEventsPerDrain = 64
	for eventCount := 0; eventCount < maxEventsPerDrain; eventCount++ {
		event, present, err := state.engine.runtime.nextEvent()
		if err != nil {
			return err
		}
		if !present {
			state.retryPendingReleases()
			if state.stopped {
				state.finishStopped()
			}
			return nil
		}
		state.handleEvent(event)
		state.engine.signalCapacity()
	}
	state.retryPendingReleases()
	return nil
}

func (state *driverState) handleEvent(event nativeEvent) {
	switch event.kind {
	case eventDiagnostic:
		if event.flags&eventFlagFatal != 0 {
			state.startShutdown(eventError(event, "native Engine fatal diagnostic"))
		}
	case eventStopped:
		state.stopped = true
		if event.status != 0 || event.flags&eventFlagFatal != 0 {
			state.terminalErr = eventError(event, "native Engine stopped")
		}
	case eventListenerStopped:
		if listener := state.listeners[event.subject]; listener != nil {
			err := eventError(event, "native listener stopped")
			listener.lifecycle.finish(err)
			listener.accept.close(err)
			delete(state.listeners, event.subject)
		}
		state.releaseHandle(event.subject, transportObjectListener)
	case eventConnectionReady:
		state.connectionReady(event)
	case eventConnectionFailed, eventConnectionClosed:
		state.connectionTerminal(event)
	case eventStreamReady:
		state.streamReady(event)
	case eventStreamFailed, eventStreamClosed:
		state.streamTerminal(event)
	case eventStreamReadable:
		if stream := state.streams[event.subject]; stream != nil && !stream.receiveDone {
			state.scheduleReadable(stream)
			state.drainReadable()
		}
	case eventReceiveFIN:
		if stream := state.streams[event.subject]; stream != nil && !stream.receiveDone {
			stream.receiveDone = true
			if event.status != 0 || event.flags&eventFlagCleanFIN == 0 {
				stream.receiveErr = eventError(event, "native stream receive failed")
			}
			state.scheduleReadable(stream)
			state.drainReadable()
			state.finishReadWaiter(stream)
		}
	case eventSendStopped:
		state.sendStopped(event)
	case AdmissionHTTP3, AdmissionWebTransport:
		state.admit(event)
	case eventSendComplete:
		if pending := state.pendingSends[event.operationID]; pending != nil {
			delete(state.pendingSends, event.operationID)
			if event.status == 0 {
				pending <- nil
			} else {
				pending <- eventError(event, "native stream send failed")
			}
		}
	default:
		state.startShutdown(fmt.Errorf("unknown native event kind %d", event.kind))
	}
}

func (state *driverState) sendStopped(event nativeEvent) {
	err := eventError(event, "native stream send stopped")
	stream := state.streams[event.subject]
	if stream == nil {
		if state.earlySendStops == nil {
			state.earlySendStops = make(map[nativeHandle]error)
		}
		if _, present := state.earlySendStops[event.subject]; !present {
			state.earlySendStops[event.subject] = err
		}
		return
	}
	state.applyEarlySendStop(stream, err)
}

func (state *driverState) applyEarlySendStop(stream *Stream, eventErr error) {
	if eventErr == nil {
		eventErr = state.earlySendStops[stream.handle]
		delete(state.earlySendStops, stream.handle)
	}
	if eventErr == nil || stream.sendDone {
		return
	}
	stream.sendDone = true
	stream.writer.fail(eventErr)
}

func (state *driverState) connectionReady(event nativeEvent) {
	if event.operationID != 0 {
		pending, present := state.pendingDials[event.operationID]
		delete(state.pendingDials, event.operationID)
		connection := state.connections[event.subject]
		if !present || pending.handle != event.subject || connection == nil {
			err := fmt.Errorf(
				"native connection ready operation %d returned unexpected handle %+v",
				event.operationID,
				event.subject,
			)
			if present {
				pending.result <- connectionResult{err: err}
			}
			state.startShutdown(err)
			return
		}
		pending.result <- connectionResult{connection: connection}
		return
	}
	listener := state.listeners[event.parent]
	if listener == nil || event.flags&eventFlagServer == 0 {
		_ = state.engine.runtime.closeConnection(event.subject, 0)
		return
	}
	config := listener.config
	if event.protocol != ProtocolAuto {
		config.Protocol = event.protocol
	}
	connection := newConnection(state.engine, event.subject, config, true)
	state.connections[event.subject] = connection
	if !listener.accept.push(connection) {
		_ = state.engine.runtime.closeConnection(event.subject, 0)
	}
}

func (state *driverState) admit(event nativeEvent) {
	admission := event.admission
	listener := state.listeners[event.parent]
	if state.closingStarted {
		if admission != nil {
			if err := admission.respond(503); err != nil {
				state.terminalErr = errors.Join(state.terminalErr, err)
			}
		}
		return
	}
	if admission == nil || listener == nil || listener.config.Admission == nil {
		if admission != nil {
			if err := admission.respond(500); err != nil {
				state.startShutdown(err)
			}
		}
		return
	}
	state.pendingAdmissions[admission] = struct{}{}
	handler := listener.config.Admission
	request := admission.request
	go func() {
		status := invokeAdmissionHandler(handler, request)
		state.engine.enqueueCleanup(func(state *driverState) {
			if _, present := state.pendingAdmissions[admission]; !present {
				return
			}
			delete(state.pendingAdmissions, admission)
			if err := admission.respond(status); err != nil {
				state.startShutdown(err)
			}
		})
	}()
}

func invokeAdmissionHandler(handler AdmissionHandler, request AdmissionRequest) (status uint16) {
	status = 500
	defer func() {
		if recover() != nil {
			status = 500
		}
		if status != 200 && (status < 400 || status > 599) {
			status = 500
		}
	}()
	return handler(request)
}

func (state *driverState) failPendingAdmissions(status uint16) {
	for admission := range state.pendingAdmissions {
		delete(state.pendingAdmissions, admission)
		if err := admission.respond(status); err != nil {
			state.terminalErr = errors.Join(state.terminalErr, err)
		}
	}
}

func (state *driverState) connectionTerminal(event nativeEvent) {
	connection := state.connections[event.subject]
	if pending, present := state.pendingDials[event.operationID]; present {
		delete(state.pendingDials, event.operationID)
		pending.result <- connectionResult{err: eventError(event, "native connection failed")}
	}
	if connection != nil {
		err := eventError(event, "native connection closed")
		connection.lifecycle.finish(err)
		connection.accept.close(err)
		delete(state.connections, event.subject)
	}
	state.releaseHandle(event.subject, transportObjectConnection)
}

func (state *driverState) streamReady(event nativeEvent) {
	if event.operationID != 0 && event.flags&eventFlagLocal != 0 {
		pending, present := state.pendingOpens[event.operationID]
		delete(state.pendingOpens, event.operationID)
		stream := state.streams[event.subject]
		if !present || pending.handle != event.subject || stream == nil {
			err := fmt.Errorf(
				"native stream ready operation %d returned unexpected handle %+v",
				event.operationID,
				event.subject,
			)
			if present {
				pending.result <- streamResult{err: err}
			}
			state.startShutdown(err)
			return
		}
		state.applyEarlySendStop(stream, nil)
		pending.result <- streamResult{stream: stream}
		return
	}
	connection := state.connections[event.parent]
	if connection == nil || event.flags&eventFlagPeer == 0 {
		_ = state.engine.runtime.abortStream(event.subject, 0)
		return
	}
	stream := newStream(state.engine, event.subject, connection.info, connection.maxFrameSize)
	state.streams[event.subject] = stream
	state.applyEarlySendStop(stream, nil)
	if !connection.accept.push(stream) {
		_ = state.engine.runtime.abortStream(event.subject, 0)
	}
}

func (state *driverState) streamTerminal(event nativeEvent) {
	delete(state.earlySendStops, event.subject)
	stream := state.streams[event.subject]
	if pending, present := state.pendingOpens[event.operationID]; present {
		delete(state.pendingOpens, event.operationID)
		pending.result <- streamResult{err: eventError(event, "native stream open failed")}
	}
	if stream == nil {
		state.releaseHandle(event.subject, transportObjectStream)
		return
	}
	err := eventError(event, "native stream closed")
	stream.terminalPending = true
	stream.terminalErr = err
	state.drainTerminalReceive(stream)
	state.drainReadable()
	stream.writer.fail(err)
	stream.lifecycle.finish(err)
	stream.cancel(err)
}

func (state *driverState) scheduleReadable(stream *Stream) {
	stream.readable = true
	if stream.scheduled || stream.readableBlocked {
		return
	}
	stream.scheduled = true
	state.readable = append(state.readable, stream)
}

func (state *driverState) deferReadable(stream *Stream) {
	if !stream.readable || stream.scheduled || stream.readableBlocked {
		return
	}
	stream.readableBlocked = true
	state.readableBlocked = append(state.readableBlocked, stream)
}

func (state *driverState) scheduleBlockedReadable() {
	for len(state.readableBlocked) != 0 {
		stream := state.readableBlocked[0]
		state.readableBlocked[0] = nil
		state.readableBlocked = state.readableBlocked[1:]
		stream.readableBlocked = false
		if !stream.readable || stream.scheduled {
			continue
		}
		state.scheduleReadable(stream)
		return
	}
}

func (state *driverState) drainReadable() {
	for len(state.readable) != 0 {
		stream := state.readable[0]
		state.readable[0] = nil
		state.readable = state.readable[1:]
		stream.scheduled = false
		if !stream.readable {
			continue
		}
		if stream.readWaiter == nil && state.queuedReceiveCount >= state.engine.maxQueuedCount {
			state.deferReadable(stream)
			return
		}
		body, err := state.engine.runtime.receiveFrame(stream.handle)
		if errors.Is(err, errNativeWouldBlock) || nativeStatusIs(err, -int(unix.ESTALE)) {
			// A transport may retire its local receive lookup as soon as it
			// commits the terminal event, before the driver dequeues that event.
			// Preserve the pending read until FIN or terminal delivery instead of
			// exposing that internal retirement race as a stream error.
			stream.readable = false
			if stream.terminalPending {
				state.finishTerminalReceive(stream)
			} else {
				state.finishReadWaiter(stream)
			}
			continue
		}
		if err != nil {
			stream.readable = false
			if stream.terminalPending {
				if !stream.receiveDone {
					stream.terminalErr = err
				}
				state.finishTerminalReceive(stream)
			} else {
				stream.receiveDone = true
				stream.receiveErr = err
				state.finishReadWaiter(stream)
			}
			continue
		}

		state.deliverReceivedBody(stream, body)
		if state.queuedReceiveBytes >= state.engine.maxQueuedBytes {
			state.deferReadable(stream)
			return
		}
		state.scheduleReadable(stream)
	}
}

func (state *driverState) deliverReceivedBody(stream *Stream, body []byte) {
	if stream.readWaiter != nil {
		waiter := stream.readWaiter
		stream.readWaiter = nil
		waiter <- receiveResult{body: body}
		return
	}
	stream.queuedBodies = append(stream.queuedBodies, body)
	stream.queuedBytes += uint64(len(body))
	stream.queuedAccounted = true
	state.queuedReceiveCount++
	state.queuedReceiveBytes += uint64(len(body))
}

func (state *driverState) drainTerminalReceive(stream *Stream) {
	for stream.terminalPending {
		body, err := state.engine.runtime.receiveFrame(stream.handle)
		if errors.Is(err, errNativeWouldBlock) ||
			nativeStatusIs(err, -int(unix.ESTALE)) {
			stream.readable = false
			state.finishTerminalReceive(stream)
			return
		}
		if err != nil {
			stream.readable = false
			if !stream.receiveDone {
				stream.terminalErr = err
			}
			state.finishTerminalReceive(stream)
			return
		}
		if !stream.readCancelled {
			state.deliverReceivedBody(stream, body)
		}
	}
}

func (state *driverState) detachQueuedReceiveAccounting(stream *Stream) {
	if !stream.queuedAccounted {
		return
	}
	state.queuedReceiveCount -= uint32(len(stream.queuedBodies))
	state.queuedReceiveBytes -= stream.queuedBytes
	stream.queuedAccounted = false
	state.scheduleBlockedReadable()
}

func (state *driverState) discardQueuedBodies(stream *Stream) {
	state.detachQueuedReceiveAccounting(stream)
	stream.queuedBodies = nil
	stream.queuedBytes = 0
}

func (state *driverState) finishTerminalReceive(stream *Stream) {
	stream.terminalPending = false
	if !stream.receiveDone {
		stream.receiveDone = true
		stream.receiveErr = stream.terminalErr
	}
	state.detachQueuedReceiveAccounting(stream)
	state.finishReadWaiter(stream)
	delete(state.streams, stream.handle)
	state.releaseHandle(stream.handle, transportObjectStream)
}

func (state *driverState) releaseHandle(handle nativeHandle, kind uint32) {
	key := handleReleaseKey{handle: handle, kind: kind}
	err := state.engine.runtime.releaseHandle(handle, kind)
	if err == nil || nativeStatusIs(err, -int(unix.ESTALE)) {
		delete(state.pendingReleases, key)
		return
	}
	if state.pendingReleases == nil {
		state.pendingReleases = make(map[handleReleaseKey]struct{})
	}
	state.pendingReleases[key] = struct{}{}
	state.terminalErr = errors.Join(state.terminalErr, err)
}

func (state *driverState) retryPendingReleases() {
	for key := range state.pendingReleases {
		err := state.engine.runtime.releaseHandle(key.handle, key.kind)
		if err == nil || nativeStatusIs(err, -int(unix.ESTALE)) {
			delete(state.pendingReleases, key)
		}
	}
}

func nativeStatusIs(err error, status int) bool {
	var nativeErr *StatusError
	return errors.As(err, &nativeErr) && nativeErr.Status == status
}

func (state *driverState) finishReadWaiter(stream *Stream) {
	if stream.readWaiter == nil || len(stream.queuedBodies) != 0 {
		return
	}
	if !stream.receiveDone && stream.readable {
		return
	}
	if !stream.receiveDone {
		return
	}
	waiter := stream.readWaiter
	stream.readWaiter = nil
	if stream.receiveErr != nil {
		waiter <- receiveResult{err: stream.receiveErr}
	} else {
		waiter <- receiveResult{err: io.EOF}
	}
}

func (state *driverState) startShutdown(cause error) {
	if state.released {
		return
	}
	if cause != nil {
		state.terminalErr = errors.Join(state.terminalErr, cause)
	}
	if state.closingStarted {
		return
	}
	state.closingStarted = true
	state.engine.closing.Store(true)
	state.failPendingAdmissions(503)
	if err := state.engine.runtime.beginClose(); err != nil {
		state.terminalErr = errors.Join(state.terminalErr, err)
	}
}

func (state *driverState) finishStopped() {
	if state.released {
		state.engine.setCloseError(state.terminalErr)
		return
	}
	state.engine.commandAdmission.Lock()
	state.engine.closing.Store(true)
	state.engine.processCommandsLocked(state)
	state.engine.commandAdmission.Unlock()
	state.failPendingAdmissions(503)
	state.failRemaining()
	state.retryPendingReleases()
	if len(state.pendingReleases) != 0 {
		startRuntimeReaperWithReport(
			state.engine.runtime,
			state.pendingReleases,
			state.engine.setCloseError,
		)
		state.pendingReleases = nil
		state.released = true
		state.engine.setCloseError(state.terminalErr)
		return
	}
	if err := state.engine.runtime.waitDrained(); err != nil {
		state.terminalErr = errors.Join(state.terminalErr, err)
	}
	consumed, err := state.engine.runtime.release()
	if err != nil {
		state.terminalErr = errors.Join(state.terminalErr, err)
	}
	if !consumed {
		startRuntimeReaperWithReport(
			state.engine.runtime,
			nil,
			state.engine.setCloseError,
		)
	}
	state.released = true
	state.engine.setCloseError(state.terminalErr)
}

func (state *driverState) failRemaining() {
	err := state.terminalErr
	if err == nil {
		err = errEngineClosed
	}
	for operationID, pending := range state.pendingDials {
		pending.result <- connectionResult{err: err}
		delete(state.pendingDials, operationID)
	}
	for operationID, pending := range state.pendingOpens {
		pending.result <- streamResult{err: err}
		delete(state.pendingOpens, operationID)
	}
	for operationID, pending := range state.pendingSends {
		pending <- err
		delete(state.pendingSends, operationID)
	}
	for _, listener := range state.listeners {
		listener.lifecycle.finish(err)
		listener.accept.close(err)
		state.releaseHandle(listener.handle, transportObjectListener)
	}
	for _, connection := range state.connections {
		connection.lifecycle.finish(err)
		connection.accept.close(err)
		state.releaseHandle(connection.handle, transportObjectConnection)
	}
	for _, stream := range state.streams {
		stream.receiveDone = true
		stream.receiveErr = err
		state.finishReadWaiter(stream)
		stream.writer.fail(err)
		stream.lifecycle.finish(err)
		stream.cancel(err)
		state.releaseHandle(stream.handle, transportObjectStream)
	}
	clear(state.listeners)
	clear(state.connections)
	clear(state.streams)
	clear(state.earlySendStops)
}

func newConnection(engine *Engine, handle nativeHandle, config EndpointConfig, server bool) *Connection {
	local := transportinternal.Address{}
	remote := endpointAddress(config.Host, config.Port)
	if server {
		local, remote = remote, transportinternal.Address{}
	}
	protocol := transportinternal.ProtocolNativeQUIC
	negotiatedProtocol := string(config.ALPN)
	switch config.Protocol {
	case ProtocolNative, ProtocolMultiplexed:
		if negotiatedProtocol == "" {
			negotiatedProtocol = "trevrpc/1"
		}
	case ProtocolHTTP3:
		protocol = transportinternal.ProtocolHTTP3
		if negotiatedProtocol == "" {
			negotiatedProtocol = "h3"
		}
	case ProtocolWebTransport:
		protocol = transportinternal.ProtocolWebTransport
		if negotiatedProtocol == "" {
			negotiatedProtocol = "h3"
		}
	}
	return &Connection{
		engine: engine,
		handle: handle,
		info: transportinternal.ConnectionInfo{
			RequestedBackend:    engine.requestedBackend,
			ResolvedBackend:     transportinternal.BackendNative,
			Protocol:            protocol,
			LocalAddress:        local,
			RemoteAddress:       remote,
			NegotiatedProtocol:  negotiatedProtocol,
			Provider:            "msquic",
			TransportABIVersion: engine.runtime.transportABIVersion(),
		},
		maxFrameSize: config.MaxFrameSize,
		accept:       newObjectQueue[*Stream](),
		lifecycle:    newLifecycle(),
	}
}

func newStream(engine *Engine, handle nativeHandle, info transportinternal.ConnectionInfo, maxFrameSize uint64) *Stream {
	ctx, cancel := context.WithCancelCause(context.Background())
	stream := &Stream{
		engine:          engine,
		handle:          handle,
		info:            info,
		lifecycle:       newLifecycle(),
		context:         ctx,
		cancel:          cancel,
		deadlineChanged: make(chan struct{}),
		readCancelDone:  make(chan struct{}),
		writeCancelDone: make(chan struct{}),
	}
	stream.reader = frameReader{maxFrameSize: maxFrameSize, receive: stream.receiveFrame}
	stream.writer = frameWriter{maxFrameSize: maxFrameSize, send: stream.sendFrame}
	return stream
}

func endpointAddress(host string, port uint16) transportinternal.Address {
	return transportinternal.Address{Network: "udp", Host: host, Port: port}
}

func streamCancellationError(reason transportinternal.CloseReason, fallback string) error {
	if reason.Err != nil {
		return reason.Err
	}
	message := reason.Message
	if message == "" {
		message = fallback
	}
	return &EventError{
		ApplicationErrorCode: reason.ApplicationCode,
		ProviderErrorCode:    reason.TransportCode,
		Local:                true,
		TransportError:       reason.TransportCode != 0,
		Clean:                reason.Clean,
		Message:              message,
	}
}

func eventError(event nativeEvent, fallback string) error {
	message := string(event.data)
	if message == "" {
		message = fallback
	}
	return &EventError{
		Status:               event.status,
		ProviderErrorCode:    event.providerErrorCode,
		ApplicationErrorCode: event.applicationErrorCode,
		Local:                event.flags&eventFlagLocal != 0,
		Peer:                 event.flags&eventFlagPeer != 0,
		PeerReset:            event.flags&eventFlagPeerReset != 0,
		TransportError:       event.flags&eventFlagTransportError != 0,
		Clean: event.flags&eventFlagCleanFIN != 0 ||
			(event.status == 0 && event.applicationErrorCode == 0 && event.providerErrorCode == 0),
		Message: message,
	}
}

func (l *Listener) Accept(ctx context.Context) (transportinternal.Connection, error) {
	connection, err := l.accept.pop(ctx)
	if err != nil {
		return nil, err
	}
	return connection, nil
}

func (l *Listener) Address() transportinternal.Address {
	return l.address
}

func (l *Listener) Close() error {
	if l == nil {
		return nil
	}
	l.closeOnce.Do(func() {
		result := make(chan error, 1)
		l.engine.enqueueCleanup(func(*driverState) {
			err := l.engine.runtime.closeListener(l.handle)
			if nativeStatusIs(err, -int(unix.ESTALE)) {
				err = nil
			}
			result <- err
		})
		select {
		case l.closeErr = <-result:
		case <-l.engine.done:
			l.closeErr = l.engine.Err()
		}
	})
	return l.closeErr
}

func (c *Connection) OpenStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	result := make(chan streamResult, 1)
	if err := c.engine.enqueue(ctx, func(state *driverState) {
		operationID := state.nextID()
		handle, err := c.engine.runtime.openStream(c.handle, operationID)
		if err != nil {
			result <- streamResult{err: err}
			return
		}
		stream := newStream(c.engine, handle, c.info, c.maxFrameSize)
		state.streams[handle] = stream
		state.pendingOpens[operationID] = pendingOpen{handle: handle, result: result}
	}); err != nil {
		return nil, err
	}
	select {
	case value := <-result:
		return value.stream, value.err
	case <-ctx.Done():
		c.abortPendingOpen(result)
		discardStreamResult(result, c.engine.done)
		return nil, ctx.Err()
	case <-c.lifecycle.Done():
		c.abortPendingOpen(result)
		discardStreamResult(result, c.engine.done)
		return nil, c.lifecycle.Err()
	case <-c.engine.done:
		return nil, c.engine.stoppedError()
	}
}

func (c *Connection) abortPendingOpen(result chan streamResult) {
	c.engine.enqueueCleanup(func(state *driverState) {
		for _, pending := range state.pendingOpens {
			if pending.result != result {
				continue
			}
			_ = c.engine.runtime.abortStream(pending.handle, 1)
			return
		}
	})
}

func (c *Connection) AcceptStream(ctx context.Context) (transportinternal.BidirectionalStream, error) {
	stream, err := c.accept.pop(ctx)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (c *Connection) Done() <-chan struct{} {
	return c.lifecycle.Done()
}

func (c *Connection) Err() error {
	return c.lifecycle.Err()
}

func (c *Connection) Info() transportinternal.ConnectionInfo {
	return c.info
}

func (c *Connection) Close(reason transportinternal.CloseReason) error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		result := make(chan error, 1)
		c.engine.enqueueCleanup(func(*driverState) {
			err := c.engine.runtime.closeConnection(c.handle, reason.ApplicationCode)
			if nativeStatusIs(err, -int(unix.ESTALE)) {
				err = nil
			}
			result <- err
		})
		select {
		case c.closeErr = <-result:
		case <-c.engine.done:
			c.closeErr = c.engine.Err()
		}
	})
	return c.closeErr
}

func (s *Stream) Read(data []byte) (int, error) {
	return s.reader.Read(data)
}

func (s *Stream) Write(data []byte) (int, error) {
	return s.writer.Write(data)
}

func (s *Stream) SetReadDeadline(deadline time.Time) error {
	if s == nil {
		return errEngineClosed
	}
	s.readDeadlineMu.Lock()
	s.readDeadline = deadline
	close(s.deadlineChanged)
	s.deadlineChanged = make(chan struct{})
	s.readDeadlineMu.Unlock()
	return nil
}

func (s *Stream) readDeadlineState() (time.Time, <-chan struct{}) {
	s.readDeadlineMu.Lock()
	defer s.readDeadlineMu.Unlock()
	return s.readDeadline, s.deadlineChanged
}

func (s *Stream) Close() error {
	if s == nil {
		return nil
	}
	s.closeOnce.Do(func() {
		framingErr := s.writer.finish()
		result := make(chan error, 1)
		s.engine.enqueueCleanup(func(state *driverState) {
			if framingErr != nil {
				if s.writeCancelled.Load() {
					result <- nil
				} else {
					result <- s.engine.runtime.abortStream(s.handle, 1)
				}
				return
			}
			err := s.engine.runtime.finishStreamSend(s.handle)
			if err == nil {
				s.sendDone = true
			}
			result <- err
		})
		select {
		case nativeErr := <-result:
			s.closeErr = errors.Join(framingErr, nativeErr)
		case <-s.engine.done:
			s.closeErr = errors.Join(framingErr, s.engine.Err())
		}
	})
	return s.closeErr
}

func (s *Stream) Context() context.Context {
	return s.context
}

func (s *Stream) CancelRead(reason transportinternal.CloseReason) {
	if s == nil {
		return
	}
	s.cancelReadOnce.Do(func() {
		s.readCancelErr = streamCancellationError(reason, "native stream receive canceled")
		s.reader.cancel(s.readCancelErr)
		close(s.readCancelDone)

		result := make(chan struct{}, 1)
		s.engine.enqueueCleanup(func(state *driverState) {
			nativeErr := s.engine.runtime.abortStreamRead(s.handle, reason.ApplicationCode)
			if nativeStatusIs(nativeErr, -int(unix.ESTALE)) {
				nativeErr = nil
			}
			s.readCancelled = true
			s.receiveDone = true
			s.receiveErr = errors.Join(s.readCancelErr, nativeErr)
			if nativeErr != nil {
				state.startShutdown(nativeErr)
			}
			s.readable = false
			state.discardQueuedBodies(s)
			state.finishReadWaiter(s)
			result <- struct{}{}
		})
		select {
		case <-result:
		case <-s.engine.done:
		}
	})
}

func (s *Stream) CancelWrite(reason transportinternal.CloseReason) {
	if s == nil {
		return
	}
	s.cancelWriteOnce.Do(func() {
		s.writeCancelErr = streamCancellationError(reason, "native stream send canceled")
		s.writeCancelled.Store(true)
		s.writer.fail(s.writeCancelErr)
		close(s.writeCancelDone)

		result := make(chan struct{}, 1)
		s.engine.enqueueCleanup(func(state *driverState) {
			nativeErr := s.engine.runtime.abortStreamWrite(s.handle, reason.ApplicationCode)
			if nativeStatusIs(nativeErr, -int(unix.ESTALE)) {
				nativeErr = nil
			}
			if nativeErr != nil {
				state.startShutdown(nativeErr)
			}
			s.sendDone = true
			result <- struct{}{}
		})
		select {
		case <-result:
		case <-s.engine.done:
		}
	})
}

func (s *Stream) sendFrame(body []byte) error {
	for {
		select {
		case <-s.writeCancelDone:
			return s.writeCancelErr
		default:
		}
		capacity := s.engine.capacityWaiter()
		result := make(chan error, 1)
		if err := s.engine.enqueue(context.Background(), func(state *driverState) {
			select {
			case <-s.writeCancelDone:
				result <- s.writeCancelErr
				return
			default:
			}
			operationID := state.nextID()
			err := s.engine.runtime.sendFrame(s.handle, operationID, body)
			if err != nil {
				result <- err
				return
			}
			state.pendingSends[operationID] = result
		}); err != nil {
			return err
		}
		select {
		case err := <-result:
			if !errors.Is(err, errNativeWouldBlock) {
				return err
			}
		case <-s.writeCancelDone:
			return s.writeCancelErr
		case <-s.lifecycle.Done():
			return s.lifecycle.Err()
		case <-s.engine.done:
			return s.engine.stoppedError()
		}
		select {
		case <-capacity:
		case <-s.writeCancelDone:
			return s.writeCancelErr
		case <-s.lifecycle.Done():
			return s.lifecycle.Err()
		case <-s.engine.done:
			return s.engine.stoppedError()
		}
	}
}

func (s *Stream) receiveFrame() ([]byte, error) {
	select {
	case <-s.readCancelDone:
		return nil, s.readCancelErr
	default:
	}
	result := make(chan receiveResult, 1)
	if err := s.engine.enqueue(context.Background(), func(state *driverState) {
		select {
		case <-s.readCancelDone:
			result <- receiveResult{err: s.readCancelErr}
			return
		default:
		}
		if len(s.queuedBodies) != 0 {
			body := s.queuedBodies[0]
			s.queuedBodies[0] = nil
			s.queuedBodies = s.queuedBodies[1:]
			s.queuedBytes -= uint64(len(body))
			if s.queuedAccounted {
				state.queuedReceiveCount--
				state.queuedReceiveBytes -= uint64(len(body))
			}
			result <- receiveResult{body: body}
			state.scheduleBlockedReadable()
			state.scheduleReadable(s)
			state.drainReadable()
			return
		}
		if s.readable {
			s.readWaiter = result
			state.scheduleReadable(s)
			state.drainReadable()
			return
		}
		if s.receiveDone {
			if s.receiveErr != nil {
				result <- receiveResult{err: s.receiveErr}
			} else {
				result <- receiveResult{err: io.EOF}
			}
			return
		}
		s.readWaiter = result
	}); err != nil {
		return nil, err
	}
	for {
		deadline, changed := s.readDeadlineState()
		if deadline.IsZero() {
			select {
			case value := <-result:
				return value.body, value.err
			case <-s.readCancelDone:
				return nil, s.readCancelErr
			case <-changed:
				continue
			case <-s.engine.done:
				return nil, s.engine.stoppedError()
			}
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			select {
			case <-s.readCancelDone:
				return nil, s.readCancelErr
			default:
			}
			return s.finishReadDeadline(result)
		}
		timer := time.NewTimer(remaining)
		select {
		case value := <-result:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return value.body, value.err
		case <-s.readCancelDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, s.readCancelErr
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
			return s.finishReadDeadline(result)
		case <-s.engine.done:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return nil, s.engine.stoppedError()
		}
	}
}

func (s *Stream) finishReadDeadline(result chan receiveResult) ([]byte, error) {
	settled := make(chan readDeadlineResult, 1)
	s.engine.enqueueCleanup(func(*driverState) {
		select {
		case value := <-result:
			settled <- readDeadlineResult{receive: value, completed: true}
		default:
			if s.readWaiter == result {
				s.readWaiter = nil
			}
			settled <- readDeadlineResult{}
		}
	})
	select {
	case value := <-settled:
		if value.completed {
			return value.receive.body, value.receive.err
		}
		return nil, os.ErrDeadlineExceeded
	case <-s.engine.done:
		return nil, s.engine.stoppedError()
	}
}
