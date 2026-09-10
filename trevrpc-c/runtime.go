package trevrpcc

const (
	EngineABIVersion    uint32 = 1
	TransportABIVersion uint32 = 1

	WakeSourcePOSIXFD      uint32 = 1
	WakeFlagBorrowed       uint32 = 1 << 0
	WakeFlagLevelTriggered uint32 = 1 << 1
)

const (
	ObjectNone       uint32 = 0
	ObjectListener   uint32 = 1
	ObjectConnection uint32 = 2
	ObjectStream     uint32 = 3
)

const (
	EventDiagnostic            uint32 = 1
	EventStopped               uint32 = 2
	EventListenerStopped       uint32 = 3
	EventConnectionReady       uint32 = 4
	EventConnectionFailed      uint32 = 5
	EventConnectionClosed      uint32 = 6
	EventStreamReady           uint32 = 7
	EventStreamFailed          uint32 = 8
	EventStreamReadable        uint32 = 9
	EventReceiveFIN            uint32 = 10
	EventSendComplete          uint32 = 11
	EventStreamClosed          uint32 = 12
	EventHTTP3Admission        uint32 = 13
	EventWebTransportAdmission uint32 = 14
	EventSendStopped           uint32 = 15
)

const (
	EventFlagFatal          uint32 = 1 << 0
	EventFlagTerminal       uint32 = 1 << 1
	EventFlagClient         uint32 = 1 << 2
	EventFlagServer         uint32 = 1 << 3
	EventFlagLocal          uint32 = 1 << 4
	EventFlagPeer           uint32 = 1 << 5
	EventFlagPeerReset      uint32 = 1 << 6
	EventFlagTransportError uint32 = 1 << 7
	EventFlagCleanFIN       uint32 = 1 << 8
)

type WakeSource struct {
	Kind         uint32
	Flags        uint32
	NativeHandle uintptr
}

func (w WakeSource) Borrowed() bool {
	return w.Flags&WakeFlagBorrowed != 0
}

func (w WakeSource) LevelTriggered() bool {
	return w.Flags&WakeFlagLevelTriggered != 0
}

type Handle struct {
	Owner      uint64
	Slot       uint32
	Generation uint32
}

// Admission owns a deferred native admission event until Respond is called.
// Request returns detached Go values that remain valid after the native event is
// released. Respond is idempotent; callers should always respond exactly once.
type Admission interface {
	Request() AdmissionRequest
	Respond(httpStatus uint16) error
}

// Event contains detached Go-owned data. Data and values returned through
// Admission.Request remain valid after the next Runtime operation.
type Event struct {
	Kind                 uint32
	Flags                uint32
	Status               int
	SubjectKind          uint32
	Sequence             uint64
	Subject              Handle
	Parent               Handle
	OperationID          uint64
	ApplicationErrorCode uint64
	ProviderErrorCode    uint64
	Protocol             uint32
	Data                 []byte
	Admission            Admission
}

type Diagnostics struct {
	ABIVersion            uint32
	State                 uint32
	TerminalStatus        int
	EventCapacity         uint32
	QueueDepth            uint32
	OrdinaryQueueDepth    uint32
	EventsEnqueued        uint64
	EventsDequeued        uint64
	EventsRejected        uint64
	ReceiveOwnedCount     uint64
	PeakReceiveOwnedCount uint64
	ReceiveOwnedBytes     uint64
	PeakReceiveOwnedBytes uint64
	PendingSendBytes      uint64
	PendingSendCount      uint64
	LiveListeners         uint64
	LiveConnections       uint64
	LiveStreams           uint64
	ActiveCallbacks       uint64
	ActiveAPICalls        uint64
	WakeSignals           uint64
	WakeWriteWouldBlock   uint64
	WakeFailures          uint64
	ProviderErrorCode     uint64
	MandatoryReservations uint64
}

// Runtime is an independently owned low-level native transport runtime.
// Slices returned by WakeSources, NextEvent, and ReceiveFrame are detached
// Go-owned values; callers may retain or modify them after the call returns.
type Runtime interface {
	WakeSources() ([]WakeSource, error)
	PollTimeoutMilliseconds() int
	ABIVersion() uint32
	NextEvent() (Event, bool, error)
	Diagnostics() (Diagnostics, error)
	Listen(EndpointConfig) (Handle, error)
	ListenerPort(Handle) (uint16, error)
	Dial(EndpointConfig, uint64) (Handle, error)
	CancelDial(Handle) error
	OpenStream(Handle, uint64) (Handle, error)
	SendFrame(Handle, uint64, []byte) error
	ReceiveFrame(Handle) ([]byte, error)
	FinishStreamSend(Handle) error
	AbortStreamRead(Handle, uint64) error
	AbortStreamWrite(Handle, uint64) error
	AbortStream(Handle, uint64) error
	CloseStream(Handle) error
	CloseConnection(Handle, uint64) error
	CloseListener(Handle) error
	ReleaseHandle(Handle, uint32) error
	BeginClose() error
	WaitDrained() error
	Release() (consumed bool, err error)
}

type EngineRuntime interface {
	Runtime
}

type TransportRuntime interface {
	Runtime
}
