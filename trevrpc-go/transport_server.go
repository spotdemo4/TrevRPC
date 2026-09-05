package trevrpc

import (
	"context"
	"errors"
	"sync"
	"time"

	transportinternal "trev.zip/llc/trevrpc/trevrpc-go/internal/transport"
)

type transportConnectionHandler func(
	context.Context,
	transportinternal.Connection,
	*Server,
	semaphore,
)

func serveTransportListener(
	ctx context.Context,
	listener transportinternal.Listener,
	server *Server,
	handleConnection transportConnectionHandler,
	listenerClosed func(error) bool,
	mapStatus func(error) error,
) error {
	runtime := server.freeze()
	connectionLimit := runtime.connectionLimit
	requestLimit := runtime.requestLimit
	connectionsCtx, stopConnections := context.WithCancel(context.Background())
	defer stopConnections()

	var connectionTasks sync.WaitGroup
	var activeMu sync.Mutex
	active := map[transportinternal.Connection]struct{}{}

	closeActiveConnections := func(message string) {
		activeMu.Lock()
		connections := make([]transportinternal.Connection, 0, len(active))
		for conn := range active {
			connections = append(connections, conn)
		}
		activeMu.Unlock()
		for _, conn := range connections {
			_ = conn.Close(TransportCloseReason{
				Local:   true,
				Message: message,
			})
		}
	}

	listenerDone := make(chan struct{})
	defer close(listenerDone)
	go func() {
		select {
		case <-ctx.Done():
			stopConnections()
			_ = listener.Close()
		case <-listenerDone:
		}
	}()

	for {
		conn, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil ||
				(listenerClosed != nil && listenerClosed(err)) {
				break
			}

			stopConnections()
			closeActiveConnections("server accept failed")
			waitForWaitGroup(
				&connectionTasks,
				runtime.options.GracefulShutdownTimeout,
				func() {
					runtime.emitDiagnostic(ServerDiagnostic{
						Phase: ServerDiagnosticShutdownIncomplete,
					})
					closeActiveConnections(
						"server connection drain timed out",
					)
				},
			)
			waitForRuntimeExecutions(runtime)
			if mapStatus != nil {
				return mapStatus(err)
			}
			return transportStatus(err)
		}

		if !tryAcquire(connectionLimit) {
			_ = conn.Close(TransportCloseReason{
				Local:   true,
				Message: "too many concurrent connections",
			})
			continue
		}

		activeMu.Lock()
		active[conn] = struct{}{}
		activeMu.Unlock()
		connectionTasks.Go(func() {
			defer release(connectionLimit)
			defer func() {
				activeMu.Lock()
				delete(active, conn)
				activeMu.Unlock()
			}()
			handleConnection(
				connectionsCtx,
				conn,
				server,
				requestLimit,
			)
		})
	}

	stopConnections()
	waitForWaitGroup(
		&connectionTasks,
		runtime.options.GracefulShutdownTimeout,
		func() {
			runtime.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticShutdownIncomplete,
			})
			closeActiveConnections(
				"server graceful shutdown timed out",
			)
		},
	)
	waitForRuntimeExecutions(runtime)
	return nil
}

func handleTransportConnection(
	ctx context.Context,
	conn transportinternal.Connection,
	server *Server,
	requestLimit semaphore,
	closeOnShutdown bool,
) {
	if conn.Info().NegotiatedProtocol != ALPN {
		_ = conn.Close(TransportCloseReason{
			Local:   true,
			Message: "unsupported ALPN",
		})
		return
	}

	handleTransportStreamEndpoint(
		ctx,
		conn,
		server,
		requestLimit,
		transportStreamEndpointOptions{
			overloadMessage:     "too many concurrent streams on connection",
			drainTimeoutMessage: "server stream drain timed out",
			closeOnShutdown:     closeOnShutdown,
			shutdownMessage:     "server drained connection",
		},
	)
}

type transportStreamEndpointOptions struct {
	overloadMessage          string
	drainTimeoutMessage      string
	shutdownMessage          string
	closeOnShutdown          bool
	closeImmediatelyOnCancel bool
	onAcceptError            func(error)
}

func handleTransportStreamEndpoint(
	ctx context.Context,
	endpoint transportinternal.StreamEndpoint,
	server *Server,
	requestLimit semaphore,
	options transportStreamEndpointOptions,
) {
	runtime := server.freeze()
	streamLimit := newSemaphore(
		runtime.options.MaxConcurrentStreamsPerConnection,
	)
	var streamTasks sync.WaitGroup

	endpointDone := make(chan struct{})
	defer close(endpointDone)
	if options.closeImmediatelyOnCancel {
		go func() {
			select {
			case <-ctx.Done():
				_ = endpoint.Close(TransportCloseReason{
					Local:   true,
					Clean:   true,
					Message: options.shutdownMessage,
				})
			case <-endpoint.Done():
			case <-endpointDone:
			}
		}()
	}

	for {
		stream, err := endpoint.AcceptStream(ctx)
		if err != nil {
			if options.onAcceptError != nil {
				options.onAcceptError(err)
			}
			break
		}
		if !tryAcquire(streamLimit) {
			writeStatusResponse(
				stream,
				Unavailable(options.overloadMessage),
				runtime.options.MaxFrameSize,
			)
			continue
		}

		streamTasks.Go(func() {
			defer release(streamLimit)
			streamCtx := context.Background()
			if contextual, ok := stream.(transportinternal.StreamContext); ok {
				streamCtx = contextual.Context()
			}
			streamCtx, cancel := contextWithAdditionalCancel(streamCtx, ctx)
			defer cancel()
			handleRPCStream(
				streamCtx,
				server,
				requestLimit,
				newTransportRPCStream(stream),
			)
		})
	}

	waitForWaitGroup(
		&streamTasks,
		runtime.options.GracefulShutdownTimeout,
		func() {
			runtime.emitDiagnostic(ServerDiagnostic{
				Phase: ServerDiagnosticShutdownIncomplete,
			})
			_ = endpoint.Close(TransportCloseReason{
				Local:   true,
				Message: options.drainTimeoutMessage,
			})
		},
	)
	if options.closeOnShutdown && ctx.Err() != nil {
		_ = endpoint.Close(TransportCloseReason{
			Local:   true,
			Clean:   true,
			Message: options.shutdownMessage,
		})
	}
}

func newTransportRPCStream(
	stream transportinternal.BidirectionalStream,
) rpcStream {
	base := transportRPCStream{stream: stream}
	if _, ok := stream.(transportinternal.ReadCanceler); ok {
		return cancellableTransportRPCStream{transportRPCStream: base}
	}
	return base
}

type transportRPCStream struct {
	stream transportinternal.BidirectionalStream
}

func (s transportRPCStream) Read(data []byte) (int, error) {
	return s.stream.Read(data)
}

func (s transportRPCStream) Write(data []byte) (int, error) {
	return s.stream.Write(data)
}

func (s transportRPCStream) Close() error {
	return s.stream.Close()
}

func (s transportRPCStream) SetReadDeadline(deadline time.Time) error {
	setter, ok := s.stream.(transportinternal.ReadDeadlineSetter)
	if !ok {
		return errors.New("transport stream does not support read deadlines")
	}
	return setter.SetReadDeadline(deadline)
}

type cancellableTransportRPCStream struct {
	transportRPCStream
}

func (s cancellableTransportRPCStream) trevrpcCancelRead() {
	cancelTransportStreamRead(s.stream)
}

func (s cancellableTransportRPCStream) trevrpcCancelReadOnContext(
	ctx context.Context,
) func() {
	return watchContextCancellation(ctx, func() {
		cancelTransportStreamRead(s.stream)
	})
}
