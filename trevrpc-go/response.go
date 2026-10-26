package trevrpc

import (
	"context"
	"errors"
	"sync"
)

// ResponseStream is a MessageStream that also reports the stream's terminal
// status, and with it the trailing metadata. Clients see the terminal status
// of a streaming call through TerminalStatus.
type ResponseStream[T ProtoMessage] interface {
	MessageStream[T]
	// TerminalStatus returns the final status once the stream has ended, after
	// Recv has returned io.EOF or the server's status error.
	TerminalStatus() (*Status, bool)
}

var (
	// ErrNoHandlerContext is returned when response metadata is set on a
	// context that does not belong to a call being handled.
	ErrNoHandlerContext = errors.New("trevrpc: context does not belong to a handled call")
	// ErrResponseHeadersSent is returned when a response header is set after
	// the headers have gone out: once the handler has returned, or from the
	// start on a route whose requests may be stream-compressed.
	ErrResponseHeadersSent = errors.New("trevrpc: response headers already sent")
	// ErrResponseMetadataSent is returned when trailing metadata is set after
	// the final status has gone out.
	ErrResponseMetadataSent = errors.New("trevrpc: response metadata already sent")
)

// SetResponseHeader sets a response header of the call that ctx, a handler's
// context, belongs to. Headers are sent in a HEADERS frame ahead of the first
// response message, as soon as the handler returns; clients read them with
// CaptureResponseHeaders or the Headers method of a response stream. The key
// is normalized with NormalizeMetadataKey and the value is copied. A failed
// call sends no headers.
//
// A route whose request messages may be stream-compressed (RouteStreamCompression
// with Requests on a client-streaming or bidirectional method whose requests
// are not Unreliable) sends HEADERS at dispatch, before the handler runs, so
// that the client learns the answer while it is still uploading. Its handlers
// cannot set response headers: SetResponseHeader returns
// ErrResponseHeadersSent from the start, on every call. They send trailing
// metadata with SetResponseMetadata instead.
func SetResponseHeader(ctx context.Context, key string, value []byte) error {
	response := responseMetadataFromContext(ctx)
	if response == nil {
		return ErrNoHandlerContext
	}
	return response.set(&response.headers, &response.headersSent, ErrResponseHeadersSent, key, value)
}

// SetResponseMetadata sets trailing metadata of the call that ctx, a
// handler's context, belongs to. It is sent with the final OK status: when a
// unary or client-streaming handler returns, and when the response stream of
// a streaming handler ends, so the stream may still set it before its Recv
// returns io.EOF. Clients read it with CaptureResponseMetadata or the
// TerminalStatus of a response stream. A failed call sends the Metadata of its
// Status instead. The key is normalized with NormalizeMetadataKey and the
// value is copied.
func SetResponseMetadata(ctx context.Context, key string, value []byte) error {
	response := responseMetadataFromContext(ctx)
	if response == nil {
		return ErrNoHandlerContext
	}
	return response.set(&response.metadata, &response.metadataSent, ErrResponseMetadataSent, key, value)
}

// responseMetadata is the response headers and trailing metadata a handler
// sets through its context. The server takes the headers when the handler
// returns and the trailing metadata when the response ends; setting either
// afterwards fails. Handlers may set them from any goroutine.
type responseMetadata struct {
	mu           sync.Mutex
	headers      Metadata
	metadata     Metadata
	headersSent  bool
	metadataSent bool
}

func (r *responseMetadata) set(target *Metadata, sent *bool, sentErr error, key string, value []byte) error {
	key = NormalizeMetadataKey(key)
	if err := validateMetadataKey(key); err != nil {
		return err
	}
	var owned []byte
	if len(value) > 0 {
		owned = append([]byte(nil), value...)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if *sent {
		return sentErr
	}
	if *target == nil {
		*target = make(Metadata, 1)
	}
	previous, existed := (*target)[key]
	(*target)[key] = owned
	if err := validateMetadata(*target); err != nil {
		if existed {
			(*target)[key] = previous
		} else {
			delete(*target, key)
		}
		return err
	}
	return nil
}

// takeHeaders returns the headers to send and makes later ones fail.
func (r *responseMetadata) takeHeaders() Metadata {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headersSent = true
	return r.headers
}

// takeMetadata returns the trailing metadata to send with the final OK status
// and makes later metadata fail.
func (r *responseMetadata) takeMetadata() Metadata {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.headersSent = true
	r.metadataSent = true
	return r.metadata
}

// okStatus is the OK status of a call whose handler set metadata, taken from
// its responseMetadata: the values are copies the handler cannot change any
// more, so unlike OK().WithMetadata it does not copy them again.
func okStatus(metadata Metadata) *Status {
	if metadata == nil {
		metadata = Metadata{}
	}
	return &Status{Code: CodeOK, Metadata: metadata}
}

func cloneStatus(status *Status) *Status {
	if status == nil {
		return nil
	}
	return NewStatus(status.Code, status.Message).WithMetadata(status.Metadata)
}
