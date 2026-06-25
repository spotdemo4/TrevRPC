package trevrpc

// Response is a decoded unary response together with response metadata.
type Response[T ProtoMessage] struct {
	Message  T
	Metadata Metadata
}

// ResponseOption mutates a response envelope.
type ResponseOption[T ProtoMessage] func(*Response[T])

// NewResponse creates a response envelope for handlers that need metadata.
func NewResponse[T ProtoMessage](message T, options ...ResponseOption[T]) *Response[T] {
	response := &Response[T]{Message: message, Metadata: Metadata{}}
	for _, option := range options {
		option(response)
	}
	return response
}

// WithResponseMetadata sets one normalized response metadata entry.
func WithResponseMetadata[T ProtoMessage](key string, value []byte) ResponseOption[T] {
	return func(response *Response[T]) {
		if response.Metadata == nil {
			response.Metadata = Metadata{}
		}
		response.Metadata[NormalizeMetadataKey(key)] = append([]byte(nil), value...)
	}
}

// WithResponseMetadataMap replaces response metadata.
func WithResponseMetadataMap[T ProtoMessage](metadata Metadata) ResponseOption[T] {
	return func(response *Response[T]) {
		response.Metadata = cloneMetadata(metadata)
	}
}
