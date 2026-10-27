package trevrpc

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"trev.zip/llc/trevrpc/trevrpc-go/internal/zstd"
)

// Compression configures compressed messages: messages that travel as zstd
// frames compressed with a dictionary both peers hold. Dictionaries are
// negotiated per connection, so a message whose dictionary the peer lacks
// travels uncompressed. Compression calls libzstd and needs cgo; Dial and
// Listen fail when dictionaries are configured in a binary built without cgo.
//
// Without dictionaries, a peer still compresses the messages of methods marked
// for compression without a dictionary (see PlainCompression), at Level.
type Compression struct {
	// Dictionaries are the dictionaries this peer holds.
	Dictionaries []Dictionary
	// Level is the zstd compression level of outgoing messages; zero means 1.
	Level int
}

// Dictionary is a zstd dictionary and the protobuf message types it compresses.
type Dictionary struct {
	// Data is the dictionary in zstd's format, as written by `zstd --train`.
	// Its header carries the Dictionary_ID that identifies it to peers, which
	// must never name different dictionaries.
	Data []byte
	// Messages are the full names of the protobuf message types the
	// dictionary compresses, such as "example.v1.Position". A message type
	// may appear in one dictionary only. A dictionary without messages is
	// still offered to peers, which may compress with it, but this peer never
	// does; adding a new dictionary that way first lets receivers hold it
	// before senders use it.
	Messages []string
}

// compressionDict is one configured dictionary.
type compressionDict struct {
	*zstd.Dict
	// info names the dictionary in queries and answers.
	info *dictionaryInfo
	// accept is the accept_dictionaries list naming only this dictionary.
	accept []uint32
	// only is the set of only this dictionary.
	only *dictSet
	// acceptPlain and withPlain are accept and only with compression without
	// a dictionary added.
	acceptPlain []uint32
	withPlain   *dictSet
}

// plainDictID is the dictionary ID that stands for compression without a
// dictionary in negotiation; no trained dictionary has it.
const plainDictID = 0

// compressionConfig is a validated Compression.
type compressionConfig struct {
	// level is the zstd level of outgoing messages.
	level  int
	byID   map[uint32]*compressionDict
	byType map[protoreflect.FullName]*compressionDict
	// plain is compression without a dictionary, also in byID under
	// plainDictID.
	plain *compressionDict
	// dictionaries says that byID holds trained dictionaries besides plain.
	dictionaries bool
	// query lists every dictionary and plain, as a client's dictionary_query.
	query []*dictionaryInfo
}

// newCompressionConfig validates and digests options. Without cgo it returns
// nil when no dictionaries are configured, and an error otherwise; with cgo the
// configuration always has compression without a dictionary.
func newCompressionConfig(options *Compression) (*compressionConfig, error) {
	var dictionaries []Dictionary
	level := 1
	if options != nil {
		dictionaries = options.Dictionaries
		if options.Level != 0 {
			level = options.Level
		}
	}
	if !zstd.Available {
		if len(dictionaries) > 0 {
			return nil, zstd.ErrUnavailable
		}
		return nil, nil
	}
	plain := &compressionDict{
		Dict:   zstd.NewPlain(level),
		info:   &dictionaryInfo{ID: plainDictID},
		accept: []uint32{plainDictID},
	}
	plain.only = &dictSet{dicts: []*compressionDict{plain}}
	config := &compressionConfig{
		level:        level,
		byID:         map[uint32]*compressionDict{plainDictID: plain},
		byType:       make(map[protoreflect.FullName]*compressionDict),
		plain:        plain,
		dictionaries: len(dictionaries) > 0,
		query:        []*dictionaryInfo{plain.info},
	}
	for i, dictionary := range dictionaries {
		digested, err := zstd.NewDict(dictionary.Data, level)
		if err != nil {
			return nil, fmt.Errorf("trevrpc: compression dictionary %d: %w", i, err)
		}
		if _, ok := config.byID[digested.ID]; ok {
			return nil, fmt.Errorf("trevrpc: compression dictionaries share ID %d", digested.ID)
		}
		d := &compressionDict{
			Dict:        digested,
			info:        &dictionaryInfo{ID: digested.ID, Fingerprint: digested.Fingerprint[:]},
			accept:      []uint32{digested.ID},
			acceptPlain: []uint32{digested.ID, plainDictID},
		}
		d.only = &dictSet{dicts: []*compressionDict{d}}
		d.withPlain = &dictSet{dicts: []*compressionDict{d, plain}}
		config.byID[d.ID] = d
		config.query = append(config.query, d.info)
		for _, name := range dictionary.Messages {
			fullName := protoreflect.FullName(name)
			if !fullName.IsValid() {
				return nil, fmt.Errorf("trevrpc: compression dictionary %d: invalid message type %q", i, name)
			}
			if _, ok := config.byType[fullName]; ok {
				return nil, fmt.Errorf("trevrpc: message type %s is in two compression dictionaries", name)
			}
			config.byType[fullName] = d
		}
	}
	return config, nil
}

// dict returns the dictionary with Dictionary_ID id, or nil.
func (c *compressionConfig) dict(id uint32) *compressionDict {
	if c == nil {
		return nil
	}
	return c.byID[id]
}

// lookup implements dictionaryLookup: a server accepts every dictionary it
// holds, and messages without a dictionary.
func (c *compressionConfig) lookup(id uint32) *zstd.Dict {
	if d := c.dict(id); d != nil {
		return d.Dict
	}
	return nil
}

// matches reports whether info names d, ID and fingerprint alike.
func (d *compressionDict) matches(info *dictionaryInfo) bool {
	return info != nil && d.ID == info.ID && bytes.Equal(d.info.Fingerprint, info.Fingerprint)
}

// dictSet is a small set of dictionaries; calls use one or two.
type dictSet struct{ dicts []*compressionDict }

func (s *dictSet) has(d *compressionDict) bool {
	return s != nil && slices.Contains(s.dicts, d)
}

// lookup implements dictionaryLookup.
func (s *dictSet) lookup(id uint32) *zstd.Dict {
	if s == nil {
		return nil
	}
	for _, d := range s.dicts {
		if d.ID == id {
			return d.Dict
		}
	}
	return nil
}

// lookupOrNil returns s as a dictionaryLookup, and a nil interface for a nil
// set, which accepts nothing.
func (s *dictSet) lookupOrNil() dictionaryLookup {
	if s == nil {
		return nil
	}
	return s
}

// lookupOrNil returns c as a dictionaryLookup, and a nil interface for a
// server without compression, which accepts nothing.
func (c *compressionConfig) lookupOrNil() dictionaryLookup {
	if c == nil {
		return nil
	}
	return c
}

// dictionaryLookup finds the dictionary a receiver accepts under an ID.
type dictionaryLookup interface {
	lookup(id uint32) *zstd.Dict
}

// countingLookup is a server's dictionaryLookup for a call's request
// messages, which counts those it decompresses without a dictionary.
type countingLookup struct {
	dicts dictionaryLookup
	plain *atomic.Int64
}

func (l countingLookup) lookup(id uint32) *zstd.Dict {
	dict := l.dicts.lookup(id)
	if dict != nil && id == plainDictID {
		l.plain.Add(1)
	}
	return dict
}

// requestLookup returns the dictionaryLookup of the request messages of
// request, which counts those without a dictionary in request.plainRequests;
// nil for a server without compression.
func (c *compressionConfig) requestLookup(request *wireRequest) dictionaryLookup {
	if c == nil {
		return nil
	}
	request.plainRequests = new(atomic.Int64)
	return countingLookup{dicts: c, plain: request.plainRequests}
}

// errUnacceptedCompression reports a ZSTD_MESSAGE frame where the receiver
// accepts no dictionary.
var errUnacceptedCompression = errors.New("compressed message where no dictionary is accepted")

// inflateMessage decompresses the payload of a ZSTD_MESSAGE frame. dicts holds
// the dictionaries the receiver accepts and may be nil. A message above
// maxSize is a FrameTooLargeError, found before decompressing; every other
// fault is a frameDecodeError.
func inflateMessage(dicts dictionaryLookup, payload []byte, maxSize int) ([]byte, error) {
	header, dict, err := checkCompressed(dicts, payload, maxSize)
	if err != nil {
		return nil, err
	}
	return decompressChecked(dict, payload, header)
}

// checkCompressed validates a ZSTD_MESSAGE payload and finds its dictionary,
// without decompressing it.
func checkCompressed(dicts dictionaryLookup, payload []byte, maxSize int) (zstd.Header, *zstd.Dict, error) {
	if dicts == nil {
		return zstd.Header{}, nil, &frameDecodeError{Err: errUnacceptedCompression}
	}
	header, err := zstd.ParseHeader(payload)
	if err != nil {
		return zstd.Header{}, nil, &frameDecodeError{Err: err}
	}
	if header.ContentSize > uint64(maxSize) {
		return zstd.Header{}, nil, &FrameTooLargeError{Len: int(min(header.ContentSize, math.MaxInt)), Max: maxSize}
	}
	dict := dicts.lookup(header.DictID)
	if dict == nil {
		return zstd.Header{}, nil, &frameDecodeError{Err: fmt.Errorf("compressed message uses dictionary %d, which is not accepted", header.DictID)}
	}
	return header, dict, nil
}

func decompressChecked(dict *zstd.Dict, payload []byte, header zstd.Header) ([]byte, error) {
	message, err := dict.Decompress(nil, payload, header)
	if err != nil {
		return nil, &frameDecodeError{Err: err}
	}
	return message, nil
}

// messageCompressor encodes the outgoing messages of one direction, with the
// dictionaries negotiation allows.
type messageCompressor struct {
	config  *compressionConfig
	allowed func(*compressionDict) bool
	// plain lets messages without an allowed dictionary for their type be
	// compressed without one, where negotiation allows that too.
	plain bool
}

// dictFor returns the trained dictionary message may be compressed with, or
// nil. It is also what a stream compression context starts from.
func (c *messageCompressor) dictFor(message proto.Message) *compressionDict {
	if c == nil {
		return nil
	}
	d := c.config.byType[message.ProtoReflect().Descriptor().FullName()]
	if d == nil || !c.allowed(d) {
		return nil
	}
	return d
}

// compressorFor returns what message may be compressed with on its own: its
// type's dictionary, or else compression without a dictionary where the
// direction may use it, or nil. A message without a dictionary is compressed
// only from plainCompressionMinSize bytes (see encodePayload).
func (c *messageCompressor) compressorFor(message proto.Message) *compressionDict {
	if d := c.dictFor(message); d != nil {
		return d
	}
	if c == nil || !c.plain || c.config.plain == nil || !c.allowed(c.config.plain) {
		return nil
	}
	return c.config.plain
}

// encode marshals message into a frame: a ZSTD_MESSAGE when compressorFor
// finds a way to compress it and that makes it smaller, and a MESSAGE
// otherwise. A nil compressor always writes a MESSAGE.
func (c *messageCompressor) encode(message ProtoMessage) (encodedMessage, error) {
	protoMessage := protoMessageV2(message)
	dict := c.compressorFor(protoMessage)
	if dict == nil {
		return encodeProtoMessage(protoMessage)
	}
	scratch := marshalScratch.Get().(*[]byte)
	defer func() {
		if cap(*scratch) <= maxRetainedMarshalScratch {
			marshalScratch.Put(scratch)
		}
	}()
	payload, err := proto.MarshalOptions{}.MarshalAppend((*scratch)[:0], protoMessage)
	if err != nil {
		return encodedMessage{}, err
	}
	*scratch = payload[:0]
	return encodePayload(payload, dict), nil
}

// marshalScratch holds the buffers messages are marshalled into before they
// are compressed.
var marshalScratch = sync.Pool{New: func() any { return new([]byte) }}

// maxRetainedMarshalScratch is the largest buffer marshalScratch keeps.
const maxRetainedMarshalScratch = 64 * 1024

// compressedFrameHeaderRoom is the room left before a compressed payload for
// its frame header: the type and the longest length varint.
const compressedFrameHeaderRoom = 1 + 8

// encodePayload frames the marshalled message payload: compressed with dict,
// if it is not nil and that is shorter, and as a MESSAGE otherwise. Without a
// dictionary, only payloads of plainCompressionMinSize bytes or more are
// compressed.
func encodePayload(payload []byte, dict *compressionDict) encodedMessage {
	if dict != nil && (dict.ID != plainDictID || len(payload) >= plainCompressionMinSize) {
		return compressPayload(payload, dict.Dict)
	}
	frame := make([]byte, 0, frameHeaderLen(frameTypeMessage, len(payload))+len(payload)+frameTailSlack)
	frame = appendFrameHeader(frame, frameTypeMessage, len(payload))
	return encodedMessage{frame: append(frame, payload...), size: len(payload)}
}

// compressPayload frames payload as a ZSTD_MESSAGE compressed with dict when
// that is shorter, and as a MESSAGE otherwise.
func compressPayload(payload []byte, dict *zstd.Dict) encodedMessage {
	buffer := make([]byte, compressedFrameHeaderRoom, compressedFrameHeaderRoom+zstd.CompressBound(len(payload))+frameTailSlack)
	compressed, err := dict.Compress(buffer, payload)
	if n := len(compressed) - compressedFrameHeaderRoom; err == nil && n < len(payload) {
		start := compressedFrameHeaderRoom - frameHeaderLen(frameTypeZstdMessage, n)
		appendFrameHeader(compressed[start:start], frameTypeZstdMessage, n)
		return encodedMessage{frame: compressed[start:], size: len(payload)}
	}
	return encodePayload(payload, nil)
}

// clientCompression is the compression state of one client connection.
type clientCompression struct {
	config *compressionConfig
	// shared is the connection's shared set, from the server's answer to a
	// query; nil until one arrives.
	shared atomic.Pointer[dictSet]
	// requests compresses request messages with the shared set, and
	// plainRequests does too on calls that may also compress them without a
	// dictionary.
	requests      messageCompressor
	plainRequests messageCompressor
}

func newClientCompression(config *compressionConfig) *clientCompression {
	if config == nil {
		return nil
	}
	c := &clientCompression{config: config}
	shared := func(d *compressionDict) bool { return c.shared.Load().has(d) }
	c.requests = messageCompressor{config: config, allowed: shared}
	c.plainRequests = messageCompressor{config: config, allowed: shared, plain: true}
	return c
}

// prepare fills the negotiation fields of request. It returns the
// dictionaries the call accepts on its responses, nil for none, and whether
// the request queries the server.
//
// A client queries until the connection has an answer: on every call when it
// holds dictionaries, and otherwise only on calls that may compress their
// requests without a dictionary, so other calls carry no negotiation.
func (c *clientCompression) prepare(request *wireRequest) (accept *dictSet, queried bool) {
	if c == nil {
		return nil, false
	}
	shared := c.shared.Load()
	if shared == nil && (c.config.dictionaries || request.plainCompression.Requests) {
		request.DictionaryQuery = c.config.query
		queried = true
	}
	// A call that queries may already accept the dictionary it asks about:
	// the server compresses with it only if its answer confirms it.
	d := c.config.byType[request.responseType]
	if d != nil && shared != nil && !shared.has(d) {
		d = nil
	}
	// Responses may be compressed without a dictionary from the first call: a
	// server that does not do that ignores the ID, as any it does not hold.
	plain := request.plainCompression.Responses
	switch {
	case d != nil && plain:
		request.AcceptDictionaries, accept = d.acceptPlain, d.withPlain
	case d != nil:
		request.AcceptDictionaries, accept = d.accept, d.only
	case plain:
		request.AcceptDictionaries, accept = c.config.plain.accept, c.config.plain.only
	}
	return accept, queried
}

// learn records the server's answer to a query as the connection's shared
// set. The first answer wins; they all come from one configuration.
func (c *clientCompression) learn(answer []*dictionaryInfo) {
	shared := &dictSet{}
	for _, info := range answer {
		if d := c.config.dict(info.ID); d != nil && d.matches(info) && !shared.has(d) {
			shared.dicts = append(shared.dicts, d)
		}
	}
	c.shared.CompareAndSwap(nil, shared)
}

// requestCompressor returns the compressor of the request messages of a call,
// which may compress them without a dictionary if plain is set, or nil.
func (c *clientCompression) requestCompressor(plain bool) *messageCompressor {
	switch {
	case c == nil:
		return nil
	case plain:
		return &c.plainRequests
	default:
		return &c.requests
	}
}

// callCompression is the server's negotiation result for one call.
type callCompression struct {
	// answered says the request queried, so the call's ANSWER must carry
	// answer.
	answered bool
	answer   []*dictionaryInfo
	// responses compresses the response messages, or is nil.
	responses *messageCompressor
}

// negotiate answers the request's query, if it has one, and picks the
// dictionaries the response messages may use, compressing them without a
// dictionary only if plain is set. It returns nil for a request without
// negotiation fields. A server without compression still answers, so clients
// stop asking.
func (c *compressionConfig) negotiate(request *wireRequest, plain bool) *callCompression {
	if len(request.DictionaryQuery) == 0 && len(request.AcceptDictionaries) == 0 {
		return nil
	}
	result := &callCompression{answered: len(request.DictionaryQuery) > 0}
	var confirmed dictSet
	for _, info := range request.DictionaryQuery {
		if d := c.dict(info.GetID()); d != nil && d.matches(info) && !confirmed.has(d) {
			confirmed.dicts = append(confirmed.dicts, d)
			result.answer = append(result.answer, d.info)
		}
	}
	var allowed dictSet
	for _, id := range request.AcceptDictionaries {
		d := c.dict(id)
		if d == nil || allowed.has(d) || (result.answered && !confirmed.has(d)) {
			continue
		}
		allowed.dicts = append(allowed.dicts, d)
	}
	if len(allowed.dicts) > 0 {
		result.responses = &messageCompressor{config: c, allowed: allowed.has, plain: plain}
	}
	return result
}

// GetID returns the dictionary ID, or zero for a nil entry.
func (m *dictionaryInfo) GetID() uint32 {
	if m == nil {
		return 0
	}
	return m.ID
}

// GetFingerprint returns the fingerprint, or nil for a nil entry.
func (m *dictionaryInfo) GetFingerprint() []byte {
	if m == nil {
		return nil
	}
	return m.Fingerprint
}

// responseCompressorFromContext returns the compressor of the response
// messages of the call ctx belongs to, or nil.
func responseCompressorFromContext(ctx contextValuer) *messageCompressor {
	state, ok := ctx.Value(handlerContextKey{}).(*callState)
	if !ok || state.compression == nil {
		return nil
	}
	return state.compression.responses
}

// responseStreamsFromContext returns the stream compression of the call ctx
// belongs to, or nil.
func responseStreamsFromContext(ctx contextValuer) *serverStreams {
	state, ok := ctx.Value(handlerContextKey{}).(*callState)
	if !ok {
		return nil
	}
	return state.streams
}

// contextValuer is the part of context.Context responseCompressorFromContext
// needs.
type contextValuer interface {
	Value(key any) any
}
