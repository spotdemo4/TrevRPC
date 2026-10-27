package trevrpc

import (
	"errors"
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/quic-go/quic-go/quicvarint"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// DefaultMaxFrameSize is the default maximum TrevRPC frame payload size in bytes.
const DefaultMaxFrameSize = 4 * 1024 * 1024

// ProtoMessage is the protobuf message interface accepted by the runtime.
type ProtoMessage = protoadapt.MessageV1

// FrameTooLargeError reports a frame whose payload size exceeds the configured
// frame size limit. StatusFromError maps it to ResourceExhausted.
type FrameTooLargeError struct {
	Len int
	Max int
}

// frameDecodeError wraps protobuf decode failures for RPC frames.
type frameDecodeError struct {
	Err error
}

// Error returns a human-readable frame size error.
func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("frame length %d exceeds maximum %d", e.Len, e.Max)
}

// Error returns a human-readable frame decode error.
func (e *frameDecodeError) Error() string {
	return "failed to decode RPC frame: " + e.Err.Error()
}

// Unwrap returns the underlying protobuf decode error.
func (e *frameDecodeError) Unwrap() error {
	return e.Err
}

// frameType is the QUIC variable-length integer type of a frame.
type frameType uint64

// Frame types. Every other type, including the grease types 0x1f*N+0x21, is
// unknown and skipped by receivers.
const (
	frameTypeMessage frameType = 0
	frameTypeRequest frameType = 1
	frameTypeHeaders frameType = 2
	frameTypeStatus  frameType = 3
	// frameTypeZstdMessage carries a message compressed with zstd and a
	// dictionary. Readers turn it into a MESSAGE frame, so nothing after them
	// sees this type.
	frameTypeZstdMessage frameType = 4
	// frameTypeZstdStreamMessage carries a message compressed with the zstd
	// context of its direction of the call. Readers turn it into a MESSAGE
	// frame too.
	frameTypeZstdStreamMessage frameType = 5
	// frameTypeAnswer carries the server's answer to the negotiation fields
	// of the REQUEST, before the response's HEADERS.
	frameTypeAnswer frameType = 6
)

// known reports whether t is a frame type this runtime interprets.
func (t frameType) known() bool { return t <= frameTypeAnswer }

func (t frameType) String() string {
	switch t {
	case frameTypeMessage:
		return "MESSAGE"
	case frameTypeRequest:
		return "REQUEST"
	case frameTypeHeaders:
		return "HEADERS"
	case frameTypeStatus:
		return "STATUS"
	case frameTypeZstdMessage:
		return "ZSTD_MESSAGE"
	case frameTypeZstdStreamMessage:
		return "ZSTD_STREAM_MESSAGE"
	case frameTypeAnswer:
		return "ANSWER"
	default:
		return fmt.Sprintf("unknown frame type %#x", uint64(t))
	}
}

// frame is one known frame read from a stream.
type frame struct {
	typ     frameType
	payload []byte
}

func protoMessageV2(message ProtoMessage) proto.Message {
	if message, ok := any(message).(proto.Message); ok {
		return message
	}

	return protoadapt.MessageV2Of(message)
}

// unmarshalMessage decodes a protobuf message payload into message.
func unmarshalMessage(payload []byte, message ProtoMessage) error {
	protoMessage := protoMessageV2(message)
	if err := preflightWireMessage(payload, protoMessage.ProtoReflect().Descriptor()); err != nil {
		return err
	}
	return proto.Unmarshal(payload, protoMessage)
}

// decodePayload decodes the protobuf payload of a REQUEST, HEADERS, ANSWER or
// STATUS frame, reporting malformed payloads as frameDecodeError.
func decodePayload(payload []byte, message ProtoMessage) error {
	if err := unmarshalMessage(payload, message); err != nil {
		return &frameDecodeError{Err: err}
	}

	return nil
}

// decodeRequest decodes the payload of a REQUEST frame into request, which
// must be empty.
func decodeRequest(payload []byte, request *wireRequest) error {
	if request.decodeFast(payload) {
		return nil
	}
	*request = wireRequest{}
	return decodePayload(payload, request)
}

// decodeStatus decodes the payload of a STATUS frame.
func decodeStatus(payload []byte) (*Status, error) {
	if len(payload) == 0 {
		return OK(), nil
	}
	var status wireStatus
	if !status.decodeFast(payload) {
		status = wireStatus{}
		if err := decodePayload(payload, &status); err != nil {
			return nil, err
		}
	}
	return &Status{Code: CodeFromUint32(status.Code), Message: status.Message, Metadata: ownedMetadata(status.Metadata), refusedStreamCompression: status.RefusedStreamCompression}, nil
}

// decodeHeaders decodes the payload of a HEADERS frame into its metadata.
func decodeHeaders(payload []byte) (Metadata, error) {
	if len(payload) == 0 {
		return Metadata{}, nil
	}
	var headers wireHeaders
	if !headers.decodeFast(payload) {
		headers = wireHeaders{}
		if err := decodePayload(payload, &headers); err != nil {
			return nil, err
		}
	}
	return ownedMetadata(headers.Metadata), nil
}

// decodeAnswer decodes the payload of an ANSWER frame.
func decodeAnswer(payload []byte) (wireAnswer, error) {
	var answer wireAnswer
	if !answer.decodeFast(payload) {
		answer = wireAnswer{}
		if err := decodePayload(payload, &answer); err != nil {
			return wireAnswer{}, err
		}
	}
	return answer, nil
}

// encodedMessage is one message ready to send: a complete MESSAGE frame whose
// payload was marshalled directly into the frame buffer, or a ZSTD_MESSAGE
// frame carrying it compressed.
type encodedMessage struct {
	frame []byte
	// size is the length of the message, which limits apply to. For a MESSAGE
	// frame it is the length of the payload within frame.
	size int
	// generation is the call of an optimistic upload the message was encoded
	// for, zero for any other message; see optimisticUpload.
	generation uint32
}

// frameTailSlack is the spare capacity encodeMessage leaves behind a frame, so
// a unary response can append its two-byte OK STATUS without copying the frame.
const frameTailSlack = 8

// encodeMessage marshals message into a MESSAGE frame. The frame size limit is
// enforced where the frame is written.
func encodeMessage(message ProtoMessage) (encodedMessage, error) {
	return encodeProtoMessage(protoMessageV2(message))
}

// encodeProtoMessage is encodeMessage for a message already converted with
// protoMessageV2.
func encodeProtoMessage(protoMessage proto.Message) (encodedMessage, error) {
	size := proto.Size(protoMessage)
	frame, err := appendProtoFrame(make([]byte, 0, frameHeaderLen(frameTypeMessage, size)+size+frameTailSlack), frameTypeMessage, protoMessage, size)
	if err != nil {
		return encodedMessage{}, err
	}
	return encodedMessage{frame: frame, size: size}, nil
}

// frameHeaderLen is the encoded length of a frame header.
func frameHeaderLen(typ frameType, length int) int {
	return quicvarint.Len(uint64(typ)) + quicvarint.Len(uint64(length))
}

// appendFrameHeader appends a frame header with minimal varints.
func appendFrameHeader(dst []byte, typ frameType, length int) []byte {
	dst = quicvarint.Append(dst, uint64(typ))
	return quicvarint.Append(dst, uint64(length))
}

// appendProtoFrame appends a frame whose payload is message marshalled
// straight into dst. size must be proto.Size(message). On error dst is
// returned unchanged in length.
func appendProtoFrame(dst []byte, typ frameType, message proto.Message, size int) ([]byte, error) {
	start := len(dst)
	dst = slices.Grow(dst, frameHeaderLen(typ, size)+size)
	dst = appendFrameHeader(dst, typ, size)
	marshalled, err := proto.MarshalOptions{UseCachedSize: true}.MarshalAppend(dst, message)
	if err != nil {
		// MarshalAppend does not return a usable buffer on failure.
		return dst[:start], err
	}
	return marshalled, nil
}

// appendLimitedProtoFrame appends a frame carrying message, rejecting payloads
// larger than maxFrameSize.
func appendLimitedProtoFrame(dst []byte, typ frameType, message ProtoMessage, maxFrameSize int) ([]byte, error) {
	protoMessage := protoMessageV2(message)
	size := proto.Size(protoMessage)
	if size > maxFrameSize {
		return dst, &FrameTooLargeError{Len: size, Max: maxFrameSize}
	}
	return appendProtoFrame(dst, typ, protoMessage, size)
}

// appendDirectFrameHeader checks the payload size against maxFrameSize and
// appends the frame header, with room for the payload behind it.
func appendDirectFrameHeader(dst []byte, typ frameType, size, maxFrameSize int) ([]byte, error) {
	if size > maxFrameSize {
		return dst, &FrameTooLargeError{Len: size, Max: maxFrameSize}
	}
	dst = slices.Grow(dst, frameHeaderLen(typ, size)+size)
	return appendFrameHeader(dst, typ, size), nil
}

// appendRequestFrame appends a REQUEST frame.
func appendRequestFrame(dst []byte, request *wireRequest, maxFrameSize int) ([]byte, error) {
	size, ok := request.payloadSize()
	if !ok {
		return appendLimitedProtoFrame(dst, frameTypeRequest, request, maxFrameSize)
	}
	dst, err := appendDirectFrameHeader(dst, frameTypeRequest, size, maxFrameSize)
	if err != nil {
		return dst, err
	}
	return request.appendPayload(dst), nil
}

// appendAnswerFrame appends an ANSWER frame.
func appendAnswerFrame(dst []byte, answer *wireAnswer, maxFrameSize int) ([]byte, error) {
	size := answer.payloadSize()
	dst, err := appendDirectFrameHeader(dst, frameTypeAnswer, size, maxFrameSize)
	if err != nil {
		return dst, err
	}
	return answer.appendPayload(dst), nil
}

// appendHeadersFrame appends a HEADERS frame carrying metadata.
func appendHeadersFrame(dst []byte, metadata Metadata, maxFrameSize int) ([]byte, error) {
	headers := wireHeaders{Metadata: metadata}
	size, ok := headers.payloadSize()
	if !ok {
		return appendLimitedProtoFrame(dst, frameTypeHeaders, &headers, maxFrameSize)
	}
	dst, err := appendDirectFrameHeader(dst, frameTypeHeaders, size, maxFrameSize)
	if err != nil {
		return dst, err
	}
	return headers.appendPayload(dst), nil
}

// appendStatusFrame appends a STATUS frame. An OK status without message or
// metadata is the two bytes 03 00.
func appendStatusFrame(dst []byte, status *Status, maxFrameSize int) ([]byte, error) {
	if status == nil {
		status = OK()
	}
	if status.Code == CodeOK && status.Message == "" && len(status.Metadata) == 0 && !status.refusedStreamCompression {
		return appendFrameHeader(dst, frameTypeStatus, 0), nil
	}
	wire := wireStatus{Code: uint32(status.Code), Message: status.Message, Metadata: status.Metadata, RefusedStreamCompression: status.refusedStreamCompression}
	size, ok := wire.payloadSize()
	if !ok {
		return appendLimitedProtoFrame(dst, frameTypeStatus, &wire, maxFrameSize)
	}
	dst, err := appendDirectFrameHeader(dst, frameTypeStatus, size, maxFrameSize)
	if err != nil {
		return dst, err
	}
	return wire.appendPayload(dst), nil
}

// appendEncodedMessage appends a message frame produced by encodeMessage.
func appendEncodedMessage(dst []byte, message encodedMessage, maxFrameSize int) ([]byte, error) {
	if message.size > maxFrameSize {
		return dst, &FrameTooLargeError{Len: message.size, Max: maxFrameSize}
	}
	return append(dst, message.frame...), nil
}

// frameReadAhead is how many bytes a frameReader with readAhead set asks its
// stream for at a time. Small frames, and the header of a larger one, then
// cost one read of the stream instead of one per field.
const frameReadAhead = 256

// frameReader reads frames from one stream. Frames of unknown type are
// skipped without being buffered.
type frameReader struct {
	r            io.Reader
	maxFrameSize int
	// maxBytes bounds the cumulative bytes of skipped frames (headers included)
	// and, once chargeMessages is set, of MESSAGE payloads that may be read, or
	// is negative for no limit.
	maxBytes int
	charged  int
	// chargeMessages counts MESSAGE payloads towards maxBytes. A request that
	// is a single message is bounded by the frame size alone; a stream of
	// messages is bounded cumulatively.
	chargeMessages bool
	// inflate holds the dictionaries the reader accepts on ZSTD_MESSAGE
	// frames, which it hands on as MESSAGE frames; nil accepts none.
	inflate dictionaryLookup
	// stream decompresses the ZSTD_STREAM_MESSAGE frames of a direction that
	// enabled stream compression, which the reader hands on as MESSAGE
	// frames; nil rejects them.
	stream *streamInflater
	// readAhead lets the reader take bytes beyond the frame it is reading from
	// the stream. Until it is set the reader takes exactly what it needs,
	// which leaves the rest of a request in the stream when the server
	// rejects it.
	readAhead bool

	// ahead[start:end] holds bytes read from r but not consumed yet. err is
	// the error r returned with them, reported once they are consumed.
	ahead      [frameReadAhead]byte
	start, end int
	err        error
	// header is the scratch space of readHeader.
	header [16]byte
}

func newFrameReader(r io.Reader, maxFrameSize, maxBytes int) *frameReader {
	return &frameReader{r: r, maxFrameSize: maxFrameSize, maxBytes: maxBytes}
}

// next returns the next frame of known type. It returns io.EOF only when the
// stream ends at a frame boundary; an end inside a frame is io.ErrUnexpectedEOF.
// A declared payload length above the frame size limit is rejected before any
// of the payload is read or allocated, whether or not the type is known.
func (fr *frameReader) next() (frame, error) {
	for {
		typ, length, headerLen, err := fr.readHeader()
		if err != nil {
			return frame{}, err
		}
		if length > uint64(fr.maxFrameSize) {
			return frame{}, &FrameTooLargeError{Len: int(min(length, math.MaxInt)), Max: fr.maxFrameSize}
		}
		size := int(length)

		if typ == frameTypeZstdMessage {
			return fr.readCompressed(size)
		}
		if typ == frameTypeZstdStreamMessage {
			return fr.readStreamCompressed(size)
		}
		if typ.known() {
			if typ == frameTypeMessage && fr.chargeMessages {
				if err := fr.charge(size); err != nil {
					return frame{}, err
				}
			}
			payload, err := fr.readPayload(size)
			if err != nil {
				return frame{}, err
			}
			return frame{typ: typ, payload: payload}, nil
		}

		if err := fr.charge(headerLen + size); err != nil {
			return frame{}, err
		}
		if err := fr.discard(size); err != nil {
			return frame{}, err
		}
	}
}

// readCompressed reads the payload of a ZSTD_MESSAGE frame and returns the
// message it decompresses to as a MESSAGE frame. The decompressed size is
// checked against the frame size limit, and charged like a MESSAGE payload,
// before it is decompressed.
func (fr *frameReader) readCompressed(size int) (frame, error) {
	payload, err := fr.readPayload(size)
	if err != nil {
		return frame{}, err
	}
	header, dict, err := checkCompressed(fr.inflate, payload, fr.maxFrameSize)
	if err != nil {
		return frame{}, err
	}
	if fr.chargeMessages {
		if err := fr.charge(int(header.ContentSize)); err != nil {
			return frame{}, err
		}
	}
	message, err := decompressChecked(dict, payload, header)
	if err != nil {
		return frame{}, err
	}
	return frame{typ: frameTypeMessage, payload: message}, nil
}

// readStreamCompressed reads the payload of a ZSTD_STREAM_MESSAGE frame and
// returns the message it decompresses to as a MESSAGE frame, checked and
// charged like readCompressed's.
func (fr *frameReader) readStreamCompressed(size int) (frame, error) {
	if fr.stream == nil {
		return frame{}, &frameDecodeError{Err: errUnenabledStreamCompression}
	}
	payload, err := fr.readPayload(size)
	if err != nil {
		return frame{}, err
	}
	message, err := fr.stream.inflate(payload, fr.maxFrameSize, func(n int) error {
		if fr.chargeMessages {
			return fr.charge(n)
		}
		return nil
	})
	if err != nil {
		return frame{}, err
	}
	return frame{typ: frameTypeMessage, payload: message}, nil
}

// charge accounts n bytes against the cumulative limit.
func (fr *frameReader) charge(n int) error {
	if fr.maxBytes < 0 {
		return nil
	}
	fr.charged = saturatingAdd(fr.charged, n)
	if fr.charged > fr.maxBytes {
		return ResourceExhausted(fmt.Sprintf("request stream exceeded maximum body size of %d bytes", fr.maxBytes))
	}
	return nil
}

// readFull fills p, like io.ReadFull: it fails with io.EOF if the stream
// ended before the first byte, and io.ErrUnexpectedEOF after it.
func (fr *frameReader) readFull(p []byte) error {
	if !fr.readAhead && fr.start == fr.end && fr.err == nil {
		_, err := io.ReadFull(fr.r, p)
		return err
	}
	read := 0
	for read < len(p) {
		if fr.start < fr.end {
			n := copy(p[read:], fr.ahead[fr.start:fr.end])
			fr.start += n
			read += n
			continue
		}
		if fr.err == nil && (!fr.readAhead || len(p)-read >= len(fr.ahead)) {
			// Nothing is gained by buffering what is read in full anyway.
			n, err := io.ReadFull(fr.r, p[read:])
			read += n
			return unexpectedEOF(err, read)
		}
		if err := fr.err; err != nil {
			fr.err = nil
			return unexpectedEOF(err, read)
		}
		n, err := fr.r.Read(fr.ahead[:])
		fr.start, fr.end, fr.err = 0, n, err
	}
	return nil
}

// unexpectedEOF reports an end of stream after read bytes as
// io.ErrUnexpectedEOF if bytes of the unit being read came before it.
func unexpectedEOF(err error, read int) error {
	if err != nil && read > 0 && errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

// discard skips n bytes. A stream that ends first is io.ErrUnexpectedEOF.
func (fr *frameReader) discard(n int) error {
	buffered := min(fr.end-fr.start, n)
	fr.start += buffered
	n -= buffered
	if n == 0 {
		return nil
	}
	if fr.err != nil {
		err := fr.err
		fr.err = nil
		return unexpectedEOF(err, 1)
	}
	if _, err := io.CopyN(io.Discard, fr.r, int64(n)); err != nil {
		return unexpectedEOF(err, 1)
	}
	return nil
}

// readHeader reads a frame header: a type and a length, each a QUIC varint.
func (fr *frameReader) readHeader() (typ frameType, length uint64, headerLen int, err error) {
	// A header is at least two bytes, so the first read takes exactly two; a
	// longer type or length costs one more.
	buf := &fr.header
	if err := fr.readFull(buf[:2]); err != nil {
		return 0, 0, 0, err
	}
	typeLen := 1 << (buf[0] >> 6)
	if err := fr.fillHeader(buf, 2, typeLen); err != nil {
		return 0, 0, 0, err
	}
	typeValue, _, err := quicvarint.Parse(buf[:typeLen])
	if err != nil {
		return 0, 0, 0, err
	}
	if err := fr.fillHeader(buf, max(2, typeLen), typeLen+1); err != nil {
		return 0, 0, 0, err
	}
	lengthLen := 1 << (buf[typeLen] >> 6)
	if err := fr.fillHeader(buf, max(2, typeLen+1), typeLen+lengthLen); err != nil {
		return 0, 0, 0, err
	}
	length, _, err = quicvarint.Parse(buf[typeLen : typeLen+lengthLen])
	if err != nil {
		return 0, 0, 0, err
	}
	return frameType(typeValue), length, typeLen + lengthLen, nil
}

// fillHeader reads header bytes buf[have:want], if any are missing. The
// stream may not end inside a header.
func (fr *frameReader) fillHeader(buf *[16]byte, have, want int) error {
	if want <= have {
		return nil
	}
	if err := fr.readFull(buf[have:want]); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	return nil
}

// readPayloadChunk is the largest payload allocated before any of it has been
// received. Larger payloads grow as data arrives, so a declared length alone
// cannot make the receiver allocate it.
const readPayloadChunk = 64 * 1024

// readPayload reads exactly length payload bytes.
func (fr *frameReader) readPayload(length int) ([]byte, error) {
	payload := make([]byte, min(length, readPayloadChunk))
	for read := 0; ; {
		if err := fr.readFull(payload[read:]); err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return nil, err
		}
		read = len(payload)
		if read == length {
			return payload, nil
		}
		payload = slices.Grow(payload, min(length-read, max(read, readPayloadChunk)))
		payload = payload[:min(length, cap(payload))]
	}
}

const maxPreflightMessageDepth = 100

func preflightWireMessage(data []byte, descriptor protoreflect.MessageDescriptor) error {
	return preflightWireMessageDepth(data, descriptor, 0)
}

func preflightWireMessageDepth(data []byte, descriptor protoreflect.MessageDescriptor, depth int) error {
	if depth > maxPreflightMessageDepth {
		return errors.New("protobuf message nesting exceeded limit")
	}

	for len(data) > 0 {
		fieldNumber, wireType, tagLength := protowire.ConsumeTag(data)
		if tagLength < 0 || fieldNumber <= 0 {
			return errors.New("invalid protobuf field tag")
		}
		data = data[tagLength:]

		field := descriptor.Fields().ByNumber(fieldNumber)
		if field == nil {
			valueLength := protowire.ConsumeFieldValue(fieldNumber, wireType, data)
			if valueLength < 0 {
				return fmt.Errorf("invalid protobuf field %d", fieldNumber)
			}
			data = data[valueLength:]
			continue
		}

		expected := protobufWireType(field.Kind())
		packed := field.IsList() && isPackableProtobufKind(field.Kind()) && wireType == protowire.BytesType
		if wireType != expected && !packed {
			return fmt.Errorf("known field %d used wire type %d; expected %d", fieldNumber, wireType, expected)
		}

		if packed {
			packedData, valueLength := protowire.ConsumeBytes(data)
			if valueLength < 0 || !preflightPackedValues(packedData, expected) {
				return fmt.Errorf("invalid packed protobuf field %d", fieldNumber)
			}
			data = data[valueLength:]
			continue
		}

		if field.Kind() == protoreflect.MessageKind {
			nested, valueLength := protowire.ConsumeBytes(data)
			if valueLength < 0 {
				return fmt.Errorf("invalid protobuf message field %d", fieldNumber)
			}
			if err := preflightWireMessageDepth(nested, field.Message(), depth+1); err != nil {
				return fmt.Errorf("invalid protobuf message field %d: %w", fieldNumber, err)
			}
			data = data[valueLength:]
			continue
		}
		if field.Kind() == protoreflect.GroupKind {
			nested, valueLength := protowire.ConsumeGroup(fieldNumber, data)
			if valueLength < 0 {
				return fmt.Errorf("invalid protobuf group field %d", fieldNumber)
			}
			if err := preflightWireMessageDepth(nested, field.Message(), depth+1); err != nil {
				return fmt.Errorf("invalid protobuf group field %d: %w", fieldNumber, err)
			}
			data = data[valueLength:]
			continue
		}

		valueLength := protowire.ConsumeFieldValue(fieldNumber, wireType, data)
		if valueLength < 0 {
			return fmt.Errorf("invalid protobuf field %d", fieldNumber)
		}
		data = data[valueLength:]
	}
	return nil
}

func protobufWireType(kind protoreflect.Kind) protowire.Type {
	switch kind {
	case protoreflect.DoubleKind, protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind:
		return protowire.Fixed64Type
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind:
		return protowire.BytesType
	case protoreflect.GroupKind:
		return protowire.StartGroupType
	case protoreflect.FloatKind, protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind:
		return protowire.Fixed32Type
	default:
		return protowire.VarintType
	}
}

func isPackableProtobufKind(kind protoreflect.Kind) bool {
	switch kind {
	case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind, protoreflect.GroupKind:
		return false
	default:
		return true
	}
}

func preflightPackedValues(data []byte, wireType protowire.Type) bool {
	for len(data) > 0 {
		var valueLength int
		switch wireType {
		case protowire.VarintType:
			_, valueLength = protowire.ConsumeVarint(data)
		case protowire.Fixed64Type:
			_, valueLength = protowire.ConsumeFixed64(data)
		case protowire.Fixed32Type:
			_, valueLength = protowire.ConsumeFixed32(data)
		default:
			return false
		}
		if valueLength < 0 {
			return false
		}
		data = data[valueLength:]
	}
	return true
}
