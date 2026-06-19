package trevrpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// DefaultMaxFrameSize is the default maximum TrevRPC frame body size in bytes.
const DefaultMaxFrameSize = 4 * 1024 * 1024

// ProtoMessage is the protobuf message interface accepted by the runtime.
type ProtoMessage = protoadapt.MessageV1

// FrameTooLargeError reports a frame whose body exceeds the configured limit.
type FrameTooLargeError struct {
	Len int
	Max int
}

// FrameDecodeError wraps protobuf decode failures for RPC frames.
type FrameDecodeError struct {
	Err error
}

type optimizedFrameReader interface {
	trevrpcReadFrame(ProtoMessage, int) (bool, error)
}

type optimizedFrameWriter interface {
	trevrpcWriteFrame(ProtoMessage, int) error
}

type optimizedMessageStreamFrameWriter interface {
	trevrpcWriteMessageStreamFrame([]byte, int) error
}

type optimizedStreamFrameReader interface {
	trevrpcReadStreamFrame(int) (streamFrameFields, bool, error)
}

type streamFrameFields struct {
	kind     RpcStreamFrameKind
	status   uint32
	message  string
	body     []byte
	metadata Metadata
}

// Error returns a human-readable frame size error.
func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("frame length %d exceeds maximum %d", e.Len, e.Max)
}

// Error returns a human-readable frame decode error.
func (e *FrameDecodeError) Error() string {
	return "failed to decode RPC frame: " + e.Err.Error()
}

// Unwrap returns the underlying protobuf decode error.
func (e *FrameDecodeError) Unwrap() error {
	return e.Err
}

// MarshalMessage encodes a protobuf message body.
func MarshalMessage(message ProtoMessage) ([]byte, error) {
	return proto.Marshal(protoadapt.MessageV2Of(message))
}

// UnmarshalMessage decodes a protobuf message body into message.
func UnmarshalMessage(body []byte, message ProtoMessage) error {
	return proto.Unmarshal(body, protoadapt.MessageV2Of(message))
}

// EncodeFrame encodes a protobuf message into a length-prefixed TrevRPC frame.
func EncodeFrame(message ProtoMessage, maxFrameSize int) ([]byte, error) {
	protoMessage := protoadapt.MessageV2Of(message)
	size := proto.Size(protoMessage)
	if size > maxFrameSize {
		return nil, &FrameTooLargeError{Len: size, Max: maxFrameSize}
	}

	frame := make([]byte, 4, 4+size)
	frame, err := proto.MarshalOptions{}.MarshalAppend(frame, protoMessage)
	if err != nil {
		return nil, err
	}

	bodyLen := len(frame) - 4
	if bodyLen > maxFrameSize {
		return nil, &FrameTooLargeError{Len: bodyLen, Max: maxFrameSize}
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(bodyLen))

	return frame, nil
}

// DecodeFrame decodes a protobuf message from a TrevRPC frame body.
func DecodeFrame(body []byte, message ProtoMessage) error {
	if err := UnmarshalMessage(body, message); err != nil {
		return &FrameDecodeError{Err: err}
	}

	return nil
}

// WriteFrame writes one length-prefixed protobuf frame.
func WriteFrame(writer io.Writer, message ProtoMessage, maxFrameSize int) error {
	if frameWriter, ok := writer.(optimizedFrameWriter); ok {
		return frameWriter.trevrpcWriteFrame(message, maxFrameSize)
	}

	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = writer.Write(frame)
	return err
}

func writeMessageStreamFrame(writer io.Writer, body []byte, maxFrameSize int) error {
	if frameWriter, ok := writer.(optimizedMessageStreamFrameWriter); ok {
		return frameWriter.trevrpcWriteMessageStreamFrame(body, maxFrameSize)
	}

	bodyLen := messageStreamFrameBodyLen(body)
	if bodyLen > maxFrameSize {
		return &FrameTooLargeError{Len: bodyLen, Max: maxFrameSize}
	}

	frame := make([]byte, 4, 4+bodyLen)
	if len(body) > 0 {
		frame = append(frame, 0x22)
		frame = appendVarint(frame, uint64(len(body)))
		frame = append(frame, body...)
	}
	binary.BigEndian.PutUint32(frame[:4], uint32(bodyLen))

	_, err := writer.Write(frame)
	return err
}

func messageStreamFrameBodyLen(body []byte) int {
	if len(body) == 0 {
		return 0
	}

	return 1 + varintLen(uint64(len(body))) + len(body)
}

func appendVarint(data []byte, value uint64) []byte {
	for value >= 0x80 {
		data = append(data, byte(value)|0x80)
		value >>= 7
	}
	return append(data, byte(value))
}

func varintLen(value uint64) int {
	length := 1
	for value >= 0x80 {
		length++
		value >>= 7
	}
	return length
}

// ReadFrame reads and decodes one length-prefixed protobuf frame.
func ReadFrame(reader io.Reader, message ProtoMessage, maxFrameSize int) error {
	read, err := ReadFrameOrEOF(reader, message, maxFrameSize)
	if err != nil {
		return err
	}

	if !read {
		return io.ErrUnexpectedEOF
	}

	return nil
}

// ReadFrameOrEOF reads one frame and reports false when the stream is already at EOF.
func ReadFrameOrEOF(reader io.Reader, message ProtoMessage, maxFrameSize int) (bool, error) {
	if frameReader, ok := reader.(optimizedFrameReader); ok {
		return frameReader.trevrpcReadFrame(message, maxFrameSize)
	}

	body, read, err := readRawFrameOrEOF(reader, maxFrameSize)
	if err != nil || !read {
		return read, err
	}

	return true, DecodeFrame(body, message)
}

func readRawFrameOrEOF(reader io.Reader, maxFrameSize int) ([]byte, bool, error) {

	header := [4]byte{}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if err == io.EOF {
			return nil, false, nil
		}

		return nil, false, err
	}

	length := int(binary.BigEndian.Uint32(header[:]))
	if length > maxFrameSize {
		return nil, false, &FrameTooLargeError{Len: length, Max: maxFrameSize}
	}

	body, err := readFrameBody(reader, length)
	if err != nil {
		return nil, false, err
	}

	return body, true, nil
}

func readFrameBody(reader io.Reader, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return nil, err
	}

	return body, nil
}

func readStreamFrameFieldsOrEOF(reader io.Reader, maxFrameSize int) (streamFrameFields, bool, error) {
	if frameReader, ok := reader.(optimizedStreamFrameReader); ok {
		return frameReader.trevrpcReadStreamFrame(maxFrameSize)
	}

	body, read, err := readRawFrameOrEOF(reader, maxFrameSize)
	if err != nil || !read {
		return streamFrameFields{}, read, err
	}

	fields, err := parseStreamFrameFields(body, false)
	return fields, true, err
}

func (f streamFrameFields) rpcStreamFrame() *RpcStreamFrame {
	return &RpcStreamFrame{
		Kind:     f.kind,
		Status:   f.status,
		Message:  f.message,
		Body:     f.body,
		Metadata: f.metadata,
	}
}

func (f streamFrameFields) statusValue() *Status {
	return NewStatus(CodeFromUint32(f.status), f.message)
}

func parseStreamFrameFields(data []byte, copyBytes bool) (streamFrameFields, error) {
	fields := streamFrameFields{kind: RpcStreamFrameKindMessage}
	for offset := 0; offset < len(data); {
		tag, ok := consumeVarint(data, &offset)
		if !ok || tag == 0 {
			return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame field tag")}
		}

		field := tag >> 3
		wireType := tag & 0x7
		switch field {
		case 1:
			if wireType != 0 {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame kind wire type")}
			}
			kind, ok := consumeVarint(data, &offset)
			if !ok {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("truncated stream frame kind")}
			}
			fields.kind = RpcStreamFrameKind(kind)
		case 2:
			if wireType != 0 {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame status wire type")}
			}
			status, ok := consumeVarint(data, &offset)
			if !ok {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("truncated stream frame status")}
			}
			fields.status = uint32(status)
		case 3:
			if wireType != 2 {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame message")}
			}
			value, ok := consumeLengthDelimited(data, &offset)
			if !ok {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame message")}
			}
			fields.message = string(value)
		case 4:
			if wireType != 2 {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame body")}
			}
			value, ok := consumeLengthDelimited(data, &offset)
			if !ok {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame body")}
			}
			fields.body = maybeCopyBytes(value, copyBytes)
		case 5:
			if wireType != 2 {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame metadata")}
			}
			value, ok := consumeLengthDelimited(data, &offset)
			if !ok {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame metadata")}
			}
			key, metadataValue, err := parseMetadataEntry(value, copyBytes)
			if err != nil {
				return streamFrameFields{}, err
			}
			if fields.metadata == nil {
				fields.metadata = Metadata{}
			}
			fields.metadata[key] = metadataValue
		default:
			if !skipProtoField(data, &offset, wireType) {
				return streamFrameFields{}, &FrameDecodeError{Err: fmt.Errorf("invalid stream frame unknown field")}
			}
		}
	}

	switch fields.kind {
	case RpcStreamFrameKindMessage, RpcStreamFrameKindStatus:
		return fields, nil
	default:
		return streamFrameFields{}, InvalidArgument("stream frame contained an unknown frame kind")
	}
}

func parseMetadataEntry(data []byte, copyBytes bool) (string, []byte, error) {
	var key string
	var value []byte
	for offset := 0; offset < len(data); {
		tag, ok := consumeVarint(data, &offset)
		if !ok || tag == 0 {
			return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata field tag")}
		}

		field := tag >> 3
		wireType := tag & 0x7
		switch field {
		case 1:
			if wireType != 2 {
				return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata key")}
			}
			keyBytes, ok := consumeLengthDelimited(data, &offset)
			if !ok {
				return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata key")}
			}
			key = string(keyBytes)
		case 2:
			if wireType != 2 {
				return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata value")}
			}
			valueBytes, ok := consumeLengthDelimited(data, &offset)
			if !ok {
				return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata value")}
			}
			value = maybeCopyBytes(valueBytes, copyBytes)
		default:
			if !skipProtoField(data, &offset, wireType) {
				return "", nil, &FrameDecodeError{Err: fmt.Errorf("invalid metadata unknown field")}
			}
		}
	}

	return key, value, nil
}

func consumeLengthDelimited(data []byte, offset *int) ([]byte, bool) {
	length, ok := consumeVarint(data, offset)
	remaining := len(data) - *offset
	if !ok || length > uint64(remaining) {
		return nil, false
	}

	start := *offset
	*offset += int(length)
	return data[start:*offset], true
}

func consumeVarint(data []byte, offset *int) (uint64, bool) {
	var value uint64
	for shift := 0; shift < 64; shift += 7 {
		if *offset >= len(data) {
			return 0, false
		}

		b := data[*offset]
		*offset = *offset + 1
		value |= uint64(b&0x7f) << shift
		if b < 0x80 {
			return value, true
		}
	}

	return 0, false
}

func skipProtoField(data []byte, offset *int, wireType uint64) bool {
	switch wireType {
	case 0:
		_, ok := consumeVarint(data, offset)
		return ok
	case 1:
		if len(data)-*offset < 8 {
			return false
		}
		*offset += 8
		return true
	case 2:
		_, ok := consumeLengthDelimited(data, offset)
		return ok
	case 5:
		if len(data)-*offset < 4 {
			return false
		}
		*offset += 4
		return true
	default:
		return false
	}
}

func maybeCopyBytes(data []byte, copyBytes bool) []byte {
	if !copyBytes || len(data) == 0 {
		return data
	}

	return append([]byte(nil), data...)
}
