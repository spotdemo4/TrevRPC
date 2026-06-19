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
const maxFrameReadChunkSize = 32 * 1024

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
	body, err := MarshalMessage(message)
	if err != nil {
		return nil, err
	}

	if len(body) > maxFrameSize {
		return nil, &FrameTooLargeError{Len: len(body), Max: maxFrameSize}
	}

	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)

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
	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = writer.Write(frame)
	return err
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
	header := [4]byte{}
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		if err == io.EOF {
			return false, nil
		}

		return false, err
	}

	length := int(binary.BigEndian.Uint32(header[:]))
	if length > maxFrameSize {
		return false, &FrameTooLargeError{Len: length, Max: maxFrameSize}
	}

	body, err := readFrameBody(reader, length)
	if err != nil {
		return false, err
	}

	return true, DecodeFrame(body, message)
}

func readFrameBody(reader io.Reader, length int) ([]byte, error) {
	if length == 0 {
		return []byte{}, nil
	}

	bufferSize := minInt(length, maxFrameReadChunkSize)
	body := make([]byte, 0, bufferSize)
	buffer := make([]byte, bufferSize)
	remaining := length
	for remaining > 0 {
		readSize := minInt(remaining, len(buffer))
		read, err := io.ReadFull(reader, buffer[:readSize])
		if read > 0 {
			body = append(body, buffer[:read]...)
			remaining -= read
		}
		if err != nil {
			return nil, err
		}
	}

	return body, nil
}

func minInt(left, right int) int {
	if left < right {
		return left
	}

	return right
}
