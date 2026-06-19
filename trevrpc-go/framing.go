package trevrpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

const DefaultMaxFrameSize = 4 * 1024 * 1024

type ProtoMessage = protoadapt.MessageV1

type FrameTooLargeError struct {
	Len int
	Max int
}

type FrameDecodeError struct {
	Err error
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("frame length %d exceeds maximum %d", e.Len, e.Max)
}

func (e *FrameDecodeError) Error() string {
	return "failed to decode RPC frame: " + e.Err.Error()
}

func (e *FrameDecodeError) Unwrap() error {
	return e.Err
}

func MarshalMessage(message ProtoMessage) ([]byte, error) {
	return proto.Marshal(protoadapt.MessageV2Of(message))
}

func UnmarshalMessage(body []byte, message ProtoMessage) error {
	return proto.Unmarshal(body, protoadapt.MessageV2Of(message))
}

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

func DecodeFrame(body []byte, message ProtoMessage) error {
	if err := UnmarshalMessage(body, message); err != nil {
		return &FrameDecodeError{Err: err}
	}

	return nil
}

func WriteFrame(writer io.Writer, message ProtoMessage, maxFrameSize int) error {
	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = writer.Write(frame)
	return err
}

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

	body := make([]byte, length)
	if _, err := io.ReadFull(reader, body); err != nil {
		return false, err
	}

	return true, DecodeFrame(body, message)
}
