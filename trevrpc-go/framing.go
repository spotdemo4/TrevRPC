package trevrpc

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
)

const DefaultMaxFrameSize = 16 * 1024 * 1024

type FrameTooLargeError struct {
	Len int
	Max int
}

func (e *FrameTooLargeError) Error() string {
	return fmt.Sprintf("frame length %d exceeds maximum %d", e.Len, e.Max)
}

func EncodeFrame(message proto.Message, maxFrameSize int) ([]byte, error) {
	body, err := proto.Marshal(message)
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

func DecodeFrame(body []byte, message proto.Message) error {
	return proto.Unmarshal(body, message)
}

func WriteFrame(writer io.Writer, message proto.Message, maxFrameSize int) error {
	frame, err := EncodeFrame(message, maxFrameSize)
	if err != nil {
		return err
	}

	_, err = writer.Write(frame)
	return err
}

func ReadFrame(reader io.Reader, message proto.Message, maxFrameSize int) error {
	read, err := ReadFrameOrEOF(reader, message, maxFrameSize)
	if err != nil {
		return err
	}

	if !read {
		return io.ErrUnexpectedEOF
	}

	return nil
}

func ReadFrameOrEOF(reader io.Reader, message proto.Message, maxFrameSize int) (bool, error) {
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
