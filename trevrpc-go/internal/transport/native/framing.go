package native

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

var errFrameStreamClosed = errors.New("native frame stream is closed")

type frameSender func([]byte) error

type frameReceiver func() ([]byte, error)

type frameWriter struct {
	writeMu      sync.Mutex
	stateMu      sync.Mutex
	maxFrameSize uint64
	send         frameSender
	pending      []byte
	err          error
}

func (w *frameWriter) Write(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	w.writeMu.Lock()
	defer w.writeMu.Unlock()

	w.stateMu.Lock()
	if w.err != nil {
		err := w.err
		w.stateMu.Unlock()
		return 0, err
	}
	w.pending = append(w.pending, data...)
	for len(w.pending) >= 4 {
		bodyLen := uint64(binary.BigEndian.Uint32(w.pending[:4]))
		if bodyLen > w.maxFrameSize {
			err := fmt.Errorf(
				"native frame length %d exceeds maximum %d",
				bodyLen,
				w.maxFrameSize,
			)
			w.err = err
			w.pending = nil
			w.stateMu.Unlock()
			return len(data), err
		}
		frameLen := bodyLen + 4
		if uint64(len(w.pending)) < frameLen {
			break
		}

		body := append([]byte(nil), w.pending[4:frameLen]...)
		w.pending = w.pending[frameLen:]
		w.stateMu.Unlock()
		if err := w.send(body); err != nil {
			w.fail(err)
			return len(data), err
		}
		w.stateMu.Lock()
		if w.err != nil {
			err := w.err
			w.stateMu.Unlock()
			return len(data), err
		}
	}
	w.stateMu.Unlock()
	return len(data), nil
}

func (w *frameWriter) finish() error {
	w.writeMu.Lock()
	defer w.writeMu.Unlock()
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	if w.err != nil {
		return w.err
	}
	if len(w.pending) != 0 {
		w.err = errors.New("native frame stream ended with a partial frame")
		w.pending = nil
		return w.err
	}
	w.err = errFrameStreamClosed
	return nil
}

func (w *frameWriter) fail(err error) {
	if err == nil {
		err = errFrameStreamClosed
	}
	w.stateMu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.pending = nil
	w.stateMu.Unlock()
}

type frameReader struct {
	readMu       sync.Mutex
	stateMu      sync.Mutex
	maxFrameSize uint64
	receive      frameReceiver
	current      []byte
	offset       int
	err          error
}

func (r *frameReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	r.readMu.Lock()
	defer r.readMu.Unlock()

	r.stateMu.Lock()
	if r.offset < len(r.current) {
		read := copy(data, r.current[r.offset:])
		r.offset += read
		r.stateMu.Unlock()
		return read, nil
	}
	r.current = nil
	r.offset = 0
	if r.err != nil {
		err := r.err
		r.stateMu.Unlock()
		return 0, err
	}
	r.stateMu.Unlock()

	body, err := r.receive()
	if err != nil {
		if !errors.Is(err, os.ErrDeadlineExceeded) {
			r.fail(err)
		}
		return 0, err
	}
	if uint64(len(body)) > r.maxFrameSize {
		err := fmt.Errorf(
			"native frame length %d exceeds maximum %d",
			len(body),
			r.maxFrameSize,
		)
		r.fail(err)
		return 0, err
	}

	r.stateMu.Lock()
	if r.err != nil {
		err := r.err
		r.stateMu.Unlock()
		return 0, err
	}
	r.current = make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(r.current[:4], uint32(len(body)))
	copy(r.current[4:], body)
	read := copy(data, r.current)
	r.offset = read
	r.stateMu.Unlock()
	return read, nil
}

func (r *frameReader) fail(err error) {
	if err == nil {
		err = io.EOF
	}
	r.stateMu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.stateMu.Unlock()
}

func (r *frameReader) cancel(err error) {
	if err == nil {
		err = errFrameStreamClosed
	}
	r.stateMu.Lock()
	if r.err == nil {
		r.err = err
	}
	r.current = nil
	r.offset = 0
	r.stateMu.Unlock()
}
