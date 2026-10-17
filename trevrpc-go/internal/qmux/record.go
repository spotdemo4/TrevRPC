package qmux

import (
	"bufio"
	"crypto/tls"
	"errors"
	"io"
	"net"

	"github.com/quic-go/quic-go/quicvarint"
)

// RecordConn carries QMux records. ReadRecord is called by one goroutine and
// the write methods by another; Close may be called from any goroutine.
type RecordConn interface {
	// ReadRecord returns the frames of the next record. The slice is valid
	// until the next call. It returns a *TransportError for a record larger
	// than the local max_record_size.
	ReadRecord() ([]byte, error)
	// WriteRecord sends one record carrying frames, possibly buffering it
	// until Flush. It does not retain frames.
	WriteRecord(frames []byte) error
	// Flush sends buffered records.
	Flush() error
	// CloseWrite gracefully ends the sending direction.
	CloseWrite() error
	// Close releases the connection.
	Close() error
}

// MessageConn carries one message per record, as the WebSocket binding does.
type MessageConn interface {
	// ReadMessage returns the next binary message. The slice is valid until
	// the next call.
	ReadMessage() ([]byte, error)
	// WriteMessage sends one binary message. It does not retain the slice.
	WriteMessage([]byte) error
	// Close ends the connection with a closing handshake.
	Close() error
	// CloseNow releases the connection at once.
	CloseNow() error
}

const streamConnBufferSize = 32 << 10

// streamConn frames records with their Size prefix over a byte stream.
type streamConn struct {
	conn          net.Conn
	reader        *bufio.Reader
	writer        *bufio.Writer
	record        []byte
	maxRecordSize uint64
	// readHeader and writeHeader are used by the reading and the writing
	// goroutine respectively.
	readHeader  [8]byte
	writeHeader [8]byte
}

// NewStreamConn carries records over a reliable byte stream such as a TLS
// connection. Records larger than maxRecordSize are refused.
func NewStreamConn(conn net.Conn, maxRecordSize uint64) RecordConn {
	return &streamConn{
		conn:          conn,
		reader:        bufio.NewReaderSize(conn, streamConnBufferSize),
		writer:        bufio.NewWriterSize(conn, streamConnBufferSize),
		maxRecordSize: maxRecordSize,
	}
}

func (c *streamConn) ReadRecord() ([]byte, error) {
	size, err := c.readSize()
	if err != nil {
		return nil, err
	}
	if size > c.maxRecordSize {
		return nil, protocolError(FrameEncodingError, 0, "record of %d bytes exceeds max_record_size %d", size, c.maxRecordSize)
	}
	if uint64(cap(c.record)) < size {
		c.record = make([]byte, size)
	}
	c.record = c.record[:size]
	if _, err := io.ReadFull(c.reader, c.record); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.ErrUnexpectedEOF
		}
		return nil, err
	}
	return c.record, nil
}

// readSize reads the Size of the next record. The stream may end cleanly only
// between records.
func (c *streamConn) readSize() (uint64, error) {
	first, err := c.reader.ReadByte()
	if err != nil {
		return 0, err
	}
	length := 1 << (first >> 6)
	c.readHeader[0] = first
	if _, err := io.ReadFull(c.reader, c.readHeader[1:length]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	size, _, err := quicvarint.Parse(c.readHeader[:length])
	return size, err
}

func (c *streamConn) WriteRecord(frames []byte) error {
	header := quicvarint.Append(c.writeHeader[:0], uint64(len(frames)))
	if _, err := c.writer.Write(header); err != nil {
		return err
	}
	_, err := c.writer.Write(frames)
	return err
}

func (c *streamConn) Flush() error { return c.writer.Flush() }

type closeWriter interface{ CloseWrite() error }

func (c *streamConn) CloseWrite() error {
	if err := c.writer.Flush(); err != nil {
		return err
	}
	if tlsConn, ok := c.conn.(*tls.Conn); ok {
		// tls.Conn sends close_notify but leaves the transport open.
		if err := tlsConn.CloseWrite(); err != nil {
			return err
		}
		if raw, ok := tlsConn.NetConn().(closeWriter); ok {
			return raw.CloseWrite()
		}
		return nil
	}
	if raw, ok := c.conn.(closeWriter); ok {
		return raw.CloseWrite()
	}
	return nil
}

func (c *streamConn) Close() error { return c.conn.Close() }

// messageConn carries one record per message. Message boundaries replace the
// record Size field.
type messageConn struct {
	conn          MessageConn
	maxRecordSize uint64
}

// NewMessageConn carries records as messages, as the WebSocket binding does.
// Messages larger than maxRecordSize are refused.
func NewMessageConn(conn MessageConn, maxRecordSize uint64) RecordConn {
	return &messageConn{conn: conn, maxRecordSize: maxRecordSize}
}

func (c *messageConn) ReadRecord() ([]byte, error) {
	message, err := c.conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	if uint64(len(message)) > c.maxRecordSize {
		return nil, protocolError(FrameEncodingError, 0, "message of %d bytes exceeds max_record_size %d", len(message), c.maxRecordSize)
	}
	return message, nil
}

func (c *messageConn) WriteRecord(frames []byte) error { return c.conn.WriteMessage(frames) }

func (c *messageConn) Flush() error { return nil }

// CloseWrite closes the whole connection: the WebSocket closing handshake
// replaces QMux draining.
func (c *messageConn) CloseWrite() error { return c.conn.Close() }

func (c *messageConn) Close() error { return c.conn.CloseNow() }
