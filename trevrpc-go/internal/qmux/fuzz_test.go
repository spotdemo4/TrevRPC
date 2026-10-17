package qmux

import (
	"context"
	"testing"
)

type nopRecordConn struct{}

func (nopRecordConn) ReadRecord() ([]byte, error) { select {} }
func (nopRecordConn) WriteRecord([]byte) error    { return nil }
func (nopRecordConn) Flush() error                { return nil }
func (nopRecordConn) CloseWrite() error           { return nil }
func (nopRecordConn) Close() error                { return nil }

// fuzzSession is a server session whose records are fed by the test.
func fuzzSession() *Session {
	ctx, cancel := context.WithCancelCause(context.Background())
	params := testParameters()
	return &Session{
		conn:           nopRecordConn{},
		role:           RoleServer,
		version:        &Draft02,
		local:          params,
		ctx:            ctx,
		cancel:         cancel,
		handshakeDone:  make(chan struct{}),
		wake:           make(chan struct{}, 1),
		readerDone:     make(chan struct{}),
		writerDone:     make(chan struct{}),
		sendBuffer:     defaultStreamSendBuffer,
		streams:        map[uint64]*Stream{},
		maxRemote:      params.InitialMaxStreamsBidi,
		connRecvMax:    params.InitialMaxData,
		connRecvWindow: params.InitialMaxData,
		peer:           Parameters{MaxRecordSize: DefaultMaxRecordSize},
	}
}

func FuzzRecord(f *testing.F) {
	params := appendTransportParameters(nil, &Draft02, testParameters())
	f.Add(params, appendStreamFrame(nil, 0, 0, []byte("hi"), true))
	f.Add(params, appendResetStream(appendStreamFrame(nil, 4, 0, []byte("abc"), false), 4, 1, 3))
	f.Add(params, appendStopSending(appendMaxStreamData(nil, 0, 10), 0, 1))
	f.Add(params, appendConnectionClose(nil, nil, 0, "bye"))
	f.Add(params, appendPing(appendMaxData(nil, 1<<30), Draft02.PingType, 1))
	f.Add([]byte{0x02}, []byte{})

	f.Fuzz(func(t *testing.T, first, second []byte) {
		session := fuzzSession()
		defer session.cancel(nil)
		for _, record := range [][]byte{first, second} {
			if err := session.handleRecord(record); err != nil {
				return
			}
		}
		session.mu.Lock()
		defer session.mu.Unlock()
		if session.connConsumed > session.connReceived || session.connReceived > session.connRecvMax {
			t.Fatalf("connection flow control out of order: consumed %d, received %d, limit %d",
				session.connConsumed, session.connReceived, session.connRecvMax)
		}
		for _, stream := range session.streams {
			if stream.read > stream.received || stream.received > stream.recvMax {
				t.Fatalf("stream %d flow control out of order: read %d, received %d, limit %d",
					stream.id, stream.read, stream.received, stream.recvMax)
			}
			if uint64(len(stream.recvBuf)-stream.recvHead) != stream.received-stream.read {
				t.Fatalf("stream %d buffers %d bytes, want %d", stream.id, len(stream.recvBuf)-stream.recvHead, stream.received-stream.read)
			}
		}
	})
}
