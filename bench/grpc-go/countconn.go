package main

import (
	"context"
	"net"
	"sync/atomic"
)

// wireCounter totals the bytes a client's TCP connections send and receive.
//
// Wire bytes are measured with a net.Conn wrapper installed through
// grpc.WithContextDialer, below the TLS layer that grpc-go adds on top of the
// dialed connection. It therefore counts the TLS records on the TCP stream
// (HTTP/2 frames, TLS record headers and authentication tags, handshake and
// session tickets), the closest analogue of the UDP payload bytes the QUIC
// clients report: neither includes IP/TCP/UDP headers or ACK-only packets.
// grpc-go's stats.Handler was not used because its WireLength covers only the
// gRPC message payload and framing, not HTTP/2 or TLS overhead.
type wireCounter struct {
	read, written atomic.Uint64
}

// total reports every byte transferred in either direction so far.
func (c *wireCounter) total() uint64 {
	return c.read.Load() + c.written.Load()
}

// dial dials addr over TCP and counts the connection's traffic.
func (c *wireCounter) dial(ctx context.Context, addr string) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return &countingConn{Conn: conn, counter: c}, nil
}

type countingConn struct {
	net.Conn
	counter *wireCounter
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.counter.read.Add(uint64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.counter.written.Add(uint64(n))
	return n, err
}
