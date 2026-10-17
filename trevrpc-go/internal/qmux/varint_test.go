package qmux

import "github.com/quic-go/quic-go/quicvarint"

func appendVarint(b []byte, value uint64) []byte { return quicvarint.Append(b, value) }
