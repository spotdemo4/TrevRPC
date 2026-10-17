package qmux

import (
	"time"

	"github.com/quic-go/quic-go/quicvarint"
)

// Parameters are the transport parameters an endpoint advertises in its
// QX_TRANSPORT_PARAMETERS frame. Unidirectional streams are never offered.
type Parameters struct {
	// MaxIdleTimeout closes a session after this long without a record in
	// either direction. Zero disables the local timeout.
	MaxIdleTimeout time.Duration
	// InitialMaxData is the connection-level receive window.
	InitialMaxData uint64
	// InitialMaxStreamDataBidiLocal is the receive window of streams this
	// endpoint opens.
	InitialMaxStreamDataBidiLocal uint64
	// InitialMaxStreamDataBidiRemote is the receive window of streams the peer
	// opens.
	InitialMaxStreamDataBidiRemote uint64
	// InitialMaxStreamsBidi is how many bidirectional streams the peer may open.
	InitialMaxStreamsBidi uint64
	// MaxRecordSize is the largest record this endpoint accepts. Zero means
	// DefaultMaxRecordSize.
	MaxRecordSize uint64
}

func (p Parameters) maxRecordSize() uint64 {
	if p.MaxRecordSize == 0 {
		return DefaultMaxRecordSize
	}
	return p.MaxRecordSize
}

// appendTransportParameters appends the QX_TRANSPORT_PARAMETERS frame that
// advertises p.
func appendTransportParameters(b []byte, version *Version, p Parameters) []byte {
	var body []byte
	integer := func(id, value uint64) {
		if value == 0 {
			return
		}
		body = quicvarint.Append(body, id)
		body = quicvarint.Append(body, uint64(quicvarint.Len(value)))
		body = quicvarint.Append(body, value)
	}
	integer(paramMaxIdleTimeout, uint64(p.MaxIdleTimeout/time.Millisecond))
	integer(paramInitialMaxData, p.InitialMaxData)
	integer(paramInitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiLocal)
	integer(paramInitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataBidiRemote)
	integer(paramInitialMaxStreamsBidi, p.InitialMaxStreamsBidi)
	integer(version.MaxRecordSizeParameter, p.maxRecordSize())

	b = quicvarint.Append(b, version.TransportParametersType)
	b = quicvarint.Append(b, uint64(len(body)))
	return append(b, body...)
}

// decodeParameters decodes the body of a peer's QX_TRANSPORT_PARAMETERS frame.
// Parameters that only make sense over QUIC packets are refused, and unknown
// ones are ignored.
func decodeParameters(body []byte, version *Version) (Parameters, error) {
	frameType := version.TransportParametersType
	paramErr := func(format string, args ...any) error {
		return protocolError(TransportParameterError, frameType, format, args...)
	}
	p := Parameters{MaxRecordSize: DefaultMaxRecordSize}
	seen := map[uint64]bool{}
	for len(body) > 0 {
		id, n, err := quicvarint.Parse(body)
		if err != nil {
			return p, paramErr("truncated transport parameter ID")
		}
		body = body[n:]
		length, n, err := quicvarint.Parse(body)
		if err != nil || length > uint64(len(body)-n) {
			return p, paramErr("truncated transport parameter 0x%x", id)
		}
		value := body[n : n+int(length)]
		body = body[n+int(length):]
		if seen[id] {
			return p, paramErr("duplicate transport parameter 0x%x", id)
		}
		seen[id] = true

		var field *uint64
		var idle uint64
		switch id {
		case paramOriginalDestinationConnectionID, paramStatelessResetToken, paramMaxUDPPayloadSize,
			paramAckDelayExponent, paramMaxAckDelay, paramDisableActiveMigration, paramPreferredAddress,
			paramActiveConnectionIDLimit, paramInitialSourceConnectionID, paramRetrySourceConnectionID:
			return p, paramErr("transport parameter 0x%x is not allowed in QMux", id)
		case paramMaxIdleTimeout:
			field = &idle
		case paramInitialMaxData:
			field = &p.InitialMaxData
		case paramInitialMaxStreamDataBidiLocal:
			field = &p.InitialMaxStreamDataBidiLocal
		case paramInitialMaxStreamDataBidiRemote:
			field = &p.InitialMaxStreamDataBidiRemote
		case paramInitialMaxStreamsBidi:
			field = &p.InitialMaxStreamsBidi
		case version.MaxRecordSizeParameter:
			field = &p.MaxRecordSize
		case paramInitialMaxStreamDataUni, paramInitialMaxStreamsUni:
			// TrevRPC never opens unidirectional streams, so these do not matter,
			// but they must still be well formed.
			field = new(uint64)
		default:
			continue
		}
		parsed, n, err := quicvarint.Parse(value)
		if err != nil || n != len(value) {
			return p, paramErr("malformed transport parameter 0x%x", id)
		}
		*field = parsed
		if id == paramMaxIdleTimeout {
			if parsed > uint64(time.Duration(1<<63-1)/time.Millisecond) {
				parsed = uint64(time.Duration(1<<63-1) / time.Millisecond)
			}
			p.MaxIdleTimeout = time.Duration(parsed) * time.Millisecond
		}
	}
	if p.MaxRecordSize < DefaultMaxRecordSize {
		return p, paramErr("max_record_size %d is below %d", p.MaxRecordSize, DefaultMaxRecordSize)
	}
	if p.InitialMaxStreamsBidi > maxStreamsLimit {
		return p, paramErr("initial_max_streams_bidi %d exceeds 2^60", p.InitialMaxStreamsBidi)
	}
	return p, nil
}
