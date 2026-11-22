package qmux

import (
	"time"
)

// Parameters are the transport parameters an endpoint advertises in its
// QX_TRANSPORT_PARAMETERS frame. A zero field is left out of the frame, so it
// takes the protocol default: no credit, no streams and no datagrams.
type Parameters struct {
	// MaxIdleTimeout closes a session after this long without activity. Every
	// record received counts, and so does one record sent after each receipt
	// (RFC 9000 section 10.1), so keep-alives alone can't keep a session to a
	// vanished peer open. While stream data or datagrams the session sent wait
	// for the peer to be heard from, it sends a QX_PING halfway through the
	// timeout, which a live peer answers. Zero disables the local timeout.
	MaxIdleTimeout time.Duration
	// InitialMaxData is the connection-level receive window.
	InitialMaxData uint64
	// InitialMaxStreamDataBidiLocal is the receive window of streams this
	// endpoint opens.
	InitialMaxStreamDataBidiLocal uint64
	// InitialMaxStreamDataBidiRemote is the receive window of streams the peer
	// opens.
	InitialMaxStreamDataBidiRemote uint64
	// InitialMaxStreamDataUni is the receive window of unidirectional streams
	// the peer opens.
	InitialMaxStreamDataUni uint64
	// InitialMaxStreamsBidi is how many bidirectional streams the peer may open
	// at once. The limit rises as they complete.
	InitialMaxStreamsBidi uint64
	// InitialMaxStreamsUni is how many unidirectional streams the peer may open
	// at once. The limit rises as they complete.
	InitialMaxStreamsUni uint64
	// MaxDatagramFrameSize is the largest DATAGRAM frame this endpoint accepts,
	// including its type and length fields (RFC 9221). Zero refuses datagrams.
	// The frame must also fit in a record, so values above MaxRecordSize act as
	// MaxRecordSize.
	MaxDatagramFrameSize uint64
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
		body = appendVarint(body, id)
		body = appendVarint(body, uint64(varintLen(value)))
		body = appendVarint(body, value)
	}
	integer(paramMaxIdleTimeout, uint64(p.MaxIdleTimeout/time.Millisecond))
	integer(paramInitialMaxData, p.InitialMaxData)
	integer(paramInitialMaxStreamDataBidiLocal, p.InitialMaxStreamDataBidiLocal)
	integer(paramInitialMaxStreamDataBidiRemote, p.InitialMaxStreamDataBidiRemote)
	integer(paramInitialMaxStreamDataUni, p.InitialMaxStreamDataUni)
	integer(paramInitialMaxStreamsBidi, p.InitialMaxStreamsBidi)
	integer(paramInitialMaxStreamsUni, p.InitialMaxStreamsUni)
	integer(paramMaxDatagramFrameSize, p.MaxDatagramFrameSize)
	integer(version.MaxRecordSizeParameter, p.maxRecordSize())

	b = appendVarint(b, version.TransportParametersType)
	b = appendVarint(b, uint64(len(body)))
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
		id, n, err := parseVarint(body)
		if err != nil {
			return p, paramErr("truncated transport parameter ID")
		}
		body = body[n:]
		length, n, err := parseVarint(body)
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
		case paramInitialMaxStreamDataUni:
			field = &p.InitialMaxStreamDataUni
		case paramInitialMaxStreamsBidi:
			field = &p.InitialMaxStreamsBidi
		case paramInitialMaxStreamsUni:
			field = &p.InitialMaxStreamsUni
		case paramMaxDatagramFrameSize:
			field = &p.MaxDatagramFrameSize
		case version.MaxRecordSizeParameter:
			field = &p.MaxRecordSize
		default:
			continue
		}
		parsed, n, err := parseVarint(value)
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
	if p.InitialMaxStreamsUni > maxStreamsLimit {
		return p, paramErr("initial_max_streams_uni %d exceeds 2^60", p.InitialMaxStreamsUni)
	}
	return p, nil
}
