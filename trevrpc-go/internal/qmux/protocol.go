// Package qmux implements the subset of QMux (draft-ietf-quic-qmux) that
// TrevRPC needs: bidirectional streams with QUIC flow control and stream
// limits, carried over a reliable byte stream (TLS over TCP) or over
// WebSocket messages (draft-lcurley-qmux-websocket). Unidirectional streams,
// datagrams and 0-RTT are not supported; a peer that tries to use them is
// refused through the transport parameters.
package qmux

import "fmt"

// Version identifies a QMux draft by the codepoints it uses on the wire. QMux
// has no version negotiation of its own: the TLS ALPN or WebSocket subprotocol
// selects the version.
type Version struct {
	// Name is the draft prefix of identifiers that select this version, such as
	// "qmux-02".
	Name string
	// TransportParametersType is the QX_TRANSPORT_PARAMETERS frame type.
	TransportParametersType uint64
	// PingType and PingResponseType are the QX_PING frame types.
	PingType         uint64
	PingResponseType uint64
	// MaxRecordSizeParameter is the max_record_size transport parameter ID.
	MaxRecordSizeParameter uint64
}

// Draft02 is draft-ietf-quic-qmux-02.
var Draft02 = Version{
	Name:                    "qmux-02",
	TransportParametersType: 0x3f5153300d0a0d0a,
	PingType:                0x348c67529ef8c7bd,
	PingResponseType:        0x348c67529ef8c7be,
	MaxRecordSizeParameter:  0x0571c59429cd0845,
}

// DefaultMaxRecordSize is the max_record_size a peer may assume until the
// transport parameters say otherwise, and the smallest value a peer may
// advertise.
const DefaultMaxRecordSize = 16382

// QUIC v1 frame types that QMux carries.
const (
	frameTypePadding                   = 0x00
	frameTypeResetStream               = 0x04
	frameTypeStopSending               = 0x05
	frameTypeStream                    = 0x08 // through 0x0f
	frameTypeMaxData                   = 0x10
	frameTypeMaxStreamData             = 0x11
	frameTypeMaxStreamsBidi            = 0x12
	frameTypeMaxStreamsUni             = 0x13
	frameTypeDataBlocked               = 0x14
	frameTypeStreamDataBlocked         = 0x15
	frameTypeStreamsBlockedBidi        = 0x16
	frameTypeStreamsBlockedUni         = 0x17
	frameTypeConnectionClose           = 0x1c
	frameTypeApplicationClose          = 0x1d
	streamFlagFin                      = 0x01
	streamFlagLen                      = 0x02
	streamFlagOff                      = 0x04
	maxStreamsLimit             uint64 = 1 << 60
)

// Transport parameter IDs (RFC 9000 §18.2).
const (
	paramOriginalDestinationConnectionID = 0x00
	paramMaxIdleTimeout                  = 0x01
	paramStatelessResetToken             = 0x02
	paramMaxUDPPayloadSize               = 0x03
	paramInitialMaxData                  = 0x04
	paramInitialMaxStreamDataBidiLocal   = 0x05
	paramInitialMaxStreamDataBidiRemote  = 0x06
	paramInitialMaxStreamDataUni         = 0x07
	paramInitialMaxStreamsBidi           = 0x08
	paramInitialMaxStreamsUni            = 0x09
	paramAckDelayExponent                = 0x0a
	paramMaxAckDelay                     = 0x0b
	paramDisableActiveMigration          = 0x0c
	paramPreferredAddress                = 0x0d
	paramActiveConnectionIDLimit         = 0x0e
	paramInitialSourceConnectionID       = 0x0f
	paramRetrySourceConnectionID         = 0x10
)

// TransportErrorCode is a QUIC transport error code (RFC 9000 §20.1).
type TransportErrorCode uint64

const (
	NoError                 TransportErrorCode = 0x00
	InternalError           TransportErrorCode = 0x01
	FlowControlError        TransportErrorCode = 0x03
	StreamLimitError        TransportErrorCode = 0x04
	StreamStateError        TransportErrorCode = 0x05
	FinalSizeError          TransportErrorCode = 0x06
	FrameEncodingError      TransportErrorCode = 0x07
	TransportParameterError TransportErrorCode = 0x08
	ProtocolViolation       TransportErrorCode = 0x0a
)

func (c TransportErrorCode) String() string {
	switch c {
	case NoError:
		return "NO_ERROR"
	case InternalError:
		return "INTERNAL_ERROR"
	case FlowControlError:
		return "FLOW_CONTROL_ERROR"
	case StreamLimitError:
		return "STREAM_LIMIT_ERROR"
	case StreamStateError:
		return "STREAM_STATE_ERROR"
	case FinalSizeError:
		return "FINAL_SIZE_ERROR"
	case FrameEncodingError:
		return "FRAME_ENCODING_ERROR"
	case TransportParameterError:
		return "TRANSPORT_PARAMETER_ERROR"
	case ProtocolViolation:
		return "PROTOCOL_VIOLATION"
	default:
		return fmt.Sprintf("0x%x", uint64(c))
	}
}
