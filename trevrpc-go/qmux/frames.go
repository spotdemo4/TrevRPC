package qmux

// frame is one decoded QMux frame. Only the fields of its type are set. data,
// reason and params alias the record they were parsed from.
type frame struct {
	typ uint64
	// size is the encoded size of the whole frame, set for DATAGRAM frames.
	size      int
	streamID  uint64
	offset    uint64
	data      []byte
	fin       bool
	errorCode uint64
	finalSize uint64
	// maximum is the limit of MAX_* and *_BLOCKED frames.
	maximum uint64
	// frameType is the frame that triggered a transport CONNECTION_CLOSE.
	frameType uint64
	reason    []byte
	params    []byte
	sequence  uint64
}

// frameParser decodes the frames of one record.
type frameParser struct {
	buf     []byte
	pos     int
	version *Version
}

func (p *frameParser) more() bool { return p.pos < len(p.buf) }

func (p *frameParser) varint(frameType uint64) (uint64, error) {
	value, n, err := parseVarint(p.buf[p.pos:])
	if err != nil {
		return 0, protocolError(FrameEncodingError, frameType, "truncated frame")
	}
	p.pos += n
	return value, nil
}

func (p *frameParser) bytes(frameType uint64, length uint64) ([]byte, error) {
	if length > uint64(len(p.buf)-p.pos) {
		return nil, protocolError(FrameEncodingError, frameType, "truncated frame")
	}
	b := p.buf[p.pos : p.pos+int(length)]
	p.pos += int(length)
	return b, nil
}

// next decodes the next frame. Frames that QMux forbids, and extensions it
// does not implement, are FRAME_ENCODING_ERRORs.
func (p *frameParser) next() (frame, error) {
	start := p.pos
	typ, err := p.varint(0)
	if err != nil {
		return frame{}, err
	}
	f := frame{typ: typ}
	switch {
	case typ == frameTypePadding:
		return f, nil
	case typ >= frameTypeStream && typ <= frameTypeStream|0x07:
		return p.streamFrame(f)
	case typ == frameTypeResetStream:
		return p.fields(&f, &f.streamID, &f.errorCode, &f.finalSize)
	case typ == frameTypeStopSending:
		return p.fields(&f, &f.streamID, &f.errorCode)
	case typ == frameTypeMaxData, typ == frameTypeDataBlocked,
		typ == frameTypeMaxStreamsBidi, typ == frameTypeMaxStreamsUni,
		typ == frameTypeStreamsBlockedBidi, typ == frameTypeStreamsBlockedUni:
		return p.fields(&f, &f.maximum)
	case typ == frameTypeMaxStreamData, typ == frameTypeStreamDataBlocked:
		return p.fields(&f, &f.streamID, &f.maximum)
	case typ == frameTypeConnectionClose, typ == frameTypeApplicationClose:
		return p.closeFrame(f)
	case typ == frameTypeDatagram, typ == frameTypeDatagramLen:
		length := uint64(len(p.buf) - p.pos)
		if typ == frameTypeDatagramLen {
			if length, err = p.varint(typ); err != nil {
				return f, err
			}
		}
		f.data, err = p.bytes(typ, length)
		f.size = p.pos - start
		return f, err
	case typ == p.version.TransportParametersType:
		length, err := p.varint(typ)
		if err != nil {
			return f, err
		}
		f.params, err = p.bytes(typ, length)
		return f, err
	case typ == p.version.PingType, typ == p.version.PingResponseType:
		return p.fields(&f, &f.sequence)
	default:
		return f, protocolError(FrameEncodingError, typ, "frame type 0x%x is not allowed in QMux", typ)
	}
}

// fields decodes varint fields of f in order.
func (p *frameParser) fields(f *frame, fields ...*uint64) (frame, error) {
	for _, field := range fields {
		value, err := p.varint(f.typ)
		if err != nil {
			return *f, err
		}
		*field = value
	}
	return *f, nil
}

func (p *frameParser) streamFrame(f frame) (frame, error) {
	var err error
	if f.streamID, err = p.varint(f.typ); err != nil {
		return f, err
	}
	if f.typ&streamFlagOff != 0 {
		if f.offset, err = p.varint(f.typ); err != nil {
			return f, err
		}
	}
	length := uint64(len(p.buf) - p.pos)
	if f.typ&streamFlagLen != 0 {
		if length, err = p.varint(f.typ); err != nil {
			return f, err
		}
	}
	if f.data, err = p.bytes(f.typ, length); err != nil {
		return f, err
	}
	if f.offset+uint64(len(f.data)) > maxVarint {
		return f, protocolError(FrameEncodingError, f.typ, "stream data beyond the maximum offset")
	}
	f.fin = f.typ&streamFlagFin != 0
	return f, nil
}

func (p *frameParser) closeFrame(f frame) (frame, error) {
	var err error
	if f.errorCode, err = p.varint(f.typ); err != nil {
		return f, err
	}
	if f.typ == frameTypeConnectionClose {
		if f.frameType, err = p.varint(f.typ); err != nil {
			return f, err
		}
	}
	length, err := p.varint(f.typ)
	if err != nil {
		return f, err
	}
	f.reason, err = p.bytes(f.typ, length)
	return f, err
}

// streamFrameOverhead is the size of a STREAM frame header carrying length
// bytes of data at offset.
func streamFrameOverhead(streamID, offset uint64, length int) int {
	n := 1 + varintLen(streamID) + varintLen(uint64(length))
	if offset > 0 {
		n += varintLen(offset)
	}
	return n
}

// appendStreamFrame appends a STREAM frame. It always carries an explicit
// length so that more frames may follow in the same record.
func appendStreamFrame(b []byte, streamID, offset uint64, data []byte, fin bool) []byte {
	typ := uint64(frameTypeStream | streamFlagLen)
	if offset > 0 {
		typ |= streamFlagOff
	}
	if fin {
		typ |= streamFlagFin
	}
	b = appendVarint(b, typ)
	b = appendVarint(b, streamID)
	if offset > 0 {
		b = appendVarint(b, offset)
	}
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

func appendResetStream(b []byte, streamID, errorCode, finalSize uint64) []byte {
	b = appendVarint(b, frameTypeResetStream)
	b = appendVarint(b, streamID)
	b = appendVarint(b, errorCode)
	return appendVarint(b, finalSize)
}

func appendStopSending(b []byte, streamID, errorCode uint64) []byte {
	b = appendVarint(b, frameTypeStopSending)
	b = appendVarint(b, streamID)
	return appendVarint(b, errorCode)
}

func appendMaxData(b []byte, maximum uint64) []byte {
	b = appendVarint(b, frameTypeMaxData)
	return appendVarint(b, maximum)
}

func appendMaxStreamData(b []byte, streamID, maximum uint64) []byte {
	b = appendVarint(b, frameTypeMaxStreamData)
	b = appendVarint(b, streamID)
	return appendVarint(b, maximum)
}

// appendMaxStreams appends MAX_STREAMS for bidirectional streams (dir
// dirBidi) or unidirectional ones (dirUni).
func appendMaxStreams(b []byte, dir int, maximum uint64) []byte {
	b = appendVarint(b, frameTypeMaxStreamsBidi+uint64(dir))
	return appendVarint(b, maximum)
}

// datagramFrameSize is the size of the DATAGRAM frame appendDatagram encodes
// for a payload of length bytes.
func datagramFrameSize(length int) int { return 1 + varintLen(uint64(length)) + length }

// maxDatagramPayload is the largest payload whose DATAGRAM frame fits in
// frameSize bytes, or -1 if none does.
func maxDatagramPayload(frameSize int) int {
	if frameSize < datagramFrameSize(0) {
		return -1
	}
	// The length field of frameSize is at least as long as the payload's, so
	// this payload fits; a shorter length field may leave room for more.
	n := frameSize - 1 - varintLen(uint64(frameSize))
	for datagramFrameSize(n+1) <= frameSize {
		n++
	}
	return n
}

// appendDatagram appends a DATAGRAM frame. It always carries an explicit
// length so that more frames may follow in the same record.
func appendDatagram(b []byte, data []byte) []byte {
	b = appendVarint(b, frameTypeDatagramLen)
	b = appendVarint(b, uint64(len(data)))
	return append(b, data...)
}

// maxReasonLength bounds the reason phrase of a CONNECTION_CLOSE frame, so
// that the frame always fits in a record.
const maxReasonLength = 256

// appendConnectionClose appends an application CONNECTION_CLOSE, or a
// transport one when transportErr is set. The reason is cut to
// maxReasonLength bytes.
func appendConnectionClose(b []byte, transportErr *TransportError, errorCode uint64, reason string) []byte {
	reason = reason[:min(len(reason), maxReasonLength)]
	if transportErr != nil {
		b = appendVarint(b, frameTypeConnectionClose)
		b = appendVarint(b, uint64(transportErr.ErrorCode))
		b = appendVarint(b, transportErr.FrameType)
	} else {
		b = appendVarint(b, frameTypeApplicationClose)
		b = appendVarint(b, errorCode)
	}
	b = appendVarint(b, uint64(len(reason)))
	return append(b, reason...)
}

func appendPing(b []byte, frameType, sequence uint64) []byte {
	b = appendVarint(b, frameType)
	return appendVarint(b, sequence)
}
