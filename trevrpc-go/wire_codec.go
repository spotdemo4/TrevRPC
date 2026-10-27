package trevrpc

import (
	"math"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
)

// This file is the direct protobuf codec of the REQUEST, HEADERS, ANSWER and
// STATUS payloads. Going through the protobuf runtime costs a reflective wrapper per
// message, so the frame functions try these first. The codec accepts only
// what a conforming sender produces and reports everything else as "not
// handled" (ok == false), leaving the reflective path to decode it or to
// produce the error, so both agree on every payload.

// Field numbers of the wire messages (see wireRequest, wireHeaders,
// wireAnswer and wireStatus).
const (
	requestMethodField     protowire.Number = 1
	requestMetadataField   protowire.Number = 2
	requestKindField       protowire.Number = 3
	requestTimeoutField    protowire.Number = 4
	requestDatagramField   protowire.Number = 5
	requestAcceptField     protowire.Number = 6
	requestQueryField      protowire.Number = 7
	requestStreamField     protowire.Number = 8
	requestConnectionField protowire.Number = 9
	headersMetadataField   protowire.Number = 1
	answerDictionaryField  protowire.Number = 1
	answerStreamField      protowire.Number = 2
	answerConnectionField  protowire.Number = 3
	connectionIDField      protowire.Number = 1
	connectionEntriesField protowire.Number = 2
	dictionaryIDField      protowire.Number = 1
	dictionaryFingerField  protowire.Number = 2
	statusCodeField        protowire.Number = 1
	statusMessageField     protowire.Number = 2
	statusMetadataField    protowire.Number = 3
	statusRefusedField     protowire.Number = 4
	mapEntryKeyField       protowire.Number = 1
	mapEntryValueField     protowire.Number = 2
)

// --- Encoding ---

// metadataFieldSize is the encoded size of the map field num holding metadata.
func metadataFieldSize(num protowire.Number, metadata Metadata) int {
	size := 0
	tag := protowire.SizeTag(num)
	for key, value := range metadata {
		entry := mapEntrySize(key, value)
		size += tag + protowire.SizeBytes(entry)
	}
	return size
}

// mapEntrySize is the size of a map entry. Both fields are always encoded,
// even when empty.
func mapEntrySize(key string, value []byte) int {
	return 1 + protowire.SizeBytes(len(key)) + 1 + protowire.SizeBytes(len(value))
}

// metadataValid reports whether every key is valid UTF-8, which a protobuf
// string requires.
func metadataValid(metadata Metadata) bool {
	for key := range metadata {
		if !utf8.ValidString(key) {
			return false
		}
	}
	return true
}

func appendMetadataField(b []byte, num protowire.Number, metadata Metadata) []byte {
	for key, value := range metadata {
		b = protowire.AppendTag(b, num, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(mapEntrySize(key, value)))
		b = protowire.AppendTag(b, mapEntryKeyField, protowire.BytesType)
		b = protowire.AppendString(b, key)
		b = protowire.AppendTag(b, mapEntryValueField, protowire.BytesType)
		b = protowire.AppendBytes(b, value)
	}
	return b
}

func packedUint32sSize(values []uint32) int {
	size := 0
	for _, value := range values {
		size += protowire.SizeVarint(uint64(value))
	}
	return size
}

func appendPackedUint32s(b []byte, num protowire.Number, values []uint32) []byte {
	if len(values) == 0 {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(packedUint32sSize(values)))
	for _, value := range values {
		b = protowire.AppendVarint(b, uint64(value))
	}
	return b
}

func packedUint32sFieldSize(num protowire.Number, values []uint32) int {
	if len(values) == 0 {
		return 0
	}
	return protowire.SizeTag(num) + protowire.SizeBytes(packedUint32sSize(values))
}

// dictionaryInfoSize is the encoded size of a DictionaryInfo message.
func dictionaryInfoSize(info *dictionaryInfo) int {
	size := 0
	if info.ID != 0 {
		size += protowire.SizeTag(dictionaryIDField) + protowire.SizeVarint(uint64(info.ID))
	}
	if len(info.Fingerprint) > 0 {
		size += protowire.SizeTag(dictionaryFingerField) + protowire.SizeBytes(len(info.Fingerprint))
	}
	return size
}

func dictionaryInfosFieldSize(num protowire.Number, infos []*dictionaryInfo) int {
	size := 0
	for _, info := range infos {
		size += protowire.SizeTag(num) + protowire.SizeBytes(dictionaryInfoSize(info))
	}
	return size
}

func appendDictionaryInfos(b []byte, num protowire.Number, infos []*dictionaryInfo) []byte {
	for _, info := range infos {
		b = protowire.AppendTag(b, num, protowire.BytesType)
		b = protowire.AppendVarint(b, uint64(dictionaryInfoSize(info)))
		if info.ID != 0 {
			b = protowire.AppendTag(b, dictionaryIDField, protowire.VarintType)
			b = protowire.AppendVarint(b, uint64(info.ID))
		}
		if len(info.Fingerprint) > 0 {
			b = protowire.AppendTag(b, dictionaryFingerField, protowire.BytesType)
			b = protowire.AppendBytes(b, info.Fingerprint)
		}
	}
	return b
}

// connectionMetadataInfoSize is the encoded size of a ConnectionMetadata
// message.
func connectionMetadataInfoSize(info *connectionMetadataInfo) int {
	size := metadataFieldSize(connectionEntriesField, info.Metadata)
	if info.ID != 0 {
		size += protowire.SizeTag(connectionIDField) + protowire.SizeVarint(uint64(info.ID))
	}
	return size
}

func connectionMetadataFieldSize(num protowire.Number, info *connectionMetadataInfo) int {
	if info == nil {
		return 0
	}
	return protowire.SizeTag(num) + protowire.SizeBytes(connectionMetadataInfoSize(info))
}

func appendConnectionMetadata(b []byte, num protowire.Number, info *connectionMetadataInfo) []byte {
	if info == nil {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	b = protowire.AppendVarint(b, uint64(connectionMetadataInfoSize(info)))
	if info.ID != 0 {
		b = protowire.AppendTag(b, connectionIDField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(info.ID))
	}
	return appendMetadataField(b, connectionEntriesField, info.Metadata)
}

// consumeConnectionMetadata decodes a ConnectionMetadata message, skipping
// unknown fields.
func consumeConnectionMetadata(data []byte) (*connectionMetadataInfo, bool) {
	info := &connectionMetadataInfo{}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 || num > protowire.MaxValidNumber {
			return nil, false
		}
		data = data[n:]
		switch {
		case num == connectionIDField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(data)
			if n < 0 || value > math.MaxUint32 {
				return nil, false
			}
			info.ID = uint32(value)
			data = data[n:]
		case num == connectionEntriesField && typ == protowire.BytesType:
			entry, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, false
			}
			var ok bool
			if info.Metadata, ok = consumeMetadataEntry(info.Metadata, entry); !ok {
				return nil, false
			}
			data = data[n:]
		case num == connectionIDField || num == connectionEntriesField:
			return nil, false
		default:
			n, ok := skipField(num, typ, data)
			if !ok {
				return nil, false
			}
			data = data[n:]
		}
	}
	return info, true
}

// payloadSize returns the encoded size of the request, and false when the
// codec does not handle it.
func (m *wireRequest) payloadSize() (int, bool) {
	if !utf8.ValidString(m.Method) || !metadataValid(m.Metadata) {
		return 0, false
	}
	if m.ConnectionMetadata != nil && !metadataValid(m.ConnectionMetadata.Metadata) {
		return 0, false
	}
	size := metadataFieldSize(requestMetadataField, m.Metadata)
	if m.Method != "" {
		size += protowire.SizeTag(requestMethodField) + protowire.SizeBytes(len(m.Method))
	}
	if m.Kind != 0 {
		size += protowire.SizeTag(requestKindField) + protowire.SizeVarint(uint64(m.Kind))
	}
	if m.TimeoutNanos != 0 {
		size += protowire.SizeTag(requestTimeoutField) + protowire.SizeVarint(m.TimeoutNanos)
	}
	if m.DatagramFlow != 0 {
		size += protowire.SizeTag(requestDatagramField) + protowire.SizeVarint(m.DatagramFlow)
	}
	size += packedUint32sFieldSize(requestAcceptField, m.AcceptDictionaries)
	size += dictionaryInfosFieldSize(requestQueryField, m.DictionaryQuery)
	if m.StreamCompression != 0 {
		size += protowire.SizeTag(requestStreamField) + protowire.SizeVarint(uint64(m.StreamCompression))
	}
	return size + connectionMetadataFieldSize(requestConnectionField, m.ConnectionMetadata), true
}

// appendPayload appends the request in field order. The request must have
// passed payloadSize.
func (m *wireRequest) appendPayload(b []byte) []byte {
	if m.Method != "" {
		b = protowire.AppendTag(b, requestMethodField, protowire.BytesType)
		b = protowire.AppendString(b, m.Method)
	}
	b = appendMetadataField(b, requestMetadataField, m.Metadata)
	if m.Kind != 0 {
		b = protowire.AppendTag(b, requestKindField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.Kind))
	}
	if m.TimeoutNanos != 0 {
		b = protowire.AppendTag(b, requestTimeoutField, protowire.VarintType)
		b = protowire.AppendVarint(b, m.TimeoutNanos)
	}
	if m.DatagramFlow != 0 {
		b = protowire.AppendTag(b, requestDatagramField, protowire.VarintType)
		b = protowire.AppendVarint(b, m.DatagramFlow)
	}
	b = appendPackedUint32s(b, requestAcceptField, m.AcceptDictionaries)
	b = appendDictionaryInfos(b, requestQueryField, m.DictionaryQuery)
	if m.StreamCompression != 0 {
		b = protowire.AppendTag(b, requestStreamField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.StreamCompression))
	}
	return appendConnectionMetadata(b, requestConnectionField, m.ConnectionMetadata)
}

func (m *wireHeaders) payloadSize() (int, bool) {
	if !metadataValid(m.Metadata) {
		return 0, false
	}
	return metadataFieldSize(headersMetadataField, m.Metadata), true
}

func (m *wireHeaders) appendPayload(b []byte) []byte {
	return appendMetadataField(b, headersMetadataField, m.Metadata)
}

func (m *wireAnswer) payloadSize() int {
	size := dictionaryInfosFieldSize(answerDictionaryField, m.Dictionaries)
	if m.StreamCompression {
		size += protowire.SizeTag(answerStreamField) + 1
	}
	if m.ConnectionMetadata != 0 {
		size += protowire.SizeTag(answerConnectionField) + protowire.SizeVarint(uint64(m.ConnectionMetadata))
	}
	return size
}

func (m *wireAnswer) appendPayload(b []byte) []byte {
	b = appendDictionaryInfos(b, answerDictionaryField, m.Dictionaries)
	if m.StreamCompression {
		b = protowire.AppendTag(b, answerStreamField, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	if m.ConnectionMetadata != 0 {
		b = protowire.AppendTag(b, answerConnectionField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.ConnectionMetadata))
	}
	return b
}

func (m *wireStatus) payloadSize() (int, bool) {
	if !utf8.ValidString(m.Message) || !metadataValid(m.Metadata) {
		return 0, false
	}
	size := metadataFieldSize(statusMetadataField, m.Metadata)
	if m.Code != 0 {
		size += protowire.SizeTag(statusCodeField) + protowire.SizeVarint(uint64(m.Code))
	}
	if m.Message != "" {
		size += protowire.SizeTag(statusMessageField) + protowire.SizeBytes(len(m.Message))
	}
	if m.RefusedStreamCompression {
		size += protowire.SizeTag(statusRefusedField) + 1
	}
	return size, true
}

func (m *wireStatus) appendPayload(b []byte) []byte {
	if m.Code != 0 {
		b = protowire.AppendTag(b, statusCodeField, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(m.Code))
	}
	if m.Message != "" {
		b = protowire.AppendTag(b, statusMessageField, protowire.BytesType)
		b = protowire.AppendString(b, m.Message)
	}
	b = appendMetadataField(b, statusMetadataField, m.Metadata)
	if m.RefusedStreamCompression {
		b = protowire.AppendTag(b, statusRefusedField, protowire.VarintType)
		b = protowire.AppendVarint(b, 1)
	}
	return b
}

// --- Decoding ---

// consumeMapEntry decodes one entry of a metadata map. Values are copied, so
// the result does not alias the payload.
func consumeMapEntry(entry []byte) (key string, value []byte, ok bool) {
	value = []byte{}
	for len(entry) > 0 {
		num, typ, n := protowire.ConsumeTag(entry)
		if n < 0 || typ != protowire.BytesType {
			return "", nil, false
		}
		entry = entry[n:]
		data, n := protowire.ConsumeBytes(entry)
		if n < 0 {
			return "", nil, false
		}
		entry = entry[n:]
		switch num {
		case mapEntryKeyField:
			if !utf8.Valid(data) {
				return "", nil, false
			}
			key = string(data)
		case mapEntryValueField:
			value = append([]byte{}, data...)
		default:
			return "", nil, false
		}
	}
	return key, value, true
}

// consumeMetadataEntry decodes the map entry in data into metadata, which it
// allocates when nil.
func consumeMetadataEntry(metadata Metadata, data []byte) (Metadata, bool) {
	key, value, ok := consumeMapEntry(data)
	if !ok {
		return nil, false
	}
	if metadata == nil {
		metadata = make(Metadata, 1)
	}
	metadata[key] = value
	return metadata, true
}

// skipField skips a field of no interest to the decoder, and reports false when it is
// malformed.
func skipField(num protowire.Number, typ protowire.Type, b []byte) (int, bool) {
	n := protowire.ConsumeFieldValue(num, typ, b)
	return n, n >= 0
}

// decodeFast decodes a REQUEST payload into m, which must be empty.
func (m *wireRequest) decodeFast(b []byte) bool {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return false
		}
		b = b[n:]
		switch {
		case num == requestMethodField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 || !utf8.Valid(data) {
				return false
			}
			m.Method = string(data)
			b = b[n:]
		case num == requestMetadataField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return false
			}
			var ok bool
			if m.Metadata, ok = consumeMetadataEntry(m.Metadata, data); !ok {
				return false
			}
			b = b[n:]
		case num == requestKindField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.Kind = RPCKind(int32(value))
			b = b[n:]
		case num == requestTimeoutField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.TimeoutNanos = value
			b = b[n:]
		case num == requestDatagramField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.DatagramFlow = value
			b = b[n:]
		case num == requestAcceptField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return false
			}
			for len(data) > 0 {
				value, n := protowire.ConsumeVarint(data)
				if n < 0 || value > math.MaxUint32 {
					return false
				}
				m.AcceptDictionaries = append(m.AcceptDictionaries, uint32(value))
				data = data[n:]
			}
			b = b[n:]
		case num == requestStreamField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.StreamCompression = streamCompressionMode(int32(value))
			b = b[n:]
		case num == requestConnectionField && typ == protowire.BytesType && m.ConnectionMetadata == nil:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return false
			}
			var ok bool
			if m.ConnectionMetadata, ok = consumeConnectionMetadata(data); !ok {
				return false
			}
			b = b[n:]
		case num == requestMethodField || num == requestMetadataField || num == requestKindField ||
			num == requestTimeoutField || num == requestDatagramField || num == requestAcceptField ||
			num == requestQueryField || num == requestStreamField || num == requestConnectionField:
			// A known field of an unexpected wire type, an unpacked dictionary
			// list or the dictionary query, which a client sends once per
			// connection: all are left to the reflective decoder.
			return false
		default:
			n, ok := skipField(num, typ, b)
			if !ok {
				return false
			}
			b = b[n:]
		}
	}
	return true
}

// decodeFast decodes a HEADERS payload into m, which must be empty.
func (m *wireHeaders) decodeFast(b []byte) bool {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return false
		}
		b = b[n:]
		switch {
		case num == headersMetadataField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return false
			}
			var ok bool
			if m.Metadata, ok = consumeMetadataEntry(m.Metadata, data); !ok {
				return false
			}
			b = b[n:]
		case num == headersMetadataField:
			return false
		default:
			n, ok := skipField(num, typ, b)
			if !ok {
				return false
			}
			b = b[n:]
		}
	}
	return true
}

// decodeFast decodes an ANSWER payload into m, which must be empty. The
// dictionary answer, which a server sends once per connection, is left to the
// reflective decoder.
func (m *wireAnswer) decodeFast(b []byte) bool {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return false
		}
		b = b[n:]
		switch {
		case num == answerStreamField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.StreamCompression = protowire.DecodeBool(value)
			b = b[n:]
		case num == answerConnectionField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 || value > math.MaxUint32 {
				return false
			}
			m.ConnectionMetadata = uint32(value)
			b = b[n:]
		case num == answerDictionaryField || num == answerStreamField || num == answerConnectionField:
			return false
		default:
			n, ok := skipField(num, typ, b)
			if !ok {
				return false
			}
			b = b[n:]
		}
	}
	return true
}

// decodeFast decodes a STATUS payload into m, which must be empty.
func (m *wireStatus) decodeFast(b []byte) bool {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 || num > protowire.MaxValidNumber {
			return false
		}
		b = b[n:]
		switch {
		case num == statusCodeField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.Code = uint32(value)
			b = b[n:]
		case num == statusMessageField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 || !utf8.Valid(data) {
				return false
			}
			m.Message = string(data)
			b = b[n:]
		case num == statusMetadataField && typ == protowire.BytesType:
			data, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return false
			}
			var ok bool
			if m.Metadata, ok = consumeMetadataEntry(m.Metadata, data); !ok {
				return false
			}
			b = b[n:]
		case num == statusRefusedField && typ == protowire.VarintType:
			value, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return false
			}
			m.RefusedStreamCompression = protowire.DecodeBool(value)
			b = b[n:]
		case num == statusCodeField || num == statusMessageField || num == statusMetadataField || num == statusRefusedField:
			return false
		default:
			n, ok := skipField(num, typ, b)
			if !ok {
				return false
			}
			b = b[n:]
		}
	}
	return true
}
