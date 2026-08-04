package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

const protocolVersion = 1

type metadataEntry struct {
	KeyHex   string `json:"key_hex"`
	ValueHex string `json:"value_hex"`
}

type normalizedRequest struct {
	Type         string          `json:"type"`
	ServiceHex   string          `json:"service_hex"`
	MethodHex    string          `json:"method_hex"`
	BodyHex      string          `json:"body_hex"`
	Metadata     []metadataEntry `json:"metadata"`
	Kind         string          `json:"kind"`
	Version      string          `json:"version"`
	TimeoutNanos string          `json:"timeout_nanos"`
}

type normalizedResponse struct {
	Type       string          `json:"type"`
	StatusRaw  string          `json:"status_raw"`
	StatusCode uint32          `json:"status_code"`
	MessageHex string          `json:"message_hex"`
	BodyHex    string          `json:"body_hex"`
	Metadata   []metadataEntry `json:"metadata"`
}

type normalizedStreamFrame struct {
	Type       string          `json:"type"`
	Kind       string          `json:"kind"`
	KindRaw    string          `json:"kind_raw"`
	StatusRaw  string          `json:"status_raw"`
	StatusCode uint32          `json:"status_code"`
	MessageHex string          `json:"message_hex"`
	BodyHex    string          `json:"body_hex"`
	Metadata   []metadataEntry `json:"metadata"`
}

type runCommand struct {
	Sequence     string
	CaseID       string
	Operation    string
	MessageType  string
	Message      any
	Body         []byte
	MaxFrameSize int
	Chunks       [][]byte
	Frames       [][]byte
}

type fieldParser struct {
	fields []string
	next   int
}

func parseCommand(line string) (*runCommand, bool, error) {
	if line == "STOP" {
		return nil, true, nil
	}
	if strings.ContainsRune(line, '\r') || !isASCII(line) {
		return nil, false, errors.New("command must be LF-terminated tab-delimited ASCII")
	}
	fields := strings.Split(line, "\t")
	if len(fields) < 4 || fields[0] != "RUN" {
		return nil, false, errors.New("expected RUN command")
	}
	if _, err := parseDecimal(fields[1], 64); err != nil {
		return nil, false, fmt.Errorf("invalid sequence: %w", err)
	}
	if !validID(fields[2]) {
		return nil, false, errors.New("invalid case ID")
	}
	parser := &fieldParser{fields: fields, next: 4}
	command := &runCommand{Sequence: fields[1], CaseID: fields[2], Operation: fields[3]}
	var err error
	switch command.Operation {
	case "codec.encode":
		command.MessageType, err = parser.token()
		if err == nil {
			command.Message, err = parser.message(command.MessageType)
		}
	case "codec.decode":
		command.MessageType, err = parser.messageType()
		if err == nil {
			command.Body, err = parser.hexBytes()
		}
	case "framing.encode":
		command.MessageType, err = parser.messageType()
		if err == nil {
			command.MaxFrameSize, err = parser.nonnegativeInt()
		}
		if err == nil {
			command.Message, err = parser.message(command.MessageType)
		}
	case "framing.decode_stream":
		command.MessageType, err = parser.messageType()
		if err == nil {
			command.MaxFrameSize, err = parser.nonnegativeInt()
		}
		if err == nil {
			command.Chunks, err = parser.hexList()
		}
	case "state.server_stream", "state.client_stream":
		command.Frames, err = parser.hexList()
	default:
		return nil, false, fmt.Errorf("unknown operation %q", command.Operation)
	}
	if err != nil {
		return nil, false, err
	}
	if parser.next != len(parser.fields) {
		return nil, false, errors.New("unexpected command fields")
	}
	return command, false, nil
}

func (p *fieldParser) token() (string, error) {
	if p.next >= len(p.fields) {
		return "", errors.New("missing command field")
	}
	value := p.fields[p.next]
	p.next++
	return value, nil
}

func (p *fieldParser) messageType() (string, error) {
	value, err := p.token()
	if err != nil {
		return "", err
	}
	if value != "rpc_request" && value != "rpc_response" && value != "rpc_stream_frame" {
		return "", fmt.Errorf("unknown message type %q", value)
	}
	return value, nil
}

func (p *fieldParser) hexBytes() ([]byte, error) {
	value, err := p.token()
	if err != nil {
		return nil, err
	}
	return decodeLowerHex(value)
}

func (p *fieldParser) nonnegativeInt() (int, error) {
	value, err := p.token()
	if err != nil {
		return 0, err
	}
	parsed, err := parseDecimal(value, strconv.IntSize)
	if err != nil {
		return 0, fmt.Errorf("invalid integer: %w", err)
	}
	return int(parsed), nil
}

func (p *fieldParser) remaining() int { return len(p.fields) - p.next }

func (p *fieldParser) hexList() ([][]byte, error) {
	count, err := p.nonnegativeInt()
	if err != nil {
		return nil, err
	}
	if count > p.remaining() {
		return nil, errors.New("list count exceeded remaining command fields")
	}
	values := make([][]byte, count)
	for index := range values {
		values[index], err = p.hexBytes()
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}

func (p *fieldParser) message(messageType string) (any, error) {
	switch messageType {
	case "rpc_request":
		service, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		method, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		body, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		metadata, entries, err := p.metadata()
		if err != nil {
			return nil, err
		}
		kind, err := p.token()
		if err != nil {
			return nil, err
		}
		kindValue, ok := map[string]trevrpc.RpcKind{"unary": trevrpc.RpcKindUnary, "client_stream": trevrpc.RpcKindClientStreaming, "server_stream": trevrpc.RpcKindServerStreaming, "bidi": trevrpc.RpcKindBidirectionalStreaming}[kind]
		if !ok {
			return nil, fmt.Errorf("unknown RPC kind %q", kind)
		}
		versionText, err := p.token()
		if err != nil {
			return nil, err
		}
		version, err := parseDecimal(versionText, 32)
		if err != nil {
			return nil, err
		}
		timeoutText, err := p.token()
		if err != nil {
			return nil, err
		}
		timeout, err := parseDecimal(timeoutText, 64)
		if err != nil {
			return nil, err
		}
		return &trevrpc.RpcRequest{Service: string(service), Method: string(method), Body: body, Metadata: metadata, Kind: kindValue, Version: uint32(version), TimeoutNanos: timeout}, validateMetadataOrder(entries)
	case "rpc_response":
		statusText, err := p.token()
		if err != nil {
			return nil, err
		}
		status, err := parseDecimal(statusText, 32)
		if err != nil {
			return nil, err
		}
		message, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		body, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		metadata, entries, err := p.metadata()
		if err != nil {
			return nil, err
		}
		return &trevrpc.RpcResponse{Status: uint32(status), Message: string(message), Body: body, Metadata: metadata}, validateMetadataOrder(entries)
	case "rpc_stream_frame":
		kindText, err := p.token()
		if err != nil {
			return nil, err
		}
		kind, err := parseDecimal(kindText, 32)
		if err != nil {
			return nil, err
		}
		statusText, err := p.token()
		if err != nil {
			return nil, err
		}
		status, err := parseDecimal(statusText, 32)
		if err != nil {
			return nil, err
		}
		message, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		body, err := p.hexBytes()
		if err != nil {
			return nil, err
		}
		metadata, entries, err := p.metadata()
		if err != nil {
			return nil, err
		}
		return &trevrpc.RpcStreamFrame{Kind: trevrpc.RpcStreamFrameKind(kind), Status: uint32(status), Message: string(message), Body: body, Metadata: metadata}, validateMetadataOrder(entries)
	default:
		return nil, fmt.Errorf("unknown message type %q", messageType)
	}
}

func (p *fieldParser) metadata() (trevrpc.Metadata, []metadataEntry, error) {
	count, err := p.nonnegativeInt()
	if err != nil {
		return nil, nil, err
	}
	if count > p.remaining()/2 {
		return nil, nil, errors.New("metadata count exceeded remaining command fields")
	}
	metadata := trevrpc.Metadata{}
	entries := make([]metadataEntry, 0, count)
	for range count {
		key, err := p.hexBytes()
		if err != nil {
			return nil, nil, err
		}
		value, err := p.hexBytes()
		if err != nil {
			return nil, nil, err
		}
		entry := metadataEntry{KeyHex: hex.EncodeToString(key), ValueHex: hex.EncodeToString(value)}
		entries = append(entries, entry)
		metadata[string(key)] = value
	}
	return metadata, entries, nil
}

func parseDecimal(value string, bits int) (uint64, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, errors.New("non-canonical decimal")
	}
	for index := range len(value) {
		if value[index] < '0' || value[index] > '9' {
			return 0, errors.New("non-decimal value")
		}
	}
	return strconv.ParseUint(value, 10, bits)
}

func decodeLowerHex(value string) ([]byte, error) {
	if len(value)%2 != 0 {
		return nil, errors.New("hex value has odd length")
	}
	for index := range len(value) {
		if !((value[index] >= '0' && value[index] <= '9') || (value[index] >= 'a' && value[index] <= 'f')) {
			return nil, errors.New("hex value must be lowercase")
		}
	}
	return hex.DecodeString(value)
}

func validID(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		byteValue := value[index]
		if !((byteValue >= 'a' && byteValue <= 'z') || (byteValue >= '0' && byteValue <= '9') || byteValue == '.' || byteValue == '_' || byteValue == '-') {
			return false
		}
	}
	return true
}

func isASCII(value string) bool {
	for index := range len(value) {
		if value[index] > 0x7f {
			return false
		}
	}
	return true
}

func validateMetadataOrder(entries []metadataEntry) error {
	for index := 1; index < len(entries); index++ {
		if entries[index-1].KeyHex >= entries[index].KeyHex {
			return errors.New("metadata entries must be key-sorted and unique")
		}
	}
	return nil
}
