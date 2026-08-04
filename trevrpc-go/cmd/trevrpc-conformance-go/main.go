package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
)

const (
	maxCommandBytes = 262_144
	maxEventBytes   = 65_536
)

var errEventTooLarge = errors.New("encoded event exceeded limit")

var capabilities = []string{
	"codec.decode",
	"codec.encode",
	"framing.decode_stream",
	"framing.encode",
	"state.client_stream",
	"state.server_stream",
}

func main() {
	if len(os.Args) != 3 || os.Args[1] != "--protocol" || os.Args[2] != "1" {
		fmt.Fprintln(os.Stderr, "usage: trevrpc-conformance-go --protocol 1")
		os.Exit(2)
	}
	writer := bufio.NewWriter(os.Stdout)
	emit := func(event any) error {
		return emitEvent(writer, event)
	}
	if err := emit(map[string]any{
		"schema_version": protocolVersion,
		"event":          "ready",
		"peer":           "go",
		"pid":            os.Getpid(),
		"capabilities":   capabilities,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	reader := bufio.NewReaderSize(os.Stdin, maxCommandBytes+1)
	for {
		line, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			fatal(emit, "command line exceeded limit")
		}
		if err != nil {
			if len(line) == 0 {
				fatal(emit, "controller input ended without STOP")
			}
			fatal(emit, "command was not LF-terminated")
		}
		line = line[:len(line)-1]
		command, stop, parseErr := parseCommand(string(line))
		if parseErr != nil {
			fatal(emit, parseErr.Error())
		}
		if stop {
			return
		}
		payload, operationErr := dispatch(command)
		result := map[string]any{
			"schema_version": protocolVersion,
			"event":          "result",
			"peer":           "go",
			"sequence":       command.Sequence,
			"case_id":        command.CaseID,
			"operation":      command.Operation,
		}
		maps.Copy(result, payload)
		if operationErr != nil {
			result["outcome"] = "error"
			result["category"] = operationErr.category
			result["status_code"] = operationErr.statusCode
			fmt.Fprintf(os.Stderr, "%s %s: %v\n", command.CaseID, operationErr.category, operationErr.native)
		} else {
			result["outcome"] = "success"
		}
		if err := emit(result); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
}

func emitEvent(writer *bufio.Writer, event any) error {
	var encoded bytes.Buffer
	if err := json.NewEncoder(&encoded).Encode(event); err != nil {
		return err
	}
	if encoded.Len() > maxEventBytes {
		return errEventTooLarge
	}
	if _, err := writer.Write(encoded.Bytes()); err != nil {
		return err
	}
	return writer.Flush()
}

func dispatch(command *runCommand) (map[string]any, *conformanceError) {
	switch command.Operation {
	case "codec.encode":
		return codecEncode(command.Message, 4*1024*1024)
	case "codec.decode":
		return codecDecode(command.MessageType, command.Body)
	case "framing.encode":
		return codecEncode(command.Message, command.MaxFrameSize)
	case "framing.decode_stream":
		return framingDecode(command.MessageType, command.MaxFrameSize, command.Chunks)
	case "state.server_stream":
		return runServerState(command.Frames)
	case "state.client_stream":
		return runClientState(command.Frames)
	default:
		return nil, internalError(fmt.Errorf("unknown operation %q", command.Operation))
	}
}

func fatal(emit func(any) error, message string) {
	_ = emit(map[string]any{
		"schema_version": protocolVersion,
		"event":          "fatal",
		"peer":           "go",
		"message":        message,
	})
	fmt.Fprintln(os.Stderr, message)
	os.Exit(2)
}
