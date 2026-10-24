package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
)

// worker runs one operation at a time on behalf of a single benchmark worker.
// do returns the number of application messages the operation carried.
type worker interface {
	do(ctx context.Context) (msgs int, err error)
	close()
}

// newWorker returns the worker of the scenario. payload supplies its request
// messages, a new one for every request.
func newWorker(ctx context.Context, client echoClient, cfg benchConfig, payload benchPayload) worker {
	switch cfg.scenario {
	case "unary":
		return &unaryWorker{client: client, payload: payload}
	case "server-stream":
		return &serverStreamWorker{client: client, payload: payload, messages: cfg.messages}
	case "client-stream":
		return &clientStreamWorker{client: client, payload: payload, messages: cfg.messages}
	default:
		w := &bidiWorker{client: client, payload: payload, parent: ctx}
		_ = w.open() // an error here surfaces on the first do, which reopens
		return w
	}
}

func checkSize(got, want int) error {
	if got != want {
		return fmt.Errorf("response message is %d bytes, want %d", got, want)
	}
	return nil
}

type unaryWorker struct {
	client  echoClient
	payload benchPayload
}

func (w *unaryWorker) do(ctx context.Context) (int, error) {
	req := &echopb.EchoRequest{Message: w.payload.next()}
	resp, err := w.client.Unary(ctx, req)
	if err != nil {
		return 0, err
	}
	return 1, checkSize(len(resp.GetMessage()), len(req.GetMessage()))
}

func (*unaryWorker) close() {}

type serverStreamWorker struct {
	client   echoClient
	payload  benchPayload
	messages int
}

func (w *serverStreamWorker) do(ctx context.Context) (int, error) {
	req := &echopb.EchoRequest{Message: w.payload.next(), Count: uint32(w.messages)}
	stream, err := w.client.ServerStream(ctx, req)
	if err != nil {
		return 0, err
	}
	received := 0
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if err := checkSize(len(resp.GetMessage()), len(req.GetMessage())); err != nil {
			return 0, err
		}
		received++
	}
	if received != w.messages {
		return 0, fmt.Errorf("received %d responses, want %d", received, w.messages)
	}
	return received, nil
}

func (*serverStreamWorker) close() {}

type clientStreamWorker struct {
	client   echoClient
	payload  benchPayload
	messages int
}

func (w *clientStreamWorker) do(ctx context.Context) (int, error) {
	stream, err := w.client.ClientStream(ctx)
	if err != nil {
		return 0, err
	}
	sent := 0
	for range w.messages {
		req := &echopb.EchoRequest{Message: w.payload.next()}
		sent += len(req.GetMessage())
		if err := stream.Send(req); err != nil {
			// Send reports io.EOF when the stream has ended; the status is
			// available from the receive side.
			if _, rerr := stream.CloseAndRecv(); rerr != nil {
				return 0, rerr
			}
			return 0, err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if err := checkSize(len(resp.GetMessage()), sent); err != nil {
		return 0, err
	}
	if int(resp.GetIndex()) != w.messages {
		return 0, fmt.Errorf("server counted %d requests, want %d", resp.GetIndex(), w.messages)
	}
	return w.messages, nil
}

func (*clientStreamWorker) close() {}

// bidiWorker keeps one Bidi stream open for the whole run, reopening it only
// after an error.
type bidiWorker struct {
	client  echoClient
	payload benchPayload
	parent  context.Context

	stream bidiStream
	cancel context.CancelFunc
	next   uint32 // index the next echo must carry
}

func (w *bidiWorker) open() error {
	ctx, cancel := context.WithCancel(w.parent)
	stream, err := w.client.Bidi(ctx)
	if err != nil {
		cancel()
		return err
	}
	w.stream, w.cancel, w.next = stream, cancel, 0
	return nil
}

func (w *bidiWorker) reset() {
	if w.cancel != nil {
		w.cancel()
	}
	w.stream, w.cancel = nil, nil
}

func (w *bidiWorker) do(context.Context) (int, error) {
	if w.stream == nil {
		if err := w.open(); err != nil {
			return 0, err
		}
	}
	req := &echopb.EchoRequest{Message: w.payload.next()}
	if err := w.stream.Send(req); err != nil {
		w.reset()
		return 0, err
	}
	resp, err := w.stream.Recv()
	if err == nil {
		err = checkSize(len(resp.GetMessage()), len(req.GetMessage()))
	}
	if err == nil && resp.GetIndex() != w.next {
		err = fmt.Errorf("echo index %d, want %d", resp.GetIndex(), w.next)
	}
	if err != nil {
		w.reset()
		return 0, err
	}
	w.next++
	return 1, nil
}

func (w *bidiWorker) close() {
	if w.stream != nil {
		_ = w.stream.CloseSend()
	}
	w.reset()
}
