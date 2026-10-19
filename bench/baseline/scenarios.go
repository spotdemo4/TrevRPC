package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"trev.zip/llc/trevrpc/bench/baseline/echopb"
)

// worker runs one operation at a time on behalf of a single benchmark worker.
// do returns the number of application messages the operation carried.
type worker interface {
	do(ctx context.Context) (msgs int, err error)
	close()
}

func newWorker(ctx context.Context, client echoClient, cfg benchConfig) worker {
	req := &echopb.EchoRequest{Message: strings.Repeat("x", cfg.size)}
	switch cfg.scenario {
	case "unary":
		return &unaryWorker{client: client, req: req}
	case "server-stream":
		req.Count = uint32(cfg.messages)
		return &serverStreamWorker{client: client, req: req, messages: cfg.messages}
	case "client-stream":
		return &clientStreamWorker{client: client, req: req, messages: cfg.messages}
	default:
		w := &bidiWorker{client: client, req: req, parent: ctx}
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
	client echoClient
	req    *echopb.EchoRequest
}

func (w *unaryWorker) do(ctx context.Context) (int, error) {
	resp, err := w.client.Unary(ctx, w.req)
	if err != nil {
		return 0, err
	}
	return 1, checkSize(len(resp.GetMessage()), len(w.req.GetMessage()))
}

func (*unaryWorker) close() {}

type serverStreamWorker struct {
	client   echoClient
	req      *echopb.EchoRequest
	messages int
}

func (w *serverStreamWorker) do(ctx context.Context) (int, error) {
	stream, err := w.client.ServerStream(ctx, w.req)
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
		if err := checkSize(len(resp.GetMessage()), len(w.req.GetMessage())); err != nil {
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
	req      *echopb.EchoRequest
	messages int
}

func (w *clientStreamWorker) do(ctx context.Context) (int, error) {
	stream, err := w.client.ClientStream(ctx)
	if err != nil {
		return 0, err
	}
	for range w.messages {
		if err := stream.Send(w.req); err != nil {
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
	if err := checkSize(len(resp.GetMessage()), w.messages*len(w.req.GetMessage())); err != nil {
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
	client echoClient
	req    *echopb.EchoRequest
	parent context.Context

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
	if err := w.stream.Send(w.req); err != nil {
		w.reset()
		return 0, err
	}
	resp, err := w.stream.Recv()
	if err == nil {
		err = checkSize(len(resp.GetMessage()), len(w.req.GetMessage()))
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
