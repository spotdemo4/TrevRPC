package trevrpc

import (
	"errors"
	"io"
	"slices"
	"testing"
)

func TestMessages(t *testing.T) {
	t.Run("complete stream", func(t *testing.T) {
		stream := &iteratorTestStream{items: []int{1, 2}}
		var got []int

		for message, err := range Messages(stream) {
			if err != nil {
				t.Fatalf("Messages() error = %v", err)
			}
			got = append(got, message)
		}

		if !slices.Equal(got, []int{1, 2}) {
			t.Fatalf("Messages() = %v, want [1 2]", got)
		}
		if stream.recvCalls != 3 {
			t.Fatalf("Recv() calls = %d, want 3", stream.recvCalls)
		}
		if stream.closeCalls != 1 {
			t.Fatalf("Close() calls = %d, want 1", stream.closeCalls)
		}
	})

	t.Run("terminal error", func(t *testing.T) {
		terminalErr := errors.New("stream failed")
		stream := &iteratorTestStream{items: []int{1}, terminalErr: terminalErr}
		var got []int
		var gotErr error

		for message, err := range Messages(stream) {
			if err != nil {
				gotErr = err
				continue
			}
			got = append(got, message)
		}

		if !slices.Equal(got, []int{1}) {
			t.Fatalf("Messages() = %v, want [1]", got)
		}
		if !errors.Is(gotErr, terminalErr) {
			t.Fatalf("Messages() error = %v, want %v", gotErr, terminalErr)
		}
		if stream.recvCalls != 2 {
			t.Fatalf("Recv() calls = %d, want 2", stream.recvCalls)
		}
		if stream.closeCalls != 1 {
			t.Fatalf("Close() calls = %d, want 1", stream.closeCalls)
		}
	})

	t.Run("early exit", func(t *testing.T) {
		stream := &iteratorTestStream{items: []int{1, 2}}

		for _, err := range Messages(stream) {
			if err != nil {
				t.Fatalf("Messages() error = %v", err)
			}
			break
		}

		if stream.recvCalls != 1 {
			t.Fatalf("Recv() calls = %d, want 1", stream.recvCalls)
		}
		if stream.closeCalls != 1 {
			t.Fatalf("Close() calls = %d, want 1", stream.closeCalls)
		}
	})
}

type iteratorTestStream struct {
	items       []int
	next        int
	terminalErr error
	recvCalls   int
	closeCalls  int
}

func (s *iteratorTestStream) Recv() (int, error) {
	s.recvCalls++
	if s.next < len(s.items) {
		message := s.items[s.next]
		s.next++
		return message, nil
	}
	if s.terminalErr != nil {
		return 0, s.terminalErr
	}
	return 0, io.EOF
}

func (s *iteratorTestStream) Close() error {
	s.closeCalls++
	return nil
}
