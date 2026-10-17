package trevrpc

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultFallbackDelay   = 300 * time.Millisecond
	defaultReprobeInterval = 5 * time.Minute
)

// racingConnector connects with the primary transport (QUIC) and, when it has
// not connected within delay or has failed, races the fallback (QMux over TLS)
// against it. The first to connect wins and the other is abandoned.
//
// Once the fallback wins, reconnects use it directly until reprobe has passed
// since the primary last failed, so a network that blocks UDP does not delay
// every reconnect.
type racingConnector struct {
	primary  channelConnector
	fallback channelConnector
	delay    time.Duration
	reprobe  time.Duration
	now      func() time.Time

	mu              sync.Mutex
	primaryFailedAt time.Time // zero while the primary is preferred
}

func newRacingConnector(primary, fallback channelConnector, delay, reprobe time.Duration) *racingConnector {
	if delay <= 0 {
		delay = defaultFallbackDelay
	}
	if reprobe <= 0 {
		reprobe = defaultReprobeInterval
	}
	return &racingConnector{primary: primary, fallback: fallback, delay: delay, reprobe: reprobe, now: time.Now}
}

type connectAttempt struct {
	generation channelGeneration
	err        error
	primary    bool
}

func (r *racingConnector) Connect(ctx context.Context) (channelGeneration, error) {
	r.mu.Lock()
	failedAt := r.primaryFailedAt
	r.mu.Unlock()
	if !failedAt.IsZero() && r.now().Sub(failedAt) < r.reprobe {
		return r.fallback.Connect(ctx)
	}

	winner, err := r.race(ctx)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if winner.primary {
		r.primaryFailedAt = time.Time{}
	} else {
		r.primaryFailedAt = r.now()
	}
	r.mu.Unlock()
	return winner.generation, nil
}

func (r *racingConnector) race(parent context.Context) (connectAttempt, error) {
	ctx, cancel := context.WithCancel(parent)
	results := make(chan connectAttempt, 2)
	attempt := func(connector channelConnector, primary bool) {
		generation, err := connector.Connect(ctx)
		results <- connectAttempt{generation: generation, err: err, primary: primary}
	}

	go attempt(r.primary, true)
	pending := 1
	fallbackStarted := false
	startFallback := func() {
		if !fallbackStarted {
			fallbackStarted = true
			pending++
			go attempt(r.fallback, false)
		}
	}
	timer := time.NewTimer(r.delay)
	defer timer.Stop()

	var errs []error
	for pending > 0 {
		select {
		case <-timer.C:
			startFallback()
		case result := <-results:
			pending--
			if result.err == nil {
				cancel()
				abandon(results, pending)
				return result, nil
			}
			errs = append(errs, result.err)
			if result.primary {
				startFallback()
			}
		case <-parent.Done():
			cancel()
			abandon(results, pending)
			return connectAttempt{}, statusFromContextError(parent.Err())
		}
	}
	cancel()
	return connectAttempt{}, Unavailable("transport unavailable: " + errors.Join(errs...).Error())
}

// abandon closes whatever the remaining attempts connect.
func abandon(results <-chan connectAttempt, pending int) {
	if pending == 0 {
		return
	}
	go func() {
		for range pending {
			if result := <-results; result.err == nil {
				_ = result.generation.Close()
			}
		}
	}()
}
