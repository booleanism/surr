// Package backoff produces the delays used by the services' restart loops.
//
// Every long-running routine (broker consumer, SRT listener, HTTP listener) is
// wrapped in a loop that re-enters on failure. Without a delay, a closed broker
// connection or a taken port turns that loop into a hot CPU/log spin with the
// service dead until somebody restarts it.
package backoff

import (
	"context"
	"time"
)

const (
	// DefaultBase is the delay before the first retry.
	DefaultBase = 250 * time.Millisecond
	// DefaultMax caps the delay so recovery stays reasonably prompt.
	DefaultMax = 30 * time.Second
	// jitterPrime de-synchronises services that failed at the same instant.
	// It is deterministic, so no random source (and no gosec exemption) is
	// needed for a value that only spreads load.
	jitterPrime = 7919
)

// Backoff yields exponentially growing delays capped at Max.
type Backoff struct {
	base    time.Duration
	max     time.Duration
	cur     time.Duration
	attempt int
}

// New returns a Backoff starting at base and capped at max. Non-positive
// values fall back to the package defaults.
func New(base, max time.Duration) *Backoff {
	if base <= 0 {
		base = DefaultBase
	}

	if max <= 0 {
		max = DefaultMax
	}

	if max < base {
		max = base
	}

	return &Backoff{base: base, max: max}
}

// Next returns the next delay without sleeping.
func (b *Backoff) Next() time.Duration {
	if b.cur == 0 {
		b.cur = b.base
	}

	d := b.cur
	if b.cur < b.max {
		b.cur *= 2
		if b.cur > b.max {
			b.cur = b.max
		}
	}

	b.attempt++

	// Spread by up to a quarter of the delay without ever shortening it to
	// zero. The spread is applied after the cap, so it is clamped back: the
	// maximum is a ceiling, not a suggestion.
	d += time.Duration(b.attempt*jitterPrime) % (d/4 + 1)
	if d > b.max {
		d = b.max
	}

	return d
}

// Reset restarts the sequence at base.
func (b *Backoff) Reset() {
	b.cur = 0
	b.attempt = 0
}

// Wait sleeps for the next delay, returning early with ctx.Err() when ctx is
// cancelled.
func (b *Backoff) Wait(ctx context.Context) error {
	timer := time.NewTimer(b.Next())
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
