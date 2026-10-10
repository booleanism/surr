// Package restart runs the services' long-running routines.
package restart

import (
	"context"
	"time"

	"github.com/booleanism/surr/internal/backoff"
	"github.com/rs/zerolog"
)

// Loop calls f until ctx is cancelled, waiting with exponential backoff between
// attempts and logging each failure.
//
// Every long-running routine (broker consumer, SRT listener, HTTP listener) is
// wrapped in one of these. Without the wait, a routine that fails immediately —
// broker gone, port already bound — re-enters at full speed: a 100% CPU hot
// spin that floods the log while the service is dead.
func Loop(ctx context.Context, name string, f func(ctx context.Context) error) {
	log := zerolog.Ctx(ctx).With().Str("routine", name).Logger()
	b := backoff.New(0, 0)
	started := false

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if started {
			log.Info().Msg("restarting routine")
		} else {
			log.Info().Msg("routine started")
		}

		begin := time.Now()
		err := f(ctx)
		if ctx.Err() != nil {
			return
		}

		if err != nil {
			log.Error().Err(err).Msg("routine stopped")
		}

		started = true

		// A routine that stayed up longer than the maximum delay was healthy,
		// so the next failure starts from the base delay again instead of
		// waiting out the backoff accumulated during an earlier outage.
		if time.Since(begin) >= backoff.DefaultMax {
			b.Reset()
		}

		err = b.Wait(ctx)
		if err != nil {
			return
		}
	}
}
