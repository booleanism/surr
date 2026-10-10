package srt

import (
	"context"

	"github.com/haivision/srtgo"
)

// listen runs a blocking SRT listener so that ctx cancellation can stop it.
//
// srtgo's Listen and Accept park inside libsrt with no timeout: they return only
// when a connection arrives or the socket is closed. A loop that calls them
// directly therefore cannot observe ctx.Done() — shutting the service down
// would have to wait for the next connection (or be killed), and the parked
// goroutine would outlive the routine that owns it.
//
// Closing the socket is the only thing that wakes libsrt, so the blocking work
// runs on its own goroutine and the socket is closed once ctx is cancelled.
// That is why the caller must not reuse the socket afterwards: on cancellation
// it is spent.
func listen(ctx context.Context, sock *srtgo.SrtSocket, backlog int, accept func() error) error {
	done := make(chan error, 1)

	go func() {
		if err := sock.Listen(backlog); err != nil {
			done <- err
			return
		}

		for {
			if err := accept(); err != nil {
				done <- err
				return
			}
		}
	}()

	select {
	case <-ctx.Done():
		sock.Close()
		return ctx.Err()
	case err := <-done:
		return err
	}
}
