package logging

import (
	"context"
	"testing"
	"time"
)

// TestDrainCleanupsReturnsOnlyBecauseItCancels reproduces main's wiring exactly:
// a goroutine that closes `done` when ctx is cancelled, and a caller that waits on
// `done`. Nothing else cancels the context, so if DrainCleanups stops cancelling,
// this test hangs instead of returning — which is the defect it exists to prevent.
//
// The timeout is what turns that hang into a failure rather than a stuck suite.
func TestDrainCleanupsReturnsOnlyBecauseItCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cleanups := &Cleanups{}
	cleanups.Register(func() error { return nil })

	ran := make(chan struct{})

	go func() {
		<-ctx.Done()
		cleanups.Run()

		close(ran)
	}()

	done := make(chan struct{})

	go func() {
		DrainCleanups(cancel, ran)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("DrainCleanups did not return: it waited on the drain without cancelling the context")
	}
}

// TestDrainCleanupsRunsTheHooks is the other half: the point of waiting is that the
// hooks actually ran, not merely that the goroutine finished.
func TestDrainCleanupsRunsTheHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cleanups := &Cleanups{}

	ran := false
	cleanups.Register(func() error {
		ran = true

		return nil
	})

	drained := make(chan struct{})

	go func() {
		<-ctx.Done()
		cleanups.Run()

		close(drained)
	}()

	DrainCleanups(cancel, drained)

	if !ran {
		t.Fatal("DrainCleanups returned before the cleanup hooks ran")
	}
}
