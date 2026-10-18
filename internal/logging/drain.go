package logging

import "context"

// DrainCleanups cancels ctx and waits for the goroutine that runs Cleanups to
// finish. Call it before returning from main.
//
// Both halves are load-bearing, and leaving out the cancel is a hang rather than
// an error. The drain goroutine is parked on `<-ctx.Done()`, so a caller that
// waits on its `done` channel without cancelling waits forever — which is what
// happened on the *success* path of `surr-<service> migrate`: the command printed
// its summary, then hung, and in a deploy script a process that never exits looks
// exactly like a slow migration. The failure path was fine only because it called
// cancel() on the way to os.Exit(1).
//
// The context is cancelled even though main is about to return: a deferred cancel
// in main runs after this call, which is too late for the goroutine it is waiting
// on.
func DrainCleanups(cancel context.CancelFunc, done <-chan struct{}) {
	cancel()
	<-done
}
