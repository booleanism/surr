package logging

import "sync"

// Cleanups is a concurrency-safe registry of exit hooks. It replaces the
// hand-rolled `beforeExit []func() error` slices: those were appended to by
// service goroutines while the cleanup goroutine ranged over them, which the
// race detector flags (and the by-value variants never ran their closers at
// all). Register from any goroutine; Run drains exactly once.
type Cleanups struct {
	pending []func() error
	once    sync.Once
	mu      sync.Mutex
}

// Register appends a cleanup hook. Safe to call concurrently with Run —
// hooks registered after the drain starts are executed within it.
func (c *Cleanups) Register(f func() error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pending = append(c.pending, f)
}

// Run drains every registered hook in order and reports each failure via
// the returned slice. It executes at most once; later calls are no-ops that
// return nil (the drain's result stays available on the first call's return).
func (c *Cleanups) Run() []error {
	var errs []error

	c.once.Do(func() {
		c.mu.Lock()
		hooks := c.pending
		c.pending = nil
		c.mu.Unlock()

		for _, f := range hooks {
			if err := f(); err != nil {
				errs = append(errs, err)
			}
		}
	})

	return errs
}
