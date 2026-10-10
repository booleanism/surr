package middlewares

import (
	"sync"
	"time"
)

// RateLimiter is a fixed-window limiter keyed by an arbitrary string.
//
// It is deliberately small: the hub has a single signin endpoint to protect,
// and the key is the client address. A key that is behind a reverse proxy is
// shared by every client behind it, which is the standard trade-off for not
// trusting forwarded headers.
type RateLimiter struct {
	window    map[string]*rateWindow
	now       func() time.Time
	lastSweep time.Time
	limit     int
	span      time.Duration
	mu        sync.Mutex
}

type rateWindow struct {
	start time.Time
	count int
}

// NewRateLimiter returns a limiter that allows limit events per span for each
// key.
func NewRateLimiter(limit int, span time.Duration) *RateLimiter {
	return &RateLimiter{
		now:    time.Now,
		window: make(map[string]*rateWindow),
		limit:  limit,
		span:   span,
	}
}

// Allow reports whether key may proceed. When it may not, the returned
// duration is how long the caller should wait before retrying.
func (l *RateLimiter) Allow(key string) (bool, time.Duration) {
	now := l.now()

	l.mu.Lock()
	defer l.mu.Unlock()

	l.sweep(now)

	w, ok := l.window[key]
	if !ok || now.Sub(w.start) >= l.span {
		l.window[key] = &rateWindow{start: now, count: 1}
		return true, 0
	}

	if w.count >= l.limit {
		return false, l.span - now.Sub(w.start)
	}

	w.count++
	return true, 0
}

// sweep drops expired windows. It runs at most once per span, so a caller
// cycling keys cannot turn every Allow into a full map walk.
func (l *RateLimiter) sweep(now time.Time) {
	if !l.lastSweep.IsZero() && now.Sub(l.lastSweep) < l.span {
		return
	}

	l.lastSweep = now

	for k, w := range l.window {
		if now.Sub(w.start) >= l.span {
			delete(l.window, k)
		}
	}
}
