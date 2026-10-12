package srt

import (
	"sync"
	"time"
)

// pendingTTL bounds how long a handshake may sit between the SRT listen
// callback and Accept.
//
// libsrt calls the listen callback when a connection request arrives and
// returns the accepted socket from Accept() once the handshake completes; the
// two are matched by the peer's address. A request that is abandoned in
// between -- the peer gives up, or the handshake fails after the callback said
// yes -- produces no Accept, so nothing ever removes its entry. Without a TTL
// the map grows for the lifetime of the process, one entry per abandoned
// handshake.
const pendingTTL = 30 * time.Second

// pendingMax caps the registry independently of the TTL, so a burst of
// requests inside one TTL window cannot grow it without bound.
const pendingMax = 4096

type pendingEntry[V any] struct {
	value  V
	queued time.Time
}

// pending is the handshake registry: what a listen callback learned about a
// peer, waiting for Accept to collect it.
type pending[V any] struct {
	m   map[string]pendingEntry[V]
	now func() time.Time
	ttl time.Duration
	max int

	mu sync.Mutex
}

func newPending[V any]() *pending[V] {
	return &pending[V]{
		m:   make(map[string]pendingEntry[V]),
		now: time.Now,
		ttl: pendingTTL,
		max: pendingMax,
	}
}

// put records what was learned about peer, first discarding the entries whose
// handshake never completed. It reports false when the registry is full, which
// the caller turns into a rejection: refusing one connection is better than
// admitting it and having nowhere to put it.
func (p *pending[V]) put(peer string, v V) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.sweepLocked()

	if _, ok := p.m[peer]; !ok && len(p.m) >= p.max {
		return false
	}

	p.m[peer] = pendingEntry[V]{value: v, queued: p.now()}

	return true
}

// take removes and returns what was recorded for peer.
func (p *pending[V]) take(peer string) (V, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	e, ok := p.m[peer]
	if !ok {
		var zero V
		return zero, false
	}

	delete(p.m, peer)

	return e.value, true
}

func (p *pending[V]) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.m)
}

// sweepLocked drops the entries that are older than the TTL. The scan runs on
// every put, which is cheap because the registry only ever holds the handshakes
// that are in flight right now.
func (p *pending[V]) sweepLocked() {
	cutoff := p.now().Add(-p.ttl)

	for peer, e := range p.m {
		if e.queued.Before(cutoff) {
			delete(p.m, peer)
		}
	}
}
