package srt

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// The two ways Subscribe can refuse a node. They are sentinels so the caller
// can turn them into an SRT rejection reason that tells the node which one it
// was: "come back when the camera is up" and "come back when there is room"
// call for different behaviour, and a peer cannot guess from a bare failure.
var (
	errNoPublisher    = errors.New("no publisher for subscription")
	errMaxSubscribers = errors.New("maximum subscriber reached")
)

const (
	// dropSampleInterval controls how often a slow-consumer drop is logged:
	// the first drop plus every Nth drop carries a warn with the running
	// dropped count, so a stalled consumer cannot become a log flood.
	dropSampleInterval = 1000

	// subscriberBuffer is the per-subscriber queue depth. A subscriber that
	// falls this far behind is dropped from, not blocked by, Publish.
	subscriberBuffer = 256
)

type Chunk struct {
	Buf  *[]byte
	N    int
	refs int64
}

func (c *Chunk) release() {
	if atomic.AddInt64(&c.refs, -1) == 0 {
		pools.BytesPoolResetAndPut(c.Buf)
	}
}

// subscription is one node's channel within a topic, plus the identity that
// lets Unsubscribe tell it apart from a newer connection for the same node.
//
// The identity is the whole point: a node that reconnects gets a new
// subscription, and the previous handleCaller's deferred Unsubscribe must not
// remove the replacement when it finally returns.
//
// Field order is not free: ch is the only pointer-bearing field, so it comes
// first and the two UUID arrays follow, which is the layout govet's
// fieldalignment asks for (the GC then scans one word instead of five).
type subscription struct {
	ch     chan *Chunk
	subID  uuid.UUID
	nodeID uuid.UUID
}

// topic is one subscription's fan-out set. Its lifetime is a publisher's
// connection: Open creates it, Close tears it down, and Subscribe refuses to
// attach to anything else.
type topic struct {
	subs  map[uuid.UUID]*subscription
	subID uuid.UUID
}

type broadcaster struct {
	topic   map[uuid.UUID]*topic
	dropped map[uuid.UUID]map[uuid.UUID]int64
	log     zerolog.Logger

	// max caps the number of live node subscriptions, and count tracks them.
	// Counting topics instead (the previous behaviour) let one subscription
	// admit any number of nodes while rejecting the (max+1)-th subscription.
	max   int64
	count int64

	dropMu sync.Mutex

	mu sync.RWMutex
}

func NewBroadcaster(max int64, log zerolog.Logger) *broadcaster {
	return &broadcaster{
		topic:   make(map[uuid.UUID]*topic),
		max:     max,
		log:     log,
		dropped: make(map[uuid.UUID]map[uuid.UUID]int64),
	}
}

// Open registers a publisher for subID and returns the topic its subscribers
// attach to.
//
// The returned handle is what Close is matched against, so a teardown cannot
// destroy a topic that a later publisher opened under the same subscription id
// -- something the previous Close(subID) had no way to tell apart.
func (b *broadcaster) Open(subID uuid.UUID) *topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.topic[subID]
	if !ok {
		t = &topic{subID: subID, subs: make(map[uuid.UUID]*subscription)}
		b.topic[subID] = t
	}

	return t
}

// Subscribe attaches nodeID to subID's topic.
//
// It fails when no publisher is open for subID. A topic exists only for as long
// as a publisher is connected, so creating one here would hand the node a
// channel that is never fed and never closed: Subscribe after Close used to
// recreate exactly that orphan.
func (b *broadcaster) Subscribe(subID, nodeID uuid.UUID) (*subscription, error) {
	b.mu.Lock()

	t, ok := b.topic[subID]
	if !ok {
		b.mu.Unlock()
		return nil, fmt.Errorf("%w: %s", errNoPublisher, subID)
	}

	if b.count >= b.max {
		b.mu.Unlock()
		return nil, fmt.Errorf("%w: %d", errMaxSubscribers, b.max)
	}

	old := t.subs[nodeID]

	s := &subscription{
		subID:  subID,
		nodeID: nodeID,
		ch:     make(chan *Chunk, subscriberBuffer),
	}

	t.subs[nodeID] = s
	if old == nil {
		b.count++
	}
	b.mu.Unlock()

	// A reconnecting node gets a fresh channel, and the old one is unreachable
	// now: Publish reads the map under the same lock, so nothing can send to it
	// any more. Closing it here ends the previous handleCaller's range loop
	// instead of leaving that goroutine blocked for the lifetime of the
	// process.
	if old != nil {
		drain(old.ch)
	}

	return s, nil
}

// Unsubscribe detaches s and closes its channel.
//
// The channel is closed outside the registry lock. handleCaller drains what is
// left as it closes, and holding the lock across that would stall every Publish
// for as long as the drain takes.
func (b *broadcaster) Unsubscribe(s *subscription) {
	b.mu.Lock()
	t, hasTopic := b.topic[s.subID]
	removed := hasTopic && t.subs[s.nodeID] == s
	if removed {
		delete(t.subs, s.nodeID)
		b.count--
	}
	b.mu.Unlock()

	// A newer connection for this node already replaced the handle: Subscribe
	// closed this channel when it installed the replacement, and the drop
	// counters belong to the replacement now. Both must be left alone -- a
	// second drain would close a closed channel.
	if !removed {
		return
	}

	b.dropMu.Lock()
	if _, has := b.dropped[s.subID]; has {
		delete(b.dropped[s.subID], s.nodeID)
		if len(b.dropped[s.subID]) == 0 {
			delete(b.dropped, s.subID)
		}
	}
	b.dropMu.Unlock()

	drain(s.ch)
}

// Close tears down the topic a publisher opened, e.g. when it disconnects.
//
// It is a no-op when t is not the live topic for its subscription, so a stale
// handle cannot close its replacement.
func (b *broadcaster) Close(t *topic) {
	b.mu.Lock()
	if b.topic[t.subID] != t {
		b.mu.Unlock()
		return
	}

	delete(b.topic, t.subID)
	subs := t.subs
	b.count -= int64(len(subs))
	b.mu.Unlock()

	b.dropMu.Lock()
	delete(b.dropped, t.subID)
	b.dropMu.Unlock()

	for _, s := range subs {
		drain(s.ch)
	}
}

// drain closes ch and releases every chunk still queued on it, so a subscriber
// that goes away cannot leak the pooled buffers it never read.
func drain(ch chan *Chunk) {
	close(ch)

	for chunk := range ch {
		chunk.release()
	}
}

// drop records a slow-consumer chunk drop for (subID, nodeID). It runs only
// on the drop path (never the healthy media path) and logs the first drop
// plus every dropSampleInterval-th drop with the running count.
func (b *broadcaster) drop(subID, nodeID uuid.UUID) {
	b.dropMu.Lock()
	defer b.dropMu.Unlock()

	if b.dropped[subID] == nil {
		b.dropped[subID] = make(map[uuid.UUID]int64)
	}

	b.dropped[subID][nodeID]++
	n := b.dropped[subID][nodeID]
	if n == 1 || n%dropSampleInterval == 0 {
		b.log.Warn().
			Str("sub_id", subID.String()).
			Str("node_id", nodeID.String()).
			Int64("dropped", n).
			Msg("slow consumer drop")
	}
}

// Publish hands buf (len n) to every current subscriber of subID without
// copying. Caller must not touch buf after calling this.
func (b *broadcaster) Publish(subID uuid.UUID, buf *[]byte, n int) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	t := b.topic[subID]
	if t == nil || len(t.subs) == 0 {
		pools.BytesPoolResetAndPut(buf)
		return
	}

	chunk := &Chunk{Buf: buf, N: n, refs: int64(len(t.subs))}
	for nodeID, s := range t.subs {
		select {
		case s.ch <- chunk:
		default:
			b.drop(subID, nodeID)
			chunk.release()
		}
	}
}
