package srt

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// dropSampleInterval controls how often a slow-consumer drop is logged:
// the first drop plus every Nth drop carries a warn with the running
// dropped count, so a stalled consumer cannot become a log flood.
const dropSampleInterval = 1000

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

type broadcaster struct {
	topic   map[uuid.UUID]map[uuid.UUID]chan *Chunk
	dropped map[uuid.UUID]map[uuid.UUID]int64
	log     zerolog.Logger
	max     int64

	dropMu sync.Mutex

	mu sync.RWMutex
}

func NewBroadcaster(max int64, log zerolog.Logger) *broadcaster {
	return &broadcaster{
		topic:   make(map[uuid.UUID]map[uuid.UUID]chan *Chunk),
		max:     max,
		log:     log,
		dropped: make(map[uuid.UUID]map[uuid.UUID]int64),
	}
}

func (b *broadcaster) Subscribe(subID, nodeID uuid.UUID) (<-chan *Chunk, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if int64(len(b.topic)) == b.max {
		return nil, fmt.Errorf("maximum subscriber reached: %d", b.max)
	}

	ch := make(chan *Chunk, 256)

	if b.topic[subID] == nil {
		b.topic[subID] = make(map[uuid.UUID]chan *Chunk)
	}

	b.topic[subID][nodeID] = ch
	return ch, nil
}

func (b *broadcaster) Unsubscribe(subID, nodeID uuid.UUID) {
	b.mu.Lock()
	subs, ok := b.topic[subID]
	if !ok {
		b.mu.Unlock()
		return
	}

	ch, ok := subs[nodeID]
	if !ok {
		b.mu.Unlock()
		return
	}

	delete(subs, nodeID)
	if len(subs) == 0 {
		delete(b.topic, subID)
	}

	b.dropMu.Lock()
	if _, ok = b.dropped[subID]; ok {
		delete(b.dropped[subID], nodeID)
		if len(b.dropped[subID]) == 0 {
			delete(b.dropped, subID)
		}
	}
	b.dropMu.Unlock()

	close(ch)
	b.mu.Unlock()

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

	subs := b.topic[subID]
	if len(subs) == 0 {
		pools.BytesPoolResetAndPut(buf)
		return
	}

	chunk := &Chunk{Buf: buf, N: n, refs: int64(len(subs))}
	for nodeID, ch := range subs {
		select {
		case ch <- chunk:
		default:
			b.drop(subID, nodeID)
			chunk.release()
		}
	}
}

// Close tears down all subscribers of subID, e.g. when the publisher disconnects.
func (b *broadcaster) Close(subID uuid.UUID) {
	b.mu.Lock()
	subs := b.topic[subID]
	delete(b.topic, subID)
	b.mu.Unlock()

	b.dropMu.Lock()
	delete(b.dropped, subID)
	b.dropMu.Unlock()

	for _, ch := range subs {
		close(ch)
		for chunk := range ch {
			chunk.release()
		}
	}
}
