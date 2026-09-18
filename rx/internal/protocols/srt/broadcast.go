package srt

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
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

type broadcaster struct {
	topic map[uuid.UUID]map[uuid.UUID]chan *Chunk
	max   int64
	mu    sync.RWMutex
}

func NewBroadcaster(max int64) *broadcaster {
	return &broadcaster{
		topic: make(map[uuid.UUID]map[uuid.UUID]chan *Chunk),
		max:   max,
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

	close(ch)
	b.mu.Unlock()

	for chunk := range ch {
		chunk.release()
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
	for _, ch := range subs {
		select {
		case ch <- chunk:
		default:
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

	for _, ch := range subs {
		close(ch)
		for chunk := range ch {
			chunk.release()
		}
	}
}
