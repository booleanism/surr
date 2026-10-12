package srt

import (
	"errors"
	"testing"

	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

func newTestBroadcaster(max int64) *broadcaster {
	return NewBroadcaster(max, zerolog.Nop())
}

func testBuf(t *testing.T) *[]byte {
	t.Helper()

	buf := pools.BytesPool.Get().(*[]byte)
	*buf = (*buf)[:cap(*buf)]

	return buf
}

// The cap counts node subscriptions, not subscription maps. Counting maps let
// one subscription admit any number of nodes while rejecting the (max+1)-th
// subscription, so a single camera could overrun the configured worker budget.
func TestTheSubscriberCapCountsNodesNotTopics(t *testing.T) {
	b := newTestBroadcaster(2)
	subID := uuid.New()

	b.Open(subID)

	if _, err := b.Subscribe(subID, uuid.New()); err != nil {
		t.Fatalf("first subscriber refused: %v", err)
	}

	if _, err := b.Subscribe(subID, uuid.New()); err != nil {
		t.Fatalf("second subscriber refused: %v", err)
	}

	_, err := b.Subscribe(subID, uuid.New())
	if !errors.Is(err, errMaxSubscribers) {
		t.Fatalf("third subscriber: got %v, want %v", err, errMaxSubscribers)
	}
}

// A topic belongs to a publisher, so there is nothing to attach to once it is
// closed. Recreating one here handed the node a channel that was never fed and
// never closed.
func TestSubscribeRefusesWithoutAPublisher(t *testing.T) {
	b := newTestBroadcaster(8)
	subID := uuid.New()

	_, err := b.Subscribe(subID, uuid.New())
	if !errors.Is(err, errNoPublisher) {
		t.Fatalf("got %v, want %v", err, errNoPublisher)
	}

	if len(b.topic) != 0 {
		t.Fatalf("Subscribe created %d topics; want none", len(b.topic))
	}
}

// A reconnecting node gets a new channel, and the old one has to be closed or
// the previous handleCaller stays blocked on it for the lifetime of the
// process. The replacement must not cost a second slot against the cap either.
func TestSubscribingANodeAgainClosesThePreviousChannel(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()
	nodeID := uuid.New()

	b.Open(subID)

	first, err := b.Subscribe(subID, nodeID)
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}

	second, err := b.Subscribe(subID, nodeID)
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	select {
	case _, ok := <-first.ch:
		if ok {
			t.Fatal("the replaced channel delivered a value")
		}
	default:
		t.Fatal("the replaced channel is still open")
	}

	select {
	case <-second.ch:
		t.Fatal("the replacement channel is closed")
	default:
	}

	if b.count != 1 {
		t.Fatalf("count is %d after a reconnect; want 1", b.count)
	}
}

// The previous handleCaller's deferred Unsubscribe runs after the replacement
// is installed, so it must not detach it.
func TestUnsubscribingAReplacedHandleLeavesTheReplacement(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()
	nodeID := uuid.New()

	b.Open(subID)

	first, err := b.Subscribe(subID, nodeID)
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}

	second, err := b.Subscribe(subID, nodeID)
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	b.Unsubscribe(first)

	if got := b.topic[subID].subs[nodeID]; got != second {
		t.Fatalf("the replacement was detached: got %p, want %p", got, second)
	}

	if b.count != 1 {
		t.Fatalf("count is %d; want 1", b.count)
	}
}

// Close is matched against the handle Open returned, so a stale handle cannot
// tear down the topic a later publisher opened under the same subscription id.
func TestClosingAStaleTopicIsANoOp(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()

	stale := b.Open(subID)
	b.Close(stale)

	live := b.Open(subID)
	if live == stale {
		t.Fatal("Open returned the closed handle")
	}

	b.Close(stale)

	if got := b.topic[subID]; got != live {
		t.Fatalf("the stale handle closed the live topic: got %p, want %p", got, live)
	}
}

// Closing a topic has to end every subscriber's range loop and release the
// chunks still queued for it, or a publisher disconnect leaks one pooled buffer
// per undelivered frame.
func TestCloseEndsEverySubscriberAndReleasesQueuedChunks(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()
	other := uuid.New()

	topic := b.Open(subID)

	first, err := b.Subscribe(subID, uuid.New())
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}

	second, err := b.Subscribe(subID, other)
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	buf := testBuf(t)
	b.Publish(subID, buf, 4)

	b.Close(topic)

	if _, ok := <-first.ch; ok {
		t.Fatal("first subscriber's channel is still open")
	}

	if _, ok := <-second.ch; ok {
		t.Fatal("second subscriber's channel is still open")
	}

	if len(*buf) != 0 {
		t.Fatalf("the queued buffer was not released: len is %d", len(*buf))
	}

	if b.count != 0 {
		t.Fatalf("count is %d after Close; want 0", b.count)
	}

	if _, has := b.topic[subID]; has {
		t.Fatal("the topic is still registered")
	}
}

// Every subscriber receives the same chunk and the last release returns the
// buffer to the pool, which is what keeps the media path copy-free.
func TestPublishSharesOneChunkAndReleasesItOnce(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()

	b.Open(subID)

	first, err := b.Subscribe(subID, uuid.New())
	if err != nil {
		t.Fatalf("first subscribe: %v", err)
	}

	second, err := b.Subscribe(subID, uuid.New())
	if err != nil {
		t.Fatalf("second subscribe: %v", err)
	}

	buf := testBuf(t)
	b.Publish(subID, buf, 4)

	a := <-first.ch
	c := <-second.ch

	if a != c {
		t.Fatal("subscribers received different chunks; the frame was copied")
	}

	if a.refs != 2 {
		t.Fatalf("refs is %d; want 2", a.refs)
	}

	a.release()

	if len(*buf) == 0 {
		t.Fatal("the buffer was released before the last subscriber")
	}

	c.release()

	if len(*buf) != 0 {
		t.Fatalf("the buffer was not released: len is %d", len(*buf))
	}
}

// With no subscriber the frame is dropped immediately rather than queued
// forever, and the buffer still goes back.
func TestPublishWithoutSubscribersReleasesTheBuffer(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()

	b.Open(subID)

	buf := testBuf(t)
	b.Publish(subID, buf, 4)

	if len(*buf) != 0 {
		t.Fatalf("the buffer was not released: len is %d", len(*buf))
	}
}

// A slow consumer is dropped from, not blocked on, and the drop is counted so
// the operator can see it.
func TestPublishDropsAndCountsASlowConsumer(t *testing.T) {
	b := newTestBroadcaster(4)
	subID := uuid.New()
	nodeID := uuid.New()

	b.Open(subID)

	sub, err := b.Subscribe(subID, nodeID)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	defer b.Unsubscribe(sub)

	// Fill the queue, then one more.
	for range subscriberBuffer + 1 {
		b.Publish(subID, testBuf(t), 4)
	}

	if got := b.dropped[subID][nodeID]; got != 1 {
		t.Fatalf("dropped count is %d; want 1", got)
	}

	if got := len(sub.ch); got != subscriberBuffer {
		t.Fatalf("queued %d chunks; want %d", got, subscriberBuffer)
	}
}
