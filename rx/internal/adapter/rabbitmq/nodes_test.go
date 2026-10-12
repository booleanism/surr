package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

// fakeSiblings answers GetName from a fixed name list and counts the lookups,
// so a test can tell "tried every node" from "gave up on the first".
type fakeSiblings struct {
	names   []protocols.SiblingName
	lookups int
}

func (f *fakeSiblings) GetName(_ protocols.Command, sn *[]protocols.SiblingName) int {
	f.lookups++

	*sn = append((*sn)[:0], f.names...)

	return len(f.names)
}

// inlineRunner runs the task on the calling goroutine, which is what the real
// pool does and what makes the send path observable without a broker.
type inlineRunner struct{}

func (inlineRunner) Run(_ context.Context, t pools.TaskFunc) error {
	t()

	return nil
}

// poolRunner runs the task on another goroutine and returns at once, the way
// the real pool does. It is what makes the reply channel observable: an error
// raised inside the task has nowhere to go except that channel.
type poolRunner struct{}

func (poolRunner) Run(_ context.Context, t pools.TaskFunc) error {
	go t()

	return nil
}

type fakeRouter struct {
	sendErr error
	recvErr error
	message string
	code    int
	sends   int
	// block makes Recv wait for the context, which is what a node that is in
	// the sibling registry but gone does.
	block bool
}

func (r *fakeRouter) Send(_ context.Context, _ protocols.Command, _ []byte, _ protocols.SiblingName) (uuid.UUID, error) {
	r.sends++

	return uuid.New(), r.sendErr
}

func (r *fakeRouter) Recv(ctx context.Context, _ uuid.UUID, resp *protocols.RouterResponse) error {
	if r.recvErr != nil {
		return r.recvErr
	}

	if r.block {
		<-ctx.Done()

		return ctx.Err()
	}

	quoted, err := json.Marshal(r.message)
	if err != nil {
		return err
	}

	*resp = protocols.RouterResponse{Code: r.code, Result: quoted}

	return nil
}

func (r *fakeRouter) Forget(_ uuid.UUID) {}

func testNodes(n int) []models.Node {
	nodes := make([]models.Node, n)
	for i := range nodes {
		nodes[i].ID = uuid.New()
	}

	return nodes
}

// StartSink must report a node it could not reach. The caller has already
// accepted the publisher by this point and never dispatches the command again,
// so a swallowed error leaves every component reporting healthy while no
// frames move.
func TestStartSinkReportsANodeThatCouldNotBeReached(t *testing.T) {
	sibs := &fakeSiblings{}
	n := NewNodes(&fakeRouter{}, sibs, inlineRunner{})

	err := n.StartSink(context.Background(), testNodes(1), "rx:4201")
	if err == nil {
		t.Fatal("StartSink returned nil with no sibling registered for the node")
	}

	if !strings.Contains(err.Error(), ".sink.start") {
		t.Fatalf("error does not name the command it failed on: %v", err)
	}
}

func TestStopSinkReportsANodeThatCouldNotBeReached(t *testing.T) {
	sibs := &fakeSiblings{}
	n := NewNodes(&fakeRouter{}, sibs, inlineRunner{})

	err := n.StopSink(context.Background(), testNodes(1))
	if err == nil {
		t.Fatal("StopSink returned nil with no sibling registered for the node")
	}
}

// One unreachable node must not stop rx from starting the others, but the
// failure still has to come back.
func TestStartSinkTriesEveryNodeAndStillReportsFailure(t *testing.T) {
	sibs := &fakeSiblings{}
	n := NewNodes(&fakeRouter{}, sibs, inlineRunner{})

	err := n.StartSink(context.Background(), testNodes(3), "rx:4201")
	if err == nil {
		t.Fatal("StartSink returned nil with no siblings registered")
	}

	if sibs.lookups != 3 {
		t.Fatalf("looked up %d nodes, want 3", sibs.lookups)
	}
}

func TestStartSinkSucceedsWhenTheNodeIsRegistered(t *testing.T) {
	sibs := &fakeSiblings{names: []protocols.SiblingName{"rpc.direct.node0"}}
	router := &fakeRouter{}
	n := NewNodes(router, sibs, inlineRunner{})

	if err := n.StartSink(context.Background(), testNodes(1), "rx:4201"); err != nil {
		t.Fatalf("StartSink returned %v with the node registered", err)
	}

	if router.sends != 1 {
		t.Fatalf("sent %d messages, want 1", router.sends)
	}
}

func TestStartSinkReportsASendFailure(t *testing.T) {
	sibs := &fakeSiblings{names: []protocols.SiblingName{"rpc.direct.node0"}}
	router := &fakeRouter{sendErr: errors.New("channel closed")}
	n := NewNodes(router, sibs, poolRunner{})

	err := n.StartSink(context.Background(), testNodes(1), "rx:4201")
	if err == nil {
		t.Fatal("StartSink returned nil after Send failed")
	}
}

// A node that is registered but gone never answers, so the failure surfaces in
// Recv -- inside the pool task. Only the reply channel carries it back to the
// caller; without it StartSink returns nil and rx reports a started publisher
// for a stream that is dead.
func TestStartSinkReportsARecvFailure(t *testing.T) {
	sibs := &fakeSiblings{names: []protocols.SiblingName{"rpc.direct.node0"}}
	router := &fakeRouter{recvErr: errors.New("connection closed")}
	n := NewNodes(router, sibs, poolRunner{})

	err := n.StartSink(context.Background(), testNodes(1), "rx:4201")
	if err == nil {
		t.Fatal("StartSink returned nil after Recv failed")
	}
}

// A non-zero reply code is the node refusing the command -- an unusable source
// address, a pipeline that would not start. It used to be logged and dropped,
// which made the refusal look exactly like a success.
func TestStartSinkReportsANodeThatRefusedTheCommand(t *testing.T) {
	sibs := &fakeSiblings{names: []protocols.SiblingName{"rpc.direct.node0"}}
	router := &fakeRouter{code: -1, message: "invalid source address"}
	n := NewNodes(router, sibs, poolRunner{})

	err := n.StartSink(context.Background(), testNodes(1), "")
	if err == nil {
		t.Fatal("StartSink returned nil for a node that replied with code -1")
	}

	if !strings.Contains(err.Error(), "invalid source address") {
		t.Fatalf("error does not carry the node's message: %v", err)
	}
}

// Recv must not wait for rx to shut down: it holds a goroutines pool worker for
// as long as it blocks, and one leaked worker per stale dispatch eventually
// starves the pool.
func TestARecvThatNeverAnswersIsBoundedByTheDeadline(t *testing.T) {
	restore := sinkCommandTimeout
	sinkCommandTimeout = 50 * time.Millisecond

	defer func() { sinkCommandTimeout = restore }()

	sibs := &fakeSiblings{names: []protocols.SiblingName{"rpc.direct.node0"}}
	router := &fakeRouter{block: true}
	n := NewNodes(router, sibs, poolRunner{})

	done := make(chan error, 1)

	go func() {
		done <- n.StartSink(context.Background(), testNodes(1), "rx:4201")
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("StartSink returned nil after Recv timed out")
		}
	case <-time.After(30 * time.Second):
		t.Fatal("StartSink did not return: Recv is not bounded by a deadline")
	}
}
