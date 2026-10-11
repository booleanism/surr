package rabbitmq

import (
	"context"
	"strings"
	"testing"

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

type fakeRouter struct {
	sendErr error
	sends   int
}

func (r *fakeRouter) Send(_ context.Context, _ protocols.Command, _ []byte, _ protocols.SiblingName) (uuid.UUID, error) {
	r.sends++

	return uuid.New(), r.sendErr
}

func (r *fakeRouter) Recv(_ context.Context, _ uuid.UUID, _ *protocols.RouterResponse) error {
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
