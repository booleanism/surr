package usecases

import (
	"context"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
)

type Nodes interface {
	SelectAllNodes(ctx context.Context, nodes *[]models.Node) (int, error)
	InsertNodes(ctx context.Context, nodes []models.Node) error
	DeleteNodes(ctx context.Context, nodes []models.Node) error
}

type SubscribedNodes interface {
	SelectAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error)
}

// LiveNodes reports which nodes are advertising themselves right now. It is
// the sibling registry, not the database, because the `nodes` table cannot
// answer the question: `nodes.add` is sent once per run and carries no
// timestamp, so a row left behind by a node that was killed without a graceful
// `nodes.remove` looks exactly like a live one (TODO.md F6).
//
// An empty result is a legitimate answer -- nothing has introduced itself yet --
// and it means the availability list is empty, not that every node is dead.
type LiveNodes interface {
	LiveNodeIDs(ids *[]uuid.UUID) int
}

type node struct {
	n Nodes
	s SubscribedNodes
	l LiveNodes
}

func NewNode(n Nodes, s SubscribedNodes, l LiveNodes) node {
	return node{
		n: n,
		s: s,
		l: l,
	}
}

func (n node) AddNodes(ctx context.Context, nodes []models.Node) error {
	return n.n.InsertNodes(ctx, nodes)
}

func (n node) RemoveNodes(ctx context.Context, nodes []models.Node) error {
	return n.n.DeleteNodes(ctx, nodes)
}

func (n node) AvailableNodes(ctx context.Context, nodes *[]models.Node) (int, error) {
	_, err := n.n.SelectAllNodes(ctx, nodes)
	if err != nil {
		return 0, err
	}

	nb := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nb)

	_, err = n.s.SelectAllSubscribedNodes(ctx, nb)
	if err != nil {
		return 0, err
	}

	subscribed := make(map[uuid.UUID]struct{}, len(*nb))
	for _, subNode := range *nb {
		subscribed[subNode.ID] = struct{}{}
	}

	// A row for a node that is not advertising itself is a row left behind by a
	// node that never came back: offering it lets a client subscribe to a node
	// that will never answer, which surfaces later as a sink that refuses to
	// start for no visible reason.
	liveIDs := pools.UUIDPool.Get().(*[]uuid.UUID)
	defer pools.UUIDPoolResetAndPut(liveIDs)

	n.l.LiveNodeIDs(liveIDs)

	live := make(map[uuid.UUID]struct{}, len(*liveIDs))
	for _, id := range *liveIDs {
		live[id] = struct{}{}
	}

	availableCount := 0
	for _, n := range *nodes {
		if _, exists := subscribed[n.ID]; exists {
			continue
		}

		if _, up := live[n.ID]; !up {
			continue
		}

		(*nodes)[availableCount] = n
		availableCount++
	}

	*nodes = (*nodes)[:availableCount]
	return availableCount, nil
}
