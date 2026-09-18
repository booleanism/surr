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

type node struct {
	n Nodes
	s SubscribedNodes
}

func NewNode(n Nodes, s SubscribedNodes) node {
	return node{
		n: n,
		s: s,
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

	availableCount := 0
	for _, n := range *nodes {
		if _, exists := subscribed[n.ID]; !exists {
			(*nodes)[availableCount] = n
			availableCount++
		}
	}

	*nodes = (*nodes)[:availableCount]
	return availableCount, nil
}
