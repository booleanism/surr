package usecases

import (
	"context"
	"fmt"

	"github.com/booleanism/surr/hub/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
)

type Subscription interface {
	SelectNodeIDsByUser(ctx context.Context, user models.User, nodes *[]models.Node) (int, error)
}

type NodesGetAnalytics interface {
	GetAnalytics(ctx context.Context, nodeID uuid.UUID, filter rabbitmq.AnalyticsFilters, buf *[]byte) (int, error)
}

type analytics struct {
	s Subscription
	n NodesGetAnalytics
}

func NewAnalytics(s Subscription, n NodesGetAnalytics) analytics {
	return analytics{
		s: s,
		n: n,
	}
}

func (a analytics) GetAnalytics(ctx context.Context, user models.User, node models.Node, filter rabbitmq.AnalyticsFilters, buf *[]byte) (int, error) {
	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	n, err := a.s.SelectNodeIDsByUser(ctx, user, nodes)
	if err != nil {
		return 0, err
	}

	for _, nd := range (*nodes)[:n] {
		if nd.ID == node.ID {
			n, err = a.n.GetAnalytics(ctx, nd.ID, filter, buf)
			if err != nil {
				return n, err
			}

			return n, nil
		}
	}

	return 0, fmt.Errorf("unrecognized node_id")
}
