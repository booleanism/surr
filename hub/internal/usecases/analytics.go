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

// NodesGetAnalytics is the analytics adapter.
//
// fleet is the set of nodes this caller may read. It is passed down rather than
// looked up there because the ownership check and the authorization are the same
// query: the nodes whose ids were compared against the requested one are exactly
// the nodes the caller owns, and asking twice would be asking a second question
// whose answer could differ.
type NodesGetAnalytics interface {
	GetAnalytics(ctx context.Context, nodeID uuid.UUID, fleet []uuid.UUID, filter rabbitmq.AnalyticsFilters, buf *[]byte) (int, error)
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
			// The caller's whole node set travels with the request: a
			// fleet-scoped query unions across it, and the analytics service
			// resolves each id to the collection that node declared. Handing
			// over ids rather than collections is what keeps the service's rule
			// that a manifest never names a collection -- and keeps a node from
			// reading another user's documents.
			fleet := pools.UUIDPool.Get().(*[]uuid.UUID)
			defer pools.UUIDPoolResetAndPut(fleet)

			for _, member := range (*nodes)[:n] {
				(*fleet) = append((*fleet), member.ID)
			}

			return a.n.GetAnalytics(ctx, nd.ID, *fleet, filter, buf)
		}
	}

	return 0, fmt.Errorf("unrecognized node_id")
}
