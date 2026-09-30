package usecases

import (
	"context"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
)

type Subscription interface {
	DeleteSubscriptions(ctx context.Context, subs []models.Subscription) error
	InsertSubscriptions(ctx context.Context, subsNodesList []models.SubscribedNodes) error
	SelectAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error)
	SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
	SelectSubscriptionsByUser(ctx context.Context, userID uuid.UUID, subs *[]models.Subscription) (int, error)
	SelectSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error
	SelectNodeIDsByUser(ctx context.Context, user models.User, nodes *[]models.Node) (int, error)
}

type subscription struct {
	s Subscription
}

func NewSubscription(s Subscription) subscription {
	return subscription{
		s: s,
	}
}

func (s subscription) AddSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error {
	return s.s.InsertSubscriptions(ctx, subsNodes)
}

func (s subscription) RemoveSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error {
	subs := pools.SubscriptionsPool.Get().(*[]models.Subscription)
	defer pools.SubscriptionPoolResetAndPut(subs)

	for _, sub := range subsNodes {
		(*subs) = append((*subs), sub.Subscription)
	}

	if len(*subs) == 0 {
		return fmt.Errorf("expected > 0 subscribed nodes")
	}

	return s.s.DeleteSubscriptions(ctx, (*subs))
}

func (s subscription) GetSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error) {
	return s.s.SelectSubscribedNodes(ctx, sub, nodes)
}

func (s subscription) GetSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error {
	return s.s.SelectSubscriptionByNode(ctx, node, subs)
}

func (s subscription) GetAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error) {
	return s.s.SelectAllSubscribedNodes(ctx, nodes)
}

func (s subscription) GetNodeIDsByUser(ctx context.Context, user models.User, nodes *[]models.Node) (int, error) {
	return s.s.SelectNodeIDsByUser(ctx, user, nodes)
}
