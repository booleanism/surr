package usecases

import (
	"context"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type Subscription interface {
	DeleteSubscriptions(ctx context.Context, subsNodeList []models.Subscription) error
	InsertSubscriptions(ctx context.Context, subsNodesList []models.SubscribedNodes) error
	SelectAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error)
	SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
	SelectSubscriptionsByUser(ctx context.Context, userID uuid.UUID, subs *[]models.Subscription) (int, error)
	SelectSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error
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

func (s subscription) RemoveSubscriptions(ctx context.Context, subsNodes []models.Subscription) error {
	return s.s.DeleteSubscriptions(ctx, subsNodes)
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
