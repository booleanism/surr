package usecases

import (
	"context"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type Subscriptions interface {
	InsertSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error
	DeleteSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error
	SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
}

type NodesSelectByID interface {
	SelectAllNodesByID(ctx context.Context, nodes *[]models.Node) (int, error)
}

type GetUser interface {
	GetUserByID(ctx context.Context, user *models.User) error
}

type subscription struct {
	sa Subscriptions
	ur GetUser
	n  NodesSelectByID
}

func NewSubscription(s Subscriptions, ur GetUser, n NodesSelectByID) subscription {
	return subscription{
		sa: s,
		ur: ur,
		n:  n,
	}
}

func (s subscription) AddSubscription(ctx context.Context, subNodes models.SubscribedNodes) (uuid.UUID, error) {
	subsID, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, err
	}

	user := models.User{
		ID: subNodes.User.ID,
	}

	err = s.ur.GetUserByID(ctx, &user)
	if err != nil {
		return uuid.Nil, err
	}

	subNodes.Subscription.ID = subsID
	subNodes.User.ID = user.ID
	err = s.sa.InsertSubscriptions(ctx, []models.SubscribedNodes{subNodes})
	if err != nil {
		return uuid.Nil, err
	}

	return subsID, nil
}

func (s subscription) RemoveSubscription(ctx context.Context, subNodes models.SubscribedNodes) (uuid.UUID, error) {
	err := s.sa.DeleteSubscriptions(ctx, []models.SubscribedNodes{subNodes})
	if err != nil {
		return uuid.Nil, err
	}

	return subNodes.Subscription.ID, nil
}

func (s subscription) GetNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error) {
	n, err := s.sa.SelectSubscribedNodes(ctx, sub, nodes)
	if err != nil {
		return 0, err
	}

	if n < 1 {
		return 0, fmt.Errorf("expect one or more subscribed nodes")
	}

	return s.n.SelectAllNodesByID(ctx, nodes)
}
