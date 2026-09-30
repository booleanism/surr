package usecases

import (
	"context"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type Subscriptions interface {
	InsertSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error
	DeleteSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error
	SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
}

type GetUser interface {
	GetUserByID(ctx context.Context, user *models.User) error
}

type subscription struct {
	sa Subscriptions
	ur GetUser
}

func NewSubscription(s Subscriptions, ur GetUser) subscription {
	return subscription{
		sa: s,
		ur: ur,
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
