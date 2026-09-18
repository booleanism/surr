package handlers

import (
	"context"
	"encoding/json"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type Subscriptions interface {
	AddSubscriptions(ctx context.Context, subsNodesList []models.SubscribedNodes) error
	RemoveSubscriptions(ctx context.Context, subsNodesList []models.Subscription) error
	GetSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
	GetAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error)
	GetSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error
}

type SetHandler interface {
	Set(c protocols.Command, h protocols.HandlerFunc) error
}

type subscriptions struct {
	h SetHandler
	s Subscriptions
}

func SetupSubscription(subs Subscriptions, h SetHandler) error {
	s := subscriptions{
		h: h,
		s: subs,
	}

	err := s.h.Set("subscriptions.add", s.addHandler)
	if err != nil {
		return err
	}

	err = s.h.Set("subscriptions.remove", s.removeHandler)
	if err != nil {
		return err
	}

	err = s.h.Set("subscriptions.by.nodeid.get", s.getByNodeID)
	if err != nil {
		return err
	}

	err = s.h.Set("subscriptions.nodes.get", s.getSubscribedNodesHandler)
	if err != nil {
		return err
	}

	return s.h.Set("subscriptions.nodes.all.get", s.getAllSubscribedNodesHandler)
}

func (s subscriptions) getByNodeID(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	subs := pools.SubscriptionsPool.Get().(*[]models.Subscription)
	defer pools.SubscriptionPoolResetAndPut(subs)

	node := models.Node{}
	err := json.Unmarshal(req.Payload, &node)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.GetSubscriptionByNode(ctx, node, subs)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	respByte, err := json.Marshal(subs)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: respByte})
}

func (s subscriptions) addHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	subed := pools.SubscribedNodesPool.Get().(*[]models.SubscribedNodes)
	defer pools.SubscribedNodesPoolResetAndPut(subed)

	err := json.Unmarshal(req.Payload, subed)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.AddSubscriptions(ctx, *subed)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res := []uuid.UUID{}
	for _, s := range *subed {
		res = append(res, s.Subscription.ID)
	}

	resByte, err := json.Marshal(&res)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: resByte})
}

func (s subscriptions) removeHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	subsb := pools.SubscriptionsPool.Get().(*[]models.Subscription)
	defer pools.SubscriptionPoolResetAndPut(subsb)

	err := json.Unmarshal(req.Payload, subsb)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.RemoveSubscriptions(ctx, *subsb)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res := []uuid.UUID{}
	for _, s := range *subsb {
		res = append(res, s.ID)
	}

	resByte, err := json.Marshal(&res)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: resByte})
}

func (s subscriptions) getAllSubscribedNodesHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	_, err := s.s.GetAllSubscribedNodes(ctx, nodes)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res, err := json.Marshal(nodes)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: res})
}

func (s subscriptions) getSubscribedNodesHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	subs := models.Subscription{}
	err := json.Unmarshal(req.Payload, &subs)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	_, err = s.s.GetSubscribedNodes(ctx, subs, nodes)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res, err := json.Marshal(nodes)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: res})
}
