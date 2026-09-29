package handlers

import (
	"context"
	"encoding/json"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Subscriptions interface {
	AddSubscriptions(ctx context.Context, subsNodesList []models.SubscribedNodes) error
	RemoveSubscriptions(ctx context.Context, subsNodesList []models.Subscription) error
	GetSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error)
	GetAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error)
	GetSubscriptionByNode(ctx context.Context, node models.Node, subs *[]models.Subscription) error
	GetNodeIDsByUser(ctx context.Context, user models.User, nodes *[]models.Node) (int, error)
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

	err = s.h.Set("subscriptions.nodes.by.user.get", s.getNodeIDsByUserHandler)
	if err != nil {
		return err
	}

	return s.h.Set("subscriptions.nodes.all.get", s.getAllSubscribedNodesHandler)
}

func (s subscriptions) logFail(ctx context.Context, command string, code int, err error) {
	zerolog.Ctx(ctx).Error().Str("command", command).Int("code", code).Err(err).Msg("handler failed")
}

func (s subscriptions) getByNodeID(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.by.nodeid.get"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	subs := pools.SubscriptionsPool.Get().(*[]models.Subscription)
	defer pools.SubscriptionPoolResetAndPut(subs)

	node := models.Node{}
	err := json.Unmarshal(req.Payload, &node)
	if err != nil {
		s.logFail(ctx, command, -1, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.GetSubscriptionByNode(ctx, node, subs)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	respByte, err := json.Marshal(subs)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: respByte})
}

func (s subscriptions) addHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.add"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	subed := pools.SubscribedNodesPool.Get().(*[]models.SubscribedNodes)
	defer pools.SubscribedNodesPoolResetAndPut(subed)

	err := json.Unmarshal(req.Payload, subed)
	if err != nil {
		s.logFail(ctx, command, -1, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.AddSubscriptions(ctx, *subed)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res := []uuid.UUID{}
	for _, s := range *subed {
		res = append(res, s.Subscription.ID)
	}

	resByte, err := json.Marshal(&res)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: resByte})
}

func (s subscriptions) removeHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.remove"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	subsb := pools.SubscriptionsPool.Get().(*[]models.Subscription)
	defer pools.SubscriptionPoolResetAndPut(subsb)

	err := json.Unmarshal(req.Payload, subsb)
	if err != nil {
		s.logFail(ctx, command, -1, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = s.s.RemoveSubscriptions(ctx, *subsb)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res := []uuid.UUID{}
	for _, s := range *subsb {
		res = append(res, s.ID)
	}

	resByte, err := json.Marshal(&res)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: resByte})
}

func (s subscriptions) getAllSubscribedNodesHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.nodes.all.get"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	_, err := s.s.GetAllSubscribedNodes(ctx, nodes)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res, err := json.Marshal(nodes)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: res})
}

func (s subscriptions) getSubscribedNodesHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.nodes.get"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	subs := models.Subscription{}
	err := json.Unmarshal(req.Payload, &subs)
	if err != nil {
		s.logFail(ctx, command, -1, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	_, err = s.s.GetSubscribedNodes(ctx, subs, nodes)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	res, err := json.Marshal(nodes)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: res})
}

func (s subscriptions) getNodeIDsByUserHandler(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	const command = "subscriptions.nodes.by.user.get"

	zerolog.Ctx(ctx).Debug().Str("command", command).Str("track_id", trackID.String()).Msg("handler invoked")

	nodes := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(nodes)

	user := models.User{}
	err := json.Unmarshal(req.Payload, &user)
	if err != nil {
		s.logFail(ctx, command, -1, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	n, err := s.s.GetNodeIDsByUser(ctx, user, nodes)
	if err != nil {
		s.logFail(ctx, command, -2, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	v := (*nodes)[:n]
	res, err := json.Marshal(&v)
	if err != nil {
		s.logFail(ctx, command, -3, err)
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
		return
	}

	_ = rep(ctx, protocols.RouterResponse{Code: 0, Result: res})
}
