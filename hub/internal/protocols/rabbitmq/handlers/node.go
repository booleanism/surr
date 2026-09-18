package handlers

import (
	"context"
	"encoding/json"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type Node interface {
	AddNodes(ctx context.Context, nodes []models.Node) error
	RemoveNodes(ctx context.Context, nodes []models.Node) error
}

type SetHandler interface {
	Set(c protocols.Command, h protocols.HandlerFunc) error
}

type node struct {
	h  SetHandler
	nr Node
}

func SetupNode(nr Node, h SetHandler) error {
	n := node{
		h:  h,
		nr: nr,
	}

	err := n.h.Set("nodes.add", n.nodesAdd)
	if err != nil {
		return err
	}

	err = n.h.Set("nodes.remove", n.nodesRemove)
	if err != nil {
		return err
	}

	return nil
}

func (n node) nodesAdd(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	n.wrap(ctx, req, rep, func(ns []models.Node) error {
		return n.nr.AddNodes(ctx, ns)
	})
}

func (n node) nodesRemove(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	n.wrap(ctx, req, rep, func(ns []models.Node) error {
		return n.nr.RemoveNodes(ctx, ns)
	})
}

func (n node) wrap(ctx context.Context, req protocols.RouterRequest, rep protocols.ReplyFunc, do func(ns []models.Node) error) {
	ns := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(ns)

	err := json.Unmarshal(req.Payload, ns)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -1, Result: []byte(err.Error())})
		return
	}

	err = do(*ns)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -2, Result: []byte(err.Error())})
		return
	}

	nodesByte, err := json.Marshal(ns)
	if err != nil {
		_ = rep(ctx, protocols.RouterResponse{Code: -3, Result: []byte(err.Error())})
	}

	_ = rep(ctx, protocols.RouterResponse{
		Code:   0,
		Result: nodesByte,
	})
}
