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
	n.wrap(ctx, trackID, req, rep, "nodes.add", func(ns []models.Node) error {
		return n.nr.AddNodes(ctx, ns)
	})
}

func (n node) nodesRemove(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc) {
	n.wrap(ctx, trackID, req, rep, "nodes.remove", func(ns []models.Node) error {
		return n.nr.RemoveNodes(ctx, ns)
	})
}

func (n node) wrap(ctx context.Context, trackID uuid.UUID, req protocols.RouterRequest, rep protocols.ReplyFunc, command string, do func(ns []models.Node) error) {
	log := zerolog.Ctx(ctx).
		With().
		Str("command", command).
		Str("track_id", trackID.String()).
		Logger()

	ns := pools.NodePool.Get().(*[]models.Node)
	defer pools.NodePoolResetAndPut(ns)

	err := json.Unmarshal(req.Payload, ns)
	if err != nil {
		log.Error().Err(err).Int("code", protocols.CodeDecodePayload).Msg("handler failed")
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeDecodePayload, err))

		return
	}

	log.Debug().Int("count", len(*ns)).Msg("handler invoked")

	err = do(*ns)
	if err != nil {
		log.Error().Err(err).Int("code", protocols.CodeDomain).Msg("handler failed")
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeDomain, err))

		return
	}

	nodesByte, err := json.Marshal(ns)
	if err != nil {
		log.Error().Err(err).Int("code", protocols.CodeMarshalReply).Msg("handler failed")
		_ = rep(ctx, protocols.ErrorResponse(protocols.CodeMarshalReply, err))

		return
	}

	log.Info().Int("count", len(*ns)).Msg("nodes updated")

	_ = rep(ctx, protocols.ResultResponse(nodesByte))
}
