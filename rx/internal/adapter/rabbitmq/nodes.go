package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/rs/zerolog"
)

type SiblingGetName interface {
	GetName(c protocols.Command, sn *[]protocols.SiblingName) int
}

type TaskRunner interface {
	Run(ctx context.Context, t pools.TaskFunc) error
}

type nodes struct {
	r protocols.Router
	s SiblingGetName
	t TaskRunner
}

func NewNodes(r protocols.Router, s SiblingGetName, t TaskRunner) nodes {
	return nodes{
		r: r,
		s: s,
		t: t,
	}
}

type NodeSource struct {
	SourceAddr string `json:"source_address"`
}

// StartSink tells every node in the set to pull frames from sourceAddr. It
// tries all of them and returns the first failure, because the caller cannot
// recover from a sink that never started: rx has already accepted the
// publisher, and a node that was not told to start is never told again. The
// concrete case is a cold start where tx connects before the node has
// introduced itself -- the sibling lookup finds nothing, and swallowing that
// leaves every component reporting healthy while no frames move.
func (n nodes) StartSink(ctx context.Context, nodes []models.Node, sourceAddr string) error {
	log := zerolog.Ctx(ctx)

	payload, err := json.Marshal(&NodeSource{
		SourceAddr: sourceAddr,
	})
	if err != nil {
		return err
	}

	var firstErr error
	for _, node := range nodes {
		cmd := protocols.Command(fmt.Sprintf("%s.sink.start", node.ID.String()))
		log.Info().Str("command", string(cmd)).Str("node_id", node.ID.String()).Msg("sink start dispatched")
		err = n.do(ctx, cmd, payload)
		if err != nil {
			log.Error().Str("command", string(cmd)).Str("node_id", node.ID.String()).Err(err).Msg("nodes sink start broadcast")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// StopSink tells every node in the set to stop. It reports the first failure
// for the same reason StartSink does, though a caller there is already tearing
// the publisher down and only logs the result.
func (n nodes) StopSink(ctx context.Context, nodes []models.Node) error {
	log := zerolog.Ctx(ctx)

	payload, err := json.Marshal(&NodeSource{})
	if err != nil {
		return err
	}

	var firstErr error
	for _, node := range nodes {
		cmd := protocols.Command(fmt.Sprintf("%s.sink.stop", node.ID.String()))
		log.Info().Str("command", string(cmd)).Str("node_id", node.ID.String()).Msg("sink stop dispatched")
		err = n.do(ctx, cmd, payload)
		if err != nil {
			log.Error().Str("command", string(cmd)).Str("node_id", node.ID.String()).Err(err).Msg("nodes sink stop broadcast")
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

func (n nodes) do(ctx context.Context, c protocols.Command, payload []byte) error {
	sns := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(sns)

	nn := n.s.GetName(c, sns)
	if nn != 1 {
		return fmt.Errorf("expected one sibling with %s command", c)
	}

	sibling := (*sns)[0]
	err := n.t.Run(ctx, func() {
		log := zerolog.Ctx(ctx)

		trackID, err := n.r.Send(ctx, c, payload, sibling)
		if err != nil {
			log.Error().Str("command", string(c)).Err(err).Msg("send message")
			return
		}

		resp := protocols.RouterResponse{}
		err = n.r.Recv(ctx, trackID, &resp)
		if err != nil {
			log.Error().Str("command", string(c)).Str("track_id", trackID.String()).Err(err).Msg("recv message")
			return
		}

		log.Debug().Str("command", string(c)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")
		if resp.Code != 0 {
			log.Warn().Str("command", string(c)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("non-zero reply code")
		}
	})
	if err != nil {
		return err
	}

	return nil
}
