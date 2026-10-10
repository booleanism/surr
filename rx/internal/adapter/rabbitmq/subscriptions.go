package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type RunTask interface {
	Run(ctx context.Context, t pools.TaskFunc) error
}

type subscriptions struct {
	r protocols.Router
	s SiblingGetName
	t RunTask
}

func NewSubscriptions(r protocols.Router, s SiblingGetName, t RunTask) subscriptions {
	return subscriptions{
		r: r,
		s: s,
		t: t,
	}
}

func (s subscriptions) SelectSubscribedNodes(ctx context.Context, subID uuid.UUID, nodes *[]models.Node) (int, error) {
	sub := models.Subscription{
		ID: subID,
	}

	payload, err := json.Marshal(sub)
	if err != nil {
		return 0, err
	}

	err = s.do(ctx, "subscriptions.nodes.get", payload, nodes)
	if err != nil {
		return 0, err
	}

	return len(*nodes), nil
}

func (s subscriptions) do(ctx context.Context, command protocols.Command, payload []byte, v any) error {
	taskCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	siblingNames := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(siblingNames)

	n := s.s.GetName(protocols.Command(command), siblingNames)
	if n < 1 {
		return fmt.Errorf("no associated siblings with %s command", command)
	}

	var errs []error
	for _, sn := range *siblingNames {
		log := zerolog.Ctx(ctx).With().Str("sibling_name", string(sn)).Str("command", string(command)).Logger()

		resp, err := s.call(taskCtx, command, payload, sn)
		if err != nil {
			// A stale registry entry must not abandon the remaining
			// candidates: record the failure and try the next sibling.
			log.Warn().Err(err).Msg("rpc call")
			errs = append(errs, fmt.Errorf("%s: %w", sn, err))

			continue
		}

		if resp.Code != 0 {
			msg := resp.ErrorMessage()
			log.Warn().Int("code", resp.Code).Msg("non-zero reply code")
			errs = append(errs, fmt.Errorf("%s: unexpected response code %d: %s", sn, resp.Code, msg))

			continue
		}

		if v != nil {
			err = json.Unmarshal(resp.Result, v)
			if err != nil {
				log.Warn().Err(err).Msg("unmarshal reply")
				errs = append(errs, fmt.Errorf("%s: %w", sn, err))

				continue
			}
		}

		return nil
	}

	return errors.Join(errs...)
}

// call performs one RPC round trip against sn. The reply is received on the
// shared goroutines pool so the caller's goroutine stays free, and the result
// is delivered over a buffered channel so a Receive that finishes after the
// caller gave up can never block a pool worker.
func (s subscriptions) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}

	trackID, err := s.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	// Released on every path, including the timeout below, so an unanswered
	// request cannot leak a pending entry in the router's track map.
	defer s.r.Forget(trackID)

	recvErr := make(chan error, 1)

	err = s.t.Run(ctx, func() {
		log := zerolog.Ctx(ctx)

		err := s.r.Recv(ctx, trackID, &resp)
		if err != nil {
			log.Error().Str("command", string(command)).Str("track_id", trackID.String()).Err(err).Msg("receiving response")
		} else {
			log.Debug().Str("command", string(command)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")
		}

		recvErr <- err
	})
	if err != nil {
		return resp, err
	}

	select {
	case err := <-recvErr:
		return resp, err
	case <-ctx.Done():
		return resp, ctx.Err()
	}
}

func (s subscriptions) SelectSubscriptionByNode(ctx context.Context, nodeID uuid.UUID, subs *[]models.Subscription) (int, error) {
	payload, err := json.Marshal(&models.Node{
		ID: nodeID,
	})
	if err != nil {
		return 0, err
	}

	err = s.do(ctx, "subscriptions.by.nodeid.get", payload, subs)
	if err != nil {
		return 0, err
	}

	return len(*subs), nil
}
