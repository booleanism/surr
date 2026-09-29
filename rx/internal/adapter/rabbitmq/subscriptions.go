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

	log := zerolog.Ctx(ctx)

	res := make(chan error, 1)
	var errs []error
	for _, sn := range *siblingNames {
		resp, err := s.call(taskCtx, command, payload, sn, res)
		if err != nil {
			return err
		}

		select {
		case err = <-res:
			if err != nil {
				return err
			}

			if resp.Code != 0 {
				log.Warn().Str("command", string(command)).Int("code", resp.Code).Msg("non-zero reply code")
				errs = append(errs, fmt.Errorf("unexpeced response code: %d, %s", resp.Code, resp.Result))
				continue
			}

			if v != nil {
				err = json.Unmarshal(resp.Result, v)
				if err != nil {
					return err
				}
			}

			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return errors.Join(errs...)
}

func (s subscriptions) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName, res chan error) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}
	trackID, err := s.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	done := make(chan struct{})
	err = s.t.Run(ctx, func() {
		log := zerolog.Ctx(ctx)

		err = s.r.Recv(ctx, trackID, &resp)
		if err != nil {
			log.Error().Str("command", string(command)).Str("track_id", trackID.String()).Err(err).Msg("receiving response")
			res <- err
		}

		log.Debug().Str("command", string(command)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")
		res <- nil
		close(done)
	})
	if err != nil {
		return resp, err
	}

	select {
	case <-ctx.Done():
		return resp, ctx.Err()
	case <-done:
		return resp, nil
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
