package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/rs/zerolog"
)

type RunTask interface {
	Run(ctx context.Context, t pools.TaskFunc) error
}

type Siblings interface {
	GetName(c protocols.Command, sn *[]protocols.SiblingName) int
}

type subscription struct {
	r protocols.Router
	t RunTask
	s Siblings
}

func NewSubscription(r protocols.Router, t RunTask, s Siblings) subscription {
	return subscription{
		r: r,
		t: t,
		s: s,
	}
}

func (s subscription) InsertSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error {
	return s.insertDeleteSubscriptionWrap(ctx, "subscriptions.add", subsNodes)
}

func (s subscription) DeleteSubscriptions(ctx context.Context, subsNodes []models.SubscribedNodes) error {
	return s.insertDeleteSubscriptionWrap(ctx, "subscriptions.remove", subsNodes)
}

func (s subscription) SelectNodeIDsByUser(ctx context.Context, user models.User, nodes *[]models.Node) (int, error) {
	payload, err := json.Marshal(&user) //nolint:gosec
	if err != nil {
		return 0, err
	}

	err = s.do(ctx, "subscriptions.nodes.by.user.get", payload, nodes)
	if err != nil {
		return 0, err
	}

	return len(*nodes), nil
}

func (s subscription) SelectSubscribedNodes(ctx context.Context, sub models.Subscription, nodes *[]models.Node) (int, error) {
	payload, err := json.Marshal(&sub)
	if err != nil {
		return 0, err
	}

	err = s.do(ctx, "subscriptions.nodes.get", payload, nodes)
	if err != nil {
		return 0, err
	}

	return len(*nodes), nil
}

func (s subscription) SelectAllSubscribedNodes(ctx context.Context, nodes *[]models.Node) (int, error) {
	err := s.do(ctx, "subscriptions.nodes.all.get", nil, nodes)
	if err != nil {
		return 0, err
	}

	return len(*nodes), nil
}

func (s subscription) insertDeleteSubscriptionWrap(ctx context.Context, command protocols.Command, subsNodes []models.SubscribedNodes) error {
	payload, err := json.Marshal(&subsNodes)
	if err != nil {
		return err
	}

	return s.do(ctx, command, payload, nil)
}

func (s subscription) do(ctx context.Context, command protocols.Command, payload []byte, v any) error {
	taskCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	siblingNames := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(siblingNames)

	nname := s.s.GetName(command, siblingNames)
	if nname == 0 {
		return fmt.Errorf("found zero siblings in %s command", command)
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
			log.Info().Int("code", resp.Code).Msg("non-zero reply code")

			// The subscriptions service reports a refused delete with a
			// dedicated code so the HTTP layer can answer 403 rather than 500.
			if resp.Code == protocols.CodeNotFound {
				errs = append(errs, fmt.Errorf("%s: %w: %s", sn, models.ErrSubscriptionNotFound, msg))

				continue
			}

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
func (s subscription) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName) (protocols.RouterResponse, error) {
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

		rpcErr := s.r.Recv(ctx, trackID, &resp)
		if rpcErr != nil {
			log.Error().Str("command", string(command)).Str("track_id", trackID.String()).Err(rpcErr).Msg("receiving response")
		} else {
			log.Debug().Str("command", string(command)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")
		}

		recvErr <- rpcErr
	})
	if err != nil {
		return resp, err
	}

	select {
	case rpcErr := <-recvErr:
		return resp, rpcErr
	case <-ctx.Done():
		return resp, ctx.Err()
	}
}
