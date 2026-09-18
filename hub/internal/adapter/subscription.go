package adapter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
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
	err := s.do(ctx, "subscriptions.nodes.get.all", nil, nodes)
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

	res := make(chan error, 1)
	siblingNames := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(siblingNames)

	nname := s.s.GetName(command, siblingNames)
	if nname == 0 {
		return fmt.Errorf("found zero siblings in %s command", command)
	}

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

func (s subscription) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName, res chan error) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}
	trackID, err := s.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	done := make(chan struct{})
	err = s.t.Run(ctx, func() {
		err = s.r.Recv(ctx, trackID, &resp)
		if err != nil {
			fmt.Printf("ERRO: receiving response: %s\n", err.Error())
			res <- err
		}

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
