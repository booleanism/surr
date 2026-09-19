package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type AnalyticsFilters struct {
	TimeCluster string `json:"time_cluster"`
}

type nodes struct {
	r protocols.Router
	s Siblings
	t RunTask
}

func NewNodes(r protocols.Router, s Siblings, t RunTask) nodes {
	return nodes{
		r: r,
		s: s,
		t: t,
	}
}

func (n nodes) GetAnalytics(ctx context.Context, nodeID uuid.UUID, f AnalyticsFilters, buf *[]byte) (int, error) {
	cmd := fmt.Sprintf("%s.sink.analytics", nodeID.String())
	payload, err := json.Marshal(&f)
	if err != nil {
		return 0, err
	}

	err = n.do(ctx, protocols.Command(cmd), payload, buf)
	if err != nil {
		return 0, err
	}

	return len(*buf), nil
}

func (n nodes) do(ctx context.Context, c protocols.Command, payload []byte, v any) error {
	sns := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(sns)

	nn := n.s.GetName(c, sns)
	if nn != 1 {
		return fmt.Errorf("expected one sibling with %s command", c)
	}

	sibling := (*sns)[0]
	res := make(chan error, 1)
	resp, err := n.call(ctx, c, payload, sibling, res)
	if err != nil {
		return err
	}

	select {
	case err = <-res:
		if err != nil {
			return err
		}

		if resp.Code != 0 {
			return fmt.Errorf("unexpected response code: %d", resp.Code)
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

func (n nodes) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName, res chan error) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}
	trackID, err := n.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	done := make(chan struct{})
	err = n.t.Run(ctx, func() {
		err = n.r.Recv(ctx, trackID, &resp)
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
