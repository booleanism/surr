package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
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

	resp, err := n.call(ctx, c, payload, sibling)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		msg := resp.ErrorMessage()
		zerolog.Ctx(ctx).Warn().Str("command", string(c)).Int("code", resp.Code).Msg("non-zero reply code")

		return fmt.Errorf("unexpected response code %d: %s", resp.Code, msg)
	}

	if v != nil {
		err = json.Unmarshal(resp.Result, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// call performs one RPC round trip against sn. The reply is received on the
// shared goroutines pool so the caller's goroutine stays free, and the result
// is delivered over a buffered channel so a Receive that finishes after the
// caller gave up can never block a pool worker.
func (n nodes) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}

	trackID, err := n.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	// Released on every path, including the timeout below, so an unanswered
	// request cannot leak a pending entry in the router's track map.
	defer n.r.Forget(trackID)

	recvErr := make(chan error, 1)

	err = n.t.Run(ctx, func() {
		log := zerolog.Ctx(ctx)

		rpcErr := n.r.Recv(ctx, trackID, &resp)
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
