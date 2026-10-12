package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/rs/zerolog"
)

// sinkCommandTimeout bounds one <node-id>.sink.* round trip.
//
// It is a var rather than a const so a test can shorten it; nothing in
// production writes to it. The value matches handshakeLookupTimeout in
// rx/internal/protocols/srt -- both bound a broker round trip that an SRT
// handshake is waiting on.
var sinkCommandTimeout = 5 * time.Second

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

// do performs one sink RPC against the single sibling that serves c.
//
// The round trip runs on the shared goroutines pool, and its outcome is handed
// back over a buffered channel. Both halves are load-bearing: without the
// channel the error would die inside the closure and this function would return
// whatever Run returned -- nil -- so StartSink would report success for a node
// that never answered; without the timeout Recv waits for ctx to be cancelled
// and holds a pool worker for the life of the process.
func (n nodes) do(ctx context.Context, c protocols.Command, payload []byte) error {
	sns := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(sns)

	nn := n.s.GetName(c, sns)
	if nn != 1 {
		return fmt.Errorf("expected one sibling with %s command", c)
	}

	sibling := (*sns)[0]

	rpcCtx, cancel := context.WithTimeout(ctx, sinkCommandTimeout)
	defer cancel()

	// Buffered, so a round trip that finishes after the caller gave up can
	// never block a pool worker.
	result := make(chan error, 1)

	err := n.t.Run(ctx, func() {
		result <- n.roundTrip(rpcCtx, c, payload, sibling)
	})
	if err != nil {
		return err
	}

	return <-result
}

// roundTrip sends one command and waits for its reply. It is the body of the
// pool task do schedules, kept separate so the error it returns can travel back
// over the channel instead of dying inside the closure.
func (n nodes) roundTrip(ctx context.Context, c protocols.Command, payload []byte, sibling protocols.SiblingName) error {
	log := zerolog.Ctx(ctx)

	trackID, err := n.r.Send(ctx, c, payload, sibling)
	if err != nil {
		log.Error().Str("command", string(c)).Err(err).Msg("send message")

		return err
	}

	resp := protocols.RouterResponse{}

	err = n.r.Recv(ctx, trackID, &resp)
	if err != nil {
		log.Error().Str("command", string(c)).Str("track_id", trackID.String()).Err(err).Msg("recv message")

		return err
	}

	log.Debug().Str("command", string(c)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")

	// A non-zero code is the node refusing the command -- an unusable source
	// address, a pipeline that would not start. It used to be logged and
	// dropped, which made the refusal indistinguishable from a success.
	if resp.Code != protocols.CodeSuccess {
		msg := resp.ErrorMessage()
		log.Warn().Str("command", string(c)).Str("track_id", trackID.String()).Int("code", resp.Code).Str("reply", msg).Msg("non-zero reply code")

		return fmt.Errorf("%s: reply code %d: %s", c, resp.Code, msg)
	}

	return nil
}
