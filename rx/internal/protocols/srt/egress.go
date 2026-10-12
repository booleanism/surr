package srt

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/haivision/srtgo"
	"github.com/rs/zerolog"
)

// writeTimeout bounds how long a subscriber's write may park.
//
// srtgo's Write parks in its poll descriptor while the peer's receive window is
// full, so a subscriber that stops reading wedges this goroutine for as long as
// it stays connected: the pooled buffer it is writing stays checked out, its
// deferred Unsubscribe never runs, and the topic keeps a subscriber that
// nothing can reach. A deadline turns that into a dropped subscriber, which the
// node reconnects.
const writeTimeout = 10 * time.Second

type SubscriptionsByNode interface {
	SelectSubscriptionByNode(ctx context.Context, nodeID uuid.UUID, subs *[]models.Subscription) (int, error)
}

type egress struct {
	*srtgo.SrtSocket
	pending *pending[subReq]

	b *broadcaster
	n SubscriptionsByNode

	log zerolog.Logger
}

type subReq struct {
	subID  uuid.UUID
	nodeID uuid.UUID
}

func NewEgress(sock *srtgo.SrtSocket, n SubscriptionsByNode, b *broadcaster, log zerolog.Logger) *egress {
	return &egress{
		SrtSocket: sock,
		n:         n,
		b:         b,
		log:       log,
		pending:   newPending[subReq](),
	}
}

func (s *egress) extractNodeID(streamID string) (uuid.UUID, error) {
	sid := strings.Split(streamID, ":")
	if len(sid) < 2 {
		return uuid.Nil, fmt.Errorf("unexpected format")
	}

	if sid[0] != "subscribe" {
		return uuid.Nil, fmt.Errorf("unsupported action: %s", sid[0])
	}

	nodeID, err := uuid.Parse(sid[1])
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to parse uuid: %w", err)
	}

	return nodeID, nil
}

func (s *egress) listenCallback(ctx context.Context) func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
	log := zerolog.Ctx(ctx)

	return func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
		nodeID, err := s.extractNodeID(streamid)
		if err != nil {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "egress rejected",
				streamid: streamid,
				reason:   "bad request",
				code:     srtgo.RejectionReasonBadRequest,
				cause:    err,
			})
			return false
		}

		subsb := pools.SubscriptionsPool.Get().(*[]models.Subscription)
		defer pools.SubscriptionPoolResetAndPut(subsb)

		lookupCtx, cancel := context.WithTimeout(ctx, handshakeLookupTimeout)
		defer cancel()

		n, err := s.n.SelectSubscriptionByNode(lookupCtx, nodeID, subsb)
		if err != nil {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "egress rejected: subscription lookup failed",
				streamid: streamid,
				reason:   "forbidden",
				code:     srtgo.RejectionReasonForbidden,
				cause:    err,
				nodeID:   nodeID,
			})
			return false
		}

		if n != 1 {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "egress rejected",
				streamid: streamid,
				reason:   "no unique subscription",
				code:     srtgo.RejectionReasonUnacceptable,
				nodeID:   nodeID,
			})
			return false
		}

		if !s.pending.put(addr.String(), subReq{subID: (*subsb)[0].ID, nodeID: nodeID}) {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "egress rejected",
				streamid: streamid,
				reason:   "too many handshakes in flight",
				code:     srtgo.RejectionReasonOverload,
				nodeID:   nodeID,
			})
			return false
		}

		log.Info().
			Str("peer", addr.String()).
			Str("streamid", streamid).
			Str("sub_id", (*subsb)[0].ID.String()).
			Str("node_id", nodeID.String()).
			Msg("egress accept")

		return true
	}
}

func (s *egress) Listen(ctx context.Context) error {
	s.SetListenCallback(s.listenCallback(ctx))

	return listen(ctx, s.SrtSocket, 10, s.accept)
}

func (s *egress) accept() error {
	sock, addr, err := s.Accept()
	if err != nil {
		return err
	}

	req, ok := s.pending.take(addr.String())
	if !ok {
		sock.Close()
		return nil
	}

	go s.handleCaller(sock, req, addr.String())
	return nil
}

// refuseReason names why a subscriber that completed the handshake was not
// attached to a topic.
func refuseReason(err error) string {
	switch {
	case errors.Is(err, errNoPublisher):
		return "no publisher"
	case errors.Is(err, errMaxSubscribers):
		return "overload"
	default:
		return "unknown"
	}
}

func (s *egress) handleCaller(sock *srtgo.SrtSocket, req subReq, peer string) {
	log := s.log

	defer sock.Close()
	defer func() {
		log.Info().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Msg("subscriber stopped")
	}()

	sub, err := s.b.Subscribe(req.subID, req.nodeID)
	if err != nil {
		// The handshake already succeeded, so SetRejectReason has nothing left to
		// say -- the peer has its acceptance. The reason goes to the log and the
		// deferred Close is what the node actually sees.
		log.Warn().
			Str("sub_id", req.subID.String()).
			Str("node_id", req.nodeID.String()).
			Str("peer", peer).
			Str("reason", refuseReason(err)).
			Err(err).
			Msg("subscriber refused")
		return
	}

	defer s.b.Unsubscribe(sub)

	log.Info().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Str("peer", peer).Msg("subscriber started")

	for chunk := range sub.ch {
		sock.SetWriteDeadline(time.Now().Add(writeTimeout))

		_, err = sock.Write((*chunk.Buf)[:chunk.N])
		chunk.release()
		if err != nil {
			log.Warn().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Str("peer", peer).Err(err).Msg("subscriber write")
			return
		}
	}
}
