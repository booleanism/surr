package srt

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/google/uuid"
	"github.com/haivision/srtgo"
	"github.com/rs/zerolog"
)

type SubscriptionsByNode interface {
	SelectSubscriptionByNode(ctx context.Context, nodeID uuid.UUID, subs *[]models.Subscription) (int, error)
}

type egress struct {
	*srtgo.SrtSocket
	pending map[string]subReq

	b *broadcaster
	n SubscriptionsByNode

	log zerolog.Logger

	mu sync.Mutex
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
		pending:   make(map[string]subReq),
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

func (s *egress) reject(log *zerolog.Logger, socket *srtgo.SrtSocket, addr *net.UDPAddr, streamid, reason string, code int) {
	err := socket.SetRejectReason(code)
	if err != nil {
		log.Error().Err(err).Str("reason", reason).Msg("set rejection reason")
	}

	log.Warn().Str("peer", addr.String()).Str("streamid", streamid).Str("reason", reason).Msg("egress rejected")
}

func (s *egress) listenCallback(ctx context.Context) func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
	log := zerolog.Ctx(ctx)

	return func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
		nodeID, err := s.extractNodeID(streamid)
		if err != nil {
			s.reject(log, socket, addr, streamid, "bad request", srtgo.RejectionReasonBadRequest)
			return false
		}

		subsb := pools.SubscriptionsPool.Get().(*[]models.Subscription)
		defer pools.SubscriptionPoolResetAndPut(subsb)

		n, err := s.n.SelectSubscriptionByNode(ctx, nodeID, subsb)
		if err != nil {
			log.Error().Str("peer", addr.String()).Str("streamid", streamid).Str("node_id", nodeID.String()).Err(err).Msg("egress rejected: subscription lookup failed")
			err = socket.SetRejectReason(srtgo.RejectionReasonForbidden)
			if err != nil {
				log.Error().Err(err).Str("reason", "forbidden").Msg("set rejection reason")
			}

			return false
		}

		if n != 1 {
			s.reject(log, socket, addr, streamid, "no unique subscription", srtgo.RejectionReasonUnacceptable)
			return false
		}

		s.mu.Lock()
		s.pending[addr.String()] = subReq{subID: (*subsb)[0].ID, nodeID: nodeID}
		s.mu.Unlock()

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

	err := s.SrtSocket.Listen(10)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			err = s.accept()
			if err != nil {
				return err
			}
		}
	}
}

func (s *egress) accept() error {
	sock, addr, err := s.Accept()
	if err != nil {
		return err
	}

	s.mu.Lock()
	req, ok := s.pending[addr.String()]
	delete(s.pending, addr.String())
	s.mu.Unlock()

	if !ok {
		sock.Close()
		return nil
	}

	go s.handleCaller(sock, req, addr.String())
	return nil
}

func (s *egress) handleCaller(sock *srtgo.SrtSocket, req subReq, peer string) {
	log := s.log

	defer sock.Close()
	defer s.b.Unsubscribe(req.subID, req.nodeID)
	defer func() {
		log.Info().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Msg("subscriber stopped")
	}()

	ch, err := s.b.Subscribe(req.subID, req.nodeID)
	if err != nil {
		subErr := err
		err = sock.SetRejectReason(srtgo.RejectionReasonOverload)
		if err != nil {
			log.Error().Err(err).Str("reason", "overload").Msg("set rejection reason")
		}

		log.Warn().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Str("peer", peer).Str("reason", "overload").Err(subErr).Msg("egress rejected")
		return
	}

	log.Info().Str("sub_id", req.subID.String()).Str("node_id", req.nodeID.String()).Str("peer", peer).Msg("subscriber started")

	for chunk := range ch {
		_, err = sock.Write((*chunk.Buf)[:chunk.N])
		chunk.release()
		if err != nil {
			return
		}
	}
}
