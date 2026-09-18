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
)

type SubscriptionsByNode interface {
	SelectSubscriptionByNode(ctx context.Context, nodeID uuid.UUID, subs *[]models.Subscription) (int, error)
}

type egress struct {
	*srtgo.SrtSocket
	pending map[string]subReq

	b *broadcaster
	n SubscriptionsByNode

	mu sync.Mutex
}

type subReq struct {
	subID  uuid.UUID
	nodeID uuid.UUID
}

func NewEgress(sock *srtgo.SrtSocket, n SubscriptionsByNode, b *broadcaster) *egress {
	return &egress{
		SrtSocket: sock,
		n:         n,
		b:         b,
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

func (s *egress) reject(socket *srtgo.SrtSocket, addr *net.UDPAddr, reason int) {
	err := socket.SetRejectReason(reason)
	if err != nil {
		fmt.Printf("ERRO: set rejection reason: %s\n", err.Error())
	}

	fmt.Printf("INFO: rejected: %s: bad request", addr.String())
}

func (s *egress) listenCallback(ctx context.Context) func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
	return func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
		nodeID, err := s.extractNodeID(streamid)
		if err != nil {
			s.reject(socket, addr, srtgo.RejectionReasonBadRequest)
			return false
		}

		subsb := pools.SubscriptionsPool.Get().(*[]models.Subscription)
		defer pools.SubscriptionPoolResetAndPut(subsb)

		n, err := s.n.SelectSubscriptionByNode(ctx, nodeID, subsb)
		if err != nil {
			s.reject(socket, addr, srtgo.RejectionReasonForbidden)
			return false
		}

		if n != 1 {
			s.reject(socket, addr, srtgo.RejectionReasonUnacceptable)
			return false
		}

		s.mu.Lock()
		s.pending[addr.String()] = subReq{subID: (*subsb)[0].ID, nodeID: nodeID}
		s.mu.Unlock()

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

	go s.handleCaller(sock, req)
	return nil
}

func (s *egress) handleCaller(sock *srtgo.SrtSocket, req subReq) {
	defer sock.Close()
	defer s.b.Unsubscribe(req.subID, req.nodeID)

	ch, err := s.b.Subscribe(req.subID, req.nodeID)
	if err != nil {
		err = sock.SetRejectReason(srtgo.RejectionReasonOverload)
		if err != nil {
			fmt.Printf("ERRO: set rejection: %s\n", err.Error())
		}

		fmt.Printf("INFO: rejected: overload")
		return
	}

	for chunk := range ch {
		_, err = sock.Write((*chunk.Buf)[:chunk.N])
		chunk.release()
		if err != nil {
			return
		}
	}
}
