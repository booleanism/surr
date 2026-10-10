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

type NodesSink interface {
	StartSink(ctx context.Context, nodes []models.Node, sourceAddr string) error
	StopSink(ctx context.Context, nodes []models.Node) error
}

type SubscriptionSubscribedNodes interface {
	SelectSubscribedNodes(ctx context.Context, subID uuid.UUID, nodes *[]models.Node) (int, error)
}

type pendingPublish struct {
	nodes []models.Node
	subID uuid.UUID
}

type ingress struct {
	*srtgo.SrtSocket
	pending map[string]pendingPublish

	b *broadcaster
	s SubscriptionSubscribedNodes
	n NodesSink

	egressAddr string

	mu sync.Mutex
}

func NewIngress(sock *srtgo.SrtSocket, s SubscriptionSubscribedNodes, n NodesSink, b *broadcaster, egressAddr string) *ingress {
	return &ingress{
		SrtSocket:  sock,
		s:          s,
		n:          n,
		b:          b,
		pending:    make(map[string]pendingPublish),
		egressAddr: egressAddr,
	}
}

func (i *ingress) extractSubscriptionID(streamID string) (uuid.UUID, error) {
	sid := strings.Split(streamID, ":")
	if len(sid) < 2 {
		return uuid.Nil, fmt.Errorf("unexpected format")
	}

	if sid[0] != "publish" {
		return uuid.Nil, fmt.Errorf("unsupported action: %s", sid[0])
	}

	subID, err := uuid.Parse(sid[1])
	if err != nil {
		return uuid.Nil, fmt.Errorf("unable to parse uuid: %w", err)
	}

	return subID, nil
}

func (i *ingress) Listen(ctx context.Context) error {
	log := zerolog.Ctx(ctx)

	i.SetListenCallback(func(socket *srtgo.SrtSocket, version int, addr *net.UDPAddr, streamid string) bool {
		subID, err := i.extractSubscriptionID(streamid)
		if err != nil {
			log.Warn().Str("peer", addr.String()).Str("streamid", streamid).Err(err).Msg("ingress rejected")
			return false
		}

		nodesPtr := pools.NodePool.Get().(*[]models.Node)
		defer pools.NodePoolResetAndPut(nodesPtr)

		n, err := i.s.SelectSubscribedNodes(ctx, subID, nodesPtr)
		if err != nil {
			log.Error().Str("peer", addr.String()).Str("streamid", streamid).Str("sub_id", subID.String()).Err(err).Msg("ingress rejected: subscription lookup failed")
			return false
		}

		if len(*nodesPtr) < 1 {
			log.Warn().Str("peer", addr.String()).Str("streamid", streamid).Str("sub_id", subID.String()).Str("reason", "no subscribed nodes").Msg("ingress rejected")
			return false
		}

		nodes := make([]models.Node, n)
		copy(nodes, (*nodesPtr)[:n])

		i.mu.Lock()
		i.pending[addr.String()] = pendingPublish{subID: subID, nodes: nodes}
		i.mu.Unlock()
		return true
	})

	return listen(ctx, i.SrtSocket, int(i.b.max), func() error {
		return i.accept(ctx)
	})
}

func (i *ingress) accept(ctx context.Context) error {
	sock, addr, err := i.Accept()
	if err != nil {
		return err
	}

	i.mu.Lock()
	p, ok := i.pending[addr.String()]
	delete(i.pending, addr.String())
	i.mu.Unlock()

	if !ok {
		sock.Close()
		return nil
	}

	zerolog.Ctx(ctx).Info().
		Str("peer", addr.String()).
		Str("sub_id", p.subID.String()).
		Int("count", len(p.nodes)).
		Msg("ingress accept")

	go i.handlePublisher(ctx, sock, p.subID, p.nodes, addr.String())
	return nil
}

func (i *ingress) handlePublisher(ctx context.Context, sock *srtgo.SrtSocket, subID uuid.UUID, nodes []models.Node, peer string) {
	log := zerolog.Ctx(ctx)

	defer sock.Close()
	defer i.b.Close(subID)
	defer func() {
		log.Info().Str("sub_id", subID.String()).Str("peer", peer).Msg("publisher stopped")
	}()

	if err := i.n.StartSink(ctx, nodes, i.egressAddr); err != nil {
		log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(err).Msg("start nodes sink")
		return
	}

	log.Info().Str("sub_id", subID.String()).Str("peer", peer).Int("count", len(nodes)).Msg("publisher started")

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		buf := pools.BytesPool.Get().(*[]byte)
		*buf = (*buf)[:cap(*buf)]

		n, err := sock.Read(*buf)
		if err == nil {
			i.b.Publish(subID, buf, n)
			continue
		}

		log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(err).Msg("publisher read")
		err = i.n.StopSink(ctx, nodes)
		if err != nil {
			log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(err).Msg("stop nodes sink")
		}

		pools.BytesPoolResetAndPut(buf)
		return
	}
}
