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

// readIdleTick bounds how long a publisher read parks before the loop looks at
// the context again.
//
// srtgo's Read parks in its poll descriptor until data arrives, a deadline
// expires or the socket breaks, so without a deadline a cancelled context is
// not observed at all: the goroutine stays in Read, its deferred Close(topic)
// and StopSink never run, and the nodes keep pulling from a receiver that is
// shutting down. The tick also gives a peer that vanished without closing (SRT
// is UDP, there is no FIN to wait for) a regular chance to surface as an error.
//
// A tick is not a frame timeout: a read that times out is retried, not treated
// as a dead publisher.
const readIdleTick = time.Second

// sinkUnavailableDelay is how long a publisher whose nodes could not be reached
// is held before its socket is closed.
//
// Closing the socket is what makes tx reconnect, and tx's srtsink does that
// immediately and without backoff: on a cold start where tx connects before the
// node has introduced itself, the two used to complete a full handshake, a
// broker lookup and a teardown roughly 34 times a second (1058 attempts in 31
// seconds, measured). rx cannot make tx back off, so it bounds the loop from
// this side by holding the publisher open for a moment first -- the retry rate
// becomes this delay rather than the handshake round trip.
const sinkUnavailableDelay = 2 * time.Second

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
	pending *pending[pendingPublish]

	b *broadcaster
	s SubscriptionSubscribedNodes
	n NodesSink

	egressAddr string
}

func NewIngress(sock *srtgo.SrtSocket, s SubscriptionSubscribedNodes, n NodesSink, b *broadcaster, egressAddr string) *ingress {
	return &ingress{
		SrtSocket:  sock,
		s:          s,
		n:          n,
		b:          b,
		pending:    newPending[pendingPublish](),
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
			rejectHandshake(log, socket, addr, rejection{
				msg:      "ingress rejected",
				streamid: streamid,
				reason:   "bad request",
				code:     srtgo.RejectionReasonBadRequest,
				cause:    err,
			})
			return false
		}

		nodesPtr := pools.NodePool.Get().(*[]models.Node)
		defer pools.NodePoolResetAndPut(nodesPtr)

		lookupCtx, cancel := context.WithTimeout(ctx, handshakeLookupTimeout)
		defer cancel()

		n, err := i.s.SelectSubscribedNodes(lookupCtx, subID, nodesPtr)
		if err != nil {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "ingress rejected: subscription lookup failed",
				streamid: streamid,
				reason:   "lookup failed",
				code:     srtgo.RejectionReasonForbidden,
				cause:    err,
			})
			return false
		}

		if len(*nodesPtr) < 1 {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "ingress rejected",
				streamid: streamid,
				reason:   "no subscribed nodes",
				code:     srtgo.RejectionReasonUnacceptable,
			})
			return false
		}

		nodes := make([]models.Node, n)
		copy(nodes, (*nodesPtr)[:n])

		if !i.pending.put(addr.String(), pendingPublish{subID: subID, nodes: nodes}) {
			rejectHandshake(log, socket, addr, rejection{
				msg:      "ingress rejected",
				streamid: streamid,
				reason:   "too many handshakes in flight",
				code:     srtgo.RejectionReasonOverload,
			})
			return false
		}

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

	p, ok := i.pending.take(addr.String())
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

	// The topic is opened before the nodes are told to start, not lazily on the
	// first frame: a node that reaches the egress first has to find a publisher
	// waiting for it, and Subscribe refuses to invent a topic.
	topic := i.b.Open(subID)

	defer sock.Close()
	defer i.b.Close(topic)
	defer func() {
		log.Info().Str("sub_id", subID.String()).Str("peer", peer).Msg("publisher stopped")
	}()

	if err := i.n.StartSink(ctx, nodes, i.egressAddr); err != nil {
		log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(err).Msg("start nodes sink")

		// Hold the socket before the deferred Close triggers a reconnect, so an
		// absent node is polled at a sane rate instead of as fast as tx can
		// retry. Cancellation cuts the wait short.
		select {
		case <-ctx.Done():
		case <-time.After(sinkUnavailableDelay):
		}

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

		sock.SetReadDeadline(time.Now().Add(readIdleTick))

		n, err := sock.Read(*buf)
		if err == nil {
			i.b.Publish(subID, buf, n)
			continue
		}

		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			// Nothing arrived within the tick. That is not a failure, so the
			// buffer goes back and the context gets looked at again.
			pools.BytesPoolResetAndPut(buf)
			continue
		}

		pools.BytesPoolResetAndPut(buf)
		i.stopSink(ctx, subID, nodes, peer, err)
		return
	}
}

// stopSink reports a publisher read failure and tears the nodes' sinks down.
func (i *ingress) stopSink(ctx context.Context, subID uuid.UUID, nodes []models.Node, peer string, cause error) {
	log := zerolog.Ctx(ctx)

	log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(cause).Msg("publisher read")

	if err := i.n.StopSink(ctx, nodes); err != nil {
		log.Error().Str("sub_id", subID.String()).Str("peer", peer).Err(err).Msg("stop nodes sink")
	}
}
