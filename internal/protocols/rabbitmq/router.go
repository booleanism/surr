package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

const (
	IntroduceCommand    = "introduce"
	BroadcastRoutingKey = "rpc.broadcast"
)

type QueueSetupReturnFunc func() (amqp091.Queue, string, string)

func directRoutingKey(consumerName string) string {
	return fmt.Sprintf("rpc.direct.%s", consumerName)
}

type GetHandler interface {
	Get(command protocols.Command) (protocols.HandlerFunc, error)
	AvailableCommand(c *[]protocols.Command) int
}

type Siblings interface {
	Get(sn protocols.SiblingName, ss *protocols.Sibling) (ok bool)
	Set(sn protocols.SiblingName, k protocols.Sibling) error
}

type RunTask interface {
	Run(ctx context.Context, t pools.TaskFunc) error
}

func SetupRouter(ch *amqp091.Channel, name, exchangeName string) (QueueSetupReturnFunc, error) {
	if err := ch.ExchangeDeclare(exchangeName, "topic", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	q, err := ch.QueueDeclare("", false, true, true, false, nil)
	if err != nil {
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	directRk := directRoutingKey(name)
	err = ch.QueueBind(q.Name, directRk, exchangeName, false, nil)
	if err != nil {
		return nil, fmt.Errorf("bind direct routing key: %w", err)
	}

	err = ch.QueueBind(q.Name, BroadcastRoutingKey, exchangeName, false, nil)
	if err != nil {
		return nil, fmt.Errorf("bind broadcast routing key: %w", err)
	}

	return func() (amqp091.Queue, string, string) {
		return q, exchangeName, directRk
	}, nil
}

type router struct {
	gh           GetHandler
	siblings     Siblings
	run          RunTask
	knownTrackID map[uuid.UUID]chan protocols.RouterResponse
	// ch and q belong to the current broker connection. The reconnect
	// supervisor swaps both after the broker restarts, so every publish and
	// every Listen call reads them through channel()/queue() instead of
	// caching a copy; chMu guards the swap.
	ch               *amqp091.Channel
	exchName         string
	directRoutingKey string
	q                amqp091.Queue
	chMu             sync.RWMutex
	mu               sync.Mutex
}

func NewRouter(ch *amqp091.Channel, q QueueSetupReturnFunc, gh GetHandler, sibs Siblings, run RunTask) *router {
	que, exchName, name := q()
	return &router{
		ch:               ch,
		q:                que,
		run:              run,
		directRoutingKey: name,
		exchName:         exchName,
		gh:               gh,
		siblings:         sibs,
		knownTrackID:     make(map[uuid.UUID]chan protocols.RouterResponse),
	}
}

// channel returns the current broker channel.
func (r *router) channel() *amqp091.Channel {
	r.chMu.RLock()
	defer r.chMu.RUnlock()

	return r.ch
}

// queue returns the queue declared on the current broker connection.
func (r *router) queue() amqp091.Queue {
	r.chMu.RLock()
	defer r.chMu.RUnlock()

	return r.q
}

// ReplaceChannel installs the channel and queue of a freshly established broker
// connection after the previous one was closed. A Listen already running on the
// dead channel returns, and the restart loop re-enters it against the new one,
// so a broker restart recovers instead of leaving the service subscribed to
// nothing.
func (r *router) ReplaceChannel(ch *amqp091.Channel, q amqp091.Queue) {
	r.chMu.Lock()
	r.ch, r.q = ch, q
	r.chMu.Unlock()
}

// Introduce broadcasts this service's command set, wire revision and build id
// on the broadcast routing key. version is the build id injected by the
// Makefile; it is advertised for diagnosis only.
func (r *router) Introduce(ctx context.Context, version string) error {
	commands := make([]protocols.Command, 0, 64)
	_ = r.gh.AvailableCommand(&commands)

	siblingByte, err := json.Marshal(&protocols.Sibling{
		Commands:         commands,
		ProtocolRevision: protocols.ProtocolRevision,
		Version:          version,
	})
	if err != nil {
		return err
	}

	zerolog.Ctx(ctx).Debug().
		Str("sibling", r.directRoutingKey).
		Int("count", len(commands)).
		Int("protocol_revision", protocols.ProtocolRevision).
		Msg("introduce broadcast")

	return r.channel().PublishWithContext(ctx, r.exchName, BroadcastRoutingKey, false, false, amqp091.Publishing{
		Type:    IntroduceCommand,
		ReplyTo: r.directRoutingKey,
		Body:    siblingByte,
	})
}

func (r *router) Send(ctx context.Context, command protocols.Command, payload []byte, siblingName protocols.SiblingName) (uuid.UUID, error) {
	trackID, err := uuid.NewRandom()
	if err != nil {
		return uuid.Nil, err
	}

	sibling := protocols.Sibling{}
	ok := r.siblings.Get(siblingName, &sibling)
	if !ok {
		return uuid.Nil, fmt.Errorf("no such sibling: %s", siblingName)
	}

	r.mu.Lock()
	// Buffered, so a reply is handed over without a receiver: the caller may
	// already have given up, and handleDelivery must never block on it.
	r.knownTrackID[trackID] = make(chan protocols.RouterResponse, 1)
	r.mu.Unlock()

	err = r.channel().PublishWithContext(ctx, r.exchName, string(siblingName), false, false, amqp091.Publishing{
		CorrelationId: trackID.String(),
		ContentType:   "application/json",
		ReplyTo:       r.directRoutingKey,
		Type:          string(command),
		Body:          payload,
	})
	if err != nil {
		r.Forget(trackID)
		return uuid.Nil, err
	}

	return trackID, nil
}

// Forget releases a pending track. Callers that abandon a reply (timeout,
// cancellation, an exhausted sibling list) must call it, otherwise the map
// grows one entry per RPC for the lifetime of the process.
func (r *router) Forget(trackID uuid.UUID) {
	r.mu.Lock()
	delete(r.knownTrackID, trackID)
	r.mu.Unlock()
}

func (r *router) Recv(ctx context.Context, trackID uuid.UUID, resp *protocols.RouterResponse) error {
	r.mu.Lock()
	c, ok := r.knownTrackID[trackID]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown trackID")
	}

	// Release the entry on every exit path, the way nodes/core/router.py pops
	// it in its finally block. A reply that never arrives, or arrives after the
	// caller stopped waiting, must not leave a pending track behind.
	defer r.Forget(trackID)

	select {
	case res := <-c:
		*resp = res
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *router) Listen(ctx context.Context) error {
	msgs, err := r.channel().ConsumeWithContext(
		ctx,
		r.queue().Name,
		r.directRoutingKey,
		false,
		true,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("delivery channel closed")
			}

			err = r.run.Run(ctx, func() {
				r.handleDelivery(ctx, msg)
			})
			if err != nil {
				zerolog.Ctx(ctx).Warn().Err(err).Msg("handle delivery on goroutines pool")
			}
		}
	}
}

func (r *router) handlerIntroCommand(ctx context.Context, repyTo string, body []byte) error {
	if repyTo == "" {
		return fmt.Errorf("empty reply to")
	}

	sibling := protocols.Sibling{}
	err := json.Unmarshal(body, &sibling)
	if err != nil {
		return err
	}

	err = r.siblings.Set(protocols.SiblingName(repyTo), sibling)
	if err != nil {
		return err
	}

	log := zerolog.Ctx(ctx)

	// Register the peer either way. The command sets usually still line up, and
	// refusing a mismatched peer would turn a warning into an outage; the point
	// is that the disagreement is on the record before it surfaces as an
	// unexplained failure on some later command.
	if sibling.ProtocolRevision != protocols.ProtocolRevision {
		log.Warn().
			Str("sibling", repyTo).
			Int("peer_protocol_revision", sibling.ProtocolRevision).
			Int("protocol_revision", protocols.ProtocolRevision).
			Str("peer_version", sibling.Version).
			Msg("sibling protocol revision mismatch")
	}

	log.Debug().
		Str("sibling", repyTo).
		Int("count", len(sibling.Commands)).
		Int("peer_protocol_revision", sibling.ProtocolRevision).
		Str("peer_version", sibling.Version).
		Msg("sibling registry updated")

	return nil
}

func (r *router) handleDelivery(ctx context.Context, msg amqp091.Delivery) {
	log := zerolog.Ctx(ctx)

	if msg.Type == IntroduceCommand {
		err := r.handlerIntroCommand(ctx, msg.ReplyTo, msg.Body)
		if err != nil {
			log.Error().Str("sibling", msg.ReplyTo).Err(err).Msg("intro handler")
			_ = msg.Nack(false, false)
			return
		}

		_ = msg.Ack(false)
		return
	}

	trackID, err := uuid.Parse(msg.CorrelationId)
	if err != nil {
		log.Warn().Str("reason", "malformed correlation id").Err(err).Msg("nack malformed message")
		_ = msg.Nack(false, false)
		return
	}

	r.mu.Lock()
	c := r.knownTrackID[trackID]
	r.mu.Unlock()

	if c == nil {
		log.Debug().
			Str("command", msg.Type).
			Str("track_id", msg.CorrelationId).
			Str("reply_to", msg.ReplyTo).
			Msg("inbound request")

		err = r.handleInbound(ctx, protocols.Command(msg.Type), msg.ReplyTo, trackID, msg.Body)
		if err != nil {
			log.Error().Str("command", msg.Type).Err(err).Msg("handle inbound")
			_ = msg.Nack(false, false)
			return
		}

		_ = msg.Ack(false)
		return
	}

	if msg.ContentType != "application/json" {
		log.Warn().Str("reason", "unexpected content type").Str("content_type", msg.ContentType).Str("track_id", msg.CorrelationId).Msg("nack reply")
		_ = msg.Nack(false, false)
		return
	}

	resp := protocols.RouterResponse{}
	err = json.Unmarshal(msg.Body, &resp)
	if err != nil {
		log.Warn().Str("reason", "unmarshal response").Str("track_id", msg.CorrelationId).Err(err).Msg("nack reply")
		_ = msg.Nack(false, false)
		return
	}

	log.Debug().Str("track_id", msg.CorrelationId).Int("code", resp.Code).Msg("reply matched")

	// The reply channel is buffered, so the hand-off never blocks: a caller
	// that already gave up (or a duplicate reply for the same track) is
	// nacked without stalling the delivery goroutine.
	select {
	case c <- resp:
		_ = msg.Ack(false)
	default:
		log.Warn().
			Str("reason", "reply arrived after the caller gave up").
			Str("track_id", msg.CorrelationId).
			Msg("nack stale reply")
		_ = msg.Nack(false, false)
	}
}

func (r *router) handleInbound(ctx context.Context, command protocols.Command, replyTo string, trackID uuid.UUID, body []byte) error {
	h, err := r.gh.Get(command)
	if err != nil {
		zerolog.Ctx(ctx).Error().Str("command", string(command)).Err(err).Msg("get handler")
		return err
	}

	rep := func(ctx context.Context, resp protocols.RouterResponse) error {
		if replyTo == "" {
			// Nothing can be sent back — there is no address — so this warning
			// is the only trace the failure leaves. Without it the requester
			// just times out, and the log of the side that could have answered
			// says nothing about why it did not.
			zerolog.Ctx(ctx).Warn().
				Str("command", string(command)).
				Str("track_id", trackID.String()).
				Msg("dropping reply: the request carried no ReplyTo")

			return fmt.Errorf("no ReplyTo on original request, cannot reply")
		}

		body, err = json.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal reply: %w", err)
		}

		return r.channel().PublishWithContext(ctx, r.exchName, replyTo, false, false, amqp091.Publishing{
			CorrelationId: trackID.String(),
			ContentType:   "application/json",
			Body:          body,
		})
	}

	h(ctx, trackID, protocols.RouterRequest{
		Payload: body,
	}, rep)

	return nil
}
