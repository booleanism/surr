package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rabbitmq/amqp091-go"
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
	gh               GetHandler
	siblings         Siblings
	ch               *amqp091.Channel
	knownTrackID     map[uuid.UUID]chan protocols.RouterResponse
	exchName         string
	directRoutingKey string
	q                amqp091.Queue
	mu               sync.Mutex
}

func NewRouter(ch *amqp091.Channel, q QueueSetupReturnFunc, gh GetHandler, sibs Siblings) *router {
	que, exchName, name := q()
	return &router{
		ch:               ch,
		q:                que,
		directRoutingKey: name,
		exchName:         exchName,
		gh:               gh,
		siblings:         sibs,
		knownTrackID:     make(map[uuid.UUID]chan protocols.RouterResponse),
	}
}

func (r *router) Introduce(ctx context.Context) error {
	commands := make([]protocols.Command, 0, 64)
	_ = r.gh.AvailableCommand(&commands)

	siblingByte, err := json.Marshal(&protocols.Sibling{
		Commands: commands,
	})
	if err != nil {
		return err
	}

	return r.ch.PublishWithContext(ctx, r.exchName, BroadcastRoutingKey, false, false, amqp091.Publishing{
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
	if ok {
		r.mu.Lock()
		r.knownTrackID[trackID] = make(chan protocols.RouterResponse)
		r.mu.Unlock()
	}
	if !ok {
		return uuid.Nil, fmt.Errorf("no such sibling: %s", siblingName)
	}

	err = r.ch.PublishWithContext(ctx, r.exchName, string(siblingName), false, false, amqp091.Publishing{
		CorrelationId: trackID.String(),
		ContentType:   "application/json",
		ReplyTo:       r.directRoutingKey,
		Type:          string(command),
		Body:          payload,
	})
	if err != nil {
		r.mu.Lock()
		delete(r.knownTrackID, trackID)
		r.mu.Unlock()
		return uuid.Nil, err
	}

	return trackID, nil
}

func (r *router) Recv(ctx context.Context, trackID uuid.UUID, resp *protocols.RouterResponse) error {
	r.mu.Lock()
	c, ok := r.knownTrackID[trackID]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown trackID")
	}
	defer func() {
		r.mu.Lock()
		delete(r.knownTrackID, trackID)
		r.mu.Unlock()
	}()

	select {
	case res := <-c:
		*resp = res
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *router) Listen(ctx context.Context) error {
	msgs, err := r.ch.ConsumeWithContext(
		ctx,
		r.q.Name,
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

			go r.handleDelivery(ctx, msg)
		}
	}
}

func (r *router) handlerIntroCommand(repyTo string, body []byte) error {
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

	return nil
}

func (r *router) handleDelivery(ctx context.Context, msg amqp091.Delivery) {
	if msg.Type == IntroduceCommand {
		err := r.handlerIntroCommand(msg.ReplyTo, msg.Body)
		if err != nil {
			fmt.Printf("ERRO: intro handler: %s\n", err.Error())
			_ = msg.Nack(false, false)
			return
		}

		_ = msg.Ack(false)
		return
	}

	trackID, err := uuid.Parse(msg.CorrelationId)
	if err != nil {
		fmt.Printf("WARN: parsing trackID: %s\n", err.Error())
		_ = msg.Nack(false, false)
		return
	}

	r.mu.Lock()
	c, ok := r.knownTrackID[trackID]
	r.mu.Unlock()

	if !ok {
		err = r.handleInbound(ctx, protocols.Command(msg.Type), msg.ReplyTo, trackID, msg.Body)
		if err != nil {
			fmt.Printf("ERRO: handle inbound: %s\n", err.Error())
			_ = msg.Nack(false, false)
			return
		}

		_ = msg.Ack(false)
		return
	}

	if msg.ContentType != "application/json" {
		fmt.Printf("WARN: unexpected content type for reply: %s\n", msg.ContentType)
		_ = msg.Nack(false, false)
		return
	}

	resp := protocols.RouterResponse{}
	err = json.Unmarshal(msg.Body, &resp)
	if err != nil {
		fmt.Printf("WARN: unmarshal response: %s\n", err.Error())
		_ = msg.Nack(false, false)
		return
	}

	select {
	case c <- resp:
		_ = msg.Ack(false)
	case <-ctx.Done():
		_ = msg.Nack(false, true)
	}
}

func (r *router) handleInbound(ctx context.Context, command protocols.Command, replyTo string, trackID uuid.UUID, body []byte) error {
	h, err := r.gh.Get(command)
	if err != nil {
		fmt.Printf("ERRO: get handler: %s\n", command)
		return err
	}

	rep := func(ctx context.Context, resp protocols.RouterResponse) error {
		if replyTo == "" {
			return fmt.Errorf("no ReplyTo on original request, cannot reply")
		}

		body, err = json.Marshal(resp)
		if err != nil {
			return fmt.Errorf("marshal reply: %w", err)
		}

		return r.ch.PublishWithContext(ctx, r.exchName, replyTo, false, false, amqp091.Publishing{
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
