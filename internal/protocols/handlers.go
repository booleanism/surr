package protocols

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type (
	SiblingName string
	Command     string

	ReplyFunc   func(ctx context.Context, resp RouterResponse) error
	HandlerFunc func(ctx context.Context, trackID uuid.UUID, req RouterRequest, rep ReplyFunc)

	RouterResponse struct {
		Result []byte `json:"result,omitempty,omitzero"`
		Code   int    `json:"code,omitempty"`
	}

	RouterRequest struct {
		Payload []byte `json:"data"`
	}
)

type Router interface {
	Send(ctx context.Context, command Command, payload []byte, siblingName SiblingName) (uuid.UUID, error)
	Recv(ctx context.Context, trackID uuid.UUID, resp *RouterResponse) error
}

type handlers struct {
	d map[Command]HandlerFunc

	mu sync.Mutex
}

func NewHandlers() *handlers {
	return &handlers{
		d: make(map[Command]HandlerFunc),
	}
}

func (l *handlers) Get(c Command) (HandlerFunc, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	h, ok := l.d[c]
	if !ok {
		return nil, fmt.Errorf("%s handler not found", c)
	}

	return h, nil
}

func (l *handlers) Set(c Command, h HandlerFunc) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.d[c] = h
	return nil
}

func (l *handlers) AvailableCommand(c *[]Command) int {
	(*c) = (*c)[:0]
	i := 0
	for k := range l.d {
		(*c) = append((*c), k)
		i++
	}

	return i
}
