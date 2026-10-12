package protocols

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/google/uuid"
)

type (
	SiblingName string
	Command     string

	ReplyFunc   func(ctx context.Context, resp RouterResponse) error
	HandlerFunc func(ctx context.Context, trackID uuid.UUID, req RouterRequest, rep ReplyFunc)

	// RouterResponse is the payload every RPC reply carries.
	//
	// Wire contract, mirrored by nodes/core/handlers.py:
	//   - Code is CodeSuccess on success and a negative Code* value on
	//     failure. Any non-zero code is an error; callers must not read Result
	//     as a domain value in that case.
	//   - Result is raw JSON. On success it is the marshalled domain value
	//     (object or array); on failure it is a JSON *string* holding the
	//     message, which ErrorResponse builds.
	//   - A bytes result (the Python nodes base64-encode bytes through their
	//     json_serializer) is decoded by unmarshalling Result into a []byte
	//     field, which is what encoding/json does with a base64 JSON string.
	//     There is deliberately no helper that guesses whether a string is a
	//     message or base64 bytes: the caller knows the shape it asked for,
	//     and a guess cannot be better than the type it decodes into.
	RouterResponse struct {
		Result json.RawMessage `json:"result,omitempty,omitzero"`
		Code   int             `json:"code,omitempty"`
	}

	RouterRequest struct {
		Payload []byte `json:"data"`
	}
)

// Reply codes. They are part of the wire protocol: nodes/core/handlers.py and
// every Go caller branch on "is Code zero" only, but the values are kept
// distinct so a log line says which stage failed.
const (
	CodeSuccess       = 0
	CodeDecodePayload = -1
	CodeDomain        = -2
	CodeMarshalReply  = -3
	// CodeNotFound reports that the addressed object does not exist, or does
	// not belong to the caller. It is distinct from CodeDomain so the hub can
	// answer 403/404 instead of 500 without string-matching the message.
	CodeNotFound = -4
)

// ResultResponse builds a success reply from an already-marshalled payload.
func ResultResponse(payload []byte) RouterResponse {
	return RouterResponse{Code: CodeSuccess, Result: payload}
}

// ErrorResponse builds a failure reply whose Result is a JSON string.
//
// Handlers used to assign []byte(err.Error()) to the json.RawMessage field,
// which made json.Marshal fail with "invalid character 's' looking for
// beginning of value". The reply was then dropped and the caller timed out
// instead of receiving the error.
func ErrorResponse(code int, err error) RouterResponse {
	msg := "unknown error"
	if err != nil {
		msg = err.Error()
	}

	// json.Marshal of a string only fails on invalid UTF-8, which it repairs;
	// keep the fallback so the reply is always valid JSON.
	quoted, marshalErr := json.Marshal(msg)
	if marshalErr != nil {
		quoted = []byte(`"error message not encodable"`)
	}

	return RouterResponse{Code: code, Result: quoted}
}

// ErrorMessage returns the message carried by a non-zero reply.
//
// ErrorResponse stores it as a JSON string, so it is decoded as one; a Result
// that is not a JSON string (an older or hand-built reply) is returned as-is.
func (r RouterResponse) ErrorMessage() string {
	if len(r.Result) == 0 || string(r.Result) == "null" {
		return ""
	}

	var msg string
	if err := json.Unmarshal(r.Result, &msg); err != nil {
		return string(r.Result)
	}

	return msg
}

type Router interface {
	Send(ctx context.Context, command Command, payload []byte, siblingName SiblingName) (uuid.UUID, error)
	Recv(ctx context.Context, trackID uuid.UUID, resp *RouterResponse) error
	// Forget releases a pending track that will never be received, so an
	// abandoned Send cannot leak an entry in the router's pending map.
	Forget(trackID uuid.UUID)
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
	l.mu.Lock()
	defer l.mu.Unlock()

	(*c) = (*c)[:0]
	i := 0
	for k := range l.d {
		(*c) = append((*c), k)
		i++
	}

	return i
}
