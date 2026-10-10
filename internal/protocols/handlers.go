package protocols

import (
	"context"
	"encoding/base64"
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
	//   - Python base64-encodes bytes results through its json_serializer, so a
	//     Result that is a JSON string may be either a plain message or base64
	//     bytes. GetBytes decodes that case; Go itself never emits it, it sends
	//     marshalled JSON directly.
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

// GetBytes returns the reply payload as bytes.
//
// It exists for the Python side of the protocol, which base64-encodes bytes
// results through its json_serializer: a Result that is a JSON string is
// decoded as base64 when that succeeds and returned verbatim otherwise. The
// convention is ambiguous by construction — a short ASCII message such as
// "internal" is also valid base64 — so GetBytes is best-effort and failure
// messages must be read with ErrorMessage instead.
func (r RouterResponse) GetBytes() ([]byte, error) {
	if len(r.Result) == 0 || string(r.Result) == "null" {
		return nil, nil
	}

	if r.Result[0] == '"' {
		var str string
		if err := json.Unmarshal(r.Result, &str); err != nil {
			return nil, err
		}

		if decoded, err := base64.StdEncoding.DecodeString(str); err == nil {
			return decoded, nil
		}

		return []byte(str), nil
	}

	return r.Result, nil
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
