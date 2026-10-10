package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type captureHandlers struct {
	handlers map[protocols.Command]protocols.HandlerFunc
}

func newCaptureHandlers() *captureHandlers {
	return &captureHandlers{handlers: make(map[protocols.Command]protocols.HandlerFunc)}
}

func (c *captureHandlers) Set(cmd protocols.Command, h protocols.HandlerFunc) error {
	c.handlers[cmd] = h
	return nil
}

// AvailableCommand mirrors protocols.handlers so the test can assert that every
// command a service advertises is actually exercised below. Without it a newly
// registered command would silently ship untested — which is exactly how the
// unmarshalable-reply bug survived.
func (c *captureHandlers) AvailableCommand(cmds *[]protocols.Command) int {
	(*cmds) = (*cmds)[:0]

	for k := range c.handlers {
		(*cmds) = append((*cmds), k)
	}

	return len(c.handlers)
}

// invoke runs one registered handler and returns the reply it published.
func (c *captureHandlers) invoke(t *testing.T, cmd protocols.Command, payload []byte) protocols.RouterResponse {
	t.Helper()

	h, ok := c.handlers[cmd]
	if !ok {
		t.Fatalf("handler %s is not registered", cmd)
	}

	var got protocols.RouterResponse

	rep := func(_ context.Context, resp protocols.RouterResponse) error {
		got = resp
		return nil
	}

	h(context.Background(), uuid.New(), protocols.RouterRequest{Payload: payload}, rep)

	return got
}

// assertWire checks that a reply survives the trip the router puts it through:
// the handler builds it, the router marshals it, the caller unmarshals it. The
// original bug was a reply that failed at the marshal step, so it was never
// published at all.
func assertWire(t *testing.T, resp protocols.RouterResponse, wantCode int) protocols.RouterResponse {
	t.Helper()

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}

	var decoded protocols.RouterResponse
	err = json.Unmarshal(body, &decoded)
	if err != nil {
		t.Fatalf("unmarshal reply %s: %v", body, err)
	}

	if decoded.Code != wantCode {
		t.Fatalf("code is %d, want %d (body %s)", decoded.Code, wantCode, body)
	}

	// The E1 invariant: every failure reply carries its message as a JSON
	// string, so a caller can always read it with ErrorMessage().
	if wantCode != protocols.CodeSuccess && !json.Valid(decoded.Result) {
		t.Fatalf("code %d: result %s is not valid JSON", wantCode, decoded.Result)
	}

	return decoded
}

type fakeNode struct {
	err error
}

func (f fakeNode) AddNodes(context.Context, []models.Node) error    { return f.err }
func (f fakeNode) RemoveNodes(context.Context, []models.Node) error { return f.err }

type nodeCase struct {
	node     fakeNode
	name     string
	command  protocols.Command
	payload  string
	wantMsg  string
	wantCode int
}

// assertEveryCommandCovered fails when a command the service advertises has no
// case in the table. The reply-marshalling bug survived a review pass because
// nothing tied the handler registry to the tests, so a command could ship
// completely untested.
func assertEveryCommandCovered(t *testing.T, tests []nodeCase) {
	t.Helper()

	h := newCaptureHandlers()
	if err := SetupNode(fakeNode{}, h); err != nil {
		t.Fatalf("setup node handlers: %v", err)
	}

	var advertised []protocols.Command
	if n := h.AvailableCommand(&advertised); n != len(advertised) {
		t.Fatalf("AvailableCommand reported %d commands but filled %d", n, len(advertised))
	}

	if len(advertised) == 0 {
		t.Fatal("no command is registered")
	}

	covered := make(map[protocols.Command]bool, len(tests))
	for _, tt := range tests {
		covered[tt.command] = true
	}

	for _, cmd := range advertised {
		if !covered[cmd] {
			t.Errorf("command %s is registered but no case covers it", cmd)
		}
	}
}

func TestNodeHandlers(t *testing.T) {
	const payload = `[{"id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f","bitrate":4000,"width":1280,"height":720}]`

	tests := []nodeCase{
		{name: "add success", command: "nodes.add", payload: payload, wantCode: protocols.CodeSuccess},
		{name: "remove success", command: "nodes.remove", payload: payload, wantCode: protocols.CodeSuccess},
		{name: "add undecodable payload", command: "nodes.add", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},
		{
			name:     "add domain error",
			command:  "nodes.add",
			payload:  payload,
			node:     fakeNode{err: errors.New("no such node")},
			wantCode: protocols.CodeDomain,
			wantMsg:  "no such node",
		},
		{
			name:     "remove domain error",
			command:  "nodes.remove",
			payload:  payload,
			node:     fakeNode{err: errors.New("database is locked")},
			wantCode: protocols.CodeDomain,
			wantMsg:  "database is locked",
		},
		{
			// A message with quotes and a newline has to survive the JSON-string
			// encoding, otherwise the caller reads a truncated error.
			name:     "remove error with json metacharacters",
			command:  "nodes.remove",
			payload:  payload,
			node:     fakeNode{err: errors.New("insert \"nodes\":\nconstraint failed")},
			wantCode: protocols.CodeDomain,
			wantMsg:  "insert \"nodes\":\nconstraint failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCaptureHandlers()
			if err := SetupNode(tt.node, h); err != nil {
				t.Fatalf("setup node handlers: %v", err)
			}

			decoded := assertWire(t, h.invoke(t, tt.command, []byte(tt.payload)), tt.wantCode)

			if tt.wantMsg != "" && decoded.ErrorMessage() != tt.wantMsg {
				t.Errorf("message is %q, want %q", decoded.ErrorMessage(), tt.wantMsg)
			}
		})
	}

	assertEveryCommandCovered(t, tests)
}
