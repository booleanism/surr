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
// published at all and the caller timed out.
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

	return decoded
}

type fakeSubscriptions struct {
	addErr    error
	removeErr error
	byNodeErr error
	nodesErr  error
	byUserErr error
	allErr    error
}

func (f fakeSubscriptions) AddSubscriptions(context.Context, []models.SubscribedNodes) error {
	return f.addErr
}

func (f fakeSubscriptions) RemoveSubscriptions(context.Context, []models.SubscribedNodes) error {
	return f.removeErr
}

func (f fakeSubscriptions) GetSubscribedNodes(context.Context, models.Subscription, *[]models.Node) (int, error) {
	return 0, f.nodesErr
}

func (f fakeSubscriptions) GetAllSubscribedNodes(context.Context, *[]models.Node) (int, error) {
	return 0, f.allErr
}

func (f fakeSubscriptions) GetSubscriptionByNode(context.Context, models.Node, *[]models.Subscription) error {
	return f.byNodeErr
}

func (f fakeSubscriptions) GetNodeIDsByUser(context.Context, models.User, *[]models.Node) (int, error) {
	return 0, f.byUserErr
}

func TestSubscriptionHandlers(t *testing.T) {
	const (
		nodeID = `{"id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"}`
		subID  = `{"id":"0d1f6a4c-2b3e-4c5d-8e9f-0a1b2c3d4e5f"}`
		subs   = `[{"id":"0d1f6a4c-2b3e-4c5d-8e9f-0a1b2c3d4e5f","nodes":[{"id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"}]}]`
		user   = `{"email":"user@example.com"}`
	)

	domainErr := errors.New("no such subscription")

	tests := []struct {
		subs     fakeSubscriptions
		name     string
		command  protocols.Command
		payload  string
		wantMsg  string
		wantCode int
	}{
		{name: "add success", command: "subscriptions.add", payload: subs, wantCode: protocols.CodeSuccess},
		{name: "remove success", command: "subscriptions.remove", payload: subs, wantCode: protocols.CodeSuccess},
		{name: "by nodeid success", command: "subscriptions.by.nodeid.get", payload: nodeID, wantCode: protocols.CodeSuccess},
		{name: "nodes success", command: "subscriptions.nodes.get", payload: subID, wantCode: protocols.CodeSuccess},
		{name: "nodes by user success", command: "subscriptions.nodes.by.user.get", payload: user, wantCode: protocols.CodeSuccess},
		{name: "nodes all success", command: "subscriptions.nodes.all.get", payload: ``, wantCode: protocols.CodeSuccess},

		{name: "add undecodable payload", command: "subscriptions.add", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},
		{name: "remove undecodable payload", command: "subscriptions.remove", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},
		{name: "by nodeid undecodable payload", command: "subscriptions.by.nodeid.get", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},
		{name: "nodes undecodable payload", command: "subscriptions.nodes.get", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},
		{name: "nodes by user undecodable payload", command: "subscriptions.nodes.by.user.get", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},

		{
			name: "add domain error", command: "subscriptions.add", payload: subs,
			subs: fakeSubscriptions{addErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
		{
			name: "remove domain error", command: "subscriptions.remove", payload: subs,
			subs: fakeSubscriptions{removeErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
		{
			name: "by nodeid domain error", command: "subscriptions.by.nodeid.get", payload: nodeID,
			subs: fakeSubscriptions{byNodeErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
		{
			name: "nodes domain error", command: "subscriptions.nodes.get", payload: subID,
			subs: fakeSubscriptions{nodesErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
		{
			name: "nodes by user domain error", command: "subscriptions.nodes.by.user.get", payload: user,
			subs: fakeSubscriptions{byUserErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
		{
			name: "nodes all domain error", command: "subscriptions.nodes.all.get", payload: ``,
			subs: fakeSubscriptions{allErr: domainErr}, wantCode: protocols.CodeDomain, wantMsg: domainErr.Error(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCaptureHandlers()
			if err := SetupSubscription(tt.subs, h); err != nil {
				t.Fatalf("setup subscription handlers: %v", err)
			}

			decoded := assertWire(t, h.invoke(t, tt.command, []byte(tt.payload)), tt.wantCode)

			if tt.wantMsg != "" && decoded.ErrorMessage() != tt.wantMsg {
				t.Errorf("message is %q, want %q", decoded.ErrorMessage(), tt.wantMsg)
			}
		})
	}
}
