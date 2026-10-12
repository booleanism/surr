package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/booleanism/surr/hub/internal/protocols/http/middlewares"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// fakeAuth lets the handlers be built without a real JWT middleware: these
// tests call the handlers directly and set the user id header themselves.
type fakeAuth struct{}

func (fakeAuth) RequireAuthHandlerFunc(h http.HandlerFunc) http.HandlerFunc {
	return h
}

// The field order is not free: govet's fieldalignment wants the pointer-bearing
// field (the error interface) packed first.
type fakeSubscriptions struct {
	err   error
	added []models.SubscribedNodes
	id    uuid.UUID
}

func (f *fakeSubscriptions) AddSubscription(_ context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error) {
	f.added = append(f.added, subsNodes)

	if f.err != nil {
		return uuid.Nil, f.err
	}

	return f.id, nil
}

func (f *fakeSubscriptions) RemoveSubscription(_ context.Context, subsNodes models.SubscribedNodes) (uuid.UUID, error) {
	return subsNodes.Subscription.ID, f.err
}

func postAdd(t *testing.T, fake *fakeSubscriptions, body string) *httptest.ResponseRecorder {
	t.Helper()

	h := NewSubscription(http.NewServeMux(), fake, fakeAuth{})

	req := httptest.NewRequest(http.MethodPost, "/add", strings.NewReader(body))
	req.Header.Set(middlewares.UserIDHeaderKey, uuid.New().String())
	rec := httptest.NewRecorder()

	h.add(rec, req)

	return rec
}

// A subscription is created with its nodes and nothing adds one afterwards, so
// an empty list would create a subscription no node can ever join -- one rx
// rejects every publish for.
func TestAddSubscriptionRejectsAnEmptyNodeList(t *testing.T) {
	fake := &fakeSubscriptions{id: uuid.New()}

	rec := postAdd(t, fake, `{"nodes":[]}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a subscription was created from an empty node list: %+v", fake.added)
	}
}

// A node with no id cannot match a real node, so accepting it stores a
// subscription that resolves to nothing.
func TestAddSubscriptionRejectsANodeWithoutAnID(t *testing.T) {
	fake := &fakeSubscriptions{id: uuid.New()}

	rec := postAdd(t, fake, `{"nodes":[{}]}`)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusBadRequest)
	}

	if len(fake.added) != 0 {
		t.Errorf("a subscription was created for a node with no id: %+v", fake.added)
	}
}

// subscribed_nodes.node_id is UNIQUE: a node already attached elsewhere is a
// conflict between the request and the stored state, not a server failure. It
// used to surface as a 500.
func TestAddSubscriptionReportsAnAlreadySubscribedNodeAsConflict(t *testing.T) {
	fake := &fakeSubscriptions{err: fmt.Errorf("%w: node insert", models.ErrNodeAlreadySubscribed)}

	rec := postAdd(t, fake, `{"nodes":[{"id":"`+uuid.New().String()+`"}]}`)

	if rec.Code != http.StatusConflict {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusConflict)
	}
}

func TestAddSubscriptionReportsAnUnexpectedFailureAsInternalError(t *testing.T) {
	fake := &fakeSubscriptions{err: fmt.Errorf("broker is gone")}

	rec := postAdd(t, fake, `{"nodes":[{"id":"`+uuid.New().String()+`"}]}`)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("status is %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestAddSubscriptionCreatesTheSubscription(t *testing.T) {
	id := uuid.New()
	nodeID := uuid.New()
	fake := &fakeSubscriptions{id: id}

	rec := postAdd(t, fake, `{"nodes":[{"id":"`+nodeID.String()+`"}]}`)

	if rec.Code != http.StatusOK {
		t.Fatalf("status is %d, want %d: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	if len(fake.added) != 1 {
		t.Fatalf("AddSubscription was called %d times, want 1", len(fake.added))
	}

	if len(fake.added[0].Nodes) != 1 || fake.added[0].Nodes[0].ID != nodeID {
		t.Errorf("AddSubscription received %+v", fake.added[0])
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if body["subscription_id"] != id.String() {
		t.Errorf("subscription_id is %q, want %q", body["subscription_id"], id.String())
	}
}
