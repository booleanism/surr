package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type fakeSiblings struct {
	names []protocols.SiblingName
}

func (f fakeSiblings) GetName(_ protocols.Command, sn *[]protocols.SiblingName) int {
	*sn = append((*sn)[:0], f.names...)

	return len(f.names)
}

// inlineRunner runs the task on the calling goroutine, which is what the real
// pool does and what makes the round trip observable without a broker.
type inlineRunner struct{}

func (inlineRunner) Run(_ context.Context, t pools.TaskFunc) error {
	t()

	return nil
}

// fakeRouter answers every Recv with the same canned reply, so a test can drive
// the reply-code translation without a broker.
type fakeRouter struct {
	reply protocols.RouterResponse
}

func (r *fakeRouter) Send(_ context.Context, _ protocols.Command, _ []byte, _ protocols.SiblingName) (uuid.UUID, error) {
	return uuid.New(), nil
}

func (r *fakeRouter) Recv(_ context.Context, _ uuid.UUID, resp *protocols.RouterResponse) error {
	*resp = r.reply

	return nil
}

func (r *fakeRouter) Forget(_ uuid.UUID) {}

// codeReply builds the reply a service sends for err: the message travels as a
// JSON string, the way protocols.ErrorResponse writes it.
func codeReply(t *testing.T, code int, err error) protocols.RouterResponse {
	t.Helper()

	quoted, marshalErr := json.Marshal(err.Error())
	if marshalErr != nil {
		t.Fatalf("marshal message: %v", marshalErr)
	}

	return protocols.RouterResponse{Code: code, Result: quoted}
}

func testSubscription(reply protocols.RouterResponse) subscription {
	return NewSubscription(
		&fakeRouter{reply: reply},
		inlineRunner{},
		fakeSiblings{names: []protocols.SiblingName{"rpc.direct.subs0"}},
	)
}

func TestInsertSubscriptionsSucceeds(t *testing.T) {
	s := testSubscription(protocols.ResultResponse([]byte(`[]`)))

	err := s.InsertSubscriptions(context.Background(), []models.SubscribedNodes{{}})
	if err != nil {
		t.Fatalf("InsertSubscriptions returned %v for a successful reply", err)
	}
}

// The subscriptions service reports an already-subscribed node with its own
// reply code. Translating that back into the sentinel is what lets the HTTP
// layer answer 409: a bare code means nothing to a handler that only sees an
// error.
func TestInsertSubscriptionsReportsAConflict(t *testing.T) {
	conflict := errors.New("node insert: node is already subscribed")
	s := testSubscription(codeReply(t, protocols.CodeConflict, conflict))

	err := s.InsertSubscriptions(context.Background(), []models.SubscribedNodes{{}})
	if !errors.Is(err, models.ErrNodeAlreadySubscribed) {
		t.Fatalf("InsertSubscriptions returned %v, want ErrNodeAlreadySubscribed", err)
	}
}

// The same translation for a refused delete, which the HTTP layer answers 403
// for.
func TestDeleteSubscriptionsReportsANotFound(t *testing.T) {
	notFound := errors.New("gone: subscription not found")
	s := testSubscription(codeReply(t, protocols.CodeNotFound, notFound))

	err := s.DeleteSubscriptions(context.Background(), []models.SubscribedNodes{{}})
	if !errors.Is(err, models.ErrSubscriptionNotFound) {
		t.Fatalf("DeleteSubscriptions returned %v, want ErrSubscriptionNotFound", err)
	}
}

// A plain domain failure keeps its message and picks up neither sentinel, so an
// internal error stays a 500 rather than being reported as a client mistake.
func TestInsertSubscriptionsKeepsADomainFailureGeneric(t *testing.T) {
	s := testSubscription(codeReply(t, protocols.CodeDomain, errors.New("database is locked")))

	err := s.InsertSubscriptions(context.Background(), []models.SubscribedNodes{{}})
	if err == nil {
		t.Fatal("InsertSubscriptions returned nil for a domain failure")
	}

	if errors.Is(err, models.ErrNodeAlreadySubscribed) || errors.Is(err, models.ErrSubscriptionNotFound) {
		t.Fatalf("a domain failure was translated into a client error: %v", err)
	}

	if !strings.Contains(err.Error(), "database is locked") {
		t.Fatalf("error does not carry the service message: %v", err)
	}
}
