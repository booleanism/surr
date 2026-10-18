package usecases

import (
	"context"
	"testing"

	"github.com/booleanism/surr/hub/internal/adapter/rabbitmq"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type stubUserNodes struct {
	all []models.Node
}

func (s stubUserNodes) SelectNodeIDsByUser(_ context.Context, _ models.User, nodes *[]models.Node) (int, error) {
	*nodes = append((*nodes)[:0], s.all...)

	return len(s.all), nil
}

// stubAnalytics records the fleet it was handed. Field order is
// govet-fieldalignment's: the slice goes last.
type stubAnalytics struct {
	fleet []uuid.UUID
	node  uuid.UUID
}

func (s *stubAnalytics) GetAnalytics(_ context.Context, nodeID uuid.UUID, fleet []uuid.UUID, _ rabbitmq.AnalyticsFilters, buf *[]byte) (int, error) {
	s.node = nodeID
	s.fleet = append([]uuid.UUID(nil), fleet...)
	*buf = append((*buf)[:0], 'x')

	return 1, nil
}

// TestGetAnalyticsHandsTheCallersWholeFleetDown pins the authorization boundary:
// the set of nodes that proves ownership is the same set a fleet-scoped query is
// allowed to union, so it is computed once and travels with the request. A
// second lookup in the adapter could answer differently, and the difference would
// be a node reading another user's documents.
func TestGetAnalyticsHandsTheCallersWholeFleetDown(t *testing.T) {
	first := models.Node{ID: uuid.MustParse("11111111-1111-4111-8111-111111111111")}
	second := models.Node{ID: uuid.MustParse("22222222-2222-4222-8222-222222222222")}

	adapter := &stubAnalytics{}

	buf := make([]byte, 0, 8)

	if _, err := NewAnalytics(stubUserNodes{all: []models.Node{first, second}}, adapter).
		GetAnalytics(context.Background(), models.User{}, second, rabbitmq.AnalyticsFilters{}, &buf); err != nil {
		t.Fatalf("get analytics: %v", err)
	}

	if adapter.node != second.ID {
		t.Fatalf("reported on %s, want %s", adapter.node, second.ID)
	}

	if len(adapter.fleet) != 2 || adapter.fleet[0] != first.ID || adapter.fleet[1] != second.ID {
		t.Fatalf("fleet is %v, want both of the caller's nodes", adapter.fleet)
	}
}

// A node the caller does not own is refused before anything is asked of the
// analytics service, so a fleet can never contain a node the request was not
// allowed to name.
func TestGetAnalyticsRefusesANodeTheCallerDoesNotOwn(t *testing.T) {
	mine := models.Node{ID: uuid.MustParse("11111111-1111-4111-8111-111111111111")}
	theirs := models.Node{ID: uuid.MustParse("33333333-3333-4333-8333-333333333333")}

	adapter := &stubAnalytics{}

	buf := make([]byte, 0, 8)

	if _, err := NewAnalytics(stubUserNodes{all: []models.Node{mine}}, adapter).
		GetAnalytics(context.Background(), models.User{}, theirs, rabbitmq.AnalyticsFilters{}, &buf); err == nil {
		t.Fatal("a node the caller does not own was reported on")
	}

	if adapter.fleet != nil {
		t.Fatalf("the analytics service was asked anyway, with fleet %v", adapter.fleet)
	}
}
