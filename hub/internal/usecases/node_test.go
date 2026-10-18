package usecases

import (
	"context"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type stubNodes struct {
	all []models.Node
}

func (s stubNodes) SelectAllNodes(_ context.Context, nodes *[]models.Node) (int, error) {
	*nodes = append((*nodes)[:0], s.all...)

	return len(s.all), nil
}

func (stubNodes) InsertNodes(_ context.Context, _ []models.Node) error { return nil }
func (stubNodes) DeleteNodes(_ context.Context, _ []models.Node) error { return nil }

type stubSubscribed struct {
	all []models.Node
}

func (s stubSubscribed) SelectAllSubscribedNodes(_ context.Context, nodes *[]models.Node) (int, error) {
	*nodes = append((*nodes)[:0], s.all...)

	return len(s.all), nil
}

type stubLive struct {
	ids []uuid.UUID
}

func (s stubLive) LiveNodeIDs(ids *[]uuid.UUID) int {
	*ids = append((*ids)[:0], s.ids...)

	return len(s.ids)
}

var (
	liveNode    = models.Node{ID: uuid.MustParse("11111111-1111-4111-8111-111111111111")}
	deadNode    = models.Node{ID: uuid.MustParse("22222222-2222-4222-8222-222222222222")}
	claimedNode = models.Node{ID: uuid.MustParse("33333333-3333-4333-8333-333333333333")}
)

// TestAvailableNodesListsOnlyLiveUnsubscribedNodes pins the three reasons a row
// is not offered. The live filter is the F6 fix: `nodes.add` is sent once per
// run and carries no timestamp, so a row left behind by a killed node is
// indistinguishable from a live one and would otherwise be offered forever.
func TestAvailableNodesListsOnlyLiveUnsubscribedNodes(t *testing.T) {
	n := NewNode(
		stubNodes{all: []models.Node{liveNode, deadNode, claimedNode}},
		stubSubscribed{all: []models.Node{claimedNode}},
		stubLive{ids: []uuid.UUID{liveNode.ID, claimedNode.ID}},
	)

	nodes := make([]models.Node, 0, 4)

	nb, err := n.AvailableNodes(context.Background(), &nodes)
	if err != nil {
		t.Fatalf("AvailableNodes: %v", err)
	}

	if nb != 1 {
		t.Fatalf("AvailableNodes reported %d nodes, want 1", nb)
	}

	if len(nodes) != 1 || nodes[0].ID != liveNode.ID {
		t.Fatalf("AvailableNodes listed %v, want just the live unsubscribed node", nodes)
	}
}

// TestAvailableNodesWithNothingAnnouncedIsEmpty pins the meaning of an empty
// registry: it is not "every node is dead", it is "nothing has introduced itself
// yet", and either way there is nothing to offer.
func TestAvailableNodesWithNothingAnnouncedIsEmpty(t *testing.T) {
	n := NewNode(stubNodes{all: []models.Node{liveNode}}, stubSubscribed{}, stubLive{})

	nodes := make([]models.Node, 0, 1)

	nb, err := n.AvailableNodes(context.Background(), &nodes)
	if err != nil {
		t.Fatalf("AvailableNodes: %v", err)
	}

	if nb != 0 || len(nodes) != 0 {
		t.Fatalf("AvailableNodes listed %v, want nothing", nodes)
	}
}
