package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// newTestSubscriptions returns a subscriptions store over an in-memory database
// carrying both tables exactly as the migration creates them, so the UNIQUE
// constraints the translation depends on are the real ones rather than a
// stand-in.
func newTestSubscriptions(t *testing.T) subscription {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	_, err = sqlDB.Exec(`
		CREATE TABLE subscriptions (
			id BLOB PRIMARY KEY DEFAULT (randomblob(16)),
			user_id BLOB NOT NULL
		);
		CREATE TABLE subscribed_nodes (
			id BLOB PRIMARY KEY DEFAULT (randomblob(16)),
			subscription_id BLOB NOT NULL,
			node_id BLOB UNIQUE NOT NULL,
			CONSTRAINT fk_subscription FOREIGN KEY (subscription_id)
				REFERENCES subscriptions(id) ON DELETE CASCADE,
			CONSTRAINT unique_node_per_subscription UNIQUE (subscription_id, node_id)
		);
	`)
	if err != nil {
		t.Fatalf("create tables: %v", err)
	}

	return NewSubscription(db.TXFactory(sqlDB))
}

// A node belongs to exactly one subscription, so the second add is a conflict
// the HTTP layer can explain rather than a generic write failure. The reply
// code the hub maps to 409 depends on this translation.
func TestInsertSubscriptionsReportsANodeAlreadySubscribed(t *testing.T) {
	store := newTestSubscriptions(t)
	ctx := context.Background()

	node := models.Node{ID: uuid.New()}
	user := models.User{ID: uuid.New()}

	first := models.SubscribedNodes{
		Nodes:        []models.Node{node},
		Subscription: models.Subscription{ID: uuid.New(), User: user},
	}

	if err := store.InsertSubscriptions(ctx, []models.SubscribedNodes{first}); err != nil {
		t.Fatalf("insert first subscription: %v", err)
	}

	second := models.SubscribedNodes{
		Nodes:        []models.Node{node},
		Subscription: models.Subscription{ID: uuid.New(), User: user},
	}

	err := store.InsertSubscriptions(ctx, []models.SubscribedNodes{second})
	if !errors.Is(err, models.ErrNodeAlreadySubscribed) {
		t.Fatalf("second insert returned %v, want ErrNodeAlreadySubscribed", err)
	}
}

// The failed insert runs inside a transaction, so the rollback has to leave no
// subscription row behind: a leftover would be an empty subscription that the
// caller was told had failed.
func TestAFailedNodeInsertLeavesNoSubscription(t *testing.T) {
	store := newTestSubscriptions(t)
	ctx := context.Background()

	node := models.Node{ID: uuid.New()}
	user := models.User{ID: uuid.New()}

	first := models.SubscribedNodes{
		Nodes:        []models.Node{node},
		Subscription: models.Subscription{ID: uuid.New(), User: user},
	}

	if err := store.InsertSubscriptions(ctx, []models.SubscribedNodes{first}); err != nil {
		t.Fatalf("insert first subscription: %v", err)
	}

	second := models.SubscribedNodes{
		Nodes:        []models.Node{node},
		Subscription: models.Subscription{ID: uuid.New(), User: user},
	}

	if err := store.InsertSubscriptions(ctx, []models.SubscribedNodes{second}); err == nil {
		t.Fatal("second insert succeeded")
	}

	var count int

	row := store.DB.QueryRowContext(ctx, "SELECT COUNT(*) FROM subscriptions WHERE id = ?", second.Subscription.ID[:])
	if err := row.Scan(&count); err != nil {
		t.Fatalf("count subscriptions: %v", err)
	}

	if count != 0 {
		t.Errorf("%d subscription rows survived the rollback, want 0", count)
	}
}

// SQLITE_CONSTRAINT covers NOT NULL, CHECK and foreign key failures as well as
// UNIQUE, so matching the primary code would report a missing field as a
// duplicate node. The driver error is built by hand because the extended code
// is the whole subject of the test.
func TestIsUniqueViolationOnlyMatchesUniqueFailures(t *testing.T) {
	unique := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: sqlite3.ErrConstraintUnique}
	if !isUniqueViolation(unique) {
		t.Error("a UNIQUE failure was not recognised")
	}

	for _, extended := range []sqlite3.ErrNoExtended{
		sqlite3.ErrConstraintNotNull,
		sqlite3.ErrConstraintCheck,
		sqlite3.ErrConstraintForeignKey,
	} {
		other := sqlite3.Error{Code: sqlite3.ErrConstraint, ExtendedCode: extended}
		if isUniqueViolation(other) {
			t.Errorf("%v was reported as a duplicate", extended)
		}
	}

	if isUniqueViolation(errors.New("no such table: subscribed_nodes")) {
		t.Error("a non-driver error was reported as a duplicate")
	}
}
