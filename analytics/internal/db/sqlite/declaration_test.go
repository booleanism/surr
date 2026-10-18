package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

const migrationsDir = "../../migrations/sqlite"

// newTestDeclaration returns a declaration store over an in-memory database
// carrying the schema the real migrations create.
//
// The migration files are executed rather than restated here, unlike the
// equivalent helper in subscriptions: a hand-copied CREATE TABLE can drift from
// the migration it is meant to mirror, and then the test passes against a schema
// no deployment has. Running the files also means a broken migration fails in
// this test rather than in the deploy stack.
func newTestDeclaration(t *testing.T) declaration {
	t.Helper()

	sqlDB, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
	})

	// An in-memory database is per connection, so a second pooled connection
	// would see no tables at all. One connection keeps the whole test on the
	// database the migrations were applied to.
	sqlDB.SetMaxOpenConns(1)

	applyMigrations(t, sqlDB)

	return NewDeclaration(db.TXFactory(sqlDB))
}

// applyMigrations runs every *.up.sql in the migrations directory, in order.
func applyMigrations(t *testing.T, sqlDB *sql.DB) {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(migrationsDir, "*.up.sql"))
	if err != nil {
		t.Fatalf("list migrations: %v", err)
	}

	if len(paths) == 0 {
		t.Fatalf("no migrations found in %s", migrationsDir)
	}

	sort.Strings(paths)

	for _, path := range paths {
		body, readErr := os.ReadFile(path) //nolint:gosec // a path this test built
		if readErr != nil {
			t.Fatalf("read %s: %v", path, readErr)
		}

		if _, execErr := sqlDB.Exec(string(body)); execErr != nil {
			t.Fatalf("apply %s: %v", path, execErr)
		}
	}
}

func testDeclaration(nodeID uuid.UUID) models.Declaration {
	return models.Declaration{
		NodeID:     nodeID,
		Collection: "50154422fd3b4cf0a761908fcbe98a1e",
		Queries: []models.Query{
			{
				Name:         "detail",
				Description:  "counts over time",
				ParamsSchema: []byte(`{"granularity":"string"}`),
				Pipeline:     []byte(`[{"$match":{"node.id":"x"}}]`),
			},
			{
				Name:     "summary",
				Pipeline: []byte(`[{"$group":{"_id":"$k"}}]`),
			},
		},
	}
}

func TestUpsertDeclarationStoresTheManifest(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(nodeID)); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	names := make([]string, 0, 8)

	n, err := store.SelectQueryNames(ctx, nodeID, &names)
	if err != nil {
		t.Fatalf("select query names: %v", err)
	}

	if n != 2 {
		t.Fatalf("got %d names, want 2 (%v)", n, names)
	}

	// Sorted, because the reply may be shown to a human and SQLite promises no
	// row order otherwise.
	want := []string{"detail", "summary"}
	for i, name := range want {
		if names[i] != name {
			t.Fatalf("names[%d] is %q, want %q (%v)", i, names[i], name, names)
		}
	}
}

// A manifest is a set: a re-declare that drops a query has to remove it.
// Upserting only the queries that were sent would keep answering a query the
// node no longer offers.
func TestReDeclaringReplacesTheWholeManifest(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(nodeID)); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	shrunk := testDeclaration(nodeID)
	shrunk.Collection = "other"
	shrunk.Queries = shrunk.Queries[:1]

	if err := store.UpsertDeclaration(ctx, shrunk); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	names := make([]string, 0, 8)

	n, err := store.SelectQueryNames(ctx, nodeID, &names)
	if err != nil {
		t.Fatalf("select query names: %v", err)
	}

	if n != 1 || names[0] != "detail" {
		t.Fatalf("got %d names %v, want just [detail]", n, names)
	}

	// The dropped query must be gone from the lookup path too, not merely
	// absent from the list.
	if _, selErr := store.SelectQuery(ctx, nodeID, "summary"); !errors.Is(selErr, ErrDeclarationNotFound) {
		t.Fatalf("summary is still resolvable after being dropped: %v", selErr)
	}

	// And the surviving query must carry the collection from the NEW manifest.
	got, err := store.SelectQuery(ctx, nodeID, "detail")
	if err != nil {
		t.Fatalf("select detail: %v", err)
	}

	if got.Collection != "other" {
		t.Fatalf("collection is %q, want %q", got.Collection, "other")
	}
}

// The pipeline has to survive the round trip byte for byte. A decoded Go map
// would lose the operand order of $sort, which changes what the query means.
func TestSelectQueryReturnsThePipelineVerbatim(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	pipeline := `[{"$sort":{"b":-1,"a":1}},{"$limit":5}]`

	decl := models.Declaration{
		NodeID:     nodeID,
		Collection: "c",
		Queries:    []models.Query{{Name: "sorted", Pipeline: []byte(pipeline)}},
	}

	if err := store.UpsertDeclaration(ctx, decl); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	got, err := store.SelectQuery(ctx, nodeID, "sorted")
	if err != nil {
		t.Fatalf("select query: %v", err)
	}

	if string(got.Query.Pipeline) != pipeline {
		t.Fatalf("pipeline is %s, want %s", got.Query.Pipeline, pipeline)
	}
}

// An absent params_schema must not be stored as the empty string: that is not
// valid JSON, so decoding the column later would fail on an otherwise
// well-formed row.
func TestAnAbsentParamsSchemaIsStoredAsAnEmptyObject(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(nodeID)); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	got, err := store.SelectQuery(ctx, nodeID, "summary")
	if err != nil {
		t.Fatalf("select query: %v", err)
	}

	if string(got.Query.ParamsSchema) != "{}" {
		t.Fatalf("params_schema is %q, want {}", got.Query.ParamsSchema)
	}
}

func TestSelectQueryReportsAnUnknownName(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(nodeID)); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	_, err := store.SelectQuery(ctx, nodeID, "not-declared")
	if !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("got %v, want ErrDeclarationNotFound", err)
	}
}

func TestUpsertDeclarationRefusesAnEmptyManifest(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	decl := testDeclaration(uuid.New())
	decl.Queries = nil

	err := store.UpsertDeclaration(ctx, decl)
	if !errors.Is(err, ErrNothingToDeclare) {
		t.Fatalf("got %v, want ErrNothingToDeclare", err)
	}
}

// A truncated read must not look like a small result set.
func TestSelectQueryNamesReportsAFullBuffer(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(nodeID)); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	names := make([]string, 0, 1)

	n, err := store.SelectQueryNames(ctx, nodeID, &names)
	if !errors.Is(err, ErrBufferFull) {
		t.Fatalf("got %v, want ErrBufferFull", err)
	}

	if n != 1 {
		t.Fatalf("reported %d rows before stopping, want 1", n)
	}
}

// A node id is a BLOB column. Binding it as TEXT would match nothing, silently,
// so this pins that a declaration stored under one node is invisible to another.
func TestDeclarationsAreScopedToTheirNode(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	first, second := uuid.New(), uuid.New()

	if err := store.UpsertDeclaration(ctx, testDeclaration(first)); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	names := make([]string, 0, 8)

	n, err := store.SelectQueryNames(ctx, second, &names)
	if err != nil {
		t.Fatalf("select query names: %v", err)
	}

	if n != 0 {
		t.Fatalf("node %s sees %d names %v, want none", second, n, names)
	}
}
