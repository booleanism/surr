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

// The list a caller runs queries from has to carry the params schema: without it
// the caller cannot tell which parameters each query takes, and would send a
// parameter the query never declared (a payload error) or omit one it did (a
// missing parameter).
func TestSelectQueryInfosCarriesTheParamsSchema(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()
	nodeID := uuid.New()

	decl := testDeclaration(nodeID)
	decl.Queries[0].ParamsSchema = []byte(`{"start_time":"string","unit_time":"string"}`)
	decl.Queries[0].Description = "average per class over time"

	if err := store.UpsertDeclaration(ctx, decl); err != nil {
		t.Fatalf("upsert declaration: %v", err)
	}

	infos := make([]models.QueryInfo, 0, 8)

	n, err := store.SelectQueryInfos(ctx, nodeID, &infos)
	if err != nil {
		t.Fatalf("select query infos: %v", err)
	}

	if n != len(decl.Queries) || len(infos) != n {
		t.Fatalf("got %d infos, want %d", len(infos), len(decl.Queries))
	}

	if string(infos[0].ParamsSchema) != `{"start_time":"string","unit_time":"string"}` {
		t.Fatalf("the params schema came back as %s", infos[0].ParamsSchema)
	}

	if infos[0].Description != "average per class over time" {
		t.Fatalf("the description came back as %q", infos[0].Description)
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

// fleetDeclaration is a manifest with one node-scoped query and one that reads
// across nodes, which is what a real node type declares.
func fleetDeclaration(nodeID uuid.UUID, nodeType, collection string) models.Declaration {
	return fleetDeclarationNamed(nodeID, nodeType, collection, "fleet_activity")
}

// fleetDeclarationNamed takes the fleet query's name so a test can tell one
// type's rollup from another's. Two types declaring the same name is the case
// that would hide a cross-type leak behind a name match.
func fleetDeclarationNamed(nodeID uuid.UUID, nodeType, collection, fleetName string) models.Declaration {
	return models.Declaration{
		NodeID:     nodeID,
		NodeType:   nodeType,
		Collection: collection,
		Queries: []models.Query{
			{
				Name:     "activity_by_class",
				Pipeline: []byte(`[{"$match":{"node.id":"x"}}]`),
			},
			{
				Name:     fleetName,
				Scope:    models.ScopeFleet,
				Pipeline: []byte(`[{"$match":{"$expr":{}}}]`),
			},
		},
	}
}

// The type's fleet manifest is a set owned by the type, so the node that
// declares one replaces whatever the type had -- including a query the new
// manifest no longer lists. Without that, a fleet query from a retired build
// would stay answerable forever: the node that declared it is not the node that
// would have to undeclare it.
func TestAFleetReDeclareReplacesTheTypesWholeManifest(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	first, second := uuid.New(), uuid.New()

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(first, "bustle", "aaa")); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	if err := store.UpsertDeclaration(ctx, fleetDeclarationNamed(second, "bustle", "bbb", "fleet_renamed")); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	// The type's set is replaced, not accumulated: the first node's rollup is
	// gone even though that node never re-declared.
	if _, err := store.SelectQuery(ctx, second, "fleet_activity"); !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("a fleet query the type no longer declares still answers: %v", err)
	}

	if _, err := store.SelectQuery(ctx, second, "fleet_renamed"); err != nil {
		t.Fatalf("the replacing fleet query does not answer: %v", err)
	}
}

// A node that declares no fleet query must leave the type's fleet manifest
// alone. Otherwise every node of a type could erase the fleet rollup by
// re-declaring itself, and the query would vanish as soon as one node restarted.
func TestANodeWithoutAFleetQueryLeavesTheFleetManifestAlone(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	declarer, plain := uuid.New(), uuid.New()

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(declarer, "bustle", "aaa")); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	withoutFleet := testDeclaration(plain)
	withoutFleet.NodeType = "bustle"

	if err := store.UpsertDeclaration(ctx, withoutFleet); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	declared, err := store.SelectQuery(ctx, plain, "fleet_activity")
	if err != nil {
		t.Fatalf("the fleet query disappeared: %v", err)
	}

	if declared.Scope != models.ScopeFleet {
		t.Fatalf("the fleet query came back with scope %q", declared.Scope)
	}
}

// A fleet query is not the declaring node's: any node of the type can run it.
// That is the whole point -- the node that published it may be the one that
// restarted.
func TestAFleetQueryIsVisibleToEveryNodeOfItsType(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	declarer, sibling, stranger := uuid.New(), uuid.New(), uuid.New()

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(declarer, "bustle", "aaa")); err != nil {
		t.Fatalf("declarer upsert: %v", err)
	}

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(sibling, "bustle", "bbb")); err != nil {
		t.Fatalf("sibling upsert: %v", err)
	}

	if err := store.UpsertDeclaration(ctx, fleetDeclarationNamed(stranger, "plate", "ccc", "plate_fleet")); err != nil {
		t.Fatalf("stranger upsert: %v", err)
	}

	if names := queryInfoNames(t, store, sibling); len(names) != 2 || names[1] != "fleet_activity" {
		t.Fatalf("a bustle node sees %v, want its own query plus the fleet one", names)
	}

	// The fleet query belongs to the type, so a node of another type neither
	// sees it nor can run it.
	if names := queryInfoNames(t, store, stranger); len(names) != 2 || names[1] != "plate_fleet" {
		t.Fatalf("a plate node sees %v, want its own query plus plate_fleet", names)
	}

	if _, err := store.SelectQuery(ctx, stranger, "fleet_activity"); !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("a plate node can run the bustle fleet query: %v", err)
	}
}

// queryInfoNames is the query names one node may run, sorted as the store
// returns them.
func queryInfoNames(t *testing.T, store declaration, nodeID uuid.UUID) []string {
	t.Helper()

	infos := make([]models.QueryInfo, 0, 8)

	if _, err := store.SelectQueryInfos(context.Background(), nodeID, &infos); err != nil {
		t.Fatalf("select query infos: %v", err)
	}

	names := make([]string, 0, len(infos))

	for _, info := range infos {
		names = append(names, info.Name)
	}

	return names
}

// Membership is the intersection of the type the registry knows and the ids the
// caller is allowed to read. Neither alone is enough: without the type a caller
// could union another node type's collection, and without the ids it could read
// another user's nodes.
func TestFleetMembersAreTheCallersNodesOfThatType(t *testing.T) {
	store := newTestDeclaration(t)
	ctx := context.Background()

	mine, theirs, otherType := uuid.New(), uuid.New(), uuid.New()

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(mine, "bustle", "mine")); err != nil {
		t.Fatalf("mine: %v", err)
	}

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(theirs, "bustle", "theirs")); err != nil {
		t.Fatalf("theirs: %v", err)
	}

	if err := store.UpsertDeclaration(ctx, fleetDeclaration(otherType, "plate", "other")); err != nil {
		t.Fatalf("other type: %v", err)
	}

	collections := make([]string, 0, 8)

	n, err := store.SelectFleetCollections(ctx, "bustle", []uuid.UUID{mine, otherType}, &collections)
	if err != nil {
		t.Fatalf("select fleet collections: %v", err)
	}

	if n != 1 || len(collections) != 1 || collections[0] != "mine" {
		t.Fatalf("fleet is %v, want just the caller's own bustle collection", collections)
	}
}

// A node that declared nothing has no collection, so it is not a member -- the
// same rule that makes a fleet query unavailable to a node that never introduced
// itself.
func TestFleetMembersExcludeANodeThatNeverDeclared(t *testing.T) {
	store := newTestDeclaration(t)

	collections := make([]string, 0, 8)

	n, err := store.SelectFleetCollections(context.Background(), "bustle", []uuid.UUID{uuid.New()}, &collections)
	if err != nil {
		t.Fatalf("select fleet collections: %v", err)
	}

	if n != 0 {
		t.Fatalf("fleet has %d collections %v, want none", n, collections)
	}
}
