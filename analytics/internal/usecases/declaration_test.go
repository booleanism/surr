package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// query is a pointer so the struct's pointer-bearing fields stay small; the
// value it points at is the caller's, which is all a fake needs.
type fakeStore struct {
	query     *models.DeclaredQuery
	upsertErr error
	queryErr  error
	stored    []models.Declaration
	infos     []models.QueryInfo
}

func (f *fakeStore) UpsertDeclaration(_ context.Context, decl models.Declaration) error {
	if f.upsertErr != nil {
		return f.upsertErr
	}

	f.stored = append(f.stored, decl)

	return nil
}

func (f *fakeStore) SelectQueryInfos(_ context.Context, _ uuid.UUID, infos *[]models.QueryInfo) (int, error) {
	if f.queryErr != nil {
		return 0, f.queryErr
	}

	(*infos) = append((*infos), f.infos...)

	return len(f.infos), nil
}

func (f *fakeStore) SelectQuery(_ context.Context, _ uuid.UUID, _ string) (models.DeclaredQuery, error) {
	if f.query == nil {
		return models.DeclaredQuery{}, f.queryErr
	}

	return *f.query, f.queryErr
}

// validDeclaration is the baseline every rejection case mutates, so a case only
// has to state the one thing it breaks.
func validDeclaration() models.Declaration {
	return models.Declaration{
		NodeID:     uuid.New(),
		Collection: "50154422fd3b4cf0a761908fcbe98a1e",
		Queries: []models.Query{
			{Name: "detail", Pipeline: []byte(`[{"$match":{"node.id":"x"}}]`)},
		},
	}
}

type rejectCase struct {
	mutate func(*models.Declaration)
	name   string
	want   string
}

func TestDeclareRejectsAnUnusableManifest(t *testing.T) {
	tests := []rejectCase{
		{
			name:   "no node id",
			mutate: func(d *models.Declaration) { d.NodeID = uuid.Nil },
			want:   "node_id is required",
		},
		{
			name:   "no collection",
			mutate: func(d *models.Declaration) { d.Collection = "" },
			want:   "collection is required",
		},
		{
			name:   "a system collection",
			mutate: func(d *models.Declaration) { d.Collection = "system.views" },
			want:   "may not be a system collection",
		},
		{
			name:   "a collection with a dollar sign",
			mutate: func(d *models.Declaration) { d.Collection = "nodes$oops" },
			want:   "may not contain '$'",
		},
		{
			name:   "an overlong collection",
			mutate: func(d *models.Declaration) { d.Collection = strings.Repeat("c", maxCollectionLength+1) },
			want:   "longer than",
		},
		{
			name:   "no queries",
			mutate: func(d *models.Declaration) { d.Queries = nil },
			want:   "at least one query",
		},
		{
			name:   "a query with no name",
			mutate: func(d *models.Declaration) { d.Queries[0].Name = "" },
			want:   "a query needs a name",
		},
		{
			name:   "an upper-case query name",
			mutate: func(d *models.Declaration) { d.Queries[0].Name = "Detail" },
			want:   "must be lowercase",
		},
		{
			name:   "an overlong query name",
			mutate: func(d *models.Declaration) { d.Queries[0].Name = strings.Repeat("q", maxQueryNameLength+1) },
			want:   "longer than",
		},
		{
			name: "a duplicated query name",
			mutate: func(d *models.Declaration) {
				d.Queries = append(d.Queries, d.Queries[0])
			},
			want: "declared twice",
		},
		{
			name:   "a pipeline that is not an array",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = []byte(`{"$match":{}}`) },
			want:   "not a JSON array",
		},
		{
			name:   "an empty pipeline",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = []byte(`[]`) },
			want:   "the pipeline is empty",
		},
		{
			name:   "a stage that is not an object",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = []byte(`[1]`) },
			want:   "not a JSON object",
		},
		{
			name:   "a stage holding two operators",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = []byte(`[{"$match":{},"$limit":5}]`) },
			want:   "exactly one operator",
		},
		{
			// The allowlist has to run at declare time, not only at query time:
			// this is where the author can still act on the answer.
			name:   "a pipeline that writes",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = []byte(`[{"$out":"stolen"}]`) },
			want:   "not an allowed stage",
		},
		{
			// A writing stage one level down is the case a blocklist loses: the
			// outer pipeline is a plain $facet.
			name: "a writing stage hidden in a facet",
			mutate: func(d *models.Declaration) {
				d.Queries[0].Pipeline = []byte(`[{"$facet":{"x":[{"$merge":"stolen"}]}}]`)
			},
			want: "not an allowed stage",
		},
		{
			name:   "a stage count over the cap",
			mutate: func(d *models.Declaration) { d.Queries[0].Pipeline = manyStages(maxPipelineStages + 1) },
			want:   "more than the",
		},
		{
			name:   "a params schema that is not an object",
			mutate: func(d *models.Declaration) { d.Queries[0].ParamsSchema = []byte(`[]`) },
			want:   "params_schema is not a JSON object",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decl := validDeclaration()
			tt.mutate(&decl)

			store := &fakeStore{}

			err := NewDeclaration(store).Declare(context.Background(), decl)
			if !errors.Is(err, ErrInvalidDeclaration) {
				t.Fatalf("got %v, want ErrInvalidDeclaration", err)
			}

			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not mention %q", err, tt.want)
			}

			// A rejected manifest must not reach storage: storing it would
			// replace a working manifest with a broken one.
			if len(store.stored) != 0 {
				t.Fatalf("a rejected declaration was stored: %+v", store.stored)
			}
		})
	}
}

// A $limit that is still a placeholder is legal at declare time: parameters have
// not been substituted yet, so the value cannot be a number. It is checked when
// the query runs.
func TestDeclareAcceptsAPlaceholderLimit(t *testing.T) {
	store := &fakeStore{}

	decl := validDeclaration()
	decl.Queries[0].Pipeline = []byte(`[{"$sort":{"n":-1}},{"$limit":"${limit}"}]`)
	decl.Queries[0].ParamsSchema = []byte(`{"limit":"number"}`)

	if err := NewDeclaration(store).Declare(context.Background(), decl); err != nil {
		t.Fatalf("declare: %v", err)
	}
}

func manyStages(n int) []byte {
	stages := make([]string, n)
	for i := range stages {
		stages[i] = `{"$match":{}}`
	}

	return []byte("[" + strings.Join(stages, ",") + "]")
}

func TestDeclareStoresAValidManifest(t *testing.T) {
	store := &fakeStore{}
	decl := validDeclaration()

	err := NewDeclaration(store).Declare(context.Background(), decl)
	if err != nil {
		t.Fatalf("declare: %v", err)
	}

	if len(store.stored) != 1 {
		t.Fatalf("stored %d declarations, want 1", len(store.stored))
	}

	if store.stored[0].Queries[0].Name != "detail" {
		t.Fatalf("stored query name is %q", store.stored[0].Queries[0].Name)
	}
}

// The whole point of validating at declare time is that the storage error is not
// swallowed into the validation sentinel: a write failure is this service's
// problem, not the node's, and the handler maps the two to different reply codes.
func TestDeclareReportsAStorageFailureAsItself(t *testing.T) {
	failure := errors.New("database is locked")
	store := &fakeStore{upsertErr: failure}

	err := NewDeclaration(store).Declare(context.Background(), validDeclaration())
	if !errors.Is(err, failure) {
		t.Fatalf("got %v, want %v", err, failure)
	}

	if errors.Is(err, ErrInvalidDeclaration) {
		t.Fatal("a storage failure was reported as an invalid declaration")
	}
}

func TestListQueriesPassesTheDeclarationsThrough(t *testing.T) {
	store := &fakeStore{infos: []models.QueryInfo{
		{Name: "detail", ParamsSchema: []byte(`{"start_time":"string","unit_time":"string"}`)},
		{Name: "summary"},
	}}
	infos := make([]models.QueryInfo, 0, 8)

	n, err := NewDeclaration(store).ListQueries(context.Background(), uuid.New(), &infos)
	if err != nil {
		t.Fatalf("list queries: %v", err)
	}

	if n != 2 || len(infos) != 2 {
		t.Fatalf("got %d declarations %v, want 2", n, infos)
	}

	// The schema has to survive the trip: the caller that runs the queries reads
	// it to decide which parameters each one takes.
	if string(infos[0].ParamsSchema) != `{"start_time":"string","unit_time":"string"}` {
		t.Fatalf("the params schema was dropped: %s", infos[0].ParamsSchema)
	}
}

func TestGetQueryPassesTheDeclaredQueryThrough(t *testing.T) {
	want := models.DeclaredQuery{
		Collection: "c",
		Query:      models.Query{Name: "detail", Pipeline: []byte(`[{"$limit":1}]`)},
	}
	store := &fakeStore{query: &want}

	got, err := NewDeclaration(store).GetQuery(context.Background(), uuid.New(), "detail")
	if err != nil {
		t.Fatalf("get query: %v", err)
	}

	if got.Collection != want.Collection || got.Query.Name != want.Query.Name {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
