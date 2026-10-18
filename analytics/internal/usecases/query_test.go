package usecases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

type fakeQuery struct {
	err        error
	collection string
	pipeline   string
	result     json.RawMessage
}

func (f *fakeQuery) Aggregate(_ context.Context, collection string, pipeline []byte, _, _ int) (json.RawMessage, error) {
	f.collection = collection
	f.pipeline = string(pipeline)

	if f.err != nil {
		return nil, f.err
	}

	if f.result == nil {
		return json.RawMessage(`[]`), nil
	}

	return f.result, nil
}

type fakeDeclarations2 struct {
	query *models.DeclaredQuery
	err   error
}

func (f fakeDeclarations2) GetQuery(context.Context, uuid.UUID, string) (models.DeclaredQuery, error) {
	if f.query == nil {
		return models.DeclaredQuery{}, f.err
	}

	return *f.query, f.err
}

func declared(pipeline, schema string) models.DeclaredQuery {
	return models.DeclaredQuery{
		Collection: "counts",
		Query: models.Query{
			Name:         "detail",
			Pipeline:     json.RawMessage(pipeline),
			ParamsSchema: json.RawMessage(schema),
		},
	}
}

// ptr is a one-line helper so a table entry can build a fake with a pointer
// field without a local variable.
func ptr(d models.DeclaredQuery) *models.DeclaredQuery {
	return &d
}

func runQuery(t *testing.T, pipeline, schema string, params map[string]any) (string, error) {
	t.Helper()

	store := &fakeQuery{}

	var out json.RawMessage

	err := NewQuery(store, fakeDeclarations2{query: ptr(declared(pipeline, schema))}).
		Execute(context.Background(), models.AnalyticsQueryRequest{NodeID: uuid.New(), QueryName: "detail", Params: params}, &out)

	return store.pipeline, err
}

// The cap has to be appended, not merged into whatever $limit the manifest
// happened to carry: a manifest that omits $limit would otherwise return the
// whole collection, which is the failure the bound exists to prevent.
func TestExecuteAppendsTheDocumentCap(t *testing.T) {
	pipeline, err := runQuery(t, `[{"$match":{}}]`, ``, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	want := fmt.Sprintf(`{"$limit":%d}]`, maxResultDocuments+1)
	if !strings.HasSuffix(pipeline, want) {
		t.Fatalf("pipeline %s does not end with the cap %s", pipeline, want)
	}
}

// A manifest's own smaller $limit is kept, and the cap still lands after it.
func TestExecuteKeepsTheManifestsOwnLimit(t *testing.T) {
	pipeline, err := runQuery(t, `[{"$match":{}},{"$limit":5}]`, ``, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if !strings.Contains(pipeline, `{"$limit":5}`) {
		t.Fatalf("the manifest's own $limit is gone: %s", pipeline)
	}

	if !strings.HasSuffix(pipeline, fmt.Sprintf(`{"$limit":%d}]`, maxResultDocuments+1)) {
		t.Fatalf("the cap is not the last stage: %s", pipeline)
	}
}

// The operand order of $sort is part of what the query means, so the bytes must
// reach storage untouched. A decode-and-re-encode would normalise them.
func TestExecutePreservesTheOperandOrder(t *testing.T) {
	pipeline, err := runQuery(t, `[{"$sort":{"b":-1,"a":1}}]`, ``, nil)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if !strings.HasPrefix(pipeline, `[{"$sort":{"b":-1,"a":1}}`) {
		t.Fatalf("pipeline came back reordered: %s", pipeline)
	}
}

// A placeholder that IS the whole string carries its type through: a number has
// to stay a number, or $limit and $dateTrunc would receive a string.
func TestAWholePlaceholderKeepsItsType(t *testing.T) {
	pipeline, err := runQuery(t, `[{"$limit":"${n}"}]`, `{"n":"number"}`, map[string]any{"n": float64(7)})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if !strings.HasPrefix(pipeline, `[{"$limit":7}`) {
		t.Fatalf("pipeline is %s, want the number 7", pipeline)
	}
}

func TestAPlaceholderInsideAStringRendersAsText(t *testing.T) {
	pipeline, err := runQuery(t, `[{"$match":{"id":"node-${node}"}}]`, `{"node":"string"}`, map[string]any{"node": "abc"})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if !strings.HasPrefix(pipeline, `[{"$match":{"id":"node-abc"}}`) {
		t.Fatalf("pipeline is %s", pipeline)
	}
}

type pipelineCase struct {
	params   map[string]any
	want     error
	name     string
	pipeline string
	schema   string
	wantMsg  string
}

func TestExecuteRefusesAPipelineItCannotTrust(t *testing.T) {
	tests := []pipelineCase{
		{
			name: "a writing stage", pipeline: `[{"$out":"elsewhere"}]`,
			want: ErrInvalidPipeline, wantMsg: `"$out"`,
		},
		{
			name: "a merging stage", pipeline: `[{"$merge":{"into":"elsewhere"}}]`,
			want: ErrInvalidPipeline, wantMsg: `"$merge"`,
		},
		{
			name: "a stage that reads another collection", pipeline: `[{"$unionWith":"elsewhere"}]`,
			want: ErrInvalidPipeline, wantMsg: `"$unionWith"`,
		},
		{
			name: "a join", pipeline: `[{"$lookup":{"from":"users"}}]`,
			want: ErrInvalidPipeline, wantMsg: `"$lookup"`,
		},
		{
			// The reason the check is an allowlist and a recursive walk: a
			// blocklist of stage names never looks inside a $facet.
			name: "a writing stage hidden in a facet", pipeline: `[{"$facet":{"a":[{"$out":"elsewhere"}]}}]`,
			want: ErrInvalidPipeline, wantMsg: `"$out"`,
		},
		{
			name: "a stage holding two operators", pipeline: `[{"$match":{},"$limit":5}]`,
			want: ErrInvalidPipeline, wantMsg: "exactly one operator",
		},
		{
			name: "a stage that is not an object", pipeline: `[1]`,
			want: ErrInvalidPipeline, wantMsg: "not a JSON object",
		},
		{
			name: "a $limit that is not a number", pipeline: `[{"$limit":"5"}]`,
			want: ErrInvalidPipeline, wantMsg: "not a whole number",
		},
		{
			name: "a $limit of zero", pipeline: `[{"$limit":0}]`,
			want: ErrInvalidPipeline, wantMsg: "a $limit of 0",
		},
		{
			name: "an empty pipeline", pipeline: `[]`,
			want: ErrInvalidPipeline, wantMsg: "empty",
		},
		{
			name: "too many stages", pipeline: `[` + strings.Repeat(`{"$match":{}},`, maxPipelineStages) + `{"$match":{}}]`,
			want: ErrInvalidPipeline, wantMsg: "more than the",
		},
		{
			name: "a parameter that is used but not declared", pipeline: `[{"$limit":"${n}"}]`,
			want: ErrUnknownParam, wantMsg: "not declared in params_schema",
			params: map[string]any{"n": float64(1)},
		},
		{
			name: "a parameter that is declared but not supplied", pipeline: `[{"$limit":"${n}"}]`,
			schema: `{"n":"number"}`,
			want:   ErrMissingParam, wantMsg: "was not supplied",
		},
		{
			name: "a parameter that is supplied but not declared", pipeline: `[{"$match":{}}]`,
			want: ErrUnknownParam, wantMsg: "was supplied but is not declared",
			params: map[string]any{"n": float64(1)},
		},
		{
			name: "a parameter of the wrong type", pipeline: `[{"$limit":"${n}"}]`,
			schema: `{"n":"number"}`, params: map[string]any{"n": "seven"},
			want: ErrInvalidPipeline, wantMsg: "which is not a number",
		},
		{
			name: "a declared type that is not one of the five", pipeline: `[{"$match":{}}]`,
			schema: `{"n":"float"}`,
			want:   ErrInvalidDeclaration, wantMsg: "which is not one of",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := runQuery(t, tt.pipeline, tt.schema, tt.params)
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}

			if !strings.Contains(err.Error(), tt.wantMsg) {
				t.Fatalf("error %q does not mention %q", err, tt.wantMsg)
			}
		})
	}
}

func TestExecuteReturnsTheDocuments(t *testing.T) {
	store := &fakeQuery{result: json.RawMessage(`[{"_id":null,"n":3}]`)}
	declarations := fakeDeclarations2{query: ptr(declared(`[{"$count":"n"}]`, ``))}

	var out json.RawMessage

	err := NewQuery(store, declarations).
		Execute(context.Background(), models.AnalyticsQueryRequest{NodeID: uuid.New(), QueryName: "detail"}, &out)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}

	if string(out) != `[{"_id":null,"n":3}]` {
		t.Fatalf("out is %s", out)
	}
}

// The storage layer's failures must reach the caller unchanged: the handler maps
// each one to a different reply code, and wrapping them all into one error here
// would collapse that distinction.
func TestExecutePassesAStorageFailureThrough(t *testing.T) {
	for _, want := range []error{models.ErrDeclarationNotFound, models.ErrQueryTooLarge} {
		store := &fakeQuery{err: want}
		declarations := fakeDeclarations2{query: ptr(declared(`[{"$match":{}}]`, ``))}

		var out json.RawMessage

		err := NewQuery(store, declarations).
			Execute(context.Background(), models.AnalyticsQueryRequest{NodeID: uuid.New()}, &out)

		if !errors.Is(err, want) {
			t.Fatalf("got %v, want %v", err, want)
		}
	}
}
