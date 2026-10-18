package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"testing"

	"github.com/booleanism/surr/analytics/internal/usecases"
	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

// The field order is not free: govet's fieldalignment wants the pointer-bearing
// fields packed first so the GC scans the fewest leading bytes, and a slice
// carries two non-pointer words after its pointer.
type fakeQueries struct {
	err error
	out json.RawMessage
	got models.AnalyticsQueryRequest
}

func (f *fakeQueries) Execute(_ context.Context, req models.AnalyticsQueryRequest, out *json.RawMessage) error {
	f.got = req

	if f.err != nil {
		return f.err
	}

	*out = f.out

	return nil
}

// assertAllCommandsRegistered fails when the two setup functions do not together
// advertise exactly the expected command set, so a command cannot be added or
// dropped without this test noticing.
func assertAllCommandsRegistered(t *testing.T) {
	t.Helper()

	h := newCaptureHandlers()

	if err := SetupDeclaration(&fakeDeclarations{}, h); err != nil {
		t.Fatalf("setup declaration handlers: %v", err)
	}

	if err := SetupQuery(&fakeQueries{}, h); err != nil {
		t.Fatalf("setup query handlers: %v", err)
	}

	var advertised []protocols.Command
	if n := h.AvailableCommand(&advertised); n != len(advertised) {
		t.Fatalf("AvailableCommand reported %d commands but filled %d", n, len(advertised))
	}

	got := make([]string, 0, len(advertised))
	for _, cmd := range advertised {
		got = append(got, string(cmd))
	}

	sort.Strings(got)

	want := []string{"analytics.declare", "analytics.queries.list", "analytics.query"}
	if len(got) != len(want) {
		t.Fatalf("advertised %v, want %v", got, want)
	}

	for i, cmd := range want {
		if got[i] != cmd {
			t.Fatalf("advertised %v, want %v", got, want)
		}
	}
}

type queryCase struct {
	queries  *fakeQueries
	name     string
	payload  string
	wantBody string
	wantMsg  string
	wantCode int
}

func TestQueryHandlers(t *testing.T) {
	const request = `{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f","query_name":"detail","params":{"n":1}}`

	tests := []queryCase{
		{
			name: "success", payload: request,
			queries:  &fakeQueries{out: json.RawMessage(`[{"_id":null,"n":3}]`)},
			wantCode: protocols.CodeSuccess,
			wantBody: `[{"_id":null,"n":3}]`,
		},
		{
			name: "an empty result", payload: request,
			queries:  &fakeQueries{out: json.RawMessage(`[]`)},
			wantCode: protocols.CodeSuccess,
			wantBody: `[]`,
		},
		{
			name: "undecodable payload", payload: `{"node_id":`,
			wantCode: protocols.CodeDecodePayload,
		},
		{
			name: "an undeclared query", payload: request,
			queries:  &fakeQueries{err: models.ErrDeclarationNotFound},
			wantCode: protocols.CodeNotFound,
			wantMsg:  "declaration not found",
		},
		{
			name: "a pipeline the service will not run", payload: request,
			queries:  &fakeQueries{err: usecases.ErrInvalidPipeline},
			wantCode: protocols.CodeDecodePayload,
			wantMsg:  "invalid pipeline",
		},
		{
			name: "a parameter that was not supplied", payload: request,
			queries:  &fakeQueries{err: usecases.ErrMissingParam},
			wantCode: protocols.CodeDecodePayload,
			wantMsg:  "missing query parameter",
		},
		{
			name: "a parameter that was not declared", payload: request,
			queries:  &fakeQueries{err: usecases.ErrUnknownParam},
			wantCode: protocols.CodeDecodePayload,
			wantMsg:  "unknown query parameter",
		},
		{
			// A result over the cap is this service's own limit, not the caller's
			// mistake, so it is a domain failure rather than a payload one.
			name: "a result over the cap", payload: request,
			queries:  &fakeQueries{err: models.ErrQueryTooLarge},
			wantCode: protocols.CodeDomain,
			wantMsg:  "query result too large",
		},
		{
			name: "a storage failure", payload: request,
			queries:  &fakeQueries{err: errors.New("connection refused")},
			wantCode: protocols.CodeDomain,
			wantMsg:  "connection refused",
		},
	}

	assertAllCommandsRegistered(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCaptureHandlers()

			queries := tt.queries
			if queries == nil {
				queries = &fakeQueries{}
			}

			err := SetupQuery(queries, h)
			if err != nil {
				t.Fatalf("setup query handlers: %v", err)
			}

			decoded := assertWire(t, h.invoke(t, "analytics.query", []byte(tt.payload)), tt.wantCode)

			if tt.wantMsg != "" {
				if decoded.ErrorMessage() != tt.wantMsg {
					t.Fatalf("message %q, want %q", decoded.ErrorMessage(), tt.wantMsg)
				}

				return
			}

			if tt.wantBody != "" && string(decoded.Result) != tt.wantBody {
				t.Fatalf("body is %s, want %s", decoded.Result, tt.wantBody)
			}
		})
	}
}

// The request must reach the usecase intact: the caller names a query and passes
// parameters, and neither is interpreted at this layer.
func TestQueryPassesTheRequestThrough(t *testing.T) {
	h := newCaptureHandlers()
	queries := &fakeQueries{out: json.RawMessage(`[]`)}

	if err := SetupQuery(queries, h); err != nil {
		t.Fatalf("setup query handlers: %v", err)
	}

	payload := `{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f","query_name":"detail","params":{"n":7}}`

	assertWire(t, h.invoke(t, "analytics.query", []byte(payload)), protocols.CodeSuccess)

	if queries.got.NodeID != uuid.MustParse("6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f") {
		t.Fatalf("node id arrived as %s", queries.got.NodeID)
	}

	if queries.got.QueryName != "detail" {
		t.Fatalf("query name arrived as %q", queries.got.QueryName)
	}

	if queries.got.Params["n"] != float64(7) {
		t.Fatalf("params arrived as %v", queries.got.Params)
	}
}
