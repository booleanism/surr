package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/booleanism/surr/analytics/internal/usecases"
	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

type captureHandlers struct {
	handlers map[protocols.Command]protocols.HandlerFunc
}

func newCaptureHandlers() *captureHandlers {
	return &captureHandlers{handlers: make(map[protocols.Command]protocols.HandlerFunc)}
}

func (c *captureHandlers) Set(cmd protocols.Command, h protocols.HandlerFunc) error {
	c.handlers[cmd] = h
	return nil
}

// AvailableCommand mirrors protocols.handlers so the test can assert that every
// command this service advertises is actually exercised below.
func (c *captureHandlers) AvailableCommand(cmds *[]protocols.Command) int {
	(*cmds) = (*cmds)[:0]

	for k := range c.handlers {
		(*cmds) = append((*cmds), k)
	}

	return len(c.handlers)
}

// invoke runs one registered handler and returns the reply it published.
func (c *captureHandlers) invoke(t *testing.T, cmd protocols.Command, payload []byte) protocols.RouterResponse {
	t.Helper()

	h, ok := c.handlers[cmd]
	if !ok {
		t.Fatalf("handler %s is not registered", cmd)
	}

	var got protocols.RouterResponse

	rep := func(_ context.Context, resp protocols.RouterResponse) error {
		got = resp
		return nil
	}

	h(context.Background(), uuid.New(), protocols.RouterRequest{Payload: payload}, rep)

	return got
}

// assertWire checks that a reply survives the trip the router puts it through:
// the handler builds it, the router marshals it, the caller unmarshals it.
func assertWire(t *testing.T, resp protocols.RouterResponse, wantCode int) protocols.RouterResponse {
	t.Helper()

	body, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal reply: %v", err)
	}

	var decoded protocols.RouterResponse

	err = json.Unmarshal(body, &decoded)
	if err != nil {
		t.Fatalf("unmarshal reply %s: %v", body, err)
	}

	if decoded.Code != wantCode {
		t.Fatalf("code is %d, want %d (body %s)", decoded.Code, wantCode, body)
	}

	// Every failure reply carries its message as a JSON string, so a caller can
	// always read it with ErrorMessage().
	if wantCode != protocols.CodeSuccess && !json.Valid(decoded.Result) {
		t.Fatalf("code %d: result %s is not valid JSON", wantCode, decoded.Result)
	}

	return decoded
}

type fakeDeclarations struct {
	declareErr  error
	listErr     error
	declared    []models.Declaration
	infos       []models.QueryInfo
	reportedRow int
	askedFor    uuid.UUID
}

func (f *fakeDeclarations) Declare(_ context.Context, decl models.Declaration) error {
	if f.declareErr != nil {
		return f.declareErr
	}

	f.declared = append(f.declared, decl)

	return nil
}

func (f *fakeDeclarations) ListQueries(_ context.Context, nodeID uuid.UUID, infos *[]models.QueryInfo) (int, error) {
	f.askedFor = nodeID

	if f.listErr != nil {
		return 0, f.listErr
	}

	(*infos) = append((*infos), f.infos...)

	n := len(f.infos)
	if f.reportedRow >= 0 && f.reportedRow < n {
		n = f.reportedRow
	}

	return n, nil
}

type declareCase struct {
	decls    *fakeDeclarations
	name     string
	command  protocols.Command
	payload  string
	wantBody string
	wantMsg  string
	wantCode int
}

// assertEveryCommandCovered fails when a command the service advertises has no
// case in the table, so a newly registered command cannot ship untested.
func assertEveryCommandCovered(t *testing.T, tests []declareCase) {
	t.Helper()

	h := newCaptureHandlers()
	if err := SetupDeclaration(&fakeDeclarations{}, h); err != nil {
		t.Fatalf("setup declaration handlers: %v", err)
	}

	var advertised []protocols.Command
	if n := h.AvailableCommand(&advertised); n != len(advertised) {
		t.Fatalf("AvailableCommand reported %d commands but filled %d", n, len(advertised))
	}

	if len(advertised) == 0 {
		t.Fatal("no command is registered")
	}

	covered := make(map[protocols.Command]bool, len(tests))
	for _, tt := range tests {
		covered[tt.command] = true
	}

	for _, cmd := range advertised {
		if !covered[cmd] {
			t.Errorf("command %s is registered but no case covers it", cmd)
		}
	}
}

func TestDeclarationHandlers(t *testing.T) {
	const (
		nodeID = "6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"
		decl   = `{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f","collection":"c",` +
			`"queries":[{"name":"detail","pipeline":[{"$match":{}}]}]}`
		node = `{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"}`
	)

	domainErr := errors.New("database is locked")

	invalidErr := errors.Join(usecases.ErrInvalidDeclaration, errors.New("collection may not be a system collection"))

	tests := []declareCase{
		{
			name: "declare success", command: "analytics.declare", payload: decl,
			wantCode: protocols.CodeSuccess,
			// The reply echoes the manifest that was stored, so a node can confirm
			// what the service now believes it offers.
			wantBody: `[{"name":"detail","pipeline":[{"$match":{}}]}]`,
		},
		{
			name: "list success", command: "analytics.queries.list", payload: node,
			decls: &fakeDeclarations{reportedRow: -1, infos: []models.QueryInfo{
				{Name: "detail", Description: "over time", ParamsSchema: []byte(`{"unit_time":"string"}`)},
				{Name: "summary"},
			}},
			wantCode: protocols.CodeSuccess,
			// The schema travels with the name: the caller reads it to decide
			// which parameters each query takes. The pipeline does not travel.
			wantBody: `[{"name":"detail","description":"over time","params_schema":{"unit_time":"string"}},{"name":"summary"}]`,
		},
		{
			// A truncated read must not be replied as if it were complete.
			name: "list truncates to the reported count", command: "analytics.queries.list", payload: node,
			decls: &fakeDeclarations{reportedRow: 2, infos: []models.QueryInfo{
				{Name: "a"}, {Name: "b"}, {Name: "c"},
			}},
			wantCode: protocols.CodeSuccess,
			wantBody: `[{"name":"a"},{"name":"b"}]`,
		},
		{
			name: "list of nothing", command: "analytics.queries.list", payload: node,
			decls:    &fakeDeclarations{reportedRow: -1},
			wantCode: protocols.CodeSuccess,
			wantBody: `[]`,
		},

		{name: "declare undecodable payload", command: "analytics.declare", payload: `{"node_id":`, wantCode: protocols.CodeDecodePayload},
		{name: "list undecodable payload", command: "analytics.queries.list", payload: `{"id":`, wantCode: protocols.CodeDecodePayload},

		{
			// A rejected manifest is the node's mistake, so it is reported as a
			// payload problem rather than a domain failure -- the distinction tells
			// the node author to fix the manifest, not to retry.
			name: "declare an invalid manifest", command: "analytics.declare", payload: decl,
			decls:    &fakeDeclarations{declareErr: invalidErr},
			wantCode: protocols.CodeDecodePayload,
			wantMsg:  invalidErr.Error(),
		},
		{
			name: "declare domain error", command: "analytics.declare", payload: decl,
			decls:    &fakeDeclarations{declareErr: domainErr},
			wantCode: protocols.CodeDomain,
			wantMsg:  domainErr.Error(),
		},
		{
			name: "list domain error", command: "analytics.queries.list", payload: node,
			decls:    &fakeDeclarations{listErr: domainErr},
			wantCode: protocols.CodeDomain,
			wantMsg:  domainErr.Error(),
		},
	}

	assertEveryCommandCovered(t, tests)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newCaptureHandlers()

			decls := tt.decls
			if decls == nil {
				decls = &fakeDeclarations{reportedRow: -1}
			}

			err := SetupDeclaration(decls, h)
			if err != nil {
				t.Fatalf("setup declaration handlers: %v", err)
			}

			decoded := assertWire(t, h.invoke(t, tt.command, []byte(tt.payload)), tt.wantCode)

			if tt.wantMsg != "" {
				if !strings.Contains(decoded.ErrorMessage(), tt.wantMsg) {
					t.Fatalf("message %q does not mention %q", decoded.ErrorMessage(), tt.wantMsg)
				}

				return
			}

			if tt.wantBody == "" {
				return
			}

			if string(decoded.Result) != tt.wantBody {
				t.Fatalf("body is %s, want %s", decoded.Result, tt.wantBody)
			}
		})
	}
}

// A declaration must reach the usecase intact: the node id, the collection and
// the pipeline bytes are all the node's, and this layer only decodes them.
// The list payload names its field node_id, like every other analytics command.
// It was decoded into models.Node first, which reads {"id": ...} instead -- the
// wrong name, and one no node would ever send, so the handler answered an empty
// list and looked like an empty registry.
func TestListQueriesReadsTheNodeIDField(t *testing.T) {
	decls := &fakeDeclarations{reportedRow: -1, infos: []models.QueryInfo{{Name: "detail"}}}

	hs := newCaptureHandlers()
	if err := SetupDeclaration(decls, hs); err != nil {
		t.Fatalf("setup: %v", err)
	}

	_ = hs.invoke(t, "analytics.queries.list", []byte(`{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"}`))

	if want := uuid.MustParse("6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f"); decls.askedFor != want {
		t.Fatalf("list asked for node %s, want %s", decls.askedFor, want)
	}
}

func TestDeclarePassesTheManifestThrough(t *testing.T) {
	h := newCaptureHandlers()
	decls := &fakeDeclarations{reportedRow: -1}

	if err := SetupDeclaration(decls, h); err != nil {
		t.Fatalf("setup declaration handlers: %v", err)
	}

	payload := `{"node_id":"6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f","collection":"c",` +
		`"queries":[{"name":"detail","pipeline":[{"$sort":{"b":-1,"a":1}}]}]}`

	assertWire(t, h.invoke(t, "analytics.declare", []byte(payload)), protocols.CodeSuccess)

	if len(decls.declared) != 1 {
		t.Fatalf("declared %d manifests, want 1", len(decls.declared))
	}

	got := decls.declared[0]
	if got.Collection != "c" || got.NodeID != uuid.MustParse("6f1c1a2e-1f4a-4a1c-9b0e-1a2b3c4d5e6f") {
		t.Fatalf("manifest arrived as %+v", got)
	}

	// The operand order of $sort is part of the query's meaning, so the bytes
	// must not be normalised on the way through.
	if string(got.Queries[0].Pipeline) != `[{"$sort":{"b":-1,"a":1}}]` {
		t.Fatalf("pipeline is %s", got.Queries[0].Pipeline)
	}
}
