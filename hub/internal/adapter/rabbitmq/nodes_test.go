package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
)

// analyticsCall is one recorded RPC: what was asked and with which payload.
type analyticsCall struct {
	command protocols.Command
	payload []byte
}

// analyticsRouter answers the two commands the endpoint uses: the node's
// declaration list and one query per declared name. A name in refuse answers with
// a domain failure, which is how a test drives the per-query error path.
// Field order is govet-fieldalignment's: pointer-bearing fields are packed
// first so the GC scans the fewest leading bytes.
type analyticsRouter struct {
	rows    map[string]string
	refuse  map[string]error
	infos   []models.QueryInfo
	calls   []analyticsCall
	badList bool
}

func (r *analyticsRouter) Send(_ context.Context, c protocols.Command, payload []byte, _ protocols.SiblingName) (uuid.UUID, error) {
	r.calls = append(r.calls, analyticsCall{command: c, payload: payload})

	return uuid.New(), nil
}

func (r *analyticsRouter) Recv(_ context.Context, _ uuid.UUID, resp *protocols.RouterResponse) error {
	call := r.calls[len(r.calls)-1]

	switch call.command {
	case "analytics.queries.list":
		if r.badList {
			*resp = protocols.ErrorResponse(protocols.CodeDomain, errors.New("declarations unavailable"))

			return nil
		}

		body, err := json.Marshal(r.infos)
		if err != nil {
			*resp = protocols.ErrorResponse(protocols.CodeMarshalReply, err)

			return nil
		}

		*resp = protocols.ResultResponse(body)

	case "analytics.query":
		var req models.AnalyticsQueryRequest
		if err := json.Unmarshal(call.payload, &req); err != nil {
			*resp = protocols.ErrorResponse(protocols.CodeDecodePayload, err)

			return nil
		}

		if err, ok := r.refuse[req.QueryName]; ok {
			*resp = protocols.ErrorResponse(protocols.CodeDomain, err)

			return nil
		}

		*resp = protocols.ResultResponse([]byte(r.rows[req.QueryName]))

	default:
		*resp = protocols.ErrorResponse(protocols.CodeNotFound, errors.New("unknown command"))
	}

	return nil
}

func (r *analyticsRouter) Forget(_ uuid.UUID) {}

// declared builds the two-query manifest bustle publishes, so the tests exercise
// the shape a real node sends rather than a convenient one.
func declared() []models.QueryInfo {
	return []models.QueryInfo{
		{
			Name:         "counts_detail",
			Description:  "Average count per class over time",
			ParamsSchema: json.RawMessage(`{"start_time":"string","unit_time":"string"}`),
		},
		{
			Name:         "counts_summary",
			Description:  "Min, average and max per class",
			ParamsSchema: json.RawMessage(`{"start_time":"string"}`),
		},
	}
}

func rowsByQuery() map[string]string {
	return map[string]string{
		"counts_detail":  `[{"name":"car","points":[["2026-10-18T15:38:56Z",1.5]]}]`,
		"counts_summary": `[{"categories":["car"],"series":[{"name":"min","values":[1]}]}]`,
	}
}

func analyticsNode(r *analyticsRouter) nodes {
	return NewNodes(r, fakeSiblings{names: []protocols.SiblingName{"rpc.direct.analytics0"}}, inlineRunner{})
}

func getReport(t *testing.T, r *analyticsRouter, cluster string) (models.AnalyticsReport, error) {
	t.Helper()

	buf := make([]byte, 0, 4096)

	n, err := analyticsNode(r).GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: cluster}, &buf)
	if err != nil {
		return models.AnalyticsReport{}, err
	}

	if n != len(buf) {
		t.Fatalf("GetAnalytics reported %d bytes but wrote %d", n, len(buf))
	}

	var report models.AnalyticsReport
	if decodeErr := json.Unmarshal(buf[:n], &report); decodeErr != nil {
		t.Fatalf("decoding the report: %v", decodeErr)
	}

	return report, nil
}

func queryRequests(t *testing.T, r *analyticsRouter) []models.AnalyticsQueryRequest {
	t.Helper()

	reqs := make([]models.AnalyticsQueryRequest, 0, len(r.calls))

	for _, call := range r.calls {
		if call.command != "analytics.query" {
			continue
		}

		var req models.AnalyticsQueryRequest
		if decodeErr := json.Unmarshal(call.payload, &req); decodeErr != nil {
			t.Fatalf("decoding a query payload: %v", decodeErr)
		}

		reqs = append(reqs, req)
	}

	return reqs
}

// The window is a presentation choice, and it is the hub's to make: the node
// publishes a pipeline with ${start_time} in it and never decides how much
// history to show.
func TestAnalyticsWindowCoversEachCluster(t *testing.T) {
	now := time.Date(2026, 10, 18, 15, 38, 56, 0, time.UTC)

	cases := []struct {
		cluster string
		unit    string
		window  time.Duration
	}{
		{cluster: "minutes", window: 5 * time.Minute, unit: "second"},
		{cluster: "hours", window: 5 * time.Hour, unit: "minute"},
		{cluster: "dailies", window: 5 * 24 * time.Hour, unit: "hour"},
		{cluster: "weeks", window: 5 * 7 * 24 * time.Hour, unit: "day"},
	}

	for _, tc := range cases {
		win := analyticsWindow(tc.cluster, now)

		if win.UnitTime != tc.unit {
			t.Fatalf("cluster %q picked unit %q, want %q", tc.cluster, win.UnitTime, tc.unit)
		}

		if want := now.Add(-tc.window).Format(time.RFC3339); win.StartTime != want {
			t.Fatalf("cluster %q started at %s, want %s", tc.cluster, win.StartTime, want)
		}

		if win.TimeCluster != tc.cluster {
			t.Fatalf("window echoed cluster %q, want %q", win.TimeCluster, tc.cluster)
		}
	}
}

// An absent or misspelled cluster has to fall back to the widest window rather
// than an empty one, and the echoed cluster has to say which window was used --
// otherwise the caller cannot tell a five-week chart from a five-minute one.
func TestAnalyticsWindowFallsBackToTheWidestWindow(t *testing.T) {
	now := time.Date(2026, 10, 18, 15, 38, 56, 0, time.UTC)

	for _, cluster := range []string{"", "monthlies", "HOURS"} {
		win := analyticsWindow(cluster, now)

		if win.TimeCluster != "weeks" {
			t.Fatalf("cluster %q echoed %q, want weeks", cluster, win.TimeCluster)
		}

		if win.UnitTime != "day" {
			t.Fatalf("cluster %q picked unit %q, want day", cluster, win.UnitTime)
		}

		if want := now.Add(-5 * 7 * 24 * time.Hour).Format(time.RFC3339); win.StartTime != want {
			t.Fatalf("cluster %q started at %s, want %s", cluster, win.StartTime, want)
		}
	}
}

// The window has to be UTC. $dateTrunc groups on the stored timestamps, which
// are UTC, so a local-zone string would shift every bucket by the offset.
func TestAnalyticsWindowIsUTC(t *testing.T) {
	zone := time.FixedZone("UTC+7", 7*60*60)
	now := time.Date(2026, 10, 18, 22, 38, 56, 0, zone)

	win := analyticsWindow("hours", now)

	if !strings.HasSuffix(win.StartTime, "Z") {
		t.Fatalf("window start %q is not UTC", win.StartTime)
	}

	if want := "2026-10-18T10:38:56Z"; win.StartTime != want {
		t.Fatalf("window start is %s, want %s", win.StartTime, want)
	}
}

// No query name is baked into the hub: it asks what the node declared and runs
// that list. A node type with different charts therefore needs no hub change.
func TestGetAnalyticsRunsEveryDeclaredQueryInNameOrder(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}

	report, err := getReport(t, r, "hours")
	if err != nil {
		t.Fatalf("GetAnalytics returned %v", err)
	}

	reqs := queryRequests(t, r)
	if len(reqs) != 2 {
		t.Fatalf("GetAnalytics ran %d queries, want 2", len(reqs))
	}

	if reqs[0].QueryName != "counts_detail" || reqs[1].QueryName != "counts_summary" {
		t.Fatalf("queries ran in order %q, %q; want the declared order", reqs[0].QueryName, reqs[1].QueryName)
	}

	if len(report.Queries) != 2 {
		t.Fatalf("the report holds %d queries, want 2", len(report.Queries))
	}

	for i, name := range []string{"counts_detail", "counts_summary"} {
		if report.Queries[i].Name != name {
			t.Fatalf("report entry %d is %q, want %q", i, report.Queries[i].Name, name)
		}
	}
}

// A query is sent only the parameters it declares. Sending unit_time to the
// summary would be rejected by the service, and withholding it from the detail
// would be a missing parameter -- so the schema is read rather than assumed.
func TestGetAnalyticsSendsOnlyTheParametersAQueryDeclares(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}

	if _, err := getReport(t, r, "dailies"); err != nil {
		t.Fatalf("GetAnalytics returned %v", err)
	}

	reqs := queryRequests(t, r)

	if _, ok := reqs[0].Params["unit_time"]; !ok {
		t.Fatal("the detail query was not sent the unit_time it declares")
	}

	if got := reqs[0].Params["unit_time"]; got != "hour" {
		t.Fatalf("the detail query got unit_time %v, want hour", got)
	}

	if _, ok := reqs[1].Params["unit_time"]; ok {
		t.Fatal("the summary query was sent a unit_time it does not declare")
	}

	if _, ok := reqs[1].Params["start_time"]; !ok {
		t.Fatal("the summary query was not sent the start_time it declares")
	}
}

// A declared parameter the endpoint cannot fill has to be reported, not sent as
// something else and not silently dropped: the query is unanswerable here, and
// the caller has to be able to see which chart is missing and why.
func TestGetAnalyticsReportsAQueryItCannotParameterise(t *testing.T) {
	infos := []models.QueryInfo{
		{
			Name:         "counts_by_class",
			Description:  "Per-class totals for chosen classes",
			ParamsSchema: json.RawMessage(`{"start_time":"string","classes":"array"}`),
		},
		{Name: "counts_summary", ParamsSchema: json.RawMessage(`{"start_time":"string"}`)},
	}
	r := &analyticsRouter{infos: infos, rows: rowsByQuery()}

	report, err := getReport(t, r, "hours")
	if err != nil {
		t.Fatalf("GetAnalytics returned %v for one unanswerable query", err)
	}

	if len(report.Queries) != 2 {
		t.Fatalf("the report holds %d queries, want 2", len(report.Queries))
	}

	if !strings.Contains(report.Queries[0].Error, "classes") {
		t.Fatalf("the unanswerable query does not name the missing parameter: %q", report.Queries[0].Error)
	}

	if report.Queries[1].Error != "" {
		t.Fatalf("the answerable query was reported as failing: %q", report.Queries[1].Error)
	}

	if reqs := queryRequests(t, r); len(reqs) != 1 {
		t.Fatalf("%d queries were sent, want only the answerable one", len(reqs))
	}
}

// The rows travel untouched under each query's own name: the hub proxies an
// aggregation it did not write, so re-encoding them would make every node's
// document shape a change to the control plane.
func TestGetAnalyticsReturnsEachQueryUnderItsOwnName(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}

	report, err := getReport(t, r, "minutes")
	if err != nil {
		t.Fatalf("GetAnalytics returned %v", err)
	}

	byName := map[string]models.AnalyticsQueryResult{}
	for _, q := range report.Queries {
		byName[q.Name] = q
	}

	if !strings.Contains(string(byName["counts_detail"].Data), `"points"`) {
		t.Fatalf("counts_detail does not carry its rows: %s", byName["counts_detail"].Data)
	}

	if !strings.Contains(string(byName["counts_summary"].Data), `"categories"`) {
		t.Fatalf("counts_summary does not carry its rows: %s", byName["counts_summary"].Data)
	}

	if byName["counts_detail"].Description == "" {
		t.Fatal("the report dropped the query description")
	}

	if report.Window.TimeCluster != "minutes" || report.Window.UnitTime != "second" {
		t.Fatalf("the report window is %+v, want the minutes cluster", report.Window)
	}
}

// One failing query must not cost the caller the others: a dashboard that loses
// every chart because one of them broke is worse than one that names the break.
func TestGetAnalyticsKeepsGoingWhenOneQueryFails(t *testing.T) {
	r := &analyticsRouter{
		infos:  declared(),
		rows:   rowsByQuery(),
		refuse: map[string]error{"counts_detail": errors.New("result too large")},
	}

	report, err := getReport(t, r, "hours")
	if err != nil {
		t.Fatalf("GetAnalytics returned %v for one failed query", err)
	}

	if len(report.Queries) != 2 {
		t.Fatalf("the report holds %d queries, want 2", len(report.Queries))
	}

	if !strings.Contains(report.Queries[0].Error, "result too large") {
		t.Fatalf("the failed query does not carry the service message: %q", report.Queries[0].Error)
	}

	if report.Queries[0].Data != nil {
		t.Fatalf("the failed query carries data as well as an error: %s", report.Queries[0].Data)
	}

	if len(report.Queries[1].Data) == 0 {
		t.Fatal("the query that succeeded lost its rows")
	}
}

// When nothing could be answered the request has failed: a 200 with an empty
// report would read like a node that has never seen anything, which is a
// different fact from a service that cannot answer.
func TestGetAnalyticsFailsWhenNoQueryCouldBeAnswered(t *testing.T) {
	r := &analyticsRouter{
		infos:  declared(),
		rows:   rowsByQuery(),
		refuse: map[string]error{"counts_detail": errors.New("boom"), "counts_summary": errors.New("boom")},
	}

	buf := make([]byte, 0, 4096)

	_, err := analyticsNode(r).GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "hours"}, &buf)
	if err == nil {
		t.Fatal("GetAnalytics returned nil when every query failed")
	}

	if !strings.Contains(err.Error(), "boom") {
		t.Fatalf("the error does not carry the failure: %v", err)
	}
}

// A node that declared nothing is legitimate -- a node type with no analytics --
// and answers with an empty list rather than an error.
func TestGetAnalyticsWithNoDeclarationsReturnsAnEmptyReport(t *testing.T) {
	r := &analyticsRouter{infos: []models.QueryInfo{}}

	report, err := getReport(t, r, "hours")
	if err != nil {
		t.Fatalf("GetAnalytics returned %v for a node with no declarations", err)
	}

	if len(report.Queries) != 0 {
		t.Fatalf("the report holds %d queries, want none", len(report.Queries))
	}

	if reqs := queryRequests(t, r); len(reqs) != 0 {
		t.Fatalf("%d queries were sent for a node that declared none", len(reqs))
	}
}

// Reusing the pooled buffer must not append to what a previous request left
// there, or the second response carries the first one's rows as well.
func TestGetAnalyticsOverwritesAPooledBuffer(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}
	buf := []byte("stale bytes from the last request")

	n, err := analyticsNode(r).GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "minutes"}, &buf)
	if err != nil {
		t.Fatalf("GetAnalytics returned %v", err)
	}

	if strings.Contains(string(buf[:n]), "stale") {
		t.Fatalf("the pooled buffer still holds the previous body: %s", buf[:n])
	}
}

// A failing declaration list is the service being unreachable for this purpose,
// so it has to surface rather than become an empty report.
func TestGetAnalyticsFailsWhenTheDeclarationsCannotBeRead(t *testing.T) {
	r := &analyticsRouter{badList: true}

	buf := make([]byte, 0, 4096)

	_, err := analyticsNode(r).GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "hours"}, &buf)
	if err == nil {
		t.Fatal("GetAnalytics returned nil when the declaration list failed")
	}

	if !strings.Contains(err.Error(), "declarations unavailable") {
		t.Fatalf("the error does not carry the service message: %v", err)
	}
}

func TestGetAnalyticsFailsWhenNoAnalyticsServiceIsUp(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}
	buf := make([]byte, 0, 4096)

	n := NewNodes(r, fakeSiblings{}, inlineRunner{})

	_, err := n.GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "hours"}, &buf)
	if err == nil {
		t.Fatal("GetAnalytics returned nil with no analytics sibling")
	}

	if !strings.Contains(err.Error(), "analytics.queries.list") {
		t.Fatalf("the error does not name the missing command: %v", err)
	}

	if len(r.calls) != 0 {
		t.Fatalf("GetAnalytics sent %d calls with no sibling to serve them", len(r.calls))
	}
}

// A transport failure has to surface too, not become an empty report.
func TestGetAnalyticsReportsATransportFailure(t *testing.T) {
	r := &failingRouter{err: errors.New("channel closed")}
	buf := make([]byte, 0, 4096)

	n := NewNodes(r, fakeSiblings{names: []protocols.SiblingName{"rpc.direct.analytics0"}}, inlineRunner{})

	if _, err := n.GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "hours"}, &buf); err == nil {
		t.Fatal("GetAnalytics returned nil for a closed channel")
	}
}

// failingRouter refuses to send, which is what a dropped broker connection looks
// like to the adapter.
type failingRouter struct {
	err error
}

func (r *failingRouter) Send(_ context.Context, _ protocols.Command, _ []byte, _ protocols.SiblingName) (uuid.UUID, error) {
	return uuid.Nil, r.err
}

func (r *failingRouter) Recv(_ context.Context, _ uuid.UUID, _ *protocols.RouterResponse) error {
	return r.err
}

func (r *failingRouter) Forget(_ uuid.UUID) {}

// TestGetAnalyticsRefusesANodeThatIsNotRunning pins the F6 guard: a declaration
// outlives the node that made it, so answering for a node that is not
// advertising itself would produce a report of empty shapes that reads exactly
// like a node that saw nothing.
func TestGetAnalyticsRefusesANodeThatIsNotRunning(t *testing.T) {
	r := &analyticsRouter{}

	n := NewNodes(
		r,
		fakeSiblings{names: []protocols.SiblingName{"rpc.direct.analytics0"}, dead: true},
		inlineRunner{},
	)

	buf := make([]byte, 0, 4096)

	_, err := n.GetAnalytics(context.Background(), uuid.New(), nil, AnalyticsFilters{TimeCluster: "hours"}, &buf)
	if !errors.Is(err, models.ErrNodeUnavailable) {
		t.Fatalf("GetAnalytics error is %v, want ErrNodeUnavailable", err)
	}

	// Nothing was asked of the analytics service: a refused report must not cost
	// a round trip.
	if len(r.calls) != 0 {
		t.Errorf("GetAnalytics sent %d commands to a node that is not running", len(r.calls))
	}
}

// The fleet list travels with every query, not only the ones that need it,
// because the scope belongs to the node type's manifest: a fleet-scoped query is
// one the hub did not write and cannot recognise, so the service decides whether
// the list means anything.
func TestGetAnalyticsSendsTheFleetWithEveryQuery(t *testing.T) {
	r := &analyticsRouter{infos: declared(), rows: rowsByQuery()}

	member := uuid.New()
	fleet := []uuid.UUID{uuid.New(), member}

	buf := make([]byte, 0, 4096)

	if _, err := analyticsNode(r).GetAnalytics(context.Background(), uuid.New(), fleet, AnalyticsFilters{}, &buf); err != nil {
		t.Fatalf("get analytics: %v", err)
	}

	sent := 0

	for _, call := range r.calls {
		if call.command != "analytics.query" {
			continue
		}

		var req models.AnalyticsQueryRequest
		if err := json.Unmarshal(call.payload, &req); err != nil {
			t.Fatalf("decode %s: %v", call.command, err)
		}

		if len(req.Fleet) != len(fleet) || req.Fleet[1] != member {
			t.Fatalf("query %q carried fleet %v, want %v", req.QueryName, req.Fleet, fleet)
		}

		sent++
	}

	if sent == 0 {
		t.Fatal("no query was sent")
	}
}
