package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/booleanism/surr/internal/models"
	"github.com/booleanism/surr/internal/pools"
	"github.com/booleanism/surr/internal/protocols"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

// AnalyticsFilters is the HTTP-level filter the endpoint accepts.
type AnalyticsFilters struct {
	TimeCluster string `json:"time_cluster"`
}

type nodes struct {
	r protocols.Router
	s Siblings
	t RunTask
}

func NewNodes(r protocols.Router, s Siblings, t RunTask) nodes {
	return nodes{
		r: r,
		s: s,
		t: t,
	}
}

// GetAnalytics builds a report from everything the node declared.
//
// No query name appears here. The charts belong to the node type -- a detector
// that counts people and one that reads plates want different views -- so the
// endpoint asks the analytics service what this node declared and runs each of
// them. A new node type with a new set of charts needs no change in the hub.
//
// The window is the one thing the endpoint decides, because it is a choice about
// how much history to show rather than about what the documents mean. Each query
// is sent only the parameters its own schema declares, and a query asking for
// something the endpoint cannot supply is reported as such instead of failing
// the whole report.
func (n nodes) GetAnalytics(ctx context.Context, nodeID uuid.UUID, fleet []uuid.UUID, f AnalyticsFilters, buf *[]byte) (int, error) {
	// Checked before the round trip, and after the caller has established that
	// this user owns the node: a manifest outlives the node that declared it, so
	// without this the endpoint answers every query with its empty shape and the
	// report reads as "this node saw nothing" instead of "this node is gone"
	// (TODO.md F6).
	if !n.s.NodeAlive(nodeID) {
		return 0, models.ErrNodeUnavailable
	}

	win := analyticsWindow(f.TimeCluster, time.Now())

	infos := make([]models.QueryInfo, 0, 8)

	payload, err := json.Marshal(&models.NodeRef{NodeID: nodeID})
	if err != nil {
		return 0, err
	}

	if listErr := n.do(ctx, "analytics.queries.list", payload, &infos); listErr != nil {
		return 0, listErr
	}

	results := make([]models.AnalyticsQueryResult, 0, len(infos))
	answered := 0

	for _, info := range infos {
		result := models.AnalyticsQueryResult{Name: info.Name, Description: info.Description}

		params, missing := analyticsParams(info.ParamsSchema, win)
		if len(missing) > 0 {
			// Refused before the round trip: the service would reject a
			// parameter it never declared, and would reject the absence of one
			// it did, so there is nothing to learn from asking.
			result.Error = fmt.Sprintf("the endpoint cannot supply %s", strings.Join(missing, ", "))
			results = append(results, result)

			continue
		}

		data, queryErr := n.query(ctx, nodeID, fleet, info.Name, params)
		if queryErr != nil {
			zerolog.Ctx(ctx).Warn().Str("query_name", info.Name).Err(queryErr).Msg("analytics query failed")

			result.Error = queryErr.Error()
			results = append(results, result)

			continue
		}

		result.Data = data
		results = append(results, result)
		answered++
	}

	if len(results) > 0 && answered == 0 {
		// Every query failed, which is a service that cannot answer rather than
		// a node with nothing to show: 200 with an empty report would read like
		// a node that has never seen anything.
		return 0, fmt.Errorf("no declared query could be answered: %s", results[0].Error)
	}

	body, err := json.Marshal(&models.AnalyticsReport{Window: win, Queries: results})
	if err != nil {
		return 0, err
	}

	*buf = append((*buf)[:0], body...)

	return len(*buf), nil
}

// analyticsWindow maps the endpoint's presentation-level time cluster onto the
// two parameters a declared query may take: where the window starts and how wide
// one bucket is.
//
// The mapping lives here rather than in the node because it is a choice about
// how much history to show, not about what the documents mean. A node that
// decided it would be baking one caller's idea of "the last five minutes" into
// its manifest, and the next caller would get the same five minutes.
//
// An unrecognised cluster becomes the widest window rather than an empty one: a
// typo in a query string must still return something a chart can draw, and the
// echoed cluster says which window was actually used.
func analyticsWindow(timeCluster string, now time.Time) models.AnalyticsWindow {
	var window time.Duration

	unit := "day"
	cluster := timeCluster

	switch timeCluster {
	case "minutes":
		window, unit = 5*time.Minute, "second"
	case "hours":
		window, unit = 5*time.Hour, "minute"
	case "dailies":
		window, unit = 5*24*time.Hour, "hour"
	default:
		window, cluster = 5*7*24*time.Hour, "weeks"
	}

	return models.AnalyticsWindow{
		TimeCluster: cluster,
		StartTime:   now.Add(-window).UTC().Format(time.RFC3339),
		UnitTime:    unit,
	}
}

// analyticsParams builds the parameter map for one query from the window, using
// only the names that query declares, and reports the declared names it cannot
// fill.
//
// Both directions matter. Sending a parameter the query never declared is a
// payload error at the service, and withholding one it does declare is a missing
// parameter -- so the schema is read rather than assumed. An absent schema means
// the query takes nothing, which is a legitimate query: an all-time total needs
// no window.
func analyticsParams(schema json.RawMessage, win models.AnalyticsWindow) (map[string]any, []string) {
	declared := map[string]string{}

	if len(schema) > 0 {
		// A schema that does not decode is the node's manifest problem, and the
		// service already validated it at declare time; treating it as empty
		// here keeps a malformed row from taking the endpoint down.
		_ = json.Unmarshal(schema, &declared)
	}

	params := make(map[string]any, len(declared))
	missing := make([]string, 0, len(declared))

	for name := range declared {
		switch name {
		case "start_time":
			params[name] = win.StartTime
		case "unit_time":
			params[name] = win.UnitTime
		default:
			missing = append(missing, name)
		}
	}

	slices.Sort(missing)

	return params, missing
}

// query runs one declared query and returns its rows untouched.
//
// The fleet list goes with every query, not only the ones that need it, because
// the scope belongs to the node type's manifest: a fleet-scoped query is one the
// hub did not write and cannot recognise, so the service decides whether the list
// means anything. It ignores it for a node-scoped query.
func (n nodes) query(ctx context.Context, nodeID uuid.UUID, fleet []uuid.UUID, name string, params map[string]any) (json.RawMessage, error) {
	payload, err := json.Marshal(&models.AnalyticsQueryRequest{
		NodeID:    nodeID,
		QueryName: name,
		Params:    params,
		Fleet:     fleet,
	})
	if err != nil {
		return nil, err
	}

	var rows json.RawMessage

	if queryErr := n.do(ctx, "analytics.query", payload, &rows); queryErr != nil {
		return nil, fmt.Errorf("query %s: %w", name, queryErr)
	}

	return rows, nil
}

func (n nodes) do(ctx context.Context, c protocols.Command, payload []byte, v any) error {
	sns := pools.SiblingNamePool.Get().(*[]protocols.SiblingName)
	defer pools.SiblingNamePoolResetAndPut(sns)

	nn := n.s.GetName(c, sns)
	if nn != 1 {
		return fmt.Errorf("expected one sibling with %s command", c)
	}

	sibling := (*sns)[0]

	resp, err := n.call(ctx, c, payload, sibling)
	if err != nil {
		return err
	}

	if resp.Code != 0 {
		msg := resp.ErrorMessage()
		zerolog.Ctx(ctx).Warn().Str("command", string(c)).Int("code", resp.Code).Msg("non-zero reply code")

		return fmt.Errorf("unexpected response code %d: %s", resp.Code, msg)
	}

	if v != nil {
		err = json.Unmarshal(resp.Result, v)
		if err != nil {
			return err
		}
	}

	return nil
}

// call performs one RPC round trip against sn. The reply is received on the
// shared goroutines pool so the caller's goroutine stays free, and the result
// is delivered over a buffered channel so a Receive that finishes after the
// caller gave up can never block a pool worker.
func (n nodes) call(ctx context.Context, command protocols.Command, payload []byte, sn protocols.SiblingName) (protocols.RouterResponse, error) {
	resp := protocols.RouterResponse{}

	trackID, err := n.r.Send(ctx, command, payload, sn)
	if err != nil {
		return resp, err
	}

	// Released on every path, including the timeout below, so an unanswered
	// request cannot leak a pending entry in the router's track map.
	defer n.r.Forget(trackID)

	recvErr := make(chan error, 1)

	err = n.t.Run(ctx, func() {
		log := zerolog.Ctx(ctx)

		rpcErr := n.r.Recv(ctx, trackID, &resp)
		if rpcErr != nil {
			log.Error().Str("command", string(command)).Str("track_id", trackID.String()).Err(rpcErr).Msg("receiving response")
		} else {
			log.Debug().Str("command", string(command)).Str("track_id", trackID.String()).Int("code", resp.Code).Msg("rpc round trip")
		}

		recvErr <- rpcErr
	})
	if err != nil {
		return resp, err
	}

	select {
	case rpcErr := <-recvErr:
		return resp, rpcErr
	case <-ctx.Done():
		return resp, ctx.Err()
	}
}
