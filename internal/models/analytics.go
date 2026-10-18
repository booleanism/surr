package models

import (
	"encoding/json"
	"errors"

	"github.com/google/uuid"
)

// ErrQueryTooLarge reports that a query hit the document or byte cap the
// analytics service enforces. It lives here rather than in the executor because
// it crosses three layers -- the Mongo client raises it, the usecase returns it
// and the handler maps it to a reply code -- and each of those would otherwise
// have to import the one below it.
var ErrQueryTooLarge = errors.New("query result too large")

// ErrDeclarationNotFound reports a lookup for a query a node never declared. It
// lives here for the same reason as ErrQueryTooLarge: the SQLite store raises
// it, the usecase passes it on and the handler turns it into the not-found reply
// code, and none of those layers should have to import the one below it.
var ErrDeclarationNotFound = errors.New("declaration not found")

// Query is one named aggregation a node declares about its own analytics.
//
// Pipeline stays raw JSON rather than a decoded structure, for two reasons.
// The document shape belongs to the node -- it is the node that writes those
// documents -- so the pipeline has to travel from the node instead of being
// written into the analytics service. And a decoded Go map would lose the
// operand order of an operator like $sort, which is part of the query's
// meaning: {"a": 1, "b": -1} and {"b": -1, "a": 1} sort differently, and a map
// preserves neither. The executor decodes these bytes into an order-preserving
// BSON array, while validation decodes them a second time into plain JSON,
// where only the set of keys matters.
//
// The field order is not free: govet's fieldalignment wants the pointer-bearing
// fields packed first so the GC scans the fewest leading bytes, and a slice
// carries two non-pointer words after its pointer. JSON is decoded by key on
// both sides, so the order costs nothing on the wire.
type Query struct {
	Name         string          `json:"name"`
	Description  string          `json:"description,omitempty"`
	Pipeline     json.RawMessage `json:"pipeline"`
	ParamsSchema json.RawMessage `json:"params_schema,omitempty"`
}

// Declaration is the manifest a node publishes with analytics.declare.
//
// It replaces whatever that node declared before rather than merging with it: a
// node owns its manifest whole, so a re-declare that drops a query really
// removes it instead of leaving a query behind that the node no longer offers.
//
// Field order follows the same rule as Query's; see the note there.
type Declaration struct {
	Collection string    `json:"collection"`
	Queries    []Query   `json:"queries"`
	NodeID     uuid.UUID `json:"node_id"`
}

// DeclaredQuery is one stored query together with the collection it reads.
//
// The collection is per node rather than per query, so a lookup by query name
// has to carry both back to the caller.
type DeclaredQuery struct {
	Collection string `json:"collection"`
	Query      Query  `json:"query"`
}

// NodeRef is the payload of a command that acts on one node and nothing else.
//
// It exists so the payload names the field it means. analytics.declare and
// analytics.query both carry node_id, and decoding analytics.queries.list into
// models.Node instead would quietly accept {"id": ...} -- a different field name
// that no other analytics command uses.
type NodeRef struct {
	NodeID uuid.UUID `json:"node_id"`
}

// AnalyticsQueryRequest is the payload of analytics.query.
// Field order follows the same rule as Query's; see the note there.
type AnalyticsQueryRequest struct {
	Params    map[string]any `json:"params,omitempty"`
	QueryName string         `json:"query_name"`
	NodeID    uuid.UUID      `json:"node_id"`
}
