package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/booleanism/surr/internal/db"
	"github.com/booleanism/surr/internal/models"
	"github.com/google/uuid"
)

// ErrBufferFull reports that a caller-supplied result buffer could not hold
// every row. A truncated read must not be indistinguishable from a small result
// set, which is what returning a short count with a nil error would do.
var ErrBufferFull = errors.New("result buffer full")

// ErrNothingToDeclare reports a declaration carrying no queries. It is refused
// here as well as in the usecase because an empty manifest would delete the
// node's previous one and leave nothing in its place.
var ErrNothingToDeclare = errors.New("nothing to declare")

// ErrDeclarationNotFound reports a lookup for a query the node never declared.
// It is the shared sentinel re-exported, because the handler that answers a
// missing declaration has to recognise it without importing this package.
var ErrDeclarationNotFound = models.ErrDeclarationNotFound

const (
	insertDeclarationStmt = `
		INSERT INTO declarations (node_id, name, collection, description, params_schema, pipeline, updated_at, node_type, scope)
		VALUES %s
	`
	deleteDeclarationStmt = `DELETE FROM declarations WHERE node_id = ?`

	// deleteFleetStmt removes the fleet manifest of a node type. It is the same
	// rule as deleteDeclarationStmt one level up: a fleet manifest is a set owned
	// by the type, so the node that declares one replaces whatever the type had
	// before, including queries it no longer lists. Without this a fleet query
	// from a retired build would stay answerable forever, because the node that
	// declared it is not the node that would have to undeclare it.
	deleteFleetStmt = `DELETE FROM declarations WHERE node_type = ? AND scope = 'fleet'`

	selectQueryNamesStmt = `SELECT name FROM declarations WHERE node_id = ? ORDER BY name`

	// selectQueryInfosStmt answers what one caller may run: the node's own
	// node-scoped queries plus the fleet queries of its type.
	//
	// The type comes from the node's own rows rather than from the caller, so a
	// node cannot claim to be a type it is not -- and a node that has declared
	// nothing selects NULL, which matches no fleet row.
	selectQueryInfosStmt = `
		SELECT name, description, params_schema, scope
		FROM declarations
		WHERE (node_id = ? AND scope = 'node')
		   OR (scope = 'fleet' AND node_type = (SELECT node_type FROM declarations WHERE node_id = ? LIMIT 1))
		ORDER BY name
	`

	selectQueryStmt = `
		SELECT collection, description, params_schema, pipeline, scope, node_type
		FROM declarations
		WHERE node_id = ? AND name = ?
	`

	// selectFleetQueryStmt finds a fleet query by the type of the asking node. It
	// is the second half of the lookup in SelectQuery: a node asks for a name, and
	// the name may belong to its own manifest or to its type's.
	selectFleetQueryStmt = `
		SELECT collection, description, params_schema, pipeline
		FROM declarations
		WHERE scope = 'fleet'
		  AND name = ?
		  AND node_type = (SELECT node_type FROM declarations WHERE node_id = ? LIMIT 1)
	`
)

type declaration struct {
	*db.Transaction
}

func NewDeclaration(txFactory db.TXFactoryFunc) declaration {
	return declaration{
		Transaction: txFactory(),
	}
}

// UpsertDeclaration replaces the whole manifest for one node.
//
// Delete-then-insert rather than INSERT OR REPLACE: a manifest is a set, and a
// re-declare that drops a query has to remove it. Upserting only the queries
// that were sent would keep answering a query the node no longer offers, and
// the caller has no way to tell the two apart.
func (d declaration) UpsertDeclaration(ctx context.Context, decl models.Declaration) error {
	if len(decl.Queries) == 0 {
		return fmt.Errorf("%w: node %s", ErrNothingToDeclare, decl.NodeID)
	}

	cleanup, err := d.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	defer cleanup()

	if _, err = d.DB.ExecContext(ctx, deleteDeclarationStmt, decl.NodeID[:]); err != nil {
		_ = d.Rollback()
		return err
	}

	// The fleet manifest of this node's type is replaced only when this manifest
	// carries one. A node that declares no fleet query leaves the type's fleet
	// manifest alone: it is not that node's to withdraw, and every node of the
	// type would otherwise be able to erase a rollup by re-declaring itself.
	if hasFleet(decl.Queries) {
		if _, err = d.DB.ExecContext(ctx, deleteFleetStmt, decl.NodeType); err != nil {
			_ = d.Rollback()
			return err
		}
	}

	valueStrings := make([]string, 0, len(decl.Queries))
	valueArgs := make([]any, 0, len(decl.Queries)*9)
	now := time.Now().Unix()

	for _, query := range decl.Queries {
		valueStrings = append(valueStrings, "(?, ?, ?, ?, ?, ?, ?, ?, ?)")
		valueArgs = append(valueArgs,
			decl.NodeID[:],
			query.Name,
			decl.Collection,
			query.Description,
			rawOrFallback(query.ParamsSchema, "{}"),
			rawOrFallback(query.Pipeline, "[]"),
			now,
			decl.NodeType,
			query.NormalizedScope(),
		)
	}

	stmt := fmt.Sprintf(insertDeclarationStmt, strings.Join(valueStrings, ", "))

	if _, err = d.DB.ExecContext(ctx, stmt, valueArgs...); err != nil {
		_ = d.Rollback()
		return err
	}

	return d.Commit()
}

// SelectQueryNames returns the declared query names for one node, sorted.
//
// Sorted because the reply is read by a caller that may show the list to a
// human, and because SQLite's row order is not otherwise promised.
func (d declaration) SelectQueryNames(ctx context.Context, nodeID uuid.UUID, names *[]string) (int, error) {
	rws, err := d.DB.QueryContext(ctx, selectQueryNamesStmt, nodeID[:])
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		if i >= cap(*names) {
			return i, fmt.Errorf("%w: stopped after %d rows", ErrBufferFull, i)
		}

		*names = append(*names, "")

		if err = rws.Scan(&(*names)[i]); err != nil {
			return i, err
		}

		i++
	}

	return i, rws.Err()
}

// SelectQueryInfos returns every declared query for one node, sorted by name,
// with the metadata a caller needs to run each of them and nothing else.
//
// Sorted for the same reason as SelectQueryNames: the reply drives what a human
// is shown, and SQLite's row order is not otherwise promised. The order is also
// what makes a report stable across requests.
func (d declaration) SelectQueryInfos(ctx context.Context, nodeID uuid.UUID, infos *[]models.QueryInfo) (int, error) {
	rws, err := d.DB.QueryContext(ctx, selectQueryInfosStmt, nodeID[:], nodeID[:])
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	i := 0
	for rws.Next() {
		if i >= cap(*infos) {
			return i, fmt.Errorf("%w: stopped after %d rows", ErrBufferFull, i)
		}

		*infos = append(*infos, models.QueryInfo{})

		var paramsSchema string

		if err = rws.Scan(&(*infos)[i].Name, &(*infos)[i].Description, &paramsSchema, &(*infos)[i].Scope); err != nil {
			return i, err
		}

		(*infos)[i].ParamsSchema = json.RawMessage(paramsSchema)
		i++
	}

	return i, rws.Err()
}

// SelectQuery returns one declared query with the collection it reads.
//
// A name is looked up in two places, in this order: the asking node's own
// manifest, then the fleet manifest of its type. The order is what lets a node
// shadow a fleet query with a local one without the two colliding, and it is
// why a node that declared a fleet query can still run it -- its own row is
// found first, and that row is the fleet row.
func (d declaration) SelectQuery(ctx context.Context, nodeID uuid.UUID, name string) (models.DeclaredQuery, error) {
	var (
		collection   string
		description  string
		paramsSchema string
		pipeline     string
		scope        string
		nodeType     string
	)

	rw := d.DB.QueryRowContext(ctx, selectQueryStmt, nodeID[:], name)

	err := rw.Scan(&collection, &description, &paramsSchema, &pipeline, &scope, &nodeType)
	if err == nil {
		return models.DeclaredQuery{
			Collection: collection,
			Scope:      scope,
			Query: models.Query{
				Name:         name,
				Description:  description,
				Scope:        scope,
				ParamsSchema: json.RawMessage(paramsSchema),
				Pipeline:     json.RawMessage(pipeline),
			},
		}, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return models.DeclaredQuery{}, err
	}

	rw = d.DB.QueryRowContext(ctx, selectFleetQueryStmt, name, nodeID[:])

	err = rw.Scan(&collection, &description, &paramsSchema, &pipeline)
	if errors.Is(err, sql.ErrNoRows) {
		return models.DeclaredQuery{}, fmt.Errorf("%w: node %s query %s", ErrDeclarationNotFound, nodeID, name)
	}

	if err != nil {
		return models.DeclaredQuery{}, err
	}

	return models.DeclaredQuery{
		Collection: collection,
		Scope:      models.ScopeFleet,
		Query: models.Query{
			Name:         name,
			Description:  description,
			Scope:        models.ScopeFleet,
			ParamsSchema: json.RawMessage(paramsSchema),
			Pipeline:     json.RawMessage(pipeline),
		},
	}, nil
}

// SelectFleetCollections resolves a fleet's member collections.
//
// The type comes from the registry -- a fleet query belongs to a type -- and the
// ids come from the caller, which is the only party that knows what this user is
// allowed to read. Neither alone is enough: without the type a caller could
// union a collection of a different node type, and without the ids it could read
// another user's nodes. Intersecting them here is what keeps the service's
// promise that a manifest never names a collection.
//
// A node that declared nothing has no collection and is therefore not a member,
// which is the same rule that makes a fleet query unavailable to a node that has
// never introduced itself.
func (d declaration) SelectFleetCollections(ctx context.Context, nodeType string, nodeIDs []uuid.UUID, out *[]string) (int, error) {
	*out = (*out)[:0]

	if nodeType == "" || len(nodeIDs) == 0 {
		return 0, nil
	}

	args := make([]any, 0, len(nodeIDs)+1)
	args = append(args, nodeType)

	placeholders := make([]string, 0, len(nodeIDs))

	for _, id := range nodeIDs {
		placeholders = append(placeholders, "?")
		args = append(args, id[:])
	}

	stmt := fmt.Sprintf(`
		SELECT DISTINCT collection
		FROM declarations
		WHERE node_type = ? AND scope = 'node' AND node_id IN (%s)
		ORDER BY collection
	`, strings.Join(placeholders, ", "))

	rws, err := d.DB.QueryContext(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}
	defer rws.Close() //nolint:errcheck

	for rws.Next() {
		if len(*out) >= cap(*out) {
			return len(*out), fmt.Errorf("%w: stopped after %d collections", ErrBufferFull, len(*out))
		}

		*out = append(*out, "")

		if err = rws.Scan(&(*out)[len(*out)-1]); err != nil {
			return len(*out), err
		}
	}

	return len(*out), rws.Err()
}

// SelectNodeType returns the type a node declared itself as, or the empty
// string when it has never declared.
//
// It reads any of the node's rows rather than only a fleet one: the type is sent
// with every manifest, so a node that declares no fleet query still knows its own
// type -- and that is exactly the node a fleet query declared by a sibling has to
// be resolvable from.
func (d declaration) SelectNodeType(ctx context.Context, nodeID uuid.UUID) (string, error) {
	var nodeType string

	err := d.DB.QueryRowContext(ctx,
		`SELECT node_type FROM declarations WHERE node_id = ? AND node_type <> '' LIMIT 1`,
		nodeID[:],
	).Scan(&nodeType)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}

	if err != nil {
		return "", err
	}

	return nodeType, nil
}

// hasFleet reports whether a manifest declares any fleet-scoped query.
func hasFleet(queries []models.Query) bool {
	for _, query := range queries {
		if query.IsFleet() {
			return true
		}
	}

	return false
}

// rawOrFallback keeps an absent JSON field from being stored as the empty
// string. The empty string is not valid JSON, so a later decode of that column
// would fail on a row that is otherwise perfectly well formed.
func rawOrFallback(raw json.RawMessage, fallback string) string {
	if len(raw) == 0 {
		return fallback
	}

	return string(raw)
}
